"""Request-scoped timings for the Harvest API.

Callers record a span only while a request trace is active. Jobs, the usage
sweeper, and pool warm-up leave the context empty, so ``record_span`` is a
no-op there. Category totals are time spent inside that client; they overlap
(a Telegram lease that also runs SQL counts in both) and are not slices of the
request wall clock.

Loki keeps the log ``msg`` and drops other JSON fields, so the summary is a
single ``key=value`` line.
"""

from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from uuid import uuid4

REQUEST_ID_HEADER = "x-request-id"
_REQUEST_ID_MAX_LEN = 128

#: Logged in this order on every request summary.
SPAN_KINDS = ("db", "s3", "embed", "quickwit", "s3vectors", "stripe", "telegram")

_SPAN_LOG_MAX = 1200
_SPAN_LOG_MAX_COUNT = 40
_NAME_CAP = 160


@dataclass
class Span:
    kind: str
    name: str
    start_ms: float
    dur_ms: float


@dataclass
class RequestTrace:
    request_id: str
    started_at: float
    spans: list[Span] = field(default_factory=list)


_current: ContextVar[RequestTrace | None] = ContextVar("request_trace", default=None)


def normalize_request_id(raw: str | None) -> str:
    """Accept a client ``x-request-id`` or mint a UUID.

    Same rules as the usage ledger: printable, no spaces, at most 128 characters.
    The value must also be latin-1 so it can be echoed as a response header.
    """
    text = (raw or "").strip()
    if not text or len(text) > _REQUEST_ID_MAX_LEN:
        return str(uuid4())
    if any(not ch.isprintable() or ch == " " for ch in text):
        return str(uuid4())
    try:
        text.encode("latin-1")
    except UnicodeEncodeError:
        return str(uuid4())
    return text


def request_id_from_headers(headers: list[tuple[bytes, bytes]] | None) -> str:
    raw: bytes | None = None
    for key, value in headers or []:
        if key.lower() == b"x-request-id":
            raw = value
            break
    if raw is None:
        return normalize_request_id(None)
    return normalize_request_id(raw.decode("latin-1").strip())


def current_trace() -> RequestTrace | None:
    return _current.get()


def start_request(request_id: str) -> Token[RequestTrace | None]:
    trace = RequestTrace(request_id=request_id, started_at=time.perf_counter())
    return _current.set(trace)


def finish_request(token: Token[RequestTrace | None]) -> RequestTrace | None:
    trace = _current.get()
    _current.reset(token)
    return trace


def record_span(kind: str, name: str, dur_s: float) -> None:
    """Append a span to the active request. No-op when no request is active."""
    trace = _current.get()
    if trace is None:
        return
    dur_ms = max(0.0, dur_s * 1000.0)
    start_ms = max(0.0, (time.perf_counter() - trace.started_at) * 1000.0 - dur_ms)
    label = name.strip() or kind
    trace.spans.append(Span(kind=kind, name=label, start_ms=start_ms, dur_ms=dur_ms))


@asynccontextmanager
async def timed(kind: str, name: str) -> AsyncIterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        record_span(kind, name, time.perf_counter() - started)


@contextmanager
def timed_sync(kind: str, name: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        record_span(kind, name, time.perf_counter() - started)


def slow_trace_ms() -> int:
    """Requests at least this slow include a ``spans=`` timeline. ``0`` logs every request."""
    raw = os.getenv("API_TRACE_SLOW_MS", "500").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 500


def sql_span_name(query: object) -> str:
    """Statement text for a log line: collapsed, literals redacted, parameters omitted."""
    if isinstance(query, bytes):
        text = query.decode("utf-8", "replace")
    elif isinstance(query, str):
        text = query
    else:
        try:
            text = str(query)
        except Exception:
            text = "query"
    text = _redact_sql_literals(text)
    text = " ".join(text.split())
    if len(text) > _NAME_CAP:
        text = text[: _NAME_CAP - 3] + "..."
    return text or "query"


def format_request_summary(
    *,
    method: str,
    route: str,
    status: int,
    request_id: str,
    total_ms: float,
    spans: list[Span],
    include_spans: bool,
) -> str:
    totals = {kind: 0.0 for kind in SPAN_KINDS}
    counts = {kind: 0 for kind in SPAN_KINDS}
    slowest: Span | None = None
    for span in spans:
        if span.kind in totals:
            totals[span.kind] += span.dur_ms
            counts[span.kind] += 1
        if slowest is None or span.dur_ms > slowest.dur_ms:
            slowest = span

    parts = [
        "request",
        f"method={_token(method) or '-'}",
        f"route={_token(route) or '-'}",
        f"status={int(status)}",
        f"request_id={_token(request_id) or '-'}",
        f"total_ms={_ms(total_ms)}",
    ]
    for kind in SPAN_KINDS:
        parts.append(f"{kind}_ms={_ms(totals[kind])}")
        parts.append(f"{kind}_n={counts[kind]}")
    if slowest is None:
        parts.append("slowest=-")
        parts.append("slowest_ms=0")
    else:
        parts.append(f"slowest={_slowest_token(slowest)}")
        parts.append(f"slowest_ms={_ms(slowest.dur_ms)}")
    if include_spans and spans:
        parts.append(f'spans="{_spans_field(spans)}"')
    return " ".join(parts)


def _ms(value: float) -> int:
    return max(0, int(round(value)))


def _token(value: str) -> str:
    return " ".join(value.replace('"', "").split()).replace(" ", "_")


def _slowest_token(span: Span) -> str:
    name = _token(span.name)
    if len(name) > 100:
        name = name[:97] + "..."
    return f"{span.kind}:{name}" if name else span.kind


def _timeline_piece(span: Span) -> str:
    name = " ".join(span.name.replace('"', "").replace("@", " ").replace(",", " ").split())
    if len(name) > 80:
        name = name[:77] + "..."
    return f"{span.kind}:{name}@{_ms(span.start_ms)}+{_ms(span.dur_ms)}"


def _spans_field(spans: list[Span]) -> str:
    parts: list[str] = []
    used = 0
    for span in spans:
        piece = _timeline_piece(span)
        extra = len(piece) + (1 if parts else 0)
        if parts and (len(parts) >= _SPAN_LOG_MAX_COUNT or used + extra > _SPAN_LOG_MAX):
            return ",".join(parts) + ",..."
        if not parts and len(piece) > _SPAN_LOG_MAX:
            return piece[: _SPAN_LOG_MAX - 3] + "..."
        parts.append(piece)
        used += extra
    return ",".join(parts)


def _redact_sql_literals(text: str) -> str:
    """Replace single-quoted SQL string literals. Leaves identifiers and ``%s`` placeholders."""
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        if text[i] != "'":
            out.append(text[i])
            i += 1
            continue
        i += 1
        while i < n:
            if text[i] == "'" and i + 1 < n and text[i + 1] == "'":
                i += 2
                continue
            if text[i] == "'":
                i += 1
                break
            i += 1
        out.append("?")
    return "".join(out)
