from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any, Literal

from utils.harvester_log import HarvesterLogFilter
from utils.user_log import UserLogFilter

LogFormat = Literal["rich", "plain", "json"]


def _quiet_third_party_loggers() -> None:
    """Keep noisy third-party lines off INFO so our logs stay readable."""
    for name in (
        "httpx",
        "httpcore",
        "h11",
        "botocore",
        "aiobotocore",
        "boto3",
        "aioboto3",
        "s3transfer",
        # Telethon update storms drown request logs in Docker/Loki.
        "telethon",
        "telethon.network",
        "telethon.client.updates",
        "telethon.client.telegrambaseclient",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def resolve_log_format(explicit: str | None = None) -> LogFormat:
    """Pick log format: env ``LOG_FORMAT`` wins, else rich on TTY, plain otherwise."""
    raw = (explicit if explicit is not None else os.getenv("LOG_FORMAT", "")).strip().lower()
    if raw in ("rich", "plain", "json"):
        return raw  # type: ignore[return-value]
    if os.getenv("NO_COLOR", "").strip():
        return "plain"
    if sys.stderr.isatty():
        return "rich"
    return "plain"


class _UtcFormatter(logging.Formatter):
    """ISO-8601 UTC timestamps without a trailing timezone name."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=UTC)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def exception_summary(exc_info: object) -> str:
    """One-line ``Type: message`` for log lines. Loki often displays only ``msg``."""
    exc = exc_info[1] if isinstance(exc_info, tuple) and len(exc_info) > 1 else None
    if not isinstance(exc, BaseException):
        return ""
    text = str(exc).strip().replace("\n", " ")
    name = type(exc).__name__
    return f"{name}: {text}" if text else name


class JsonFormatter(_UtcFormatter):
    """One JSON object per line — easy to parse in Loki / Grafana Explore."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        exc_text = ""
        if record.exc_info:
            exc_text = self.formatException(record.exc_info).strip()
            summary = exception_summary(record.exc_info)
            if summary and summary not in msg:
                msg = f"{msg}: {summary}"
        payload: dict[str, Any] = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "user": getattr(record, "user", None) or "system",
            "logger": record.name,
            "msg": msg,
        }
        harvester = getattr(record, "harvester", None)
        if harvester:
            payload["harvester"] = harvester
        if exc_text:
            payload["exc_info"] = exc_text
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _build_handler(fmt: LogFormat) -> logging.Handler:
    if fmt == "rich":
        from rich.console import Console
        from rich.highlighter import NullHighlighter
        from rich.logging import RichHandler
        from rich.text import Text

        class _AnsiRichHandler(RichHandler):
            def render_message(self, record: logging.LogRecord, message: str) -> Text:
                return Text.from_ansi(message)

        force_color = os.getenv("FORCE_COLOR", "").strip() != ""
        console = Console(
            highlight=False,
            stderr=True,
            force_terminal=force_color or None,
        )
        return _AnsiRichHandler(
            console=console,
            rich_tracebacks=True,
            # Paths become OSC-8 hyperlinks that look like garbage in Loki.
            show_path=False,
            markup=False,
            highlighter=NullHighlighter(),
            log_time_format="[%X]",
        )

    handler = logging.StreamHandler(sys.stderr)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            _UtcFormatter(
                fmt="%(asctime)s %(levelname)s [%(user)s] [%(name)s] %(message)s",
            )
        )
    return handler


def _attach_uvicorn_to_root() -> None:
    """Uvicorn installs its own handlers; point them at root for one format."""
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        log = logging.getLogger(name)
        log.handlers.clear()
        log.propagate = True


def configure_logging(*, level: str | None = None, log_format: str | None = None) -> None:
    """
    Configure service logging.

    Formats (``LOG_FORMAT`` env, or ``log_format=``):
      - ``rich``  — colored CLI (default when stderr is a TTY)
      - ``plain`` — UTC text lines, no ANSI (default in Docker / pipes)
      - ``json``  — one JSON object per line (best for Loki / Grafana)

    Also set ``PYTHONUNBUFFERED=1`` for timely log lines in containers.
    """
    chosen = (level or "INFO").upper()
    numeric = logging.getLevelNamesMapping().get(chosen, logging.INFO)
    fmt = resolve_log_format(log_format)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(numeric)

    handler = _build_handler(fmt)
    handler.setLevel(numeric)
    handler.addFilter(UserLogFilter())
    handler.addFilter(HarvesterLogFilter())
    if fmt == "rich":
        # RichHandler renders level/time itself; keep user + message.
        logging.basicConfig(
            level=numeric,
            format="[%(user)s] %(message)s",
            datefmt="[%X]",
            handlers=[handler],
            force=True,
        )
        # basicConfig replaces handlers — re-attach filters on the live handler.
        for h in logging.getLogger().handlers:
            h.addFilter(UserLogFilter())
            h.addFilter(HarvesterLogFilter())
    else:
        root.addHandler(handler)

    _attach_uvicorn_to_root()
    _quiet_third_party_loggers()
