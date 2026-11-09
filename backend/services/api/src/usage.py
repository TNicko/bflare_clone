"""Meter catalog and request recording for the usage ledger.

Records search, image, and Telegram sync/resolve calls. Expensive meters are
checked against the effective plan before the handler runs. Recording failures
are logged and never fail the request.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import time
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from functools import wraps
from typing import Any
from uuid import UUID

from fastapi import HTTPException, Request

from billing.api_credits import credits_to_keep
from billing.enforce import should_enforce
from db.crud.user.usage import (
    complete_usage_event,
    discard_started_usage_event,
    insert_started_usage_event,
)
from services.api.src.api_usage import ApiCreditHold, _live_flag, assert_and_spend_api_credits, settle_api_credits
from services.api.src.credit_notices import schedule_credit_notices
from services.api.src.posthog_capture import capture_api_search
from usage.meters import Meter
from services.api.src.auth.channel import is_billed_channel
from services.api.src.auth.models import PlatformUser
from services.api.src.crud.supabase import get_conn
from services.api.src.entitlements import assert_meter_allowed
from utils.request_spans import REQUEST_ID_HEADER, normalize_request_id

lg = logging.getLogger(__name__)

_PARAM_KEYS = (
    "scope",
    "kind",
    "limit",
    "offset",
    "date_from",
    "date_to",
    "peer_type",
    "external_id",
    "message_id",
    "mode",
    "since_days",
    "after_message_id",
    "before_message_id",
)


@dataclass
class UsageNote:
    """Mutable extras the handler fills before the decorator completes the event."""

    event_id: UUID | None = None
    skip_complete: bool = False
    query_text: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    result_count: int | None = None
    units: dict[str, int | float] = field(default_factory=dict)
    error_code: str | None = None
    #: Served from a saved run/row. Drop the event so it does not count as usage.
    omit: bool = False


def query_hash(query_text: str | None) -> str | None:
    """Stable grouping key: SHA-256 of NFKC + casefold + collapsed whitespace."""
    normalized = normalize_query(query_text)
    if not normalized:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def normalize_query(query_text: str | None) -> str | None:
    if query_text is None:
        return None
    folded = unicodedata.normalize("NFKC", query_text).casefold()
    collapsed = " ".join(folded.split())
    return collapsed or None


def store_query_text(query_text: str | None) -> str | None:
    if query_text is None:
        return None
    stripped = query_text.strip()
    return stripped or None


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return str(value)
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return str(value)


def status_for_http(status_code: int) -> str:
    if 400 <= status_code < 500:
        return "rejected"
    return "failed"


def error_code_from_http(exc: HTTPException) -> str:
    detail = exc.detail
    if isinstance(detail, dict):
        code = detail.get("code") or detail.get("detail")
        if isinstance(code, str) and code.strip():
            return code.strip()[:80]
        return f"http_{exc.status_code}"
    if isinstance(detail, str) and detail.strip():
        return detail.strip()[:80]
    return f"http_{exc.status_code}"


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _paged_total(page: Any) -> int | None:
    """Full match count for a name-search page. Ignore lookup dicts (``.items()``)."""
    if page is None or isinstance(page, (dict, list, str, bytes)):
        return None
    total = _as_int(getattr(page, "total", None))
    if total is not None:
        return total
    items = getattr(page, "items", None)
    if isinstance(items, list):
        return len(items)
    return None


def infer_result_count(value: Any) -> int | None:
    """Unpaged match total when the response has one, otherwise rows returned."""
    if value is None:
        return None
    if isinstance(value, list):
        return len(value)
    synced = _as_int(getattr(value, "synced_count", None))
    if synced is not None:
        return synced

    # Keyword search: ``total`` is the full match count; ``users`` is a lookup map.
    total = _as_int(getattr(value, "total", None))
    if total is not None and not isinstance(value, dict):
        return total

    community_total = _paged_total(getattr(value, "communities", None))
    user_total = _paged_total(getattr(value, "users", None))
    if community_total is not None or user_total is not None:
        return (community_total or 0) + (user_total or 0)

    found = getattr(value, "found", None)
    if isinstance(found, bool):
        return 1 if found else 0
    items = getattr(value, "items", None)
    if isinstance(items, list):
        return len(items)
    messages = getattr(value, "messages", None)
    if isinstance(messages, list):
        return len(messages)
    return None


def infer_units(meter: str, value: Any, existing: dict[str, int | float]) -> dict[str, int | float]:
    units: dict[str, int | float] = {"searches": 1}
    units.update(existing)
    if meter in (Meter.TELEGRAM_SYNC, Meter.TELEGRAM_RESOLVE):
        synced = getattr(value, "synced_count", None)
        if synced is not None:
            units["messages_synced"] = int(synced)
    if meter == Meter.SEARCH_NAME_TELEGRAM:
        skipped = bool(getattr(value, "skipped", False))
        units["telegram_calls"] = 0 if skipped else 1
        if skipped:
            units["searches"] = 0
    if meter == Meter.SEARCH_SEMANTIC:
        units.setdefault("embedding_calls", 1)
        units.setdefault("milvus_searches", 1)
    if meter == Meter.SEARCH_IMAGE:
        units.setdefault("embedding_calls", 1)
        units.setdefault("milvus_searches", 1)
    if meter in (Meter.SEARCH_IMAGE_HASH, Meter.SEARCH_IMAGE_HASH_BY_MESSAGE):
        units.setdefault("milvus_searches", 1)
    return units


def current_usage(request: Request) -> UsageNote | None:
    note = getattr(request.state, "usage_note", None)
    return note if isinstance(note, UsageNote) else None


def _request_id(request: Request) -> str:
    existing = getattr(request.state, "request_id", None)
    if isinstance(existing, str) and existing:
        return existing
    return normalize_request_id(request.headers.get(REQUEST_ID_HEADER))


def _platform_user(request: Request) -> PlatformUser | None:
    user = getattr(request.state, "platform_user", None)
    if isinstance(user, PlatformUser):
        return user
    return None


def _bound(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        bound = inspect.signature(fn).bind_partial(*args, **kwargs)
    except TypeError:
        return dict(kwargs)
    return dict(bound.arguments)


def _find_request(values: dict[str, Any]) -> Request | None:
    for value in values.values():
        if isinstance(value, Request):
            return value
    return None


def _infer_query_text(values: dict[str, Any]) -> str | None:
    q = values.get("q")
    if isinstance(q, str):
        return store_query_text(q)
    body = values.get("body")
    query = getattr(body, "query", None)
    if isinstance(query, str):
        return store_query_text(query)
    return None


def _infer_params(values: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key in _PARAM_KEYS:
        if key not in values or values[key] is None:
            continue
        params[key] = jsonable(values[key])
    body = values.get("body")
    if body is not None:
        for key in ("limit", "offset", "date_from", "date_to", "message_id", "date"):
            raw = getattr(body, key, None)
            if raw is not None and key not in params:
                params[key] = jsonable(raw)
    return params


def _apply_response(note: UsageNote, meter: str, value: Any) -> None:
    if note.result_count is None:
        note.result_count = infer_result_count(value)
    skipped = getattr(value, "skipped", None)
    if isinstance(skipped, bool):
        note.params.setdefault("skipped", skipped)
        if meter == Meter.SEARCH_NAME_TELEGRAM and skipped:
            # Rehydrated from telegram_search_query_runs — no Telegram call, not usage.
            note.omit = True
    note.units = infer_units(meter, value, note.units)


def metered(
    meter: Meter,
    *,
    enforce: bool | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Record one usage event around an authenticated route handler.

    ``enforce`` defaults to on for expensive meters. Pass ``False`` for routes
    that often serve a saved row and only bill when Telegram is actually called.
    """

    check = should_enforce(str(meter)) if enforce is None else enforce

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            values = _bound(fn, args, kwargs)
            request = _find_request(values)
            if request is None:
                return await fn(*args, **kwargs)

            user = _platform_user(request)
            channel = getattr(request.state, "usage_channel", "app")
            billed = is_billed_channel(channel)
            live = billed and _live_flag(values)
            if live and str(meter) == str(Meter.SEARCH_NAME) and str(values.get("scope", "all")) == "user":
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "live_requires_communities",
                        "detail": "live=true requires a community scope",
                    },
                )
            request_id = _request_id(request)
            hold: ApiCreditHold | None = None

            note = UsageNote(
                query_text=_infer_query_text(values),
                params=_infer_params(values),
            )
            # A stored image-search id reads another page. It does not run the search again.
            paging = bool(str(values.get("search_id") or "").strip())
            if paging:
                note.omit = True
            if live:
                note.params["live"] = True
            if billed:
                note.params["channel"] = channel
            request.state.usage_note = note
            started = time.perf_counter()
            event_id: UUID | None = None

            outcome = "succeeded"
            http_status = 200
            error_code: str | None = None
            result: Any = None
            try:
                if user is not None and billed and not paging:
                    hold = await assert_and_spend_api_credits(
                        request,
                        meter,
                        user,
                        live=live,
                        request_id=request_id,
                    )
                elif user is not None and not paging:
                    await assert_meter_allowed(
                        request,
                        meter,
                        user=user,
                        check_plan=check,
                    )

                if user is not None and not paging:
                    try:
                        async with get_conn() as conn:
                            event_id, existing_status = await insert_started_usage_event(
                                conn,
                                user_id=user.id,
                                request_id=request_id,
                                meter=str(meter),
                                route=request.url.path,
                                method=request.method,
                                query_text=note.query_text,
                                query_hash=query_hash(note.query_text),
                                params=jsonable(note.params),
                            )
                        note.event_id = event_id
                        if existing_status not in (None, "started"):
                            note.skip_complete = True
                    except Exception:
                        lg.exception("Failed to start usage event meter=%s", meter)

                result = await fn(*args, **kwargs)
                return result
            except HTTPException as exc:
                outcome = status_for_http(exc.status_code)
                http_status = exc.status_code
                error_code = error_code_from_http(exc)
                raise
            except Exception:
                outcome = "failed"
                http_status = 500
                error_code = "internal_error"
                raise
            finally:
                if note.error_code:
                    error_code = note.error_code
                if outcome == "succeeded":
                    try:
                        _apply_response(note, str(meter), result)
                    except Exception:
                        lg.exception("Failed to infer usage details meter=%s", meter)
                await _finish(
                    note,
                    outcome=outcome,
                    http_status=http_status,
                    error_code=error_code,
                    started=started,
                )
                if user is not None and billed:
                    kept = 0
                    plan_id = getattr(request.state, "api_plan_id", None)
                    if hold is not None:
                        plan_id = hold.plan_id
                        if outcome == "succeeded":
                            kept = credits_to_keep(
                                cost=hold.cost,
                                live=hold.live,
                                live_omitted=bool(note.params.get("live_omitted")),
                                omitted=note.omit,
                            )
                        try:
                            kept = await settle_api_credits(user_id=user.id, hold=hold, keep=kept)
                        except Exception:
                            lg.exception("Failed to settle API credits request=%s", request_id)
                            kept = hold.split.total if outcome == "succeeded" else 0
                    _schedule_api_search(
                        distinct_id=str(user.id),
                        route=request.url.path,
                        meter=str(meter),
                        credits=kept,
                        plan_id=str(plan_id) if plan_id else None,
                        live=live,
                        status=outcome,
                        error=error_code,
                        query_hash=query_hash(note.query_text),
                        channel=channel,
                    )
                    if outcome == "succeeded" and kept > 0:
                        schedule_credit_notices(
                            user_id=user.id,
                            email=user.email,
                            plan_id=str(plan_id) if plan_id else "free",
                        )

        wrapper.__signature__ = inspect.signature(fn)  # type: ignore[attr-defined]
        return wrapper

    return decorator


async def _finish(
    note: UsageNote,
    *,
    outcome: str,
    http_status: int,
    error_code: str | None,
    started: float,
) -> None:
    if note.event_id is None or note.skip_complete:
        return
    if note.omit:
        try:
            async with get_conn() as conn:
                await discard_started_usage_event(conn, note.event_id)
        except Exception:
            lg.exception("Failed to discard non-billable usage event %s", note.event_id)
        return
    duration_ms = max(0, int((time.perf_counter() - started) * 1000))
    units = note.units or {"searches": 1}
    try:
        async with get_conn() as conn:
            await complete_usage_event(
                conn,
                event_id=note.event_id,
                status=outcome,
                http_status=http_status,
                query_text=note.query_text,
                query_hash=query_hash(note.query_text),
                params=jsonable(note.params),
                result_count=note.result_count,
                duration_ms=duration_ms,
                units=units,
                error_code=error_code,
            )
    except Exception:
        lg.exception("Failed to complete usage event %s", note.event_id)


def _schedule_api_search(**kwargs: Any) -> None:
    task = asyncio.create_task(capture_api_search(**kwargs))

    def _done(done: asyncio.Task[None]) -> None:
        if done.cancelled():
            return
        exc = done.exception()
        if exc is not None:
            lg.exception("PostHog api_search task failed", exc_info=exc)

    task.add_done_callback(_done)
