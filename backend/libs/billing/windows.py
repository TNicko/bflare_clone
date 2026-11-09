"""Pure helpers for Free chat windows, tip-only sync, and pin caps."""

from __future__ import annotations

from typing import Any


def free_history_sync_forbidden(
    *,
    live_updates: bool,
    before_message_id: int | None,
    after_message_id: int | None,
    refresh_recent: bool,
) -> bool:
    """True when Free must reject older backfill, live poll, or refresh_recent."""
    if live_updates:
        return False
    return bool(refresh_recent or before_message_id is not None or after_message_id is not None)


def clamp_tip_sync_limit(limit: int, *, live_updates: bool, window: int | None) -> int:
    if live_updates or window is None:
        return limit
    return min(limit, window)


def page_is_past_window(*, floor_id: int | None, before_message_id: int | None) -> bool:
    if floor_id is None or before_message_id is None:
        return False
    return before_message_id <= floor_id


def next_before_for_window(
    *,
    row_count: int,
    limit: int,
    last_id: int | None,
    min_message_id: int | None,
) -> int | None:
    if last_id is None or row_count == 0:
        return None
    if row_count < limit:
        return None
    if min_message_id is not None and last_id <= min_message_id:
        return None
    return last_id


def pin_cap_for(limits: dict[str, Any], kind: str) -> int | None:
    pins = limits.get("pins")
    if not isinstance(pins, dict):
        return None
    cap = pins.get(kind)
    if cap is None:
        return None
    return int(cap)


def pin_limit_reached(count: int, cap: int | None) -> bool:
    return cap is not None and count >= cap
