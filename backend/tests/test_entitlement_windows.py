"""Free chat window, tip-only sync, and pin cap helpers."""

from __future__ import annotations

from billing.catalog import PLAN_FREE, PLAN_PRO, plan_limits_for
from billing.windows import (
    clamp_tip_sync_limit,
    free_history_sync_forbidden,
    next_before_for_window,
    page_is_past_window,
    pin_cap_for,
    pin_limit_reached,
)


def test_free_sync_allows_tip_only() -> None:
    limits = plan_limits_for(PLAN_FREE)
    assert limits["live_updates"] is False
    assert not free_history_sync_forbidden(
        live_updates=False,
        before_message_id=None,
        after_message_id=None,
        refresh_recent=False,
    )
    assert free_history_sync_forbidden(
        live_updates=False,
        before_message_id=50,
        after_message_id=None,
        refresh_recent=False,
    )
    assert free_history_sync_forbidden(
        live_updates=False,
        before_message_id=None,
        after_message_id=10,
        refresh_recent=False,
    )
    assert free_history_sync_forbidden(
        live_updates=False,
        before_message_id=None,
        after_message_id=None,
        refresh_recent=True,
    )


def test_pro_sync_allows_history_modes() -> None:
    assert not free_history_sync_forbidden(
        live_updates=True,
        before_message_id=50,
        after_message_id=None,
        refresh_recent=True,
    )


def test_free_tip_sync_limit_clamped_to_100() -> None:
    assert clamp_tip_sync_limit(1000, live_updates=False, window=100) == 100
    assert clamp_tip_sync_limit(50, live_updates=False, window=100) == 50
    assert clamp_tip_sync_limit(1000, live_updates=True, window=None) == 1000


def test_older_page_past_window_is_empty() -> None:
    assert page_is_past_window(floor_id=10, before_message_id=10) is True
    assert page_is_past_window(floor_id=10, before_message_id=9) is True
    assert page_is_past_window(floor_id=10, before_message_id=11) is False
    assert page_is_past_window(floor_id=None, before_message_id=1) is False


def test_next_before_clears_at_window_floor() -> None:
    assert next_before_for_window(row_count=50, limit=50, last_id=20, min_message_id=10) == 20
    assert next_before_for_window(row_count=50, limit=50, last_id=10, min_message_id=10) is None
    assert next_before_for_window(row_count=12, limit=50, last_id=40, min_message_id=10) is None
    assert next_before_for_window(row_count=50, limit=50, last_id=20, min_message_id=None) == 20


def test_free_pin_caps() -> None:
    limits = plan_limits_for(PLAN_FREE)
    assert pin_cap_for(limits, "users") == 5
    assert pin_cap_for(limits, "communities") == 5
    assert pin_cap_for(limits, "messages") == 5
    assert pin_limit_reached(5, 5) is True
    assert pin_limit_reached(4, 5) is False


def test_pro_pin_caps() -> None:
    limits = plan_limits_for(PLAN_PRO)
    assert pin_cap_for(limits, "users") == 100
    assert pin_cap_for(limits, "communities") == 100
    assert pin_cap_for(limits, "messages") == 100
    assert pin_limit_reached(100, 100) is True
    assert pin_limit_reached(99, 100) is False
