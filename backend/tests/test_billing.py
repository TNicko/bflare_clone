"""Effective plan resolution. Enforcement is a later step."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from billing.catalog import PLAN_FREE, PLAN_INTERNAL, PLAN_PRO, features_for
from billing.entitlements import effective_plan, meter_entitlements, override_is_active


def _override(**kwargs: object) -> dict:
    row = {
        "plan_id": PLAN_PRO,
        "kind": "comp",
        "expires_at": None,
        "revoked_at": None,
    }
    row.update(kwargs)
    return row


def test_complimentary_override_beats_free() -> None:
    now = datetime.now(UTC)
    plan_id, source = effective_plan(_override(plan_id=PLAN_INTERNAL), None, now)
    assert plan_id == PLAN_INTERNAL
    assert source == "override"


def test_expired_trial_falls_through_to_free() -> None:
    now = datetime.now(UTC)
    row = _override(kind="trial", expires_at=now - timedelta(seconds=1))
    assert override_is_active(row, now) is False
    plan_id, source = effective_plan(row, None, now)
    assert plan_id == PLAN_FREE
    assert source == "free"


def test_revoked_override_is_inactive() -> None:
    now = datetime.now(UTC)
    row = _override(revoked_at=now)
    assert override_is_active(row, now) is False


def test_free_enables_quota_meters() -> None:
    meters = {row["meter"]: row for row in meter_entitlements(features_for(PLAN_FREE), {})}
    assert meters["search.name"]["enabled"] is True
    assert meters["search.semantic"]["enabled"] is True
    assert meters["search.image"]["enabled"] is True
    assert meters["search.bluesky"]["enabled"] is True
    assert meters["telegram.sync"]["enabled"] is True
    assert meters["search.name_telegram"]["enabled"] is False
    assert meters["telegram.resolve"]["enabled"] is False
    assert meters["telegram.media_download"]["enabled"] is False


def test_pro_enables_meters_without_hardcoded_caps() -> None:
    meters = {row["meter"]: row for row in meter_entitlements(features_for(PLAN_PRO), {})}
    assert meters["search.semantic"]["enabled"] is True
    assert meters["telegram.resolve"]["included"] is None


def test_active_subscription_applies_when_no_override() -> None:
    now = datetime.now(UTC)
    subscription = {"status": "active", "plan_id": PLAN_PRO}
    plan_id, source = effective_plan(None, subscription, now)
    assert plan_id == PLAN_PRO
    assert source == "subscription"


def test_past_due_grace_window_applies_then_expires() -> None:
    now = datetime.now(UTC)
    in_grace = {
        "status": "past_due",
        "plan_id": PLAN_PRO,
        "current_period_end": now - timedelta(days=2),
        "past_due_grace_days": 7,
    }
    assert effective_plan(None, in_grace, now) == (PLAN_PRO, "past_due_grace")
    expired = {
        "status": "past_due",
        "plan_id": PLAN_PRO,
        "current_period_end": now - timedelta(days=10),
        "past_due_grace_days": 7,
    }
    assert effective_plan(None, expired, now) == (PLAN_FREE, "free")
