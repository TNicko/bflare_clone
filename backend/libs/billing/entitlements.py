"""Effective plan precedence: override, subscription, grace, free."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from billing.catalog import PLAN_FREE
from usage.meters import METERS


def override_is_active(row: dict[str, Any] | None, now: datetime) -> bool:
    if not row or row.get("revoked_at") is not None:
        return False
    expires_at = row.get("expires_at")
    return expires_at is None or expires_at > now


def _active_subscription_source(subscription: dict[str, Any] | None, now: datetime) -> tuple[str, str] | None:
    if not subscription:
        return None
    status = str(subscription.get("status") or "")
    plan_id = str(subscription.get("plan_id") or PLAN_FREE)
    if status in ("trialing", "active"):
        return plan_id, "subscription"
    if status == "past_due":
        grace_days = int(subscription.get("past_due_grace_days") or 7)
        end = subscription.get("current_period_end")
        if isinstance(end, datetime) and now <= end + timedelta(days=grace_days):
            return plan_id, "past_due_grace"
    return None


def effective_plan(
    override: dict[str, Any] | None,
    subscription: dict[str, Any] | None,
    now: datetime,
) -> tuple[str, str]:
    """Return ``(plan_id, source)`` where source is override/subscription/grace/free."""
    if override_is_active(override, now):
        return str(override["plan_id"]), "override"
    sub = _active_subscription_source(subscription, now)
    if sub is not None:
        return sub
    return PLAN_FREE, "free"


def meter_entitlements(
    features: dict[str, Any] | None,
    included_units: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    flags = features if isinstance(features, dict) else {}
    units = included_units if isinstance(included_units, dict) else {}
    rows: list[dict[str, Any]] = []
    for meter in sorted(METERS):
        enabled = bool(flags.get(meter, False))
        included: int | None = None
        if enabled and meter in units and units[meter] is not None:
            included = int(units[meter])
        rows.append({"meter": meter, "enabled": enabled, "included": included})
    return rows
