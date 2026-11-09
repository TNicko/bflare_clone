"""Plan feature and period-cap checks before expensive product work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from billing.catalog import PLAN_FREE, features_for, included_units_for
from billing.entitlements import effective_plan, override_is_active
from billing.rate_limit import rate_limit_retry_after


@dataclass(frozen=True, slots=True)
class MeterDecision:
    allowed: bool
    plan_id: str
    source: str
    code: str | None = None
    detail: str | None = None
    retry_after: int | None = None
    http_status: int | None = None


def meter_enabled(features: dict[str, Any] | None, meter: str) -> bool:
    if not isinstance(features, dict):
        return False
    return bool(features.get(meter, False))


def meter_included(included_units: dict[str, Any] | None, meter: str) -> int | None:
    if not isinstance(included_units, dict) or meter not in included_units:
        return None
    raw = included_units[meter]
    if raw is None:
        return None
    return int(raw)


def decide_meter(
    *,
    meter: str,
    plan_id: str,
    source: str,
    features: dict[str, Any] | None,
    included_units: dict[str, Any] | None,
    period_event_count: int,
    user_id: UUID | None = None,
    telegram_rpm: int = 0,
    embedding_rpm: int = 0,
    now: float | None = None,
) -> MeterDecision:
    """Feature off → 403. Cap exhausted → 402. Rate limit → 429."""
    if not meter_enabled(features, meter):
        return MeterDecision(
            allowed=False,
            plan_id=plan_id,
            source=source,
            code="feature_not_in_plan",
            detail=f"{meter} is not included in the {plan_id} plan",
            http_status=403,
        )

    included = meter_included(included_units, meter)
    if included is not None and period_event_count >= included:
        return MeterDecision(
            allowed=False,
            plan_id=plan_id,
            source=source,
            code="quota_exceeded",
            detail=f"{meter} daily quota of {included} is exhausted",
            http_status=402,
        )

    if user_id is not None:
        retry_after = rate_limit_retry_after(
            user_id=user_id,
            meter=meter,
            telegram_rpm=telegram_rpm,
            embedding_rpm=embedding_rpm,
            now=now,
        )
        if retry_after is not None:
            return MeterDecision(
                allowed=False,
                plan_id=plan_id,
                source=source,
                code="rate_limited",
                detail="Too many requests. Try again shortly.",
                retry_after=retry_after,
                http_status=429,
            )

    return MeterDecision(allowed=True, plan_id=plan_id, source=source)


def resolve_plan_context(
    override: dict[str, Any] | None,
    subscription: dict[str, Any] | None,
    plan: dict[str, Any] | None,
    *,
    now: datetime,
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    plan_id, source = effective_plan(override, subscription, now)
    if plan is not None and str(plan.get("id")) == plan_id:
        raw_features = plan.get("features")
        features: dict[str, Any] = dict(raw_features) if isinstance(raw_features, dict) else features_for(plan_id)
        raw_included = plan.get("included_units")
        included: dict[str, Any] = dict(raw_included) if isinstance(raw_included, dict) else {}
        return plan_id, source, features, included
    # Missing DB row: fall back to catalog so enforcement never fails open.
    fallback_id = plan_id if plan_id else PLAN_FREE
    return fallback_id, source, features_for(fallback_id), included_units_for(fallback_id)


def should_enforce(meter: str) -> bool:
    """Every metered route checks the plan. Daily buckets use included_units."""
    return True


def override_still_active(override: dict[str, Any] | None, now: datetime) -> bool:
    return override_is_active(override, now)
