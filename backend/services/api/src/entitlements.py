"""API-side plan enforcement before expensive product work."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, Request

from billing.catalog import PLAN_INTERNAL, plan_limits_for, quota_bucket_for
from billing.enforce import decide_meter, resolve_plan_context, should_enforce
from billing.entitlements import effective_plan
from billing.rate_limit import EMBEDDING_RATE_METERS, TELEGRAM_RATE_METERS, rate_limit_retry_after
from billing.windows import pin_cap_for, pin_limit_reached
from db.crud.user.billing import get_current_override, get_current_subscription, get_plan
from db.crud.user.usage import count_daily_meter_events
from services.api.src.auth.models import PlatformUser
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase import get_conn
from usage.meters import Meter

lg = logging.getLogger(__name__)


def _platform_user(request: Request) -> PlatformUser | None:
    user = getattr(request.state, "platform_user", None)
    return user if isinstance(user, PlatformUser) else None


def _http_detail(
    *,
    code: str | None,
    detail: str | None,
    plan_id: str | None,
    meter: str,
    retry_after: int | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "detail": detail, "meter": meter}
    if plan_id is not None:
        payload["plan_id"] = plan_id
    if retry_after is not None:
        payload["retry_after"] = retry_after
    return payload


def _raise_denied(
    *,
    user_id: Any,
    meter: str,
    plan_id: str | None,
    code: str | None,
    detail: str | None,
    http_status: int,
    retry_after: int | None = None,
) -> None:
    lg.info(
        "Meter denied user=%s meter=%s plan=%s code=%s",
        user_id,
        meter,
        plan_id,
        code,
    )
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    raise HTTPException(
        status_code=http_status,
        detail=_http_detail(
            code=code,
            detail=detail,
            plan_id=plan_id,
            meter=meter,
            retry_after=retry_after,
        ),
        headers=headers,
    )


async def current_plan_id_and_limits(request: Request) -> tuple[str, dict[str, Any]]:
    platform_user = _platform_user(request)
    if platform_user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    now = datetime.now(UTC)
    async with get_conn() as conn:
        override = await get_current_override(conn, platform_user.id)
        subscription = await get_current_subscription(conn, platform_user.id)
        plan_id, _ = effective_plan(override, subscription, now)
    return plan_id, plan_limits_for(plan_id)


async def current_plan_limits(request: Request) -> dict[str, Any]:
    _, limits = await current_plan_id_and_limits(request)
    return limits


def raise_history_not_in_plan(*, plan_id: str | None = None) -> None:
    payload: dict[str, Any] = {
        "code": "history_not_in_plan",
        "detail": "Upgrade for full history and live updates",
        "meter": str(Meter.TELEGRAM_SYNC),
    }
    if plan_id is not None:
        payload["plan_id"] = plan_id
    raise HTTPException(status_code=403, detail=payload)


async def require_internal_plan(request: Request) -> str:
    """Categories browse is internal-plan only."""
    plan_id, _ = await current_plan_id_and_limits(request)
    if plan_id != PLAN_INTERNAL:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "feature_not_in_plan",
                "detail": "Categories are available on the internal plan",
                "plan_id": plan_id,
            },
        )
    return plan_id


async def assert_pin_kind_allowed(
    conn: Any,
    *,
    user_id: Any,
    kind: str,
) -> None:
    now = datetime.now(UTC)
    override = await get_current_override(conn, user_id)
    subscription = await get_current_subscription(conn, user_id)
    plan_id, _ = effective_plan(override, subscription, now)
    limits = plan_limits_for(plan_id)
    cap = pin_cap_for(limits, kind)
    if cap is None:
        return
    from services.api.src.crud.supabase.projects import count_user_pins

    counts = await count_user_pins(conn, user_id=user_id)
    if pin_limit_reached(int(counts.get(kind, 0)), cap):
        raise HTTPException(
            status_code=402,
            detail={
                "code": "pin_limit_exceeded",
                "kind": kind,
                "included": cap,
                "plan_id": plan_id,
                "detail": f"This plan includes {cap} pinned {kind}",
            },
        )


async def assert_meter_allowed(
    request: Request,
    meter: Meter | str,
    *,
    user: PlatformUser | None = None,
    check_plan: bool | None = None,
) -> None:
    """Raise 403 / 402 / 429 when the caller may not spend this meter.

    Plan/feature checks run for expensive meters. Rate limits run for live
    Telegram and embedding meters on every plan.
    """
    meter_key = str(meter)
    platform_user = user or _platform_user(request)
    if platform_user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")

    enforce_plan = should_enforce(meter_key) if check_plan is None else check_plan
    needs_rate = meter_key in TELEGRAM_RATE_METERS or meter_key in EMBEDDING_RATE_METERS
    if not enforce_plan and not needs_rate:
        return

    cfg = ConfigFactory.get_config()
    plan_id: str | None = None
    source = "free"
    features: dict[str, Any] = {}
    included: dict[str, Any] = {}
    period_count = 0

    if enforce_plan:
        now = datetime.now(UTC)
        async with get_conn() as conn:
            override = await get_current_override(conn, platform_user.id)
            subscription = await get_current_subscription(conn, platform_user.id)
            plan_id, _ = effective_plan(override, subscription, now)
            plan = await get_plan(conn, plan_id)
            plan_id, source, features, included = resolve_plan_context(override, subscription, plan, now=now)
            period_count = 0
            quota_included = dict(included)
            bucket = quota_bucket_for(meter_key)
            if bucket is not None:
                cap = included.get(bucket.key)
                if cap is not None:
                    quota_included = {meter_key: int(cap)}
                    period_count = await count_daily_meter_events(
                        conn,
                        user_id=platform_user.id,
                        meters=bucket.meters,
                        moment=now,
                    )
        decision = decide_meter(
            meter=meter_key,
            plan_id=plan_id,
            source=source,
            features=features,
            included_units=quota_included if bucket is not None else included,
            period_event_count=period_count,
            user_id=None,
            telegram_rpm=0,
            embedding_rpm=0,
        )
        if not decision.allowed:
            _raise_denied(
                user_id=platform_user.id,
                meter=meter_key,
                plan_id=decision.plan_id,
                code=decision.code,
                detail=decision.detail,
                http_status=int(decision.http_status or 403),
            )

    if needs_rate:
        retry_after = rate_limit_retry_after(
            user_id=platform_user.id,
            meter=meter_key,
            telegram_rpm=cfg.usage_telegram_rpm,
            embedding_rpm=cfg.usage_embedding_rpm,
        )
        if retry_after is not None:
            _raise_denied(
                user_id=platform_user.id,
                meter=meter_key,
                plan_id=plan_id,
                code="rate_limited",
                detail="Too many requests. Try again shortly.",
                http_status=429,
                retry_after=retry_after,
            )
