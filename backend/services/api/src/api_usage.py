"""Charge API-key search calls against the monthly allowance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from fastapi import HTTPException, Request

from billing.api_credits import SpendSplit, credit_cost
from billing.enforce import decide_meter, resolve_plan_context
from billing.entitlements import effective_plan
from billing.rate_limit import EMBEDDING_RATE_METERS, TELEGRAM_RATE_METERS, rate_limit_retry_after
from db.crud.user.api_access import refund_api_credits, spend_api_credits
from db.crud.user.billing import get_current_override, get_current_subscription, get_plan
from services.api.src.auth.models import PlatformUser
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase import get_conn
from services.api.src.entitlements import _raise_denied
from usage.meters import Meter


@dataclass(frozen=True, slots=True)
class ApiCreditHold:
    plan_id: str
    cost: int
    split: SpendSplit
    request_id: str
    live: bool


def _live_flag(values: dict[str, object]) -> bool:
    return values.get("live") is True


async def assert_and_spend_api_credits(
    request: Request,
    meter: Meter | str,
    user: PlatformUser,
    *,
    live: bool,
    request_id: str,
) -> ApiCreditHold:
    meter_key = str(meter)
    feature_meter = str(Meter.SEARCH_NAME_TELEGRAM) if live and meter_key == str(Meter.SEARCH_NAME) else meter_key
    rate_meter = feature_meter if live and meter_key == str(Meter.SEARCH_NAME) else meter_key
    now = datetime.now(UTC)
    async with get_conn() as conn:
        override = await get_current_override(conn, user.id)
        subscription = await get_current_subscription(conn, user.id)
        plan_id, _source = effective_plan(override, subscription, now)
        plan = await get_plan(conn, plan_id)
        plan_id, source, features, _included = resolve_plan_context(override, subscription, plan, now=now)
    request.state.api_plan_id = plan_id
    decision = decide_meter(
        meter=feature_meter,
        plan_id=plan_id,
        source=source,
        features=features,
        included_units={},
        period_event_count=0,
        user_id=None,
    )
    if not decision.allowed:
        _raise_denied(
            user_id=user.id,
            meter=feature_meter,
            plan_id=decision.plan_id,
            code=decision.code,
            detail=decision.detail,
            http_status=int(decision.http_status or 403),
        )

    cfg = ConfigFactory.get_config()
    if rate_meter in TELEGRAM_RATE_METERS or rate_meter in EMBEDDING_RATE_METERS:
        retry_after = rate_limit_retry_after(
            user_id=user.id,
            meter=rate_meter,
            telegram_rpm=cfg.usage_telegram_rpm,
            embedding_rpm=cfg.usage_embedding_rpm,
        )
        if retry_after is not None:
            _raise_denied(
                user_id=user.id,
                meter=rate_meter,
                plan_id=plan_id,
                code="rate_limited",
                detail="Too many requests. Try again shortly.",
                http_status=429,
                retry_after=retry_after,
            )

    try:
        cost = credit_cost(meter_key, live=live)
    except ValueError:
        cost = 0
    if cost == 0:
        return ApiCreditHold(plan_id=plan_id, cost=0, split=SpendSplit(0, 0), request_id=request_id, live=live)

    async with get_conn() as conn:
        async with conn.transaction():
            split = await spend_api_credits(
                conn,
                user_id=user.id,
                plan_id=plan_id,
                cost=cost,
                request_id=request_id,
                now=now,
            )
    if split is None:
        raise HTTPException(
            status_code=402,
            detail={
                "code": "api_credits_exhausted",
                "detail": "API credit allowance is exhausted",
                "meter": meter_key,
                "plan_id": plan_id,
            },
        )
    return ApiCreditHold(plan_id=plan_id, cost=cost, split=split, request_id=request_id, live=live)


async def settle_api_credits(
    *,
    user_id: UUID,
    hold: ApiCreditHold,
    keep: int,
) -> int:
    refund = hold.split.total - keep
    if refund <= 0:
        return hold.split.total
    async with get_conn() as conn:
        async with conn.transaction():
            await refund_api_credits(
                conn,
                user_id=user_id,
                charged=hold.split,
                refund=refund,
                request_id=hold.request_id,
            )
    return keep
