"""API credit weights and allowance math. No database access."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from uuid import UUID

from billing.catalog import PLAN_FREE, PLAN_INTERNAL, PLAN_PRO

FREE_API_ALLOWANCE = 25
PRO_API_ALLOWANCE = 500
LIVE_NAME_EXTRA_CREDITS = 3

NOTICE_ALLOWANCE_80 = "allowance_80"
NOTICE_ALLOWANCE_EXHAUSTED = "allowance_exhausted"
NOTICE_PREPAID_EMPTY = "prepaid_empty"

CREDIT_TOPUP_KIND = "credit_topup"

_METER_CREDITS: dict[str, int] = {
    "search.name": 1,
    "search.username": 1,
    "search.phone": 1,
    "search.keyword": 1,
    "search.bluesky": 1,
    "search.semantic": 2,
    "search.image": 2,
    "search.image_hash": 2,
    "search.image_hash_by_message": 2,
}


@dataclass(frozen=True, slots=True)
class CreditAccountState:
    prepaid_credits: int
    allowance_used: int
    allowance_period_start: date


@dataclass(frozen=True, slots=True)
class SpendSplit:
    allowance: int
    prepaid: int

    @property
    def total(self) -> int:
        return self.allowance + self.prepaid


def api_allowance_for(plan_id: str) -> int | None:
    """Monthly included credits. None means the plan is not capped."""
    if plan_id == PLAN_FREE:
        return FREE_API_ALLOWANCE
    if plan_id == PLAN_PRO:
        return PRO_API_ALLOWANCE
    if plan_id == PLAN_INTERNAL:
        return None
    return FREE_API_ALLOWANCE


def month_start(moment: datetime) -> date:
    return date(moment.year, moment.month, 1)


def credit_cost(meter: str, *, live: bool = False) -> int:
    base = _METER_CREDITS.get(meter)
    if base is None:
        raise ValueError(f"Meter is not billed as API credits: {meter}")
    if meter == "search.name" and live:
        return base + LIVE_NAME_EXTRA_CREDITS
    return base


def credits_to_keep(
    *,
    cost: int,
    live: bool,
    live_omitted: bool,
    omitted: bool,
) -> int:
    """Credits that stay spent after the handler finishes.

    An omitted call keeps nothing. A community search
    whose live Telegram lookup was rehydrated keeps the indexed-search credit
    and drops the extra live credits.
    """
    if omitted:
        return 0
    if live and live_omitted:
        return max(0, cost - LIVE_NAME_EXTRA_CREDITS)
    return cost


def roll_allowance_period(state: CreditAccountState, moment: datetime) -> CreditAccountState:
    start = month_start(moment)
    if state.allowance_period_start == start:
        return state
    return CreditAccountState(
        prepaid_credits=state.prepaid_credits,
        allowance_used=0,
        allowance_period_start=start,
    )


def quote_spend(
    state: CreditAccountState,
    *,
    allowance: int | None,
    cost: int,
) -> SpendSplit | None:
    """How to pay ``cost``. None when the balance cannot cover it.

    Allowance is spent first. A null allowance is uncapped and records the
    whole cost against allowance_used.
    """
    if cost < 0:
        raise ValueError("cost must be >= 0")
    if cost == 0:
        return SpendSplit(allowance=0, prepaid=0)
    if allowance is None:
        return SpendSplit(allowance=cost, prepaid=0)
    remaining = max(0, allowance - state.allowance_used)
    from_allowance = min(remaining, cost)
    rest = cost - from_allowance
    if rest > state.prepaid_credits:
        return None
    return SpendSplit(allowance=from_allowance, prepaid=rest)


@dataclass(frozen=True, slots=True)
class CreditPack:
    credits: int
    amount_cents: int
    currency: str = "usd"


#: One rate for every amount: 500 credits per $8.
CREDIT_PACKS: tuple[CreditPack, ...] = (CreditPack(500, 800),)
MIN_TOPUP_CENTS = 500
MAX_TOPUP_CENTS = 1_000_000


def credit_pack(credits: int) -> CreditPack | None:
    for pack in CREDIT_PACKS:
        if pack.credits == credits:
            return pack
    return None


def topup_tier(amount_cents: int) -> CreditPack:
    """The single credit rate. Every amount uses it."""
    del amount_cents
    return CREDIT_PACKS[0]


def quote_topup_credits(amount_cents: int) -> int | None:
    """Credits purchased by ``amount_cents``, or None outside $5–$10,000."""
    if amount_cents < MIN_TOPUP_CENTS or amount_cents > MAX_TOPUP_CENTS:
        return None
    tier = topup_tier(amount_cents)
    credits = (amount_cents * tier.credits + tier.amount_cents // 2) // tier.amount_cents
    if credits <= 0:
        return None
    return credits


def credit_notice_kind(
    *,
    allowance: int | None,
    allowance_used: int,
    prepaid_credits: int,
) -> str | None:
    """Which usage alert the balance currently warrants, if any.

    Uncapped plans have no allowance threshold. Crossing 80% and then
    exhausting the allowance are separate kinds so each can notify once.
    An empty prepaid balance is alerted only once the allowance is also gone.
    """
    if allowance is None or allowance <= 0:
        return None
    if allowance_used >= allowance:
        if prepaid_credits <= 0:
            return NOTICE_PREPAID_EMPTY
        return NOTICE_ALLOWANCE_EXHAUSTED
    if allowance_used * 5 >= allowance * 4:
        return NOTICE_ALLOWANCE_80
    return None


def parse_credit_topup(session: dict[str, Any], *, paid: bool) -> tuple[UUID, int, str] | None:
    """User, credits, and Checkout session id for a paid credit top-up.

    Credits come from the amount Stripe collected, at the tier rate. Metadata
    that claims a different credit count is rejected.
    """
    if not paid:
        return None
    metadata = session.get("metadata") or {}
    if not isinstance(metadata, dict) or metadata.get("kind") != CREDIT_TOPUP_KIND:
        return None
    currency = str(session.get("currency") or "usd").lower()
    if currency != "usd":
        return None
    amount = _as_int(session.get("amount_subtotal"))
    if amount <= 0:
        amount = _as_int(session.get("amount_total"))
    credits = quote_topup_credits(amount)
    session_id = str(session.get("id") or "").strip()
    user_raw = metadata.get("user_id") or session.get("client_reference_id")
    if credits is None or _as_int(metadata.get("credits")) != credits or not session_id or not user_raw:
        return None
    try:
        user_id = UUID(str(user_raw))
    except ValueError:
        return None
    return user_id, credits, session_id


def parse_credit_payment(intent: dict[str, Any], *, paid: bool) -> tuple[UUID, int, str] | None:
    """User, credits, and PaymentIntent id for a succeeded in-app top-up.

    Credits come from the amount collected. Metadata that claims a different
    count is rejected.
    """
    if not paid:
        return None
    metadata = intent.get("metadata") or {}
    if not isinstance(metadata, dict) or metadata.get("kind") != CREDIT_TOPUP_KIND:
        return None
    currency = str(intent.get("currency") or "usd").lower()
    if currency != "usd":
        return None
    amount = _as_int(intent.get("amount_received"))
    if amount <= 0:
        amount = _as_int(intent.get("amount"))
    credits = quote_topup_credits(amount)
    payment_id = str(intent.get("id") or "").strip()
    user_raw = metadata.get("user_id")
    if (
        credits is None
        or _as_int(metadata.get("credits")) != credits
        or not payment_id.startswith("pi_")
        or not user_raw
    ):
        return None
    try:
        user_id = UUID(str(user_raw))
    except ValueError:
        return None
    return user_id, credits, payment_id


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def refund_split(charged: SpendSplit, refund: int) -> SpendSplit:
    """What remains charged after giving back ``refund`` credits.

    Prepaid is returned first, then allowance.
    """
    if refund < 0 or refund > charged.total:
        raise ValueError("refund must be between 0 and the amount charged")
    prepaid_back = min(charged.prepaid, refund)
    allowance_back = min(charged.allowance, refund - prepaid_back)
    return SpendSplit(
        allowance=charged.allowance - allowance_back,
        prepaid=charged.prepaid - prepaid_back,
    )
