"""API credit weights, allowance math, and public-route auth rules."""

from __future__ import annotations

from datetime import UTC, datetime

from uuid import UUID

from billing.api_credits import (
    CREDIT_TOPUP_KIND,
    FREE_API_ALLOWANCE,
    LIVE_NAME_EXTRA_CREDITS,
    NOTICE_ALLOWANCE_80,
    NOTICE_ALLOWANCE_EXHAUSTED,
    NOTICE_PREPAID_EMPTY,
    PRO_API_ALLOWANCE,
    CreditAccountState,
    SpendSplit,
    api_allowance_for,
    credit_cost,
    credit_notice_kind,
    credit_pack,
    credits_to_keep,
    parse_credit_payment,
    parse_credit_topup,
    quote_topup_credits,
    quote_spend,
    refund_split,
    roll_allowance_period,
)
from billing.catalog import PLAN_FREE, PLAN_INTERNAL, PLAN_PRO
from db.crud.user.api_access import hash_api_key
from services.api.src.auth.api_route import is_hidden_public_route, requires_api_key


def test_credit_weights() -> None:
    assert credit_cost("search.name") == 1
    assert credit_cost("search.username") == 1
    assert credit_cost("search.keyword") == 1
    assert credit_cost("search.bluesky") == 1
    assert credit_cost("search.phone") == 1
    assert credit_cost("search.semantic") == 2
    assert credit_cost("search.image") == 2
    assert credit_cost("search.image_hash") == 2
    assert credit_cost("search.image_hash_by_message") == 2
    assert LIVE_NAME_EXTRA_CREDITS == 3
    assert credit_cost("search.name", live=True) == 1 + LIVE_NAME_EXTRA_CREDITS
    assert credit_cost("search.semantic", live=True) == 2


def test_allowances() -> None:
    assert api_allowance_for(PLAN_FREE) == FREE_API_ALLOWANCE == 25
    assert api_allowance_for(PLAN_PRO) == PRO_API_ALLOWANCE == 500
    assert api_allowance_for(PLAN_INTERNAL) is None


def test_allowance_spent_before_prepaid() -> None:
    state = CreditAccountState(
        prepaid_credits=10, allowance_used=20, allowance_period_start=datetime(2026, 10, 1, tzinfo=UTC).date()
    )
    split = quote_spend(state, allowance=25, cost=6)
    assert split == SpendSplit(allowance=5, prepaid=1)


def test_exhausted_allowance_and_prepaid_is_unpaid() -> None:
    state = CreditAccountState(
        prepaid_credits=0, allowance_used=25, allowance_period_start=datetime(2026, 10, 14, tzinfo=UTC).date()
    )
    assert quote_spend(state, allowance=25, cost=1) is None


def test_internal_is_uncapped() -> None:
    state = CreditAccountState(
        prepaid_credits=0, allowance_used=10_000, allowance_period_start=datetime(2026, 10, 1, tzinfo=UTC).date()
    )
    split = quote_spend(state, allowance=None, cost=6)
    assert split == SpendSplit(allowance=6, prepaid=0)


def test_month_roll_clears_allowance_used() -> None:
    state = CreditAccountState(
        prepaid_credits=4, allowance_used=25, allowance_period_start=datetime(2026, 9, 1, tzinfo=UTC).date()
    )
    rolled = roll_allowance_period(state, datetime(2026, 10, 2, tzinfo=UTC))
    assert rolled.allowance_used == 0
    assert rolled.prepaid_credits == 4
    assert quote_spend(rolled, allowance=25, cost=1) == SpendSplit(allowance=1, prepaid=0)


def test_live_omit_keeps_indexed_credit_only() -> None:
    cost = 1 + LIVE_NAME_EXTRA_CREDITS
    assert credits_to_keep(cost=cost, live=True, live_omitted=True, omitted=False) == 1
    assert credits_to_keep(cost=cost, live=True, live_omitted=False, omitted=False) == cost
    assert credits_to_keep(cost=1, live=False, live_omitted=False, omitted=True) == 0


def test_refund_returns_prepaid_first() -> None:
    kept = refund_split(SpendSplit(allowance=1, prepaid=5), 5)
    assert kept == SpendSplit(allowance=1, prepaid=0)


def test_api_key_hash_is_not_the_secret() -> None:
    secret = "bf_live_example-secret"
    digest = hash_api_key(secret)
    assert digest != secret
    assert digest == hash_api_key(secret)
    assert len(digest) == 64


def test_credit_pack_is_the_flat_rate() -> None:
    assert credit_pack(500) is not None
    assert credit_pack(2_000) is None
    assert credit_pack(100) is None


def test_notice_kind_follows_allowance_then_prepaid() -> None:
    assert credit_notice_kind(allowance=500, allowance_used=399, prepaid_credits=0) is None
    assert credit_notice_kind(allowance=500, allowance_used=400, prepaid_credits=0) == NOTICE_ALLOWANCE_80
    assert credit_notice_kind(allowance=500, allowance_used=500, prepaid_credits=10) == NOTICE_ALLOWANCE_EXHAUSTED
    assert credit_notice_kind(allowance=500, allowance_used=500, prepaid_credits=0) == NOTICE_PREPAID_EMPTY
    assert credit_notice_kind(allowance=None, allowance_used=10_000, prepaid_credits=0) is None


def test_quote_topup_credits_uses_one_rate() -> None:
    assert quote_topup_credits(499) is None
    assert quote_topup_credits(500) == 313
    assert quote_topup_credits(800) == 500
    assert quote_topup_credits(1_000) == 625
    assert quote_topup_credits(2_499) == 1_562
    assert quote_topup_credits(2_500) == 1_563
    assert quote_topup_credits(5_000) == 3_125
    assert quote_topup_credits(9_000) == 5_625
    assert quote_topup_credits(10_000) == 6_250
    assert quote_topup_credits(1_000_000) == 625_000
    assert quote_topup_credits(1_000_001) is None


def test_parse_credit_topup_uses_the_amount_stripe_collected() -> None:
    user_id = UUID("11111111-1111-1111-1111-111111111111")
    session = {
        "id": "cs_test_1",
        "currency": "usd",
        "amount_subtotal": 800,
        "client_reference_id": str(user_id),
        "metadata": {"kind": CREDIT_TOPUP_KIND, "credits": "500", "user_id": str(user_id)},
        "payment_status": "paid",
    }
    assert parse_credit_topup(session, paid=True) == (user_id, 500, "cs_test_1")
    assert parse_credit_topup(session, paid=False) is None
    session["amount_subtotal"] = 1_000
    session["metadata"]["credits"] = "625"
    assert parse_credit_topup(session, paid=True) == (user_id, 625, "cs_test_1")
    session["metadata"]["credits"] = "5000"
    assert parse_credit_topup(session, paid=True) is None
    session["amount_subtotal"] = 400
    session["metadata"]["credits"] = "0"
    assert parse_credit_topup(session, paid=True) is None


def test_parse_credit_payment_uses_the_collected_amount() -> None:
    user_id = UUID("11111111-1111-1111-1111-111111111111")
    intent = {
        "id": "pi_test_1",
        "currency": "usd",
        "amount": 1_000,
        "amount_received": 1_000,
        "status": "succeeded",
        "metadata": {"kind": CREDIT_TOPUP_KIND, "credits": "625", "user_id": str(user_id)},
    }
    assert parse_credit_payment(intent, paid=True) == (user_id, 625, "pi_test_1")
    assert parse_credit_payment(intent, paid=False) is None
    intent["metadata"]["credits"] = "500"
    assert parse_credit_payment(intent, paid=True) is None
    intent["id"] = "cs_test_1"
    intent["metadata"]["credits"] = "625"
    assert parse_credit_payment(intent, paid=True) is None


def test_v1_requires_api_key_and_hides_live_route() -> None:
    assert requires_api_key("/v1/name") is True
    assert requires_api_key("/search/name") is False
    assert requires_api_key("/project") is False
    assert requires_api_key("/billing/me") is False
    assert requires_api_key("/mcp") is True
    assert requires_api_key("/mcp/") is True
    assert requires_api_key("/mcp-other") is False
    assert is_hidden_public_route("/v1/name/telegram") is True
    assert is_hidden_public_route("/search/name/telegram") is False
