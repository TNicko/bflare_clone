"""Billing: effective plan, Stripe checkout/portal, and webhook ingestion."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from billing.api_credits import (
    CREDIT_PACKS,
    MAX_TOPUP_CENTS,
    MIN_TOPUP_CENTS,
    parse_credit_payment,
    parse_credit_topup,
    quote_topup_credits,
)
from db.crud.user.api_access import api_credit_summary, grant_prepaid_topup
from db.crud.user.billing import (
    billing_snapshot,
    clear_billing_stripe_link,
    get_billing_account,
    get_billing_account_by_customer,
    get_or_create_billing_account,
    mark_stripe_event_processed,
    mark_stripe_event_received,
    set_billing_customer_id,
    upsert_subscription,
)
from services.api.src.auth.deps import get_current_user
from services.api.src.auth.models import PlatformUser
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase import get_conn
from services.api.src.stripe_client import (
    construct_event,
    create_checkout_session,
    create_credit_payment_intent,
    create_portal_session,
    create_pro_subscription_intent,
    create_setup_intent,
    detach_payment_method,
    ensure_stripe_customer,
    is_unknown_customer_error,
    list_invoices,
    list_payment_methods,
    retrieve_payment_intent,
    retrieve_subscription,
    set_default_payment_method,
    set_subscription_cancel_at_period_end,
    stripe_plan_for_price,
    to_dt,
    upcoming_invoice,
)

router = APIRouter(tags=["billing"])
lg = logging.getLogger(__name__)


class MeterEntitlementOut(BaseModel):
    meter: str
    enabled: bool
    included: int | None = None


class QuotaBucketOut(BaseModel):
    key: str
    used: int
    included: int | None = None
    resets_at: datetime


class PinLimitsOut(BaseModel):
    users: int
    communities: int
    messages: int


class PlanLimitsOut(BaseModel):
    chat_message_window: int | None = None
    media_unlocked_count: int | None = None
    pins: PinLimitsOut | None = None
    live_updates: bool = True
    media_download: bool = True


class BillingOverrideOut(BaseModel):
    id: UUID
    plan_id: str
    kind: Literal["comp", "trial"]
    starts_at: datetime
    expires_at: datetime | None = None
    granted_by: str
    reason: str | None = None
    revoked_at: datetime | None = None
    revoked_by: str | None = None


class ApiCreditsOut(BaseModel):
    allowance: int | None = None
    allowance_used: int
    allowance_remaining: int | None = None
    prepaid_credits: int
    spent_credits: int = 0
    period_start: date


class BillingMeOut(BaseModel):
    plan_id: str
    source: Literal["override", "subscription", "past_due_grace", "free"]
    period_start: datetime
    period_end: datetime
    override: BillingOverrideOut | None = None
    subscription: BillingSubscriptionOut | None = None
    meters: list[MeterEntitlementOut] = Field(default_factory=list)
    quotas: list[QuotaBucketOut] = Field(default_factory=list)
    limits: PlanLimitsOut
    api_credits: ApiCreditsOut


class BillingSubscriptionOut(BaseModel):
    stripe_subscription_id: str
    status: str
    plan_id: str
    current_period_start: datetime | None = None
    current_period_end: datetime | None = None
    cancel_at_period_end: bool = False


class CheckoutSessionOut(BaseModel):
    url: str
    session_id: str


class CreditPackOut(BaseModel):
    credits: int
    amount_cents: int
    currency: str


class CreditCheckoutIn(BaseModel):
    amount_cents: int = Field(ge=MIN_TOPUP_CENTS, le=MAX_TOPUP_CENTS)


class CreditPaymentIntentOut(BaseModel):
    publishable_key: str
    customer_id: str
    payment_intent_id: str
    client_secret: str
    customer_session_client_secret: str
    amount_due: int
    currency: str
    credits: int


class CreditPaymentConfirmIn(BaseModel):
    payment_intent_id: str = Field(min_length=3, max_length=255)


class PortalSessionOut(BaseModel):
    url: str


class SubscriptionIntentOut(BaseModel):
    publishable_key: str
    customer_id: str
    subscription_id: str
    client_secret: str
    customer_session_client_secret: str
    amount_due: int | None = None
    currency: str
    price_id: str


class PaymentMethodOut(BaseModel):
    id: str
    brand: str
    last4: str
    exp_month: int | None = None
    exp_year: int | None = None
    is_default: bool = False


class SetupIntentOut(BaseModel):
    publishable_key: str
    client_secret: str
    customer_session_client_secret: str
    setup_intent_id: str


class InvoiceOut(BaseModel):
    id: str
    number: str | None = None
    status: str
    currency: str
    amount_due: int | None = None
    amount_paid: int | None = None
    created: datetime | None = None
    hosted_invoice_url: str | None = None
    invoice_pdf: str | None = None
    receipt_url: str | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None


class UpcomingInvoiceLineOut(BaseModel):
    description: str
    amount: int
    currency: str


class UpcomingInvoiceOut(BaseModel):
    currency: str
    amount_due: int
    period_start: datetime | None = None
    period_end: datetime | None = None
    next_payment_attempt: datetime | None = None
    lines: list[UpcomingInvoiceLineOut] = Field(default_factory=list)


class CancelSubscriptionIn(BaseModel):
    cancel: bool = True


async def _require_customer_id(user: PlatformUser) -> str:
    async with get_conn() as conn:
        account = await get_billing_account(conn, user.id)
    customer_id = account.get("stripe_customer_id") if account else None
    if not customer_id:
        raise HTTPException(
            status_code=404,
            detail={"code": "no_billing_customer", "detail": "No billing account"},
        )
    return str(customer_id)


async def _drop_stale_stripe_customer(user_id: UUID, *, customer_id: str, exc: BaseException) -> None:
    if not is_unknown_customer_error(exc):
        return
    lg.warning(
        "Clearing stale Stripe customer user=%s customer_id=%s err=%s",
        user_id,
        customer_id,
        exc,
    )
    async with get_conn() as conn:
        await clear_billing_stripe_link(conn, user_id=user_id)


async def _persist_customer_id(user_id: UUID, *, previous: str | None, customer_id: str) -> None:
    if previous == customer_id:
        return
    async with get_conn() as conn:
        if previous and previous != customer_id:
            await clear_billing_stripe_link(conn, user_id=user_id)
        await set_billing_customer_id(conn, user_id=user_id, stripe_customer_id=customer_id)


def _billing_me_out(snapshot: dict[str, Any]) -> BillingMeOut:
    return BillingMeOut(
        plan_id=snapshot["plan_id"],
        source=snapshot["source"],
        period_start=snapshot["period_start"],
        period_end=snapshot["period_end"],
        override=_override_out(snapshot["override"]),
        subscription=_subscription_out(snapshot.get("subscription")),
        meters=[MeterEntitlementOut.model_validate(row) for row in snapshot["meters"]],
        quotas=[QuotaBucketOut.model_validate(row) for row in snapshot.get("quotas") or []],
        limits=PlanLimitsOut.model_validate(snapshot["limits"]),
        api_credits=ApiCreditsOut.model_validate(snapshot["api_credits"]),
    )


def _override_out(row: dict[str, Any] | None) -> BillingOverrideOut | None:
    if row is None:
        return None
    return BillingOverrideOut.model_validate(row)


def _subscription_out(row: dict[str, Any] | None) -> BillingSubscriptionOut | None:
    if row is None:
        return None
    return BillingSubscriptionOut.model_validate(row)


@router.get("/me", response_model=BillingMeOut)
async def billing_me(user: PlatformUser = Depends(get_current_user)) -> BillingMeOut:
    async with get_conn() as conn:
        snapshot = await billing_snapshot(conn, user.id)
        snapshot["api_credits"] = await api_credit_summary(conn, user.id, snapshot["plan_id"])
    return _billing_me_out(snapshot)


@router.post("/checkout", response_model=CheckoutSessionOut)
async def billing_checkout(user: PlatformUser = Depends(get_current_user)) -> CheckoutSessionOut:
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_secret_key.strip():
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    async with get_conn() as conn:
        account = await get_or_create_billing_account(conn, user.id)
    previous = account.get("stripe_customer_id")
    try:
        session = create_checkout_session(
            user_id=str(user.id),
            email=user.email,
            customer_id=previous,
        )
    except Exception:
        lg.exception("Stripe checkout session create failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    session_customer = session.get("customer")
    if isinstance(session_customer, str) and session_customer:
        await _persist_customer_id(user.id, previous=previous, customer_id=session_customer)
    return CheckoutSessionOut(url=str(session["url"]), session_id=str(session["id"]))


@router.get("/credit-packs", response_model=list[CreditPackOut])
async def billing_credit_packs(_user: PlatformUser = Depends(get_current_user)) -> list[CreditPackOut]:
    return [
        CreditPackOut(credits=pack.credits, amount_cents=pack.amount_cents, currency=pack.currency)
        for pack in CREDIT_PACKS
    ]


@router.post("/credits/intent", response_model=CreditPaymentIntentOut)
async def billing_credit_intent(
    body: CreditCheckoutIn,
    user: PlatformUser = Depends(get_current_user),
) -> CreditPaymentIntentOut:
    """Create a PaymentIntent so credit top-up uses the in-app payment form."""
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_secret_key.strip() or not cfg.stripe_publishable_key.strip():
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    if quote_topup_credits(body.amount_cents) is None:
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_topup_amount", "detail": "Enter an amount from $5 to $10,000"},
        )
    async with get_conn() as conn:
        account = await get_or_create_billing_account(conn, user.id)
    previous = account.get("stripe_customer_id")
    try:
        intent = create_credit_payment_intent(
            user_id=str(user.id),
            email=user.email,
            customer_id=previous,
            amount_cents=body.amount_cents,
        )
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_topup_amount", "detail": "Enter an amount from $5 to $10,000"},
        )
    except Exception:
        lg.exception("Stripe credit intent failed user=%s amount_cents=%s", user.id, body.amount_cents)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    await _persist_customer_id(user.id, previous=previous, customer_id=str(intent["customer_id"]))
    return CreditPaymentIntentOut.model_validate(intent)


@router.post("/credits/confirm")
async def billing_credit_confirm(
    body: CreditPaymentConfirmIn,
    user: PlatformUser = Depends(get_current_user),
) -> dict[str, int | bool]:
    """Grant credits after the in-app payment succeeds. Safe to call twice."""
    if not body.payment_intent_id.startswith("pi_"):
        raise HTTPException(status_code=422, detail={"code": "invalid_payment", "detail": "Unknown payment"})
    try:
        intent = retrieve_payment_intent(body.payment_intent_id)
    except Exception:
        lg.exception("Stripe credit confirm failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    metadata = intent.get("metadata") or {}
    if not isinstance(metadata, dict) or str(metadata.get("user_id") or "") != str(user.id):
        raise HTTPException(status_code=403, detail={"code": "forbidden", "detail": "Payment does not belong to you"})
    parsed = parse_credit_payment(intent, paid=intent.get("status") == "succeeded")
    if parsed is None:
        raise HTTPException(status_code=409, detail={"code": "payment_incomplete", "detail": "Payment is not complete"})
    _user_id, credits, payment_id = parsed
    await _grant_prepaid_credits(intent, user_id=_user_id, credits=credits, external_id=payment_id)
    return {"ok": True, "credits": credits}


@router.post("/subscription/intent", response_model=SubscriptionIntentOut)
async def billing_subscription_intent(user: PlatformUser = Depends(get_current_user)) -> SubscriptionIntentOut:
    """Create an incomplete Pro subscription for in-app Payment Element checkout."""
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_secret_key.strip() or not cfg.stripe_publishable_key.strip():
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    async with get_conn() as conn:
        account = await get_or_create_billing_account(conn, user.id)
        snapshot = await billing_snapshot(conn, user.id)
    if snapshot["source"] == "subscription" and snapshot["plan_id"] == "pro":
        raise HTTPException(status_code=409, detail={"code": "already_subscribed", "detail": "Already on Pro"})
    previous = account.get("stripe_customer_id")
    try:
        intent = create_pro_subscription_intent(
            user_id=str(user.id),
            email=user.email,
            customer_id=previous,
        )
    except Exception:
        lg.exception("Stripe subscription intent create failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    await _persist_customer_id(user.id, previous=previous, customer_id=str(intent["customer_id"]))
    return SubscriptionIntentOut.model_validate(intent)


@router.post("/portal", response_model=PortalSessionOut)
async def billing_portal(user: PlatformUser = Depends(get_current_user)) -> PortalSessionOut:
    customer_id = await _require_customer_id(user)
    try:
        session = create_portal_session(customer_id=customer_id)
    except Exception as exc:
        await _drop_stale_stripe_customer(user.id, customer_id=customer_id, exc=exc)
        if is_unknown_customer_error(exc):
            raise HTTPException(
                status_code=404,
                detail={"code": "no_billing_customer", "detail": "No billing account"},
            )
        lg.exception("Stripe portal session create failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    return PortalSessionOut(url=str(session["url"]))


@router.get("/payment-methods", response_model=list[PaymentMethodOut])
async def billing_payment_methods(user: PlatformUser = Depends(get_current_user)) -> list[PaymentMethodOut]:
    customer_id = await _require_customer_id(user)
    try:
        rows = list_payment_methods(customer_id=customer_id)
    except Exception as exc:
        await _drop_stale_stripe_customer(user.id, customer_id=customer_id, exc=exc)
        if is_unknown_customer_error(exc):
            return []
        lg.exception("Stripe list payment methods failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    return [PaymentMethodOut.model_validate(row) for row in rows]


@router.post("/payment-methods/setup-intent", response_model=SetupIntentOut)
async def billing_payment_method_setup_intent(user: PlatformUser = Depends(get_current_user)) -> SetupIntentOut:
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_secret_key.strip() or not cfg.stripe_publishable_key.strip():
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    async with get_conn() as conn:
        account = await get_or_create_billing_account(conn, user.id)
    previous = account.get("stripe_customer_id")
    try:
        customer_id = ensure_stripe_customer(
            user_id=str(user.id),
            email=user.email,
            customer_id=previous,
        )
        await _persist_customer_id(user.id, previous=previous, customer_id=customer_id)
        intent = create_setup_intent(customer_id=customer_id)
    except Exception:
        lg.exception("Stripe setup intent create failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    return SetupIntentOut.model_validate(intent)


@router.post("/payment-methods/{payment_method_id}/default", status_code=204)
async def billing_payment_method_default(
    payment_method_id: str,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    customer_id = await _require_customer_id(user)
    try:
        set_default_payment_method(customer_id=customer_id, payment_method_id=payment_method_id)
    except PermissionError:
        raise HTTPException(status_code=404, detail={"code": "payment_method_not_found", "detail": "Not found"})
    except Exception as exc:
        await _drop_stale_stripe_customer(user.id, customer_id=customer_id, exc=exc)
        if is_unknown_customer_error(exc):
            raise HTTPException(
                status_code=404,
                detail={"code": "no_billing_customer", "detail": "No billing account"},
            )
        lg.exception("Stripe set default payment method failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})


@router.delete("/payment-methods/{payment_method_id}", status_code=204)
async def billing_payment_method_delete(
    payment_method_id: str,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    customer_id = await _require_customer_id(user)
    try:
        detach_payment_method(customer_id=customer_id, payment_method_id=payment_method_id)
    except PermissionError:
        raise HTTPException(status_code=404, detail={"code": "payment_method_not_found", "detail": "Not found"})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"code": "cannot_remove_default", "detail": str(exc)})
    except Exception as exc:
        await _drop_stale_stripe_customer(user.id, customer_id=customer_id, exc=exc)
        if is_unknown_customer_error(exc):
            raise HTTPException(
                status_code=404,
                detail={"code": "no_billing_customer", "detail": "No billing account"},
            )
        lg.exception("Stripe detach payment method failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})


@router.get("/invoices", response_model=list[InvoiceOut])
async def billing_invoices(user: PlatformUser = Depends(get_current_user)) -> list[InvoiceOut]:
    customer_id = await _require_customer_id(user)
    try:
        rows = list_invoices(customer_id=customer_id)
    except Exception as exc:
        await _drop_stale_stripe_customer(user.id, customer_id=customer_id, exc=exc)
        if is_unknown_customer_error(exc):
            return []
        lg.exception("Stripe list invoices failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    return [InvoiceOut.model_validate(row) for row in rows]


@router.get("/upcoming-invoice", response_model=UpcomingInvoiceOut | None)
async def billing_upcoming_invoice(
    user: PlatformUser = Depends(get_current_user),
) -> UpcomingInvoiceOut | None:
    customer_id = await _require_customer_id(user)
    async with get_conn() as conn:
        snapshot = await billing_snapshot(conn, user.id)
    subscription = snapshot.get("subscription") or {}
    # No renewal will be collected once cancel-at-period-end is set.
    if subscription and bool(subscription.get("cancel_at_period_end")):
        return None
    subscription_id = subscription.get("stripe_subscription_id")
    try:
        preview = upcoming_invoice(
            customer_id=customer_id,
            subscription_id=str(subscription_id) if subscription_id else None,
        )
    except Exception as exc:
        await _drop_stale_stripe_customer(user.id, customer_id=customer_id, exc=exc)
        if is_unknown_customer_error(exc):
            return None
        lg.exception("Stripe upcoming invoice failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    if preview is None:
        return None
    return UpcomingInvoiceOut.model_validate(preview)


@router.post("/subscription/cancel", response_model=BillingMeOut)
async def billing_subscription_cancel(
    body: CancelSubscriptionIn,
    user: PlatformUser = Depends(get_current_user),
) -> BillingMeOut:
    async with get_conn() as conn:
        snapshot = await billing_snapshot(conn, user.id)
        account = await get_billing_account(conn, user.id)
    subscription = snapshot.get("subscription")
    if not subscription or not account:
        raise HTTPException(status_code=404, detail={"code": "no_subscription", "detail": "No active subscription"})
    subscription_id = str(subscription["stripe_subscription_id"])
    try:
        sub = set_subscription_cancel_at_period_end(subscription_id=subscription_id, cancel=body.cancel)
        fields = _subscription_fields(sub)
        async with get_conn() as conn:
            await upsert_subscription(conn, billing_account_id=account["id"], **fields)
            refreshed = await billing_snapshot(conn, user.id)
            refreshed["api_credits"] = await api_credit_summary(conn, user.id, refreshed["plan_id"])
    except Exception:
        lg.exception("Stripe cancel subscription failed user=%s", user.id)
        raise HTTPException(status_code=503, detail={"code": "billing_unavailable", "detail": "Billing unavailable"})
    return _billing_me_out(refreshed)


def _subscription_fields(sub: dict[str, Any]) -> dict[str, Any]:
    price = ((sub.get("items") or {}).get("data") or [{}])[0].get("price") or {}
    price_id = price.get("id")
    return {
        "stripe_subscription_id": str(sub["id"]),
        "stripe_price_id": str(price_id) if price_id else None,
        "status": str(sub.get("status") or "incomplete"),
        "plan_id": stripe_plan_for_price(str(price_id) if price_id else None),
        "current_period_start": to_dt(sub.get("current_period_start")),
        "current_period_end": to_dt(sub.get("current_period_end")),
        "cancel_at_period_end": bool(sub.get("cancel_at_period_end")),
        "canceled_at": to_dt(sub.get("canceled_at")),
        "ended_at": to_dt(sub.get("ended_at")),
        "raw": sub,
    }


async def _upsert_subscription_from_payload(payload: dict[str, Any], *, fallback_user_id: str | None = None) -> None:
    customer_id = payload.get("customer")
    if not customer_id:
        return
    async with get_conn() as conn:
        account = await get_billing_account_by_customer(conn, str(customer_id))
        if account is None and fallback_user_id:
            account = await set_billing_customer_id(
                conn, user_id=UUID(fallback_user_id), stripe_customer_id=str(customer_id)
            )
        if account is None:
            return
        fields = _subscription_fields(payload)
        await upsert_subscription(conn, billing_account_id=account["id"], **fields)


async def _grant_prepaid_credits(
    payload: dict[str, Any],
    *,
    user_id: UUID,
    credits: int,
    external_id: str,
) -> None:
    customer_id = payload.get("customer")
    async with get_conn() as conn:
        if customer_id:
            await set_billing_customer_id(conn, user_id=user_id, stripe_customer_id=str(customer_id))
        granted = await grant_prepaid_topup(
            conn,
            user_id=user_id,
            credits=credits,
            stripe_checkout_session_id=external_id,
        )
    if granted:
        lg.info("Granted %s prepaid credits user=%s ref=%s", credits, user_id, external_id)


async def _grant_credit_topup(session: dict[str, Any]) -> None:
    # The caller only invokes this after it has decided the session is paid.
    parsed = parse_credit_topup(session, paid=True)
    if parsed is None:
        lg.warning("Ignoring credit top-up session id=%s", session.get("id"))
        return
    user_id, credits, session_id = parsed
    await _grant_prepaid_credits(session, user_id=user_id, credits=credits, external_id=session_id)


async def _grant_credit_payment(intent: dict[str, Any]) -> None:
    parsed = parse_credit_payment(intent, paid=intent.get("status") == "succeeded")
    if parsed is None:
        lg.warning("Ignoring credit payment id=%s", intent.get("id"))
        return
    user_id, credits, payment_id = parsed
    await _grant_prepaid_credits(intent, user_id=user_id, credits=credits, external_id=payment_id)


async def _process_stripe_event(event: dict[str, Any]) -> None:
    event_type = str(event.get("type") or "")
    obj = (event.get("data") or {}).get("object") or {}
    if event_type == "payment_intent.succeeded":
        metadata = obj.get("metadata") or {}
        if isinstance(metadata, dict) and metadata.get("kind") == "credit_topup":
            await _grant_credit_payment(obj)
            return
    if event_type in ("checkout.session.completed", "checkout.session.async_payment_succeeded"):
        metadata = obj.get("metadata") or {}
        if isinstance(metadata, dict) and metadata.get("kind") == "credit_topup":
            paid = event_type == "checkout.session.async_payment_succeeded" or obj.get("payment_status") == "paid"
            if paid:
                await _grant_credit_topup(obj)
            return
    if event_type == "checkout.session.completed":
        user_id = (obj.get("metadata") or {}).get("user_id") or obj.get("client_reference_id")
        customer_id = obj.get("customer")
        if user_id and customer_id:
            async with get_conn() as conn:
                await set_billing_customer_id(conn, user_id=UUID(str(user_id)), stripe_customer_id=str(customer_id))
        subscription_id = obj.get("subscription")
        if subscription_id:
            sub = retrieve_subscription(str(subscription_id))
            await _upsert_subscription_from_payload(sub, fallback_user_id=str(user_id) if user_id else None)
        return
    if event_type.startswith("customer.subscription."):
        await _upsert_subscription_from_payload(obj)
        return
    if event_type in ("invoice.paid", "invoice.payment_failed"):
        subscription_id = obj.get("subscription")
        if subscription_id:
            sub = retrieve_subscription(str(subscription_id))
            await _upsert_subscription_from_payload(sub)


@router.post("/stripe/webhook")
async def billing_stripe_webhook(request: Request) -> dict[str, bool]:
    body = await request.body()
    signature = request.headers.get("Stripe-Signature")
    try:
        event = construct_event(body, signature)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook signature")
    event_id = str(event.get("id") or "")
    event_type = str(event.get("type") or "unknown")
    if not event_id:
        raise HTTPException(status_code=400, detail="Missing event id")
    async with get_conn() as conn:
        is_new = await mark_stripe_event_received(conn, event_id=event_id, event_type=event_type, payload=event)
    if not is_new:
        return {"ok": True}
    error: str | None = None
    try:
        await _process_stripe_event(event)
    except Exception as exc:
        error = str(exc)[:500]
        lg.exception("Stripe webhook processing failed event=%s type=%s", event_id, event_type)
    async with get_conn() as conn:
        await mark_stripe_event_processed(conn, event_id=event_id, error=error)
    if error:
        raise HTTPException(status_code=500, detail="Webhook processing failed")
    return {"ok": True}
