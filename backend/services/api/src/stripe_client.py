from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from billing.api_credits import CREDIT_TOPUP_KIND, quote_topup_credits
from services.api.src.config import ConfigFactory
from utils.request_spans import timed_sync


def _stripe():
    try:
        import stripe as stripe_module
    except Exception as exc:
        raise RuntimeError("Stripe is not installed") from exc
    return stripe_module


class _StripeProxy:
    """Time each Stripe SDK call. Resource objects stay proxied until a method runs."""

    def __init__(self, inner: Any, path: str) -> None:
        self._inner = inner
        self._path = path

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)
        path = f"{self._path}.{name}" if self._path else name
        if not callable(target):
            return _StripeProxy(target, path)

        def call(*args: Any, **kwargs: Any) -> Any:
            with timed_sync("stripe", path):
                return target(*args, **kwargs)

        return call


def _client() -> _StripeProxy:
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_secret_key.strip():
        raise RuntimeError("Stripe is not configured")
    return _StripeProxy(_stripe().StripeClient(cfg.stripe_secret_key.strip()), "")


def to_dt(epoch_seconds: int | None) -> datetime | None:
    if not epoch_seconds:
        return None
    return datetime.fromtimestamp(int(epoch_seconds), tz=UTC)


def stripe_plan_for_price(price_id: str | None) -> str:
    cfg = ConfigFactory.get_config()
    if price_id and cfg.stripe_pro_price_id and price_id == cfg.stripe_pro_price_id:
        return "pro"
    return "pro"


def create_checkout_session(*, user_id: str, email: str | None, customer_id: str | None) -> dict[str, Any]:
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_pro_price_id.strip():
        raise RuntimeError("STRIPE_PRO_PRICE_ID is not configured")
    params: dict[str, Any] = {
        "mode": "subscription",
        "line_items": [{"price": cfg.stripe_pro_price_id.strip(), "quantity": 1}],
        "success_url": cfg.stripe_success_url,
        "cancel_url": cfg.stripe_cancel_url,
        "client_reference_id": user_id,
        "metadata": {"user_id": user_id, "plan_id": "pro"},
        "subscription_data": {"metadata": {"user_id": user_id, "plan_id": "pro"}},
        "allow_promotion_codes": True,
    }
    if customer_id:
        # Verify (or replace stale test-mode ids) before attaching to Checkout.
        params["customer"] = ensure_stripe_customer(
            user_id=user_id,
            email=email,
            customer_id=customer_id,
        )
    elif email:
        params["customer_email"] = email
    session = _client().checkout.sessions.create(params)
    return dict(session)


def create_credit_payment_intent(
    *,
    user_id: str,
    email: str | None,
    customer_id: str | None,
    amount_cents: int,
) -> dict[str, Any]:
    """Create a card PaymentIntent for an in-app credit top-up."""
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_publishable_key.strip():
        raise RuntimeError("STRIPE_PUBLISHABLE_KEY is not configured")
    credits = quote_topup_credits(amount_cents)
    if credits is None:
        raise ValueError(f"Top-up amount is outside the allowed range: {amount_cents}")
    resolved_customer_id = ensure_stripe_customer(
        user_id=user_id,
        email=email,
        customer_id=customer_id,
    )
    intent = dict(
        _client().payment_intents.create(
            {
                "amount": amount_cents,
                "currency": "usd",
                "customer": resolved_customer_id,
                "payment_method_types": ["card"],
                "description": f"Blackflare API credits ({credits})",
                "metadata": {
                    "user_id": user_id,
                    "kind": CREDIT_TOPUP_KIND,
                    "credits": str(credits),
                    "amount_cents": str(amount_cents),
                },
            }
        )
    )
    client_secret = intent.get("client_secret")
    if not client_secret:
        raise RuntimeError("Stripe PaymentIntent did not return a client secret")
    return {
        "publishable_key": cfg.stripe_publishable_key.strip(),
        "customer_id": resolved_customer_id,
        "payment_intent_id": str(intent["id"]),
        "client_secret": str(client_secret),
        "customer_session_client_secret": create_customer_session(customer_id=resolved_customer_id),
        "amount_due": amount_cents,
        "currency": "usd",
        "credits": credits,
    }


def retrieve_payment_intent(payment_intent_id: str) -> dict[str, Any]:
    return dict(_client().payment_intents.retrieve(payment_intent_id))


def create_portal_session(*, customer_id: str) -> dict[str, Any]:
    cfg = ConfigFactory.get_config()
    session = _client().billing_portal.sessions.create(
        {
            "customer": customer_id,
            "return_url": cfg.stripe_portal_return_url,
        }
    )
    return dict(session)


def is_unknown_customer_error(exc: BaseException) -> bool:
    """True when Stripe rejects a customer id as missing or wrong mode (test vs live)."""
    stripe = _stripe()
    err_cls = getattr(getattr(stripe, "error", None), "InvalidRequestError", None) or getattr(
        stripe, "InvalidRequestError", None
    )
    if err_cls is None or not isinstance(exc, err_cls):
        return False
    message = str(exc).lower()
    return (
        "no such customer" in message
        or "similar object exists in test mode" in message
        or "similar object exists in live mode" in message
    )


def ensure_stripe_customer(*, user_id: str, email: str | None, customer_id: str | None) -> str:
    if customer_id:
        try:
            _client().customers.retrieve(customer_id)
            return customer_id
        except Exception as exc:
            if not is_unknown_customer_error(exc):
                raise
            # Stale test-mode (or deleted) customer left in the DB — create a live one.
    params: dict[str, Any] = {"metadata": {"user_id": user_id}}
    if email:
        params["email"] = email
    customer = _client().customers.create(params)
    return str(customer["id"])


def _payment_client_secret(subscription: dict[str, Any]) -> str | None:
    invoice = subscription.get("latest_invoice")
    if not isinstance(invoice, dict):
        return None
    confirmation = invoice.get("confirmation_secret")
    if isinstance(confirmation, dict):
        secret = confirmation.get("client_secret")
        if secret:
            return str(secret)
    payment_intent = invoice.get("payment_intent")
    if isinstance(payment_intent, dict):
        secret = payment_intent.get("client_secret")
        if secret:
            return str(secret)
    if isinstance(payment_intent, str) and payment_intent:
        pi = _client().payment_intents.retrieve(payment_intent)
        secret = dict(pi).get("client_secret")
        if secret:
            return str(secret)
    return None


def create_customer_session(*, customer_id: str) -> str:
    session = dict(
        _client().customer_sessions.create(
            {
                "customer": customer_id,
                "components": {
                    "payment_element": {
                        "enabled": True,
                        "features": {
                            "payment_method_redisplay": "enabled",
                            "payment_method_save": "enabled",
                            "payment_method_save_usage": "off_session",
                            "payment_method_remove": "disabled",
                            "payment_method_allow_redisplay_filters": [
                                "always",
                                "limited",
                                "unspecified",
                            ],
                        },
                    }
                },
            }
        )
    )
    secret = session.get("client_secret")
    if not secret:
        raise RuntimeError("Stripe Customer Session did not return a client secret")
    return str(secret)


def create_pro_subscription_intent(
    *,
    user_id: str,
    email: str | None,
    customer_id: str | None,
) -> dict[str, Any]:
    """Create an incomplete Pro subscription and return Payment Element secrets."""
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_pro_price_id.strip():
        raise RuntimeError("STRIPE_PRO_PRICE_ID is not configured")
    if not cfg.stripe_publishable_key.strip():
        raise RuntimeError("STRIPE_PUBLISHABLE_KEY is not configured")

    resolved_customer_id = ensure_stripe_customer(
        user_id=user_id,
        email=email,
        customer_id=customer_id,
    )
    price = dict(_client().prices.retrieve(cfg.stripe_pro_price_id.strip()))
    subscription = dict(
        _client().subscriptions.create(
            {
                "customer": resolved_customer_id,
                "items": [{"price": cfg.stripe_pro_price_id.strip(), "quantity": 1}],
                "payment_behavior": "default_incomplete",
                "payment_settings": {
                    "save_default_payment_method": "on_subscription",
                    "payment_method_types": ["card"],
                },
                "metadata": {"user_id": user_id, "plan_id": "pro"},
                "expand": ["latest_invoice.confirmation_secret", "latest_invoice.payment_intent"],
            }
        )
    )
    client_secret = _payment_client_secret(subscription)
    if not client_secret:
        raise RuntimeError("Stripe subscription did not return a payment client secret")

    # Payment Element / Dashboard configs can still surface bank methods unless the
    # underlying PaymentIntent is locked to card.
    invoice = subscription.get("latest_invoice")
    if isinstance(invoice, dict):
        payment_intent = invoice.get("payment_intent")
        payment_intent_id: str | None = None
        if isinstance(payment_intent, dict):
            payment_intent_id = str(payment_intent["id"]) if payment_intent.get("id") else None
        elif isinstance(payment_intent, str) and payment_intent:
            payment_intent_id = payment_intent
        if payment_intent_id:
            _client().payment_intents.update(
                payment_intent_id,
                {"payment_method_types": ["card"]},
            )

    unit_amount = price.get("unit_amount")
    currency = str(price.get("currency") or "usd")
    invoice = subscription.get("latest_invoice")
    amount_due = None
    if isinstance(invoice, dict) and invoice.get("amount_due") is not None:
        amount_due = int(invoice["amount_due"])
    elif unit_amount is not None:
        amount_due = int(unit_amount)

    return {
        "publishable_key": cfg.stripe_publishable_key.strip(),
        "customer_id": resolved_customer_id,
        "subscription_id": str(subscription["id"]),
        "client_secret": client_secret,
        "customer_session_client_secret": create_customer_session(customer_id=resolved_customer_id),
        "amount_due": amount_due,
        "currency": currency,
        "price_id": cfg.stripe_pro_price_id.strip(),
    }


def construct_event(payload: bytes, signature: str | None) -> dict[str, Any]:
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_webhook_secret.strip():
        raise RuntimeError("STRIPE_WEBHOOK_SECRET is not configured")
    if not signature:
        raise ValueError("Missing stripe signature")
    event = _stripe().Webhook.construct_event(payload, signature, cfg.stripe_webhook_secret.strip())
    return dict(event)


def retrieve_subscription(subscription_id: str) -> dict[str, Any]:
    sub = _client().subscriptions.retrieve(subscription_id)
    return dict(sub)


def _customer_default_payment_method_id(customer_id: str) -> str | None:
    customer = dict(_client().customers.retrieve(customer_id))
    invoice_settings = customer.get("invoice_settings") or {}
    default_pm = invoice_settings.get("default_payment_method")
    if isinstance(default_pm, dict):
        return str(default_pm.get("id") or "") or None
    if isinstance(default_pm, str) and default_pm:
        return default_pm
    return None


def list_payment_methods(*, customer_id: str) -> list[dict[str, Any]]:
    default_id = _customer_default_payment_method_id(customer_id)
    result = _client().payment_methods.list({"customer": customer_id, "type": "card", "limit": 100})
    rows: list[dict[str, Any]] = []
    for item in list(getattr(result, "data", None) or result.get("data") or []):
        pm = dict(item)
        card = pm.get("card") if isinstance(pm.get("card"), dict) else {}
        rows.append(
            {
                "id": str(pm["id"]),
                "brand": str(card.get("brand") or "card"),
                "last4": str(card.get("last4") or "****"),
                "exp_month": int(card["exp_month"]) if card.get("exp_month") is not None else None,
                "exp_year": int(card["exp_year"]) if card.get("exp_year") is not None else None,
                "is_default": str(pm["id"]) == default_id,
            }
        )
    rows.sort(key=lambda row: (not row["is_default"], row["brand"], row["last4"]))
    return rows


def create_setup_intent(*, customer_id: str) -> dict[str, Any]:
    cfg = ConfigFactory.get_config()
    if not cfg.stripe_publishable_key.strip():
        raise RuntimeError("STRIPE_PUBLISHABLE_KEY is not configured")
    intent = dict(
        _client().setup_intents.create(
            {
                "customer": customer_id,
                "usage": "off_session",
                "payment_method_types": ["card"],
            }
        )
    )
    secret = intent.get("client_secret")
    if not secret:
        raise RuntimeError("Stripe SetupIntent did not return a client secret")
    return {
        "publishable_key": cfg.stripe_publishable_key.strip(),
        "client_secret": str(secret),
        "customer_session_client_secret": create_customer_session(customer_id=customer_id),
        "setup_intent_id": str(intent["id"]),
    }


def set_default_payment_method(*, customer_id: str, payment_method_id: str) -> None:
    pm = dict(_client().payment_methods.retrieve(payment_method_id))
    if str(pm.get("customer") or "") != customer_id:
        raise PermissionError("Payment method does not belong to this customer")
    _client().customers.update(
        customer_id,
        {"invoice_settings": {"default_payment_method": payment_method_id}},
    )
    # Keep active subscriptions on the same default card.
    subs = _client().subscriptions.list({"customer": customer_id, "status": "all", "limit": 20})
    for item in list(getattr(subs, "data", None) or subs.get("data") or []):
        sub = dict(item)
        status = str(sub.get("status") or "")
        if status in ("canceled", "incomplete_expired"):
            continue
        _client().subscriptions.update(
            str(sub["id"]),
            {"default_payment_method": payment_method_id},
        )


def detach_payment_method(*, customer_id: str, payment_method_id: str) -> None:
    pm = dict(_client().payment_methods.retrieve(payment_method_id))
    if str(pm.get("customer") or "") != customer_id:
        raise PermissionError("Payment method does not belong to this customer")
    default_id = _customer_default_payment_method_id(customer_id)
    if default_id == payment_method_id:
        raise ValueError("Cannot remove the default payment method")
    _client().payment_methods.detach(payment_method_id)


def _charge_receipt_url(charge: Any) -> str | None:
    if isinstance(charge, dict):
        url = charge.get("receipt_url")
        return str(url) if url else None
    if isinstance(charge, str) and charge.strip():
        try:
            loaded = dict(_client().charges.retrieve(charge))
        except Exception:
            return None
        url = loaded.get("receipt_url")
        return str(url) if url else None
    return None


def _invoice_receipt_url(inv: dict[str, Any]) -> str | None:
    return _charge_receipt_url(inv.get("charge"))


def _credit_purchase_rows(*, customer_id: str, limit: int) -> list[dict[str, Any]]:
    """Succeeded in-app credit purchases. These are PaymentIntents, not subscription invoices."""
    result = _client().payment_intents.list(
        {
            "customer": customer_id,
            "limit": min(max(limit, 1), 100),
            "expand": ["data.latest_charge"],
        }
    )
    rows: list[dict[str, Any]] = []
    for item in list(getattr(result, "data", None) or result.get("data") or []):
        intent = dict(item)
        metadata = intent.get("metadata") or {}
        if not isinstance(metadata, dict) or metadata.get("kind") != CREDIT_TOPUP_KIND:
            continue
        if str(intent.get("status") or "") != "succeeded":
            continue
        try:
            credits = int(str(metadata.get("credits") or ""))
            label = f"{credits:,} API credits"
        except ValueError:
            label = "API credits"
        amount = intent.get("amount_received")
        if amount is None:
            amount = intent.get("amount")
        paid = int(amount) if amount is not None else None
        rows.append(
            {
                "id": str(intent["id"]),
                "number": label,
                "status": "paid",
                "currency": str(intent.get("currency") or "usd"),
                "amount_due": paid,
                "amount_paid": paid,
                "created": to_dt(intent.get("created")),
                "hosted_invoice_url": None,
                "invoice_pdf": None,
                "receipt_url": _charge_receipt_url(intent.get("latest_charge")),
                "period_start": None,
                "period_end": None,
            }
        )
    return rows


def list_invoices(*, customer_id: str, limit: int = 100) -> list[dict[str, Any]]:
    page_limit = min(max(limit, 1), 100)
    result = _client().invoices.list(
        {
            "customer": customer_id,
            "limit": page_limit,
            "expand": ["data.charge"],
        }
    )
    rows: list[dict[str, Any]] = []
    for item in list(getattr(result, "data", None) or result.get("data") or []):
        inv = dict(item)
        rows.append(
            {
                "id": str(inv["id"]),
                "number": inv.get("number"),
                "status": str(inv.get("status") or "unknown"),
                "currency": str(inv.get("currency") or "usd"),
                "amount_due": int(inv["amount_due"]) if inv.get("amount_due") is not None else None,
                "amount_paid": int(inv["amount_paid"]) if inv.get("amount_paid") is not None else None,
                "created": to_dt(inv.get("created")),
                "hosted_invoice_url": inv.get("hosted_invoice_url"),
                "invoice_pdf": inv.get("invoice_pdf"),
                "receipt_url": _invoice_receipt_url(inv),
                "period_start": to_dt(inv.get("period_start")),
                "period_end": to_dt(inv.get("period_end")),
            }
        )
    rows.extend(_credit_purchase_rows(customer_id=customer_id, limit=page_limit))
    rows.sort(key=lambda row: row["created"] or datetime.min.replace(tzinfo=UTC), reverse=True)
    return rows


def upcoming_invoice(*, customer_id: str, subscription_id: str | None = None) -> dict[str, Any] | None:
    params: dict[str, Any] = {"customer": customer_id}
    if subscription_id:
        params["subscription"] = subscription_id
    try:
        inv = dict(_client().invoices.create_preview(params))
    except Exception:
        # No upcoming bill (free / canceled / nothing to preview).
        return None
    lines_out: list[dict[str, Any]] = []
    lines = inv.get("lines") or {}
    for item in list(getattr(lines, "data", None) or lines.get("data") or []):
        line = dict(item)
        lines_out.append(
            {
                "description": str(line.get("description") or "Item"),
                "amount": int(line["amount"]) if line.get("amount") is not None else 0,
                "currency": str(line.get("currency") or inv.get("currency") or "usd"),
            }
        )
    return {
        "currency": str(inv.get("currency") or "usd"),
        "amount_due": int(inv["amount_due"]) if inv.get("amount_due") is not None else 0,
        "period_start": to_dt(inv.get("period_start")),
        "period_end": to_dt(inv.get("period_end")),
        "next_payment_attempt": to_dt(inv.get("next_payment_attempt")),
        "lines": lines_out,
    }


def set_subscription_cancel_at_period_end(*, subscription_id: str, cancel: bool) -> dict[str, Any]:
    sub = dict(
        _client().subscriptions.update(
            subscription_id,
            {"cancel_at_period_end": cancel},
        )
    )
    return sub
