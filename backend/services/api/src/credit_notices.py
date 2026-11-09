"""Usage banners' email twin. One Postmark message per kind per UTC month.

A send failure releases the claim so a later API call can retry. It never
fails the search that scheduled it.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

import httpx

from billing.api_credits import (
    NOTICE_ALLOWANCE_80,
    NOTICE_ALLOWANCE_EXHAUSTED,
    NOTICE_PREPAID_EMPTY,
    credit_notice_kind,
)
from db.crud.user.api_access import api_credit_summary, claim_usage_notice, release_usage_notice
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase import get_conn

lg = logging.getLogger(__name__)

_SUBJECTS = {
    NOTICE_ALLOWANCE_80: "You've used 80% of your Blackflare API allowance",
    NOTICE_ALLOWANCE_EXHAUSTED: "Your Blackflare API allowance is used up",
    NOTICE_PREPAID_EMPTY: "Your Blackflare API credits are used up",
}


def _body(kind: str, summary: dict[str, object]) -> str:
    used = summary["allowance_used"]
    allowance = summary["allowance"]
    prepaid = summary["prepaid_credits"]
    if kind == NOTICE_ALLOWANCE_80:
        return (
            f"You've used {used} of {allowance} included API credits this month. "
            "Included credits reset at the start of the next UTC month and do not roll over. "
            f"Purchased credits: {prepaid}."
        )
    if kind == NOTICE_ALLOWANCE_EXHAUSTED:
        return f"This month's included API credits are used up. Calls now spend your purchased credits ({prepaid} credits)."
    return (
        "This month's included API credits are used up and you have no purchased credits left. "
        "API calls stop until the next UTC month or you add credits."
    )


async def _send_postmark(*, to: str, kind: str, summary: dict[str, object]) -> None:
    cfg = ConfigFactory.get_config()
    token = cfg.postmark_server_token.strip()
    sender = cfg.postmark_from.strip()
    if not token or not sender:
        raise RuntimeError("Postmark is not configured")
    payload = {
        "From": sender,
        "To": to,
        "Subject": _SUBJECTS[kind],
        "TextBody": _body(kind, summary),
        "MessageStream": cfg.postmark_message_stream.strip() or "outbound",
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(
            "https://api.postmarkapp.com/email",
            json=payload,
            headers={
                "X-Postmark-Server-Token": token,
                "Accept": "application/json",
            },
        )
        response.raise_for_status()


async def _notify(user_id: UUID, email: str, plan_id: str) -> None:
    cfg = ConfigFactory.get_config()
    if not cfg.postmark_server_token.strip() or not cfg.postmark_from.strip():
        return
    async with get_conn() as conn:
        summary = await api_credit_summary(conn, user_id, plan_id)
    kind = credit_notice_kind(
        allowance=summary["allowance"],
        allowance_used=int(summary["allowance_used"]),
        prepaid_credits=int(summary["prepaid_credits"]),
    )
    if kind is None:
        return
    async with get_conn() as conn:
        claimed = await claim_usage_notice(
            conn,
            user_id=user_id,
            period_start=summary["period_start"],
            kind=kind,
        )
    if not claimed:
        return
    try:
        await _send_postmark(to=email, kind=kind, summary=summary)
    except Exception:
        lg.exception("API usage notice failed user=%s kind=%s", user_id, kind)
        async with get_conn() as conn:
            await release_usage_notice(
                conn,
                user_id=user_id,
                period_start=summary["period_start"],
                kind=kind,
            )


def schedule_credit_notices(*, user_id: UUID, email: str | None, plan_id: str) -> None:
    """Send after the response. No-op when Postmark or the address is missing."""
    if not email or not email.strip():
        return
    task = asyncio.create_task(_notify(user_id, email.strip(), plan_id), name="api-credit-notice")

    def _log(done: asyncio.Task[None]) -> None:
        if done.cancelled():
            return
        exc = done.exception()
        if exc is not None:
            lg.error("API usage notice task failed", exc_info=exc)

    task.add_done_callback(_log)
