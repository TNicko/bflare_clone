"""API keys and credit-account spends. Callers hold the connection transaction."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row

from billing.api_credits import (
    CreditAccountState,
    SpendSplit,
    api_allowance_for,
    month_start,
    quote_spend,
    refund_split,
    roll_allowance_period,
)

MAX_ACTIVE_API_KEYS = 10
API_KEY_PREFIX = "bf_live_"


def hash_api_key(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def generate_api_key() -> tuple[str, str, str, str]:
    """Return secret, prefix, last4, and sha256 hex. The secret is shown once."""
    secret = f"{API_KEY_PREFIX}{secrets.token_urlsafe(32)}"
    return secret, secret[:12], secret[-4:], hash_api_key(secret)


def is_api_key_token(token: str | None) -> bool:
    return bool(token) and token.startswith(API_KEY_PREFIX)


async def count_active_api_keys(conn: AsyncConnection[DictRow], user_id: UUID) -> int:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM api_keys
            WHERE user_id = %s AND revoked_at IS NULL
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


async def insert_api_key(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    name: str,
    key_prefix: str,
    key_last4: str,
    key_hash: str,
) -> dict[str, Any]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO api_keys (user_id, name, key_prefix, key_last4, key_hash)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, name, key_prefix, key_last4, created_at, last_used_at
            """,
            (user_id, name, key_prefix, key_last4, key_hash),
        )
        row = await cur.fetchone()
    if row is None:
        raise RuntimeError("api key insert returned no row")
    return dict(row)


async def list_active_api_keys(conn: AsyncConnection[DictRow], user_id: UUID) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, name, key_prefix, key_last4, created_at, last_used_at
            FROM api_keys
            WHERE user_id = %s AND revoked_at IS NULL
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        rows = await cur.fetchall()
    return [dict(row) for row in rows]


async def revoke_api_key(conn: AsyncConnection[DictRow], *, user_id: UUID, key_id: UUID) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE api_keys
            SET revoked_at = now()
            WHERE id = %s AND user_id = %s AND revoked_at IS NULL
            """,
            (key_id, user_id),
        )
        return cur.rowcount > 0


async def resolve_api_key_user(conn: AsyncConnection[DictRow], secret: str) -> tuple[UUID, str | None] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE api_keys AS k
            SET last_used_at = now()
            FROM auth.users AS u
            WHERE k.user_id = u.id
              AND k.key_hash = %s
              AND k.revoked_at IS NULL
            RETURNING k.user_id, u.email
            """,
            (hash_api_key(secret),),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    email = row["email"]
    return row["user_id"], str(email) if email else None


async def api_credit_summary(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
    plan_id: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    moment = now or datetime.now(UTC)
    period = month_start(moment)
    allowance = api_allowance_for(plan_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT prepaid_credits, allowance_used, allowance_period_start
            FROM api_credit_accounts
            WHERE user_id = %s
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    used = 0
    prepaid = 0
    if row is not None and row["allowance_period_start"] == period:
        used = int(row["allowance_used"])
    if row is not None:
        prepaid = int(row["prepaid_credits"])
    remaining = None if allowance is None else max(0, allowance - used)
    spent = await _credits_spent_this_period(conn, user_id, period)
    return {
        "allowance": allowance,
        "allowance_used": used,
        "allowance_remaining": remaining,
        "prepaid_credits": prepaid,
        "spent_credits": max(used, spent),
        "period_start": period,
    }


def _next_month(period: date) -> date:
    if period.month == 12:
        return date(period.year + 1, 1, 1)
    return date(period.year, period.month + 1, 1)


async def _credits_spent_this_period(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
    period: date,
) -> int:
    """Net credits spent in the UTC month, including purchased credits."""
    start = datetime(period.year, period.month, period.day, tzinfo=UTC)
    end_day = _next_month(period)
    end = datetime(end_day.year, end_day.month, end_day.day, tzinfo=UTC)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT COALESCE(SUM(
              CASE kind
                WHEN 'spend' THEN allowance_credits + prepaid_credits
                WHEN 'refund' THEN -(allowance_credits + prepaid_credits)
                ELSE 0
              END
            ), 0) AS n
            FROM api_credit_ledger
            WHERE user_id = %s
              AND (
                (kind = 'spend' AND period_start = %s)
                OR (kind = 'refund' AND created_at >= %s AND created_at < %s)
              )
            """,
            (user_id, period, start, end),
        )
        row = await cur.fetchone()
    if row is None or row["n"] is None:
        return 0
    return max(0, int(row["n"]))


async def _lock_account(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
    moment: datetime,
) -> CreditAccountState:
    period = month_start(moment)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO api_credit_accounts (user_id, allowance_period_start)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user_id, period),
        )
        await cur.execute(
            """
            SELECT prepaid_credits, allowance_used, allowance_period_start
            FROM api_credit_accounts
            WHERE user_id = %s
            FOR UPDATE
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise RuntimeError("credit account missing after insert")
    state = CreditAccountState(
        prepaid_credits=int(row["prepaid_credits"]),
        allowance_used=int(row["allowance_used"]),
        allowance_period_start=row["allowance_period_start"],
    )
    rolled = roll_allowance_period(state, moment)
    if rolled != state:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE api_credit_accounts
                SET allowance_used = 0,
                    allowance_period_start = %s,
                    updated_at = now()
                WHERE user_id = %s
                """,
                (rolled.allowance_period_start, user_id),
            )
    return rolled


async def _existing_spend(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    request_id: str,
) -> SpendSplit | None:
    """Return a prior spend for this user+request_id, if any.

    Idempotency is scoped per ``user_id`` so one client's ``X-Request-Id``
    cannot skip charges (or inflate refunds) for another account.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT allowance_credits, prepaid_credits
            FROM api_credit_ledger
            WHERE user_id = %s AND request_id = %s AND kind = 'spend'
            """,
            (user_id, request_id),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return SpendSplit(allowance=int(row["allowance_credits"]), prepaid=int(row["prepaid_credits"]))


async def spend_api_credits(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    plan_id: str,
    cost: int,
    request_id: str,
    now: datetime | None = None,
) -> SpendSplit | None:
    """Lock the account and spend. None means the balance cannot cover ``cost``.

    A repeated ``request_id`` for the same user returns the original split and
    does not charge again.
    """
    moment = now or datetime.now(UTC)
    existing = await _existing_spend(conn, user_id=user_id, request_id=request_id)
    if existing is not None:
        return existing
    state = await _lock_account(conn, user_id, moment)
    split = quote_spend(state, allowance=api_allowance_for(plan_id), cost=cost)
    if split is None:
        return None
    if split.total == 0:
        return split
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE api_credit_accounts
            SET allowance_used = allowance_used + %s,
                prepaid_credits = prepaid_credits - %s,
                updated_at = now()
            WHERE user_id = %s
            """,
            (split.allowance, split.prepaid, user_id),
        )
        await cur.execute(
            """
            INSERT INTO api_credit_ledger (
              user_id, kind, credits, allowance_credits, prepaid_credits, period_start, request_id
            )
            VALUES (%s, 'spend', %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                split.total,
                split.allowance,
                split.prepaid,
                month_start(moment),
                request_id,
            ),
        )
    return split


async def refund_api_credits(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    charged: SpendSplit,
    refund: int,
    request_id: str,
) -> SpendSplit:
    """Return prepaid first, then allowance. Returns the split that stays spent."""
    if refund == 0:
        return charged
    kept = refund_split(charged, refund)
    allowance_back = charged.allowance - kept.allowance
    prepaid_back = charged.prepaid - kept.prepaid
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE api_credit_accounts
            SET allowance_used = GREATEST(0, allowance_used - %s),
                prepaid_credits = prepaid_credits + %s,
                updated_at = now()
            WHERE user_id = %s
            """,
            (allowance_back, prepaid_back, user_id),
        )
        await cur.execute(
            """
            INSERT INTO api_credit_ledger (
              user_id, kind, credits, allowance_credits, prepaid_credits, request_id
            )
            VALUES (%s, 'refund', %s, %s, %s, %s)
            """,
            (user_id, refund, allowance_back, prepaid_back, f"{request_id}:refund"),
        )
    return kept


async def grant_prepaid_topup(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    credits: int,
    stripe_checkout_session_id: str,
    now: datetime | None = None,
) -> bool:
    """Add prepaid credits once for a Checkout session. False when already granted."""
    if credits <= 0:
        raise ValueError("credits must be > 0")
    moment = now or datetime.now(UTC)
    async with conn.transaction():
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO api_credit_accounts (user_id, allowance_period_start)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id, month_start(moment)),
            )
            await cur.execute(
                """
                INSERT INTO api_credit_ledger (
                  user_id, kind, credits, allowance_credits, prepaid_credits,
                  stripe_checkout_session_id
                )
                VALUES (%s, 'topup', %s, 0, %s, %s)
                ON CONFLICT (stripe_checkout_session_id)
                  WHERE stripe_checkout_session_id IS NOT NULL
                DO NOTHING
                RETURNING id
                """,
                (user_id, credits, credits, stripe_checkout_session_id),
            )
            inserted = await cur.fetchone()
            if inserted is None:
                return False
            await cur.execute(
                """
                UPDATE api_credit_accounts
                SET prepaid_credits = prepaid_credits + %s,
                    updated_at = now()
                WHERE user_id = %s
                """,
                (credits, user_id),
            )
    return True


async def claim_usage_notice(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    period_start: date,
    kind: str,
) -> bool:
    """True when this month's notice was newly claimed."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO api_usage_notices (user_id, period_start, kind)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, period_start, kind) DO NOTHING
            RETURNING kind
            """,
            (user_id, period_start, kind),
        )
        return await cur.fetchone() is not None


async def release_usage_notice(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    period_start: date,
    kind: str,
) -> None:
    """Drop a claim when the email send failed so a later call can retry."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM api_usage_notices
            WHERE user_id = %s AND period_start = %s AND kind = %s
            """,
            (user_id, period_start, kind),
        )
