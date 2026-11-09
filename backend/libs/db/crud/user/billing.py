"""CRUD for plans, overrides, Stripe accounts, and subscriptions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row

from billing.catalog import PLAN_INTERNAL, PLAN_PRO, QUOTA_BUCKETS, plan_limits_for
from billing.entitlements import effective_plan, meter_entitlements
from db.crud.user.usage import count_daily_meter_events, utc_day_bounds, utc_month_bounds

OVERRIDE_COLUMNS = """
  id,
  user_id,
  plan_id,
  kind,
  starts_at,
  expires_at,
  granted_by,
  reason,
  revoked_at,
  revoked_by,
  created_at
"""

SUBSCRIPTION_COLUMNS = """
  id,
  billing_account_id,
  stripe_subscription_id,
  stripe_price_id,
  status,
  plan_id,
  current_period_start,
  current_period_end,
  cancel_at_period_end,
  canceled_at,
  ended_at,
  raw,
  created_at,
  updated_at
"""


def _qualify_columns(columns: str, alias: str) -> str:
    return ",\n  ".join(f"{alias}.{part.strip()}" for part in columns.split(",") if part.strip())


async def get_plan(conn: AsyncConnection[DictRow], plan_id: str) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, name, included_units, features, past_due_grace_days
            FROM plans
            WHERE id = %s
            """,
            (plan_id,),
        )
        return await cur.fetchone()


async def find_user(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID | None = None,
    email: str | None = None,
) -> dict[str, Any] | None:
    if user_id is None and not email:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        if user_id is not None:
            await cur.execute(
                """
                SELECT id, email
                FROM auth.users
                WHERE id = %s
                """,
                (user_id,),
            )
        else:
            await cur.execute(
                """
                SELECT id, email
                FROM auth.users
                WHERE lower(email) = lower(%s)
                """,
                (email.strip(),),
            )
        return await cur.fetchone()


async def get_billing_account(conn: AsyncConnection[DictRow], user_id: UUID) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, user_id, stripe_customer_id, created_at, updated_at
            FROM billing_accounts
            WHERE user_id = %s
            """,
            (user_id,),
        )
        return await cur.fetchone()


async def get_or_create_billing_account(conn: AsyncConnection[DictRow], user_id: UUID) -> dict[str, Any]:
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO billing_accounts (user_id)
                VALUES (%s)
                ON CONFLICT (user_id) DO UPDATE SET updated_at = now()
                RETURNING id, user_id, stripe_customer_id, created_at, updated_at
                """,
                (user_id,),
            )
            row = await cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to upsert billing account")
    return row


async def set_billing_customer_id(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    stripe_customer_id: str,
) -> dict[str, Any]:
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO billing_accounts (user_id, stripe_customer_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET stripe_customer_id = EXCLUDED.stripe_customer_id, updated_at = now()
                RETURNING id, user_id, stripe_customer_id, created_at, updated_at
                """,
                (user_id, stripe_customer_id),
            )
            row = await cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to set billing customer id")
    return row


async def clear_billing_stripe_link(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
) -> dict[str, Any] | None:
    """Drop a stale Stripe customer id and end open subscriptions for the account.

    Used when prod holds a test-mode (or otherwise missing) customer id after a
    Stripe key/mode switch.
    """
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE billing_accounts
                SET stripe_customer_id = NULL, updated_at = now()
                WHERE user_id = %s
                RETURNING id, user_id, stripe_customer_id, created_at, updated_at
                """,
                (user_id,),
            )
            account = await cur.fetchone()
            if account is None:
                return None
            await cur.execute(
                """
                UPDATE subscriptions
                SET
                  status = CASE WHEN ended_at IS NULL THEN 'canceled' ELSE status END,
                  ended_at = COALESCE(ended_at, now()),
                  canceled_at = COALESCE(canceled_at, now()),
                  updated_at = now()
                WHERE billing_account_id = %s
                  AND ended_at IS NULL
                """,
                (account["id"],),
            )
    return account


async def list_billing_accounts_with_customers(
    conn: AsyncConnection[DictRow],
) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, user_id, stripe_customer_id, created_at, updated_at
            FROM billing_accounts
            WHERE stripe_customer_id IS NOT NULL
            ORDER BY updated_at DESC
            """
        )
        return list(await cur.fetchall())


async def get_billing_account_by_customer(
    conn: AsyncConnection[DictRow], stripe_customer_id: str
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, user_id, stripe_customer_id, created_at, updated_at
            FROM billing_accounts
            WHERE stripe_customer_id = %s
            """,
            (stripe_customer_id,),
        )
        return await cur.fetchone()


async def get_current_override(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {OVERRIDE_COLUMNS}
            FROM billing_overrides
            WHERE user_id = %s
              AND revoked_at IS NULL
            """,
            (user_id,),
        )
        return await cur.fetchone()


async def list_override_history(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {OVERRIDE_COLUMNS}
            FROM billing_overrides
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (user_id, limit),
        )
        return list(await cur.fetchall())


async def get_current_subscription(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_qualify_columns(SUBSCRIPTION_COLUMNS, "s")}
            FROM subscriptions s
            JOIN billing_accounts a ON a.id = s.billing_account_id
            WHERE a.user_id = %s
              AND s.ended_at IS NULL
            ORDER BY s.updated_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        return await cur.fetchone()


async def upsert_subscription(
    conn: AsyncConnection[DictRow],
    *,
    billing_account_id: UUID,
    stripe_subscription_id: str,
    stripe_price_id: str | None,
    status: str,
    plan_id: str,
    current_period_start: datetime | None,
    current_period_end: datetime | None,
    cancel_at_period_end: bool,
    canceled_at: datetime | None,
    ended_at: datetime | None,
    raw: dict[str, Any],
) -> dict[str, Any]:
    # Stripe fans out many events at once (checkout + subscription + invoice).
    # Serialize per billing account so concurrent upserts cannot both insert a
    # current row and trip subscriptions_one_current_per_account_idx.
    terminal = status in {"canceled", "unpaid", "incomplete_expired"} or ended_at is not None
    effective_ended_at = ended_at if ended_at is not None else (datetime.now(UTC) if terminal else None)
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id
                FROM billing_accounts
                WHERE id = %s
                FOR UPDATE
                """,
                (billing_account_id,),
            )
            if await cur.fetchone() is None:
                raise RuntimeError(f"billing account not found: {billing_account_id}")
            if effective_ended_at is None:
                await cur.execute(
                    """
                    UPDATE subscriptions
                    SET ended_at = now(), updated_at = now()
                    WHERE billing_account_id = %s
                      AND stripe_subscription_id <> %s
                      AND ended_at IS NULL
                    """,
                    (billing_account_id, stripe_subscription_id),
                )
            await cur.execute(
                f"""
                INSERT INTO subscriptions (
                  billing_account_id,
                  stripe_subscription_id,
                  stripe_price_id,
                  status,
                  plan_id,
                  current_period_start,
                  current_period_end,
                  cancel_at_period_end,
                  canceled_at,
                  ended_at,
                  raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (stripe_subscription_id)
                DO UPDATE SET
                  billing_account_id = EXCLUDED.billing_account_id,
                  stripe_price_id = EXCLUDED.stripe_price_id,
                  status = EXCLUDED.status,
                  plan_id = EXCLUDED.plan_id,
                  current_period_start = EXCLUDED.current_period_start,
                  current_period_end = EXCLUDED.current_period_end,
                  cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                  canceled_at = EXCLUDED.canceled_at,
                  ended_at = EXCLUDED.ended_at,
                  raw = EXCLUDED.raw,
                  updated_at = now()
                RETURNING {SUBSCRIPTION_COLUMNS}
                """,
                (
                    billing_account_id,
                    stripe_subscription_id,
                    stripe_price_id,
                    status,
                    plan_id,
                    current_period_start,
                    current_period_end,
                    cancel_at_period_end,
                    canceled_at,
                    effective_ended_at,
                    _json(raw),
                ),
            )
            row = await cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to upsert subscription")
    return row


def _json(value: Any) -> str:
    import json

    return json.dumps(value, default=str, separators=(",", ":"))


async def mark_stripe_event_received(
    conn: AsyncConnection[DictRow],
    *,
    event_id: str,
    event_type: str,
    payload: dict[str, Any],
) -> bool:
    """Insert Stripe event idempotently. Returns True if this is new."""
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO stripe_events (id, type, payload)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                """,
                (event_id, event_type, _json(payload)),
            )
            row = await cur.fetchone()
    return row is not None


async def mark_stripe_event_processed(
    conn: AsyncConnection[DictRow],
    *,
    event_id: str,
    error: str | None = None,
) -> None:
    async with conn.transaction():
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE stripe_events
                SET processed_at = now(), error = %s
                WHERE id = %s
                """,
                (error, event_id),
            )


async def replace_current_override(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    plan_id: str,
    kind: str,
    expires_at: datetime | None,
    granted_by: str,
    reason: str | None,
) -> dict[str, Any]:
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE billing_overrides
                SET revoked_at = now(), revoked_by = %s
                WHERE user_id = %s
                  AND revoked_at IS NULL
                """,
                (granted_by, user_id),
            )
            await cur.execute(
                f"""
                INSERT INTO billing_overrides (
                  user_id,
                  plan_id,
                  kind,
                  expires_at,
                  granted_by,
                  reason
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING {OVERRIDE_COLUMNS}
                """,
                (user_id, plan_id, kind, expires_at, granted_by, reason),
            )
            row = await cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to insert billing override")
    return row


async def revoke_current_override(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    revoked_by: str,
) -> dict[str, Any] | None:
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE billing_overrides
                SET revoked_at = now(), revoked_by = %s
                WHERE user_id = %s
                  AND revoked_at IS NULL
                RETURNING {OVERRIDE_COLUMNS}
                """,
                (revoked_by, user_id),
            )
            return await cur.fetchone()


async def assign_internal(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    granted_by: str,
    reason: str | None,
) -> dict[str, Any]:
    return await replace_current_override(
        conn,
        user_id=user_id,
        plan_id=PLAN_INTERNAL,
        kind="comp",
        expires_at=None,
        granted_by=granted_by,
        reason=reason,
    )


async def assign_pro(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    granted_by: str,
    reason: str | None,
) -> dict[str, Any]:
    return await replace_current_override(
        conn,
        user_id=user_id,
        plan_id=PLAN_PRO,
        kind="comp",
        expires_at=None,
        granted_by=granted_by,
        reason=reason,
    )


async def grant_pro_trial(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    days: int,
    granted_by: str,
    reason: str | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    start = now or datetime.now(UTC)
    return await replace_current_override(
        conn,
        user_id=user_id,
        plan_id=PLAN_PRO,
        kind="trial",
        expires_at=start + timedelta(days=days),
        granted_by=granted_by,
        reason=reason,
    )


async def stamp_internal_overrides(
    conn: AsyncConnection[DictRow],
    *,
    granted_by: str = "system",
    reason: str = "invite cohort before open signup",
) -> int:
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO billing_overrides (user_id, plan_id, kind, granted_by, reason)
                SELECT u.id, %s, 'comp', %s, %s
                FROM auth.users u
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM billing_overrides o
                  WHERE o.user_id = u.id
                    AND o.revoked_at IS NULL
                )
                RETURNING id
                """,
                (PLAN_INTERNAL, granted_by, reason),
            )
            rows = await cur.fetchall()
    return len(rows)


async def billing_snapshot(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    moment = now or datetime.now(UTC)
    override = await get_current_override(conn, user_id)
    subscription = await get_current_subscription(conn, user_id)
    plan_id, source = effective_plan(override, subscription, moment)
    plan = await get_plan(conn, plan_id)
    features = plan["features"] if plan else {}
    included = plan["included_units"] if plan else {}
    _, day_end = utc_day_bounds(moment)
    quotas: list[dict[str, Any]] = []
    for bucket in QUOTA_BUCKETS:
        cap = None
        if isinstance(included, dict) and bucket.key in included and included[bucket.key] is not None:
            cap = int(included[bucket.key])
        used = 0
        if cap is not None:
            used = await count_daily_meter_events(
                conn,
                user_id=user_id,
                meters=bucket.meters,
                moment=moment,
            )
        quotas.append(
            {
                "key": bucket.key,
                "used": used,
                "included": cap,
                "resets_at": day_end,
            }
        )
    try:
        limits = plan_limits_for(plan_id)
    except ValueError:
        limits = plan_limits_for("free")

    snapshot_core = {
        "plan_id": plan_id,
        "source": source,
        "override": override,
        "subscription": subscription,
        "meters": meter_entitlements(features, included),
        "quotas": quotas,
        "limits": limits,
    }

    if source in ("subscription", "past_due_grace") and isinstance(subscription, dict):
        period_start = subscription.get("current_period_start")
        period_end = subscription.get("current_period_end")
        if isinstance(period_start, datetime) and isinstance(period_end, datetime):
            return {
                **snapshot_core,
                "period_start": period_start,
                "period_end": period_end,
            }

    period_start, period_end = utc_month_bounds(moment)
    return {
        **snapshot_core,
        "period_start": period_start,
        "period_end": period_end,
    }
