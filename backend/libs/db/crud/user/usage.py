"""CRUD for the per-user usage ledger."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row

STALE_USAGE_AFTER_SECONDS = 30 * 60

INSERT_USAGE_EVENT_SQL = """
INSERT INTO usage_events (
  user_id,
  request_id,
  meter,
  route,
  method,
  status,
  query_text,
  query_hash,
  params,
  units
)
VALUES (%s, %s, %s, %s, %s, 'started', %s, %s, %s::jsonb, '{}'::jsonb)
ON CONFLICT (user_id, request_id) DO NOTHING
RETURNING id, status
"""

FETCH_USAGE_EVENT_BY_REQUEST_SQL = """
SELECT id, status
FROM usage_events
WHERE user_id = %s AND request_id = %s
"""

COMPLETE_USAGE_EVENT_SQL = """
UPDATE usage_events
SET
  status = %s,
  completed_at = now(),
  http_status = %s,
  query_text = %s,
  query_hash = %s,
  params = %s::jsonb,
  result_count = %s,
  duration_ms = %s,
  units = %s::jsonb,
  error_code = %s
WHERE id = %s
  AND status = 'started'
RETURNING id, user_id, meter
"""

INCREMENT_USAGE_PERIOD_SQL = """
INSERT INTO usage_periods (
  user_id,
  meter,
  period_start,
  period_end,
  event_count,
  units
)
VALUES (%s, %s, %s, %s, 1, %s::jsonb)
ON CONFLICT (user_id, meter, period_start)
DO UPDATE SET
  event_count = usage_periods.event_count + 1,
  units = (
    SELECT COALESCE(jsonb_object_agg(key, to_jsonb(val)), '{}'::jsonb)
    FROM (
      SELECT key, SUM(value::numeric) AS val
      FROM (
        SELECT key, value
        FROM jsonb_each_text(usage_periods.units)
        UNION ALL
        SELECT key, value
        FROM jsonb_each_text(%s::jsonb)
      ) parts
      GROUP BY key
    ) summed
  ),
  updated_at = now()
"""

SWEEP_STALE_USAGE_EVENTS_SQL = """
UPDATE usage_events
SET
  status = 'failed',
  completed_at = now(),
  http_status = COALESCE(http_status, 500),
  error_code = 'abandoned',
  units = CASE
    WHEN units = '{}'::jsonb THEN '{"searches": 1}'::jsonb
    ELSE units
  END,
  duration_ms = GREATEST(
    0,
    (EXTRACT(EPOCH FROM (now() - occurred_at)) * 1000)::integer
  )
WHERE status = 'started'
  AND occurred_at < now() - make_interval(secs => %s)
RETURNING id, user_id, meter, units, occurred_at
"""


def utc_month_bounds(moment: datetime | None = None) -> tuple[datetime, datetime]:
    now = (moment or datetime.now(UTC)).astimezone(UTC)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def utc_day_bounds(moment: datetime | None = None) -> tuple[datetime, datetime]:
    now = (moment or datetime.now(UTC)).astimezone(UTC)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def _json(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


async def insert_started_usage_event(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    request_id: str,
    meter: str,
    route: str,
    method: str,
    query_text: str | None,
    query_hash: str | None,
    params: dict[str, Any],
) -> tuple[UUID | None, str | None]:
    """Insert a started event. Returns (id, status) of the row we should complete.

    On ``(user_id, request_id)`` conflict, returns the existing row so a retry
    does not create a second event (and cannot attach to another user's event).
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            INSERT_USAGE_EVENT_SQL,
            (
                user_id,
                request_id,
                meter,
                route,
                method,
                query_text,
                query_hash,
                _json(params),
            ),
        )
        row = await cur.fetchone()
        if row is None:
            await cur.execute(FETCH_USAGE_EVENT_BY_REQUEST_SQL, (user_id, request_id))
            row = await cur.fetchone()
    if row is None:
        return None, None
    return row["id"], row["status"]


DISCARD_STARTED_USAGE_EVENT_SQL = """
DELETE FROM usage_events
WHERE id = %s
  AND status = 'started'
"""


async def discard_started_usage_event(conn: AsyncConnection[DictRow], event_id: UUID) -> None:
    """Drop a started row that must not count (cache hit, no external search)."""
    async with conn.cursor() as cur:
        await cur.execute(DISCARD_STARTED_USAGE_EVENT_SQL, (event_id,))


async def complete_usage_event(
    conn: AsyncConnection[DictRow],
    *,
    event_id: UUID,
    status: str,
    http_status: int | None,
    query_text: str | None,
    query_hash: str | None,
    params: dict[str, Any],
    result_count: int | None,
    duration_ms: int,
    units: dict[str, int | float],
    error_code: str | None,
) -> bool:
    """Complete a started event and roll it into the calendar-month period."""
    period_start, period_end = utc_month_bounds()
    payload = _json(units)
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                COMPLETE_USAGE_EVENT_SQL,
                (
                    status,
                    http_status,
                    query_text,
                    query_hash,
                    _json(params),
                    result_count,
                    duration_ms,
                    payload,
                    error_code,
                    event_id,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                return False
            await cur.execute(
                INCREMENT_USAGE_PERIOD_SQL,
                (
                    row["user_id"],
                    row["meter"],
                    period_start,
                    period_end,
                    payload,
                    payload,
                ),
            )
    return True


async def sweep_stale_usage_events(
    conn: AsyncConnection[DictRow],
    *,
    older_than_seconds: int = STALE_USAGE_AFTER_SECONDS,
) -> int:
    """Mark abandoned started events failed and count them in the period rollup."""
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(SWEEP_STALE_USAGE_EVENTS_SQL, (older_than_seconds,))
            rows = await cur.fetchall()
            for row in rows:
                occurred_at = row["occurred_at"]
                if isinstance(occurred_at, datetime):
                    period_start, period_end = utc_month_bounds(occurred_at)
                else:
                    period_start, period_end = utc_month_bounds()
                units = row["units"] if isinstance(row["units"], dict) else {"searches": 1}
                payload = _json(units)
                await cur.execute(
                    INCREMENT_USAGE_PERIOD_SQL,
                    (
                        row["user_id"],
                        row["meter"],
                        period_start,
                        period_end,
                        payload,
                        payload,
                    ),
                )
    return len(rows)


async def list_user_usage_overview(
    conn: AsyncConnection[DictRow],
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """All users with usage totals, newest activity first. Unused accounts last."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            WITH totals AS (
              SELECT
                user_id,
                COUNT(*) FILTER (WHERE meter LIKE 'search.%%')::integer AS search_count,
                COUNT(*) FILTER (WHERE meter LIKE '%%.sync')::integer AS sync_count,
                COUNT(*) FILTER (WHERE meter LIKE '%%.resolve')::integer AS resolve_count,
                MAX(occurred_at) AS last_usage_at
              FROM usage_events
              GROUP BY user_id
            ),
            latest AS (
              SELECT DISTINCT ON (user_id)
                user_id,
                meter AS last_meter,
                query_text AS last_query,
                status AS last_status
              FROM usage_events
              ORDER BY user_id, occurred_at DESC
            )
            SELECT
              u.id,
              u.email,
              p.display_name,
              u.created_at,
              u.last_sign_in_at,
              t.last_usage_at,
              COALESCE(ov.plan_id, 'free') AS plan_id,
              COALESCE(t.search_count, 0) AS search_count,
              COALESCE(t.sync_count, 0) AS sync_count,
              COALESCE(t.resolve_count, 0) AS resolve_count,
              l.last_meter,
              l.last_query,
              l.last_status
            FROM auth.users u
            LEFT JOIN profiles p ON p.id = u.id
            LEFT JOIN totals t ON t.user_id = u.id
            LEFT JOIN latest l ON l.user_id = u.id
            LEFT JOIN billing_overrides ov
              ON ov.user_id = u.id
             AND ov.revoked_at IS NULL
             AND (ov.expires_at IS NULL OR ov.expires_at > now())
            ORDER BY t.last_usage_at DESC NULLS LAST, u.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return list(await cur.fetchall())


async def list_usage_users(
    conn: AsyncConnection[DictRow],
    *,
    query: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    needle = query.strip()
    pattern = f"%{needle}%"
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              u.id,
              u.email,
              p.display_name,
              u.created_at,
              u.last_sign_in_at
            FROM auth.users u
            LEFT JOIN profiles p ON p.id = u.id
            WHERE u.email ILIKE %s
               OR p.display_name ILIKE %s
               OR u.id::text = %s
            ORDER BY u.last_sign_in_at DESC NULLS LAST, u.created_at DESC
            LIMIT %s
            """,
            (pattern, pattern, needle, limit),
        )
        return list(await cur.fetchall())


async def list_usage_events(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    meter: str | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    filters = ["user_id = %s"]
    args: list[Any] = [user_id]
    if meter:
        filters.append("meter = %s")
        args.append(meter)
    where = " AND ".join(filters)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT COUNT(*) AS n FROM usage_events WHERE {where}",
            args,
        )
        total_row = await cur.fetchone()
        total = int(total_row["n"]) if total_row else 0
        await cur.execute(
            f"""
            SELECT
              id,
              user_id,
              request_id,
              occurred_at,
              completed_at,
              meter,
              route,
              method,
              status,
              http_status,
              query_text,
              query_hash,
              params,
              result_count,
              duration_ms,
              units,
              error_code
            FROM usage_events
            WHERE {where}
            ORDER BY occurred_at DESC
            LIMIT %s OFFSET %s
            """,
            [*args, limit, offset],
        )
        rows = list(await cur.fetchall())
    return rows, total


async def get_meter_period_usage(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    meter: str,
    period_start: datetime | None = None,
) -> int:
    """Calendar-month event_count for one meter. Missing row is zero."""
    start, _ = utc_month_bounds(period_start)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT event_count
            FROM usage_periods
            WHERE user_id = %s
              AND meter = %s
              AND period_start = %s
            """,
            (user_id, meter, start),
        )
        row = await cur.fetchone()
    if row is None:
        return 0
    return int(row["event_count"] or 0)


async def list_usage_periods(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    limit: int = 48,
) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              id,
              user_id,
              meter,
              period_start,
              period_end,
              event_count,
              units,
              updated_at
            FROM usage_periods
            WHERE user_id = %s
            ORDER BY period_start DESC, meter ASC
            LIMIT %s
            """,
            (user_id, limit),
        )
        return list(await cur.fetchall())


async def count_daily_meter_events(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    meters: list[str] | tuple[str, ...] | frozenset[str],
    moment: datetime | None = None,
) -> int:
    """Succeeded or in-flight events for these meters since UTC midnight."""
    keys = list(meters)
    if not keys:
        return 0
    start, _ = utc_day_bounds(moment)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT COUNT(*)::int AS event_count
            FROM usage_events
            WHERE user_id = %s
              AND meter = ANY(%s)
              AND status IN ('started', 'succeeded')
              AND occurred_at >= %s
              AND COALESCE(params->>'channel', 'app') NOT IN ('api', 'mcp')
            """,
            (user_id, keys, start),
        )
        row = await cur.fetchone()
    if row is None:
        return 0
    return int(row["event_count"] or 0)
