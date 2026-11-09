"""Record and read harvester flood-waits / operational blocks."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)


def flood_wait_scope(exc: BaseException, *, default: str = "telegram") -> str:
    """Best-effort RPC / method name for the flood wait."""
    request = getattr(exc, "request", None)
    if request is not None:
        name = type(request).__name__
        if name:
            return name
    text = str(exc)
    for needle in (
        "GetHistoryRequest",
        "GetParticipantsRequest",
        "ResolveUsernameRequest",
        "GetFullChannelRequest",
        "GetMessagesViewsRequest",
        "SendMessageRequest",
    ):
        if needle in text:
            return needle
    if "GetHistory" in text:
        return "GetHistoryRequest"
    return default


def flood_wait_seconds(exc: FloodWaitError) -> int:
    raw = getattr(exc, "seconds", None)
    try:
        seconds = int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        seconds = 0
    return max(0, seconds)


async def record_harvester_flood_wait(
    conn: Any,
    *,
    harvester_id: UUID | str,
    wait_seconds: int,
    scope: str = "telegram",
    peer_external_id: str | None = None,
    error_message: str | None = None,
    source: str | None = None,
    started_at: datetime | None = None,
) -> datetime:
    """Upsert a flood-wait window for a harvester+scope. Returns ``until_at``."""
    seconds = max(0, int(wait_seconds))
    started = started_at or datetime.now(UTC)
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    until = started + timedelta(seconds=seconds)
    scope_key = (scope or "telegram").strip() or "telegram"
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO telegram_harvester_flood_waits (
              harvester_id, scope, wait_seconds, started_at, until_at,
              peer_external_id, error_message, source, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (harvester_id, scope) DO UPDATE SET
              wait_seconds = EXCLUDED.wait_seconds,
              started_at = EXCLUDED.started_at,
              until_at = GREATEST(
                telegram_harvester_flood_waits.until_at,
                EXCLUDED.until_at
              ),
              peer_external_id = COALESCE(
                EXCLUDED.peer_external_id,
                telegram_harvester_flood_waits.peer_external_id
              ),
              error_message = COALESCE(
                EXCLUDED.error_message,
                telegram_harvester_flood_waits.error_message
              ),
              source = COALESCE(EXCLUDED.source, telegram_harvester_flood_waits.source),
              updated_at = now()
            RETURNING until_at
            """,
            (
                str(harvester_id),
                scope_key,
                seconds,
                started,
                until,
                peer_external_id,
                error_message,
                source,
            ),
        )
        row = await cur.fetchone()
    until_at = row[0] if row else until
    logger.warning(
        "harvester flood-wait recorded harvester_id=%s scope=%s seconds=%s until=%s source=%s peer=%s",
        harvester_id,
        scope_key,
        seconds,
        until_at,
        source,
        peer_external_id,
    )
    return until_at


async def record_harvester_flood_wait_from_exc(
    conn: Any,
    *,
    harvester_id: UUID | str,
    exc: FloodWaitError,
    peer_external_id: str | None = None,
    source: str | None = None,
    scope: str | None = None,
) -> datetime:
    return await record_harvester_flood_wait(
        conn,
        harvester_id=harvester_id,
        wait_seconds=flood_wait_seconds(exc),
        scope=scope or flood_wait_scope(exc),
        peer_external_id=peer_external_id,
        error_message=str(exc) or None,
        source=source,
    )


async def list_active_harvester_flood_waits(
    conn: Any,
    *,
    harvester_ids: list[UUID | str] | None = None,
) -> list[dict[str, Any]]:
    """Active (until_at > now) flood waits, optionally filtered to harvesters."""
    async with conn.cursor(row_factory=dict_row) as cur:
        if harvester_ids:
            await cur.execute(
                """
                SELECT
                  harvester_id,
                  scope,
                  wait_seconds,
                  started_at,
                  until_at,
                  peer_external_id,
                  error_message,
                  source,
                  updated_at,
                  GREATEST(
                    0,
                    EXTRACT(EPOCH FROM (until_at - now()))::bigint
                  ) AS seconds_remaining
                FROM telegram_harvester_flood_waits
                WHERE until_at > now()
                  AND harvester_id = ANY(%s::uuid[])
                ORDER BY until_at DESC, scope
                """,
                ([str(h) for h in harvester_ids],),
            )
        else:
            await cur.execute(
                """
                SELECT
                  harvester_id,
                  scope,
                  wait_seconds,
                  started_at,
                  until_at,
                  peer_external_id,
                  error_message,
                  source,
                  updated_at,
                  GREATEST(
                    0,
                    EXTRACT(EPOCH FROM (until_at - now()))::bigint
                  ) AS seconds_remaining
                FROM telegram_harvester_flood_waits
                WHERE until_at > now()
                ORDER BY until_at DESC, scope
                """
            )
        return [dict(r) for r in await cur.fetchall()]


async def harvester_ids_with_active_flood_wait(conn: Any) -> set[str]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT DISTINCT harvester_id::text
            FROM telegram_harvester_flood_waits
            WHERE until_at > now()
            """
        )
        return {str(r[0]) for r in await cur.fetchall()}


async def delete_expired_harvester_flood_waits(conn: Any) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM telegram_harvester_flood_waits
            WHERE until_at <= now()
            """
        )
        return int(cur.rowcount or 0)
