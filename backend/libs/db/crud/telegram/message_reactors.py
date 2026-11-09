"""CRUD for ``telegram_message_reactors``."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from psycopg import AsyncConnection

from db.schemas.telegram.message_reactor import TelegramMessageReactorSchema
from db.utils import adapt_param


async def message_has_reactors(
    conn: AsyncConnection,
    *,
    message_id: UUID,
    message_date: datetime,
) -> bool:
    query = """
        SELECT EXISTS(
          SELECT 1
          FROM telegram_message_reactors
          WHERE message_id = %s AND message_date = %s
        )
    """
    async with conn.cursor() as cur:
        await cur.execute(query, (message_id, message_date))
        row = await cur.fetchone()
    return bool(row and row[0])


async def fetch_message_row_ids(
    conn: AsyncConnection,
    keys: Sequence[tuple[str, int, datetime]],
) -> dict[tuple[str, int, datetime], UUID]:
    """Map (source_external_id, message_id, date) → message UUID."""
    if not keys:
        return {}
    query = """
        SELECT source_external_id, message_id, date, id
        FROM telegram_messages
        WHERE (source_external_id, message_id, date) IN (
          SELECT * FROM unnest(%s::text[], %s::bigint[], %s::timestamptz[])
            AS t(source_external_id, message_id, date)
        )
    """
    sources = [k[0] for k in keys]
    mids = [k[1] for k in keys]
    dates = [k[2] for k in keys]
    async with conn.cursor() as cur:
        await cur.execute(query, (sources, mids, dates))
        rows = await cur.fetchall()
    out: dict[tuple[str, int, datetime], UUID] = {}
    for source, mid, date, mid_uuid in rows:
        out[(str(source), int(mid), date)] = mid_uuid if isinstance(mid_uuid, UUID) else UUID(str(mid_uuid))
    return out


async def fetch_existing_reaction_totals(
    conn: AsyncConnection,
    keys: Sequence[tuple[str, int, datetime]],
) -> dict[tuple[str, int, datetime], tuple[UUID, int, bool]]:
    """Before upsert: (source, msg_id, date) → (uuid, reaction_total, has_reactors)."""
    if not keys:
        return {}
    query = """
        SELECT
          m.source_external_id,
          m.message_id,
          m.date,
          m.id,
          COALESCE((
            SELECT SUM((elem->>'count')::int)
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(m.reactions) = 'object' AND m.reactions ? 'results'
                  THEN m.reactions->'results'
                ELSE '[]'::jsonb
              END
            ) AS elem
          ), 0)::int AS reaction_total,
          EXISTS(
            SELECT 1 FROM telegram_message_reactors r
            WHERE r.message_id = m.id AND r.message_date = m.date
          ) AS has_reactors
        FROM telegram_messages m
        WHERE (m.source_external_id, m.message_id, m.date) IN (
          SELECT * FROM unnest(%s::text[], %s::bigint[], %s::timestamptz[])
            AS t(source_external_id, message_id, date)
        )
    """
    sources = [k[0] for k in keys]
    mids = [k[1] for k in keys]
    dates = [k[2] for k in keys]
    async with conn.cursor() as cur:
        await cur.execute(query, (sources, mids, dates))
        rows = await cur.fetchall()
    out: dict[tuple[str, int, datetime], tuple[UUID, int, bool]] = {}
    for source, mid, date, mid_uuid, total, has_reactors in rows:
        uid = mid_uuid if isinstance(mid_uuid, UUID) else UUID(str(mid_uuid))
        out[(str(source), int(mid), date)] = (uid, int(total or 0), bool(has_reactors))
    return out


async def replace_message_reactors(
    conn: AsyncConnection,
    *,
    message_id: UUID,
    message_date: datetime,
    rows: Sequence[TelegramMessageReactorSchema],
) -> int:
    """Delete existing reactors for the message, then insert ``rows``. Returns insert count."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM telegram_message_reactors
            WHERE message_id = %s AND message_date = %s
            """,
            (message_id, message_date),
        )
        if not rows:
            return 0
        insert_sql = """
            INSERT INTO telegram_message_reactors (
              message_id, message_date, source_external_id, reactor_external_id, peer_type,
              reaction_key, reaction, reacted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id, message_date, reactor_external_id, reaction_key)
            DO UPDATE SET
              source_external_id = EXCLUDED.source_external_id,
              peer_type = EXCLUDED.peer_type,
              reaction = EXCLUDED.reaction,
              reacted_at = EXCLUDED.reacted_at
        """
        params = [
            (
                r.message_id,
                r.message_date,
                r.source_external_id,
                r.reactor_external_id,
                r.peer_type,
                r.reaction_key,
                adapt_param(r.reaction.model_dump(mode="json")),
                r.reacted_at,
            )
            for r in rows
        ]
        await cur.executemany(insert_sql, params)
    return len(rows)
