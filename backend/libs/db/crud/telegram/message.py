from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from db.schemas.telegram.message import TelegramMessageSchema
from db.utils import adapt_param
from embedding.preprocess import normalize_text
from telegram.web_provenance import WEB_TME_HARVESTER_EXTERNAL_ID, is_web_harvester_external_id
from utils.hash import sha256_hex

lg = logging.getLogger(__name__)

_MESSAGE_UPSERT_COLUMNS: tuple[str, ...] = tuple(key for key in TelegramMessageSchema.model_fields if key != "id")

#: Fat body columns — do not re-fatten PG when row is archived+stripped (content IS NULL).
_FAT_BODY_COLS = frozenset(
    {
        "content",
        "content_hash",
        "reactions",
        "metadata",
        "fwd_from",
        "reply_to",
        "action_data",
        "action_type",
        "message_type",
        "grouped_id",
        "has_url",
        "has_mention",
        "views",
        "from_external_id",
    }
)


def content_hash_from_content(content: str | None) -> str | None:
    """Hash NFKC-normalized text. Short tokens / URLs still get a hash; empty does not."""
    normalized = normalize_text(content)
    return sha256_hex(normalized) if normalized is not None else None


def _with_content_hash(message: TelegramMessageSchema) -> TelegramMessageSchema:
    return message.model_copy(update={"content_hash": content_hash_from_content(message.content)})


def message_stat_deltas(
    rows: Sequence[tuple[Any, ...]],
) -> list[tuple[str, str, int, datetime | None]]:
    """Collapse freshly inserted messages into per user/community increments.

    ``rows`` are ``(from_external_id, source_external_id, date, inserted)``.
    Updates and messages with no sender are ignored. Deleted messages still count:
    a later tombstone does not change the total.
    """
    totals: dict[tuple[str, str], list[Any]] = defaultdict(lambda: [0, None])
    for from_external_id, source_external_id, message_date, inserted in rows:
        if not inserted or not from_external_id or not source_external_id:
            continue
        key = (str(from_external_id), str(source_external_id))
        count, last_at = totals[key]
        count += 1
        if isinstance(message_date, datetime) and (last_at is None or message_date > last_at):
            last_at = message_date
        totals[key] = [count, last_at]
    return [(user_id, peer_id, count, last_at) for (user_id, peer_id), (count, last_at) in totals.items()]


def _fat_col_assign(col: str, table_ident: sql.Identifier) -> sql.Composed:
    """Merge fat columns with archive-strip + web/MTProto provenance rules.

    - Stripped MTProto rows keep body (unless EXCLUDED upgrades a web stub).
    - Web scrapes never overwrite a non-web (MTProto) row.
    - MTProto always overwrites a web-tagged row.
    """
    return sql.SQL(
        """
        {col} = CASE
          WHEN COALESCE(EXCLUDED.harvester_external_id, '') = {web}
               AND COALESCE({table}.harvester_external_id, '') <> {web}
               AND {table}.harvester_external_id IS NOT NULL
          THEN {table}.{col}
          WHEN COALESCE({table}.harvester_external_id, '') = {web}
               AND COALESCE(EXCLUDED.harvester_external_id, '') <> {web}
          THEN EXCLUDED.{col}
          WHEN {table}.archive_key IS NOT NULL AND {table}.content IS NULL
          THEN {table}.{col}
          ELSE EXCLUDED.{col}
        END
        """
    ).format(
        col=sql.Identifier(col),
        table=table_ident,
        web=sql.Literal(WEB_TME_HARVESTER_EXTERNAL_ID),
    )


async def _fetch_existing_archive_meta(
    conn: AsyncConnection,
    messages: Sequence[TelegramMessageSchema],
    *,
    table_name: str,
) -> dict[tuple[str, int, datetime], dict[str, Any]]:
    """Map (source, message_id, date) → archive / edit / strip meta for cold overlay decisions."""
    keys = [
        (m.source_external_id, m.message_id, m.date)
        for m in messages
        if m.source_external_id and m.message_id is not None and m.date is not None
    ]
    if not keys:
        return {}
    values = sql.SQL(", ").join(
        sql.SQL("({}::text, {}::bigint, {}::timestamptz)").format(
            sql.Placeholder(), sql.Placeholder(), sql.Placeholder()
        )
        for _ in keys
    )
    query = sql.SQL(
        """
        SELECT
          tm.id,
          tm.source_external_id::text AS source_external_id,
          tm.message_id,
          tm.date,
          tm.archive_key,
          tm.edit_date,
          tm.deleted,
          tm.harvester_external_id,
          (tm.content IS NOT NULL) AS has_content
        FROM {} tm
        JOIN (VALUES {}) AS k(source_external_id, message_id, date)
          ON tm.source_external_id = k.source_external_id
         AND tm.message_id = k.message_id
         AND tm.date = k.date
        """
    ).format(sql.Identifier(table_name), values)
    params: list[Any] = [v for triple in keys for v in triple]
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()
    out: dict[tuple[str, int, datetime], dict[str, Any]] = {}
    for row in rows:
        out[(str(row["source_external_id"]), int(row["message_id"]), row["date"])] = dict(row)
    return out


def _needs_cold_overlay(
    message: TelegramMessageSchema,
    existing: dict[str, Any] | None,
) -> bool:
    """True when body must be written to S3 overlay (archived+stripped + edit/delete/provenance)."""
    if not existing or not existing.get("archive_key") or existing.get("has_content"):
        return False
    # Web→MTProto upgrade of a cold row: overlay carries the richer Telethon body.
    if is_web_harvester_external_id(existing.get("harvester_external_id")) and not is_web_harvester_external_id(
        message.harvester_external_id
    ):
        return True
    if bool(message.deleted) != bool(existing.get("deleted")):
        return True
    return message.edit_date != existing.get("edit_date")


def _write_cold_overlays(
    prepared: Sequence[TelegramMessageSchema],
    existing_by_key: Mapping[tuple[str, int, datetime], dict[str, Any]],
) -> int:
    from msgstore import (
        get_message_archive_prefix,
        get_message_archive_sink,
        overlay_payload_from_message_row,
        put_overlay,
    )

    sink = get_message_archive_sink()
    if sink is None:
        return 0
    prefix = get_message_archive_prefix()
    n = 0
    for message in prepared:
        if message.date is None:
            continue
        key = (message.source_external_id, message.message_id, message.date)
        existing = existing_by_key.get(key)
        if not _needs_cold_overlay(message, existing):
            continue
        row_id = existing.get("id") if existing else None
        if row_id is None:
            continue
        payload = overlay_payload_from_message_row(
            {
                **message.model_dump(mode="json"),
                "id": str(row_id),
            }
        )
        try:
            put_overlay(sink, str(row_id), payload, prefix=prefix)
            n += 1
        except Exception:
            lg.exception("failed writing cold overlay id=%s", row_id)
    return n


async def upsert_messages(
    conn: AsyncConnection,
    messages: list[TelegramMessageSchema],
    *,
    table_name: str = "telegram_messages",
    conflict_columns: tuple[str, ...] = ("source_external_id", "message_id", "date"),
) -> None:
    if not messages:
        return

    prepared = [_with_content_hash(m) for m in messages]
    existing_meta: dict[tuple[str, int, datetime], dict[str, Any]] = {}
    if table_name == "telegram_messages":
        try:
            existing_meta = await _fetch_existing_archive_meta(conn, prepared, table_name=table_name)
        except Exception:
            # Pre-migration / missing archive columns — fall back to legacy upsert.
            lg.warning("archive meta lookup failed; upserting without cold overlay gate", exc_info=True)
            existing_meta = {}

    # Let server defaults set created_at / updated_at on INSERT. Explicit NULLs would
    # override DEFAULT now(), and xmax-based insert detection is unsupported on
    # partitioned tables (FeatureNotSupported: cannot retrieve a system column…).
    columns = tuple(c for c in _MESSAGE_UPSERT_COLUMNS if c not in {"created_at", "updated_at"})
    rows: list[Mapping[str, Any]] = [m.model_dump(mode="json", exclude_none=True, exclude={"id"}) for m in prepared]
    immutable = set(conflict_columns) | {"created_at"}
    update_cols = [c for c in columns if c not in immutable and c not in {"media", "harvester_external_id"}]
    table_ident = sql.Identifier(table_name)
    update_fragments: list[sql.Composable] = []
    for col in update_cols:
        if col in _FAT_BODY_COLS:
            update_fragments.append(_fat_col_assign(col, table_ident))
        else:
            update_fragments.append(sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col)))
    update_fragments.append(sql.SQL("updated_at = now()"))
    update_fragments.append(
        sql.SQL(
            """
            harvester_external_id = CASE
                WHEN COALESCE(EXCLUDED.harvester_external_id, '') = {web}
                     AND COALESCE({table}.harvester_external_id, '') <> {web}
                     AND {table}.harvester_external_id IS NOT NULL
                THEN {table}.harvester_external_id
                ELSE COALESCE(EXCLUDED.harvester_external_id, {table}.harvester_external_id)
            END
            """
        ).format(table=table_ident, web=sql.Literal(WEB_TME_HARVESTER_EXTERNAL_ID))
    )
    update_fragments.append(
        sql.SQL(
            """
            media = CASE
                WHEN COALESCE(EXCLUDED.harvester_external_id, '') = {web}
                     AND {table}.media IS NOT NULL
                     AND {table}.media <> '{{}}'::jsonb
                     AND COALESCE({table}.harvester_external_id, '') <> {web}
                THEN {table}.media
                WHEN COALESCE({table}.harvester_external_id, '') = {web}
                     AND COALESCE(EXCLUDED.harvester_external_id, '') <> {web}
                THEN EXCLUDED.media
                WHEN {table}.archive_key IS NOT NULL AND {table}.content IS NULL
                     AND COALESCE({table}.harvester_external_id, '') <> {web}
                THEN {table}.media
                WHEN {table}.media IS NULL OR {table}.media = '{{}}'::jsonb
                THEN EXCLUDED.media
                WHEN COALESCE({table}.media->>'stripped_thumb', '') = ''
                     AND EXCLUDED.media ? 'stripped_thumb'
                THEN {table}.media || jsonb_build_object(
                    'stripped_thumb', EXCLUDED.media->'stripped_thumb'
                )
                ELSE {table}.media
            END
            """
        ).format(table=table_ident, web=sql.Literal(WEB_TME_HARVESTER_EXTERNAL_ID))
    )
    updates = sql.SQL(", ").join(update_fragments)
    track_stats = table_name == "telegram_messages"
    # Partitioned tables cannot RETURNING xmax; treat rows whose created_at was set
    # in this transaction as inserts (DEFAULT now() == transaction_timestamp()).
    returning = (
        sql.SQL(
            """
            RETURNING from_external_id, source_external_id, date,
                      (created_at = transaction_timestamp()) AS inserted
            """
        )
        if track_stats
        else sql.SQL("")
    )
    query = sql.SQL(
        """
        INSERT INTO {table} ({fields})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_fields})
        DO UPDATE SET {updates}
        {returning}
        """
    ).format(
        table=table_ident,
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        conflict_fields=sql.SQL(", ").join(map(sql.Identifier, conflict_columns)),
        updates=updates,
        returning=returning,
    )
    params_list = [tuple(adapt_param(row.get(col)) for col in columns) for row in rows]

    async with conn.cursor() as cur:
        await cur.executemany(query, params_list, returning=track_stats)
        if track_stats:
            inserted: list[tuple[Any, ...]] = []
            async for _ in cur.results():
                inserted.extend(await cur.fetchall())
            deltas = message_stat_deltas(inserted)
            if deltas:
                await cur.execute(
                    """
                    INSERT INTO user_peer_message_stats (
                      user_external_id, peer_external_id, message_count, last_message_at
                    )
                    SELECT *
                    FROM unnest(%s::text[], %s::text[], %s::bigint[], %s::timestamptz[])
                      AS t(user_external_id, peer_external_id, message_count, last_message_at)
                    ON CONFLICT (user_external_id, peer_external_id) DO UPDATE SET
                      message_count = user_peer_message_stats.message_count + EXCLUDED.message_count,
                      last_message_at = CASE
                        WHEN user_peer_message_stats.last_message_at IS NULL THEN EXCLUDED.last_message_at
                        WHEN EXCLUDED.last_message_at IS NULL THEN user_peer_message_stats.last_message_at
                        ELSE GREATEST(
                          user_peer_message_stats.last_message_at,
                          EXCLUDED.last_message_at
                        )
                      END
                    """,
                    (
                        [row[0] for row in deltas],
                        [row[1] for row in deltas],
                        [row[2] for row in deltas],
                        [row[3] for row in deltas],
                    ),
                )
    await conn.commit()

    if existing_meta:
        written = _write_cold_overlays(prepared, existing_meta)
        if written:
            lg.info("wrote %d cold message overlay(s)", written)


async def mark_missing_messages_deleted(
    conn: AsyncConnection,
    *,
    source_external_id: str,
    id_lo: int,
    id_hi: int,
    seen_message_ids: Sequence[int],
    since: datetime | None = None,
) -> list[int]:
    """Mark DB rows in ``[id_lo, id_hi]`` deleted when absent from Telegram's page.

    Returns the message ids newly marked deleted. Optional ``since`` further
    restricts to rows with ``date >= since`` so we never tombstone older history
    outside the refresh window.
    """
    if id_lo > id_hi:
        return []

    seen = sorted({int(mid) for mid in seen_message_ids if mid is not None})
    since_sql = " AND date >= %s" if since is not None else ""

    if seen:
        query = f"""
            UPDATE telegram_messages
            SET deleted = true
            WHERE source_external_id = %s
              AND message_id >= %s
              AND message_id <= %s
              AND deleted = false
              AND NOT (message_id = ANY(%s))
              {since_sql}
            RETURNING message_id
            """
        params: list[Any] = [source_external_id, id_lo, id_hi, seen]
    else:
        query = f"""
            UPDATE telegram_messages
            SET deleted = true
            WHERE source_external_id = %s
              AND message_id >= %s
              AND message_id <= %s
              AND deleted = false
              {since_sql}
            RETURNING message_id
            """
        params = [source_external_id, id_lo, id_hi]

    if since is not None:
        params.append(since)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, tuple(params))
        rows = await cur.fetchall()
    await conn.commit()
    return [int(r["message_id"]) for r in rows]
