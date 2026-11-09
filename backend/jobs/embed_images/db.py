from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.slim_message_media import SlimTypeMessageMediaSchema
from db.query_timeouts import read_query_transaction
from db.utils import adapt_param
from telegram.media_kinds import parse_slim_media
from utils.coverage_intervals import CoverageInterval, merge_intervals

from .queries import (
    DELETE_EMBED_COVERAGE_SQL,
    INSERT_EMBED_COVERAGE_SQL,
    LIST_PEERS_FOR_CATEGORIES_SQL,
    LIST_PEERS_FOR_COMMUNITY_ID_SQL,
    LIST_PEERS_FOR_EXTERNAL_ID_SQL,
    LIST_PEERS_FOR_NAME_SQL,
    LIST_PEERS_PENDING_EMBED_SQL,
    LIST_PEERS_WITH_DOWNLOAD_COVERAGE_OVERLAPPING_SQL,
    LIST_PEERS_WITH_DOWNLOAD_COVERAGE_SQL,
    SELECT_DOWNLOAD_COVERAGE_FOR_PEER_SQL,
    SELECT_EMBED_COVERAGE_SQL,
    SELECT_EMBED_CURSOR_SQL,
    SELECT_MESSAGES_NEEDING_EMBED_SQL,
    UPDATE_MESSAGE_MEDIA_SQL,
    UPSERT_EMBED_CURSOR_SQL,
)

from psycopg import AsyncConnection

logger = logging.getLogger(__name__)


class EmbedPeerRow(BaseModel):
    """Peer to embed images for (no Telegram harvester — media already on S3)."""

    model_config = ConfigDict(frozen=True)

    peer_external_id: str
    peer_type: TelegramPeerTypeSchema


class MessageMediaRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    message_id: int
    source_external_id: str
    peer_type: TelegramPeerTypeSchema
    media: SlimTypeMessageMediaSchema | None
    date: datetime | None


class EmbedCursorState(BaseModel):
    model_config = ConfigDict(frozen=True)

    last_message_id: int
    is_complete: bool


def _rows_to_peers(rows: list[tuple[object, ...]]) -> list[EmbedPeerRow]:
    out: list[EmbedPeerRow] = []
    for peer_external_id, peer_type in rows:
        out.append(
            EmbedPeerRow(
                peer_external_id=str(peer_external_id),
                peer_type=peer_type,  # type: ignore[arg-type]
            )
        )
    return out


async def list_peers_pending_embed(
    conn: AsyncConnection,
    *,
    model: str,
    peer_types: list[TelegramPeerTypeSchema],
    window_after: datetime | None,
    window_before: datetime | None,
    limit: int,
) -> list[EmbedPeerRow]:
    """Peers whose download coverage has advanced past embed (or incomplete cursor)."""
    async with conn.cursor() as cur:
        await cur.execute(
            LIST_PEERS_PENDING_EMBED_SQL,
            (
                window_after,
                window_before,
                model,
                peer_types,
                model,
                limit,
            ),
        )
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_peers_with_download_coverage(
    conn: AsyncConnection,
    *,
    peer_types: list[TelegramPeerTypeSchema],
    limit: int,
) -> list[EmbedPeerRow]:
    async with conn.cursor() as cur:
        await cur.execute(LIST_PEERS_WITH_DOWNLOAD_COVERAGE_SQL, (peer_types, limit))
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_peers_with_download_coverage_overlapping(
    conn: AsyncConnection,
    *,
    peer_types: list[TelegramPeerTypeSchema],
    window_after: datetime,
    window_before: datetime,
    limit: int | None = None,
) -> list[EmbedPeerRow]:
    """Peers whose media-download islands overlap ``[window_after, window_before)``."""
    async with conn.cursor() as cur:
        await cur.execute(
            LIST_PEERS_WITH_DOWNLOAD_COVERAGE_OVERLAPPING_SQL,
            (peer_types, window_before, window_after, limit if limit and limit > 0 else 1_000_000),
        )
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_peers_for_external_id(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
) -> list[EmbedPeerRow]:
    async with conn.cursor() as cur:
        await cur.execute(
            LIST_PEERS_FOR_EXTERNAL_ID_SQL,
            (peer_external_id, peer_external_id, peer_external_id),
        )
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_peers_for_community_id(
    conn: AsyncConnection,
    *,
    community_id: UUID,
) -> list[EmbedPeerRow]:
    async with conn.cursor() as cur:
        await cur.execute(LIST_PEERS_FOR_COMMUNITY_ID_SQL, (community_id,))
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_peers_for_name_query(
    conn: AsyncConnection,
    *,
    peer_name_query: str,
) -> list[EmbedPeerRow]:
    pattern = f"%{peer_name_query}%"
    async with conn.cursor() as cur:
        await cur.execute(LIST_PEERS_FOR_NAME_SQL, (pattern,))
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_peers_for_categories(
    conn: AsyncConnection,
    *,
    category_slugs: list[str],
) -> list[EmbedPeerRow]:
    async with conn.cursor() as cur:
        await cur.execute(LIST_PEERS_FOR_CATEGORIES_SQL, (category_slugs,))
        rows = await cur.fetchall()
    return _rows_to_peers(rows)


async def list_download_coverage_for_peer(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> list[CoverageInterval]:
    """Union download coverage across all harvesters for this peer."""
    async with conn.cursor() as cur:
        await cur.execute(
            SELECT_DOWNLOAD_COVERAGE_FOR_PEER_SQL,
            (peer_external_id, peer_type),
        )
        rows = await cur.fetchall()
    return merge_intervals([CoverageInterval(covered_after=r[0], covered_before=r[1]) for r in rows])


async def list_embed_coverage(
    conn: AsyncConnection,
    *,
    model: str,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> list[CoverageInterval]:
    async with conn.cursor() as cur:
        await cur.execute(
            SELECT_EMBED_COVERAGE_SQL,
            (model, peer_external_id, peer_type),
        )
        rows = await cur.fetchall()
    return [CoverageInterval(covered_after=r[0], covered_before=r[1]) for r in rows]


async def replace_embed_coverage(
    conn: AsyncConnection,
    *,
    model: str,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    intervals: list[CoverageInterval],
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            DELETE_EMBED_COVERAGE_SQL,
            (model, peer_external_id, peer_type),
        )
        if intervals:
            params = [
                (
                    model,
                    peer_external_id,
                    peer_type,
                    i.covered_after,
                    i.covered_before,
                )
                for i in intervals
            ]
            await cur.executemany(INSERT_EMBED_COVERAGE_SQL, params)
    await conn.commit()


async def fetch_embed_cursor(
    conn: AsyncConnection,
    *,
    model: str,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    window_after: datetime | None,
    window_before: datetime,
) -> EmbedCursorState:
    async with conn.cursor() as cur:
        await cur.execute(
            SELECT_EMBED_CURSOR_SQL,
            (
                model,
                peer_external_id,
                peer_type,
                window_before,
                window_after,
            ),
        )
        row = await cur.fetchone()
    if row is None:
        return EmbedCursorState(last_message_id=0, is_complete=False)
    return EmbedCursorState(last_message_id=int(row[0]), is_complete=bool(row[1]))


async def upsert_embed_cursor(
    conn: AsyncConnection,
    *,
    model: str,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    window_after: datetime | None,
    window_before: datetime,
    last_message_id: int,
    is_complete: bool,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            UPSERT_EMBED_CURSOR_SQL,
            (
                model,
                peer_external_id,
                peer_type,
                window_after,
                window_before,
                last_message_id,
                is_complete,
            ),
        )
    await conn.commit()


async def list_messages_needing_embed(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    covered_after: datetime | None,
    covered_before: datetime,
    last_message_id: int,
    batch_size: int,
    ignore_embedding_id: bool,
    model: str,
) -> list[MessageMediaRow]:
    started = time.monotonic()
    logger.debug(
        "DB list_messages_needing_embed peer=%s model=%s after=%s before=%s message_id>%s limit=%s",
        peer_external_id,
        model,
        covered_after,
        covered_before,
        last_message_id,
        batch_size,
    )
    async with read_query_transaction(conn):
        async with conn.cursor() as cur:
            await cur.execute(
                SELECT_MESSAGES_NEEDING_EMBED_SQL,
                (
                    peer_external_id,
                    covered_after,
                    covered_after,
                    covered_before,
                    last_message_id,
                    ignore_embedding_id,
                    model,
                    model,
                    batch_size,
                ),
            )
            rows = await cur.fetchall()
    logger.debug(
        "DB list_messages_needing_embed peer=%s: %s row(s) in %.2fs",
        peer_external_id,
        len(rows),
        time.monotonic() - started,
    )
    out: list[MessageMediaRow] = []
    for row_id, message_id, source_external_id, peer_type, media, date in rows:
        out.append(
            MessageMediaRow(
                id=row_id,
                message_id=int(message_id),
                source_external_id=str(source_external_id),
                peer_type=peer_type,
                media=parse_slim_media(media),
                date=date,
            )
        )
    return out


async def update_message_media(
    conn: AsyncConnection,
    *,
    row_id: UUID,
    date: datetime,
    media: dict[str, Any],
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(UPDATE_MESSAGE_MEDIA_SQL, (adapt_param(media), row_id, date))
    await conn.commit()


async def register_micro_shards(
    conn: AsyncConnection,
    *,
    model: str,
    modality: str,
    shards: list[tuple[str, str, int]],
) -> None:
    """Insert micro catalog rows ``(shard_key, month, row_count)`` as pending Milvus."""
    if not shards:
        return
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO embedding_store_shards AS s (
              model,
              modality,
              shard_key,
              tier,
              month,
              row_count,
              milvus_upserted_at,
              updated_at
            )
            VALUES (%s, %s, %s, 'micro', %s, %s, NULL, now())
            ON CONFLICT (model, modality, shard_key)
            DO UPDATE SET
              tier = 'micro',
              month = EXCLUDED.month,
              row_count = EXCLUDED.row_count,
              updated_at = now()
            """,
            [(model, modality, shard_key, month, row_count) for shard_key, month, row_count in shards],
        )
    await conn.commit()


async def delete_micro_shard_catalog(
    conn: AsyncConnection,
    *,
    model: str,
    modality: str,
    shard_keys: list[str],
) -> None:
    """Best-effort catalog cleanup when rolling back S3 micros after a failed cursor."""
    if not shard_keys:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM embedding_store_shards
            WHERE model = %s
              AND modality = %s
              AND shard_key = ANY(%s)
            """,
            (model, modality, shard_keys),
        )
    await conn.commit()
