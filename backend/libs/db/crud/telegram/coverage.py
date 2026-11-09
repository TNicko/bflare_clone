"""Shared message-fetch and media-download coverage row helpers.

Used by fetch_messages workers and the API on-demand
coverage path. Keep SQL here so services do not import each other.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from db.schemas.general.primitives import TelegramPeerTypeSchema
from utils.coverage_intervals import (
    FetchCoverageInterval,
    MediaCoverageInterval,
    empty_media_counts,
    fetch_interval_with_media_counts,
    fetch_message_count_total,
    merge_fetch_intervals,
)

_FETCH_MEDIA_COUNT_COLS = """
  COALESCE(count_image, 0) AS count_image,
  COALESCE(count_gif, 0) AS count_gif,
  COALESCE(count_video, 0) AS count_video,
  COALESCE(count_audio, 0) AS count_audio,
  COALESCE(count_document, 0) AS count_document,
  COALESCE(count_webpage, 0) AS count_webpage,
  COALESCE(count_url, 0) AS count_url,
  COALESCE(count_mention, 0) AS count_mention
"""

SELECT_MESSAGE_FETCH_COVERAGE_SQL = f"""
SELECT covered_after, covered_before, COALESCE(message_count, 0) AS message_count,
       {_FETCH_MEDIA_COUNT_COLS}
FROM telegram_message_fetch_coverage
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
ORDER BY covered_after NULLS FIRST, covered_before
"""

SELECT_PEER_MESSAGE_FETCH_COVERAGE_SQL = f"""
SELECT covered_after, covered_before, COALESCE(message_count, 0) AS message_count,
       {_FETCH_MEDIA_COUNT_COLS}
FROM telegram_message_fetch_coverage
WHERE peer_external_id = %s
  AND peer_type = %s
ORDER BY covered_after NULLS FIRST, covered_before
"""

DELETE_MESSAGE_FETCH_COVERAGE_SQL = """
DELETE FROM telegram_message_fetch_coverage
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
"""

INSERT_MESSAGE_FETCH_COVERAGE_SQL = """
INSERT INTO telegram_message_fetch_coverage (
  harvester_id,
  peer_external_id,
  peer_type,
  covered_after,
  covered_before,
  message_count,
  count_image,
  count_gif,
  count_video,
  count_audio,
  count_document,
  count_webpage,
  count_url,
  count_mention,
  is_web,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
"""

SELECT_MESSAGE_FETCH_FORWARD_COVERED_SQL = """
SELECT COALESCE(bool_or(has_forward_sources), false) AS has_forward_sources
FROM telegram_message_fetch_coverage
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
"""

MARK_MESSAGE_FETCH_FORWARD_COVERED_SQL = """
UPDATE telegram_message_fetch_coverage
SET has_forward_sources = true, updated_at = now()
WHERE harvester_id = %s
  AND peer_external_id = %s
  AND peer_type = %s
"""

HAS_FORWARD_SOURCES_COLUMN_SQL = """
SELECT EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'telegram_message_fetch_coverage'
    AND column_name = 'has_forward_sources'
) AS present
"""

INSERT_MESSAGE_FETCH_COVERAGE_WITH_FORWARD_SQL = """
INSERT INTO telegram_message_fetch_coverage (
  harvester_id,
  peer_external_id,
  peer_type,
  covered_after,
  covered_before,
  has_forward_sources,
  message_count,
  count_image,
  count_gif,
  count_video,
  count_audio,
  count_document,
  count_webpage,
  count_url,
  count_mention,
  is_web,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
"""

COUNT_MESSAGES_IN_WINDOW_SQL = """
SELECT COUNT(*)::bigint AS n
FROM telegram_messages
WHERE source_external_id = %s
  AND (%s::timestamptz IS NULL OR date >= %s::timestamptz)
  AND date < %s::timestamptz
"""

# Single-pass window stats. Kind FILTER predicates mirror
# ``telegram.media_kinds.classify_downloadable_kind`` / API media list SQL
# (includes webpage; does not require harvest.s3_uri).
COUNT_FETCH_WINDOW_STATS_SQL = """
SELECT
  COUNT(*)::bigint AS message_count,
  COUNT(*) FILTER (
    WHERE media IS NOT NULL
      AND media <> '{}'::jsonb
      AND (
        media->>'tl' = 'MessageMediaPhoto'
        OR (
          media->>'tl' = 'MessageMediaDocument'
          AND COALESCE(media->>'animated', 'false') NOT IN ('true', 't', '1')
          AND COALESCE(media->>'video', 'false') NOT IN ('true', 't', '1')
          AND COALESCE(media->>'voice', 'false') NOT IN ('true', 't', '1')
          AND COALESCE(media->>'mime_type', '') <> 'image/gif'
          AND NOT COALESCE(media->>'mime_type', '') LIKE 'video/%%'
          AND NOT COALESCE(media->>'mime_type', '') LIKE 'audio/%%'
          AND (
            COALESCE(media->>'mime_type', '') LIKE 'image/%%'
            OR COALESCE(media->>'sticker', 'false') IN ('true', 't', '1')
          )
          AND COALESCE(media->>'mime_type', '') NOT IN (
            'image/vnd.dwg', 'image/x-dwg', 'image/vnd.dxf', 'image/x-dxf',
            'image/vnd.dwf', 'image/svg+xml', 'image/svg',
            'application/x-tgsticker', 'application/x-tgsticker-video'
          )
        )
      )
  )::bigint AS count_image,
  COUNT(*) FILTER (
    WHERE media->>'tl' = 'MessageMediaDocument'
      AND (
        COALESCE(media->>'animated', 'false') IN ('true', 't', '1')
        OR COALESCE(media->>'mime_type', '') = 'image/gif'
      )
  )::bigint AS count_gif,
  COUNT(*) FILTER (
    WHERE media->>'tl' = 'MessageMediaDocument'
      AND COALESCE(media->>'animated', 'false') NOT IN ('true', 't', '1')
      AND COALESCE(media->>'mime_type', '') <> 'image/gif'
      AND (
        COALESCE(media->>'video', 'false') IN ('true', 't', '1')
        OR COALESCE(media->>'mime_type', '') LIKE 'video/%%'
      )
  )::bigint AS count_video,
  COUNT(*) FILTER (
    WHERE media->>'tl' = 'MessageMediaDocument'
      AND (
        COALESCE(media->>'voice', 'false') IN ('true', 't', '1')
        OR COALESCE(media->>'mime_type', '') LIKE 'audio/%%'
      )
  )::bigint AS count_audio,
  COUNT(*) FILTER (
    WHERE media->>'tl' = 'MessageMediaDocument'
      AND COALESCE(media->>'animated', 'false') NOT IN ('true', 't', '1')
      AND COALESCE(media->>'video', 'false') NOT IN ('true', 't', '1')
      AND COALESCE(media->>'voice', 'false') NOT IN ('true', 't', '1')
      AND COALESCE(media->>'sticker', 'false') NOT IN ('true', 't', '1')
      AND COALESCE(media->>'mime_type', '') <> 'image/gif'
      AND NOT COALESCE(media->>'mime_type', '') LIKE 'video/%%'
      AND NOT COALESCE(media->>'mime_type', '') LIKE 'audio/%%'
      AND NOT (
        COALESCE(media->>'mime_type', '') LIKE 'image/%%'
        AND COALESCE(media->>'mime_type', '') NOT IN (
          'image/vnd.dwg', 'image/x-dwg', 'image/vnd.dxf', 'image/x-dxf',
          'image/vnd.dwf', 'image/svg+xml', 'image/svg'
        )
      )
      AND COALESCE(media->>'mime_type', '') NOT IN (
        'application/x-tgsticker', 'application/x-tgsticker-video'
      )
  )::bigint AS count_document,
  COUNT(*) FILTER (
    WHERE media->>'tl' = 'MessageMediaWebPage'
  )::bigint AS count_webpage,
  COUNT(*) FILTER (
    WHERE COALESCE(has_url, false)
  )::bigint AS count_url,
  COUNT(*) FILTER (
    WHERE COALESCE(has_mention, false)
  )::bigint AS count_mention
FROM telegram_messages
WHERE source_external_id = %s
  AND (%s::timestamptz IS NULL OR date >= %s::timestamptz)
  AND date < %s::timestamptz
"""


def _fetch_interval_from_row(row: Mapping[str, Any]) -> FetchCoverageInterval:
    return fetch_interval_with_media_counts(
        covered_after=row["covered_after"],
        covered_before=row["covered_before"],
        message_count=int(row["message_count"] or 0),
        media_counts={
            "count_image": int(row["count_image"] or 0),
            "count_gif": int(row["count_gif"] or 0),
            "count_video": int(row["count_video"] or 0),
            "count_audio": int(row["count_audio"] or 0),
            "count_document": int(row["count_document"] or 0),
            "count_webpage": int(row["count_webpage"] or 0),
            "count_url": int(row.get("count_url") or 0),
            "count_mention": int(row.get("count_mention") or 0),
        },
    )


def _fetch_insert_count_tuple(
    i: FetchCoverageInterval,
) -> tuple[int, int, int, int, int, int, int, int, int]:
    return (
        i.message_count,
        i.count_image,
        i.count_gif,
        i.count_video,
        i.count_audio,
        i.count_document,
        i.count_webpage,
        i.count_url,
        i.count_mention,
    )


SELECT_MEDIA_DOWNLOAD_COVERAGE_SQL = """
SELECT covered_after, covered_before,
       count_image, count_gif, count_video, count_audio, count_document, count_webpage,
       phashes_in_milvus, videos_excluded, large_excluded, seals_gap
FROM telegram_media_download_coverage
WHERE peer_external_id = %s
  AND peer_type = %s
ORDER BY covered_after NULLS FIRST, covered_before
"""

DELETE_MEDIA_DOWNLOAD_COVERAGE_SQL = """
DELETE FROM telegram_media_download_coverage
WHERE peer_external_id = %s
  AND peer_type = %s
"""

INSERT_MEDIA_DOWNLOAD_COVERAGE_SQL = """
INSERT INTO telegram_media_download_coverage (
  peer_external_id,
  peer_type,
  covered_after,
  covered_before,
  count_image,
  count_gif,
  count_video,
  count_audio,
  count_document,
  count_webpage,
  phashes_in_milvus,
  videos_excluded,
  large_excluded,
  seals_gap,
  updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
"""

LOCK_MEDIA_DOWNLOAD_COVERAGE_SQL = """
SELECT pg_advisory_xact_lock(hashtext(%s::text), hashtext(%s::text))
"""


async def list_coverage(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> list[FetchCoverageInterval]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            SELECT_MESSAGE_FETCH_COVERAGE_SQL,
            (harvester_id, peer_external_id, peer_type),
        )
        rows = await cur.fetchall()
    return [_fetch_interval_from_row(r) for r in rows]


async def list_peer_coverage(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> list[FetchCoverageInterval]:
    """Merged message-fetch coverage for a peer across all harvesters.

    Messages are stored globally, so a full backfill by one harvester should
    count as coverage for later fetches of the same peer.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            SELECT_PEER_MESSAGE_FETCH_COVERAGE_SQL,
            (peer_external_id, peer_type),
        )
        rows = await cur.fetchall()
    return merge_fetch_intervals([_fetch_interval_from_row(r) for r in rows])


async def peer_coverage_message_count(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> int:
    """Peer inventory ``message_count`` after merging fetch-coverage islands.

    Overlapping harvester rows take max (no double-count); adjacent windows sum.
    """
    intervals = await list_peer_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    return fetch_message_count_total(intervals)


async def count_messages_in_window(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    covered_after: datetime | None,
    covered_before: datetime,
) -> int:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            COUNT_MESSAGES_IN_WINDOW_SQL,
            (peer_external_id, covered_after, covered_after, covered_before),
        )
        row = await cur.fetchone()
    return int(row["n"] if row else 0)


async def count_fetch_window_stats(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    covered_after: datetime | None,
    covered_before: datetime,
) -> tuple[int, dict[str, int]]:
    """Return ``(message_count, media_counts)`` for ``[after, before)`` in one scan.

    Media kinds match UI filters (includes webpage). Does **not** require
    ``harvest.s3_uri``.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            COUNT_FETCH_WINDOW_STATS_SQL,
            (peer_external_id, covered_after, covered_after, covered_before),
        )
        row = await cur.fetchone()
    if not row:
        return 0, empty_media_counts()
    media_counts = empty_media_counts()
    media_counts["count_image"] = int(row["count_image"] or 0)
    media_counts["count_gif"] = int(row["count_gif"] or 0)
    media_counts["count_video"] = int(row["count_video"] or 0)
    media_counts["count_audio"] = int(row["count_audio"] or 0)
    media_counts["count_document"] = int(row["count_document"] or 0)
    media_counts["count_webpage"] = int(row["count_webpage"] or 0)
    media_counts["count_url"] = int(row.get("count_url") or 0)
    media_counts["count_mention"] = int(row.get("count_mention") or 0)
    return int(row["message_count"] or 0), media_counts


async def count_media_kinds_in_window(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    covered_after: datetime | None,
    covered_before: datetime,
) -> dict[str, int]:
    """Count downloadable media kinds in ``[after, before)`` (SQL aggregate)."""
    _message_count, media_counts = await count_fetch_window_stats(
        conn,
        peer_external_id=peer_external_id,
        covered_after=covered_after,
        covered_before=covered_before,
    )
    return media_counts


async def build_fetch_interval_for_window(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    covered_after: datetime | None,
    covered_before: datetime,
    message_count: int | None = None,
) -> FetchCoverageInterval:
    """Build a fetch island with DB-reconciled message + media-kind counts."""
    db_message_count, media_counts = await count_fetch_window_stats(
        conn,
        peer_external_id=peer_external_id,
        covered_after=covered_after,
        covered_before=covered_before,
    )
    return fetch_interval_with_media_counts(
        covered_after=covered_after,
        covered_before=covered_before,
        message_count=db_message_count if message_count is None else message_count,
        media_counts=media_counts,
    )


async def replace_coverage(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    intervals: list[FetchCoverageInterval],
    is_web: bool = False,
) -> None:
    has_forward_sources = False
    has_forward_sources_col = False
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(HAS_FORWARD_SOURCES_COLUMN_SQL)
        col_row = await cur.fetchone()
        has_forward_sources_col = bool(col_row and col_row["present"])
        if has_forward_sources_col:
            await cur.execute(
                SELECT_MESSAGE_FETCH_FORWARD_COVERED_SQL,
                (harvester_id, peer_external_id, peer_type),
            )
            row = await cur.fetchone()
            has_forward_sources = bool(row and row["has_forward_sources"])
        await cur.execute(
            DELETE_MESSAGE_FETCH_COVERAGE_SQL,
            (harvester_id, peer_external_id, peer_type),
        )
        if intervals:
            if has_forward_sources_col:
                params = [
                    (
                        harvester_id,
                        peer_external_id,
                        peer_type,
                        i.covered_after,
                        i.covered_before,
                        has_forward_sources,
                        *_fetch_insert_count_tuple(i),
                        bool(is_web),
                    )
                    for i in intervals
                ]
                await cur.executemany(INSERT_MESSAGE_FETCH_COVERAGE_WITH_FORWARD_SQL, params)
            else:
                params = [
                    (
                        harvester_id,
                        peer_external_id,
                        peer_type,
                        i.covered_after,
                        i.covered_before,
                        *_fetch_insert_count_tuple(i),
                        bool(is_web),
                    )
                    for i in intervals
                ]
                await cur.executemany(INSERT_MESSAGE_FETCH_COVERAGE_SQL, params)
    await conn.commit()


async def mark_forward_sources_covered(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(HAS_FORWARD_SOURCES_COLUMN_SQL)
        col_row = await cur.fetchone()
        if not (col_row and col_row["present"]):
            return
        await cur.execute(
            MARK_MESSAGE_FETCH_FORWARD_COVERED_SQL,
            (harvester_id, peer_external_id, peer_type),
        )
    await conn.commit()


async def list_media_download_coverage(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> list[MediaCoverageInterval]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            SELECT_MEDIA_DOWNLOAD_COVERAGE_SQL,
            (peer_external_id, peer_type),
        )
        rows = await cur.fetchall()
    return [
        MediaCoverageInterval(
            covered_after=r["covered_after"],
            covered_before=r["covered_before"],
            count_image=int(r["count_image"] or 0),
            count_gif=int(r["count_gif"] or 0),
            count_video=int(r["count_video"] or 0),
            count_audio=int(r["count_audio"] or 0),
            count_document=int(r["count_document"] or 0),
            count_webpage=int(r["count_webpage"] or 0),
            phashes_in_milvus=bool(r["phashes_in_milvus"]),
            videos_excluded=bool(r["videos_excluded"]),
            large_excluded=bool(r["large_excluded"]),
            seals_gap=bool(r["seals_gap"]) if "seals_gap" in r else True,
        )
        for r in rows
    ]


async def lock_media_download_coverage(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> None:
    """Transaction-scoped advisory lock for coverage RMW on one peer."""
    async with conn.cursor() as cur:
        await cur.execute(
            LOCK_MEDIA_DOWNLOAD_COVERAGE_SQL,
            (peer_external_id, peer_type),
        )


async def replace_media_download_coverage(
    conn: AsyncConnection,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    intervals: list[MediaCoverageInterval],
    commit: bool = True,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            DELETE_MEDIA_DOWNLOAD_COVERAGE_SQL,
            (peer_external_id, peer_type),
        )
        if intervals:
            params = [
                (
                    peer_external_id,
                    peer_type,
                    i.covered_after,
                    i.covered_before,
                    i.count_image,
                    i.count_gif,
                    i.count_video,
                    i.count_audio,
                    i.count_document,
                    i.count_webpage,
                    i.phashes_in_milvus,
                    i.videos_excluded,
                    i.large_excluded,
                    i.seals_gap,
                )
                for i in intervals
            ]
            await cur.executemany(INSERT_MEDIA_DOWNLOAD_COVERAGE_SQL, params)
    if commit:
        await conn.commit()
