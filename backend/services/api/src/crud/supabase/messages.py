"""Fetch telegram_messages and telegram_message_edits for a peer (paginated)."""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, Literal

from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row

from billing.windows import next_before_for_window
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView

lg = logging.getLogger(__name__)


async def _hydrate_peer_rows(rows: list[dict[str, Any]]) -> None:
    """Fill stripped cold bodies from Parquet/overlays when needed."""
    if not rows:
        return
    try:
        from msgstore import (
            get_message_archive_prefix,
            get_message_archive_sink,
            hydrate_message_rows_async,
        )

        await hydrate_message_rows_async(
            rows,
            get_message_archive_sink(),
            prefix=get_message_archive_prefix(),
        )
    except Exception:
        lg.warning("message archive hydrate failed for peer page", exc_info=True)


MediaKindFilter = Literal["all", "visual", "image", "gif", "video", "audio", "document", "webpage"]

# Approximate SQL predicates mirroring ``telegram.media_kinds.classify_downloadable_kind``.
# Note: use a single ``{}`` here (valid jsonb). ``{{}}`` is only for Python str.format escapes
# and would be sent literally when this fragment is composed via psycopg.sql.
_DOWNLOADABLE_MEDIA_SQL = sql.SQL(
    """
    media IS NOT NULL
    AND media <> '{}'::jsonb
    AND (
      media->>'tl' = 'MessageMediaPhoto'
      OR (
        media->>'tl' = 'MessageMediaDocument'
        AND COALESCE(media->>'mime_type', '') NOT IN (
          'application/x-tgsticker',
          'application/x-tgsticker-video'
        )
      )
      OR media->>'tl' = 'MessageMediaWebPage'
    )
    """
)

_KIND_PREDICATES: dict[str, sql.Composable] = {
    "image": sql.SQL(
        """
        (
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
        """
    ),
    "gif": sql.SQL(
        """
        media->>'tl' = 'MessageMediaDocument'
        AND (
          COALESCE(media->>'animated', 'false') IN ('true', 't', '1')
          OR COALESCE(media->>'mime_type', '') = 'image/gif'
        )
        """
    ),
    "video": sql.SQL(
        """
        media->>'tl' = 'MessageMediaDocument'
        AND COALESCE(media->>'animated', 'false') NOT IN ('true', 't', '1')
        AND COALESCE(media->>'mime_type', '') <> 'image/gif'
        AND (
          COALESCE(media->>'video', 'false') IN ('true', 't', '1')
          OR COALESCE(media->>'mime_type', '') LIKE 'video/%%'
        )
        """
    ),
    "audio": sql.SQL(
        """
        media->>'tl' = 'MessageMediaDocument'
        AND (
          COALESCE(media->>'voice', 'false') IN ('true', 't', '1')
          OR COALESCE(media->>'mime_type', '') LIKE 'audio/%%'
        )
        """
    ),
    "document": sql.SQL(
        """
        media->>'tl' = 'MessageMediaDocument'
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
        """
    ),
    "webpage": sql.SQL(
        """
        media->>'tl' = 'MessageMediaWebPage'
        """
    ),
}

# Image + GIF + video (Free unlock window counts these, matching the client).
_KIND_PREDICATES["visual"] = sql.SQL(
    """
    (
      ({image})
      OR ({gif})
      OR ({video})
    )
    """
).format(
    image=_KIND_PREDICATES["image"],
    gif=_KIND_PREDICATES["gif"],
    video=_KIND_PREDICATES["video"],
)


def _media_where_clause(kind: MediaKindFilter) -> sql.Composable:
    parts: list[sql.Composable] = [_DOWNLOADABLE_MEDIA_SQL]
    predicate = _KIND_PREDICATES.get(kind)
    if predicate is not None:
        parts.append(predicate)
    return sql.SQL(" AND ").join(parts)


def _date_range_sql(
    date_from: datetime | None,
    date_to: datetime | None,
) -> tuple[sql.Composable, tuple[Any, ...]]:
    """Peer ``date`` bounds (uses ``telegram_messages_source_date_idx``)."""
    parts: list[sql.Composable] = []
    params: list[Any] = []
    if date_from is not None:
        parts.append(sql.SQL(" AND date >= %s"))
        params.append(date_from)
    if date_to is not None:
        parts.append(sql.SQL(" AND date <= %s"))
        params.append(date_to)
    if not parts:
        return sql.SQL(""), ()
    return sql.SQL("").join(parts), tuple(params)


async def count_peer_messages(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    min_message_id: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> int:
    """Count rows matching the same filters as :func:`_fetch_messages_page`."""
    min_sql, min_params = _min_message_sql(min_message_id)
    date_sql, date_params = _date_range_sql(date_from, date_to)
    query = sql.SQL(
        """
        SELECT COUNT(*)::bigint AS total
        FROM telegram_messages
        WHERE source_external_id = %s
          {min_pred}
          {date_pred}
        """
    ).format(min_pred=min_sql, date_pred=date_sql)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id, *min_params, *date_params))
        row = await cur.fetchone()
    if not row:
        return 0
    return int(row["total"] or 0)


async def count_peer_media_messages(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    kind: MediaKindFilter = "all",
) -> int:
    where_media = _media_where_clause(kind)
    query = sql.SQL(
        """
        SELECT COUNT(*)::bigint AS total
        FROM telegram_messages
        WHERE source_external_id = %s
          AND {media_pred}
        """
    ).format(media_pred=where_media)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id,))
        row = await cur.fetchone()
    if not row:
        return 0
    return int(row["total"] or 0)


async def peer_message_id_floor(
    conn: AsyncConnection[DictRow],
    external_id: str,
    window: int,
) -> int | None:
    """Message id of the ``window``-th newest row, or None if fewer exist."""
    if window <= 0:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT message_id
            FROM telegram_messages
            WHERE source_external_id = %s
            ORDER BY message_id DESC
            OFFSET %s
            LIMIT 1
            """,
            (external_id, window - 1),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return int(row["message_id"])


async def peer_media_id_floor(
    conn: AsyncConnection[DictRow],
    external_id: str,
    window: int,
    *,
    kind: MediaKindFilter = "all",
) -> int | None:
    """Message id of the ``window``-th newest downloadable media row."""
    if window <= 0:
        return None
    media_pred = _media_where_clause(kind)
    query = sql.SQL(
        """
        SELECT message_id
        FROM telegram_messages
        WHERE source_external_id = %s
          AND {media_pred}
        ORDER BY message_id DESC
        OFFSET %s
        LIMIT 1
        """
    ).format(media_pred=media_pred)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id, window - 1))
        row = await cur.fetchone()
    if row is None:
        return None
    return int(row["message_id"])


async def list_peer_messages(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    limit: int = 50,
    before_message_id: int | None = None,
    after_message_id: int | None = None,
    min_message_id: int | None = None,
    page: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> tuple[list[TelegramMessageView], int | None, int]:
    """Return messages (newest first), next_before_message_id, and total_count.

    ``before_message_id`` pages older history.
    ``after_message_id`` returns only messages newer than that id (for live sync).
    ``min_message_id`` keeps Free users inside the newest-N window.
    ``page`` (1-based) uses OFFSET paging; incompatible with before/after cursors.
    ``date_from`` / ``date_to`` bound ``date`` (inclusive; uses source+date index).
    """
    total_count = await count_peer_messages(
        conn,
        external_id,
        min_message_id=min_message_id,
        date_from=date_from,
        date_to=date_to,
    )
    offset = None
    if page is not None:
        if page < 1:
            raise ValueError("page must be >= 1")
        if before_message_id is not None or after_message_id is not None:
            raise ValueError("page cannot be combined with before_message_id/after_message_id")
        offset = (page - 1) * limit
    rows, next_before = await _fetch_messages_page(
        conn,
        external_id,
        limit=limit,
        before_message_id=before_message_id,
        after_message_id=after_message_id,
        min_message_id=min_message_id,
        offset=offset,
        date_from=date_from,
        date_to=date_to,
    )
    if not rows:
        return [], None, total_count

    page_rows = [dict(row) for row in rows]
    await _hydrate_peer_rows(page_rows)
    message_ids = [row["message_id"] for row in page_rows]
    edits_by_message = await _fetch_edits_for_messages(conn, external_id, message_ids)
    messages = [
        TelegramMessageView.from_row(
            row,
            edits=edits_by_message.get(row["message_id"], []),
            source_external_id=external_id,
        )
        for row in page_rows
    ]
    return messages, next_before, total_count


async def locate_peer_message_page(
    conn: AsyncConnection[DictRow],
    external_id: str,
    message_id: int,
    *,
    page_size: int = 100,
    min_message_id: int | None = None,
) -> dict[str, Any] | None:
    """Locate which newest-first page contains ``message_id``.

    Returns ``{page, page_size, total_count, newer_count}`` or None if missing /
    outside the Free chat window.
    """
    if page_size < 1:
        raise ValueError("page_size must be >= 1")
    msg = await get_peer_message_by_message_id(conn, external_id, message_id)
    if msg is None:
        return None
    if min_message_id is not None and message_id < min_message_id:
        return None

    min_sql, min_params = _min_message_sql(min_message_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)::bigint AS n
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND message_id > %s
                  {min_pred}
                """
            ).format(min_pred=min_sql),
            (external_id, message_id, *min_params),
        )
        newer_row = await cur.fetchone()
        newer_count = int(newer_row["n"]) if newer_row else 0

        await cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)::bigint AS n
                FROM telegram_messages
                WHERE source_external_id = %s
                  {min_pred}
                """
            ).format(min_pred=min_sql),
            (external_id, *min_params),
        )
        total_row = await cur.fetchone()
        total_count = int(total_row["n"]) if total_row else 0

    page = (newer_count // page_size) + 1
    return {
        "page": page,
        "page_size": page_size,
        "total_count": total_count,
        "newer_count": newer_count,
        "message": msg,
    }


async def list_peer_media_messages(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    limit: int = 50,
    before_message_id: int | None = None,
    after_message_id: int | None = None,
    kind: MediaKindFilter = "all",
    min_message_id: int | None = None,
    include_total: bool = False,
    include_edits: bool = False,
) -> tuple[list[TelegramMessageView], int | None, int]:
    """Return downloadable media messages (newest first) for the Media tab grid.

    ``include_total`` / ``include_edits`` default off: the Media tab uses coverage
    totals and does not render edit history, and a full media COUNT(*) is expensive
    on large peers.
    """
    if kind not in ("all", "visual", "image", "gif", "video", "audio", "document", "webpage"):
        kind = "all"
    total_count = await count_peer_media_messages(conn, external_id, kind=kind) if include_total else 0
    rows, next_before = await _fetch_media_messages_page(
        conn,
        external_id,
        limit=limit,
        before_message_id=before_message_id,
        after_message_id=after_message_id,
        kind=kind,
        min_message_id=min_message_id,
    )
    if not rows:
        return [], None, total_count

    page = [dict(row) for row in rows]
    await _hydrate_peer_rows(page)
    edits_by_message: dict[int, list[Mapping[str, Any]]] = {}
    if include_edits:
        message_ids = [row["message_id"] for row in page]
        edits_by_message = await _fetch_edits_for_messages(conn, external_id, message_ids)
    messages = [
        TelegramMessageView.from_row(
            row,
            edits=edits_by_message.get(row["message_id"], []),
            source_external_id=external_id,
        )
        for row in page
    ]
    return messages, next_before, total_count


async def get_peer_message_by_message_id(
    conn: AsyncConnection[DictRow],
    external_id: str,
    message_id: int,
) -> TelegramMessageView | None:
    """Return one message by Telegram ``message_id`` for a peer, with edit history."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT *
            FROM telegram_messages
            WHERE source_external_id = %s
              AND message_id = %s
            LIMIT 1
            """,
            (external_id, message_id),
        )
        row = await cur.fetchone()

    if not row:
        return None

    page = [dict(row)]
    await _hydrate_peer_rows(page)
    edits_by_message = await _fetch_edits_for_messages(conn, external_id, [message_id])
    return TelegramMessageView.from_row(
        page[0],
        edits=edits_by_message.get(message_id, []),
        source_external_id=external_id,
    )


def _min_message_sql(min_message_id: int | None) -> tuple[sql.Composable, tuple[Any, ...]]:
    if min_message_id is None:
        return sql.SQL(""), ()
    return sql.SQL(" AND message_id >= %s"), (min_message_id,)


def _page_next_before(
    dict_rows: list[dict[str, Any]],
    *,
    limit: int,
    after_message_id: int | None,
    min_message_id: int | None,
) -> int | None:
    if after_message_id is not None or not dict_rows:
        return None
    return next_before_for_window(
        row_count=len(dict_rows),
        limit=limit,
        last_id=dict_rows[-1]["message_id"],
        min_message_id=min_message_id,
    )


async def _fetch_messages_page(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    limit: int = 50,
    before_message_id: int | None = None,
    after_message_id: int | None = None,
    min_message_id: int | None = None,
    offset: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    min_sql, min_params = _min_message_sql(min_message_id)
    date_sql, date_params = _date_range_sql(date_from, date_to)
    async with conn.cursor(row_factory=dict_row) as cur:
        if after_message_id is not None:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND message_id > %s
                  {min_pred}
                  {date_pred}
                ORDER BY message_id DESC
                LIMIT %s
                """
            ).format(min_pred=min_sql, date_pred=date_sql)
            params: tuple[Any, ...] = (
                external_id,
                after_message_id,
                *min_params,
                *date_params,
                limit,
            )
        elif before_message_id is not None:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND message_id < %s
                  {min_pred}
                  {date_pred}
                ORDER BY message_id DESC
                LIMIT %s
                """
            ).format(min_pred=min_sql, date_pred=date_sql)
            params = (
                external_id,
                before_message_id,
                *min_params,
                *date_params,
                limit,
            )
        elif offset is not None:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  {min_pred}
                  {date_pred}
                ORDER BY message_id DESC
                LIMIT %s OFFSET %s
                """
            ).format(min_pred=min_sql, date_pred=date_sql)
            params = (external_id, *min_params, *date_params, limit, offset)
        else:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  {min_pred}
                  {date_pred}
                ORDER BY message_id DESC
                LIMIT %s
                """
            ).format(min_pred=min_sql, date_pred=date_sql)
            params = (external_id, *min_params, *date_params, limit)
        await cur.execute(query, params)
        rows = await cur.fetchall()

    if not rows:
        return [], None

    dict_rows = [dict(row) for row in rows]
    # Offset pages do not expose a cursor; clients use page numbers.
    if offset is not None:
        return dict_rows, None
    return dict_rows, _page_next_before(
        dict_rows,
        limit=limit,
        after_message_id=after_message_id,
        min_message_id=min_message_id,
    )


async def _fetch_media_messages_page(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    limit: int = 50,
    before_message_id: int | None = None,
    after_message_id: int | None = None,
    kind: MediaKindFilter = "all",
    min_message_id: int | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    media_pred = _media_where_clause(kind)
    min_sql, min_params = _min_message_sql(min_message_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        if after_message_id is not None:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND message_id > %s
                  AND {media_pred}
                  {min_pred}
                ORDER BY message_id DESC
                LIMIT %s
                """
            ).format(media_pred=media_pred, min_pred=min_sql)
            params: tuple[Any, ...] = (external_id, after_message_id, *min_params, limit)
        elif before_message_id is not None:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND message_id < %s
                  AND {media_pred}
                  {min_pred}
                ORDER BY message_id DESC
                LIMIT %s
                """
            ).format(media_pred=media_pred, min_pred=min_sql)
            params = (external_id, before_message_id, *min_params, limit)
        else:
            query = sql.SQL(
                """
                SELECT *
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND {media_pred}
                  {min_pred}
                ORDER BY message_id DESC
                LIMIT %s
                """
            ).format(media_pred=media_pred, min_pred=min_sql)
            params = (external_id, *min_params, limit)
        await cur.execute(query, params)
        rows = await cur.fetchall()

    if not rows:
        return [], None

    dict_rows = [dict(row) for row in rows]
    return dict_rows, _page_next_before(
        dict_rows,
        limit=limit,
        after_message_id=after_message_id,
        min_message_id=min_message_id,
    )


async def _fetch_edits_for_messages(
    conn: AsyncConnection[DictRow],
    external_id: str,
    message_ids: list[int],
) -> dict[int, list[Mapping[str, Any]]]:
    if not message_ids:
        return {}

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT *
            FROM telegram_message_edits
            WHERE source_external_id = %s
              AND message_id = ANY(%s)
            ORDER BY message_id, edit_date ASC NULLS LAST
            """,
            (external_id, message_ids),
        )
        rows = await cur.fetchall()

    grouped: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        item = dict(row)
        grouped[item["message_id"]].append(item)
    return grouped


async def get_messages_for_graph(
    conn: AsyncConnection[DictRow],
    ids: list[UUID],
) -> list[dict[str, Any]]:
    if not ids:
        return []

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, message_id, source_external_id, peer_type, message_type,
                   content, from_external_id, date, out, edit_date, deleted
            FROM telegram_messages
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        rows = await cur.fetchall()

    return list(rows)
