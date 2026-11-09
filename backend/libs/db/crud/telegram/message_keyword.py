from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any
from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row

lg = logging.getLogger(__name__)

#: Keyword cards must not block on multi‑part Parquet pulls (tens of MB each).
_ARCHIVE_HYDRATE_TIMEOUT_SECS = 8.0


_WHITESPACE_RE = re.compile(r"\s+")


def _query_tokens(query: str) -> list[str]:
    return [t for t in _WHITESPACE_RE.split(query.strip()) if t]


def _like_pattern(token: str) -> str:
    escaped = token.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _keyword_where(tokens: list[str]) -> tuple["sql.Composable", list[str]]:
    """Build ``AND``-joined ``ILIKE`` clauses and their bind parameters."""
    if not tokens:
        return sql.SQL("FALSE"), []
    clauses: list[Any] = [sql.SQL("tm.content ILIKE {} ESCAPE '\\'").format(sql.Placeholder()) for _ in tokens]
    return sql.SQL(" AND ").join(clauses), [_like_pattern(t) for t in tokens]


def _date_where(
    *,
    date_from: datetime | None,
    date_to: datetime | None,
) -> tuple["sql.Composable | None", list[datetime]]:
    """Optional message date window as a SQL clause + bind params."""
    clauses: list[Any] = []
    params: list[datetime] = []
    if date_from is not None:
        clauses.append(sql.SQL("tm.date >= {}").format(sql.Placeholder()))
        params.append(date_from)
    if date_to is not None:
        clauses.append(sql.SQL("tm.date <= {}").format(sql.Placeholder()))
        params.append(date_to)
    if not clauses:
        return None, []
    return sql.SQL(" AND ").join(clauses), params


def _source_where(
    source_external_id: str | None,
) -> tuple["sql.Composable | None", list[str]]:
    """Optional peer scope filter."""
    if not source_external_id or not source_external_id.strip():
        return None, []
    return (
        sql.SQL("tm.source_external_id = {}").format(sql.Placeholder()),
        [source_external_id.strip()],
    )


def _count_query(keyword_where: "sql.Composable") -> "sql.Composed":
    return sql.SQL(
        """
        SELECT COUNT(*)::bigint AS count
        FROM telegram_messages tm
        WHERE COALESCE(TRIM(tm.content), '') <> ''
          AND {}
        """
    ).format(keyword_where)


def _select_query(keyword_where: "sql.Composable") -> "sql.Composed":
    return sql.SQL(
        """
        SELECT
          tm.id,
          tm.message_id,
          tm.source_external_id::text,
          tm.peer_type,
          tm.message_type,
          tm.content,
          tm.from_external_id::text,
          tm.date,
          tm.media,
          tm.fwd_from,
          tm.reply_to
        FROM telegram_messages tm
        WHERE COALESCE(TRIM(tm.content), '') <> ''
          AND {}
        ORDER BY tm.date DESC NULLS LAST, tm.message_id DESC
        LIMIT {} OFFSET {}
        """
    ).format(keyword_where, sql.Placeholder(), sql.Placeholder())


async def hydrate_keyword_messages_by_keys(
    conn: AsyncConnection[DictRow],
    keys: list[tuple[str, str]],
    *,
    archive_hydrate: bool = True,
) -> dict[str, dict[str, Any]]:
    """Load full keyword rows for ``(id, date)`` pairs (for example Quickwit hits).

    Matching on both ``id`` and ``date`` lets Postgres prune to the relevant monthly partition instead of
    probing every partition's PK index. Returns a mapping ``str(id) -> row`` so the caller can re-order the
    result to match the search ranking. ``date`` strings are cast to ``timestamptz``; they round-trip from
    the same Postgres rows, so equality holds.

    Set ``archive_hydrate=False`` on the interactive keyword path: pulling cold Parquet parts
    (tens of MB each) can stall the request for minutes. Callers that skip archive should fill
    missing ``content`` from the search index hit.
    """
    if not keys:
        return {}

    values = sql.SQL(", ").join(
        sql.SQL("({}::uuid, {}::timestamptz)").format(sql.Placeholder(), sql.Placeholder()) for _ in keys
    )
    query = sql.SQL(
        """
        SELECT
          tm.id,
          tm.message_id,
          tm.source_external_id::text AS source_external_id,
          tm.peer_type,
          tm.message_type,
          tm.content,
          tm.from_external_id::text AS from_external_id,
          tm.date,
          tm.media,
          tm.fwd_from,
          tm.reply_to,
          tm.archive_key,
          tm.archive_row,
          tm.deleted,
          tm.edit_date
        FROM telegram_messages tm
        JOIN (VALUES {}) AS k(id, date) ON tm.id = k.id AND tm.date = k.date
        """
    ).format(values)
    params: list[str] = [value for key in keys for value in key]

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()

    out_rows = [dict(row) for row in rows]
    if not archive_hydrate:
        return {str(row["id"]): row for row in out_rows}

    try:
        from msgstore import (
            get_message_archive_prefix,
            get_message_archive_sink,
            hydrate_message_rows_async,
        )

        await asyncio.wait_for(
            hydrate_message_rows_async(
                out_rows,
                get_message_archive_sink(),
                prefix=get_message_archive_prefix(),
            ),
            timeout=_ARCHIVE_HYDRATE_TIMEOUT_SECS,
        )
    except TimeoutError:
        lg.warning(
            "keyword archive hydrate timed out after %.1fs (%d row(s)); returning PG fields",
            _ARCHIVE_HYDRATE_TIMEOUT_SECS,
            len(out_rows),
        )
    except Exception:
        # Hydrate is best-effort; search cards still return PG fields.
        lg.warning("keyword archive hydrate failed; returning PG fields", exc_info=True)

    return {str(row["id"]): row for row in out_rows}


async def search_messages_keyword(
    conn: AsyncConnection[DictRow],
    query: str,
    *,
    limit: int = 10,
    offset: int = 0,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    source_external_id: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return message rows and total count matching all whitespace-separated terms in content."""
    tokens = _query_tokens(query)
    keyword_where, filter_params = _keyword_where(tokens)
    date_where, date_params = _date_where(date_from=date_from, date_to=date_to)
    if date_where is not None:
        keyword_where = sql.SQL("({}) AND ({})").format(keyword_where, date_where)
        filter_params = [*filter_params, *date_params]
    source_where, source_params = _source_where(source_external_id)
    if source_where is not None:
        keyword_where = sql.SQL("({}) AND ({})").format(keyword_where, source_where)
        filter_params = [*filter_params, *source_params]
    if not tokens:
        return [], 0

    count_query = _count_query(keyword_where)
    select_query = _select_query(keyword_where)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(count_query, filter_params)
        count_row = await cur.fetchone()
        total = int(count_row["count"]) if count_row else 0

        await cur.execute(select_query, (*filter_params, limit, offset))
        rows = await cur.fetchall()

    return list(rows), total
