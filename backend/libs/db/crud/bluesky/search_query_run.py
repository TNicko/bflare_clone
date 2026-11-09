"""CRUD for ``bluesky_search_query_runs`` (App View searchActors dedup / resume)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

INSERT_SEARCH_QUERY_RUN_SQL = """
INSERT INTO bluesky_search_query_runs (
  query_text,
  status,
  started_at,
  source,
  next_cursor,
  pages_done
)
VALUES (%s, 'running', now(), %s, %s, %s)
RETURNING id
"""

CHECKPOINT_SEARCH_QUERY_RUN_SQL = """
UPDATE bluesky_search_query_runs
SET
  status = 'running',
  finished_at = NULL,
  result_count = %s,
  actors_seen = %s,
  actors_persisted = %s,
  actors_skipped = %s,
  next_cursor = %s,
  pages_done = %s,
  error = NULL,
  updated_at = now()
WHERE id = %s
"""

CHECKPOINT_SEARCH_QUERY_RUN_CURSOR_SQL = """
UPDATE bluesky_search_query_runs
SET
  status = 'running',
  finished_at = NULL,
  next_cursor = %s,
  pages_done = %s,
  error = NULL,
  updated_at = now()
WHERE id = %s
"""

MARK_SEARCH_QUERY_RUN_SUCCEEDED_SQL = """
UPDATE bluesky_search_query_runs
SET
  status = 'succeeded',
  finished_at = now(),
  result_count = %s,
  actors_seen = %s,
  actors_persisted = %s,
  actors_skipped = %s,
  next_cursor = NULL,
  pages_done = %s,
  error = NULL,
  updated_at = now()
WHERE id = %s
"""

MARK_SEARCH_QUERY_RUN_FAILED_SQL = """
UPDATE bluesky_search_query_runs
SET
  status = 'failed',
  finished_at = now(),
  error = %s,
  updated_at = now()
WHERE id = %s
"""

FETCH_SKIP_BLUESKY_SEARCH_QUERIES_SQL = """
SELECT DISTINCT query_text
FROM bluesky_search_query_runs
WHERE query_text = ANY(%s)
  AND (
    status = 'succeeded'
    OR (
      status = 'running'
      AND next_cursor IS NULL
      AND pages_done = 0
      AND started_at > now() - interval '6 hours'
    )
  )
"""

FETCH_RESUMABLE_SEARCH_QUERY_RUN_SQL = """
SELECT
  id,
  query_text,
  status,
  next_cursor,
  pages_done,
  actors_seen,
  actors_persisted,
  actors_skipped
FROM bluesky_search_query_runs
WHERE query_text = %s
  AND (
    (
      status = 'running'
      AND (next_cursor IS NOT NULL OR pages_done > 0)
    )
    OR (
      status = 'failed'
      AND next_cursor IS NOT NULL
    )
  )
ORDER BY updated_at DESC NULLS LAST, started_at DESC
LIMIT 1
"""

LIST_DISTINCT_TELEGRAM_SEARCH_QUERIES_SQL = """
SELECT DISTINCT query_text
FROM telegram_search_query_runs
WHERE query_text IS NOT NULL
  AND btrim(query_text) <> ''
ORDER BY query_text
"""


async def insert_search_query_run(
    conn: AsyncConnection,
    *,
    query_text: str,
    source: str = "appview",
    next_cursor: str | None = None,
    pages_done: int = 0,
) -> UUID:
    """Insert a ``running`` row and return its id."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            INSERT_SEARCH_QUERY_RUN_SQL,
            (query_text, source, next_cursor, pages_done),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise RuntimeError("INSERT bluesky_search_query_runs did not return id")
    return row["id"]


async def checkpoint_search_query_run(
    conn: AsyncConnection,
    run_id: UUID,
    *,
    result_count: int,
    actors_seen: int,
    actors_persisted: int,
    actors_skipped: int,
    next_cursor: str | None,
    pages_done: int,
) -> None:
    """Persist mid-run progress (cursor + counts) so Ctrl-C / crash can resume."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            CHECKPOINT_SEARCH_QUERY_RUN_SQL,
            (
                result_count,
                actors_seen,
                actors_persisted,
                actors_skipped,
                next_cursor,
                pages_done,
                run_id,
            ),
        )
        await conn.commit()


async def checkpoint_search_query_run_cursor(
    conn: AsyncConnection,
    run_id: UUID,
    *,
    next_cursor: str | None,
    pages_done: int,
) -> None:
    """Save resume cursor without rewriting counts (incomplete page)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            CHECKPOINT_SEARCH_QUERY_RUN_CURSOR_SQL,
            (next_cursor, pages_done, run_id),
        )
        await conn.commit()


async def mark_search_query_run_succeeded(
    conn: AsyncConnection,
    run_id: UUID,
    *,
    result_count: int,
    actors_seen: int,
    actors_persisted: int,
    actors_skipped: int,
    pages_done: int = 0,
) -> None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            MARK_SEARCH_QUERY_RUN_SUCCEEDED_SQL,
            (
                result_count,
                actors_seen,
                actors_persisted,
                actors_skipped,
                pages_done,
                run_id,
            ),
        )
        await conn.commit()


async def mark_search_query_run_failed(
    conn: AsyncConnection,
    run_id: UUID,
    error: str,
) -> None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(MARK_SEARCH_QUERY_RUN_FAILED_SQL, (error[:4000], run_id))
        await conn.commit()


async def fetch_queries_to_skip(
    conn: AsyncConnection,
    queries: list[str],
) -> set[str]:
    """Return query texts that already succeeded or are locked as freshly running.

    Resumable runs (``running`` with a cursor / pages_done) are *not* skipped.
    """
    if not queries:
        return set()
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(FETCH_SKIP_BLUESKY_SEARCH_QUERIES_SQL, (queries,))
        rows = await cur.fetchall()
    return {str(row["query_text"]) for row in rows}


async def fetch_resumable_search_query_run(
    conn: AsyncConnection,
    query_text: str,
) -> dict[str, Any] | None:
    """Latest incomplete run for ``query_text`` that can continue from a cursor."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(FETCH_RESUMABLE_SEARCH_QUERY_RUN_SQL, (query_text,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def list_distinct_telegram_search_queries(
    conn: AsyncConnection,
) -> list[str]:
    """Distinct ``query_text`` values from ``telegram_search_query_runs``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(LIST_DISTINCT_TELEGRAM_SEARCH_QUERIES_SQL)
        rows = await cur.fetchall()
    return [str(row["query_text"]).strip() for row in rows if str(row["query_text"]).strip()]
