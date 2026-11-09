"""CRUD for ``telegram_search_query_runs`` (MTProto contacts.search dedup)."""

from __future__ import annotations

import json
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

INSERT_SEARCH_QUERY_RUN_SQL = """
INSERT INTO telegram_search_query_runs (
  query_text,
  harvester_id,
  status,
  started_at
)
VALUES (%s, %s, 'running', now())
RETURNING id
"""

MARK_SEARCH_QUERY_RUN_SUCCEEDED_SQL = """
UPDATE telegram_search_query_runs
SET
  status = 'succeeded',
  finished_at = now(),
  result_count = %s,
  peers_seen = %s,
  peers_persisted = %s,
  peers_skipped = %s,
  peers_found = %s::jsonb,
  error = NULL,
  updated_at = now()
WHERE id = %s
"""

MARK_SEARCH_QUERY_RUN_FAILED_SQL = """
UPDATE telegram_search_query_runs
SET
  status = 'failed',
  finished_at = now(),
  error = %s,
  updated_at = now()
WHERE id = %s
"""

FETCH_SKIP_TELEGRAM_SEARCH_QUERIES_SQL = """
SELECT DISTINCT query_text
FROM telegram_search_query_runs
WHERE query_text = ANY(%s)
  AND (
    status = 'succeeded'
    OR (
      status = 'running'
      AND started_at > now() - interval '6 hours'
    )
  )
"""

FETCH_LATEST_SUCCEEDED_SEARCH_QUERY_PEERS_SQL = """
SELECT peers_found
FROM telegram_search_query_runs
WHERE query_text = %s
  AND status = 'succeeded'
ORDER BY finished_at DESC NULLS LAST, started_at DESC
LIMIT 1
"""


async def insert_search_query_run(
    conn: AsyncConnection,
    *,
    query_text: str,
    harvester_id: UUID,
) -> UUID:
    """Insert a ``running`` row and return its id."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            INSERT_SEARCH_QUERY_RUN_SQL,
            (query_text, harvester_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise RuntimeError("INSERT telegram_search_query_runs did not return id")
    return row["id"]


async def mark_search_query_run_succeeded(
    conn: AsyncConnection,
    run_id: UUID,
    *,
    result_count: int,
    peers_seen: int,
    peers_persisted: int,
    peers_skipped: int,
    peers_found: list[dict[str, object]],
) -> None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            MARK_SEARCH_QUERY_RUN_SUCCEEDED_SQL,
            (
                result_count,
                peers_seen,
                peers_persisted,
                peers_skipped,
                json.dumps(peers_found),
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
    """Return query texts that already succeeded or are actively running."""
    if not queries:
        return set()
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(FETCH_SKIP_TELEGRAM_SEARCH_QUERIES_SQL, (queries,))
        rows = await cur.fetchall()
    return {str(row["query_text"]) for row in rows}


async def fetch_latest_succeeded_search_query_peers(
    conn: AsyncConnection,
    query_text: str,
) -> list[dict[str, object]] | None:
    """Return ``peers_found`` from the latest succeeded run, or ``None`` if none exists."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(FETCH_LATEST_SUCCEEDED_SEARCH_QUERY_PEERS_SQL, (query_text,))
        row = await cur.fetchone()
    if row is None:
        return None
    raw = row.get("peers_found")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []
