"""Queue CRUD for ``telegram_search_ingest``.

The fleet job claims one row at a time so several harvesters can drain the
table together. A claim older than two hours is free again after a crash.
Callers delete a row once the query is skipped or the search has been recorded,
and clear ``claimed_at`` when the search never started or hit FloodWait.
"""

from __future__ import annotations

from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

COUNT_SEARCH_INGEST_SQL = """
SELECT COUNT(*) AS n
FROM telegram_search_ingest
"""

CLAIM_SEARCH_INGEST_SQL = """
UPDATE telegram_search_ingest AS q
SET claimed_at = now()
WHERE q.id IN (
  SELECT id
  FROM telegram_search_ingest
  WHERE claimed_at IS NULL
     OR claimed_at < now() - interval '2 hours'
  ORDER BY created_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING id, query_text
"""

DELETE_SEARCH_INGEST_SQL = """
DELETE FROM telegram_search_ingest
WHERE id = %s
"""

RELEASE_SEARCH_INGEST_SQL = """
UPDATE telegram_search_ingest
SET claimed_at = NULL
WHERE id = %s
"""


async def count_search_ingest(conn: AsyncConnection) -> int:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(COUNT_SEARCH_INGEST_SQL)
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


async def claim_search_ingest_query(conn: AsyncConnection) -> dict[str, object] | None:
    """Claim the oldest waiting query. ``None`` when the queue is empty."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(CLAIM_SEARCH_INGEST_SQL)
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        return None
    return {"id": row["id"], "query_text": str(row["query_text"])}


async def delete_search_ingest(conn: AsyncConnection, ingest_id: UUID) -> None:
    async with conn.cursor() as cur:
        await cur.execute(DELETE_SEARCH_INGEST_SQL, (ingest_id,))
        await conn.commit()


async def release_search_ingest(conn: AsyncConnection, ingest_id: UUID) -> None:
    """Put a claimed query back so another harvester can take it."""
    async with conn.cursor() as cur:
        await cur.execute(RELEASE_SEARCH_INGEST_SQL, (ingest_id,))
        await conn.commit()
