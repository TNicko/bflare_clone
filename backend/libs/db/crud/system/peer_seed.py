"""Queue a background seed when a community peer is inserted.

New peers land in ``peer_seed_queue``. Nothing here starts a harvest job.
A running channel-recommendations process seeds them on the Telegram sessions
it already has open. A ``peer_seed`` job starts only when that job selects
harvesters that are not already on a job, and that process drains the peers
still waiting.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Iterator
from uuid import UUID

from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

SEED_MESSAGE_LIMIT = 100

_COMMUNITY_PEER_TYPES = frozenset({"channel", "megagroup", "chat"})
_CLAIM_LIMIT = 20


@dataclass(frozen=True, slots=True)
class PeerSeedLineage:
    """Root and depth for peers inserted while a seed job is running."""

    root_external_id: str
    depth: int


_lineage: ContextVar[PeerSeedLineage | None] = ContextVar("peer_seed_lineage", default=None)


@contextmanager
def bind_peer_seed_lineage(root_external_id: str, depth: int) -> Iterator[None]:
    """New peers inserted inside the block enqueue as children of ``root``."""
    token: Token[PeerSeedLineage | None] = _lineage.set(PeerSeedLineage(root_external_id=root_external_id, depth=depth))
    try:
        yield
    finally:
        _lineage.reset(token)


@dataclass(frozen=True, slots=True)
class PeerSeedQueueRow:
    external_id: str
    peer_type: str
    root_external_id: str
    depth: int
    attempt_count: int


def seed_enqueue_decision(
    *,
    peer_type: str,
    already_seeded: bool,
) -> str | None:
    """Return a skip reason, or None when the peer should join the drain queue.

    The chain keeps going for every newly inserted community. A peer stops
    when it is already seeded, or when it is not a channel, megagroup, or chat.
    """
    if peer_type not in _COMMUNITY_PEER_TYPES:
        return "not a community"
    if already_seeded:
        return "already seeded"
    return None


def queue_status_action(existing: str | None) -> str | None:
    """Return ``insert``, ``requeue``, or None when the queue row should stay."""
    if existing is None:
        return "insert"
    if existing == "failed":
        return "requeue"
    return None


def should_start_peer_seed_job(*, job_active: bool, has_pending: bool) -> bool:
    """Inserting a peer never starts a harvest job.

    Channel recommendations seeds on the sessions it already holds. A
    ``peer_seed`` job starts by selecting harvesters. ``job_active`` and ``has_pending``
    stay in the signature so callers can pass the queue state they already know.
    """
    del job_active, has_pending
    return False


def _lineage_for(
    external_id: str,
    *,
    root_external_id: str | None,
    depth: int | None,
) -> tuple[str, int]:
    current = _lineage.get()
    if root_external_id is None:
        root_external_id = current.root_external_id if current is not None else external_id
    if depth is None:
        depth = current.depth if current is not None else 0
    return root_external_id, depth


async def enqueue_peer_seed(
    conn: Any,
    *,
    external_id: str,
    peer_type: str,
    root_external_id: str | None = None,
    depth: int | None = None,
    retry: bool = False,
    actor_type: str = "system",
    actor_id: str = "peer_seed",
    source: str = "peer_insert",
) -> str | None:
    """Add one peer to the drain queue.

    Returns None. The row waits for a channel-recommendations process that
    already has a session, or for a peer_seed job started with selected harvesters.
    """
    del actor_type, actor_id, source
    eid = str(external_id).strip()
    if not eid:
        return None
    root, seed_depth = _lineage_for(eid, root_external_id=root_external_id, depth=depth)
    root = root.strip() or eid

    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s::text), hashtext(%s::text))",
                ("peer_seed", eid),
            )
            await cur.execute(
                "SELECT status FROM peer_seed_queue WHERE external_id = %s",
                (eid,),
            )
            existing = await cur.fetchone()
        existing_status = str(existing["status"]) if existing else None
        if retry and existing_status not in (None, "succeeded"):
            action: str | None = "requeue"
        else:
            action = queue_status_action(existing_status)
        reason = seed_enqueue_decision(
            peer_type=peer_type,
            already_seeded=action is None and existing_status == "succeeded",
        )
        if reason == "not a community":
            logger.info("peer seed skipped external_id=%s peer_type=%s reason=%s", eid, peer_type, reason)
            return None
        if action == "insert":
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO peer_seed_queue (
                      external_id, peer_type, root_external_id, depth, status
                    )
                    VALUES (%s, %s, %s, %s, 'pending')
                    """,
                    (eid, peer_type, root, seed_depth),
                )
        elif action == "requeue":
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE peer_seed_queue
                    SET status = 'pending',
                        peer_type = %s,
                        root_external_id = %s,
                        depth = %s,
                        not_before = NULL,
                        last_error = NULL,
                        updated_at = now()
                    WHERE external_id = %s
                    """,
                    (peer_type, root, seed_depth, eid),
                )
        elif existing_status == "succeeded":
            logger.info("peer seed skipped external_id=%s reason=already seeded", eid)
        else:
            logger.info(
                "peer seed already waiting external_id=%s status=%s",
                eid,
                existing_status,
            )
    return None


async def release_running_peer_seeds(conn: Any, external_ids: list[str]) -> None:
    """Put abandoned claims back on the queue after a batch."""
    if not external_ids:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE peer_seed_queue
            SET status = 'pending', updated_at = now()
            WHERE status = 'running'
              AND external_id = ANY(%s)
            """,
            (external_ids,),
        )


async def reset_running_peer_seeds(conn: Any) -> None:
    """Return claims left behind by a dead drain to the queue."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE peer_seed_queue
            SET status = 'pending', updated_at = now()
            WHERE status = 'running'
            """
        )


async def claim_peer_seed_batch(conn: Any, *, limit: int = _CLAIM_LIMIT) -> list[PeerSeedQueueRow]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE peer_seed_queue AS q
            SET status = 'running', updated_at = now()
            WHERE q.external_id IN (
              SELECT external_id
              FROM peer_seed_queue
              WHERE status = 'pending'
                AND (not_before IS NULL OR not_before <= now())
              ORDER BY created_at
              FOR UPDATE SKIP LOCKED
              LIMIT %s
            )
            RETURNING external_id, peer_type, root_external_id, depth, attempt_count
            """,
            (max(1, limit),),
        )
        rows = await cur.fetchall()
    return [_queue_row(row) for row in rows]


async def claim_peer_seed_batch_for_harvester(
    conn: Any,
    *,
    harvester_id: UUID,
    exclude_external_ids: list[str],
    limit: int = _CLAIM_LIMIT,
) -> list[PeerSeedQueueRow]:
    """Claim peers this harvester should open.

    A peer with a usable parent role is claimed only by that harvester. A peer
    with no usable role on any harvester can be claimed by whoever is connected.
    Peers owned by some other account stay pending for the job that has it.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE peer_seed_queue AS q
            SET status = 'running', updated_at = now()
            WHERE q.external_id IN (
              SELECT q2.external_id
              FROM peer_seed_queue q2
              WHERE q2.status = 'pending'
                AND (q2.not_before IS NULL OR q2.not_before <= now())
                AND NOT (q2.external_id = ANY(%s::text[]))
                AND (
                  EXISTS (
                    SELECT 1
                    FROM telegram_harvester_peer_roles r
                    WHERE r.external_id = q2.external_id
                      AND r.harvester_id = %s
                      AND r.role = 'parent'
                      AND r.access_error_code IS NULL
                      AND (q2.peer_type = 'chat' OR r.access_hash IS NOT NULL)
                  )
                  OR NOT EXISTS (
                    SELECT 1
                    FROM telegram_harvester_peer_roles r
                    INNER JOIN telegram_harvesters h
                      ON h.id = r.harvester_id
                     AND h.is_active = true
                    WHERE r.external_id = q2.external_id
                      AND r.role = 'parent'
                      AND r.access_error_code IS NULL
                      AND (q2.peer_type = 'chat' OR r.access_hash IS NOT NULL)
                  )
                )
              ORDER BY q2.created_at
              FOR UPDATE OF q2 SKIP LOCKED
              LIMIT %s
            )
            RETURNING external_id, peer_type, root_external_id, depth, attempt_count
            """,
            (exclude_external_ids, harvester_id, max(1, limit)),
        )
        rows = await cur.fetchall()
    return [_queue_row(row) for row in rows]


async def peer_seed_wait_seconds(conn: Any) -> float | None:
    """Seconds until the next pending peer is ready.

    ``None`` means the queue has no pending rows. ``0`` means a row is ready now.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              COUNT(*) FILTER (
                WHERE not_before IS NULL OR not_before <= now()
              )::int AS ready,
              COUNT(*)::int AS pending,
              EXTRACT(EPOCH FROM (MIN(not_before) FILTER (WHERE not_before > now()) - now())) AS wait_s
            FROM peer_seed_queue
            WHERE status = 'pending'
            """
        )
        row = await cur.fetchone()
    if row is None or int(row["pending"] or 0) == 0:
        return None
    if int(row["ready"] or 0) > 0:
        return 0.0
    wait_s = row.get("wait_s")
    if wait_s is None:
        return 0.0
    return max(0.0, float(wait_s))


async def defer_peer_seed(
    conn: Any,
    *,
    external_id: str,
    seconds: float,
    error: str,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE peer_seed_queue
            SET status = 'pending',
                not_before = now() + make_interval(secs => %s),
                last_error = %s,
                updated_at = now()
            WHERE external_id = %s
            """,
            (max(0.0, float(seconds)), error[:500], external_id),
        )


async def finish_peer_seed(
    conn: Any,
    *,
    external_id: str,
    status: str,
    error: str | None = None,
    attempt_count: int = 0,
) -> None:
    """Mark a peer finished. A failed peer with attempts left goes back to pending."""
    final = status
    not_before_sql = "NULL"
    params: list[Any] = [error[:500] if error else None, external_id]
    if status == "failed" and attempt_count + 1 < 3:
        final = "pending"
        not_before_sql = "now() + interval '30 seconds'"
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE peer_seed_queue
            SET status = %s,
                last_error = %s,
                not_before = {not_before_sql},
                attempt_count = attempt_count + CASE WHEN %s = 'failed' THEN 1 ELSE 0 END,
                updated_at = now()
            WHERE external_id = %s
            """,
            (final, params[0], status, params[1]),
        )


def _queue_row(row: dict[str, Any]) -> PeerSeedQueueRow:
    root = str(row.get("root_external_id") or row["external_id"])
    return PeerSeedQueueRow(
        external_id=str(row["external_id"]),
        peer_type=str(row["peer_type"]),
        root_external_id=root,
        depth=int(row.get("depth") or 0),
        attempt_count=int(row.get("attempt_count") or 0),
    )


async def enqueue_peer_seed_quiet(
    conn: Any,
    *,
    external_id: str,
    peer_type: str,
    root_external_id: str | None = None,
    depth: int | None = None,
) -> None:
    """Enqueue a seed without failing the caller that just stored the peer."""
    try:
        await enqueue_peer_seed(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            root_external_id=root_external_id,
            depth=depth,
        )
    except Exception:
        logger.exception(
            "peer seed enqueue failed external_id=%s peer_type=%s",
            external_id,
            peer_type,
        )
