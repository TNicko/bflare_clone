"""Drain the Postgres ``search_outbox`` into configured keyword index sinks.

Each pass runs inside ONE Postgres transaction:

  1. Claim a batch of outbox rows with ``FOR UPDATE SKIP LOCKED`` (so multiple workers are safe).
  2. Coalesce by ``entity_id`` (the latest event per message wins) - collapses edit storms / re-upserts.
  3. Hydrate the current row state from Postgres by ``(id, date)`` (partition-pruned) and build docs;
     an ``upsert`` whose row is already gone is downgraded to a ``delete``.
  4. Apply to Quickwit, then
  5. ``DELETE`` the claimed outbox rows, then commit.

Because the transaction wraps the sink apply, a crash before commit leaves the rows in place to be
reprocessed - i.e. at-least-once delivery, made safe by Quickwit delete-then-ingest.
Postgres reads are SELECT-only on ``telegram_messages``.
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from db.connection import connect
from quickwit.client import (
    QuickwitClient,
    bulk_delete_messages as qw_bulk_delete,
    bulk_index_messages as qw_bulk_index,
    ensure_index as qw_ensure_index,
    get_client as qw_get_client,
    message_row_to_doc,
)

from .config import Config

log = logging.getLogger(__name__)

#: Outbox entities this worker is responsible for.
_SUPPORTED_ENTITIES = ("message",)

_CLAIM_SQL = """
    SELECT id, entity_id, entity_date, op
    FROM search_outbox
    WHERE entity = ANY(%s)
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT %s
"""

_HYDRATE_SQL = """
    SELECT
      tm.id,
      tm.message_id,
      tm.source_external_id::text     AS source_external_id,
      tm.peer_type,
      tm.message_type,
      tm.content,
      tm.from_external_id::text       AS from_external_id,
      tm.harvester_external_id::text  AS harvester_external_id,
      tm.date,
      tm.views
    FROM telegram_messages tm
    JOIN (VALUES {values}) AS k(id, date)
      ON tm.id = k.id AND tm.date = k.date
"""


class IndexSinks:
    """Open handle for the Quickwit keyword index."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.quickwit: QuickwitClient | None = None

    def open(self) -> IndexSinks:
        self.quickwit = qw_get_client(self.cfg.quickwit_url)
        qw_ensure_index(self.quickwit)
        log.info("indexer sink ready: quickwit")
        return self

    def close(self) -> None:
        if self.quickwit is not None:
            try:
                self.quickwit.close()
            except Exception:
                pass
            self.quickwit = None


def _hydrate_docs(
    conn: psycopg.Connection[dict[str, Any]],
    keys: list[tuple[str, str]],
) -> dict[str, dict[str, Any]]:
    """Load current message rows for ``(id, date)`` keys; returns ``{str(id): doc}``."""
    if not keys:
        return {}
    values = ", ".join("(%s::uuid, %s::timestamptz)" for _ in keys)
    params: list[str] = [value for key in keys for value in key]
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_HYDRATE_SQL.format(values=values), params)
        rows = cur.fetchall()
    return {str(row["id"]): message_row_to_doc(row) for row in rows}


def _iso(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _close_quietly(conn: psycopg.Connection[Any] | None) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def _ensure_conn(
    conn: psycopg.Connection[dict[str, Any]] | None,
    dsn: str,
) -> psycopg.Connection[dict[str, Any]]:
    """Return an open connection, reconnecting if the previous one was dropped."""
    if conn is not None and not conn.closed:
        return conn
    _close_quietly(conn)
    return connect(dsn)


def _apply_sinks(
    sinks: IndexSinks,
    docs: list[dict[str, Any]],
    delete_ids: set[str],
    cfg: Config,
) -> tuple[int, int]:
    """Write the batch to every configured sink. Returns ``(indexed, deleted)`` counts (max across sinks)."""
    indexed = 0
    deleted = 0

    if sinks.quickwit is not None:
        # Delete-then-ingest so edits replace prior docs (delete-tasks apply to splits
        # created before the task; the following ingest is kept).
        qw_delete_ids = set(delete_ids) | {str(d["id"]) for d in docs}
        if qw_delete_ids:
            n, delete_errors = qw_bulk_delete(sinks.quickwit, qw_delete_ids)
            if delete_errors:
                raise RuntimeError(f"quickwit: {len(delete_errors)} delete error(s); first: {delete_errors[0]}")
            deleted = max(deleted, n)
        if docs:
            commit = "force" if cfg.refresh_after_batch else "auto"
            n, index_errors = qw_bulk_index(sinks.quickwit, docs, commit=commit)
            if index_errors:
                raise RuntimeError(f"quickwit: {len(index_errors)} index error(s); first: {index_errors[0]}")
            indexed = max(indexed, n)

    return indexed, deleted


def run_once(
    conn: psycopg.Connection[dict[str, Any]],
    sinks: IndexSinks,
    cfg: Config,
) -> int:
    """Process one batch. Returns the number of outbox events consumed (0 means the queue was empty)."""
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(_CLAIM_SQL, (list(_SUPPORTED_ENTITIES), cfg.batch_size))
            claimed = cur.fetchall()

        if not claimed:
            return 0

        claimed_ids = [row["id"] for row in claimed]

        # Coalesce: latest event per message wins (claimed is ordered by ascending id).
        final_op: dict[str, str] = {}
        final_date: dict[str, str] = {}
        for row in claimed:
            eid = str(row["entity_id"])
            final_op[eid] = row["op"]
            final_date[eid] = _iso(row["entity_date"])

        upsert_keys = [(eid, final_date[eid]) for eid, op in final_op.items() if op == "upsert"]
        delete_ids = {eid for eid, op in final_op.items() if op == "delete"}

        docs_by_id = _hydrate_docs(conn, upsert_keys)

        # An upsert whose row no longer exists (deleted between enqueue and now) becomes a delete.
        for eid, _date in upsert_keys:
            if eid not in docs_by_id:
                delete_ids.add(eid)

        docs = list(docs_by_id.values())
        indexed, deleted = _apply_sinks(sinks, docs, delete_ids, cfg)

        with conn.cursor() as cur:
            cur.execute("DELETE FROM search_outbox WHERE id = ANY(%s)", (claimed_ids,))

        log.info(
            "batch: events=%d coalesced=%d indexed=%d deleted=%d sinks=%s",
            len(claimed),
            len(final_op),
            indexed,
            deleted,
            ",".join(cfg.index_sinks),
        )
        return len(claimed)


def run_drain(cfg: Config) -> int:
    """Process batches until the outbox is empty, then return. Returns total events consumed."""
    cfg.validate_required()
    sinks = IndexSinks(cfg).open()

    total = 0
    conn: psycopg.Connection[dict[str, Any]] | None = None
    try:
        while True:
            try:
                conn = _ensure_conn(conn, cfg.postgres_dsn)
                n = run_once(conn, sinks, cfg)
                if n == 0:
                    break
                total += n
            except (psycopg.OperationalError, psycopg.InterfaceError):
                # Pooler/idle/network drop mid-batch: txn rolled back; outbox rows remain (at-least-once).
                log.exception("postgres connection lost during drain; reconnecting")
                _close_quietly(conn)
                conn = None
                time.sleep(cfg.poll_interval_seconds)
    finally:
        _close_quietly(conn)
        sinks.close()
    log.info("drain complete: %d events consumed", total)
    return total


def run_loop(cfg: Config) -> None:
    """Continuously drain, sleeping ``poll_interval_seconds`` whenever the queue is empty.

    Connections are not held across idle polls (Supabase poolers drop idle SSL sockets).
    Mid-batch disconnects reconnect and retry; uncommitted outbox claims stay queued.
    """
    cfg.validate_required()
    sinks = IndexSinks(cfg).open()

    stopping = {"flag": False}

    def _stop(signum: int, _frame: Any) -> None:
        log.info("received signal %s; finishing current batch then exiting", signum)
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info(
        "indexer loop started (batch=%d poll=%.1fs sinks=%s)",
        cfg.batch_size,
        cfg.poll_interval_seconds,
        ",".join(cfg.index_sinks),
    )
    conn: psycopg.Connection[dict[str, Any]] | None = None
    try:
        while not stopping["flag"]:
            try:
                conn = _ensure_conn(conn, cfg.postgres_dsn)
                n = run_once(conn, sinks, cfg)
                if n == 0:
                    # Avoid idle-timeout kills on the pooler between polls.
                    _close_quietly(conn)
                    conn = None
                    time.sleep(cfg.poll_interval_seconds)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                log.exception("postgres connection lost; reconnecting")
                _close_quietly(conn)
                conn = None
                if not stopping["flag"]:
                    time.sleep(cfg.poll_interval_seconds)
    finally:
        _close_quietly(conn)
        sinks.close()
    log.info("indexer loop stopped")


def outbox_status(cfg: Config) -> dict[str, Any]:
    """Return queue depth + age of the oldest pending event (for monitoring/CLI)."""
    cfg.validate_required()
    with connect(cfg.postgres_dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)::bigint                                            AS depth,
                  COUNT(*) FILTER (WHERE op = 'upsert')::bigint               AS upserts,
                  COUNT(*) FILTER (WHERE op = 'delete')::bigint               AS deletes,
                  EXTRACT(EPOCH FROM (now() - MIN(enqueued_at)))::double precision AS oldest_age_seconds
                FROM search_outbox
                WHERE entity = ANY(%s)
                """,
                (list(_SUPPORTED_ENTITIES),),
            )
            row = cur.fetchone()
    return dict(row) if row else {"depth": 0, "upserts": 0, "deletes": 0, "oldest_age_seconds": None}
