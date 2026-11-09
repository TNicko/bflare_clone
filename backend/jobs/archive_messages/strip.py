"""Phase 3c: NULL fat columns on archived cold telegram_messages (keep skinny index).

CRITICAL: clearing ``content`` would fire search_outbox + edit-history triggers and
enqueue/copy tens of millions of rows. Strip disables those triggers for the run.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterator
from uuid import UUID

from db.connection import connect
from msgstore.layout import parse_month

from .archive import list_cold_partition_months, month_has_backlog
from .config import Config, hot_cutoff_month_start, month_bounds

lg = logging.getLogger(__name__)

#: Larger than the old 500: strip sets statement_timeout=0 and is restartable.
_STRIP_CHUNK = 2_000

#: Sentinel for keyset start: every real (date, id) in the month is greater.
_CURSOR_ID_MIN = UUID(int=0)

#: Prefer ``content IS NOT NULL`` as the strip cursor (cheap). Media-only leftovers
#: are cleared in the same UPDATE SET list once a row is selected.
#: Keep skinny ``{"harvest": ...}`` on cold rows; TL media stays in Parquet.
_MEDIA_TO_HARVEST_STUB = """
  CASE
    WHEN media ? 'harvest'
         AND jsonb_typeof(media->'harvest') = 'object'
         AND media->'harvest' <> '{}'::jsonb
    THEN jsonb_build_object('harvest', media->'harvest')
    ELSE NULL
  END
"""

#: Keyset ``(date, id) > (%s, %s)`` avoids re-scanning already-stripped prefix rows
#: every chunk (which made late-month progress O(n²) and multi-day ETAs).
_STRIP_SQL = f"""
WITH batch AS (
  SELECT id, date
  FROM telegram_messages
  WHERE date >= %s
    AND date < %s
    AND (date, id) > (%s, %s)
    AND archive_key IS NOT NULL
    AND content IS NOT NULL
  ORDER BY date ASC, id ASC
  LIMIT %s
)
UPDATE telegram_messages AS tm
SET
  content = NULL,
  media = {_MEDIA_TO_HARVEST_STUB},
  reactions = NULL,
  metadata = NULL,
  fwd_from = NULL,
  reply_to = NULL,
  action_data = NULL,
  updated_at = now()
FROM batch
WHERE tm.id = batch.id
  AND tm.date = batch.date
RETURNING tm.id, tm.date
"""

#: Fat media left after content strip (or re-fattened by an old persist) → harvest stub.
_MEDIA_ONLY_STRIP_SQL = f"""
WITH batch AS (
  SELECT id, date
  FROM telegram_messages
  WHERE date >= %s
    AND date < %s
    AND (date, id) > (%s, %s)
    AND archive_key IS NOT NULL
    AND content IS NULL
    AND media IS NOT NULL
    AND media <> '{{}}'::jsonb
    AND (media - 'harvest') <> '{{}}'::jsonb
  ORDER BY date ASC, id ASC
  LIMIT %s
)
UPDATE telegram_messages AS tm
SET
  media = {_MEDIA_TO_HARVEST_STUB},
  reactions = NULL,
  metadata = NULL,
  fwd_from = NULL,
  reply_to = NULL,
  action_data = NULL,
  updated_at = now()
FROM batch
WHERE tm.id = batch.id
  AND tm.date = batch.date
RETURNING tm.id, tm.date
"""

_TRIGGERS = (
    "telegram_messages_search_outbox_upd",
    "telegram_messages_save_edit_history",
)


@contextmanager
def _suppress_message_triggers(conn: Any) -> Iterator[None]:
    """Disable strip-sensitive triggers; always re-enable on exit."""
    with conn.cursor() as cur:
        for name in _TRIGGERS:
            cur.execute(f"ALTER TABLE telegram_messages DISABLE TRIGGER {name}")
        conn.commit()
        lg.warning(
            "disabled triggers %s on telegram_messages for strip (re-enabled when this month finishes)",
            ", ".join(_TRIGGERS),
        )
    try:
        yield
    finally:
        with conn.cursor() as cur:
            for name in _TRIGGERS:
                cur.execute(f"ALTER TABLE telegram_messages ENABLE TRIGGER {name}")
            conn.commit()
        lg.info("re-enabled triggers %s on telegram_messages", ", ".join(_TRIGGERS))


def strip_month_has_work(cfg: Config, month: str) -> bool:
    start, end = month_bounds(parse_month(month))
    with connect(cfg.postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '60s'")
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM telegram_messages
              WHERE date >= %s AND date < %s
                AND archive_key IS NOT NULL
                AND content IS NOT NULL
              LIMIT 1
            ) AS has_work
            """,
            (start, end),
        )
        row = cur.fetchone()
        if row and row["has_work"]:
            return True
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM telegram_messages
              WHERE date >= %s AND date < %s
                AND archive_key IS NOT NULL
                AND content IS NULL
                AND media IS NOT NULL
                AND media <> '{}'::jsonb
                AND (media - 'harvest') <> '{}'::jsonb
              LIMIT 1
            ) AS has_work
            """,
            (start, end),
        )
        row = cur.fetchone()
        return bool(row and row["has_work"])


def strip_backlog_count(cfg: Config, *, month: str) -> int:
    """Approximate strip backlog: rows with archive_key and non-null content."""
    start, end = month_bounds(parse_month(month))
    with connect(cfg.postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '120s'")
        cur.execute(
            """
            SELECT COUNT(*)::bigint AS n
            FROM telegram_messages
            WHERE date >= %s AND date < %s
              AND archive_key IS NOT NULL
              AND content IS NOT NULL
            """,
            (start, end),
        )
        row = cur.fetchone()
        return int(row["n"] if row else 0)


def _row_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def strip_month(
    cfg: Config,
    month: str,
    *,
    dry_run: bool = False,
    chunk_size: int = _STRIP_CHUNK,
) -> dict[str, int]:
    """Strip one cold archived month. Refuses if archive_key backlog remains."""
    month = parse_month(month)
    cutoff = hot_cutoff_month_start(hot_months=cfg.hot_months)
    y, m = map(int, month.split("-"))
    if date(y, m, 1) >= cutoff:
        raise ValueError(f"month {month} is still hot (cutoff {cutoff.isoformat()}, hot_months={cfg.hot_months})")
    if month_has_backlog(cfg, month):
        raise ValueError(f"month {month} still has archive_key IS NULL rows; finish archive before strip")

    start, end = month_bounds(month)
    if dry_run:
        n = strip_backlog_count(cfg, month=month)
        lg.info("dry_run strip month=%s would_strip≈%d", month, n)
        return {"stripped": 0, "would_strip": n, "chunks": 0}

    stripped = 0
    chunks = 0
    with connect(cfg.postgres_dsn) as conn:
        with _suppress_message_triggers(conn):
            for label, sql in (("content", _STRIP_SQL), ("media-only", _MEDIA_ONLY_STRIP_SQL)):
                # (start, uuid_nil): every real row on/after start matches the keyset.
                cursor_date: date = start
                cursor_id: UUID = _CURSOR_ID_MIN
                while True:
                    with conn.cursor() as cur:
                        cur.execute("SET LOCAL statement_timeout = 0")
                        cur.execute("SET LOCAL lock_timeout = '10min'")
                        # Restartable job: skip flush wait on each 2k-row commit.
                        cur.execute("SET LOCAL synchronous_commit = off")
                        cur.execute(sql, (start, end, cursor_date, cursor_id, chunk_size))
                        rows = cur.fetchall()
                        conn.commit()
                    n = len(rows)
                    if n == 0:
                        break
                    last = rows[-1]
                    cursor_date = _row_date(last["date"])
                    cursor_id = UUID(str(last["id"]))
                    stripped += n
                    chunks += 1
                    lg.info(
                        "month=%s pass=%s stripped_chunk=%d total_stripped=%d cursor=%s/%s",
                        month,
                        label,
                        n,
                        stripped,
                        cursor_date.isoformat(),
                        cursor_id,
                    )

    return {"stripped": stripped, "would_strip": 0, "chunks": chunks}


def list_cold_months_needing_strip(cfg: Config, *, limit: int = 500) -> list[str]:
    out: list[str] = []
    for month in list_cold_partition_months(cfg):
        if month_has_backlog(cfg, month):
            continue
        if strip_month_has_work(cfg, month):
            out.append(month)
            if len(out) >= limit:
                break
    return out


def run_strip(
    cfg: Config,
    *,
    months: list[str] | None = None,
    max_months: int = 0,
    dry_run: bool = False,
) -> dict[str, Any]:
    cfg.validate_required()
    if months:
        targets = [parse_month(m) for m in months]
    else:
        targets = list_cold_months_needing_strip(cfg)
        if max_months > 0:
            targets = targets[:max_months]

    totals: dict[str, Any] = {
        "months": 0,
        "stripped": 0,
        "would_strip": 0,
        "chunks": 0,
        "targets": targets,
    }
    for month in targets:
        lg.info("stripping month=%s dry_run=%s", month, dry_run)
        stats = strip_month(cfg, month, dry_run=dry_run)
        totals["months"] += 1
        totals["stripped"] += stats["stripped"]
        totals["would_strip"] += stats["would_strip"]
        totals["chunks"] += stats["chunks"]
    return totals
