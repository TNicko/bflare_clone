"""Copy cold telegram_messages months to S3 Parquet and set archive_key."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from db.connection import connect
from msgstore import LocalSink, MessageShardWriter, S3Sink, Sink
from msgstore.layout import parse_month

from .config import Config, hot_cutoff_month_start, month_bounds

lg = logging.getLogger(__name__)

_PART_RE = re.compile(r"^telegram_messages_p(\d{4})_(\d{2})$")

_SELECT_SQL = """
    SELECT
      tm.id,
      tm.message_id,
      tm.source_external_id::text AS source_external_id,
      tm.peer_type,
      tm.message_type,
      tm.content,
      tm.content_hash,
      tm.out,
      tm.date,
      tm.edit_date,
      tm.created_at,
      tm.updated_at,
      tm.reactions,
      tm.views,
      tm.media,
      tm.pinned,
      tm.deleted,
      tm.silent,
      tm.mentioned,
      tm.grouped_id,
      tm.action_type,
      tm.action_data,
      tm.metadata,
      tm.fwd_from,
      tm.reply_to,
      tm.from_external_id::text AS from_external_id,
      tm.harvester_external_id::text AS harvester_external_id
    FROM telegram_messages tm
    WHERE tm.date >= %s
      AND tm.date < %s
      AND tm.archive_key IS NULL
      {cursor}
    ORDER BY tm.date ASC, tm.id ASC
    LIMIT %s
"""

_MARK_SQL = """
    UPDATE telegram_messages AS tm
    SET
      archive_key = data.archive_key,
      archive_row = data.archive_row,
      archived_at = now()
    FROM unnest(%s::uuid[], %s::timestamptz[], %s::text[], %s::int[])
      AS data(id, date, archive_key, archive_row)
    WHERE tm.id = data.id
      AND tm.date = data.date
      AND tm.archive_key IS NULL
"""

#: Supabase pooler statement_timeout kills multi-k row UPDATEs; mark in chunks.
_MARK_CHUNK = 500


class _DryRunSink:
    """Accepts puts without storing (dry-run)."""

    def put_bytes(self, key: str, data: bytes) -> None:
        lg.info("dry_run put key=%s bytes=%d", key, len(data))

    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def download_to_path(self, key: str, path: Path) -> int:
        raise NotImplementedError

    def object_size(self, key: str) -> int:
        raise NotImplementedError

    def list_keys(self, prefix: str):
        return iter(())

    def delete_key(self, key: str) -> None:
        return None

    def delete_keys(self, keys: list[str]) -> int:
        return 0

    def delete_prefix(self, prefix: str) -> int:
        return 0


def build_sink(cfg: Config) -> Sink:
    if cfg.dry_run:
        return _DryRunSink()
    if cfg.store_backend == "local":
        return LocalSink(cfg.local_dir)
    return S3Sink(
        cfg.s3_bucket,
        region=cfg.aws_region,
        access_key=cfg.aws_access_key_id,
        secret_key=cfg.aws_secret_access_key,
        endpoint_url=cfg.s3_endpoint_url,
    )


def _load_checkpoint(path: Path) -> tuple[str, str] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return str(data["last_date"]), str(data["last_id"])
    except Exception:
        lg.warning("could not read checkpoint %s; starting over", path)
        return None


def _save_checkpoint(path: Path, last_date: str, last_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_date": last_date, "last_id": last_id}))


def _fetch_batch(
    conn: psycopg.Connection[Any],
    *,
    month_start: datetime,
    month_end: datetime,
    cursor: tuple[str, str] | None,
    batch_size: int,
) -> list[dict[str, Any]]:
    if cursor is not None:
        cursor_sql = "AND (tm.date, tm.id) > (%s::timestamptz, %s::uuid)"
        params: list[Any] = [month_start, month_end, cursor[0], cursor[1], batch_size]
    else:
        cursor_sql = ""
        params = [month_start, month_end, batch_size]
    sql = _SELECT_SQL.format(cursor=cursor_sql)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def _mark_archived(
    conn: psycopg.Connection[Any],
    *,
    key: str,
    rows: list[dict[str, object]],
    dry_run: bool,
    chunk_size: int = _MARK_CHUNK,
) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    marked = 0
    with conn.cursor() as cur:
        for offset in range(0, len(rows), chunk_size):
            batch = rows[offset : offset + chunk_size]
            ids = [UUID(str(r["id"])) for r in batch]
            dates = [r["date"] for r in batch]
            keys = [key] * len(batch)
            # archive_row is the index within the Parquet part (global, not chunk-local).
            row_idxs = list(range(offset, offset + len(batch)))
            # SET LOCAL applies to this transaction only (reset on commit).
            cur.execute("SET LOCAL statement_timeout = 0")
            cur.execute("SET LOCAL lock_timeout = '5min'")
            cur.execute(_MARK_SQL, (ids, dates, keys, row_idxs))
            marked += int(cur.rowcount)
            conn.commit()
            lg.info(
                "marked chunk offset=%d n=%d key=%s total_marked=%d",
                offset,
                len(batch),
                key,
                marked,
            )
    return marked


def archive_month(
    cfg: Config,
    month: str,
    *,
    sink: Sink | None = None,
    reset: bool = False,
) -> dict[str, int]:
    """Archive one calendar month. Returns counts."""
    month = parse_month(month)
    cutoff = hot_cutoff_month_start(hot_months=cfg.hot_months)
    y, m = map(int, month.split("-"))
    month_start_date = date(y, m, 1)
    if month_start_date >= cutoff:
        raise ValueError(f"month {month} is still hot (cutoff {cutoff.isoformat()}, hot_months={cfg.hot_months})")

    cfg.validate_required()
    sink = sink or build_sink(cfg)
    month_start, month_end = month_bounds(month)
    ck_path = cfg.checkpoint_path(month)
    if reset:
        ck_path.unlink(missing_ok=True)
    cursor = None if reset else _load_checkpoint(ck_path)
    if cursor:
        lg.info("month=%s resume checkpoint date=%s id=%s", month, cursor[0], cursor[1])

    writer = MessageShardWriter(
        sink,
        prefix=cfg.store_prefix,
        max_rows_per_shard=cfg.max_rows_per_shard,
        auto_flush=False,
    )
    scanned = 0
    marked = 0
    parts = 0

    def _commit_flush(key: str, rows: list[dict[str, object]]) -> None:
        nonlocal marked, parts
        n = _mark_archived(conn, key=key, rows=rows, dry_run=cfg.dry_run)
        marked += n
        parts += 1
        # Persist resume point only after PG marks succeed (avoid skipping unmarked rows).
        if rows and not cfg.dry_run:
            last = rows[-1]
            last_date = last["date"].isoformat() if isinstance(last["date"], datetime) else str(last["date"])
            _save_checkpoint(ck_path, last_date, str(last["id"]))
        lg.info(
            "month=%s part=%s rows=%d marked=%d dry_run=%s",
            month,
            key,
            len(rows),
            n,
            cfg.dry_run,
        )

    with connect(cfg.postgres_dsn) as conn:
        # Stale checkpoint after a failed mark can sit past unmarked rows; reset if so.
        if cursor is not None:
            probe = _fetch_batch(
                conn,
                month_start=month_start,
                month_end=month_end,
                cursor=cursor,
                batch_size=1,
            )
            if not probe and month_has_backlog(cfg, month):
                lg.warning(
                    "month=%s checkpoint %s is past unmarked backlog; resetting",
                    month,
                    cursor,
                )
                cursor = None
                ck_path.unlink(missing_ok=True)

        while True:
            rows = _fetch_batch(
                conn,
                month_start=month_start,
                month_end=month_end,
                cursor=cursor,
                batch_size=cfg.batch_size,
            )
            if not rows:
                break
            for row in rows:
                writer.add_row(row)
                if writer.pending_count(month) >= cfg.max_rows_per_shard:
                    flushed = writer.flush_month(month)
                    if flushed is not None:
                        key, _n, batch_rows = flushed
                        _commit_flush(key, batch_rows)
            last = rows[-1]
            last_date = last["date"].isoformat() if isinstance(last["date"], datetime) else str(last["date"])
            cursor = (last_date, str(last["id"]))
            scanned += len(rows)
            lg.info("month=%s scanned=%d marked=%d parts=%d", month, scanned, marked, parts)

        flushed = writer.flush_month(month)
        if flushed is not None:
            key, _n, batch_rows = flushed
            _commit_flush(key, batch_rows)

    if not cfg.dry_run:
        remaining = month_has_backlog(cfg, month)
        if not remaining:
            ck_path.unlink(missing_ok=True)

    return {"scanned": scanned, "marked": marked, "parts": parts}


def month_has_backlog(cfg: Config, month: str) -> bool:
    start, end = month_bounds(parse_month(month))
    with connect(cfg.postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM telegram_messages
              WHERE date >= %s AND date < %s AND archive_key IS NULL
            ) AS has_backlog
            """,
            (start, end),
        )
        row = cur.fetchone()
        return bool(row and row["has_backlog"])


def backlog_count(cfg: Config, *, month: str) -> int:
    """Exact backlog count for one month (partition-pruned)."""
    start, end = month_bounds(parse_month(month))
    with connect(cfg.postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint AS n
            FROM telegram_messages
            WHERE date >= %s AND date < %s AND archive_key IS NULL
            """,
            (start, end),
        )
        row = cur.fetchone()
        return int(row["n"] if row else 0)


def list_cold_partition_months(cfg: Config) -> list[str]:
    """Cold calendar months that have a ``telegram_messages_pYYYY_MM`` partition."""
    cutoff = hot_cutoff_month_start(hot_months=cfg.hot_months)
    with connect(cfg.postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname AS relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'p')
              AND c.relname ~ '^telegram_messages_p[0-9]{4}_[0-9]{2}$'
            ORDER BY c.relname
            """
        )
        months: list[str] = []
        for row in cur.fetchall():
            relname = str(row["relname"])
            m = _PART_RE.match(relname)
            if not m:
                continue
            month_start = date(int(m.group(1)), int(m.group(2)), 1)
            if month_start < cutoff:
                months.append(f"{m.group(1)}-{m.group(2)}")
        return months


def list_cold_months_with_backlog(cfg: Config, *, limit: int = 500) -> list[str]:
    """Oldest cold months that still have ``archive_key IS NULL`` (EXISTS per month)."""
    out: list[str] = []
    for month in list_cold_partition_months(cfg):
        if month_has_backlog(cfg, month):
            out.append(month)
            if len(out) >= limit:
                break
    return out


def run_archive(
    cfg: Config,
    *,
    months: list[str] | None = None,
    max_months: int = 0,
    reset: bool = False,
) -> dict[str, Any]:
    """Archive explicit months, or oldest cold backlog months."""
    cfg.validate_required()
    sink = build_sink(cfg)
    if months:
        targets = [parse_month(m) for m in months]
    else:
        targets = list_cold_months_with_backlog(cfg)
        if max_months > 0:
            targets = targets[:max_months]

    totals = {"months": 0, "scanned": 0, "marked": 0, "parts": 0, "targets": targets}
    for month in targets:
        lg.info("archiving month=%s dry_run=%s", month, cfg.dry_run)
        stats = archive_month(cfg, month, sink=sink, reset=reset)
        totals["months"] += 1
        totals["scanned"] += stats["scanned"]
        totals["marked"] += stats["marked"]
        totals["parts"] += stats["parts"]
    return totals


def status_summary(cfg: Config, *, month: str | None = None) -> dict[str, Any]:
    cutoff = hot_cutoff_month_start(hot_months=cfg.hot_months)
    if month:
        return {
            "hot_cutoff": cutoff.isoformat(),
            "month": parse_month(month),
            "backlog": backlog_count(cfg, month=month),
            "has_backlog": month_has_backlog(cfg, month),
        }
    cold = list_cold_partition_months(cfg)
    # Sample first few with EXISTS for a quick signal without full COUNT(*).
    pending: list[str] = []
    for m in cold:
        if month_has_backlog(cfg, m):
            pending.append(m)
            if len(pending) >= 20:
                break
    return {
        "hot_cutoff": cutoff.isoformat(),
        "hot_months": cfg.hot_months,
        "cold_partition_months": len(cold),
        "pending_months_sample": pending,
        "pending_months_sample_truncated": len(pending) >= 20,
        "prefix": cfg.store_prefix,
        "bucket": cfg.s3_bucket,
        "backend": cfg.store_backend,
        "dry_run": cfg.dry_run,
        "now_utc": datetime.now(timezone.utc).isoformat(),
    }
