"""Buffered writer that emits monthly message Parquet parts to a ``Sink``.

Flush happens when a month buffer hits ``max_rows_per_shard`` (and on ``close``).
Each flush is one immutable object; callers mark ``archive_key`` / ``archive_row``
in Postgres only after a successful PUT.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .layout import DEFAULT_PREFIX, message_archive_schema, month_key, shard_key

if TYPE_CHECKING:
    from embstore.sink import Sink


def _json_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str, separators=(",", ":"))


def _as_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raise TypeError(f"expected datetime, got {type(value)!r}")


class MessageShardWriter:
    """Accumulate full message rows and write monthly Parquet parts."""

    def __init__(
        self,
        sink: Sink,
        *,
        prefix: str = DEFAULT_PREFIX,
        max_rows_per_shard: int = 100_000,
        auto_flush: bool = False,
        compression: str = "zstd",
    ) -> None:
        self._sink = sink
        self._prefix = prefix.strip("/")
        self._max_rows = max_rows_per_shard
        self._auto_flush = auto_flush
        self._compression = compression
        self._schema = message_archive_schema()
        self._buffers: dict[str, list[dict[str, object]]] = {}
        self.shards_written: list[str] = []
        self.rows_written = 0

    def add_row(self, row: dict[str, Any]) -> list[tuple[str, int, list[dict[str, object]]]]:
        """Buffer one DB row. Returns flushes triggered by auto_flush: ``(key, n, rows)``."""
        date = _as_utc_datetime(row.get("date"))
        if date is None:
            raise ValueError("message row missing date")
        month = month_key(date)
        buffered = {
            "id": str(row["id"]),
            "message_id": int(row["message_id"]),
            "source_external_id": str(row["source_external_id"]),
            "peer_type": str(row["peer_type"]),
            "message_type": str(row["message_type"]) if row.get("message_type") is not None else None,
            "content": row.get("content"),
            "content_hash": row.get("content_hash"),
            "out": bool(row["out"]) if row.get("out") is not None else None,
            "date": date,
            "edit_date": _as_utc_datetime(row.get("edit_date")),
            "created_at": _as_utc_datetime(row.get("created_at")),
            "updated_at": _as_utc_datetime(row.get("updated_at")),
            "reactions": _json_text(row.get("reactions")),
            "views": int(row["views"]) if row.get("views") is not None else None,
            "media": _json_text(row.get("media")),
            "pinned": bool(row["pinned"]) if row.get("pinned") is not None else None,
            "deleted": bool(row["deleted"]) if row.get("deleted") is not None else None,
            "silent": bool(row["silent"]) if row.get("silent") is not None else None,
            "mentioned": bool(row["mentioned"]) if row.get("mentioned") is not None else None,
            "has_url": bool(row["has_url"]) if row.get("has_url") is not None else None,
            "has_mention": bool(row["has_mention"]) if row.get("has_mention") is not None else None,
            "grouped_id": int(row["grouped_id"]) if row.get("grouped_id") is not None else None,
            "action_type": str(row["action_type"]) if row.get("action_type") is not None else None,
            "action_data": _json_text(row.get("action_data")),
            "metadata": _json_text(row.get("metadata")),
            "fwd_from": _json_text(row.get("fwd_from")),
            "reply_to": _json_text(row.get("reply_to")),
            "from_external_id": str(row["from_external_id"]) if row.get("from_external_id") is not None else None,
            "harvester_external_id": (
                str(row["harvester_external_id"]) if row.get("harvester_external_id") is not None else None
            ),
        }
        buf = self._buffers.setdefault(month, [])
        buf.append(buffered)
        flushed: list[tuple[str, int, list[dict[str, object]]]] = []
        if self._auto_flush and len(buf) >= self._max_rows:
            item = self.flush_month(month)
            if item is not None:
                flushed.append(item)
        return flushed

    def pending_count(self, month: str | None = None) -> int:
        if month is None:
            return sum(len(v) for v in self._buffers.values())
        return len(self._buffers.get(month, []))

    def flush_month(self, month: str) -> tuple[str, int, list[dict[str, object]]] | None:
        """Write one part for ``month``. Returns ``(key, n, rows_in_order)`` or None if empty."""
        rows = self._buffers.get(month)
        if not rows:
            return None
        # Take ownership so a failed PUT can restore.
        batch = rows
        self._buffers[month] = []
        try:
            key, data = self._build_parquet(month, batch)
            self._sink.put_bytes(key, data)
        except Exception:
            self._buffers[month] = batch + self._buffers.get(month, [])
            raise
        self.shards_written.append(key)
        self.rows_written += len(batch)
        return key, len(batch), batch

    def flush(self) -> list[tuple[str, int, list[dict[str, object]]]]:
        written: list[tuple[str, int, list[dict[str, object]]]] = []
        for month in list(self._buffers.keys()):
            item = self.flush_month(month)
            if item is not None:
                written.append(item)
        return written

    def discard(self) -> None:
        self._buffers.clear()

    def _build_parquet(self, month: str, rows: list[dict[str, object]]) -> tuple[str, bytes]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = {name: [row[name] for row in rows] for name in self._schema.names}
        table = pa.Table.from_pydict(columns, schema=self._schema)
        out = pa.BufferOutputStream()
        pq.write_table(table, out, compression=self._compression)
        key = shard_key(month, uuid.uuid4().hex, prefix=self._prefix)
        return key, out.getvalue().to_pybytes()

    def close(self) -> list[tuple[str, int, list[dict[str, object]]]]:
        return self.flush()

    def __enter__(self) -> MessageShardWriter:
        return self

    def __exit__(self, exc_type: object, *exc: object) -> None:
        if exc_type is not None:
            self.discard()
            return
        self.close()
