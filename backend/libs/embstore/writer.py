"""Buffered writer that emits monthly Parquet shards to a ``Sink``.

Rows are grouped by their ``dt=YYYY-MM`` month and flushed to a new ``part-{uuid}.parquet`` shard whenever a
month's buffer reaches ``max_rows_per_shard`` (and again on ``close``). Each flush is one immutable object -
nothing is ever appended to in place, which keeps writes idempotent-ish and safe to retry (a failed upload
just leaves a partial set the reader can tolerate, and re-running rewrites with fresh part ids).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from .layout import (
    DEFAULT_PREFIX,
    embedding_schema,
    image_embedding_schema,
    micro_shard_key,
    month_key,
    shard_key,
)

if TYPE_CHECKING:
    from .sink import Sink


class EmbeddingShardWriter:
    """Accumulate embedding rows and write them out as monthly Parquet shards.

    Use as a context manager so ``close`` (final flush) always runs::

        with EmbeddingShardWriter(sink, model="bge-m3", modality="text", dim=1024) as w:
            for row in rows:
                w.add(id=..., date=..., vector=...)

    For durable embed jobs, use ``tier="micro"`` and ``auto_flush=False``, then
    ``await aflush()`` once per DB page so coverage advances only after S3 success.
    """

    def __init__(
        self,
        sink: Sink,
        *,
        model: str,
        modality: str,
        dim: int,
        prefix: str = DEFAULT_PREFIX,
        max_rows_per_shard: int = 200_000,
        auto_flush: bool = True,
        tier: str = "canonical",
    ) -> None:
        if tier not in ("canonical", "micro"):
            raise ValueError(f"tier must be 'canonical' or 'micro', got {tier!r}")
        self._sink = sink
        self._model = model
        self._modality = modality
        self._dim = dim
        self._prefix = prefix
        self._max_rows = max_rows_per_shard
        self._auto_flush = auto_flush
        self._tier = tier
        self._schema = embedding_schema(dim)
        #: month -> list of row dicts pending flush.
        self._buffers: dict[str, list[dict[str, object]]] = {}
        self.shards_written: list[str] = []
        self.rows_written = 0

    def add(
        self,
        *,
        id: str,
        date: datetime,
        vector: Sequence[float],
        message_id: int | None = None,
        source_external_id: str | None = None,
        peer_type: str | None = None,
    ) -> None:
        """Buffer one embedding row. Raises if ``vector`` length != configured ``dim`` (fail loud)."""
        if len(vector) != self._dim:
            raise ValueError(f"vector dim {len(vector)} != expected {self._dim} for id={id}")
        month = month_key(date)
        buf = self._buffers.setdefault(month, [])
        buf.append(
            {
                "id": str(id),
                "date": date,
                "message_id": int(message_id) if message_id is not None else None,
                "source_external_id": str(source_external_id) if source_external_id is not None else None,
                "peer_type": str(peer_type) if peer_type is not None else None,
                "vector": [float(x) for x in vector],
            }
        )
        if self._auto_flush and len(buf) >= self._max_rows:
            self._flush_month(month)

    def _part_key(self, month: str, part_id: str) -> str:
        if self._tier == "micro":
            return micro_shard_key(self._model, self._modality, month, part_id, prefix=self._prefix)
        return shard_key(self._model, self._modality, month, part_id, prefix=self._prefix)

    def _build_shard(self, month: str) -> tuple[str, bytes, int] | None:
        rows = self._buffers.get(month)
        if not rows:
            return None
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = {name: [row[name] for row in rows] for name in self._schema.names}
        table = pa.Table.from_pydict(columns, schema=self._schema)

        out = pa.BufferOutputStream()
        pq.write_table(table, out, compression="snappy")
        data = out.getvalue().to_pybytes()
        key = self._part_key(month, uuid.uuid4().hex)
        return key, data, len(rows)

    def _flush_month(self, month: str) -> tuple[str, int] | None:
        built = self._build_shard(month)
        if built is None:
            return None
        key, data, n = built
        self._sink.put_bytes(key, data)
        self.shards_written.append(key)
        self.rows_written += n
        self._buffers[month] = []
        return key, n

    async def _aflush_month(self, month: str) -> tuple[str, int] | None:
        # Build on the event-loop thread (owns the buffer); only the sink upload is offloaded.
        built = self._build_shard(month)
        if built is None:
            return None
        key, data, n = built
        try:
            await asyncio.to_thread(self._sink.put_bytes, key, data)
        except Exception:
            # Leave rows in buffer so caller can retry or discard explicitly.
            raise
        self.shards_written.append(key)
        self.rows_written += n
        self._buffers[month] = []
        return key, n

    def flush(self) -> list[tuple[str, int]]:
        """Write out every buffered month (each as its own shard).

        Returns ``(key, row_count)`` pairs successfully written.
        """
        written: list[tuple[str, int]] = []
        for month in list(self._buffers.keys()):
            item = self._flush_month(month)
            if item is not None:
                written.append(item)
        return written

    async def aflush(self) -> list[tuple[str, int]]:
        """Async flush: Parquet build on loop thread; sink upload in a worker thread.

        Returns ``(key, row_count)`` pairs successfully written. On failure mid-flush,
        earlier keys may already be on the sink — caller should roll them back.
        """
        written: list[tuple[str, int]] = []
        for month in list(self._buffers.keys()):
            item = await self._aflush_month(month)
            if item is not None:
                written.append(item)
        return written

    def discard(self) -> None:
        """Drop all buffered rows without writing (failed page before any PUT)."""
        self._buffers.clear()

    async def rollback_keys(self, keys: list[str]) -> None:
        """Best-effort delete of keys after a failed cursor commit (or partial flush)."""
        for key in keys:
            try:
                await asyncio.to_thread(self._sink.delete_key, key)
            except Exception:
                # Leave orphan micro; next run may re-embed (Milvus upserts by id).
                pass

    def close(self) -> None:
        self.flush()

    async def aclose(self) -> None:
        await self.aflush()

    def __enter__(self) -> EmbeddingShardWriter:
        return self

    def __exit__(self, exc_type: object, *exc: object) -> None:
        if exc_type is not None:
            self.discard()
            return
        self.close()

    async def __aenter__(self) -> EmbeddingShardWriter:
        return self

    async def __aexit__(self, exc_type: object, *exc: object) -> None:
        if exc_type is not None:
            self.discard()
            return
        await self.aclose()


class ImageEmbeddingShardWriter:
    """Buffered writer for image embedding Parquet shards.

    For durable embed jobs, use ``tier="micro"`` and ``auto_flush=False``, then
    ``await aflush()`` once per DB page so coverage advances only after S3 success.
    """

    def __init__(
        self,
        sink: Sink,
        *,
        model: str,
        modality: str = "image",
        dim: int,
        prefix: str = DEFAULT_PREFIX,
        max_rows_per_shard: int = 200_000,
        auto_flush: bool = True,
        tier: str = "canonical",
    ) -> None:
        if tier not in ("canonical", "micro"):
            raise ValueError(f"tier must be 'canonical' or 'micro', got {tier!r}")
        self._sink = sink
        self._model = model
        self._modality = modality
        self._dim = dim
        self._prefix = prefix
        self._max_rows = max_rows_per_shard
        self._auto_flush = auto_flush
        self._tier = tier
        self._schema = image_embedding_schema(dim)
        self._buffers: dict[str, list[dict[str, object]]] = {}
        self.shards_written: list[str] = []
        self.rows_written = 0

    def add(
        self,
        *,
        id: str,
        date: datetime,
        vector: Sequence[float],
        msg_id: str,
        s3_key: str,
        filename: str | None = None,
        message_id: int | None = None,
        source_external_id: str | None = None,
        peer_type: str | None = None,
    ) -> None:
        if len(vector) != self._dim:
            raise ValueError(f"vector dim {len(vector)} != expected {self._dim} for id={id}")
        month = month_key(date)
        buf = self._buffers.setdefault(month, [])
        buf.append(
            {
                "id": str(id),
                "date": date,
                "msg_id": str(msg_id),
                "s3_key": str(s3_key),
                "filename": str(filename) if filename is not None else None,
                "message_id": int(message_id) if message_id is not None else None,
                "source_external_id": str(source_external_id) if source_external_id is not None else None,
                "peer_type": str(peer_type) if peer_type is not None else None,
                "vector": [float(x) for x in vector],
            }
        )
        if self._auto_flush and len(buf) >= self._max_rows:
            self._flush_month(month)

    def _part_key(self, month: str, part_id: str) -> str:
        if self._tier == "micro":
            return micro_shard_key(self._model, self._modality, month, part_id, prefix=self._prefix)
        return shard_key(self._model, self._modality, month, part_id, prefix=self._prefix)

    def _build_shard(self, month: str) -> tuple[str, bytes, int] | None:
        rows = self._buffers.get(month)
        if not rows:
            return None
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = {name: [row[name] for row in rows] for name in self._schema.names}
        table = pa.Table.from_pydict(columns, schema=self._schema)

        out = pa.BufferOutputStream()
        pq.write_table(table, out, compression="snappy")
        data = out.getvalue().to_pybytes()
        key = self._part_key(month, uuid.uuid4().hex)
        return key, data, len(rows)

    def _flush_month(self, month: str) -> tuple[str, int] | None:
        built = self._build_shard(month)
        if built is None:
            return None
        key, data, n = built
        self._sink.put_bytes(key, data)
        self.shards_written.append(key)
        self.rows_written += n
        self._buffers[month] = []
        return key, n

    async def _aflush_month(self, month: str) -> tuple[str, int] | None:
        built = self._build_shard(month)
        if built is None:
            return None
        key, data, n = built
        try:
            await asyncio.to_thread(self._sink.put_bytes, key, data)
        except Exception:
            raise
        self.shards_written.append(key)
        self.rows_written += n
        self._buffers[month] = []
        return key, n

    def flush(self) -> list[tuple[str, int]]:
        written: list[tuple[str, int]] = []
        for month in list(self._buffers.keys()):
            item = self._flush_month(month)
            if item is not None:
                written.append(item)
        return written

    async def aflush(self) -> list[tuple[str, int]]:
        written: list[tuple[str, int]] = []
        for month in list(self._buffers.keys()):
            item = await self._aflush_month(month)
            if item is not None:
                written.append(item)
        return written

    def discard(self) -> None:
        self._buffers.clear()

    async def rollback_keys(self, keys: list[str]) -> None:
        for key in keys:
            try:
                await asyncio.to_thread(self._sink.delete_key, key)
            except Exception:
                pass

    def close(self) -> None:
        self.flush()

    async def aclose(self) -> None:
        await self.aflush()

    def __enter__(self) -> ImageEmbeddingShardWriter:
        return self

    def __exit__(self, exc_type: object, *exc: object) -> None:
        if exc_type is not None:
            self.discard()
            return
        self.close()

    async def __aenter__(self) -> ImageEmbeddingShardWriter:
        return self

    async def __aexit__(self, exc_type: object, *exc: object) -> None:
        if exc_type is not None:
            self.discard()
            return
        await self.aclose()
