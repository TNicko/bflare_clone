"""Read embedding shards back from a ``Sink`` (for Milvus backfill / rebuilds).

Lists Parquet shards under a model+modality prefix (optionally scoped to a set of ``YYYY-MM`` months) and
streams their rows as plain dicts. The Milvus backfill batches these into upserts; an image pipeline would
read the same way.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from .layout import (
    DEFAULT_PREFIX,
    collection_prefix,
    is_canonical_shard_key,
    is_micro_shard_key,
    month_prefix,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from .sink import Sink

ShardKind = Literal["all", "canonical", "micro"]


def list_shards(
    sink: Sink,
    model: str,
    modality: str,
    *,
    months: Iterable[str] | None = None,
    prefix: str = DEFAULT_PREFIX,
    kind: ShardKind = "all",
) -> list[str]:
    """Parquet shard keys for a model+modality.

    ``kind``:
    - ``all`` — canonical ``part-*.parquet`` and ``micro/part-*.parquet``
    - ``canonical`` — only top-level month parts (not under ``micro/``)
    - ``micro`` — only ``.../micro/part-*.parquet``
    """
    if months is None:
        roots = [collection_prefix(model, modality, prefix=prefix)]
    else:
        roots = [month_prefix(model, modality, m, prefix=prefix) for m in months]

    keys: list[str] = []
    for root in roots:
        keys.extend(k for k in sink.list_keys(root) if k.endswith(".parquet"))

    if kind == "canonical":
        keys = [k for k in keys if is_canonical_shard_key(k)]
    elif kind == "micro":
        keys = [k for k in keys if is_micro_shard_key(k)]
    else:
        keys = [k for k in keys if is_canonical_shard_key(k) or is_micro_shard_key(k)]
    return sorted(keys)


def read_shard_rows(sink: Sink, key: str) -> list[dict[str, Any]]:
    """Read one shard into a list of row dicts (``id``, ``date``, ``message_id``, ``source_external_id``,
    ``peer_type``, ``vector``). ``date`` is a tz-aware datetime; ``vector`` is a ``list[float]``."""
    rows: list[dict[str, Any]] = []
    for batch in iter_shard_row_batches(sink, key):
        rows.extend(batch)
    return rows


def iter_shard_row_batches_from_path(
    path: str,
    *,
    batch_rows: int = 1000,
    skip_rows: int = 0,
) -> Iterator[list[dict[str, Any]]]:
    """Yield row-dict batches from a local Parquet file.

    ``skip_rows`` drops leading rows (mid-shard resume). Does not load the full
    table into memory.
    """
    import pyarrow.parquet as pq

    if batch_rows < 1:
        raise ValueError("batch_rows must be >= 1")
    if skip_rows < 0:
        raise ValueError("skip_rows must be >= 0")

    pf = pq.ParquetFile(path)
    skipped = 0
    for batch in pf.iter_batches(batch_size=batch_rows):
        rows = batch.to_pylist()
        if skipped < skip_rows:
            need = skip_rows - skipped
            if need >= len(rows):
                skipped += len(rows)
                continue
            rows = rows[need:]
            skipped = skip_rows
        if rows:
            yield rows


def parquet_row_count(path: str) -> int:
    """Row count from Parquet footer metadata (no full scan)."""
    import pyarrow.parquet as pq

    meta = pq.ParquetFile(path).metadata
    if meta is None:
        return 0
    return int(meta.num_rows)


def iter_shard_row_batches(
    sink: Sink,
    key: str,
    *,
    batch_rows: int = 1000,
) -> Iterator[list[dict[str, Any]]]:
    """Yield row-dict batches from one Parquet shard without ``to_pylist()`` on the full table.

    Canonical parts are ~100k × 1024-d vectors; materializing them all as Python objects OOMs
    small backfill hosts. Streams the object to a temp file, then decodes ``batch_rows`` at a time.
    """
    import tempfile
    from pathlib import Path

    if batch_rows < 1:
        raise ValueError("batch_rows must be >= 1")

    with tempfile.TemporaryDirectory(prefix="embstore-shard-") as tmp_dir:
        local = Path(tmp_dir) / "shard.parquet"
        sink.download_to_path(key, local)
        yield from iter_shard_row_batches_from_path(str(local), batch_rows=batch_rows)


def iter_embedding_rows(
    sink: Sink,
    model: str,
    modality: str,
    *,
    months: Iterable[str] | None = None,
    prefix: str = DEFAULT_PREFIX,
    kind: ShardKind = "all",
) -> Iterator[dict[str, Any]]:
    """Stream every embedding row across all matching shards, shard by shard."""
    for key in list_shards(sink, model, modality, months=months, prefix=prefix, kind=kind):
        for batch in iter_shard_row_batches(sink, key):
            yield from batch
