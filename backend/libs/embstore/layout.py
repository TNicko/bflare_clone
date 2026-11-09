"""Canonical S3 layout + Arrow schema for the embedding store.

The embedding store is the **durable source of truth** for vectors. Milvus is a derived index rebuilt from
here, so this layout must be stable and self-describing.

Key convention (one object = one Parquet shard)::

    {prefix}/{model}/{modality}/dt=YYYY-MM/part-{uuid}.parquet          # canonical (compacted)
    {prefix}/{model}/{modality}/dt=YYYY-MM/micro/part-{uuid}.parquet    # micro (per DB-batch flush)

    e.g.  embeddings/bge-m3/text/dt=2026-06/part-7f3a....parquet
          embeddings/bge-m3/text/dt=2026-06/micro/part-....parquet
          embeddings/siglip2-so400m-p16-384/image/dt=2026-06/part-....parquet

- **model** in the path => re-embedding under a new model is a new prefix (and a new Milvus collection
  version), never an in-place overwrite.
- **dt=YYYY-MM** mirrors the monthly ``telegram_messages`` partitions, so a rebuild can target a date range
  by listing only the relevant month prefixes.
- **micro/** holds small durable shards written on each embed DB page; a compactor merges them into
  canonical ``part-*.parquet`` objects.
- Columnar monthly shards (not one object per vector) keep S3 list/GET costs and rebuild times sane at
  billions of rows.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

#: Default top-level prefix under the bucket.
DEFAULT_PREFIX = "embeddings"

_DT_RE = re.compile(r"/dt=(\d{4}-\d{2})/")


def month_from_shard_key(key: str) -> str | None:
    """Extract ``YYYY-MM`` from a shard key containing ``/dt=YYYY-MM/``."""
    m = _DT_RE.search(key)
    return m.group(1) if m else None


def collection_prefix(model: str, modality: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """``{prefix}/{model}/{modality}`` - the root for one model+modality's shards."""
    return f"{prefix.strip('/')}/{model}/{modality}"


def month_key(date: datetime) -> str:
    """``YYYY-MM`` partition key for a row's date (used in the ``dt=`` path segment)."""
    return f"{date.year:04d}-{date.month:02d}"


def month_prefix(model: str, modality: str, month: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """``{prefix}/{model}/{modality}/dt=YYYY-MM`` - all shards for one month."""
    return f"{collection_prefix(model, modality, prefix=prefix)}/dt={month}"


def shard_key(model: str, modality: str, month: str, part_id: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """Full key for one canonical Parquet shard within a month."""
    return f"{month_prefix(model, modality, month, prefix=prefix)}/part-{part_id}.parquet"


def micro_shard_key(model: str, modality: str, month: str, part_id: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """Full key for one micro (per-batch) Parquet shard within a month."""
    return f"{month_prefix(model, modality, month, prefix=prefix)}/micro/part-{part_id}.parquet"


def micro_month_prefix(model: str, modality: str, month: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """``.../dt=YYYY-MM/micro`` — listing root for micro shards in one month."""
    return f"{month_prefix(model, modality, month, prefix=prefix)}/micro"


def is_micro_shard_key(key: str) -> bool:
    """True if ``key`` is under a ``.../micro/part-*.parquet`` path segment."""
    return "/micro/part-" in key and key.endswith(".parquet")


def is_canonical_shard_key(key: str) -> bool:
    """True if ``key`` is a top-level month ``part-*.parquet`` (not under ``micro/``)."""
    if not key.endswith(".parquet") or "/part-" not in key:
        return False
    return not is_micro_shard_key(key)


def embedding_schema(dim: int) -> pa.Schema:
    """Arrow schema for an embedding shard. ``vector`` is a fixed-size float32 list of length ``dim``.

    Columns are exactly what Milvus needs to (a) key/hydrate (``id``, ``date``) and (b) filter
    (``source_external_id``, ``peer_type``), plus ``message_id`` for dedupe/debugging.
    """
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("date", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("message_id", pa.int64(), nullable=True),
            pa.field("source_external_id", pa.string(), nullable=True),
            pa.field("peer_type", pa.string(), nullable=True),
            pa.field("vector", pa.list_(pa.float32(), dim), nullable=False),
        ]
    )


def image_embedding_schema(dim: int) -> pa.Schema:
    """Arrow schema for image embedding shards (reverse image search).

    From blob-first SigLIP onward ``id`` / ``msg_id`` are the exact 16-hex pHash
    (unique bytes). Message fan-out uses Postgres ``telegram_image_blob_messages``.
    """
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("date", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("msg_id", pa.string(), nullable=False),
            pa.field("s3_key", pa.string(), nullable=False),
            pa.field("filename", pa.string(), nullable=True),
            pa.field("message_id", pa.int64(), nullable=True),
            pa.field("source_external_id", pa.string(), nullable=True),
            pa.field("peer_type", pa.string(), nullable=True),
            pa.field("vector", pa.list_(pa.float32(), dim), nullable=False),
        ]
    )


def to_epoch_ms(date: datetime) -> int:
    """Convert a datetime to epoch milliseconds (how Milvus stores ``date``)."""
    return int(date.timestamp() * 1000)
