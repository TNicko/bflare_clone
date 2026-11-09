"""S3 canonical embedding store: durable, model-versioned Parquet shards (the source of truth for vectors).

Milvus is rebuilt from this store, so a Milvus collection can always be dropped and recreated without a GPU.
The same shards seed dev (Milvus Lite) and prod (self-hosted Milvus).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .layout import (
    DEFAULT_PREFIX,
    collection_prefix,
    embedding_schema,
    image_embedding_schema,
    is_canonical_shard_key,
    is_micro_shard_key,
    micro_month_prefix,
    micro_shard_key,
    month_from_shard_key,
    month_key,
    month_prefix,
    shard_key,
    to_epoch_ms,
)
from .reader import (
    iter_embedding_rows,
    iter_shard_row_batches,
    iter_shard_row_batches_from_path,
    list_shards,
    parquet_row_count,
    read_shard_rows,
)
from .sink import LocalSink, S3Sink, Sink
from .writer import EmbeddingShardWriter, ImageEmbeddingShardWriter

if TYPE_CHECKING:
    from .compact import CompactedMonth

__all__ = [
    "DEFAULT_PREFIX",
    "CompactedMonth",
    "EmbeddingShardWriter",
    "ImageEmbeddingShardWriter",
    "LocalSink",
    "S3Sink",
    "Sink",
    "collection_prefix",
    "compact_micro_shards",
    "embedding_schema",
    "image_embedding_schema",
    "is_canonical_shard_key",
    "is_micro_shard_key",
    "iter_embedding_rows",
    "iter_shard_row_batches",
    "iter_shard_row_batches_from_path",
    "list_shards",
    "micro_month_prefix",
    "micro_shard_key",
    "month_from_shard_key",
    "month_key",
    "month_prefix",
    "parquet_row_count",
    "read_shard_rows",
    "reconcile_micro_shards",
    "shard_key",
    "to_epoch_ms",
]


def __getattr__(name: str) -> Any:
    # Lazy: avoid circular import with db.crud.system.embedding_store via compact.
    if name in {"CompactedMonth", "compact_micro_shards", "reconcile_micro_shards"}:
        from . import compact as _compact

        return getattr(_compact, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
