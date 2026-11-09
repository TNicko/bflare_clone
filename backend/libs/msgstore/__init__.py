"""Cold telegram message Parquet archive (Phase 3a writers + 3b hydrate/overlays)."""

from __future__ import annotations

from .layout import (
    DEFAULT_PREFIX,
    is_part_shard_key,
    message_archive_schema,
    month_from_shard_key,
    month_key,
    month_prefix,
    overlay_key,
    parse_month,
    shard_key,
)
from .overlay import (
    get_overlay,
    merge_fat_fields,
    overlay_payload_from_message_row,
    put_overlay,
)
from .reader import (
    apply_harvest_onto_media,
    extract_harvest,
    hydrate_message_rows,
    hydrate_message_rows_async,
    is_harvest_stub,
    needs_body_hydrate,
    read_parquet_row,
)
from .runtime import (
    get_message_archive_prefix,
    get_message_archive_sink,
    message_archive_bucket,
    message_archive_prefix,
    reset_message_archive_sink_for_tests,
)
from .writer import MessageShardWriter

from embstore.sink import LocalSink, S3Sink, Sink

__all__ = [
    "DEFAULT_PREFIX",
    "LocalSink",
    "MessageShardWriter",
    "S3Sink",
    "Sink",
    "get_message_archive_prefix",
    "get_message_archive_sink",
    "apply_harvest_onto_media",
    "extract_harvest",
    "get_overlay",
    "hydrate_message_rows",
    "hydrate_message_rows_async",
    "is_harvest_stub",
    "is_part_shard_key",
    "merge_fat_fields",
    "message_archive_bucket",
    "message_archive_prefix",
    "message_archive_schema",
    "month_from_shard_key",
    "month_key",
    "month_prefix",
    "needs_body_hydrate",
    "overlay_key",
    "overlay_payload_from_message_row",
    "parse_month",
    "put_overlay",
    "read_parquet_row",
    "reset_message_archive_sink_for_tests",
    "shard_key",
]
