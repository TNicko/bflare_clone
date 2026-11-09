"""Canonical S3 layout + Arrow schema for cold telegram message Parquet.

Layout (immutable parts; never append in place)::

    {prefix}/dt=YYYY-MM/part-{uuid}.parquet
    {prefix}/overlays/{uuid}.json.zst          # stub for Phase 3b+ edits

    e.g.  messages/telegram/dt=2024-07/part-7f3a....parquet

``dt=YYYY-MM`` mirrors ``telegram_messages_pYYYY_MM`` and embstore months.
Overlays are documented here for path stability; readers land in Phase 3b.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

#: Default prefix under the harvest bucket (no leading/trailing slash).
DEFAULT_PREFIX = "messages/telegram"

_DT_RE = re.compile(r"/dt=(\d{4}-\d{2})/")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def month_from_shard_key(key: str) -> str | None:
    """Extract ``YYYY-MM`` from a part key containing ``/dt=YYYY-MM/``."""
    m = _DT_RE.search(key)
    return m.group(1) if m else None


def parse_month(month: str) -> str:
    """Normalize and validate ``YYYY-MM``."""
    m = month.strip()
    if not _MONTH_RE.match(m):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return m


def month_key(date: datetime) -> str:
    """``YYYY-MM`` partition key for a message ``date``."""
    return f"{date.year:04d}-{date.month:02d}"


def month_prefix(month: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """``{prefix}/dt=YYYY-MM``."""
    return f"{prefix.strip('/')}/dt={parse_month(month)}"


def shard_key(month: str, part_id: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """Full key for one Parquet part within a month."""
    return f"{month_prefix(month, prefix=prefix)}/part-{part_id}.parquet"


def overlay_key(message_id: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    """Sparse post-archive edit/delete overlay (Phase 3b+). ``message_id`` = row UUID."""
    return f"{prefix.strip('/')}/overlays/{str(message_id).strip().lower()}.json.zst"


def is_part_shard_key(key: str) -> bool:
    """True if ``key`` looks like a monthly ``part-*.parquet`` (not an overlay)."""
    return "/dt=" in key and "/part-" in key and key.endswith(".parquet") and "/overlays/" not in key


def message_archive_schema() -> pa.Schema:
    """Arrow schema for a full harvested message snapshot (pre-strip).

    JSONB columns are stored as UTF-8 JSON text for simple round-trip without
    Arrow extension types. UUIDs are strings.
    """
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("message_id", pa.int64(), nullable=False),
            pa.field("source_external_id", pa.string(), nullable=False),
            pa.field("peer_type", pa.string(), nullable=False),
            pa.field("message_type", pa.string(), nullable=True),
            pa.field("content", pa.string(), nullable=True),
            pa.field("content_hash", pa.string(), nullable=True),
            pa.field("out", pa.bool_(), nullable=True),
            pa.field("date", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("edit_date", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("reactions", pa.string(), nullable=True),
            pa.field("views", pa.int32(), nullable=True),
            pa.field("media", pa.string(), nullable=True),
            pa.field("pinned", pa.bool_(), nullable=True),
            pa.field("deleted", pa.bool_(), nullable=True),
            pa.field("silent", pa.bool_(), nullable=True),
            pa.field("mentioned", pa.bool_(), nullable=True),
            pa.field("has_url", pa.bool_(), nullable=True),
            pa.field("has_mention", pa.bool_(), nullable=True),
            pa.field("grouped_id", pa.int64(), nullable=True),
            pa.field("action_type", pa.string(), nullable=True),
            pa.field("action_data", pa.string(), nullable=True),
            pa.field("metadata", pa.string(), nullable=True),
            pa.field("fwd_from", pa.string(), nullable=True),
            pa.field("reply_to", pa.string(), nullable=True),
            pa.field("from_external_id", pa.string(), nullable=True),
            pa.field("harvester_external_id", pa.string(), nullable=True),
        ]
    )
