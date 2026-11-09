"""Sparse post-archive edit/delete overlays (zstd JSON beside monthly Parquet)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from .layout import DEFAULT_PREFIX, overlay_key

lg = logging.getLogger(__name__)

_JSON_FIELDS = frozenset(
    {
        "reactions",
        "media",
        "action_data",
        "metadata",
        "fwd_from",
        "reply_to",
    }
)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _compress(data: bytes) -> bytes:
    """zstd with a 4-byte little-endian uncompressed length prefix (for pyarrow)."""
    import pyarrow as pa

    compressed = bytes(pa.compress(data, codec="zstd"))
    return len(data).to_bytes(4, "little") + compressed


def _decompress(data: bytes) -> bytes:
    import pyarrow as pa

    if len(data) < 4:
        raise ValueError("overlay payload too short")
    size = int.from_bytes(data[:4], "little")
    return bytes(pa.decompress(data[4:], decompressed_size=size, codec="zstd"))


def put_overlay(
    sink: Any,
    message_uuid: str,
    payload: dict[str, Any],
    *,
    prefix: str = DEFAULT_PREFIX,
) -> str:
    """Write overlay for ``message_uuid`` (telegram_messages.id). Returns object key."""
    key = overlay_key(message_uuid, prefix=prefix)
    body = json.dumps(payload, default=_json_default, separators=(",", ":")).encode("utf-8")
    sink.put_bytes(key, _compress(body))
    return key


def get_overlay(
    sink: Any,
    message_uuid: str,
    *,
    prefix: str = DEFAULT_PREFIX,
) -> dict[str, Any] | None:
    """Return overlay dict or None if missing."""
    key = overlay_key(message_uuid, prefix=prefix)
    try:
        raw = sink.get_bytes(key)
    except Exception:
        return None
    try:
        return json.loads(_decompress(raw).decode("utf-8"))
    except Exception:
        lg.warning("corrupt overlay key=%s", key, exc_info=True)
        return None


def overlay_payload_from_message_row(row: dict[str, Any]) -> dict[str, Any]:
    """Build overlay body from a harvest/upsert-shaped row (fat fields)."""
    out: dict[str, Any] = {}
    for name in (
        "id",
        "message_id",
        "source_external_id",
        "peer_type",
        "message_type",
        "content",
        "content_hash",
        "out",
        "date",
        "edit_date",
        "reactions",
        "views",
        "media",
        "pinned",
        "deleted",
        "silent",
        "mentioned",
        "has_url",
        "has_mention",
        "grouped_id",
        "action_type",
        "action_data",
        "metadata",
        "fwd_from",
        "reply_to",
        "from_external_id",
        "harvester_external_id",
    ):
        if name in row and row[name] is not None:
            out[name] = row[name]
    return out


def parse_json_field(value: Any) -> Any:
    """Parquet stores JSONB columns as UTF-8 text; normalize to Python objects."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return value
    return value


def _dest_field_missing(dest: dict[str, Any], name: str) -> bool:
    """True when ``dest[name]`` should be filled from Parquet (overlay already won)."""
    cur = dest.get(name)
    if name == "media":
        if cur is None:
            return True
        if isinstance(cur, str):
            cur = parse_json_field(cur)
        if not isinstance(cur, dict) or not cur:
            return True
        # Skinny harvest stub is not a full TL body.
        return set(cur.keys()) <= {"harvest"}
    if cur is None:
        return True
    if isinstance(cur, str) and cur.strip() == "":
        return True
    return False


def merge_fat_fields(
    dest: dict[str, Any],
    src: dict[str, Any],
    *,
    only_if_missing: bool = False,
) -> None:
    """Copy fat / display fields from ``src`` onto ``dest`` in place.

    When ``only_if_missing`` is True (Parquet after overlay), do not clobber
    fields already set on ``dest`` (e.g. edited content from an overlay).
    """
    for name in (
        "content",
        "content_hash",
        "message_type",
        "out",
        "edit_date",
        "reactions",
        "views",
        "media",
        "pinned",
        "deleted",
        "silent",
        "mentioned",
        "has_url",
        "has_mention",
        "grouped_id",
        "action_type",
        "action_data",
        "metadata",
        "fwd_from",
        "reply_to",
        "from_external_id",
        "harvester_external_id",
    ):
        if name not in src:
            continue
        if only_if_missing and not _dest_field_missing(dest, name):
            continue
        val = src[name]
        if name in _JSON_FIELDS:
            val = parse_json_field(val)
        dest[name] = val
