"""Read cold message bodies from Parquet parts (+ overlays)."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from .layout import DEFAULT_PREFIX
from .overlay import get_overlay, merge_fat_fields, parse_json_field

lg = logging.getLogger(__name__)


def extract_harvest(media: Any) -> dict[str, Any] | None:
    """Return ``media.harvest`` when it is a non-empty object."""
    if isinstance(media, str):
        media = parse_json_field(media)
    if not isinstance(media, dict):
        return None
    harvest = media.get("harvest")
    if isinstance(harvest, dict) and harvest:
        return dict(harvest)
    return None


def is_harvest_stub(media: Any) -> bool:
    """True when PG media is only a skinny ``{"harvest": ...}`` (or empty)."""
    if isinstance(media, str):
        media = parse_json_field(media)
    if media is None:
        return True
    if not isinstance(media, dict):
        return False
    if not media:
        return True
    return set(media.keys()) <= {"harvest"}


def apply_harvest_onto_media(row: dict[str, Any], harvest: dict[str, Any]) -> None:
    """Merge a PG harvest stub onto hydrated ``row["media"]`` (PG wins)."""
    media = row.get("media")
    if isinstance(media, str):
        media = parse_json_field(media)
    out: dict[str, Any] = dict(media) if isinstance(media, dict) else {}
    raw_existing = out.get("harvest")
    existing: dict[str, Any] = dict(raw_existing) if isinstance(raw_existing, dict) else {}
    out["harvest"] = {**existing, **harvest}
    row["media"] = out


def needs_body_hydrate(row: dict[str, Any]) -> bool:
    """True when row has an archive pointer and Postgres is missing fat body fields.

    Unstripped / hot rows keep ``content`` and full TL ``media`` in PG. Cold strip
    nulls content and leaves at most a skinny ``{"harvest": ...}`` stub — Parquet
    supplies the rest. A harvest stub alone must not skip hydrate.
    """
    key = row.get("archive_key")
    if not key:
        return False
    content = row.get("content")
    content_ok = content is not None and str(content).strip() != ""
    media_ok = not is_harvest_stub(row.get("media"))
    return not (content_ok and media_ok)


def read_parquet_row(sink: Any, archive_key: str, archive_row: int) -> dict[str, Any] | None:
    """Load one row by index from a monthly part."""
    import pyarrow.parquet as pq

    try:
        raw = sink.get_bytes(archive_key)
    except Exception:
        lg.warning("failed GET archive_key=%s", archive_key, exc_info=True)
        return None
    table = pq.read_table(pa_buffer(raw))
    if archive_row < 0 or archive_row >= table.num_rows:
        lg.warning(
            "archive_row out of range key=%s row=%s n=%s",
            archive_key,
            archive_row,
            table.num_rows,
        )
        return None
    batch = table.slice(archive_row, 1).to_pydict()
    out: dict[str, Any] = {}
    for name, values in batch.items():
        out[name] = values[0] if values else None
    for name in ("reactions", "media", "action_data", "metadata", "fwd_from", "reply_to"):
        if name in out:
            out[name] = parse_json_field(out[name])
    return out


def pa_buffer(data: bytes):
    import pyarrow as pa

    return pa.BufferReader(data)


def read_parquet_rows_for_indices(
    sink: Any,
    archive_key: str,
    indices: list[int],
) -> dict[int, dict[str, Any]]:
    """Read several row indices from one part (one S3 GET)."""
    import pyarrow.parquet as pq

    if not indices:
        return {}
    try:
        raw = sink.get_bytes(archive_key)
    except Exception:
        lg.warning("failed GET archive_key=%s", archive_key, exc_info=True)
        return {}
    table = pq.read_table(pa_buffer(raw))
    n = table.num_rows
    out: dict[int, dict[str, Any]] = {}
    for idx in indices:
        if idx < 0 or idx >= n:
            lg.warning("archive_row out of range key=%s row=%s n=%s", archive_key, idx, n)
            continue
        batch = table.slice(idx, 1).to_pydict()
        row = {name: (values[0] if values else None) for name, values in batch.items()}
        for name in ("reactions", "media", "action_data", "metadata", "fwd_from", "reply_to"):
            if name in row:
                row[name] = parse_json_field(row[name])
        out[idx] = row
    return out


def hydrate_message_rows(
    rows: list[dict[str, Any]],
    sink: Any | None,
    *,
    prefix: str = DEFAULT_PREFIX,
) -> int:
    """Fill missing fat fields from overlay ⊕ Parquet. Mutates ``rows``. Returns hydrate count.

    Postgres may keep a skinny ``{"harvest": ...}`` stub on cold rows; that harvest
    is re-applied after Parquet/overlay merge so S3 URIs are not lost.
    """
    if sink is None or not rows:
        return 0

    pending: list[dict[str, Any]] = [r for r in rows if needs_body_hydrate(r)]
    if not pending:
        return 0

    pg_harvest_by_id: dict[str, dict[str, Any]] = {}
    for row in pending:
        harvest = extract_harvest(row.get("media"))
        if harvest is not None:
            pg_harvest_by_id[str(row["id"])] = harvest

    hydrated = 0
    # Overlay first (per message).
    still: list[dict[str, Any]] = []
    for row in pending:
        ov = get_overlay(sink, str(row["id"]), prefix=prefix)
        if ov is not None:
            merge_fat_fields(row, ov)
            hydrated += 1
            # Overlay can restore content; only continue to Parquet when still stripped.
            if needs_body_hydrate(row):
                still.append(row)
        else:
            still.append(row)

    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in still:
        key = str(row.get("archive_key") or "")
        if key:
            by_key[key].append(row)

    for key, group in by_key.items():
        indices = []
        for row in group:
            idx = row.get("archive_row")
            if idx is None:
                continue
            indices.append(int(idx))
        loaded = read_parquet_rows_for_indices(sink, key, indices)
        for row in group:
            idx = row.get("archive_row")
            if idx is None:
                continue
            src = loaded.get(int(idx))
            if src is None:
                continue
            merge_fat_fields(row, src, only_if_missing=True)
            hydrated += 1

    for row in pending:
        harvest = pg_harvest_by_id.get(str(row["id"]))
        if harvest is not None:
            apply_harvest_onto_media(row, harvest)

    return hydrated


async def hydrate_message_rows_async(
    rows: list[dict[str, Any]],
    sink: Any | None,
    *,
    prefix: str = DEFAULT_PREFIX,
) -> int:
    """Fill cold bodies off the event loop.

    Parquet and S3 reads are synchronous.     API handlers must await this instead of
    ``hydrate_message_rows`` so one cold page cannot stall every other request.
    """
    return await asyncio.to_thread(hydrate_message_rows, rows, sink, prefix=prefix)
