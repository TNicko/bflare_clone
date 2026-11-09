"""Compute and persist ``harvest.phash`` for a stored message image.

Shared image pHash helper for the Harvest API.
Postgres ``telegram_image_blobs`` / harvest.phash is the source of truth for Hamming
search; Milvus is no longer upserted on demand.
"""

from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

from aws.s3 import download_s3_object_to_path
from db.crud.telegram.message_media_write import persist_message_media
from phash.core import phash_hex_from_bytes
from telegram.media import harvest_s3_uri, merge_harvest_into_media_dict
from telegram.media_kinds import parse_slim_media

lg = logging.getLogger(__name__)


def _phash_hex_from_media_raw(media: Any) -> str | None:
    if not isinstance(media, dict):
        return None
    harvest = media.get("harvest")
    if not isinstance(harvest, dict):
        return None
    raw = harvest.get("phash")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _s3_uri_from_media_raw(media: Any) -> str | None:
    if not isinstance(media, dict):
        return None
    harvest = media.get("harvest")
    if not isinstance(harvest, dict):
        return None
    uri = harvest.get("s3_uri")
    if not isinstance(uri, str):
        return None
    text = uri.strip()
    return text or None


async def ensure_message_image_phash(
    conn: Any,
    *,
    row: dict[str, Any],
    region: str,
    milvus_client: Any = None,  # unused; kept for call-site compatibility
) -> str | None:
    """Return ``harvest.phash`` for a message, computing from S3 when missing.

    When computed: persists Postgres ``harvest.phash``. Returns ``None`` when there
    is no S3 image or hashing fails.
    """
    del milvus_client  # formerly dual-wrote Milvus; Postgres Hamming is SoT
    media = row.get("media")
    existing = _phash_hex_from_media_raw(media)
    if existing:
        return existing

    slim = parse_slim_media(media)
    s3_uri = harvest_s3_uri(slim) if slim is not None else None
    if not s3_uri:
        s3_uri = _s3_uri_from_media_raw(media)
    if not s3_uri:
        return None

    tmp_dir = tempfile.mkdtemp(prefix=f"phash_{row.get('id')}_")
    try:
        local = Path(tmp_dir) / "image.bin"
        await download_s3_object_to_path(s3_uri, local, region=region)
        phash_hex = phash_hex_from_bytes(local.read_bytes())
    except Exception:
        lg.exception(
            "pHash from S3 failed message_id=%s",
            row.get("id"),
        )
        return None
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if not phash_hex:
        return None

    updated_media = merge_harvest_into_media_dict(
        slim if slim is not None else (media if isinstance(media, dict) else {}),
        phash=phash_hex,
    )
    message_date = row.get("date")
    if message_date is None:
        lg.warning("Cannot persist phash without date message_id=%s", row.get("id"))
        return phash_hex
    try:
        await persist_message_media(
            conn,
            row_id=row["id"],
            date=message_date,
            media=updated_media,
        )
    except Exception:
        lg.exception("Failed to persist computed phash message_id=%s", row.get("id"))
        return phash_hex

    return phash_hex
