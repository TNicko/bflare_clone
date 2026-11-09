"""Shared harvest JSON normalization for slim media payloads."""

from __future__ import annotations

from typing import Any

from db.schemas.telegram.slim_media import SlimHarvestMediaStateSchema


def _image_embedded(raw: Any) -> dict[str, bool] | None:
    if not isinstance(raw, dict):
        return None
    out = {str(key): bool(value) for key, value in raw.items() if str(key).strip()}
    return out or None


def slim_harvest_from_dict(harvest: dict[str, Any] | None) -> SlimHarvestMediaStateSchema | None:
    if not harvest:
        return None
    return SlimHarvestMediaStateSchema(
        s3_uri=harvest.get("s3_uri"),
        image_embedding_id=harvest.get("image_embedding_id"),
        image_embedded=_image_embedded(harvest.get("image_embedded")),
        phash=harvest.get("phash"),
        size_bytes=harvest.get("size_bytes"),
        motion_s3_uri=harvest.get("motion_s3_uri"),
        motion_size_bytes=harvest.get("motion_size_bytes"),
        content_type=harvest.get("content_type"),
        media_kind=harvest.get("media_kind"),
    )
