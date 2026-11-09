"""Shared slim shapes for harvested media JSONB (message media, profile photos, etc.)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class SlimHarvestMediaStateSchema(BaseModel):
    """Pipeline + asset metadata attached to a harvested image or video."""

    model_config = ConfigDict(extra="forbid")

    s3_uri: str | None = None
    #: Browser-safe JPEG (or other web raster) when ``s3_uri`` stores HEIF/HEIC master bytes.
    display_s3_uri: str | None = None
    image_embedding_id: UUID | None = None
    #: Per-model done flags, e.g. ``{"siglip2-so400m-p16-384": true, "mobilenet-v3-large": true}``.
    image_embedded: dict[str, bool] | None = None
    phash: str | None = None
    size_bytes: int | None = None
    display_size_bytes: int | None = None
    motion_s3_uri: str | None = None
    motion_size_bytes: int | None = None
    #: MIME of the object at ``s3_uri`` (original Telegram / file bytes).
    content_type: str | None = None
    #: MIME of the object at ``display_s3_uri`` (typically ``image/jpeg``).
    display_content_type: str | None = None
    media_kind: str | None = None
    #: EXIF + basic file tags extracted from the original at harvest time.
    exif: dict[str, Any] | None = None
