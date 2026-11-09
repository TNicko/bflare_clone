"""Downloadable media kind classification for harvest coverage.

Operates on slim persisted media (``SlimTypeMessageMediaSchema``). Convert JSONB
dicts at the DB boundary with ``parse_slim_media`` — classifiers do not accept
raw dicts.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import TypeAdapter, ValidationError

from db.schemas.telegram.slim_message_media import (
    SlimMessageMediaDocumentSchema,
    SlimMessageMediaPhotoSchema,
    SlimMessageMediaWebPageSchema,
    SlimTypeMessageMediaSchema,
)

DownloadableKind = Literal["image", "gif", "video", "audio", "document", "webpage"]

_SLIM_ADAPTER = TypeAdapter(SlimTypeMessageMediaSchema)
logger = logging.getLogger(__name__)

_STICKER_MIMES = frozenset(
    {
        "application/x-tgsticker",
        "application/x-tgsticker-video",
    }
)

# Vendor ``image/*`` types that are CAD/vector drawings, not raster photos.
# Telegram (and some clients) label AutoCAD DWG as ``image/vnd.dwg``.
_NON_RASTER_IMAGE_MIMES = frozenset(
    {
        "image/vnd.dwg",
        "image/x-dwg",
        "image/vnd.dxf",
        "image/x-dxf",
        "image/vnd.dwf",
        "image/svg+xml",
        "image/svg",
    }
)


def is_raster_image_mime(mime: str | None) -> bool:
    """True for raster ``image/*`` MIMEs suitable for photo/phash/SigLIP pipelines.

    CAD drawings (DWG/DXF/…) and SVG are ``image/*`` by IANA name but are not
    bitmap images — classify those as documents instead.
    """
    if not mime:
        return False
    normalized = mime.strip().lower()
    if not normalized.startswith("image/"):
        return False
    return normalized not in _NON_RASTER_IMAGE_MIMES


def parse_slim_media(
    media: SlimTypeMessageMediaSchema | dict[str, Any] | str | None,
) -> SlimTypeMessageMediaSchema | None:
    """Validate DB JSONB (or an already-parsed model) into slim media.

    Accepts ``dict``, JSON ``str``, mappings, or an already-validated slim model.
    Empty ``{}`` and unparseable payloads return ``None``.
    """
    if media is None:
        return None
    # Already a slim pydantic model (has discriminator + model_dump).
    if hasattr(media, "tl") and hasattr(media, "model_dump") and not isinstance(media, Mapping):
        return media  # type: ignore[return-value]
    if isinstance(media, str):
        try:
            media = json.loads(media)
        except json.JSONDecodeError:
            return None
    if isinstance(media, Mapping) and not isinstance(media, dict):
        media = dict(media)
    if not isinstance(media, dict):
        return None
    if not media:
        return None
    try:
        return _SLIM_ADAPTER.validate_python(media)
    except ValidationError:
        logger.debug("slim media validation failed for payload keys=%s", sorted(media.keys()))
        return None


def _classify_document(doc: Any) -> DownloadableKind | None:
    mime = (getattr(doc, "mime_type", None) or "").lower()
    # Lottie / video sticker containers are not raster previews.
    if mime in _STICKER_MIMES:
        return None
    # Static stickers (typically image/webp) render like photos in chat.
    if getattr(doc, "sticker", None):
        return "image" if is_raster_image_mime(mime) else None
    if getattr(doc, "animated", None) or mime == "image/gif":
        return "gif"
    if getattr(doc, "voice", None) or mime.startswith("audio/"):
        return "audio"
    if getattr(doc, "video", None) or mime.startswith("video/"):
        return "video"
    if is_raster_image_mime(mime):
        return "image"
    return "document"


def _classify_webpage(page: Any) -> DownloadableKind | None:
    """Link-preview cards are their own UI kind — never harvested as attachments."""
    _ = page
    return "webpage"


def classify_downloadable_kind(
    media: SlimTypeMessageMediaSchema | None,
) -> DownloadableKind | None:
    """Return media kind for UI/filters, or None when not a file-like attachment.

    ``webpage`` is included for link-card rendering/filters but is **not** harvested —
    use ``is_downloadable_media`` for S3 harvest eligibility.

    Static raster stickers (``image/webp``, …) classify as ``image`` so chat can
    harvest and preview them. Lottie/video sticker containers stay excluded.
    """
    if media is None:
        return None
    tl = getattr(media, "tl", None)
    if tl == "MessageMediaPhoto" or isinstance(media, SlimMessageMediaPhotoSchema):
        return "image"
    if tl == "MessageMediaDocument" or isinstance(media, SlimMessageMediaDocumentSchema):
        return _classify_document(media)
    if tl == "MessageMediaWebPage" or isinstance(media, SlimMessageMediaWebPageSchema):
        return _classify_webpage(media)
    return None


def is_downloadable_media(media: SlimTypeMessageMediaSchema | None) -> bool:
    """True when media should be harvested to S3.

    Excludes TGS/video stickers, non-files, and ``MessageMediaWebPage`` link cards
    (site URLs with optional preview thumbs — not chat attachments).
    """
    kind = classify_downloadable_kind(media)
    return kind is not None and kind != "webpage"
