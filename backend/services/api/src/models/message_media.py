"""Shared message media preview model (kept free of view imports to avoid cycles)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

DownloadableMediaKind = Literal["image", "gif", "video", "audio", "document", "webpage"]
MessageMediaStatus = Literal["none", "ready", "pending", "unavailable"]
# Only meaningful when status == "unavailable".
MessageMediaFailureReason = Literal["expired", "missing", "failed"]


class MessageMediaPreview(BaseModel):
    """Lightweight message attachment preview for search / on-demand media fetch."""

    status: MessageMediaStatus
    kind: DownloadableMediaKind | None = None
    url: str | None = None
    filename: str | None = None
    content_type: str | None = None
    original_content_type: str | None = None
    width: int | None = None
    height: int | None = None
    size_bytes: int | None = None
    duration_seconds: float | None = None
    failure_reason: MessageMediaFailureReason | None = None
    # External page URL / site for MessageMediaWebPage link cards.
    page_url: str | None = None
    site_name: str | None = None
    description: str | None = None
    # Inline Telegram document/photo thumb (data URL) when the file itself is not harvested.
    thumb_url: str | None = None


class MessageMediaResponse(BaseModel):
    """GET /telegram/message_media envelope keyed by message UUID string."""

    items: dict[str, MessageMediaPreview]
