from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from db.schemas.general.primitives import PeerTypeSchema, TelegramPeerTypeSchema
from services.api.src.models.message_media import MessageMediaPreview
from telegram.mappers import get_b64_image_url_from_photo, profile_photo_media_kind

KeywordPeerTypeSchema = Literal["channel", "megagroup", "chat"]


class KeywordPeerView(BaseModel):
    """Lightweight peer (community) partial attached to keyword results.

    Keyed by ``external_id`` in the response so many messages from the same peer share one object.
    Category labels are intentionally omitted - keyword results don't need them.
    """

    external_id: str
    peer_type: PeerTypeSchema
    title: str
    username: str | None = None
    profile_photo_s3_uri: str | None = None
    profile_photo_media_kind: Literal["image", "video"] | None = None
    stripped_profile_photo: str | None = None
    is_verified: bool | None = None

    @classmethod
    def from_row(
        cls,
        row: Mapping[str, Any],
        *,
        peer_type: KeywordPeerTypeSchema,
    ) -> KeywordPeerView:
        external_id = str(row["external_id"])
        title = (row.get("title") or "").strip() or external_id
        return cls(
            external_id=external_id,
            peer_type=peer_type,
            title=title,
            username=(row.get("username") or None),
            profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            profile_photo_media_kind=profile_photo_media_kind(
                photo=row.get("photo"),
                profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            ),
            stripped_profile_photo=get_b64_image_url_from_photo(row.get("photo")),
            is_verified=bool(row["is_verified"]) if row.get("is_verified") is not None else None,
        )


class KeywordMessageView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    message_id: int
    source_external_id: str
    peer_type: TelegramPeerTypeSchema
    message_type: str | None = None
    content: str | None = None
    from_external_id: str | None = None
    date: datetime | None = None
    media: MessageMediaPreview | None = None
    fwd_from: dict[str, Any] | None = None
    reply_to: dict[str, Any] | None = None

    @classmethod
    def from_row(
        cls,
        row: Mapping[str, Any],
        *,
        media: MessageMediaPreview | None = None,
    ) -> KeywordMessageView:
        fwd = row.get("fwd_from")
        reply = row.get("reply_to")
        return cls(
            id=row["id"],
            message_id=int(row["message_id"]),
            source_external_id=str(row["source_external_id"]),
            peer_type=row["peer_type"],
            message_type=row.get("message_type"),
            content=row.get("content"),
            from_external_id=(str(row["from_external_id"]) if row.get("from_external_id") is not None else None),
            date=row.get("date"),
            media=media,
            fwd_from=dict(fwd) if isinstance(fwd, Mapping) else None,
            reply_to=dict(reply) if isinstance(reply, Mapping) else None,
        )
