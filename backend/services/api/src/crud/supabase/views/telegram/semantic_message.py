from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from db.schemas.general.primitives import TelegramPeerTypeSchema
from services.api.src.models.message_media import MessageMediaPreview


class SemanticMessageView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    message_id: int
    source_external_id: str
    peer_type: TelegramPeerTypeSchema
    message_type: str | None = None
    content: str | None = None
    from_external_id: str | None = None
    date: datetime | None = None
    similarity: float = Field(description="Cosine similarity (higher is more similar)")
    media: MessageMediaPreview | None = None
    fwd_from: dict[str, Any] | None = None
    reply_to: dict[str, Any] | None = None

    @classmethod
    def from_dense_search_row(
        cls,
        row: Mapping[str, Any] | Sequence[Any],
        *,
        media: MessageMediaPreview | None = None,
    ) -> SemanticMessageView:
        if isinstance(row, Mapping):
            fwd = row.get("fwd_from")
            reply = row.get("reply_to")
            return cls(
                id=row["id"],
                message_id=int(row["message_id"]),
                source_external_id=str(row["source_external_id"]),
                peer_type=row["peer_type"],
                message_type=row.get("message_type"),
                content=row.get("content"),
                from_external_id=row.get("from_external_id"),
                date=row.get("date"),
                similarity=float(row["similarity"]),
                media=media if media is not None else row.get("media_preview"),
                fwd_from=dict(fwd) if isinstance(fwd, Mapping) else None,
                reply_to=dict(reply) if isinstance(reply, Mapping) else None,
            )

        (
            row_id,
            message_id,
            source_external_id,
            peer_type,
            message_type,
            content,
            from_external_id,
            date,
            similarity,
        ) = row
        return cls(
            id=row_id,
            message_id=int(message_id),
            source_external_id=str(source_external_id),
            peer_type=peer_type,
            message_type=message_type,
            content=content,
            from_external_id=from_external_id,
            date=date,
            similarity=float(similarity),
            media=media,
        )
