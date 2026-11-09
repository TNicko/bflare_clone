from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.models.message_media import MessageMediaPreview


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


class TelegramMessageEditView(BaseModel):
    id: UUID | None = None
    message_id: int
    source_external_id: str
    content: str | None = None
    edit_date: datetime | None = None
    reactions: dict[str, Any] | None = None
    read_by: list[Any] = Field(default_factory=list)
    delivered_to: list[Any] = Field(default_factory=list)
    media: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any], *, source_external_id: str) -> TelegramMessageEditView:
        return cls(
            id=row.get("id"),
            message_id=row["message_id"],
            source_external_id=source_external_id,
            content=row.get("content"),
            edit_date=row.get("edit_date"),
            reactions=_dict_or_none(row.get("reactions")),
            media=_dict_or_none(row.get("media")),
            metadata=_dict_or_none(row.get("metadata")),
            created_at=row.get("created_at"),
        )


class TelegramMessageView(BaseModel):
    id: UUID | None = None
    message_id: int
    target_external_id: str
    source_external_id: str
    peer_type: PeerTypeSchema | None = None
    message_type: str | None = None
    content: str | None = None
    from_external_id: str | None = None
    out: bool = False
    date: datetime | None = None
    edit_date: datetime | None = None
    reactions: dict[str, Any] | None = None
    read_by: list[Any] = Field(default_factory=list)
    delivered_to: list[Any] = Field(default_factory=list)
    media: dict[str, Any] | None = None
    media_preview: MessageMediaPreview | None = None
    views: int | None = None
    read_count: int = 0
    delivered_count: int = 0
    pinned: bool = False
    deleted: bool = False
    silent: bool = False
    mentioned: bool = False
    grouped_id: int | None = None
    fwd_from: dict[str, Any] | None = None
    reply_to: dict[str, Any] | None = None
    action_type: str | None = None
    action_data: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    history: list[TelegramMessageEditView] = Field(default_factory=list)

    @classmethod
    def from_row(
        cls,
        row: Mapping[str, Any],
        *,
        edits: list[Mapping[str, Any]],
        source_external_id: str,
    ) -> TelegramMessageView:
        views = row.get("views")
        history = [TelegramMessageEditView.from_row(edit, source_external_id=source_external_id) for edit in edits]
        raw_id = row.get("id")
        return cls(
            id=raw_id if isinstance(raw_id, UUID) else (UUID(str(raw_id)) if raw_id is not None else None),
            message_id=row["message_id"],
            target_external_id=source_external_id,
            source_external_id=row["source_external_id"],
            peer_type=row.get("peer_type"),
            message_type=row.get("message_type"),
            content=row.get("content"),
            from_external_id=row.get("from_external_id"),
            # Harvester-sent messages may be stored with Telegram's `out=true`,
            # but the product user is the analyst — never expose them as ours.
            out=False,
            date=row.get("date"),
            edit_date=row.get("edit_date"),
            reactions=_dict_or_none(row.get("reactions")),
            media=_dict_or_none(row.get("media")),
            views=views,
            read_count=int(views) if views is not None else 0,
            pinned=bool(row.get("pinned", False)),
            deleted=bool(row.get("deleted", False)),
            silent=bool(row.get("silent", False)),
            mentioned=bool(row.get("mentioned", False)),
            grouped_id=row.get("grouped_id"),
            fwd_from=_dict_or_none(row.get("fwd_from")),
            reply_to=_dict_or_none(row.get("reply_to")),
            action_type=row.get("action_type"),
            action_data=_dict_or_none(row.get("action_data")),
            metadata=_dict_or_none(row.get("metadata")),
            history=history,
        )
