from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from db.schemas.general.primitives import TelegramPeerTypeSchema
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelView,
    TelegramChatView,
    TelegramMegagroupView,
)
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.models.message_media import MessageMediaPreview


class ProjectAlertView(BaseModel):
    id: UUID
    alert_name: str | None = None
    alert_type: str
    alert_phrase: str
    alert_user: str | None = None


class ProjectReportView(BaseModel):
    id: UUID
    report_name: str | None = None
    report_location: str


class ProjectMessageMappingView(BaseModel):
    """Pinned message mapping, optionally hydrated for dashboard display."""

    id: UUID
    project: UUID
    message: UUID
    message_id: int | None = None
    source_external_id: str | None = None
    peer_type: TelegramPeerTypeSchema | None = None
    message_type: str | None = None
    content: str | None = None
    from_external_id: str | None = None
    date: datetime | None = None
    media: MessageMediaPreview | None = None
    fwd_from: dict[str, Any] | None = None
    reply_to: dict[str, Any] | None = None
    peer_title: str | None = None
    peer_username: str | None = None
    peer_profile_photo_s3_uri: str | None = None
    peer_profile_photo_media_kind: str | None = None
    peer_stripped_profile_photo: str | None = None


class ProjectView(BaseModel):
    id: UUID
    name: str
    description: str | None = None
    created_at: datetime | None = None
    telegram_channels: list[TelegramChannelView] = Field(default_factory=list)
    telegram_megagroups: list[TelegramMegagroupView] = Field(default_factory=list)
    telegram_chats: list[TelegramChatView] = Field(default_factory=list)
    telegram_users: list[UserPartialView] = Field(default_factory=list)
    telegram_messages: list[ProjectMessageMappingView] = Field(default_factory=list)
    """Peers referenced by pinned messages (source / forward / reply), keyed by external_id."""
    telegram_message_peers: dict[str, KeywordPeerView] = Field(default_factory=dict)
    """Users referenced by pinned message replies/senders, keyed by external_id."""
    telegram_message_users: dict[str, UserPartialView] = Field(default_factory=dict)
    alerts: list[ProjectAlertView] = Field(default_factory=list)
    reports: list[ProjectReportView] = Field(default_factory=list)
