"""Persisted row schema for ``telegram_message_reactors``."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from db.schemas.telegram.tl import TelegramTLPeerTypeSchema, TypeReactionSchema


class TelegramMessageReactorSchema(BaseModel):
    """One reactor entry for a harvested message (full list when ``can_see_list``)."""

    model_config = ConfigDict(extra="forbid")

    message_id: UUID
    message_date: datetime
    source_external_id: str = Field(min_length=1)
    reactor_external_id: str
    peer_type: TelegramTLPeerTypeSchema
    reaction_key: str = Field(min_length=1)
    reaction: TypeReactionSchema
    reacted_at: datetime | None = None
