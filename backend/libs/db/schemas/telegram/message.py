from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue, model_validator

from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.slim_message_media import SlimTypeMessageMediaSchema
from telegram.slim_reactions import slim_reactions_from_full
from telegram.slim_reply_to import slim_reply_to_from_full
from .slim_reactions import SlimMessageReactionsSchema
from telegram.utils import normalize_tl_keys


def _slim_reactions_field(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    if "reactions" not in data:
        return data
    raw = data.get("reactions")
    if raw is None or raw == [] or raw == {}:
        data = {**data, "reactions": None}
        return data
    slim = slim_reactions_from_full(raw)
    return {**data, "reactions": slim.model_dump(mode="json") if slim is not None else None}


def _slim_reply_to_field(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    if "reply_to" not in data:
        return data
    raw = data.get("reply_to")
    if raw is None or raw == [] or raw == {}:
        return {**data, "reply_to": None}
    return {**data, "reply_to": slim_reply_to_from_full(raw)}


class TelegramMessageSchema(BaseModel):
    model_config = ConfigDict(ser_json_bytes="base64")

    @model_validator(mode="before")
    @classmethod
    def _normalize_tl_discriminators(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = normalize_tl_keys(data)
            data = _slim_reactions_field(data)
            return _slim_reply_to_field(data)
        return data

    id: UUID | None = None
    message_id: int
    source_external_id: str
    peer_type: TelegramPeerTypeSchema
    message_type: str | None = None
    content: str | None = None
    content_hash: str | None = None
    from_external_id: str | None = None
    out: bool = False
    date: datetime | None = None
    edit_date: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    reactions: SlimMessageReactionsSchema | None = None
    views: int | None = None
    media: SlimTypeMessageMediaSchema | None = None
    pinned: bool = False
    deleted: bool = False
    silent: bool = False
    mentioned: bool = False
    #: URL/TextUrl entity and/or MessageMediaWebPage (Telegram-style “has a link”).
    has_url: bool = False
    #: MessageEntityMention / MessageEntityMentionName in entities.
    has_mention: bool = False
    grouped_id: int | None = None
    action_type: str | None = None
    metadata: JsonValue | None = None
    action_data: JsonValue | None = None
    fwd_from: JsonValue | None = None
    reply_to: JsonValue | None = None
    harvester_external_id: str | None = None


class TelegramMessageEditRow(BaseModel):
    model_config = ConfigDict(ser_json_bytes="base64")

    @model_validator(mode="before")
    @classmethod
    def _normalize_tl_discriminators(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = normalize_tl_keys(data)
            return _slim_reactions_field(data)
        return data

    id: UUID | None = None
    message_id: int
    source_external_id: str
    content: str | None = None
    edit_date: datetime | None = None
    reactions: SlimMessageReactionsSchema | None = None
    media: SlimTypeMessageMediaSchema | None = None
    metadata: JsonValue | None = None
    harvester_external_id: str | None = None
    created_at: datetime | None = None
