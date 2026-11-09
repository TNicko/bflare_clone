"""Slim message reply_to JSONB (ids / quote / peer; no reply_media)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from .tl import TelegramPeerRefSchema


class SlimReplyFromSchema(BaseModel):
    """Slim ``MessageFwdHeader`` subset used inside reply headers."""

    model_config = ConfigDict(extra="forbid")

    date: datetime | None = None
    from_id: TelegramPeerRefSchema | None = None
    from_name: str | None = None
    channel_post: int | None = None
    post_author: str | None = None


class SlimMessageReplyHeaderSchema(BaseModel):
    """Persisted shape for ``telegram_messages.reply_to`` (MessageReplyHeader)."""

    model_config = ConfigDict(extra="forbid")

    reply_to_msg_id: int | None = None
    reply_to_top_id: int | None = None
    reply_to_peer_id: TelegramPeerRefSchema | None = None
    forum_topic: bool | None = None
    reply_to_scheduled: bool | None = None
    quote: bool | None = None
    quote_text: str | None = None
    quote_offset: int | None = None
    todo_item_id: int | None = None
    reply_from: SlimReplyFromSchema | None = None


class SlimMessageReplyStoryHeaderSchema(BaseModel):
    """Persisted shape for story replies (``MessageReplyStoryHeader``)."""

    model_config = ConfigDict(extra="forbid")

    peer: TelegramPeerRefSchema
    story_id: int = Field(ge=0)
