"""Map full TL MessageReplyHeader / MessageReplyStoryHeader to slim persisted shapes."""

from __future__ import annotations

from typing import Any

from db.schemas.telegram.slim_reply_to import (
    SlimMessageReplyHeaderSchema,
    SlimMessageReplyStoryHeaderSchema,
    SlimReplyFromSchema,
)
from db.schemas.telegram.tl import TelegramPeerRefSchema
from pydantic import ValidationError

from telegram.utils import normalize_tl_keys, peer_ref_from_telethon


def _peer_ref(raw: Any) -> TelegramPeerRefSchema | None:
    if raw is None:
        return None
    try:
        if isinstance(raw, dict):
            return TelegramPeerRefSchema.model_validate(raw)
        ref = peer_ref_from_telethon(raw)
        if ref is None:
            return None
        return TelegramPeerRefSchema.model_validate(ref)
    except ValidationError:
        return None


def _truthy_flag(value: Any) -> bool | None:
    if value is True:
        return True
    return None


def _slim_reply_from(raw: Any) -> SlimReplyFromSchema | None:
    if not isinstance(raw, dict):
        return None
    data = normalize_tl_keys(raw)
    slim = SlimReplyFromSchema(
        date=data.get("date"),
        from_id=_peer_ref(data.get("from_id")),
        from_name=data.get("from_name") or None,
        channel_post=data.get("channel_post") if isinstance(data.get("channel_post"), int) else None,
        post_author=data.get("post_author") or None,
    )
    dumped = slim.model_dump(exclude_none=True)
    if not dumped:
        return None
    return slim


def slim_reply_to_from_full(raw: Any) -> dict[str, Any] | None:
    """Return slim reply_to dict (exclude nulls), or None if empty / unknown."""
    if raw is None:
        return None
    if isinstance(raw, (SlimMessageReplyHeaderSchema, SlimMessageReplyStoryHeaderSchema)):
        return raw.model_dump(mode="json", exclude_none=True) or None
    if not isinstance(raw, dict):
        return None

    data = normalize_tl_keys(raw)
    if data == {} or data == []:
        return None

    tl = data.get("tl")
    if tl == "MessageReplyStoryHeader":
        peer = _peer_ref(data.get("peer"))
        story_id = data.get("story_id")
        if peer is None or not isinstance(story_id, int):
            return None
        return SlimMessageReplyStoryHeaderSchema(peer=peer, story_id=story_id).model_dump(
            mode="json", exclude_none=True
        )

    # MessageReplyHeader (default) — drop reply_media, quote_entities, nulls, empties.
    msg_id = data.get("reply_to_msg_id")
    top_id = data.get("reply_to_top_id")
    slim = SlimMessageReplyHeaderSchema(
        reply_to_msg_id=msg_id if isinstance(msg_id, int) else None,
        reply_to_top_id=top_id if isinstance(top_id, int) else None,
        reply_to_peer_id=_peer_ref(data.get("reply_to_peer_id")),
        forum_topic=_truthy_flag(data.get("forum_topic")),
        reply_to_scheduled=_truthy_flag(data.get("reply_to_scheduled")),
        quote=_truthy_flag(data.get("quote")),
        quote_text=(data.get("quote_text") or None) if isinstance(data.get("quote_text"), str) else None,
        quote_offset=data.get("quote_offset") if isinstance(data.get("quote_offset"), int) else None,
        todo_item_id=data.get("todo_item_id") if isinstance(data.get("todo_item_id"), int) else None,
        reply_from=_slim_reply_from(data.get("reply_from")),
    )
    out = slim.model_dump(mode="json", exclude_none=True)
    return out or None
