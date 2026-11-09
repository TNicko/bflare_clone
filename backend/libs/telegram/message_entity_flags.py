"""Derive cheap message inventory flags from Telethon entities + slim media.

``has_url`` matches Telegram's channel “links” idea: a URL/TextUrl entity and/or
a ``MessageMediaWebPage`` preview card.

``has_mention`` is true when the text contains an @username mention entity
(``MessageEntityMention``) or a name-mention (``MessageEntityMentionName``).

These are booleans for inventory/filters — not a substitute for storing full
entities when you need the TextUrl target or mention username later.
"""

from __future__ import annotations

from typing import Any

from telethon.tl.types import (
    MessageEntityMention,
    MessageEntityMentionName,
    MessageEntityTextUrl,
    MessageEntityUrl,
)

_URL_ENTITY_TYPES = (MessageEntityUrl, MessageEntityTextUrl)
_MENTION_ENTITY_TYPES = (MessageEntityMention, MessageEntityMentionName)


def message_has_url_entity(entities: Any) -> bool:
    if not entities:
        return False
    return any(isinstance(ent, _URL_ENTITY_TYPES) for ent in entities)


def message_has_mention_entity(entities: Any) -> bool:
    if not entities:
        return False
    return any(isinstance(ent, _MENTION_ENTITY_TYPES) for ent in entities)


def media_is_webpage(media: Any) -> bool:
    """True for slim media models/dicts with ``tl == MessageMediaWebPage``."""
    if media is None:
        return False
    tl = getattr(media, "tl", None)
    if tl is None and isinstance(media, dict):
        tl = media.get("tl")
    return tl == "MessageMediaWebPage"


def message_entity_flags(
    *,
    entities: Any = None,
    media: Any = None,
) -> tuple[bool, bool]:
    """Return ``(has_url, has_mention)`` for a harvested message."""
    has_url = message_has_url_entity(entities) or media_is_webpage(media)
    has_mention = message_has_mention_entity(entities)
    return has_url, has_mention
