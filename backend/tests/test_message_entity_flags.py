"""Tests for has_url / has_mention derivation from Telethon entities."""

from __future__ import annotations

from types import SimpleNamespace

from telethon.tl.types import (
    MessageEntityBold,
    MessageEntityMention,
    MessageEntityMentionName,
    MessageEntityTextUrl,
    MessageEntityUrl,
)

from telegram.message_entity_flags import message_entity_flags


def test_url_entity_sets_has_url() -> None:
    has_url, has_mention = message_entity_flags(
        entities=[MessageEntityUrl(offset=0, length=12)],
    )
    assert has_url is True
    assert has_mention is False


def test_text_url_entity_sets_has_url() -> None:
    has_url, has_mention = message_entity_flags(
        entities=[MessageEntityTextUrl(offset=0, length=4, url="https://example.com")],
    )
    assert has_url is True
    assert has_mention is False


def test_webpage_media_sets_has_url_without_entities() -> None:
    has_url, has_mention = message_entity_flags(
        entities=None,
        media=SimpleNamespace(tl="MessageMediaWebPage"),
    )
    assert has_url is True
    assert has_mention is False


def test_webpage_dict_sets_has_url() -> None:
    has_url, _ = message_entity_flags(media={"tl": "MessageMediaWebPage"})
    assert has_url is True


def test_mention_entity_sets_has_mention() -> None:
    has_url, has_mention = message_entity_flags(
        entities=[MessageEntityMention(offset=0, length=6)],
    )
    assert has_url is False
    assert has_mention is True


def test_mention_name_entity_sets_has_mention() -> None:
    _, has_mention = message_entity_flags(
        entities=[MessageEntityMentionName(offset=0, length=4, user_id=1)],
    )
    assert has_mention is True


def test_formatting_only_sets_neither() -> None:
    has_url, has_mention = message_entity_flags(
        entities=[MessageEntityBold(offset=0, length=3)],
        media={"tl": "MessageMediaPhoto"},
    )
    assert has_url is False
    assert has_mention is False


def test_url_and_mention_together() -> None:
    has_url, has_mention = message_entity_flags(
        entities=[
            MessageEntityUrl(offset=0, length=5),
            MessageEntityMention(offset=6, length=6),
        ],
    )
    assert has_url is True
    assert has_mention is True
