"""Stable usage meter keys. Kept free of API imports so callers can validate them."""

from __future__ import annotations

from enum import StrEnum


class Meter(StrEnum):
    SEARCH_NAME = "search.name"
    SEARCH_USERNAME = "search.username"
    SEARCH_PHONE = "search.phone"
    SEARCH_KEYWORD = "search.keyword"
    SEARCH_SEMANTIC = "search.semantic"
    SEARCH_IMAGE = "search.image"
    SEARCH_IMAGE_HASH = "search.image_hash"
    SEARCH_IMAGE_HASH_BY_MESSAGE = "search.image_hash_by_message"
    SEARCH_NAME_TELEGRAM = "search.name_telegram"
    SEARCH_BLUESKY = "search.bluesky"
    TELEGRAM_SYNC = "telegram.sync"
    TELEGRAM_RESOLVE = "telegram.resolve"
    TELEGRAM_MEDIA_DOWNLOAD = "telegram.media_download"


METERS = frozenset(Meter)
