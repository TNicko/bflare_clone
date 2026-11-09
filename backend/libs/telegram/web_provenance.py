"""Markers for messages / coverage produced by public ``t.me/s`` scraping."""

from __future__ import annotations

from uuid import UUID

#: Stored on ``telegram_messages.harvester_external_id`` for web-scraped rows.
WEB_TME_HARVESTER_EXTERNAL_ID = "web_tme"

#: Stable UUID for the synthetic harvester that owns web fetch-coverage islands.
WEB_TME_HARVESTER_ID = UUID("a0000000-0000-4000-8000-000000776562")


def is_web_harvester_external_id(value: str | None) -> bool:
    return (value or "").strip() == WEB_TME_HARVESTER_EXTERNAL_ID
