"""Helpers for community ``bot_info`` user ids."""

from __future__ import annotations

from typing import Any


def bot_external_ids_from_bot_info(bot_info: Any) -> list[str]:
    """Ordered unique Telegram user ids from channel ``bot_info``."""
    if not bot_info:
        return []
    ordered: list[str] = []
    seen: set[str] = set()
    for entry in bot_info:
        user_id = getattr(entry, "user_id", None)
        if user_id is None and isinstance(entry, dict):
            user_id = entry.get("user_id")
        if user_id is None:
            continue
        external_id = str(user_id)
        if external_id in seen:
            continue
        seen.add(external_id)
        ordered.append(external_id)
    return ordered
