"""NFKC search-text helpers for Bluesky actor matching."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from embedding.preprocess import normalize_text

from db.schemas.bluesky.actor import normalize_handle

__all__ = [
    "normalize_text",
    "build_display_name_search",
    "build_handle_search",
    "attach_actor_search_fields",
]


def build_display_name_search(display_name: str | None) -> str | None:
    return normalize_text(display_name)


def build_handle_search(handle: str | None) -> str | None:
    return normalize_text(normalize_handle(handle))


def attach_actor_search_fields(
    values: dict[str, Any],
    *,
    old_row: Mapping[str, Any] | None = None,
) -> None:
    """Set ``display_name_search`` / ``handle_search`` from upsert values (or prior row)."""
    display_name = values["display_name"] if "display_name" in values else (old_row or {}).get("display_name")
    handle = values["handle"] if "handle" in values else (old_row or {}).get("handle")
    if "display_name" in values or old_row is None:
        values["display_name_search"] = build_display_name_search(
            display_name if isinstance(display_name, str) else None
        )
    if "handle" in values or old_row is None:
        values["handle_search"] = build_handle_search(handle if isinstance(handle, str) else None)
