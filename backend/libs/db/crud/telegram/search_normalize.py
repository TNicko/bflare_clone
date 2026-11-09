"""NFKC search-text helpers for peer name/username/title matching.

Name search is character matching (ILIKE / trigram). The only transform shared
with embedding preprocess is :func:`embedding.preprocess.normalize_text`
(NFKC + strip + collapse whitespace), applied to both the query and stored
``*_search`` columns.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from embedding.preprocess import normalize_text

__all__ = [
    "normalize_text",
    "build_user_name_search",
    "build_username_search",
    "build_title_search",
    "build_usernames_search",
    "attach_user_search_fields",
    "attach_channel_search_fields",
    "attach_chat_search_fields",
]


def build_user_name_search(
    first_name: str | None,
    last_name: str | None,
) -> str | None:
    """Match ``coalesce(first,'') || ' ' || coalesce(last,'')`` then NFKC."""
    return normalize_text(f"{first_name or ''} {last_name or ''}")


def build_username_search(username: str | None) -> str | None:
    return normalize_text(username)


def build_title_search(title: str | None) -> str | None:
    return normalize_text(title)


def build_usernames_search(usernames: list[Any] | None) -> str | None:
    """Space-joined active handles from ``usernames`` JSON only (NFKC'd).

    Primary ``username`` is searched via ``username_search``.
    """
    parts: list[str] = []
    seen: set[str] = set()

    if not usernames:
        return None

    for entry in usernames:
        if not isinstance(entry, Mapping):
            continue
        if entry.get("active") is False:
            continue
        raw = entry.get("username")
        if not isinstance(raw, str):
            continue
        normalized = normalize_text(raw)
        if normalized is None:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        parts.append(normalized)

    return normalize_text(" ".join(parts)) if parts else None


def _merged(
    values: Mapping[str, Any],
    old_row: Mapping[str, Any] | None,
    key: str,
) -> Any:
    if key in values:
        return values[key]
    if old_row is not None:
        return old_row.get(key)
    return None


def attach_user_search_fields(
    values: dict[str, Any],
    *,
    old_row: Mapping[str, Any] | None,
) -> None:
    """Add ``name_search`` / ``username_search`` when source fields change."""
    if {"first_name", "last_name"} & values.keys():
        values["name_search"] = build_user_name_search(
            _merged(values, old_row, "first_name"),
            _merged(values, old_row, "last_name"),
        )
    if "username" in values:
        values["username_search"] = build_username_search(values["username"])


def attach_channel_search_fields(
    values: dict[str, Any],
    *,
    old_row: Mapping[str, Any] | None,
) -> None:
    """Add title/username/usernames search columns when source fields change."""
    if "title" in values:
        values["title_search"] = build_title_search(values["title"])
    if "username" in values:
        values["username_search"] = build_username_search(values["username"])
    if "usernames" in values:
        values["usernames_search"] = build_usernames_search(values["usernames"])


def attach_chat_search_fields(
    values: dict[str, Any],
    *,
    old_row: Mapping[str, Any] | None = None,
) -> None:
    """Add ``title_search`` when ``title`` changes."""
    _ = old_row
    if "title" in values:
        values["title_search"] = build_title_search(values["title"])
