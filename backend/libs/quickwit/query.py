"""Build Quickwit query-language strings for AND-of-terms keyword search."""

from __future__ import annotations

from datetime import datetime
from typing import Any

#: Fields we expect on each hit (from store_source / stored content).
KEYWORD_SOURCE_FIELDS = [
    "id",
    "message_id",
    "source_external_id",
    "peer_type",
    "message_type",
    "content",
    "from_external_id",
    "date",
]

# Quickwit reserved characters that must be escaped in unquoted terms.
_SPECIAL = set('+^`:{}"[]()~!\\* ')


def escape_term(value: str) -> str:
    """Escape a single query term for use in an unquoted clause."""
    return "".join(f"\\{ch}" if ch in _SPECIAL else ch for ch in value)


def tokenize_query(query: str) -> list[str]:
    """Split a user query into non-empty whitespace tokens (AND semantics)."""
    return [t for t in query.split() if t]


def messages_keyword_query(
    query: str,
    *,
    source_external_id: str | None = None,
    peer_type: str | list[str] | None = None,
) -> str:
    """AND-of-terms on ``content`` plus optional exact filters.

    Date windows are applied via Quickwit ``start_timestamp`` / ``end_timestamp`` (seconds),
    not in the query string — ISO datetimes contain ``:`` which breaks the query language.
    """
    tokens = tokenize_query(query)
    if not tokens:
        raise ValueError("query has no searchable tokens")

    clauses: list[str] = [f"content:{escape_term(t)}" for t in tokens]

    if source_external_id:
        clauses.append(f"source_external_id:{escape_term(source_external_id)}")

    if peer_type:
        if isinstance(peer_type, list):
            if len(peer_type) == 1:
                clauses.append(f"peer_type:{escape_term(peer_type[0])}")
            elif peer_type:
                inner = " OR ".join(f"peer_type:{escape_term(p)}" for p in peer_type)
                clauses.append(f"({inner})")
        else:
            clauses.append(f"peer_type:{escape_term(peer_type)}")

    return " AND ".join(clauses)


def delete_ids_query(ids: list[str]) -> str:
    """Term-set query matching documents by ``id`` (for delete-tasks)."""
    if not ids:
        raise ValueError("ids must be non-empty")
    if len(ids) == 1:
        return f"id:{escape_term(ids[0])}"
    joined = " ".join(escape_term(i) for i in ids)
    return f"id:IN [{joined}]"


def iso_to_unix_seconds(value: str | datetime | None) -> int | None:
    """Parse an ISO / datetime into unix seconds for Quickwit ``start_timestamp`` / ``end_timestamp``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        # Accept trailing Z.
        text = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    return int(dt.timestamp())


def messages_keyword_sort() -> str:
    """Relevance first, then most-recent.

    Quickwit REST expects a comma-separated string (not a JSON array).
    """
    return "_score,-date"


def hit_source(hit: dict[str, Any]) -> dict[str, Any]:
    """Normalize a Quickwit hit into a flat source dict (prefer store_source payload)."""
    if not isinstance(hit, dict):
        return {}
    # When store_source is on, the hit is typically the original document.
    # Some versions nest under `_source`; prefer top-level message fields when present.
    if "id" in hit and ("date" in hit or "content" in hit):
        return hit
    nested = hit.get("_source")
    if isinstance(nested, dict):
        return nested
    return hit
