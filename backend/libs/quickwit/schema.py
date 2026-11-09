"""Quickwit index config for the ``messages`` keyword index.

Keyword document shape for Quickwit ``messages``.
so the indexer can dual-write the same JSON docs to both backends.
"""

from __future__ import annotations

from typing import Any

from .constants import INDEX_CONFIG_VERSION, MESSAGES_INDEX


def messages_index_config(
    *,
    index_id: str = MESSAGES_INDEX,
    commit_timeout_secs: int = 10,
) -> dict[str, Any]:
    """Return a Quickwit ``IndexConfig`` body for ``POST /api/v1/indexes``.

    - Identifier / enum fields use the ``raw`` tokenizer (exact term filters).
    - ``content`` is tokenized with positions + fieldnorms so AND-of-terms + BM25 ``_score`` work.
    - ``date`` is the timestamp field (rfc3339 / iso8601) for time pruning on search/delete.
    - ``store_source`` keeps the original JSON for hit hydration keys (id + date).
    """
    return {
        "version": INDEX_CONFIG_VERSION,
        "index_id": index_id,
        "doc_mapping": {
            "mode": "strict",
            "field_mappings": [
                {"name": "id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "message_id", "type": "i64", "fast": True},
                {"name": "source_external_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "peer_type", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "message_type", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "from_external_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "harvester_external_id", "type": "text", "tokenizer": "raw", "fast": True},
                {
                    "name": "date",
                    "type": "datetime",
                    "fast": True,
                    "input_formats": ["rfc3339", "iso8601"],
                    "fast_precision": "seconds",
                },
                {"name": "views", "type": "i64", "fast": True},
                {
                    "name": "content",
                    "type": "text",
                    "tokenizer": "default",
                    "record": "position",
                    "fieldnorms": True,
                    "stored": True,
                },
            ],
            "timestamp_field": "date",
            "store_source": True,
        },
        "search_settings": {
            "default_search_fields": ["content"],
        },
        "indexing_settings": {
            "commit_timeout_secs": commit_timeout_secs,
        },
    }
