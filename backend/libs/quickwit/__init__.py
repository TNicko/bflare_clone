"""Quickwit keyword index client."""

from __future__ import annotations

from quickwit.async_client import AsyncQuickwitClient, get_async_client, search_message_keyword
from quickwit.client import (
    QuickwitClient,
    bulk_delete_messages,
    bulk_index_messages,
    ensure_index,
    get_client,
    message_row_to_doc,
    search_hits,
)
from quickwit.constants import INDEX_CONFIG_VERSION, MESSAGES_INDEX

__all__ = [
    "AsyncQuickwitClient",
    "INDEX_CONFIG_VERSION",
    "MESSAGES_INDEX",
    "QuickwitClient",
    "bulk_delete_messages",
    "bulk_index_messages",
    "ensure_index",
    "get_async_client",
    "get_client",
    "message_row_to_doc",
    "search_hits",
    "search_message_keyword",
]
