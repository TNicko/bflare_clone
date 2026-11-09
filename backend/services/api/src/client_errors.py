"""Approved client-facing API error strings.

Keep infra names (Milvus, S3, Docker, URLs, env vars, session IDs) in logs only.
"""

from __future__ import annotations

# Search / retrieval
SEMANTIC_UNAVAILABLE = "Semantic search is temporarily unavailable. Please try again later."
SEMANTIC_FAILED = "Semantic search failed"
KEYWORD_SEARCH_UNAVAILABLE = "Keyword search is temporarily unavailable. Please try again later."
KEYWORD_SEARCH_FAILED = "Keyword search failed"
IMAGE_SEARCH_UNAVAILABLE = "Image search is temporarily unavailable. Please try again later."
IMAGE_SEARCH_FAILED = "Image search failed"
HASH_IMAGE_SEARCH_FAILED = "Image search failed"
PHONE_SEARCH_UNAVAILABLE = "Phone search is temporarily unavailable. Please try again later."
PHONE_SEARCH_FAILED = "Phone search failed"
TELEGRAM_NAME_SEARCH_UNAVAILABLE = "Telegram name search is temporarily unavailable. Please try again later."
TELEGRAM_NAME_SEARCH_FAILED = "Telegram name search failed"
SEARCH_UNAVAILABLE = "Search is temporarily unavailable. Please try again later."

# Embedding helpers (query-time)
EMBEDDING_UNAVAILABLE = SEMANTIC_UNAVAILABLE
IMAGE_EMBEDDING_UNAVAILABLE = IMAGE_SEARCH_UNAVAILABLE

# Auth / identity resolution (already used by username search — keep aligned)
USERNAME_SEARCH_UNAVAILABLE = "Username search temporarily unavailable"
USERNAME_SEARCH_FAILED = "Username search failed"

# Generic ops
SERVICE_UNAVAILABLE = "This service is temporarily unavailable. Please try again later."
RATE_LIMITED = "Too many requests. Try again shortly."


def rate_limited_retry(seconds: int) -> str:
    return f"Too many requests. Try again in {seconds} seconds."
