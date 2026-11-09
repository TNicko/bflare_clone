"""API helpers for query-time embedding (semantic search, graph, etc.)."""

from __future__ import annotations

import logging

from embedding.client import embed_text
from embedding.errors import EmbeddingServiceError
from fastapi import HTTPException

from services.api.src.client_errors import EMBEDDING_UNAVAILABLE
from services.api.src.config import ConfigFactory
from utils.request_spans import timed

lg = logging.getLogger(__name__)


async def embed_search_query(text: str) -> list[float]:
    """Embed a search query via the configured Ollama service. Maps failures to HTTP 503."""
    cfg = ConfigFactory.get_config()
    try:
        async with timed("embed", "text"):
            return await embed_text(
                service_url=cfg.embedding_service_url,
                model=cfg.embedding_model,
                text=text,
            )
    except EmbeddingServiceError:
        lg.exception(
            "embedding service failed (url=%s model=%s)",
            cfg.embedding_service_url,
            cfg.embedding_model,
        )
        raise HTTPException(
            status_code=503,
            detail=EMBEDDING_UNAVAILABLE,
        ) from None
