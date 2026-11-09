"""API helpers for query-time image embedding (reverse image search)."""

from __future__ import annotations

import logging

from embedding.errors import EmbeddingServiceError
from embedding.image_client import embed_image_bytes as _embed_image_bytes
from fastapi import HTTPException

from services.api.src.client_errors import IMAGE_EMBEDDING_UNAVAILABLE
from services.api.src.config import ConfigFactory
from utils.request_spans import timed

lg = logging.getLogger(__name__)


async def embed_search_image(image: bytes) -> list[float]:
    """Embed an uploaded image via the SigLIP HTTP service. Maps failures to HTTP 503."""
    cfg = ConfigFactory.get_config()
    lg.info(
        "image embedding: calling service (%s, %s bytes)…",
        cfg.image_embedding_service_url,
        len(image),
    )
    try:
        async with timed("embed", "image"):
            vector = await _embed_image_bytes(
                service_url=cfg.image_embedding_service_url,
                image=image,
            )
    except EmbeddingServiceError:
        lg.exception(
            "image embedding service failed (url=%s)",
            cfg.image_embedding_service_url,
        )
        raise HTTPException(
            status_code=503,
            detail=IMAGE_EMBEDDING_UNAVAILABLE,
        ) from None
    lg.info("image embedding: got vector dim=%s", len(vector))
    return vector
