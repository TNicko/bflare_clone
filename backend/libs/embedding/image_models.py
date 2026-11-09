"""Image embedding model slugs (embstore path + harvest.image_embedded keys)."""

from __future__ import annotations

from embedding.constants import (
    IMAGES_EMBED_MODEL,
    IMAGES_VECTOR_DIM,
    MOBILENET_EMBED_MODEL,
    MOBILENET_VECTOR_DIM,
)

SIGLIP_EMBED_MODEL = IMAGES_EMBED_MODEL
MOBILENET_EMBED_MODEL_SLUG = MOBILENET_EMBED_MODEL

IMAGE_EMBED_DIMS: dict[str, int] = {
    SIGLIP_EMBED_MODEL: IMAGES_VECTOR_DIM,
    MOBILENET_EMBED_MODEL_SLUG: MOBILENET_VECTOR_DIM,
}

_ALIASES: dict[str, str] = {
    "siglip": SIGLIP_EMBED_MODEL,
    "siglip2": SIGLIP_EMBED_MODEL,
    SIGLIP_EMBED_MODEL: SIGLIP_EMBED_MODEL,
    "google/siglip2-so400m-patch16-384": SIGLIP_EMBED_MODEL,
    "mobilenet": MOBILENET_EMBED_MODEL_SLUG,
    MOBILENET_EMBED_MODEL_SLUG: MOBILENET_EMBED_MODEL_SLUG,
}


def resolve_image_embed_model(raw: str | None) -> tuple[str, int]:
    """Return canonical model slug + vector dim (``siglip`` / ``mobilenet`` aliases allowed)."""
    key = (raw or SIGLIP_EMBED_MODEL).strip()
    model = _ALIASES.get(key) or _ALIASES.get(key.lower())
    if model is None:
        raise ValueError(
            f"Unknown image embedding model {raw!r}; use siglip / {SIGLIP_EMBED_MODEL} "
            f"or mobilenet / {MOBILENET_EMBED_MODEL_SLUG}"
        )
    return model, IMAGE_EMBED_DIMS[model]


def default_image_embed_service_url(model: str) -> str:
    if model == MOBILENET_EMBED_MODEL_SLUG:
        return "http://localhost:11436"
    return "http://localhost:11435"


def resolve_image_embed_service_url(
    model: str,
    *,
    service_url: str | None = None,
    mobilenet_service_url: str | None = None,
) -> str:
    """Pick the HTTP encoder. SigLIP uses ``IMAGE_EMBEDDING_SERVICE_URL``; MobileNet does not inherit it."""
    if model == MOBILENET_EMBED_MODEL_SLUG:
        raw = (mobilenet_service_url or "").strip() or default_image_embed_service_url(model)
    else:
        raw = (service_url or "").strip() or default_image_embed_service_url(model)
    return raw.rstrip("/")
