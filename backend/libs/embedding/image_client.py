"""HTTP client for SigLIP / MobileNet ``/embed`` (same JSON payload)."""

from __future__ import annotations

import base64

import httpx

from embedding.errors import EmbeddingServiceError


async def embed_images_bytes(
    *,
    service_url: str,
    images: list[bytes],
    timeout: float = 120.0,
) -> list[list[float]]:
    """Request L2-normalized dense vectors for one or more images."""
    if not images:
        return []

    base = service_url.rstrip("/")
    payload = {
        "images_b64": [base64.standard_b64encode(img).decode("ascii") for img in images],
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(f"{base}/embed", json=payload)
            response.raise_for_status()
            data = response.json()
    except httpx.ConnectError as exc:
        raise EmbeddingServiceError(
            f"image embedding service unreachable at {base!r} "
            "(container down, or still booting — SigLIP loads on first start and can take several minutes)"
        ) from exc
    except httpx.TimeoutException as exc:
        raise EmbeddingServiceError(f"image embedding service timed out at {base!r}") from exc
    except httpx.HTTPStatusError as exc:
        detail = (exc.response.text or "").strip() or exc.response.reason_phrase
        if exc.response.status_code == 503:
            raise EmbeddingServiceError(f"image embedding service not ready at {base!r}: {detail}") from exc
        raise EmbeddingServiceError(
            f"image embedding service HTTP {exc.response.status_code} at {base!r}: {detail}"
        ) from exc
    except httpx.HTTPError as exc:
        raise EmbeddingServiceError(f"image embedding service request failed at {base!r}") from exc

    embeddings = data.get("embeddings")
    if not isinstance(embeddings, list) or len(embeddings) != len(images):
        raise EmbeddingServiceError(f"unexpected image embedding response from {base!r}: {data!r}")
    return [[float(x) for x in emb] for emb in embeddings]


async def embed_image_bytes(
    *,
    service_url: str,
    image: bytes,
    timeout: float = 120.0,
) -> list[float]:
    """Request a single image embedding vector."""
    vectors = await embed_images_bytes(
        service_url=service_url,
        images=[image],
        timeout=timeout,
    )
    return vectors[0]
