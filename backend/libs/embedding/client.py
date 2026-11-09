from __future__ import annotations

import httpx
from embedding.errors import EmbeddingServiceError


async def embed_texts(
    *,
    service_url: str,
    model: str,
    texts: list[str],
    timeout: float = 120.0,
    client: httpx.AsyncClient | None = None,
) -> list[list[float]]:
    """Request dense embedding vectors for one or more texts (Ollama ``/api/embed``).

    Pass a shared ``client`` to reuse connections across batches (recommended for
    long embed jobs). When omitted, a one-shot client is created per call.
    """
    if not texts:
        return []

    base = service_url.rstrip("/")
    owns_client = client is None
    http = client or httpx.AsyncClient(timeout=timeout)
    try:
        try:
            response = await http.post(
                f"{base}/api/embed",
                json={"model": model, "input": texts},
            )
            response.raise_for_status()
            data = response.json()
        except httpx.ConnectError as exc:
            raise EmbeddingServiceError(f"embedding service unreachable at {base!r}") from exc
        except httpx.TimeoutException as exc:
            raise EmbeddingServiceError(f"embedding service timed out at {base!r}") from exc
        except httpx.HTTPStatusError as exc:
            body = (exc.response.text or "").strip().replace("\n", " ")
            if len(body) > 500:
                body = body[:500] + "…"
            detail = f": {body}" if body else ""
            raise EmbeddingServiceError(
                f"embedding service returned HTTP {exc.response.status_code} at {base!r}"
                f" (batch_size={len(texts)}){detail}"
            ) from exc
        except httpx.HTTPError as exc:
            raise EmbeddingServiceError(f"embedding service request failed at {base!r}") from exc
    finally:
        if owns_client:
            await http.aclose()

    embeddings = data.get("embeddings")
    if not isinstance(embeddings, list) or len(embeddings) != len(texts):
        raise EmbeddingServiceError(f"unexpected embedding response from {base!r}: {data!r}")
    return [[float(x) for x in emb] for emb in embeddings]


async def embed_text(
    *,
    service_url: str,
    model: str,
    text: str,
    timeout: float = 120.0,
    client: httpx.AsyncClient | None = None,
) -> list[float]:
    """Request a single dense embedding vector."""
    vectors = await embed_texts(
        service_url=service_url,
        model=model,
        texts=[text],
        timeout=timeout,
        client=client,
    )
    return vectors[0]
