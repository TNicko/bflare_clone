"""BGE-M3 CUDA text embedding HTTP service.

Exposes an Ollama-compatible ``POST /api/embed`` so ``jobs.embed_messages`` can
point ``EMBEDDING_SERVICE_URL`` here without client changes. Dense-only.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.text_embedding.config import ConfigFactory
from services.text_embedding.model import encode_texts, is_model_ready, preload_model
from utils.logger import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = ConfigFactory.get_config()
    configure_logging(level=cfg.log_level)
    log = logging.getLogger(__name__)
    log.info(
        "Text embedding service listening (loading BGE-M3 in background: model=%s dim=%s max_length=%s)",
        cfg.model_id,
        cfg.vector_dim,
        cfg.max_length,
    )

    async def _load_model() -> None:
        try:
            await asyncio.to_thread(preload_model, cfg)
            log.info(
                "BGE-M3 ready (model=%s dim=%s device max_length=%s)",
                cfg.model_id,
                cfg.vector_dim,
                cfg.max_length,
            )
        except Exception:
            log.exception("BGE-M3 preload failed — /api/embed will return 503 until fixed")
            raise

    load_task = asyncio.create_task(_load_model())
    app.state.model_load_task = load_task
    yield
    load_task.cancel()


app = FastAPI(title="Harvest Text Embedding", version="0.1.0", lifespan=lifespan)


class OllamaEmbedRequest(BaseModel):
    """Subset of Ollama ``/api/embed`` used by ``embedding.client.embed_texts``."""

    model: str | None = None
    input: str | list[str] = Field(..., min_length=1)


class OllamaEmbedResponse(BaseModel):
    embeddings: list[list[float]]


def _require_model_ready() -> None:
    if not is_model_ready():
        raise HTTPException(
            status_code=503,
            detail="BGE-M3 is still loading. Retry in a minute or check text-embedding logs.",
        )


def _normalize_inputs(raw: str | list[str]) -> list[str]:
    if isinstance(raw, str):
        texts = [raw]
    else:
        texts = list(raw)
    if not texts:
        raise HTTPException(status_code=400, detail="input must be a non-empty string or list")
    return texts


@app.get("/health")
def health() -> dict[str, str]:
    if is_model_ready():
        return {"status": "ok"}
    return {"status": "loading"}


@app.post("/api/embed", response_model=OllamaEmbedResponse)
def embed(body: OllamaEmbedRequest) -> OllamaEmbedResponse:
    _require_model_ready()
    cfg = ConfigFactory.get_config()
    texts = _normalize_inputs(body.input)
    try:
        vectors = encode_texts(cfg, texts)
    except Exception as exc:
        logging.getLogger(__name__).exception("Text encode failed (n=%s)", len(texts))
        raise HTTPException(status_code=500, detail="Text encoding failed") from exc
    return OllamaEmbedResponse(embeddings=vectors)


def main() -> None:
    import uvicorn

    cfg = ConfigFactory.get_config()
    uvicorn.run(
        "services.text_embedding.main:app",
        host=cfg.host,
        port=cfg.port,
        log_level=cfg.log_level.lower(),
    )


if __name__ == "__main__":
    main()
