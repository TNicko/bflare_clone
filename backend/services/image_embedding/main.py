"""SigLIP2 image embedding HTTP service."""

from __future__ import annotations

import asyncio
import base64
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from utils.logger import configure_logging
from services.image_embedding.config import ConfigFactory
from services.image_embedding.model import encode_images, preload_model, is_model_ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = ConfigFactory.get_config()
    configure_logging(level=cfg.log_level)
    log = logging.getLogger(__name__)
    log.info(
        "Image embedding service listening (loading SigLIP vision tower in background: model=%s dim=%s)",
        cfg.model_id,
        cfg.vector_dim,
    )

    async def _load_model() -> None:
        try:
            await asyncio.to_thread(preload_model, cfg)
            log.info("SigLIP vision tower ready (model=%s dim=%s)", cfg.model_id, cfg.vector_dim)
        except Exception:
            log.exception("SigLIP vision preload failed — /embed will return 503 until fixed")
            raise

    load_task = asyncio.create_task(_load_model())
    app.state.model_load_task = load_task
    yield
    load_task.cancel()


app = FastAPI(title="Harvest Image Embedding", version="0.1.0", lifespan=lifespan)


class EmbedRequest(BaseModel):
    images_b64: list[str] = Field(min_length=1)


class EmbedResponse(BaseModel):
    model: str
    dim: int
    embeddings: list[list[float]]


def _require_model_ready() -> None:
    if not is_model_ready():
        raise HTTPException(
            status_code=503,
            detail="SigLIP vision tower is still loading. Retry in a minute or check image-embedding logs.",
        )


@app.get("/health")
def health() -> dict[str, str]:
    """Always 200 — use status field; Docker healthcheck waits for status=ok."""
    if is_model_ready():
        return {"status": "ok"}
    return {"status": "loading"}


@app.post("/embed", response_model=EmbedResponse)
def embed(body: EmbedRequest) -> EmbedResponse:
    _require_model_ready()
    cfg = ConfigFactory.get_config()
    try:
        raw_images = [base64.standard_b64decode(chunk) for chunk in body.images_b64]
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid base64 image data") from exc

    try:
        vectors = encode_images(cfg, raw_images)
    except Exception as exc:
        logging.getLogger(__name__).exception("Image encode failed")
        raise HTTPException(status_code=500, detail="Image encoding failed") from exc

    return EmbedResponse(model=cfg.model_id, dim=cfg.vector_dim, embeddings=vectors)


def main() -> None:
    import uvicorn

    cfg = ConfigFactory.get_config()
    uvicorn.run(
        "services.image_embedding.main:app",
        host=cfg.host,
        port=cfg.port,
        log_level=cfg.log_level.lower(),
    )


if __name__ == "__main__":
    main()
