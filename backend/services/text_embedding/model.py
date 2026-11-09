"""BGE-M3 dense text encoder (FlagEmbedding, CUDA). Loaded once per process."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from services.text_embedding.config import Config

logger = logging.getLogger(__name__)

_load_lock = threading.Lock()
_model: Any | None = None
_device_label: str | None = None


def is_model_ready() -> bool:
    return _model is not None


def _resolve_device(requested: str | None) -> str:
    import torch

    raw = (requested or "").strip().lower()
    if raw in ("", "auto"):
        return "cuda" if torch.cuda.is_available() else "cpu"
    if raw.startswith("cuda") and not torch.cuda.is_available():
        logger.warning("TEXT_EMBEDDING_DEVICE=%s requested but CUDA unavailable; using CPU", raw)
        return "cpu"
    return raw


def _load(cfg: Config) -> tuple[Any, str]:
    from FlagEmbedding import BGEM3FlagModel

    device = _resolve_device(getattr(cfg, "device", None))
    logger.info(
        "Loading BGE-M3 %s on %s (fp16, dense-only, max_length=%s)",
        cfg.model_id,
        device,
        cfg.max_length,
    )
    # devices= accepts "cuda:0" / "cpu"; FlagEmbedding handles placement.
    model = BGEM3FlagModel(
        cfg.model_id,
        use_fp16=device != "cpu",
        devices=device,
    )
    logger.info("BGE-M3 ready on %s", device)
    return model, device


def preload_model(cfg: Config) -> None:
    global _model, _device_label
    if _model is not None:
        return
    with _load_lock:
        if _model is not None:
            return
        _model, _device_label = _load(cfg)


def encode_texts(cfg: Config, texts: list[str]) -> list[list[float]]:
    """Dense 1024-d vectors; L2-normalized by FlagEmbedding defaults."""
    preload_model(cfg)
    assert _model is not None
    if not texts:
        return []

    out = _model.encode(
        texts,
        batch_size=max(1, cfg.encode_batch_size),
        max_length=max(8, cfg.max_length),
        return_dense=True,
        return_sparse=False,
        return_colbert_vecs=False,
    )
    dense = out["dense_vecs"]
    # numpy ndarray (n, dim) or list
    if hasattr(dense, "tolist"):
        vectors = dense.tolist()
    else:
        vectors = [list(map(float, row)) for row in dense]

    if len(vectors) != len(texts):
        raise RuntimeError(f"encode returned {len(vectors)} vectors for {len(texts)} texts")
    if vectors and len(vectors[0]) != cfg.vector_dim:
        raise RuntimeError(f"unexpected embedding dim {len(vectors[0])} (configured {cfg.vector_dim})")
    return vectors
