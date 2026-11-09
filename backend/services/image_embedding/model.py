"""SigLIP2 image encoder (vision tower only; loaded once per process).

We only need image→image reverse search, so we load ``SiglipVisionModel`` instead of the
full dual-encoder ``AutoModel``. Non-naflex SigLIP2 checkpoints (including
``google/siglip2-so400m-patch16-384``) use the SigLIP v1 architecture; the full model’s
``get_image_features`` is exactly ``vision_model(...)``, so vision-only weights produce the
same 1152-d pooled vectors as before while skipping the ~0.6B-param text tower (~2.4 GB FP32).
"""

from __future__ import annotations

import io
import logging
import threading
from typing import TYPE_CHECKING, Any

from PIL import Image

if TYPE_CHECKING:
    from services.image_embedding.config import Config

logger = logging.getLogger(__name__)

_load_lock = threading.Lock()
_model: Any | None = None
_processor: Any | None = None
_device: Any | None = None


def is_model_ready() -> bool:
    return _model is not None and _processor is not None


def _from_pretrained_kwargs(hf_token: str | None) -> dict[str, str]:
    token = (hf_token or "").strip()
    return {"token": token} if token else {}


def _resolve_device(requested: str | None) -> Any:
    import torch

    raw = (requested or "").strip().lower()
    if raw in ("", "auto"):
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")
    if raw.startswith("cuda") and not torch.cuda.is_available():
        logger.warning("IMAGE_EMBEDDING_DEVICE=%s requested but CUDA unavailable; using CPU", raw)
        return torch.device("cpu")
    return torch.device(raw)


def _load(cfg: Config) -> tuple[Any, Any, Any]:
    from transformers import AutoImageProcessor, SiglipVisionModel

    kwargs = _from_pretrained_kwargs(cfg.hf_token)
    device = _resolve_device(getattr(cfg, "device", None))
    logger.info("Loading SigLIP image processor %s", cfg.model_id)
    processor = AutoImageProcessor.from_pretrained(cfg.model_id, **kwargs)
    logger.info(
        "Loading SigLIP vision tower only %s on %s (skips text encoder)",
        cfg.model_id,
        device,
    )
    # Vision-only: same pooled embeddings as AutoModel.get_image_features for this checkpoint.
    model = SiglipVisionModel.from_pretrained(cfg.model_id, **kwargs, low_cpu_mem_usage=True)
    model.to(device)
    model.eval()
    logger.info("SigLIP vision tower loaded on %s", device)
    return model, processor, device


def preload_model(cfg: Config) -> None:
    """Load model weights once (call from app lifespan or first encode)."""
    global _model, _processor, _device
    if _model is not None and _processor is not None:
        return
    with _load_lock:
        if _model is not None and _processor is not None:
            return
        _model, _processor, _device = _load(cfg)


def _vision_pooler_output(vision_outputs: Any) -> Any:
    """Extract pooled vision embeddings from SigLIP vision-model outputs."""
    if isinstance(vision_outputs, tuple):
        # (last_hidden_state, pooler_output) when return_dict=False
        return vision_outputs[1]
    pooler = vision_outputs.pooler_output
    if pooler is None:
        raise ValueError("SigLIP vision model returned no pooler_output")
    return pooler


def encode_images(cfg: Config, images: list[bytes]) -> list[list[float]]:
    """L2-normalized dense vectors for raw image bytes (JPEG/PNG/WebP)."""
    import torch

    if not images:
        return []

    preload_model(cfg)
    assert _model is not None and _processor is not None

    from phash.pillow_io import ensure_heif_opener

    ensure_heif_opener()
    pil_images = [Image.open(io.BytesIO(raw)).convert("RGB") for raw in images]
    device = _device if _device is not None else torch.device("cpu")
    inputs = _processor(images=pil_images, return_tensors="pt")
    inputs = {key: value.to(device) if hasattr(value, "to") else value for key, value in inputs.items()}

    with torch.no_grad():
        vision_outputs = _model(**inputs)
        features = _vision_pooler_output(vision_outputs)
        features = features / features.norm(p=2, dim=-1, keepdim=True)

    vectors = features.detach().float().cpu().tolist()
    for vector in vectors:
        if len(vector) != cfg.vector_dim:
            raise ValueError(f"model returned dim {len(vector)}, expected {cfg.vector_dim}")
    return vectors
