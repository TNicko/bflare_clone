from __future__ import annotations

from pydantic import BaseModel
from utils.env import getenv, load_service_dotenv

load_service_dotenv(__file__)

# Default matches embstore layout (`siglip2-so400m-p16-384`) and Milvus images collection plans.
# Service loads the vision tower only (image→image); text encoder is not needed.
DEFAULT_MODEL_ID = "google/siglip2-so400m-patch16-384"
DEFAULT_VECTOR_DIM = 1152


class Config(BaseModel):
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 11435
    model_id: str = DEFAULT_MODEL_ID
    vector_dim: int = DEFAULT_VECTOR_DIM
    hf_token: str | None = None
    #: ``auto`` (CUDA if available), ``cpu``, ``cuda``, or ``cuda:N``.
    device: str = "auto"


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            cls._config = Config(
                log_level=getenv("LOG_LEVEL", "INFO"),
                host=getenv("IMAGE_EMBEDDING_HOST", "0.0.0.0"),
                port=int(getenv("IMAGE_EMBEDDING_PORT") or "11435"),
                model_id=getenv("IMAGE_EMBEDDING_MODEL", DEFAULT_MODEL_ID),
                vector_dim=int(getenv("IMAGE_EMBEDDING_VECTOR_DIM") or str(DEFAULT_VECTOR_DIM)),
                hf_token=getenv("HF_TOKEN") or None,
                device=(getenv("IMAGE_EMBEDDING_DEVICE") or "auto").strip() or "auto",
            )
        return cls._config
