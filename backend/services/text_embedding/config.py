from __future__ import annotations

from pydantic import BaseModel
from utils.env import getenv, load_service_dotenv

load_service_dotenv(__file__)

DEFAULT_MODEL_ID = "BAAI/bge-m3"
DEFAULT_VECTOR_DIM = 1024


class Config(BaseModel):
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 11434
    model_id: str = DEFAULT_MODEL_ID
    vector_dim: int = DEFAULT_VECTOR_DIM
    hf_token: str | None = None
    #: ``auto`` (CUDA if available), ``cpu``, ``cuda``, or ``cuda:N``.
    device: str = "auto"
    #: Max tokens per text (FlagEmbedding / BERT). 512 is the fast bulk default;
    #: raise toward 8192 only if retrieval quality requires long context.
    max_length: int = 512
    #: Internal encode micro-batch when the HTTP request carries a large list.
    encode_batch_size: int = 64


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            cls._config = Config(
                log_level=getenv("LOG_LEVEL", "INFO"),
                host=getenv("TEXT_EMBEDDING_HOST", "0.0.0.0"),
                port=int(getenv("TEXT_EMBEDDING_PORT") or "11434"),
                model_id=getenv("TEXT_EMBEDDING_MODEL") or getenv("EMBEDDING_MODEL") or DEFAULT_MODEL_ID,
                vector_dim=int(getenv("TEXT_EMBEDDING_VECTOR_DIM") or str(DEFAULT_VECTOR_DIM)),
                hf_token=getenv("HF_TOKEN") or None,
                device=(getenv("TEXT_EMBEDDING_DEVICE") or "auto").strip() or "auto",
                max_length=int(getenv("TEXT_EMBEDDING_MAX_LENGTH") or "512"),
                encode_batch_size=int(getenv("TEXT_EMBEDDING_ENCODE_BATCH_SIZE") or "64"),
            )
        return cls._config
