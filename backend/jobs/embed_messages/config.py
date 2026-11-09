from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel
from utils.env import getenv, env_csv, env_float, env_int, env_str, env_truthy, load_service_dotenv, parse_dt

load_service_dotenv(__file__)


class Config(BaseModel):
    environment: str = "local"
    postgres_dsn: str = ""

    embedding_service_url: str = "http://localhost:11434"
    embedding_model: str = "bge-m3"
    #: HTTP timeout for Ollama ``/api/embed`` (seconds). Raise for large GPU batches / cold start.
    embedding_timeout_seconds: float = 120.0

    telegram_fetch_before: datetime | None = None
    telegram_fetch_after: datetime | None = None

    db_batch_size: int = 250
    embed_batch_size: int = 32
    delay_seconds: float = 0.0
    log_level: str = "INFO"
    embed_ignore_cursor: bool = False

    embed_pending_only: bool = True
    peer_batch_size: int = 500

    #: Embedding store (S3 canonical shards).
    store_backend: str = "s3"
    s3_bucket: str = ""
    aws_region: str = "eu-west-2"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    s3_endpoint_url: str | None = None
    local_dir: str = ""
    store_prefix: str = "embeddings"
    embedding_modality: str = "text"
    vector_dim: int = 1024

    category_slugs: list[str] = []
    peer_external_id: str | None = None
    community_id: str | None = None
    peer_name_query: str | None = None

    def validate_required(self) -> None:
        missing: list[str] = []
        if not self.postgres_dsn.strip():
            missing.append("POSTGRES_DSN")
        if not self.embedding_service_url.strip():
            missing.append("EMBEDDING_SERVICE_URL")
        if self.store_backend == "s3" and not self.s3_bucket.strip():
            missing.append("S3_BUCKET (or EMBEDDING_STORE_S3_BUCKET)")
        if self.store_backend == "local" and not self.local_dir.strip():
            missing.append("EMBEDDING_STORE_LOCAL_DIR")
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            pending_raw = getenv("EMBED_PENDING_ONLY")
            if pending_raw is None or pending_raw.strip() == "":
                embed_pending_only = True
            else:
                embed_pending_only = env_truthy("EMBED_PENDING_ONLY")

            cls._config = Config(
                environment=getenv("ENVIRONMENT", "local"),
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                embedding_service_url=getenv("EMBEDDING_SERVICE_URL", "http://localhost:11434").rstrip("/"),
                embedding_model=getenv("EMBEDDING_MODEL", "bge-m3"),
                embedding_timeout_seconds=env_float("EMBEDDING_TIMEOUT_SECONDS") or 120.0,
                telegram_fetch_before=parse_dt(getenv("TELEGRAM_FETCH_BEFORE", "")),
                telegram_fetch_after=parse_dt(getenv("TELEGRAM_FETCH_AFTER", "")),
                log_level=getenv("LOG_LEVEL", "INFO"),
                db_batch_size=env_int("DB_BATCH_SIZE") or 250,
                embed_batch_size=env_int("EMBED_BATCH_SIZE") or 32,
                delay_seconds=env_float("TELEGRAM_DELAY_SECONDS") or 0.0,
                embed_ignore_cursor=env_truthy("MESSAGE_EMBED_IGNORE_CURSOR"),
                embed_pending_only=embed_pending_only,
                peer_batch_size=env_int("PEER_BATCH_SIZE") or 500,
                store_backend=(getenv("EMBEDDING_STORE_BACKEND") or "s3").strip().lower(),
                s3_bucket=(
                    getenv("EMBEDDING_STORE_S3_BUCKET") or getenv("S3_BUCKET") or "messenger-harvest-embeddings"
                ),
                aws_region=getenv("AWS_REGION", "eu-west-2"),
                aws_access_key_id=env_str("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=env_str("AWS_SECRET_ACCESS_KEY"),
                s3_endpoint_url=env_str("S3_ENDPOINT_URL"),
                local_dir=getenv("EMBEDDING_STORE_LOCAL_DIR", ""),
                store_prefix=getenv("EMBEDDING_STORE_PREFIX", "embeddings"),
                embedding_modality=getenv("EMBEDDING_MODALITY", "text"),
                vector_dim=int(getenv("EMBEDDING_VECTOR_DIM") or "1024"),
                category_slugs=env_csv("TELEGRAM_CATEGORY_SLUGS"),
                peer_external_id=env_str("TELEGRAM_PEER_EXTERNAL_ID"),
                community_id=env_str("TELEGRAM_COMMUNITY_ID"),
                peer_name_query=env_str("TELEGRAM_PEER_NAME_QUERY"),
            )
        return cls._config
