from __future__ import annotations

import os
from datetime import datetime

from embedding.image_models import (
    IMAGE_EMBED_DIMS,
    SIGLIP_EMBED_MODEL,
    resolve_image_embed_model,
    resolve_image_embed_service_url,
)
from pydantic import BaseModel
from utils.env import (
    env_csv,
    env_float,
    env_int,
    env_str,
    env_truthy,
    getenv,
    load_service_dotenv,
    override_service_env,
    parse_dt,
)

load_service_dotenv(__file__)


class Config(BaseModel):
    environment: str = "local"
    postgres_dsn: str = ""

    image_embedding_service_url: str = "http://localhost:11435"
    embedding_model: str = SIGLIP_EMBED_MODEL
    embedding_timeout_seconds: float = 120.0

    telegram_fetch_before: datetime | None = None
    telegram_fetch_after: datetime | None = None

    db_batch_size: int = 250
    embed_batch_size: int = 8
    download_concurrency: int = 256
    delay_seconds: float = 0.0
    log_level: str = "INFO"
    embed_ignore_cursor: bool = False

    #: Default True: only peers where download coverage has advanced past embed.
    embed_pending_only: bool = True
    peer_batch_size: int = 500

    store_backend: str = "s3"
    s3_bucket: str = ""
    aws_region: str = "eu-west-2"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    s3_endpoint_url: str | None = None
    local_dir: str = ""
    store_prefix: str = "embeddings"
    embedding_modality: str = "image"
    vector_dim: int = IMAGE_EMBED_DIMS[SIGLIP_EMBED_MODEL]

    category_slugs: list[str] = []
    peer_external_id: str | None = None
    community_id: str | None = None
    peer_name_query: str | None = None

    def validate_required(self) -> None:
        missing: list[str] = []
        if not self.postgres_dsn.strip():
            missing.append("POSTGRES_DSN")
        if not self.image_embedding_service_url.strip():
            missing.append("IMAGE_EMBEDDING_SERVICE_URL")
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

            model_raw = (
                os.environ.get("IMAGE_EMBEDDING_MODEL") or getenv("IMAGE_EMBEDDING_MODEL") or ""
            ).strip() or None
            model, dim = resolve_image_embed_model(model_raw)
            cls._config = Config(
                environment=getenv("ENVIRONMENT", "local"),
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                image_embedding_service_url=resolve_image_embed_service_url(
                    model,
                    service_url=os.environ.get("IMAGE_EMBEDDING_SERVICE_URL") or getenv("IMAGE_EMBEDDING_SERVICE_URL"),
                    mobilenet_service_url=os.environ.get("MOBILENET_EMBEDDING_SERVICE_URL")
                    or getenv("MOBILENET_EMBEDDING_SERVICE_URL"),
                ),
                embedding_model=model,
                embedding_timeout_seconds=env_float("EMBEDDING_TIMEOUT_SECONDS") or 120.0,
                telegram_fetch_before=parse_dt(getenv("TELEGRAM_FETCH_BEFORE", "")),
                telegram_fetch_after=parse_dt(getenv("TELEGRAM_FETCH_AFTER", "")),
                log_level=getenv("LOG_LEVEL", "INFO"),
                db_batch_size=env_int("DB_BATCH_SIZE") or 250,
                embed_batch_size=env_int("EMBED_BATCH_SIZE") or 8,
                download_concurrency=env_int("DOWNLOAD_CONCURRENCY") or 256,
                delay_seconds=env_float("TELEGRAM_DELAY_SECONDS") or 0.0,
                embed_ignore_cursor=env_truthy("IMAGE_EMBED_IGNORE_CURSOR"),
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
                embedding_modality=getenv("EMBEDDING_MODALITY", "image"),
                vector_dim=dim,
                category_slugs=env_csv("TELEGRAM_CATEGORY_SLUGS"),
                peer_external_id=env_str("TELEGRAM_PEER_EXTERNAL_ID"),
                community_id=env_str("TELEGRAM_COMMUNITY_ID"),
                peer_name_query=env_str("TELEGRAM_PEER_NAME_QUERY"),
            )
        return cls._config

    @classmethod
    def apply_cli_overrides(cls, *, model: str | None = None, service_url: str | None = None) -> None:
        """Apply ``--model`` / ``--service-url`` into the dotenv snapshot (not only os.environ)."""
        if model:
            override_service_env("IMAGE_EMBEDDING_MODEL", model)
        if service_url:
            from embedding.image_models import MOBILENET_EMBED_MODEL_SLUG, resolve_image_embed_model

            resolved, _ = resolve_image_embed_model(
                model or os.environ.get("IMAGE_EMBEDDING_MODEL") or getenv("IMAGE_EMBEDDING_MODEL")
            )
            key = (
                "MOBILENET_EMBEDDING_SERVICE_URL"
                if resolved == MOBILENET_EMBED_MODEL_SLUG
                else "IMAGE_EMBEDDING_SERVICE_URL"
            )
            override_service_env(key, service_url.rstrip("/"))
        if model or service_url:
            cls._config = None
