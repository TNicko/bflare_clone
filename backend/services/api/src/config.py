from __future__ import annotations

from pydantic import BaseModel
from utils.env import getenv, env_csv, load_service_dotenv

from telegram.session_env import append_telegram_session_env_errors

load_service_dotenv(__file__)


class Config(BaseModel):
    environment: str = "local"
    postgres_dsn: str = ""
    log_level: str = "INFO"
    embedding_service_url: str = "http://localhost:11434"
    embedding_model: str = "bge-m3"

    #: Keyword-search backend. "pg" or "quickwit" (Quickwit hits are hydrated from Postgres).
    search_keyword_backend: str = "pg"
    quickwit_url: str = "http://localhost:7280"
    #: httpx timeout for Quickwit search (seconds). Keep >= cold/concurrent search p99.
    quickwit_timeout_secs: float = 60.0

    #: Semantic and reverse-image ANN use S3 Vectors.
    #: pHash Hamming near-dup search uses Postgres on telegram_image_blobs.
    search_semantic_backend: str = "s3vectors"
    search_image_backend: str = "s3vectors"
    #: S3 Vectors vector-bucket + indexes (used when backends are s3vectors).
    s3_vectors_bucket: str = "blackflare-message-vectors"
    s3_vectors_index: str = "messages-bge-m3-v1"
    s3_vectors_images_index: str = "images-siglip2-v1"

    image_phash_max_hamming: int = 5
    #: Distinct near-dup image hashes from Postgres Hamming (not message copies).
    image_phash_search_top_n: int = 100
    image_search_top_n: int = 100
    #: Default page size for reverse-image / pHash copy search responses.
    image_search_page_size: int = 50
    #: Max messages to consider per matching pHash before pagination (fan-out ceiling).
    #: Uncapped SQL fan-out; a small cap hid most copies of viral images.
    image_max_messages_per_phash: int = 10_000
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    #: Message media, profile photos, image-search presigned URLs (`messenger-harvest`).
    harvest_s3_bucket: str = "messenger-harvest"
    #: Cold message Parquet archive (Phase 3 hydrate / overlays). Same harvest bucket by default.
    message_archive_bucket: str = "messenger-harvest"
    message_archive_prefix: str = "messages/telegram"
    #: Canonical embedding Parquet shards (`messenger-harvest-embeddings`). Reserved for future API use.
    embedding_store_s3_bucket: str = "messenger-harvest-embeddings"
    embedding_store_prefix: str = "embeddings"
    aws_region: str = ""
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    peer_profile_refresh_ttl_minutes: int = 1
    telegram_session_location_id: str = ""
    telegram_server_id: str = ""
    telegram_session_encryption_key: str = ""
    image_embedding_service_url: str = "http://localhost:11435"

    #: Platform auth (Supabase JWT via JWKS). Required on every request except health probes.
    supabase_url: str = ""
    platform_cors_origins: list[str] = ["http://localhost:5173", "http://127.0.0.1:5173"]
    #: Per-user sliding-window caps for live Telegram and embedding meters (all plans).
    usage_telegram_rpm: int = 30
    usage_embedding_rpm: int = 20
    stripe_secret_key: str = ""
    stripe_publishable_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_pro_price_id: str = ""
    stripe_success_url: str = "http://localhost:5173/billing?checkout=success"
    stripe_cancel_url: str = "http://localhost:5173/billing?checkout=canceled"
    stripe_portal_return_url: str = "http://localhost:5173/billing"
    posthog_project_token: str = ""
    posthog_host: str = "https://us.i.posthog.com"
    postmark_server_token: str = ""
    postmark_from: str = ""
    postmark_message_stream: str = "outbound"

    def validate_auth(self) -> None:
        if not self.supabase_url.strip():
            raise ValueError("SUPABASE_URL is required")

    def validate_telegram_session_if_configured(self) -> None:
        """Require session env when Telegram API credentials are set."""
        if self.telegram_api_id <= 0 or not self.telegram_api_hash.strip():
            return
        missing: list[str] = []
        append_telegram_session_env_errors(missing)
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            cls._config = Config(
                environment=getenv("ENVIRONMENT", "local"),
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                log_level=getenv("LOG_LEVEL", "INFO"),
                embedding_service_url=getenv("EMBEDDING_SERVICE_URL", "http://localhost:11434").rstrip("/"),
                embedding_model=getenv("EMBEDDING_MODEL", "bge-m3"),
                image_embedding_service_url=getenv("IMAGE_EMBEDDING_SERVICE_URL", "http://localhost:11435").rstrip("/"),
                search_keyword_backend=(getenv("SEARCH_KEYWORD_BACKEND", "pg").strip().lower() or "pg"),
                quickwit_url=(getenv("QUICKWIT_URL") or "http://localhost:7280").rstrip("/") or "http://localhost:7280",
                quickwit_timeout_secs=float(getenv("QUICKWIT_TIMEOUT_SECS") or "60"),
                search_semantic_backend=(getenv("SEARCH_SEMANTIC_BACKEND", "s3vectors").strip().lower() or "s3vectors"),
                search_image_backend=(getenv("SEARCH_IMAGE_BACKEND", "s3vectors").strip().lower() or "s3vectors"),
                s3_vectors_bucket=(getenv("S3_VECTORS_BUCKET") or "blackflare-message-vectors").strip()
                or "blackflare-message-vectors",
                s3_vectors_index=(getenv("S3_VECTORS_INDEX") or "messages-bge-m3-v1").strip() or "messages-bge-m3-v1",
                s3_vectors_images_index=(getenv("S3_VECTORS_IMAGES_INDEX") or "images-siglip2-v1").strip()
                or "images-siglip2-v1",
                image_phash_max_hamming=int(getenv("IMAGE_PHASH_MAX_HAMMING") or "5"),
                image_phash_search_top_n=int(getenv("IMAGE_PHASH_SEARCH_TOP_N") or "100"),
                image_search_top_n=int(getenv("IMAGE_SEARCH_TOP_N") or "100"),
                image_search_page_size=int(getenv("IMAGE_SEARCH_PAGE_SIZE") or "50"),
                image_max_messages_per_phash=int(getenv("IMAGE_MAX_MESSAGES_PER_PHASH") or "10000"),
                telegram_api_id=int(getenv("TELEGRAM_API_ID") or "0"),
                telegram_api_hash=getenv("TELEGRAM_API_HASH", ""),
                harvest_s3_bucket=(getenv("HARVEST_S3_BUCKET") or getenv("S3_BUCKET") or "messenger-harvest"),
                message_archive_bucket=(
                    getenv("MESSAGE_ARCHIVE_BUCKET")
                    or getenv("HARVEST_S3_BUCKET")
                    or getenv("S3_BUCKET")
                    or "messenger-harvest"
                ),
                message_archive_prefix=(
                    (getenv("MESSAGE_ARCHIVE_PREFIX") or "messages/telegram")
                    .removeprefix("s3://messenger-harvest/")
                    .strip("/")
                    or "messages/telegram"
                ),
                embedding_store_s3_bucket=(
                    getenv("EMBEDDING_STORE_S3_BUCKET")
                    or getenv("EMBEDDINGS_S3_BUCKET")
                    or "messenger-harvest-embeddings"
                ),
                embedding_store_prefix=getenv("EMBEDDING_STORE_PREFIX", "embeddings"),
                aws_region=getenv("AWS_REGION", ""),
                aws_access_key_id=getenv("AWS_ACCESS_KEY_ID") or None,
                aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY") or None,
                peer_profile_refresh_ttl_minutes=int(getenv("PEER_PROFILE_REFRESH_TTL_MINUTES") or "1"),
                telegram_session_location_id=getenv("TELEGRAM_SESSION_LOCATION_ID", ""),
                telegram_server_id=getenv("TELEGRAM_SERVER_ID", ""),
                telegram_session_encryption_key=getenv("TELEGRAM_SESSION_ENCRYPTION_KEY", ""),
                supabase_url=getenv("SUPABASE_URL", "").rstrip("/"),
                platform_cors_origins=env_csv("PLATFORM_CORS_ORIGINS")
                or ["http://localhost:5173", "http://127.0.0.1:5173"],
                usage_telegram_rpm=int(getenv("USAGE_TELEGRAM_RPM") or "30"),
                usage_embedding_rpm=int(getenv("USAGE_EMBEDDING_RPM") or "20"),
                stripe_secret_key=getenv("STRIPE_SECRET_KEY", ""),
                stripe_publishable_key=getenv("STRIPE_PUBLISHABLE_KEY", ""),
                stripe_webhook_secret=getenv("STRIPE_WEBHOOK_SECRET", ""),
                stripe_pro_price_id=getenv("STRIPE_PRO_PRICE_ID", ""),
                stripe_success_url=getenv("STRIPE_SUCCESS_URL", "http://localhost:5173/billing?checkout=success"),
                stripe_cancel_url=getenv("STRIPE_CANCEL_URL", "http://localhost:5173/billing?checkout=canceled"),
                stripe_portal_return_url=getenv(
                    "STRIPE_PORTAL_RETURN_URL",
                    "http://localhost:5173/billing",
                ),
                posthog_project_token=getenv("POSTHOG_PROJECT_TOKEN", ""),
                posthog_host=getenv("POSTHOG_HOST", "https://us.i.posthog.com"),
                postmark_server_token=getenv("POSTMARK_SERVER_TOKEN", ""),
                postmark_from=getenv("POSTMARK_FROM", ""),
                postmark_message_stream=getenv("POSTMARK_MESSAGE_STREAM", "outbound") or "outbound",
            )
            cls._config.validate_auth()
            cls._config.validate_telegram_session_if_configured()
        return cls._config
