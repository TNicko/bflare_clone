from __future__ import annotations

from pydantic import BaseModel, Field
from utils.env import getenv, env_truthy, load_service_dotenv

load_service_dotenv(__file__)


class Config(BaseModel):
    environment: str = "local"
    log_level: str = "INFO"

    postgres_dsn: str = ""

    #: Keyword index sink. Always Quickwit.
    index_sinks: list[str] = Field(default_factory=lambda: ["quickwit"])

    quickwit_url: str = "http://localhost:7280"

    #: Outbox rows claimed (and applied) per batch.
    batch_size: int = 500
    #: Seconds to sleep when the outbox is empty before polling again (loop mode).
    poll_interval_seconds: float = 2.0
    #: Make indexed docs visible immediately after each batch (dev convenience).
    #: Quickwit: ingest ``commit=force``.
    refresh_after_batch: bool = True

    @property
    def write_quickwit(self) -> bool:
        return True

    def validate_required(self) -> None:
        missing: list[str] = []
        if not self.postgres_dsn.strip():
            missing.append("POSTGRES_DSN")
        if not self.quickwit_url.strip():
            missing.append("QUICKWIT_URL")
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            cls._config = Config(
                environment=getenv("ENVIRONMENT", "local"),
                log_level=getenv("LOG_LEVEL", "INFO"),
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                index_sinks=["quickwit"],
                quickwit_url=(getenv("QUICKWIT_URL") or "http://localhost:7280").rstrip("/") or "http://localhost:7280",
                batch_size=int(getenv("INDEXER_BATCH_SIZE") or "500"),
                poll_interval_seconds=float(getenv("INDEXER_POLL_INTERVAL_SECONDS") or "2"),
                refresh_after_batch=env_truthy("INDEXER_REFRESH_AFTER_BATCH", default=True),
            )
        return cls._config
