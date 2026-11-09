"""Telegram contacts.search fleet job config (central ``jobs/.env``)."""

from __future__ import annotations

from pydantic import BaseModel
from utils.env import getenv, load_service_dotenv

from telegram.session_env import append_telegram_session_env_errors, telegram_session_config_from_env

load_service_dotenv(__file__)


class Config(BaseModel):
    environment: str = "local"
    log_level: str = "INFO"
    postgres_dsn: str = ""

    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    s3_bucket: str = ""
    aws_region: str = ""
    harvester_external_id: str = ""
    telegram_delay_seconds: float = 1.5

    telegram_session_location_id: str = ""
    telegram_server_id: str = ""
    telegram_session_encryption_key: str = ""

    def validate_required(self) -> None:
        missing: list[str] = []
        if not self.postgres_dsn.strip():
            missing.append("POSTGRES_DSN")
        if self.telegram_api_id <= 0:
            missing.append("TELEGRAM_API_ID")
        if not self.telegram_api_hash.strip():
            missing.append("TELEGRAM_API_HASH")
        if not self.s3_bucket.strip():
            missing.append("S3_BUCKET or HARVEST_S3_BUCKET")
        if not self.aws_region.strip():
            missing.append("AWS_REGION")
        append_telegram_session_env_errors(missing)
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")

    # Alias used by pipeline (same as former search Config API).
    def validate_telegram_required(self) -> None:
        self.validate_required()


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            s3 = getenv("S3_BUCKET") or getenv("HARVEST_S3_BUCKET") or ""
            cls._config = Config(
                environment=getenv("ENVIRONMENT", "local"),
                log_level=getenv("LOG_LEVEL", "INFO"),
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                telegram_api_id=int(getenv("TELEGRAM_API_ID") or "0"),
                telegram_api_hash=getenv("TELEGRAM_API_HASH", ""),
                s3_bucket=s3,
                aws_region=getenv("AWS_REGION", ""),
                harvester_external_id=getenv("HARVESTER_EXTERNAL_ID", ""),
                telegram_delay_seconds=float(getenv("TELEGRAM_DELAY_SECONDS") or "1.5"),
                **telegram_session_config_from_env(),
            )
        return cls._config
