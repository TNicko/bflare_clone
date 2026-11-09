from __future__ import annotations

import tempfile
from datetime import datetime
from typing import Any

from pydantic import BaseModel
from utils.env import getenv, env_truthy, load_service_dotenv

from telegram.session_env import append_telegram_session_env_errors, telegram_session_config_from_env

load_service_dotenv(__file__)


class Config(BaseModel):
    environment: str = "local"
    log_level: str = "INFO"
    tmp_dir: str = ""

    postgres_dsn: str = ""
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    aws_region: str = "eu-west-2"
    s3_bucket: str = "messenger-harvest"

    # telegram_fetch_after: datetime | None = datetime.fromisoformat("2026-05-20T12:00:00+00:00")
    telegram_fetch_after: datetime | None = None
    telegram_fetch_before: datetime | None = None
    telegram_fetch_per_peer_limit: int = 0

    db_batch_size: int = 200
    delay_seconds: float = 0.5

    # Default "process all" mode: pending peers only, batched per run.
    fetch_pending_only: bool = True
    peer_batch_size: int = 500
    catchup_slack_minutes: int = 60

    category_slugs: list[str] = []
    peer_external_id: str | None = None
    community_id: str | None = None
    peer_name_query: str | None = None

    #: Optional pin (CLI ``--harvester-external-id`` preferred). Empty = all eligible.
    harvester_external_id: str = ""

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
            missing.append("S3_BUCKET")
        if not self.aws_region.strip():
            missing.append("AWS_REGION")
        append_telegram_session_env_errors(missing)
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")


class ConfigFactory:
    _config: Config | None = None

    @classmethod
    def get_config(cls) -> Config:
        if cls._config is None:
            tmp_dir = getenv("TMP_DIR") or tempfile.gettempdir()

            payload: dict[str, Any] = dict(
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                telegram_api_id=int(getenv("TELEGRAM_API_ID", "0")),
                telegram_api_hash=getenv("TELEGRAM_API_HASH", ""),
                aws_region=getenv("AWS_REGION", "eu-west-2"),
                s3_bucket=getenv("HARVEST_S3_BUCKET", "messenger-harvest"),
                tmp_dir=tmp_dir,
                log_level=getenv("LOG_LEVEL", "INFO"),
                **telegram_session_config_from_env(),
            )

            db_batch_size = getenv("DB_BATCH_SIZE")
            if db_batch_size is not None:
                payload["db_batch_size"] = int(db_batch_size.strip())

            delay = getenv("TELEGRAM_DELAY_SECONDS")
            if delay is not None:
                payload["delay_seconds"] = float(delay.strip())

            per_peer_limit = getenv("TELEGRAM_FETCH_PER_PEER_LIMIT")
            if per_peer_limit is not None:
                payload["telegram_fetch_per_peer_limit"] = int(per_peer_limit.strip())

            telegram_fetch_after = getenv("TELEGRAM_FETCH_AFTER")
            if telegram_fetch_after is not None and telegram_fetch_after.strip():
                payload["telegram_fetch_after"] = datetime.fromisoformat(telegram_fetch_after.strip())

            telegram_fetch_before = getenv("TELEGRAM_FETCH_BEFORE")
            if telegram_fetch_before is not None and telegram_fetch_before.strip():
                payload["telegram_fetch_before"] = datetime.fromisoformat(telegram_fetch_before.strip())
            # Env pin is optional convenience only; empty / unset = all eligible.
            # Prefer CLI ``--harvester-external-id``.
            harvester_external_id = getenv("HARVESTER_EXTERNAL_ID")
            if harvester_external_id is not None and harvester_external_id.strip():
                payload["harvester_external_id"] = harvester_external_id.strip()

            category_slugs = getenv("TELEGRAM_CATEGORY_SLUGS")
            if category_slugs is not None:
                payload["category_slugs"] = [part.strip() for part in category_slugs.split(",") if part.strip()]

            peer_external_id = getenv("TELEGRAM_PEER_EXTERNAL_ID")
            if peer_external_id is not None:
                payload["peer_external_id"] = peer_external_id.strip() or None

            community_id = getenv("TELEGRAM_COMMUNITY_ID")
            if community_id is not None:
                payload["community_id"] = community_id.strip() or None

            peer_name_query = getenv("TELEGRAM_PEER_NAME_QUERY")
            if peer_name_query is not None:
                payload["peer_name_query"] = peer_name_query.strip() or None

            payload["fetch_pending_only"] = env_truthy(
                "TELEGRAM_FETCH_PENDING_ONLY",
                default=True,
            )

            peer_batch_size = getenv("TELEGRAM_FETCH_PEER_BATCH_SIZE")
            if peer_batch_size is not None:
                payload["peer_batch_size"] = int(peer_batch_size.strip())

            catchup_slack = getenv("TELEGRAM_FETCH_CATCHUP_SLACK_MINUTES")
            if catchup_slack is not None:
                payload["catchup_slack_minutes"] = int(catchup_slack.strip())

            cfg = Config(**payload)
            cfg.validate_required()
            cls._config = cfg
        return cls._config
