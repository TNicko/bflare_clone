"""Env for telegram_messages → S3 Parquet archive (Phase 3a)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field
from utils.env import getenv, env_truthy, load_service_dotenv

from msgstore.layout import DEFAULT_PREFIX

load_service_dotenv(__file__)


def hot_cutoff_month_start(*, now: date | None = None, hot_months: int = 3) -> date:
    """First day of the oldest hot calendar month (inclusive).

    Hot = current month + previous ``hot_months`` months.
    With hot_months=3 on 2026-10-14 → 2026-07-01.
    """
    today = now or datetime.now(timezone.utc).date()
    return today.replace(day=1) - relativedelta(months=hot_months)


def month_bounds(month: str) -> tuple[datetime, datetime]:
    """Inclusive start / exclusive end timestamptz bounds for ``YYYY-MM`` (UTC)."""
    y, m = map(int, month.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = start + relativedelta(months=1)
    return start, end


class Config(BaseModel):
    environment: str = "local"
    log_level: str = "INFO"
    postgres_dsn: str = ""

    store_backend: str = "s3"
    s3_bucket: str = "messenger-harvest"
    aws_region: str = "eu-west-2"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    s3_endpoint_url: str | None = None
    local_dir: str = ""
    store_prefix: str = DEFAULT_PREFIX

    hot_months: int = Field(default=3, ge=0, le=36)
    batch_size: int = Field(default=5_000, ge=100, le=50_000)
    max_rows_per_shard: int = Field(default=100_000, ge=1_000, le=500_000)
    checkpoint_dir: str = ".archive_messages_checkpoints"
    dry_run: bool = False

    def validate_required(self) -> None:
        missing: list[str] = []
        if not self.postgres_dsn.strip():
            missing.append("POSTGRES_DSN")
        if self.store_backend == "s3" and not self.s3_bucket.strip():
            missing.append("MESSAGE_ARCHIVE_BUCKET (or S3_BUCKET)")
        if self.store_backend == "local" and not self.local_dir.strip():
            missing.append("MESSAGE_ARCHIVE_LOCAL_DIR")
        if not self.aws_region.strip() and self.store_backend == "s3":
            missing.append("AWS_REGION")
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")

    def checkpoint_path(self, month: str) -> Path:
        return Path(self.checkpoint_dir) / f"{month}.json"


class ConfigFactory:
    @classmethod
    def get_config(cls) -> Config:
        prefix = (getenv("MESSAGE_ARCHIVE_PREFIX") or DEFAULT_PREFIX).strip()
        # Allow full s3://bucket/prefix URI from terraform output.
        bucket = (getenv("MESSAGE_ARCHIVE_BUCKET") or getenv("S3_BUCKET") or "messenger-harvest").strip()
        if prefix.startswith("s3://"):
            without = prefix[len("s3://") :]
            parts = without.split("/", 1)
            bucket = parts[0] or bucket
            prefix = parts[1] if len(parts) > 1 else DEFAULT_PREFIX
        prefix = prefix.strip("/") or DEFAULT_PREFIX

        return Config(
            environment=getenv("ENVIRONMENT", "local"),
            log_level=getenv("LOG_LEVEL", "INFO"),
            postgres_dsn=getenv("POSTGRES_DSN", ""),
            store_backend=(getenv("MESSAGE_ARCHIVE_BACKEND") or "s3").strip().lower() or "s3",
            s3_bucket=bucket,
            aws_region=getenv("AWS_REGION", "eu-west-2"),
            aws_access_key_id=getenv("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY") or None,
            s3_endpoint_url=getenv("S3_ENDPOINT_URL") or None,
            local_dir=getenv("MESSAGE_ARCHIVE_LOCAL_DIR", ""),
            store_prefix=prefix,
            hot_months=int(getenv("MESSAGE_ARCHIVE_HOT_MONTHS", "3") or "3"),
            batch_size=int(getenv("MESSAGE_ARCHIVE_BATCH_SIZE", "5000") or "5000"),
            max_rows_per_shard=int(getenv("MESSAGE_ARCHIVE_ROWS_PER_PART", "100000") or "100000"),
            checkpoint_dir=getenv("MESSAGE_ARCHIVE_CHECKPOINT_DIR", ".archive_messages_checkpoints"),
            dry_run=env_truthy("MESSAGE_ARCHIVE_DRY_RUN"),
        )
