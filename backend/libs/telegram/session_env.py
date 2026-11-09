from __future__ import annotations

import os
import socket

from telegram.session_crypto import SessionEncryptionError, ensure_encryption_configured
from utils.env import getenv

LEASE_TTL_SECONDS = 15 * 60
HEARTBEAT_INTERVAL_SECONDS = 3 * 60


def get_session_location_id() -> str:
    location_id = (getenv("TELEGRAM_SESSION_LOCATION_ID") or "").strip()
    if not location_id:
        raise ValueError("TELEGRAM_SESSION_LOCATION_ID is required")
    return location_id


def validate_telegram_session_env() -> None:
    """Ensure worker/API session leasing env is configured."""
    missing: list[str] = []
    append_telegram_session_env_errors(missing)
    if missing:
        raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")


def telegram_session_config_from_env() -> dict[str, str]:
    """Load DB-backed session env fields for service Config models."""
    return {
        "telegram_session_location_id": getenv("TELEGRAM_SESSION_LOCATION_ID") or "",
        "telegram_server_id": getenv("TELEGRAM_SERVER_ID") or "",
        "telegram_session_encryption_key": getenv("TELEGRAM_SESSION_ENCRYPTION_KEY") or "",
    }


def append_telegram_session_env_errors(missing: list[str]) -> None:
    """Append missing/invalid session env names when Telethon uses DB sessions."""
    if not (getenv("TELEGRAM_SESSION_LOCATION_ID") or "").strip():
        missing.append("TELEGRAM_SESSION_LOCATION_ID")
    if not (getenv("TELEGRAM_SESSION_ENCRYPTION_KEY") or "").strip():
        missing.append("TELEGRAM_SESSION_ENCRYPTION_KEY")
        return
    try:
        ensure_encryption_configured()
    except SessionEncryptionError:
        missing.append("TELEGRAM_SESSION_ENCRYPTION_KEY")


def get_server_id(*, service_name: str = "worker") -> str:
    explicit = (getenv("TELEGRAM_SERVER_ID") or "").strip()
    if explicit:
        return explicit
    return f"{service_name}:{socket.gethostname()}:{os.getpid()}"
