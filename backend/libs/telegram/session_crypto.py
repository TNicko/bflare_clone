from __future__ import annotations

from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken
from utils.env import getenv


class SessionEncryptionError(RuntimeError):
    """Raised when session encryption key is missing or invalid."""


@lru_cache(maxsize=1)
def _fernet() -> Fernet:
    raw = (getenv("TELEGRAM_SESSION_ENCRYPTION_KEY") or "").strip()
    if not raw:
        raise SessionEncryptionError("TELEGRAM_SESSION_ENCRYPTION_KEY is required to read or write session auth")
    try:
        return Fernet(raw.encode("ascii"))
    except Exception as exc:
        raise SessionEncryptionError("TELEGRAM_SESSION_ENCRYPTION_KEY must be a valid Fernet key") from exc


def encrypt_bytes(plaintext: bytes) -> bytes:
    return _fernet().encrypt(plaintext)


def decrypt_bytes(token: bytes) -> bytes:
    try:
        return _fernet().decrypt(token)
    except InvalidToken as exc:
        raise SessionEncryptionError("Failed to decrypt session auth payload") from exc


def encrypt_text(plaintext: str) -> str:
    return encrypt_bytes(plaintext.encode("utf-8")).decode("ascii")


def decrypt_text(token: str) -> str:
    return decrypt_bytes(token.encode("ascii")).decode("utf-8")


def ensure_encryption_configured() -> None:
    """Load Fernet key; raises SessionEncryptionError if missing/invalid."""
    _fernet()
