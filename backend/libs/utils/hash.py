"""Generic hashing helpers."""

from __future__ import annotations

import hashlib


def sha256_hex(text: str) -> str:
    """SHA-256 hex digest of a UTF-8 string."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
