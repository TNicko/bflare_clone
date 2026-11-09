"""Shared auto-harvest size threshold (bytes).

Must stay in sync with ``client/src/lib/media/limits.ts``
(``MAX_AUTO_HARVEST_BYTES``).
"""

from __future__ import annotations

from typing import Any

# 10 MiB — viewport auto-harvest skips at or above this size.
MAX_AUTO_HARVEST_BYTES = 10 * 1024 * 1024


def slim_media_size_bytes(media: Any) -> int | None:
    """Best-effort Telegram/harvest size from a slim media model or dict."""
    if media is None:
        return None
    harvest = getattr(media, "harvest", None)
    if harvest is not None:
        harvested = getattr(harvest, "size_bytes", None)
        if isinstance(harvested, int) and harvested >= 0:
            return harvested
        if isinstance(harvest, dict):
            harvested = harvest.get("size_bytes")
            if isinstance(harvested, int) and harvested >= 0:
                return harvested
    size = getattr(media, "size", None)
    if isinstance(size, int) and size >= 0:
        return size
    if isinstance(media, dict):
        size = media.get("size")
        if isinstance(size, int) and size >= 0:
            return size
        harvest_d = media.get("harvest")
        if isinstance(harvest_d, dict):
            harvested = harvest_d.get("size_bytes")
            if isinstance(harvested, int) and harvested >= 0:
                return harvested
    return None


def is_large_media(media: Any, *, threshold: int = MAX_AUTO_HARVEST_BYTES) -> bool:
    """True when known size is at or above the auto-harvest threshold."""
    size = slim_media_size_bytes(media)
    return size is not None and size >= threshold
