"""Per-user sliding-window rate limits for expensive product meters.

In-process only. Enough to protect harvester flood-wait and embedding
capacity until a shared store exists. Limits apply to every plan, including
internal.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from uuid import UUID

from usage.meters import Meter

TELEGRAM_RATE_METERS = frozenset(
    {
        str(Meter.SEARCH_NAME_TELEGRAM),
        str(Meter.SEARCH_PHONE),
        str(Meter.TELEGRAM_SYNC),
        str(Meter.TELEGRAM_RESOLVE),
        str(Meter.TELEGRAM_MEDIA_DOWNLOAD),
    }
)
EMBEDDING_RATE_METERS = frozenset(
    {
        str(Meter.SEARCH_SEMANTIC),
        str(Meter.SEARCH_IMAGE),
        str(Meter.SEARCH_IMAGE_HASH),
        str(Meter.SEARCH_IMAGE_HASH_BY_MESSAGE),
        str(Meter.SEARCH_BLUESKY),
    }
)

_lock = threading.Lock()
_windows: dict[tuple[str, str], deque[float]] = defaultdict(deque)


def _bucket_for(meter: str) -> str | None:
    if meter in TELEGRAM_RATE_METERS:
        return "telegram"
    if meter in EMBEDDING_RATE_METERS:
        return "embedding"
    return None


def clear_rate_limits() -> None:
    """Test helper."""
    with _lock:
        _windows.clear()


def rate_limit_retry_after(
    *,
    user_id: UUID,
    meter: str,
    telegram_rpm: int,
    embedding_rpm: int,
    now: float | None = None,
) -> int | None:
    """Return seconds until a slot opens, or None if the call is allowed."""
    bucket = _bucket_for(meter)
    if bucket is None:
        return None
    limit = telegram_rpm if bucket == "telegram" else embedding_rpm
    if limit <= 0:
        return None

    moment = now if now is not None else time.monotonic()
    key = (str(user_id), bucket)
    window_start = moment - 60.0
    with _lock:
        stamps = _windows[key]
        while stamps and stamps[0] < window_start:
            stamps.popleft()
        if len(stamps) >= limit:
            return max(1, int(stamps[0] + 60.0 - moment) + 1)
        stamps.append(moment)
    return None
