"""In-process TTL + LRU session cache for reverse / hash image search pagination.

First request builds the full message plan (embed/Milvus/expand). Later pages only
hydrate + presign a slice. Process-local is fine while the API runs with
``replicas: 1``; Redis can replace this later without changing the API contract.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar
from uuid import UUID, uuid4

from db.crud.telegram.message_keyword import hydrate_keyword_messages_by_keys

lg = logging.getLogger(__name__)

#: How long a search session stays paginable without re-running ANN.
_DEFAULT_TTL_SECONDS = 20 * 60
#: Max concurrent sessions in this process (oldest evicted).
_DEFAULT_MAX_SESSIONS = 64
#: Cap stored hits per session so one viral image cannot balloon RAM.
_DEFAULT_MAX_HITS_PER_SESSION = 5_000

K = TypeVar("K")
V = TypeVar("V")


class TtlLruCache(Generic[K, V]):
    """Thread-safe LRU cache with per-entry TTL and a hard entry cap."""

    def __init__(self, *, max_entries: int, ttl_seconds: float) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._max_entries = int(max_entries)
        self._ttl_seconds = float(ttl_seconds)
        self._items: OrderedDict[K, tuple[float, V]] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: K) -> V | None:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            expires_at, value = item
            if time.monotonic() >= expires_at:
                del self._items[key]
                return None
            self._items.move_to_end(key)
            return value

    def set(self, key: K, value: V) -> None:
        with self._lock:
            self._items[key] = (time.monotonic() + self._ttl_seconds, value)
            self._items.move_to_end(key)
            while len(self._items) > self._max_entries:
                self._items.popitem(last=False)

    def delete(self, key: K) -> None:
        with self._lock:
            self._items.pop(key, None)

    def __len__(self) -> int:
        with self._lock:
            now = time.monotonic()
            expired = [k for k, (exp, _) in self._items.items() if now >= exp]
            for k in expired:
                del self._items[k]
            return len(self._items)


@dataclass(frozen=True, slots=True)
class ImageSearchPlanHit:
    """One message appearance in a reverse/hash search plan (pre-hydrate)."""

    message_id: str
    message_date_iso: str
    phash: str
    s3_key: str
    score: float
    hamming_distance: float | None = None


@dataclass(frozen=True, slots=True)
class ImageSearchSession:
    owner_user_id: str
    kind: str
    hits: tuple[ImageSearchPlanHit, ...]


_sessions: TtlLruCache[str, ImageSearchSession] = TtlLruCache(
    max_entries=_DEFAULT_MAX_SESSIONS,
    ttl_seconds=_DEFAULT_TTL_SECONDS,
)


def _iso_from_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def clamp_page(*, limit: int, offset: int) -> tuple[int, int]:
    page_limit = max(1, min(int(limit), 100))
    page_offset = max(0, int(offset))
    return page_limit, page_offset


def truncate_plan_hits(
    hits: Sequence[ImageSearchPlanHit],
    *,
    max_hits: int = _DEFAULT_MAX_HITS_PER_SESSION,
) -> tuple[ImageSearchPlanHit, ...]:
    if len(hits) <= max_hits:
        return tuple(hits)
    lg.warning(
        "image search session truncated plan hits from %s to %s",
        len(hits),
        max_hits,
    )
    return tuple(hits[:max_hits])


def store_image_search_session(
    *,
    owner_user_id: str,
    kind: str,
    hits: Sequence[ImageSearchPlanHit],
) -> str:
    """Persist a plan and return an opaque ``search_id`` for later pages."""
    search_id = str(uuid4())
    session = ImageSearchSession(
        owner_user_id=str(owner_user_id),
        kind=str(kind),
        hits=truncate_plan_hits(hits),
    )
    _sessions.set(search_id, session)
    lg.info(
        "image search session stored id=%s… kind=%s hits=%s sessions=%s",
        search_id[:8],
        kind,
        len(session.hits),
        len(_sessions),
    )
    return search_id


def get_image_search_session(
    search_id: str,
    *,
    owner_user_id: str,
) -> ImageSearchSession | None:
    session = _sessions.get(str(search_id).strip())
    if session is None:
        return None
    if session.owner_user_id != str(owner_user_id):
        lg.warning(
            "image search session owner mismatch id=%s…",
            str(search_id)[:8],
        )
        return None
    return session


async def materialize_image_search_page(
    hits: Sequence[ImageSearchPlanHit],
    *,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    """Hydrate one page of plan hits into API match dicts (no S3 presign yet)."""
    from services.api.src.crud.supabase import get_conn
    from services.api.src.search_message_peers import (
        enrich_message_row_peers,
        resolve_source_and_from_partials,
    )

    page_limit, page_offset = clamp_page(limit=limit, offset=offset)
    total = len(hits)
    page = list(hits[page_offset : page_offset + page_limit])
    if not page:
        return [], total

    lg.info(
        "image search: hydrating page (%s/%s messages, offset=%s)…",
        len(page),
        total,
        page_offset,
    )
    keys = [(hit.message_id, hit.message_date_iso) for hit in page]
    async with get_conn() as conn:
        rows_by_id = await hydrate_keyword_messages_by_keys(conn, keys)
        source_peers, from_peers, users = await resolve_source_and_from_partials(conn, rows_by_id.values())

    out: list[dict[str, Any]] = []
    for hit in page:
        row = rows_by_id.get(hit.message_id)
        if row is None:
            continue
        item: dict[str, Any] = {
            "id": hit.message_id,
            "phash": hit.phash,
            "s3_key": hit.s3_key,
            "filename": row.get("filename") or "",
            "score": hit.score,
            "row": enrich_message_row_peers(
                row,
                source_peers=source_peers,
                from_peers=from_peers,
                users=users,
            ),
        }
        if hit.hamming_distance is not None:
            item["hamming_distance"] = hit.hamming_distance
        out.append(item)
    return out, total


def plan_hit_from_map_row(
    *,
    phash: str,
    score: float,
    s3_key: str,
    map_row: dict[str, Any],
    hamming_distance: float | None = None,
) -> ImageSearchPlanHit | None:
    message_id = map_row.get("message_id")
    message_date = map_row.get("message_date")
    if message_id is None or message_date is None:
        return None
    msg_id = str(message_id if isinstance(message_id, UUID) else message_id)
    if not isinstance(message_date, datetime):
        return None
    return ImageSearchPlanHit(
        message_id=msg_id,
        message_date_iso=_iso_from_datetime(message_date),
        phash=phash,
        s3_key=s3_key or "",
        score=float(score),
        hamming_distance=hamming_distance,
    )
