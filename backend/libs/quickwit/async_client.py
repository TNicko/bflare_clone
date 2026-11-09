"""Async Quickwit search client for the Harvest API keyword path."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from utils.request_spans import timed

from .client import search_hits
from .constants import MESSAGES_INDEX
from .query import iso_to_unix_seconds, messages_keyword_query, messages_keyword_sort

lg = logging.getLogger(__name__)


# Match the sync ingest client. Search on the small S3-backed node can exceed 10s
# under concurrency (t4g.medium, ~100M docs); a short timeout becomes a false 502.
_DEFAULT_TIMEOUT_SECS = 60.0


class AsyncQuickwitClient:
    """Pooled async httpx client for Quickwit search."""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = _DEFAULT_TIMEOUT_SECS,
        index_id: str = MESSAGES_INDEX,
    ) -> None:
        self.base_url = url.rstrip("/")
        self.index_id = index_id
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def health(self) -> dict[str, Any]:
        r = await self._http.get("/api/v1/version")
        r.raise_for_status()
        return r.json()

    async def search(
        self,
        query: str,
        *,
        limit: int = 10,
        offset: int = 0,
        source_external_id: str | None = None,
        peer_type: str | list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        q = messages_keyword_query(
            query,
            source_external_id=source_external_id,
            peer_type=peer_type,
        )
        body: dict[str, Any] = {
            "query": q,
            "max_hits": limit,
            "start_offset": offset,
            "sort_by": messages_keyword_sort(),
        }
        start_ts = iso_to_unix_seconds(date_from)
        end_ts = iso_to_unix_seconds(date_to)
        if start_ts is not None:
            body["start_timestamp"] = start_ts
        if end_ts is not None:
            body["end_timestamp"] = end_ts + 1

        async with timed("quickwit", "search"):
            r = await self._http.post(f"/api/v1/{self.index_id}/search", json=body)
        if r.is_error:
            detail = (r.text or "").strip()
            raise httpx.HTTPStatusError(
                f"{r.status_code} from Quickwit search: {detail[:500]}",
                request=r.request,
                response=r,
            )
        return r.json()


def get_async_client(
    url: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT_SECS,
    index_id: str = MESSAGES_INDEX,
) -> AsyncQuickwitClient:
    return AsyncQuickwitClient(url, timeout=timeout, index_id=index_id)


async def search_message_keyword(
    client: AsyncQuickwitClient,
    query: str,
    *,
    limit: int = 10,
    offset: int = 0,
    source_external_id: str | None = None,
    peer_type: str | list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return ``(hits, total)`` where each hit is ``{id, date, score, source}``."""
    resp = await client.search(
        query,
        limit=limit,
        offset=offset,
        source_external_id=source_external_id,
        peer_type=peer_type,
        date_from=date_from,
        date_to=date_to,
    )
    return search_hits(resp)
