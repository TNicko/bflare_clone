"""Synchronous Quickwit REST helpers: index bootstrap, NDJSON ingest, delete-tasks, search.

Used by the indexer. The API uses
``quickwit.async_client`` instead. Keep this free of service config; callers pass values in.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, Literal

import httpx

from .constants import MESSAGES_INDEX
from .query import (
    delete_ids_query,
    hit_source,
    iso_to_unix_seconds,
    messages_keyword_query,
    messages_keyword_sort,
)
from .schema import messages_index_config

lg = logging.getLogger(__name__)

CommitMode = Literal["auto", "wait_for", "force"]

MESSAGE_DOC_FIELDS = (
    "id",
    "message_id",
    "source_external_id",
    "peer_type",
    "message_type",
    "content",
    "from_external_id",
    "harvester_external_id",
    "date",
    "views",
)


def message_row_to_doc(row: Mapping[str, Any]) -> dict[str, Any]:
    """Map a ``telegram_messages`` row to a messages document."""
    doc: dict[str, Any] = {}
    for field in MESSAGE_DOC_FIELDS:
        value = row.get(field)
        if value is None:
            continue
        if isinstance(value, datetime):
            value = value.isoformat()
        doc[field] = value

    doc["id"] = str(row["id"])
    for id_field in ("source_external_id", "from_external_id", "harvester_external_id"):
        if doc.get(id_field) is not None:
            doc[id_field] = str(doc[id_field])
    return doc


class QuickwitClient:
    """Thin httpx wrapper around Quickwit REST (``/api/v1/...``)."""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 60.0,
        index_id: str = MESSAGES_INDEX,
    ) -> None:
        base = url.rstrip("/")
        self.base_url = base
        self.index_id = index_id
        self._http = httpx.Client(base_url=base, timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> QuickwitClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def health(self) -> dict[str, Any]:
        r = self._http.get("/api/v1/version")
        r.raise_for_status()
        return r.json()

    def get_index(self) -> dict[str, Any] | None:
        r = self._http.get(f"/api/v1/indexes/{self.index_id}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def ensure_index(self, *, commit_timeout_secs: int = 10) -> None:
        """Create the messages index if missing. Idempotent."""
        existing = self.get_index()
        if existing is not None:
            lg.info("index %s already exists", self.index_id)
            return
        body = messages_index_config(index_id=self.index_id, commit_timeout_secs=commit_timeout_secs)
        r = self._http.post("/api/v1/indexes", json=body)
        if r.status_code == 400 and "already exists" in (r.text or "").lower():
            lg.info("index %s already exists (race)", self.index_id)
            return
        r.raise_for_status()
        lg.info("created index %s", self.index_id)

    def ingest_messages(
        self,
        docs: Iterable[Mapping[str, Any]],
        *,
        commit: CommitMode = "auto",
    ) -> tuple[int, int]:
        """NDJSON-ingest message docs. Returns ``(accepted, rejected)``."""
        lines: list[str] = []
        for doc in docs:
            payload = dict(doc)
            # Ensure id is always a string key Quickwit can filter on.
            if "id" in payload:
                payload["id"] = str(payload["id"])
            lines.append(json.dumps(payload, default=_json_default, separators=(",", ":")))
        if not lines:
            return 0, 0

        body = "\n".join(lines) + "\n"
        r = self._http.post(
            f"/api/v1/{self.index_id}/ingest",
            content=body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            params={"commit": commit},
        )
        r.raise_for_status()
        data = r.json()
        accepted = int(data.get("num_ingested_docs") or data.get("num_docs_for_processing") or 0)
        rejected = int(data.get("num_rejected_docs") or 0)
        if rejected:
            lg.warning("quickwit ingest rejected %d doc(s) (accepted=%d)", rejected, accepted)
        return accepted, rejected

    def delete_messages(self, ids: Iterable[str]) -> dict[str, Any] | None:
        """Enqueue a delete-task for the given message ids. Returns the task body or None if empty."""
        id_list = [str(i) for i in ids]
        if not id_list:
            return None
        payload = {"query": delete_ids_query(id_list)}
        r = self._http.post(f"/api/v1/{self.index_id}/delete-tasks", json=payload)
        r.raise_for_status()
        return r.json()

    def search_raw(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """POST a raw search body to the index."""
        r = self._http.post(f"/api/v1/{self.index_id}/search", json=dict(body))
        if r.is_error:
            detail = (r.text or "").strip()
            raise httpx.HTTPStatusError(
                f"{r.status_code} from Quickwit search: {detail[:500]}",
                request=r.request,
                response=r,
            )
        return r.json()

    def search_messages(
        self,
        query: str,
        *,
        size: int = 10,
        offset: int = 0,
        source_external_id: str | None = None,
        peer_type: str | list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        """Keyword search; returns the raw Quickwit search response."""
        q = messages_keyword_query(
            query,
            source_external_id=source_external_id,
            peer_type=peer_type,
        )
        body: dict[str, Any] = {
            "query": q,
            "max_hits": size,
            "start_offset": offset,
            "sort_by": messages_keyword_sort(),
        }
        start_ts = iso_to_unix_seconds(date_from)
        end_ts = iso_to_unix_seconds(date_to)
        if start_ts is not None:
            body["start_timestamp"] = start_ts
        if end_ts is not None:
            # Quickwit end_timestamp is exclusive; bump by 1s so ISO upper bound stays inclusive.
            body["end_timestamp"] = end_ts + 1

        return self.search_raw(body)


def get_client(url: str, *, timeout: float = 60.0, index_id: str = MESSAGES_INDEX) -> QuickwitClient:
    """Build a sync Quickwit client pointing at ``url`` (e.g. ``http://harvest-quickwit:7280``)."""
    return QuickwitClient(url, timeout=timeout, index_id=index_id)


def ensure_index(client: QuickwitClient, *, commit_timeout_secs: int = 10) -> None:
    client.ensure_index(commit_timeout_secs=commit_timeout_secs)


def bulk_index_messages(
    client: QuickwitClient,
    docs: Iterable[Mapping[str, Any]],
    *,
    commit: CommitMode = "auto",
) -> tuple[int, list[Any]]:
    """Ingest docs; returns ``(success_count, errors)`` (errors non-empty only when any rejected)."""
    accepted, rejected = client.ingest_messages(docs, commit=commit)
    errors: list[Any] = [{"rejected": rejected}] if rejected else []
    return accepted, errors


def bulk_delete_messages(client: QuickwitClient, ids: Iterable[str]) -> tuple[int, list[Any]]:
    """Enqueue delete-tasks for ``ids``. Returns ``(queued_count, errors)``."""
    id_list = list(ids)
    if not id_list:
        return 0, []
    client.delete_messages(id_list)
    return len(id_list), []


def search_hits(
    resp: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    """Parse a search response into ``(hits, total)``.

    Each hit is ``{id, date, score, source}``. Duplicate ``id``s (from edit re-ingest) keep the
    first occurrence in sort order.
    """
    total = int(resp.get("num_hits") or 0)
    hits: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in resp.get("hits") or []:
        source = hit_source(raw if isinstance(raw, dict) else {})
        doc_id = source.get("id")
        if doc_id is None:
            continue
        doc_id_s = str(doc_id)
        if doc_id_s in seen:
            continue
        seen.add(doc_id_s)
        score = raw.get("score") if isinstance(raw, dict) else None
        if score is None and isinstance(raw, dict):
            score = raw.get("_score")
        hits.append(
            {
                "id": doc_id_s,
                "date": source.get("date"),
                "score": score,
                "source": source,
            }
        )
    return hits, total


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
