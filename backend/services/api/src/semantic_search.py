from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from db.crud.telegram.message_keyword import hydrate_keyword_messages_by_keys
from s3vectors import (
    get_client as get_s3vectors_client,
    search_message_vectors as search_message_vectors_s3,
)
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase import get_conn
from services.api.src.crud.supabase.views.telegram.semantic_message import SemanticMessageView
from services.api.src.message_media import build_message_media_preview
from utils.request_spans import record_span

lg = logging.getLogger(__name__)


def _search_hits(
    query_dense: Sequence[float],
    *,
    limit: int,
    offset: int,
    date_from_epoch_ms: int | None,
    date_to_epoch_ms: int | None,
) -> list[dict[str, Any]]:
    cfg = ConfigFactory.get_config()
    client = get_s3vectors_client(region=cfg.aws_region or None)
    return search_message_vectors_s3(
        client,
        query_dense,
        limit=limit,
        offset=offset,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
        vector_bucket_name=cfg.s3_vectors_bucket,
        index_name=cfg.s3_vectors_index,
    )


async def semantic_search_items(
    query_dense: Sequence[float],
    limit: int,
    offset: int = 0,
    *,
    date_from_epoch_ms: int | None = None,
    date_to_epoch_ms: int | None = None,
) -> list[SemanticMessageView]:
    """Rank via S3 Vectors, then hydrate authoritative rows from Postgres.

    Results are returned in ANN rank order (best match first). Pagination is applied on the
    ranked hit list before hydration, so page boundaries stay stable.
    """
    started = time.perf_counter()
    hits = await asyncio.to_thread(
        _search_hits,
        query_dense,
        limit=limit,
        offset=offset,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
    )
    record_span("s3vectors", "search_messages", time.perf_counter() - started)

    rank_by_id: dict[str, int] = {}
    keys: list[tuple[str, str]] = []
    similarity_by_id: dict[str, float] = {}
    for rank, hit in enumerate(hits):
        hid, date_ms = hit.get("id"), hit.get("date")
        if hid is None or date_ms is None:
            continue
        hid_str = str(hid)
        rank_by_id[hid_str] = rank
        ts = datetime.fromtimestamp(int(date_ms) / 1000, tz=UTC)
        keys.append((hid_str, ts.isoformat()))
        similarity_by_id[hid_str] = float(hit.get("similarity") or 0.0)
    if not keys:
        return []

    async with get_conn() as conn:
        rows_by_id = await hydrate_keyword_messages_by_keys(conn, keys)

    region = ConfigFactory.get_config().aws_region.strip()
    ordered_rows: list[dict] = []
    for hit in hits:
        hid = str(hit.get("id"))
        row = rows_by_id.get(hid)
        if row is None:
            continue
        ordered_rows.append({**row, "similarity": similarity_by_id.get(hid, 0.0)})

    previews = await asyncio.gather(
        *[build_message_media_preview(row.get("media"), region=region) for row in ordered_rows]
    )
    items: list[SemanticMessageView] = [
        SemanticMessageView.from_dense_search_row(row, media=preview)
        for row, preview in zip(ordered_rows, previews, strict=True)
    ]
    items.sort(key=lambda item: rank_by_id.get(str(item.id), 10**9))
    return items
