"""Dense ANN + Postgres map fan-out for reverse image search.

``images`` is keyed by exact pHash. Hits expand to messages via
``telegram_image_blob_messages``, then hydrate from ``telegram_messages``.

Backend: S3 Vectors.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from db.crud.telegram.image_blobs import list_messages_for_phashes
from s3vectors import (
    get_client as get_s3vectors_client,
    search_image_vectors as search_image_vectors_s3,
)
from services.api.src.config import ConfigFactory
from utils.request_spans import record_span
from services.api.src.image_search_session import (
    ImageSearchPlanHit,
    clamp_page,
    materialize_image_search_page,
    plan_hit_from_map_row,
)

lg = logging.getLogger(__name__)

# Cosine similarity cutoff after normalizing ANN hits (higher = closer).
_MIN_COSINE_SIMILARITY = 0.5
#: Safety ceiling when callers omit max_messages_per_phash (viral images can have 80+ copies).
_DEFAULT_MAX_MESSAGES_PER_PHASH = 10_000
_DEFAULT_PAGE_SIZE = 50


def _passes_similarity_threshold(similarity: float | None) -> bool:
    if similarity is None:
        return True
    return float(similarity) >= _MIN_COSINE_SIMILARITY


def _search_image_hits(
    query_vector: Sequence[float],
    *,
    top_n: int,
    date_from_epoch_ms: int | None,
    date_to_epoch_ms: int | None,
) -> list[dict[str, Any]]:
    cfg = ConfigFactory.get_config()
    client = get_s3vectors_client(region=cfg.aws_region or None)
    return search_image_vectors_s3(
        client,
        query_vector,
        limit=top_n,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
        vector_bucket_name=cfg.s3_vectors_bucket,
        index_name=cfg.s3_vectors_images_index,
    )


async def build_image_search_plan(
    query_vector: Sequence[float],
    *,
    top_n: int = 100,
    max_messages_per_phash: int = _DEFAULT_MAX_MESSAGES_PER_PHASH,
    date_from_epoch_ms: int | None = None,
    date_to_epoch_ms: int | None = None,
) -> list[ImageSearchPlanHit]:
    """Rank unique images via ANN and expand to an ordered message plan."""
    per_phash = max(1, int(max_messages_per_phash))
    lg.info("image search: S3 Vectors ANN (top_n=%s)…", top_n)
    started = time.perf_counter()
    hits = await asyncio.to_thread(
        _search_image_hits,
        query_vector,
        top_n=top_n,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
    )
    record_span("s3vectors", "search_images", time.perf_counter() - started)
    lg.info("image search: S3 Vectors returned %s hits", len(hits))

    scored_phashes: list[tuple[str, float, dict[str, Any]]] = []
    for hit in hits:
        phash = str(hit.get("id") or "").strip().lower()
        if len(phash) != 16:
            continue
        if not _passes_similarity_threshold(hit.get("similarity")):
            continue
        scored_phashes.append((phash, float(hit.get("similarity") or 0.0), hit))

    if not scored_phashes:
        lg.info("image search: 0 phashes after similarity filter")
        return []

    from services.api.src.crud.supabase import get_conn

    lg.info("image search: expanding %s phashes → messages…", len(scored_phashes))
    async with get_conn() as conn:
        map_rows = await list_messages_for_phashes(conn, [p for p, _, _ in scored_phashes])
        by_phash: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in map_rows:
            by_phash[str(row["phash"])].append(row)

        planned: list[ImageSearchPlanHit] = []
        for phash, score, hit in scored_phashes:
            messages = by_phash.get(phash) or []
            s3_key = str(hit.get("s3_key") or "")
            for map_row in messages[:per_phash]:
                plan_hit = plan_hit_from_map_row(
                    phash=phash,
                    score=score,
                    s3_key=s3_key,
                    map_row=map_row,
                )
                if plan_hit is not None:
                    planned.append(plan_hit)

    lg.info("image search: plan ready (%s messages)", len(planned))
    return planned


async def search_image_hits(
    query_vector: Sequence[float],
    *,
    top_n: int = 100,
    max_messages_per_phash: int = _DEFAULT_MAX_MESSAGES_PER_PHASH,
    limit: int = _DEFAULT_PAGE_SIZE,
    offset: int = 0,
    date_from_epoch_ms: int | None = None,
    date_to_epoch_ms: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Rank unique images via ANN, fan out to messages, hydrate one page.

    Returns ``(page_items, total_matching_messages)``.
    """
    page_limit, page_offset = clamp_page(limit=limit, offset=offset)
    planned = await build_image_search_plan(
        query_vector,
        top_n=top_n,
        max_messages_per_phash=max_messages_per_phash,
        date_from_epoch_ms=date_from_epoch_ms,
        date_to_epoch_ms=date_to_epoch_ms,
    )
    return await materialize_image_search_page(
        planned,
        limit=page_limit,
        offset=page_offset,
    )
