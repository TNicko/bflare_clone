"""Postgres pHash Hamming + map fan-out for hash image search.

Near-dups come from ``telegram_image_blobs`` (bit_count Hamming). Hits expand to
messages via ``telegram_image_blob_messages``, then hydrate from ``telegram_messages``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Literal
from uuid import UUID

from db.crud.telegram.image_blobs import list_messages_for_phashes, search_near_duplicate_phashes
from phash.core import compute_phash, match_score, passes_hamming_threshold
from services.api.src.image_search_session import (
    ImageSearchPlanHit,
    clamp_page,
    materialize_image_search_page,
    plan_hit_from_map_row,
)

logger = logging.getLogger(__name__)

#: Safety ceiling when callers omit max_messages_per_phash (viral images can have 80+ copies).
_DEFAULT_MAX_MESSAGES_PER_PHASH = 10_000
_DEFAULT_PAGE_SIZE = 50

SortMode = Literal["score", "date_desc"]


async def build_image_hash_search_plan_from_phash(
    phash_hex: str,
    *,
    top_n: int = 100,
    max_hamming: int = 5,
    max_messages_per_phash: int = _DEFAULT_MAX_MESSAGES_PER_PHASH,
    sort: SortMode = "score",
    exclude_message_ids: set[str] | frozenset[str] | None = None,
) -> list[ImageSearchPlanHit]:
    """Find near-duplicate images from a stored hex pHash; return ordered plan."""
    per_phash = max(1, int(max_messages_per_phash))
    logger.info(
        "hash search: Postgres Hamming (phash=%s… top_n=%s max_hamming=%s)…",
        phash_hex[:8],
        top_n,
        max_hamming,
    )

    from services.api.src.crud.supabase import get_conn

    async with get_conn() as conn:
        hits = await search_near_duplicate_phashes(
            conn,
            phash_hex,
            max_hamming=max_hamming,
            limit=top_n,
        )
        logger.info("hash search: Postgres returned %s hits", len(hits))

        scored_phashes: list[tuple[str, float, dict[str, Any]]] = []
        for hit in hits:
            phash = str(hit.get("phash") or "").strip().lower()
            if len(phash) != 16:
                continue
            distance = hit.get("distance")
            if not passes_hamming_threshold(distance, max_hamming=max_hamming):
                continue
            scored_phashes.append((phash, float(distance or 0.0), hit))

        if not scored_phashes:
            logger.info("hash search: 0 phashes after hamming filter (max=%s)", max_hamming)
            return []

        exclude = {str(x) for x in (exclude_message_ids or ())}
        logger.info("hash search: expanding %s phashes → messages…", len(scored_phashes))
        map_rows = await list_messages_for_phashes(conn, [p for p, _, _ in scored_phashes])
        by_phash: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in map_rows:
            by_phash[str(row["phash"])].append(row)

        planned: list[ImageSearchPlanHit] = []
        for phash, distance, hit in scored_phashes:
            messages = by_phash.get(phash) or []
            s3_key = str(hit.get("s3_key") or "")
            score = match_score(distance)
            for map_row in messages[:per_phash]:
                message_id = map_row.get("message_id")
                if message_id is not None:
                    msg_id = str(message_id if isinstance(message_id, UUID) else message_id)
                    if msg_id in exclude:
                        continue
                plan_hit = plan_hit_from_map_row(
                    phash=phash,
                    score=score,
                    s3_key=s3_key,
                    map_row=map_row,
                    hamming_distance=float(distance),
                )
                if plan_hit is not None:
                    planned.append(plan_hit)

        if sort == "date_desc":
            planned.sort(key=lambda item: item.message_date_iso, reverse=True)

    logger.info("hash search: plan ready (%s messages)", len(planned))
    return planned


async def search_image_hash_hits_from_phash(
    phash_hex: str,
    *,
    top_n: int = 100,
    max_hamming: int = 5,
    max_messages_per_phash: int = _DEFAULT_MAX_MESSAGES_PER_PHASH,
    limit: int = _DEFAULT_PAGE_SIZE,
    offset: int = 0,
    sort: SortMode = "score",
    exclude_message_ids: set[str] | frozenset[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Find near-duplicate images from a stored hex pHash (Postgres Hamming).

    Returns ``(page_items, total_matching_messages)``.
    """
    page_limit, page_offset = clamp_page(limit=limit, offset=offset)
    planned = await build_image_hash_search_plan_from_phash(
        phash_hex,
        top_n=top_n,
        max_hamming=max_hamming,
        max_messages_per_phash=max_messages_per_phash,
        sort=sort,
        exclude_message_ids=exclude_message_ids,
    )
    return await materialize_image_search_page(
        planned,
        limit=page_limit,
        offset=page_offset,
    )


async def build_image_hash_search_plan(
    image_bytes: bytes,
    *,
    top_n: int = 100,
    max_hamming: int = 5,
    max_messages_per_phash: int = _DEFAULT_MAX_MESSAGES_PER_PHASH,
    sort: SortMode = "score",
    exclude_message_ids: set[str] | frozenset[str] | None = None,
) -> list[ImageSearchPlanHit]:
    """Compute pHash from image bytes, then build the near-duplicate plan."""
    from phash.core import int_to_hex

    logger.info("hash search: computing pHash (%s bytes)…", len(image_bytes))
    phash_hex = int_to_hex(compute_phash(image_bytes))
    logger.info("hash search: pHash=%s", phash_hex)
    return await build_image_hash_search_plan_from_phash(
        phash_hex,
        top_n=top_n,
        max_hamming=max_hamming,
        max_messages_per_phash=max_messages_per_phash,
        sort=sort,
        exclude_message_ids=exclude_message_ids,
    )


async def search_image_hash_hits(
    image_bytes: bytes,
    *,
    top_n: int = 100,
    max_hamming: int = 5,
    max_messages_per_phash: int = _DEFAULT_MAX_MESSAGES_PER_PHASH,
    limit: int = _DEFAULT_PAGE_SIZE,
    offset: int = 0,
    sort: SortMode = "score",
    exclude_message_ids: set[str] | frozenset[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Compute pHash from image bytes, then search near-duplicates."""
    page_limit, page_offset = clamp_page(limit=limit, offset=offset)
    planned = await build_image_hash_search_plan(
        image_bytes,
        top_n=top_n,
        max_hamming=max_hamming,
        max_messages_per_phash=max_messages_per_phash,
        sort=sort,
        exclude_message_ids=exclude_message_ids,
    )
    return await materialize_image_search_page(
        planned,
        limit=page_limit,
        offset=page_offset,
    )
