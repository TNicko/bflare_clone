"""Single funnel: keep ``telegram_media_download_coverage`` counts = harvested S3 inventory.

Every successful ``harvest.s3_uri`` write must call :func:`record_harvested_media`.
Gap / on-demand completion calls :func:`claim_media_download_window` to seal a
date range for gap planning while reconciling counts from messages.

Counts are inventory of messages with ``harvest.s3_uri`` (by downloadable kind).
At most one ``seals_gap=false`` island is kept per peer (running inventory while a
gap is in progress). ``seals_gap=true`` islands close ranges for gap planning.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row

from db.crud.telegram.coverage import (
    list_media_download_coverage,
    lock_media_download_coverage,
    replace_media_download_coverage,
)
from db.schemas.general.primitives import TelegramPeerTypeSchema
from telegram.media import harvest_s3_uri, requires_harvest_for_coverage
from telegram.media_kinds import DownloadableKind, classify_downloadable_kind, parse_slim_media
from telegram.media_limits import is_large_media
from utils.coverage_intervals import (
    CoverageInterval,
    MediaCoverageInterval,
    add_media_count,
    default_phashes_in_milvus,
    empty_media_counts,
    ensure_utc,
    exclusive_end_after,
    merge_media_intervals,
)

lg = logging.getLogger(__name__)

_COUNT_HARVESTED_IN_WINDOW_SQL = """
SELECT media
FROM telegram_messages
WHERE source_external_id = %s
  AND (%s::timestamptz IS NULL OR date >= %s::timestamptz)
  AND date < %s::timestamptz
  AND media IS NOT NULL
  AND media <> '{}'::jsonb
"""


def _intervals_overlap(a: CoverageInterval, b: CoverageInterval) -> bool:
    a0 = a.covered_after
    b0 = b.covered_after
    a1 = a.covered_before
    b1 = b.covered_before
    if a1 is None or b1 is None:
        return False
    if a0 is not None and a0 >= b1:
        return False
    if b0 is not None and b0 >= a1:
        return False
    return True


def _date_in_interval(date: datetime, island: MediaCoverageInterval) -> bool:
    d = ensure_utc(date)
    start = island.covered_after
    end = ensure_utc(island.covered_before)
    if end is None:
        return False
    if start is not None and d < ensure_utc(start):
        return False
    return d < end


def _counts_dict(island: MediaCoverageInterval) -> dict[str, int]:
    return {
        "count_image": island.count_image,
        "count_gif": island.count_gif,
        "count_video": island.count_video,
        "count_audio": island.count_audio,
        "count_document": island.count_document,
        "count_webpage": island.count_webpage,
    }


def _sum_island_counts(islands: list[MediaCoverageInterval]) -> dict[str, int]:
    totals = empty_media_counts()
    for island in islands:
        for key, value in _counts_dict(island).items():
            totals[key] += value
    return totals


def _bump_counts(island: MediaCoverageInterval, kind: DownloadableKind) -> MediaCoverageInterval:
    counts = _counts_dict(island)
    add_media_count(counts, kind)
    milvus = island.phashes_in_milvus
    if kind == "image":
        milvus = default_phashes_in_milvus(count_image=counts["count_image"], milvus_synced=False)
    return MediaCoverageInterval(
        covered_after=island.covered_after,
        covered_before=island.covered_before,
        phashes_in_milvus=milvus,
        videos_excluded=island.videos_excluded,
        large_excluded=island.large_excluded,
        seals_gap=island.seals_gap,
        **counts,
    )


def _min_after(dates: list[datetime | None]) -> datetime | None:
    finite = [ensure_utc(d) for d in dates if d is not None]
    if len(finite) < len(dates):
        return None
    return min(finite) if finite else None


def _max_before(dates: list[datetime]) -> datetime:
    return max(ensure_utc(d) for d in dates)


def _apply_count_delta(
    island: MediaCoverageInterval,
    delta: dict[str, int],
) -> MediaCoverageInterval:
    counts = _counts_dict(island)
    for key, value in delta.items():
        counts[key] += value
    milvus = island.phashes_in_milvus
    if counts["count_image"] != island.count_image:
        milvus = default_phashes_in_milvus(count_image=counts["count_image"], milvus_synced=False)
    return MediaCoverageInterval(
        covered_after=island.covered_after,
        covered_before=island.covered_before,
        phashes_in_milvus=milvus,
        videos_excluded=island.videos_excluded,
        large_excluded=island.large_excluded,
        seals_gap=island.seals_gap,
        **counts,
    )


def collapse_unsealed_inventory(
    unsealed: list[MediaCoverageInterval],
    *,
    extra_date: datetime | None = None,
    extra_dates: list[datetime] | None = None,
    extra_kind: DownloadableKind | None = None,
    extra_counts: dict[str, int] | None = None,
    span: CoverageInterval | None = None,
) -> MediaCoverageInterval | None:
    """Merge inventory-only islands into a single ``seals_gap=false`` row.

    When ``span`` is set (active download gap), the island bounds match the gap
    so one in-progress row covers the whole window while counts grow.
    Otherwise bounds are the min/max of existing unsealed tips plus ``extra_date``.
    """
    counts = _sum_island_counts(unsealed)
    if extra_kind is not None:
        add_media_count(counts, extra_kind)
    if extra_counts is not None:
        for key, value in extra_counts.items():
            counts[key] += value

    total = sum(counts.values())
    if total <= 0 and not unsealed and extra_date is None:
        return None

    if span is not None:
        after = ensure_utc(span.covered_after)
        before = ensure_utc(span.covered_before)
    else:
        afters: list[datetime | None] = [i.covered_after for i in unsealed]
        befores: list[datetime] = [i.covered_before for i in unsealed]
        if extra_dates:
            for d in extra_dates:
                afters.append(d)
                befores.append(exclusive_end_after(d))
        elif extra_date is not None:
            afters.append(extra_date)
            befores.append(exclusive_end_after(extra_date))
        if not befores:
            return None
        after = _min_after(afters)
        before = _max_before(befores)

    return MediaCoverageInterval(
        covered_after=after,
        covered_before=before,
        phashes_in_milvus=default_phashes_in_milvus(
            count_image=counts["count_image"],
            milvus_synced=False,
        ),
        videos_excluded=False,
        large_excluded=False,
        seals_gap=False,
        **counts,
    )


def _clip_coverage_bounds(
    island: CoverageInterval,
    *,
    cut: CoverageInterval,
) -> list[CoverageInterval]:
    """Return sub-ranges of ``island`` that do not overlap ``cut``."""
    a0 = ensure_utc(island.covered_after)
    a1 = ensure_utc(island.covered_before)
    c0 = ensure_utc(cut.covered_after)
    c1 = ensure_utc(cut.covered_before)
    if a1 is None or c1 is None:
        return []

    out: list[CoverageInterval] = []
    # Left remnant: [a0, min(a1, c0)) when a0 < c0 (c0 None ⇒ cut starts −∞ ⇒ no left)
    if c0 is not None:
        left_end = c0 if a1 is None else min(a1, c0)
        if a0 is None or a0 < left_end:
            out.append(CoverageInterval(covered_after=a0, covered_before=left_end))
    # Right remnant: [max(a0, c1), a1)
    right_start = c1 if a0 is None else max(a0, c1)
    if right_start < a1:
        out.append(CoverageInterval(covered_after=right_start, covered_before=a1))
    return out


async def count_harvested_media_in_window(
    conn: Any,
    *,
    peer_external_id: str,
    covered_after: datetime | None,
    covered_before: datetime,
) -> tuple[dict[str, int], bool]:
    """Return (kind counts with s3_uri, large_excluded) for ``[after, before)``.

    ``large_excluded`` is true when any downloadable message in the window lacks
    S3 and is oversized (intentionally deferred).
    """
    before = ensure_utc(covered_before)
    after = ensure_utc(covered_after)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _COUNT_HARVESTED_IN_WINDOW_SQL,
            (peer_external_id, after, after, before),
        )
        rows = await cur.fetchall()

    counts = empty_media_counts()
    large_excluded = False
    for row in rows:
        slim = parse_slim_media(row.get("media"))
        if not requires_harvest_for_coverage(slim):
            continue
        kind = classify_downloadable_kind(slim)
        if kind is None:
            continue
        if harvest_s3_uri(slim):
            add_media_count(counts, kind)
            continue
        if is_large_media(slim):
            large_excluded = True
    return counts, large_excluded


async def record_harvested_media(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    message_date: datetime,
    kind: DownloadableKind,
    inventory_span: CoverageInterval | None = None,
) -> None:
    """Funnel entry: bump coverage inventory after a successful S3 harvest write.

    - If ``message_date`` falls in a sealed island, bump that island (fill / re-harvest).
    - Otherwise keep a **single** ``seals_gap=false`` inventory row (merged). When
      ``inventory_span`` is the active download gap, that row uses the gap bounds
      so mid-run progress is one row, not one tip per file.
    """
    await record_harvested_media_batch(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
        harvests=[(message_date, kind)],
        inventory_span=inventory_span,
        commit=True,
    )


async def record_harvested_media_batch(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    harvests: list[tuple[datetime, DownloadableKind]],
    inventory_span: CoverageInterval | None = None,
    commit: bool = True,
) -> None:
    """Bump coverage inventory for several successful harvest writes in one reconcile."""
    if not harvests:
        return

    await lock_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    current = await list_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )

    sealed = [i for i in current if i.seals_gap]
    unsealed = [i for i in current if not i.seals_gap]
    sealed_deltas: dict[int, dict[str, int]] = {}
    unsealed_extra = empty_media_counts()
    unsealed_extra_dates: list[datetime] = []

    for message_date, kind in harvests:
        date = ensure_utc(message_date)
        sealed_hit_idxs = [idx for idx, i in enumerate(sealed) if _date_in_interval(date, i)]
        if sealed_hit_idxs:
            sealed_hit_idxs.sort(
                key=lambda idx: (
                    sealed[idx].covered_after is not None,
                    sealed[idx].covered_after or date,
                )
            )
            target = sealed_hit_idxs[0]
            delta = sealed_deltas.setdefault(target, empty_media_counts())
            add_media_count(delta, kind)
            continue
        add_media_count(unsealed_extra, kind)
        if inventory_span is None:
            unsealed_extra_dates.append(date)

    new_sealed = list(sealed)
    for idx, delta in sealed_deltas.items():
        new_sealed[idx] = _apply_count_delta(new_sealed[idx], delta)

    has_unsealed_extra = sum(unsealed_extra.values()) > 0
    collapsed: MediaCoverageInterval | None = None
    if has_unsealed_extra:
        collapsed = collapse_unsealed_inventory(
            unsealed,
            extra_counts=unsealed_extra,
            extra_dates=unsealed_extra_dates if inventory_span is None else None,
            span=inventory_span,
        )
    elif unsealed:
        collapsed = collapse_unsealed_inventory(unsealed, span=inventory_span)

    updated = new_sealed + ([collapsed] if collapsed is not None else [])

    merged = merge_media_intervals(updated)
    await replace_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
        intervals=merged,
        commit=commit,
    )


async def claim_media_download_window(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    covered_after: datetime | None,
    covered_before: datetime,
    videos_excluded: bool,
    large_excluded: bool | None = None,
    preserve_milvus_flag: bool = True,
) -> dict[str, int]:
    """Seal ``[covered_after, covered_before)`` with harvested-only counts (reconcile).

    Overlapping sealed islands are replaced. Overlapping unsealed inventory is
    clipped; remnants outside the claim keep recounted harvested counts so a
    wide in-progress inventory row is not wiped by a small on-demand seal.
    """
    gap = CoverageInterval(
        covered_after=ensure_utc(covered_after),
        covered_before=ensure_utc(covered_before),
    )
    await lock_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    current = await list_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )

    gap_counts, large_from_db = await count_harvested_media_in_window(
        conn,
        peer_external_id=peer_external_id,
        covered_after=gap.covered_after,
        covered_before=gap.covered_before,
    )
    resolved_large = large_from_db if large_excluded is None else bool(large_excluded or large_from_db)

    milvus_flag = default_phashes_in_milvus(
        count_image=int(gap_counts.get("count_image") or 0),
        milvus_synced=False,
    )
    remaining: list[MediaCoverageInterval] = []
    for island in current:
        if not _intervals_overlap(island.as_coverage_interval(), gap):
            remaining.append(island)
            continue
        if preserve_milvus_flag:
            milvus_flag = island.phashes_in_milvus
        if island.seals_gap:
            # Replaced by the new sealed claim for this span.
            continue
        # Clip unsealed inventory; recount remnants from S3.
        for remnant in _clip_coverage_bounds(island.as_coverage_interval(), cut=gap):
            rem_counts, _large = await count_harvested_media_in_window(
                conn,
                peer_external_id=peer_external_id,
                covered_after=remnant.covered_after,
                covered_before=remnant.covered_before,
            )
            if sum(rem_counts.values()) <= 0:
                continue
            remaining.append(
                MediaCoverageInterval(
                    covered_after=remnant.covered_after,
                    covered_before=remnant.covered_before,
                    phashes_in_milvus=default_phashes_in_milvus(
                        count_image=rem_counts["count_image"],
                        milvus_synced=False,
                    ),
                    videos_excluded=False,
                    large_excluded=False,
                    seals_gap=False,
                    **rem_counts,
                )
            )

    # Keep at most one unsealed inventory row.
    sealed_kept = [i for i in remaining if i.seals_gap]
    unsealed_kept = [i for i in remaining if not i.seals_gap]
    collapsed_unsealed = collapse_unsealed_inventory(unsealed_kept)

    sealed = MediaCoverageInterval(
        covered_after=gap.covered_after,
        covered_before=gap.covered_before,
        phashes_in_milvus=milvus_flag,
        videos_excluded=videos_excluded,
        large_excluded=resolved_large,
        seals_gap=True,
        **gap_counts,
    )
    pieces = sealed_kept + ([collapsed_unsealed] if collapsed_unsealed is not None else []) + [sealed]
    merged = merge_media_intervals(pieces)
    await replace_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
        intervals=merged,
        commit=True,
    )
    lg.info(
        "Claimed media coverage peer=%s type=%s span=[%s, %s) counts=%s videos_excluded=%s large_excluded=%s",
        peer_external_id,
        peer_type,
        gap.covered_after,
        gap.covered_before,
        gap_counts,
        videos_excluded,
        resolved_large,
    )
    return gap_counts


async def replace_peer_media_coverage(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    intervals: list[MediaCoverageInterval],
) -> None:
    """Locked full replace (repair / rebuild)."""
    await lock_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    sealed = [i for i in intervals if i.seals_gap]
    unsealed = [i for i in intervals if not i.seals_gap]
    collapsed = collapse_unsealed_inventory(unsealed)
    merged = merge_media_intervals(sealed + ([collapsed] if collapsed is not None else []))
    await replace_media_download_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
        intervals=merged,
        commit=True,
    )
