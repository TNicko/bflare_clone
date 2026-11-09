"""Record small harvest coverage islands for on-demand chat sync / media fetch.

Workers own full-window backfill; these helpers only merge tight islands for what
the API actually pulled so progress is visible and download gaps can open.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row

from db.crud.telegram.coverage import (
    build_fetch_interval_for_window,
    list_coverage,
    replace_coverage,
)
from db.schemas.general.primitives import TelegramPeerTypeSchema
from services.api.src.crud.supabase.messages import count_peer_messages
from telegram.media import harvest_s3_uri, requires_harvest_for_coverage
from telegram.media_kinds import classify_downloadable_kind, parse_slim_media
from telegram.media_limits import is_large_media
from utils.coverage_intervals import (
    FetchCoverageInterval,
    add_media_count,
    empty_media_counts,
    ensure_utc,
    merge_fetch_intervals,
)

lg = logging.getLogger(__name__)


def _exclusive_end(max_date: datetime) -> datetime:
    """Half-open coverage end strictly after ``max_date``."""
    return ensure_utc(max_date) + timedelta(microseconds=1)


async def record_on_demand_message_fetch_coverage(
    conn: Any,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    message_dates: list[datetime],
    tip_complete: bool,
    empty_chat: bool = False,
    after_message_id: int | None = None,
) -> None:
    """Merge a tip island ``[min_date, now)`` when the sync exhausted newer history.

    Matches fetch_messages early-stop semantics: only claim coverage when the
    contiguous tip from Telegram is complete (not a truncated ``limit`` page of
    an incomplete newer set). An empty *initial* tip (``empty_chat``) claims
    ``[−∞, now)``. Incremental polls with no new rows still seal the tip by
    anchoring to ``after_message_id``'s date when present.
    """
    if not tip_complete:
        return

    before = datetime.now(UTC)
    dates = [ensure_utc(d) for d in message_dates if d is not None]
    if dates:
        min_date = min(dates)
        if min_date >= before:
            before = _exclusive_end(min_date)
        island = await build_fetch_interval_for_window(
            conn,
            peer_external_id=peer_external_id,
            covered_after=min_date,
            covered_before=before,
        )
    elif empty_chat:
        # Live GetHistory can be empty when the session cannot read the peer even
        # though workers already scraped messages into DB — never claim −∞ then.
        existing = await count_peer_messages(conn, peer_external_id)
        if existing > 0:
            lg.info(
                "Skipping empty-chat tip coverage; peer already has %s DB messages peer=%s type=%s",
                existing,
                peer_external_id,
                peer_type,
            )
            return
        island = await build_fetch_interval_for_window(
            conn,
            peer_external_id=peer_external_id,
            covered_after=None,
            covered_before=before,
            message_count=existing,
        )
    else:
        # Incremental sync can validly return zero new rows while still proving
        # the tip is complete. Anchor coverage to the known after_message_id date.
        if after_message_id is None or after_message_id <= 0:
            return
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT date
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND message_id = %s
                  AND COALESCE(deleted, false) = false
                LIMIT 1
                """,
                (peer_external_id, after_message_id),
            )
            anchor = await cur.fetchone()
        anchor_date = ensure_utc(anchor["date"]) if anchor and anchor.get("date") is not None else None
        if anchor_date is None:
            return
        if anchor_date >= before:
            before = _exclusive_end(anchor_date)
        island = await build_fetch_interval_for_window(
            conn,
            peer_external_id=peer_external_id,
            covered_after=anchor_date,
            covered_before=before,
        )

    try:
        current = await list_coverage(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
        )
        merged = merge_fetch_intervals([*current, island])
        await replace_coverage(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
            intervals=merged,
        )
        lg.info(
            "On-demand message coverage peer=%s type=%s island=[%s, %s) islands=%s",
            peer_external_id,
            peer_type,
            island.covered_after,
            island.covered_before,
            len(merged),
        )
    except Exception:
        lg.exception(
            "Failed to record on-demand message coverage peer=%s",
            peer_external_id,
        )


async def record_on_demand_recent_window_coverage(
    conn: Any,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    since_days: int,
    message_dates: list[datetime] | None = None,
    refresh_complete: bool = False,
) -> None:
    """Extend coverage as ``refresh_recent`` pages land; seal the since-window when done.

    Each in-progress page merges ``[min(message_dates), now)`` (same tip-through-now
    shape as older paging) so inventory badges track saved batches mid-walk. When
    ``refresh_complete``, also claim ``[now − since_days, now)`` so 7d / 30d volume
    widgets can gate on a continuous recent window.
    """
    days = max(1, int(since_days))
    # Claim slightly past "now" so a same-request volume gate (which re-samples
    # datetime.now) does not see a half-open tip gap after this write.
    before = datetime.now(UTC) + timedelta(minutes=1)
    dates = [ensure_utc(d) for d in (message_dates or []) if d is not None]

    islands: list[FetchCoverageInterval] = []
    if dates:
        min_date = min(dates)
        page_before = before
        if min_date >= page_before:
            page_before = _exclusive_end(min_date)
        islands.append(
            await build_fetch_interval_for_window(
                conn,
                peer_external_id=peer_external_id,
                covered_after=min_date,
                covered_before=page_before,
            )
        )

    if refresh_complete:
        after = before - timedelta(days=days)
        islands.append(
            await build_fetch_interval_for_window(
                conn,
                peer_external_id=peer_external_id,
                covered_after=after,
                covered_before=before,
            )
        )

    if not islands:
        return

    try:
        current = await list_coverage(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
        )
        merged = merge_fetch_intervals([*current, *islands])
        await replace_coverage(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
            intervals=merged,
        )
        newest = islands[-1]
        lg.info(
            "On-demand recent-window coverage peer=%s type=%s since_days=%s "
            "refresh_complete=%s page_dates=%s island=[%s, %s) islands=%s",
            peer_external_id,
            peer_type,
            days,
            refresh_complete,
            len(dates),
            newest.covered_after,
            newest.covered_before,
            len(merged),
        )
    except Exception:
        lg.exception(
            "Failed to record recent-window coverage peer=%s",
            peer_external_id,
        )


async def record_on_demand_older_message_fetch_coverage(
    conn: Any,
    *,
    harvester_id: UUID,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    message_dates: list[datetime],
    reached_beginning: bool,
) -> None:
    """Extend coverage toward −∞ while paging older history from the live UI.

    Each older page claims ``[min_date, now)`` (same tip-through-now shape as tip
    sync) so sparse date gaps between consecutive pages still merge. When
    Telegram returns a short/empty oldest page, claim ``covered_after=None``.
    """
    before = datetime.now(UTC)
    dates = [ensure_utc(d) for d in message_dates if d is not None]

    if reached_beginning:
        if not dates:
            # Empty older page (e.g. before_message_id=1 with no Telethon access)
            # must not mark full history when DB already has scraped rows.
            existing = await count_peer_messages(conn, peer_external_id)
            if existing > 0:
                lg.info(
                    "Skipping reached_beginning −∞ coverage; peer already has %s "
                    "DB messages and empty older page peer=%s type=%s",
                    existing,
                    peer_external_id,
                    peer_type,
                )
                return
        island = await build_fetch_interval_for_window(
            conn,
            peer_external_id=peer_external_id,
            covered_after=None,
            covered_before=before,
        )
    elif dates:
        min_date = min(dates)
        if min_date >= before:
            before = _exclusive_end(min_date)
        island = await build_fetch_interval_for_window(
            conn,
            peer_external_id=peer_external_id,
            covered_after=min_date,
            covered_before=before,
        )
    else:
        return

    try:
        current = await list_coverage(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
        )
        merged = merge_fetch_intervals([*current, island])
        await replace_coverage(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
            intervals=merged,
        )
        lg.info(
            "On-demand older coverage peer=%s type=%s reached_beginning=%s island=[%s, %s) islands=%s",
            peer_external_id,
            peer_type,
            reached_beginning,
            island.covered_after,
            island.covered_before,
            len(merged),
        )
    except Exception:
        lg.exception(
            "Failed to record on-demand older coverage peer=%s",
            peer_external_id,
        )


async def record_on_demand_media_download_coverage(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    harvested_dates: list[datetime],
) -> None:
    """Seal a media-download island when every downloadable msg is harvested or large-deferred.

    Oversized attachments (≥ auto-harvest threshold) may remain without S3; the
    island is still claimed with ``large_excluded=true`` so coverage stays
    continuous. Avoids marking a shared timestamp (e.g. album) covered when only
    one small item was downloaded and siblings are still pending (non-large).

    Per-message inventory bumps happen via :func:`record_harvested_media` at
    persist time; this only seals the tip when the span is complete.
    """
    dates = [ensure_utc(d) for d in harvested_dates if d is not None]
    if not dates:
        return

    lo = min(dates)
    hi = _exclusive_end(max(dates))

    try:
        _counts, complete, large_excluded = await _downloadable_harvest_status(
            conn,
            peer_external_id=peer_external_id,
            covered_after=lo,
            covered_before=hi,
        )
        if not complete:
            lg.info(
                "On-demand media coverage skip seal peer=%s span=[%s, %s): "
                "not all downloadable media harvested or large-deferred yet",
                peer_external_id,
                lo,
                hi,
            )
            return

        from telegram.media_coverage_sync import claim_media_download_window

        await claim_media_download_window(
            conn,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
            covered_after=lo,
            covered_before=hi,
            videos_excluded=False,
            large_excluded=large_excluded,
        )
    except Exception:
        lg.exception(
            "Failed to record on-demand media coverage peer=%s",
            peer_external_id,
        )


async def _downloadable_harvest_status(
    conn: Any,
    *,
    peer_external_id: str,
    covered_after: datetime,
    covered_before: datetime,
) -> tuple[dict[str, int], bool, bool]:
    """Return (harvested kind counts, span_complete, large_excluded) for the date span.

    ``span_complete`` is true when every downloadable message is either harvested
    or intentionally deferred as oversized. Counts only include rows with S3.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT media
            FROM telegram_messages
            WHERE source_external_id = %s
              AND date >= %s::timestamptz
              AND date < %s::timestamptz
            """,
            (peer_external_id, covered_after, covered_before),
        )
        rows = await cur.fetchall()

    counts = empty_media_counts()
    downloadable = 0
    satisfied = 0
    large_excluded = False
    for row in rows:
        slim = parse_slim_media(row.get("media"))
        if not requires_harvest_for_coverage(slim):
            continue
        kind = classify_downloadable_kind(slim)
        if kind is None:
            continue
        downloadable += 1
        if harvest_s3_uri(slim):
            add_media_count(counts, kind)
            satisfied += 1
            continue
        if is_large_media(slim):
            satisfied += 1
            large_excluded = True

    if downloadable == 0:
        return counts, False, False
    return counts, satisfied >= downloadable, large_excluded
