"""Peer message-history completeness for gating API ``total_count``.

History is complete when merged fetch-coverage for the peer has no gaps in
``[−∞, now − tip_slack)`` — i.e. a contiguous island from the beginning of
time through a tip that is still within the last month (by default).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg.rows import dict_row

from db.crud.telegram.coverage import peer_coverage_message_count
from utils.coverage_intervals import CoverageInterval, merge_intervals, partition_envelope

lg = logging.getLogger(__name__)

# Tip may lag by up to this long and still count as "complete" for totals.
DEFAULT_HISTORY_TIP_SLACK_DAYS = 30

#: Chat open fills gaps inside this window. Older gaps stay on the incomplete-history button.
RECENT_WINDOW_DAYS = 30

#: Ignore a trailing sliver so a sync that just reached "now" is not immediately stale.
#: A missing day, or any hole inside the window, still counts as incomplete.
RECENT_WINDOW_TIP_SLACK = timedelta(minutes=2)

#: Exclude ``is_web`` islands — t.me/s coverage is not MTProto-faithful history.
_LIST_PEER_COVERAGE_SQL = """
SELECT covered_after, covered_before
FROM telegram_message_fetch_coverage
WHERE peer_external_id = %s
  AND (%s::text IS NULL OR peer_type = %s)
  AND COALESCE(is_web, false) = false
ORDER BY covered_after NULLS FIRST, covered_before
"""


async def list_peer_message_coverage(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str | None = None,
) -> list[CoverageInterval]:
    """Load fetch-coverage islands for a peer across all harvesters."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _LIST_PEER_COVERAGE_SQL,
            (peer_external_id, peer_type, peer_type),
        )
        rows = await cur.fetchall()
    return [CoverageInterval(covered_after=r["covered_after"], covered_before=r["covered_before"]) for r in rows]


def latest_coverage_before(
    intervals: list[CoverageInterval],
) -> datetime | None:
    """Exclusive end of the newest merged coverage island, if any."""
    if not intervals:
        return None
    merged = merge_intervals(intervals)
    if not merged:
        return None
    return max(interval.covered_before for interval in merged)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def is_history_complete(
    intervals: list[CoverageInterval],
    *,
    tip_slack_days: int = DEFAULT_HISTORY_TIP_SLACK_DAYS,
    now: datetime | None = None,
) -> bool:
    """True when coverage has no gaps from −∞ through ``now − tip_slack_days``.

    If the newest coverage tip is older than that window, history is incomplete
    for totals (stale tip is ignored).
    """
    if not intervals:
        return False
    merged = merge_intervals(intervals)
    if not merged:
        return False
    tip = (now or datetime.now(UTC)) - timedelta(days=max(0, tip_slack_days))
    _, gaps = partition_envelope(merged, window_after=None, window_before=tip)
    return len(gaps) == 0


def is_recent_window_complete(
    intervals: list[CoverageInterval],
    *,
    since_days: int = RECENT_WINDOW_DAYS,
    now: datetime | None = None,
) -> bool:
    """True when ``[now − since_days, now)`` has no fetch-coverage gaps.

    Gaps older than the window are ignored — those stay on the incomplete-history
    button. A trailing gap shorter than :data:`RECENT_WINDOW_TIP_SLACK` is ignored
    so the live tip sync can cover it without another full walk.
    """
    if not intervals:
        return False
    end = _as_utc(now or datetime.now(UTC))
    start = end - timedelta(days=max(1, since_days))
    tip = end - RECENT_WINDOW_TIP_SLACK
    if tip <= start:
        return False
    # Do not pre-merge: an open-ended island (−∞) would swallow later islands
    # and hide a hole. ``partition_envelope`` clips to this window first.
    _, gaps = partition_envelope(intervals, window_after=start, window_before=tip)
    return len(gaps) == 0


def is_volume_window_displayable(
    intervals: list[CoverageInterval],
    *,
    days: int,
    now: datetime | None = None,
) -> bool:
    """True when 7d/30d activity counts may be shown from DB coverage.

    Requires:
    - a coverage tip whose UTC calendar date is today (missing &lt; 1 full day at tip), and
    - continuous coverage in ``[now − days, tip)`` (window behind the tip).
    """
    if not intervals or days <= 0:
        return False
    tip = latest_coverage_before(intervals)
    if tip is None:
        return False
    end = now or datetime.now(UTC)
    tip_utc = _as_utc(tip)
    end_utc = _as_utc(end)
    # Tip stuck on a prior UTC calendar day → ≥1 full day missing at tip.
    if tip_utc.date() < end_utc.date():
        return False
    start = end_utc - timedelta(days=days)
    if tip_utc <= start:
        return False
    merged = merge_intervals(intervals)
    _, gaps = partition_envelope(merged, window_after=start, window_before=tip_utc)
    return len(gaps) == 0


async def peer_message_history_complete(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str | None = None,
    tip_slack_days: int = DEFAULT_HISTORY_TIP_SLACK_DAYS,
) -> bool:
    intervals = await list_peer_message_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    return is_history_complete(intervals, tip_slack_days=tip_slack_days)


def _interval_payload(interval: CoverageInterval) -> dict[str, str | None]:
    return {
        "covered_after": interval.covered_after.isoformat() if interval.covered_after else None,
        "covered_before": interval.covered_before.isoformat(),
    }


def history_coverage_report(
    intervals: list[CoverageInterval],
    *,
    tip_slack_days: int = DEFAULT_HISTORY_TIP_SLACK_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build covered / gap islands for the history-completeness envelope.

    Envelope is ``[−∞, now − tip_slack)`` — same rule as :func:`is_history_complete`.
    """
    end = now or datetime.now(UTC)
    tip = end - timedelta(days=max(0, tip_slack_days))
    merged = merge_intervals(intervals) if intervals else []
    covered, gaps = partition_envelope(merged, window_after=None, window_before=tip)
    return {
        "complete": len(gaps) == 0 and bool(merged),
        "tip_slack_days": tip_slack_days,
        "envelope_before": tip.isoformat(),
        "covered": [_interval_payload(i) for i in covered],
        "gaps": [_interval_payload(i) for i in gaps],
    }


async def peer_history_coverage_report(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str | None = None,
    tip_slack_days: int = DEFAULT_HISTORY_TIP_SLACK_DAYS,
) -> dict[str, Any]:
    intervals = await list_peer_message_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    return history_coverage_report(intervals, tip_slack_days=tip_slack_days)


async def peer_latest_coverage_before(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str | None = None,
) -> datetime | None:
    intervals = await list_peer_message_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    return latest_coverage_before(intervals)


async def gated_total_count(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str | None,
    tip_slack_days: int = DEFAULT_HISTORY_TIP_SLACK_DAYS,
) -> int | None:
    """Return merged fetch-coverage ``message_count`` when history is complete.

    Uses pre-aggregated per-window counts maintained by harvesters (same total as
    the coverage timeline). Overlapping harvester rows are de-duped.
    Requires ``peer_type`` so coverage rows are scoped correctly.
    """
    complete = await peer_message_history_complete(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
        tip_slack_days=tip_slack_days,
    )
    if not complete or peer_type not in ("channel", "megagroup", "chat"):
        return None
    return await peer_coverage_message_count(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,  # type: ignore[arg-type]
    )


async def peer_message_inventory_count(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str,
) -> int:
    """Merged fetch-coverage ``message_count`` for chat inventory badges (ungated).

    Overlapping harvester islands are de-duped (max); adjacent windows sum.
    Same source as media inventory badges: whatever islands we have, even when
    history is incomplete. Pager / insights ``total_count`` stays gated via
    :func:`gated_total_count`.
    """
    if peer_type not in ("channel", "megagroup", "chat"):
        return 0
    return await peer_coverage_message_count(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,  # type: ignore[arg-type]
    )


async def peer_media_inventory_counts(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str,
) -> dict[str, int]:
    """Merged denormalized **message-fetch** coverage media-kind counts (cheap).

    Counts come from ``telegram_message_fetch_coverage`` (message ``media`` JSON
    at fetch time), not media-download / S3 harvest. Overlapping harvester
    islands take per-kind max; adjacent windows sum. Returns keys ``image``,
    ``gif``, ``video``, ``audio``, ``document``, ``webpage``, plus ``all``.

    May be partial until fetch history is complete. When denormalized media
    columns are still zero after a migration (pre-backfill) but history is
    complete, falls back to a single-pass message COUNT so badges stay honest.
    """
    from db.crud.telegram.coverage import list_peer_coverage
    from utils.coverage_intervals import fetch_media_counts_total

    empty = {
        "image": 0,
        "gif": 0,
        "video": 0,
        "audio": 0,
        "document": 0,
        "webpage": 0,
        "url": 0,
        "mention": 0,
        "all": 0,
    }
    if peer_type not in ("channel", "megagroup", "chat"):
        return empty

    intervals = await list_peer_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,  # type: ignore[arg-type]
    )
    raw = fetch_media_counts_total(intervals)
    out = {
        "image": int(raw["count_image"]),
        "gif": int(raw["count_gif"]),
        "video": int(raw["count_video"]),
        "audio": int(raw["count_audio"]),
        "document": int(raw["count_document"]),
        "webpage": int(raw["count_webpage"]),
        "url": int(raw["count_url"]),
        "mention": int(raw["count_mention"]),
    }
    # ``all`` stays downloadable/preview media kinds (excludes url/mention entity flags).
    out["all"] = sum(out[k] for k in ("image", "gif", "video", "audio", "document", "webpage"))

    # Pre-backfill rows have message_count but zero media columns.
    if out["all"] == 0 and any(i.message_count > 0 for i in intervals):
        complete = await peer_message_history_complete(
            conn,
            peer_external_id=peer_external_id,
            peer_type=peer_type,
        )
        if complete:
            from services.api.src.crud.supabase.messages import count_peer_media_messages

            for kind in ("image", "gif", "video", "audio", "document", "webpage"):
                out[kind] = await count_peer_media_messages(
                    conn,
                    peer_external_id,
                    kind=kind,  # type: ignore[arg-type]
                )
            out["all"] = sum(out[k] for k in ("image", "gif", "video", "audio", "document", "webpage"))

    return out


async def gated_recent_volume_counts(
    conn: Any,
    *,
    peer_external_id: str,
    peer_type: str | None,
    messages_7d: int,
    messages_30d: int,
) -> tuple[int | None, int | None]:
    """Gate 7d / 30d volume counts on calendar-day tip freshness + window coverage.

    Returns ``None`` when the tip is on a prior UTC day (show loaders) or when
    the window behind the tip has gaps (never synced that far back).
    """
    intervals = await list_peer_message_coverage(
        conn,
        peer_external_id=peer_external_id,
        peer_type=peer_type,
    )
    now = datetime.now(UTC)
    out_7d = messages_7d if is_volume_window_displayable(intervals, days=7, now=now) else None
    out_30d = messages_30d if is_volume_window_displayable(intervals, days=30, now=now) else None
    return out_7d, out_30d
