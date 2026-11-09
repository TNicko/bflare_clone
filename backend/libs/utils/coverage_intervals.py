"""Date-interval helpers for message fetch coverage.

Used by ``fetch_messages`` for envelope vs
coverage gap detection. Not DB row schemas — pure interval algebra.

Intervals are half-open ``[covered_after, covered_before)``: start inclusive, end
exclusive. ``covered_after=None`` means −∞ (from the beginning of time).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import overload

from pydantic import BaseModel, ConfigDict


class CoverageInterval(BaseModel):
    """One contiguous date range (fetch, download, or embed coverage)."""

    model_config = ConfigDict(frozen=True)

    covered_after: datetime | None
    covered_before: datetime


class FetchCoverageInterval(BaseModel):
    """Message-fetch coverage interval with message + per-kind media inventory.

    Media kind counts are from message ``media`` JSON at fetch time (not S3
    harvest). They should match UI media filters once history is covered.
    """

    model_config = ConfigDict(frozen=True)

    covered_after: datetime | None
    covered_before: datetime
    message_count: int = 0
    count_image: int = 0
    count_gif: int = 0
    count_video: int = 0
    count_audio: int = 0
    count_document: int = 0
    count_webpage: int = 0
    #: Messages with ``has_url`` (URL/TextUrl entity and/or webpage media).
    count_url: int = 0
    #: Messages with ``has_mention`` (@username / MentionName entity).
    count_mention: int = 0

    def as_coverage_interval(self) -> CoverageInterval:
        return CoverageInterval(
            covered_after=self.covered_after,
            covered_before=self.covered_before,
        )

    def media_counts(self) -> dict[str, int]:
        return {
            "count_image": self.count_image,
            "count_gif": self.count_gif,
            "count_video": self.count_video,
            "count_audio": self.count_audio,
            "count_document": self.count_document,
            "count_webpage": self.count_webpage,
            "count_url": self.count_url,
            "count_mention": self.count_mention,
        }


class MediaCoverageInterval(BaseModel):
    """Download coverage interval with per-kind attachment counts."""

    model_config = ConfigDict(frozen=True)

    covered_after: datetime | None
    covered_before: datetime
    count_image: int = 0
    count_gif: int = 0
    count_video: int = 0
    count_audio: int = 0
    count_document: int = 0
    count_webpage: int = 0
    #: True when every embeddable image in the island is synced to Milvus,
    #: or when ``count_image == 0`` (vacuously done).
    phashes_in_milvus: bool = False
    #: True when this island was completed with video downloads skipped
    #: (``--exclude-video``). A later full run treats these as video-only work.
    videos_excluded: bool = False
    #: True when this island was claimed while oversized (≥ auto-harvest threshold)
    #: media was left undownloaded (on-demand). A later download run fills these.
    large_excluded: bool = False
    #: When False, island contributes to inventory counts only and is ignored by
    #: gap planning (so mid-download / partial album harvests do not seal dates).
    seals_gap: bool = True

    def as_coverage_interval(self) -> CoverageInterval:
        return CoverageInterval(
            covered_after=self.covered_after,
            covered_before=self.covered_before,
        )


def default_phashes_in_milvus(*, count_image: int, milvus_synced: bool = False) -> bool:
    """Flag for a new/rewritten island: true if no images, or Milvus sync verified."""
    if count_image <= 0:
        return True
    return milvus_synced


def empty_media_counts() -> dict[str, int]:
    return {
        "count_image": 0,
        "count_gif": 0,
        "count_video": 0,
        "count_audio": 0,
        "count_document": 0,
        "count_webpage": 0,
        "count_url": 0,
        "count_mention": 0,
    }


def add_media_count(counts: dict[str, int], kind: str) -> None:
    key = f"count_{kind}"
    if key not in counts:
        raise ValueError(f"unknown media kind for coverage count: {kind}")
    counts[key] += 1


def merge_media_intervals(intervals: list[MediaCoverageInterval]) -> list[MediaCoverageInterval]:
    """Merge overlapping/touching media intervals, summing per-kind counts.

    Touching islands with different ``videos_excluded``, ``large_excluded``, or
    ``seals_gap`` stay separate so inventory-only bumps are not absorbed into
    sealed gap windows (and vice versa). Overlapping rewrites use AND for
    exclusion flags so a fill pass (``False``) can clear a prior ``True``.
    """

    def start_key(x: MediaCoverageInterval) -> tuple[int, datetime | None]:
        return (0, None) if x.covered_after is None else (1, x.covered_after)

    def same_merge_flags(a: MediaCoverageInterval, b: MediaCoverageInterval) -> bool:
        return (
            a.videos_excluded == b.videos_excluded
            and a.large_excluded == b.large_excluded
            and a.seals_gap == b.seals_gap
        )

    normalized = [
        MediaCoverageInterval(
            covered_after=ensure_utc(i.covered_after),
            covered_before=ensure_utc(i.covered_before),
            count_image=i.count_image,
            count_gif=i.count_gif,
            count_video=i.count_video,
            count_audio=i.count_audio,
            count_document=i.count_document,
            count_webpage=i.count_webpage,
            phashes_in_milvus=i.phashes_in_milvus,
            videos_excluded=i.videos_excluded,
            large_excluded=i.large_excluded,
            seals_gap=i.seals_gap,
        )
        for i in intervals
        if i.covered_before is not None
    ]
    normalized.sort(key=start_key)

    out: list[MediaCoverageInterval] = []
    for cur in normalized:
        if not out:
            out.append(cur)
            continue
        prev = out[-1]
        prev_start = prev.covered_after
        cur_start = cur.covered_after
        overlaps = cur_start is None or prev_start is None or cur_start < prev.covered_before
        touches = cur_start is not None and cur_start == prev.covered_before
        if (overlaps or touches) and same_merge_flags(prev, cur):
            out[-1] = MediaCoverageInterval(
                covered_after=prev.covered_after,
                covered_before=max(prev.covered_before, cur.covered_before),
                count_image=prev.count_image + cur.count_image,
                count_gif=prev.count_gif + cur.count_gif,
                count_video=prev.count_video + cur.count_video,
                count_audio=prev.count_audio + cur.count_audio,
                count_document=prev.count_document + cur.count_document,
                count_webpage=prev.count_webpage + cur.count_webpage,
                phashes_in_milvus=prev.phashes_in_milvus and cur.phashes_in_milvus,
                videos_excluded=prev.videos_excluded and cur.videos_excluded,
                large_excluded=prev.large_excluded and cur.large_excluded,
                seals_gap=prev.seals_gap,
            )
        else:
            out.append(cur)
    return out


def sealed_media_intervals(intervals: list[MediaCoverageInterval]) -> list[MediaCoverageInterval]:
    """Islands that claim date ranges for download gap planning."""
    return [i for i in intervals if i.seals_gap]


def media_counts_total(intervals: list[MediaCoverageInterval]) -> dict[str, int]:
    """Sum per-kind inventory counts across islands (sealed + unsealed)."""
    totals = empty_media_counts()
    for i in intervals:
        totals["count_image"] += i.count_image
        totals["count_gif"] += i.count_gif
        totals["count_video"] += i.count_video
        totals["count_audio"] += i.count_audio
        totals["count_document"] += i.count_document
        totals["count_webpage"] += i.count_webpage
    return totals


def exclusive_end_after(max_date: datetime) -> datetime:
    """Half-open coverage end strictly after ``max_date`` (1µs)."""
    return ensure_utc(max_date) + timedelta(microseconds=1)


@overload
def ensure_utc(value: datetime) -> datetime: ...


@overload
def ensure_utc(value: None) -> None: ...


def ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize ``value`` to UTC so interval comparisons are timezone-safe.

    Returns ``None`` unchanged. Naive datetimes are treated as UTC.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def merge_fetch_intervals(intervals: list[FetchCoverageInterval]) -> list[FetchCoverageInterval]:
    """Merge overlapping/touching fetch intervals for inventory + timeline display.

    Adjacent (touching) islands cover disjoint half-open ranges, so message and
    media counts are **summed**. Overlapping islands — typical when several
    harvesters each store a full-history row for the same peer — take the
    **per-field max** so the same messages are not double-counted in client
    totals (``message_count``, media badges, coverage timelines).
    """

    def start_key(x: FetchCoverageInterval) -> tuple[int, datetime | None]:
        return (0, None) if x.covered_after is None else (1, x.covered_after)

    normalized = [
        FetchCoverageInterval(
            covered_after=ensure_utc(i.covered_after),
            covered_before=ensure_utc(i.covered_before),
            message_count=i.message_count,
            count_image=i.count_image,
            count_gif=i.count_gif,
            count_video=i.count_video,
            count_audio=i.count_audio,
            count_document=i.count_document,
            count_webpage=i.count_webpage,
            count_url=i.count_url,
            count_mention=i.count_mention,
        )
        for i in intervals
        if i.covered_before is not None
    ]
    normalized.sort(key=start_key)

    out: list[FetchCoverageInterval] = []
    for cur in normalized:
        if not out:
            out.append(cur)
            continue
        prev = out[-1]
        prev_start = prev.covered_after
        cur_start = cur.covered_after
        # Open starts or a strict time overlap → same messages may appear in both.
        overlaps = cur_start is None or prev_start is None or cur_start < prev.covered_before
        touches = cur_start is not None and cur_start == prev.covered_before
        if overlaps:
            out[-1] = FetchCoverageInterval(
                covered_after=prev.covered_after,
                covered_before=max(prev.covered_before, cur.covered_before),
                message_count=max(prev.message_count, cur.message_count),
                count_image=max(prev.count_image, cur.count_image),
                count_gif=max(prev.count_gif, cur.count_gif),
                count_video=max(prev.count_video, cur.count_video),
                count_audio=max(prev.count_audio, cur.count_audio),
                count_document=max(prev.count_document, cur.count_document),
                count_webpage=max(prev.count_webpage, cur.count_webpage),
                count_url=max(prev.count_url, cur.count_url),
                count_mention=max(prev.count_mention, cur.count_mention),
            )
        elif touches:
            out[-1] = FetchCoverageInterval(
                covered_after=prev.covered_after,
                covered_before=max(prev.covered_before, cur.covered_before),
                message_count=prev.message_count + cur.message_count,
                count_image=prev.count_image + cur.count_image,
                count_gif=prev.count_gif + cur.count_gif,
                count_video=prev.count_video + cur.count_video,
                count_audio=prev.count_audio + cur.count_audio,
                count_document=prev.count_document + cur.count_document,
                count_webpage=prev.count_webpage + cur.count_webpage,
                count_url=prev.count_url + cur.count_url,
                count_mention=prev.count_mention + cur.count_mention,
            )
        else:
            out.append(cur)
    return out


def fetch_message_count_total(intervals: list[FetchCoverageInterval]) -> int:
    """Total messages after merge (sum of island counts; overlaps already de-duped)."""
    return sum(i.message_count for i in merge_fetch_intervals(intervals))


def fetch_media_counts_total(intervals: list[FetchCoverageInterval]) -> dict[str, int]:
    """Per-kind media inventory after merge (overlaps already de-duped via max)."""
    totals = empty_media_counts()
    for i in merge_fetch_intervals(intervals):
        totals["count_image"] += i.count_image
        totals["count_gif"] += i.count_gif
        totals["count_video"] += i.count_video
        totals["count_audio"] += i.count_audio
        totals["count_document"] += i.count_document
        totals["count_webpage"] += i.count_webpage
        totals["count_url"] += i.count_url
        totals["count_mention"] += i.count_mention
    return totals


def fetch_interval_with_media_counts(
    *,
    covered_after: datetime | None,
    covered_before: datetime,
    message_count: int,
    media_counts: dict[str, int] | None = None,
) -> FetchCoverageInterval:
    """Build a fetch island with optional media-kind inventory."""
    counts = media_counts or empty_media_counts()
    return FetchCoverageInterval(
        covered_after=covered_after,
        covered_before=covered_before,
        message_count=int(message_count or 0),
        count_image=int(counts.get("count_image") or 0),
        count_gif=int(counts.get("count_gif") or 0),
        count_video=int(counts.get("count_video") or 0),
        count_audio=int(counts.get("count_audio") or 0),
        count_document=int(counts.get("count_document") or 0),
        count_webpage=int(counts.get("count_webpage") or 0),
        count_url=int(counts.get("count_url") or 0),
        count_mention=int(counts.get("count_mention") or 0),
    )


def merge_intervals(intervals: list[CoverageInterval]) -> list[CoverageInterval]:
    """Collapse overlapping or touching intervals into a minimal sorted list.

    Input may be messy (multiple DB rows, partial overlaps). Output is sorted by
    start and has no redundant intervals. Rows without ``covered_before`` are dropped.
    """

    def start_key(x: CoverageInterval) -> tuple[int, datetime | None]:
        # Sort NULL starts first (−∞), then ascending finite starts
        return (0, None) if x.covered_after is None else (1, x.covered_after)

    # 1. Normalize bounds and drop invalid intervals
    normalized = [
        CoverageInterval(covered_after=ensure_utc(i.covered_after), covered_before=ensure_utc(i.covered_before))
        for i in intervals
        if i.covered_before is not None
    ]
    # 2. Process in chronological order
    normalized.sort(key=start_key)

    out: list[CoverageInterval] = []
    for cur in normalized:
        if not out:
            out.append(cur)
            continue

        prev = out[-1]
        prev_start = prev.covered_after
        cur_start = cur.covered_after

        # 3. Merge if cur starts at or before prev ends (overlap or adjacency)
        if cur_start is None or prev_start is None or cur_start <= prev.covered_before:
            out[-1] = CoverageInterval(
                covered_after=prev.covered_after,
                covered_before=max(prev.covered_before, cur.covered_before),
            )
        else:
            # Disjoint — keep as a separate interval
            out.append(cur)
    return out


def clip_interval(
    interval: CoverageInterval,
    *,
    window_after: datetime | None,
    window_before: datetime | None,
) -> CoverageInterval | None:
    """Return the intersection of ``interval`` with ``[window_after, window_before)``.

    ``None`` window bounds are open on that side (−∞ for ``window_after``,
    +∞ for ``window_before``). Returns ``None`` when the intersection is empty.
    """
    a = ensure_utc(interval.covered_after)
    b = ensure_utc(interval.covered_before)
    if b is None:
        return None

    wa = ensure_utc(window_after)
    wb = ensure_utc(window_before)

    # Intersection start = max(interval start, window_after)
    start = a
    if wa is not None:
        if start is None or wa > start:
            start = wa

    # Intersection end = min(interval end, window_before)
    end = b if wb is None or wb >= b else wb

    # Empty intersection
    if start is not None and start >= end:
        return None
    return CoverageInterval(covered_after=start, covered_before=end)


def _gaps_in_slice(
    covered_in_slice: list[CoverageInterval],
    *,
    slice_after: datetime | None,
    slice_before: datetime | None,
) -> list[CoverageInterval]:
    """Sub-ranges of ``[slice_after, slice_before)`` not covered by ``covered_in_slice``."""
    sa = ensure_utc(slice_after)
    sb = ensure_utc(slice_before)

    gaps: list[CoverageInterval] = []

    def lt(a: datetime | None, b: datetime | None) -> bool:
        """``a < b`` with ``None`` as −∞ on the left only."""
        if b is None:
            return a is not None
        if a is None:
            return True
        return a < b

    cursor = sa
    for iv in covered_in_slice:
        gap_end = iv.covered_after
        if gap_end is not None and lt(cursor, gap_end):
            gaps.append(CoverageInterval(covered_after=cursor, covered_before=gap_end))
        cursor = iv.covered_before

    if sb is None:
        if cursor is not None:
            gaps.append(CoverageInterval(covered_after=cursor, covered_before=datetime.now(UTC)))
        elif sa is None:
            gaps.append(CoverageInterval(covered_after=None, covered_before=datetime.now(UTC)))
    elif cursor is None:
        gaps.append(CoverageInterval(covered_after=sa, covered_before=sb))
    elif cursor < sb:
        gaps.append(CoverageInterval(covered_after=cursor, covered_before=sb))
    return gaps


def _partition_slice(
    coverage: list[CoverageInterval],
    *,
    slice_after: datetime | None,
    slice_before: datetime | None,
) -> tuple[list[CoverageInterval], list[CoverageInterval]]:
    clipped = [
        part
        for i in coverage
        if (part := clip_interval(i, window_after=slice_after, window_before=slice_before)) is not None
    ]
    covered = merge_intervals(clipped)
    gaps = _gaps_in_slice(covered, slice_after=slice_after, slice_before=slice_before)
    return covered, gaps


def partition_envelope(
    coverage: list[CoverageInterval],
    *,
    envelope: list[CoverageInterval] | None = None,
    window_after: datetime | None = None,
    window_before: datetime | None = None,
) -> tuple[list[CoverageInterval], list[CoverageInterval]]:
    """Compare ``coverage`` to an envelope and return ``(covered, gaps)``.

    ``covered`` is ``coverage`` clipped to the envelope, merged. ``gaps`` are
    sub-ranges of the envelope with no coverage.

    Pass either:

    - ``envelope``: one or more half-open intervals (e.g. fetch-processable
      windows when diffing against image-media coverage), or
    - ``window_after`` / ``window_before``: a single slice; bounds may be open
      (``None`` = −∞ / +∞; used for config vs fetch coverage).

    When both window bounds are ``None``, the envelope is the open-ended
    range ``[−∞, now)`` (fetch-all / full-history semantics).
    """
    if envelope is not None:
        covered_parts: list[CoverageInterval] = []
        gap_parts: list[CoverageInterval] = []
        for iv in envelope:
            slice_covered, slice_gaps = _partition_slice(
                coverage,
                slice_after=iv.covered_after,
                slice_before=iv.covered_before,
            )
            covered_parts.extend(slice_covered)
            gap_parts.extend(slice_gaps)
        return merge_intervals(covered_parts), merge_intervals(gap_parts)

    wa = ensure_utc(window_after)
    wb = ensure_utc(window_before)

    return _partition_slice(coverage, slice_after=wa, slice_before=wb)


def intersect_interval_lists(
    left: list[CoverageInterval],
    right: list[CoverageInterval],
) -> list[CoverageInterval]:
    """Return merged intervals where ``left`` and ``right`` overlap."""
    parts: list[CoverageInterval] = []
    for left_iv in left:
        for right_iv in right:
            clipped = clip_interval(
                left_iv,
                window_after=right_iv.covered_after,
                window_before=right_iv.covered_before,
            )
            if clipped is not None:
                parts.append(clipped)
    return merge_intervals(parts)
