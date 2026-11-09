"""Tests for fetch coverage interval helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from utils.coverage_intervals import (
    FetchCoverageInterval,
    fetch_media_counts_total,
    fetch_message_count_total,
    merge_fetch_intervals,
)

TIP = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)
START = TIP - timedelta(days=10)
MID = TIP - timedelta(days=5)


def test_merge_fetch_intervals_sums_message_count() -> None:
    merged = merge_fetch_intervals(
        [
            FetchCoverageInterval(
                covered_after=START,
                covered_before=MID,
                message_count=12,
                count_image=3,
                count_gif=1,
            ),
            FetchCoverageInterval(
                covered_after=MID,
                covered_before=TIP,
                message_count=8,
                count_image=2,
                count_video=4,
            ),
        ]
    )
    assert len(merged) == 1
    assert merged[0].message_count == 20
    assert merged[0].count_image == 5
    assert merged[0].count_gif == 1
    assert merged[0].count_video == 4


def test_merge_fetch_intervals_keeps_disjoint_islands() -> None:
    gap_end = START + timedelta(days=2)
    gap_start = gap_end + timedelta(days=1)
    merged = merge_fetch_intervals(
        [
            FetchCoverageInterval(covered_after=START, covered_before=gap_end, message_count=3),
            FetchCoverageInterval(covered_after=gap_start, covered_before=TIP, message_count=7),
        ]
    )
    assert len(merged) == 2
    assert merged[0].message_count == 3
    assert merged[1].message_count == 7


def test_merge_fetch_intervals_overlapping_harvesters_take_max() -> None:
    """Multiple harvesters covering the same peer must not double-count inventory."""
    older_tip = TIP - timedelta(days=60)
    nested = older_tip - timedelta(days=20)
    intervals = [
        FetchCoverageInterval(
            covered_after=None,
            covered_before=TIP,
            message_count=483,
            count_image=5,
            count_video=4,
            count_gif=3,
            count_webpage=9,
        ),
        FetchCoverageInterval(
            covered_after=None,
            covered_before=older_tip,
            message_count=477,
            count_image=105,
            count_video=45,
            count_gif=15,
            count_webpage=97,
        ),
        FetchCoverageInterval(
            covered_after=nested,
            covered_before=older_tip - timedelta(days=5),
            message_count=12,
            count_image=3,
            count_gif=2,
            count_webpage=2,
        ),
    ]
    assert fetch_message_count_total(intervals) == 483
    media = fetch_media_counts_total(intervals)
    assert media["count_image"] == 105
    assert media["count_video"] == 45
    assert media["count_gif"] == 15
    assert media["count_webpage"] == 97
    assert media["count_url"] == 0
    assert media["count_mention"] == 0
    # Naive sum across raw rows (previous bug) would be 972 / 113 images.
    assert sum(i.message_count for i in intervals) == 972


def test_merge_fetch_intervals_maxes_url_and_mention_on_overlap() -> None:
    merged = merge_fetch_intervals(
        [
            FetchCoverageInterval(
                covered_after=None,
                covered_before=TIP,
                message_count=100,
                count_url=40,
                count_mention=10,
            ),
            FetchCoverageInterval(
                covered_after=None,
                covered_before=MID,
                message_count=80,
                count_url=55,
                count_mention=3,
            ),
        ]
    )
    assert len(merged) == 1
    assert merged[0].count_url == 55
    assert merged[0].count_mention == 10
