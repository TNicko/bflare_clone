"""Tests for message-history completeness gating."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from utils.coverage_intervals import CoverageInterval, FetchCoverageInterval, merge_fetch_intervals

from services.api.src.message_history_coverage import (
    is_history_complete,
    is_recent_window_complete,
)


TIP = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)
START = TIP - timedelta(days=100)


def test_is_history_complete_requires_no_gaps_before_tip_slack() -> None:
    complete = [
        CoverageInterval(covered_after=START, covered_before=TIP - timedelta(days=31)),
        CoverageInterval(covered_after=TIP - timedelta(days=20), covered_before=TIP),
    ]
    assert is_history_complete(complete, now=TIP) is False

    contiguous = [CoverageInterval(covered_after=None, covered_before=TIP)]
    assert is_history_complete(contiguous, now=TIP) is True


def test_recent_window_complete_ignores_older_gaps_and_a_fresh_tip() -> None:
    # Last week missing, older history continuous through now−30d: recent window is not done,
    # but history completeness (which stops at now−30d) still holds.
    week_hole = [CoverageInterval(covered_after=None, covered_before=TIP - timedelta(days=7))]
    assert is_recent_window_complete(week_hole, now=TIP) is False
    assert is_history_complete(week_hole, now=TIP) is True

    covered = [CoverageInterval(covered_after=None, covered_before=TIP)]
    assert is_recent_window_complete(covered, now=TIP) is True

    # Interior hole inside the last 30 days.
    split = [
        CoverageInterval(covered_after=None, covered_before=TIP - timedelta(days=10)),
        CoverageInterval(covered_after=TIP - timedelta(days=3), covered_before=TIP),
    ]
    assert is_recent_window_complete(split, now=TIP) is False

    # A few seconds of tip lag is the live poll's job, not another 30-day walk.
    fresh = [CoverageInterval(covered_after=None, covered_before=TIP - timedelta(seconds=30))]
    assert is_recent_window_complete(fresh, now=TIP) is True

    assert is_recent_window_complete([], now=TIP) is False


def test_peer_coverage_message_count_matches_merged_fetch_total() -> None:
    intervals = merge_fetch_intervals(
        [
            FetchCoverageInterval(covered_after=START, covered_before=TIP - timedelta(days=10), message_count=40),
            FetchCoverageInterval(covered_after=TIP - timedelta(days=10), covered_before=TIP, message_count=60),
        ]
    )
    assert sum(i.message_count for i in intervals) == 100
