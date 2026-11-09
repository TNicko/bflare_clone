"""Tests for peer-scoped media coverage consolidation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from utils.coverage_intervals import MediaCoverageInterval, merge_media_intervals

TIP = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)
START = TIP - timedelta(days=100)


def test_merge_media_intervals_across_harvester_style_rows() -> None:
    """Simulates two harvesters' islands for the same peer before consolidation."""
    merged = merge_media_intervals(
        [
            MediaCoverageInterval(
                covered_after=None,
                covered_before=TIP - timedelta(days=9),
                count_image=100,
                videos_excluded=True,
                large_excluded=True,
                seals_gap=True,
            ),
            MediaCoverageInterval(
                covered_after=TIP - timedelta(days=9),
                covered_before=TIP,
                count_image=10,
                count_video=2,
                seals_gap=True,
            ),
        ]
    )
    assert len(merged) == 2
    assert merged[0].count_image == 100
    assert merged[1].count_video == 2
