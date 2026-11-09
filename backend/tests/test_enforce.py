"""Plan feature, quota, and rate-limit decisions."""

from __future__ import annotations

from uuid import uuid4

from billing.catalog import PLAN_FREE, PLAN_INTERNAL, PLAN_PRO, features_for, included_units_for
from billing.enforce import decide_meter, should_enforce
from billing.rate_limit import clear_rate_limits, rate_limit_retry_after
from usage.meters import Meter


def test_search_buckets_are_enforced() -> None:
    assert should_enforce(Meter.SEARCH_SEMANTIC) is True
    assert should_enforce(Meter.SEARCH_NAME) is True
    assert should_enforce(Meter.TELEGRAM_SYNC) is True


def test_free_plan_allows_semantic() -> None:
    decision = decide_meter(
        meter=Meter.SEARCH_SEMANTIC,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={},
        period_event_count=0,
    )
    assert decision.allowed is True


def test_free_plan_blocks_live_telegram_name() -> None:
    decision = decide_meter(
        meter=Meter.SEARCH_NAME_TELEGRAM,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={},
        period_event_count=0,
    )
    assert decision.allowed is False
    assert decision.code == "feature_not_in_plan"
    assert decision.http_status == 403


def test_free_plan_blocks_media_download() -> None:
    decision = decide_meter(
        meter=Meter.TELEGRAM_MEDIA_DOWNLOAD,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={},
        period_event_count=0,
    )
    assert decision.allowed is False
    assert decision.code == "feature_not_in_plan"


def test_internal_plan_allows_expensive_meters() -> None:
    decision = decide_meter(
        meter=Meter.TELEGRAM_RESOLVE,
        plan_id=PLAN_INTERNAL,
        source="override",
        features=features_for(PLAN_INTERNAL),
        included_units={},
        period_event_count=0,
    )
    assert decision.allowed is True


def test_daily_telegram_search_quota_returns_402() -> None:
    decision = decide_meter(
        meter=Meter.SEARCH_KEYWORD,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={str(Meter.SEARCH_KEYWORD): 20},
        period_event_count=20,
    )
    assert decision.allowed is False
    assert decision.code == "quota_exceeded"
    assert decision.http_status == 402
    assert "daily" in (decision.detail or "")


def test_free_included_units_are_daily_buckets() -> None:
    units = included_units_for(PLAN_FREE)
    assert units["search.telegram"] == 20
    assert units["search.image_all"] == 5
    assert units["search.bluesky"] == 50
    assert included_units_for(PLAN_PRO) == {}


def test_daily_image_quota_returns_402() -> None:
    decision = decide_meter(
        meter=Meter.SEARCH_IMAGE,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={str(Meter.SEARCH_IMAGE): 5},
        period_event_count=5,
    )
    assert decision.allowed is False
    assert decision.code == "quota_exceeded"
    assert decision.http_status == 402


def test_daily_bluesky_quota_allows_until_50() -> None:
    decision = decide_meter(
        meter=Meter.SEARCH_BLUESKY,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={str(Meter.SEARCH_BLUESKY): 50},
        period_event_count=49,
    )
    assert decision.allowed is True


def test_free_semantic_allowed_until_telegram_bucket() -> None:
    decision = decide_meter(
        meter=Meter.SEARCH_SEMANTIC,
        plan_id=PLAN_FREE,
        source="free",
        features=features_for(PLAN_FREE),
        included_units={str(Meter.SEARCH_SEMANTIC): 20},
        period_event_count=19,
    )
    assert decision.allowed is True


def test_rate_limit_blocks_after_rpm() -> None:
    clear_rate_limits()
    user_id = uuid4()
    now = 1_000.0
    for _ in range(3):
        assert (
            rate_limit_retry_after(
                user_id=user_id,
                meter=str(Meter.TELEGRAM_SYNC),
                telegram_rpm=3,
                embedding_rpm=10,
                now=now,
            )
            is None
        )
        now += 0.01
    retry = rate_limit_retry_after(
        user_id=user_id,
        meter=str(Meter.TELEGRAM_SYNC),
        telegram_rpm=3,
        embedding_rpm=10,
        now=now,
    )
    assert retry is not None
    assert retry >= 1
