"""Plan documents stored in ``plans``. Free daily caps live in ``included_units``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from usage.meters import METERS, Meter

PLAN_FREE = "free"
PLAN_PRO = "pro"
PLAN_INTERNAL = "internal"

PLAN_IDS = frozenset({PLAN_FREE, PLAN_PRO, PLAN_INTERNAL})

PAST_DUE_GRACE_DAYS = 7

FREE_CHAT_MESSAGE_WINDOW = 100
FREE_MEDIA_UNLOCKED_COUNT = 48
FREE_PIN_USERS = 5
FREE_PIN_COMMUNITIES = 5
FREE_PIN_MESSAGES = 5
PRO_PIN_USERS = 100
PRO_PIN_COMMUNITIES = 100
PRO_PIN_MESSAGES = 100

# Indexed + semantic Telegram search. Live Telegram name stays Pro-only.
CHEAP_METERS = frozenset(
    {
        str(Meter.SEARCH_NAME),
        str(Meter.SEARCH_USERNAME),
        str(Meter.SEARCH_PHONE),
        str(Meter.SEARCH_KEYWORD),
        str(Meter.SEARCH_SEMANTIC),
        str(Meter.SEARCH_IMAGE),
        str(Meter.SEARCH_IMAGE_HASH),
        str(Meter.SEARCH_IMAGE_HASH_BY_MESSAGE),
        str(Meter.SEARCH_BLUESKY),
        str(Meter.TELEGRAM_SYNC),
    }
)
EXPENSIVE_METERS = METERS - CHEAP_METERS

BUCKET_TELEGRAM_SEARCH = "search.telegram"
BUCKET_IMAGE_SEARCH = "search.image_all"
BUCKET_BLUESKY_SEARCH = "search.bluesky"


@dataclass(frozen=True, slots=True)
class QuotaBucket:
    key: str
    meters: frozenset[str]
    free_included: int


QUOTA_BUCKETS: tuple[QuotaBucket, ...] = (
    QuotaBucket(
        key=BUCKET_TELEGRAM_SEARCH,
        meters=frozenset(
            {
                str(Meter.SEARCH_NAME),
                str(Meter.SEARCH_USERNAME),
                str(Meter.SEARCH_PHONE),
                str(Meter.SEARCH_KEYWORD),
                str(Meter.SEARCH_SEMANTIC),
            }
        ),
        free_included=20,
    ),
    QuotaBucket(
        key=BUCKET_IMAGE_SEARCH,
        meters=frozenset(
            {
                str(Meter.SEARCH_IMAGE),
                str(Meter.SEARCH_IMAGE_HASH),
                str(Meter.SEARCH_IMAGE_HASH_BY_MESSAGE),
            }
        ),
        free_included=5,
    ),
    QuotaBucket(
        key=BUCKET_BLUESKY_SEARCH,
        meters=frozenset({str(Meter.SEARCH_BLUESKY)}),
        free_included=50,
    ),
)

_BUCKET_BY_METER: dict[str, QuotaBucket] = {meter: bucket for bucket in QUOTA_BUCKETS for meter in bucket.meters}


def quota_bucket_for(meter: str) -> QuotaBucket | None:
    return _BUCKET_BY_METER.get(meter)


def features_for(plan_id: str) -> dict[str, bool]:
    if plan_id == PLAN_FREE:
        return {meter: meter in CHEAP_METERS for meter in sorted(METERS)}
    if plan_id in (PLAN_PRO, PLAN_INTERNAL):
        return {meter: True for meter in sorted(METERS)}
    raise ValueError(f"Unknown plan: {plan_id}")


def included_units_for(plan_id: str) -> dict[str, int | None]:
    """Bucket keys on Free. Missing or null means unlimited."""
    if plan_id not in PLAN_IDS:
        raise ValueError(f"Unknown plan: {plan_id}")
    if plan_id == PLAN_FREE:
        return {bucket.key: bucket.free_included for bucket in QUOTA_BUCKETS}
    return {}


def plan_limits_for(plan_id: str) -> dict[str, Any]:
    if plan_id not in PLAN_IDS:
        raise ValueError(f"Unknown plan: {plan_id}")
    if plan_id == PLAN_FREE:
        return {
            "chat_message_window": FREE_CHAT_MESSAGE_WINDOW,
            "media_unlocked_count": FREE_MEDIA_UNLOCKED_COUNT,
            "pins": {
                "users": FREE_PIN_USERS,
                "communities": FREE_PIN_COMMUNITIES,
                "messages": FREE_PIN_MESSAGES,
            },
            "live_updates": False,
            "media_download": False,
        }
    if plan_id == PLAN_PRO:
        return {
            "chat_message_window": None,
            "media_unlocked_count": None,
            "pins": {
                "users": PRO_PIN_USERS,
                "communities": PRO_PIN_COMMUNITIES,
                "messages": PRO_PIN_MESSAGES,
            },
            "live_updates": True,
            "media_download": True,
        }
    return {
        "chat_message_window": None,
        "media_unlocked_count": None,
        "pins": None,
        "live_updates": True,
        "media_download": True,
    }


def plan_row(plan_id: str, name: str) -> dict[str, Any]:
    return {
        "id": plan_id,
        "name": name,
        "included_units": included_units_for(plan_id),
        "features": features_for(plan_id),
        "past_due_grace_days": PAST_DUE_GRACE_DAYS,
    }


PLAN_ROWS = (
    plan_row(PLAN_FREE, "Free"),
    plan_row(PLAN_PRO, "Pro"),
    plan_row(PLAN_INTERNAL, "Internal"),
)
