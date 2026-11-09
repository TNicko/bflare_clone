from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, TypeVar

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ForbiddenError, PeerIdInvalidError, RPCError, UserPrivacyRestrictedError
from telethon.tl.types import InputPeerUser, TypeInputPeer

from aws.s3 import s3_configured
from db.crud.telegram.user import update_user_stories_and_gifts
from telegram.api import get_pinned_stories, get_saved_star_gifts
from telegram.availability import is_user_unavailable_error
from telegram.profile_media import (
    harvest_pinned_stories_payload,
    harvest_saved_star_gifts_payload,
    pinned_stories_need_harvest,
    saved_star_gifts_need_harvest,
)
from telegram.utils import json_safe_value, from_telethon

lg = logging.getLogger(__name__)

_T = TypeVar("_T")

_DEFINITIVE_ERRORS: tuple[type[RPCError], ...] = (
    UserPrivacyRestrictedError,
    ForbiddenError,
    PeerIdInvalidError,
)

_PeerLookup = TypeInputPeer | int | str


@dataclass(slots=True)
class StoriesAndGiftsFetchResult:
    pinned_stories_fetch_pending: bool = False
    saved_star_gifts_fetch_pending: bool = False
    pinned_stories_fetch_warnings: list[str] = field(default_factory=list)
    saved_star_gifts_fetch_warnings: list[str] = field(default_factory=list)


def _peer_candidates(
    *,
    external_id: str,
    access_hash: int | None,
    username: str | None,
) -> list[tuple[str, _PeerLookup]]:
    """Ordered peer lookups: stored access_hash first, then @username.

    Stored hashes can be stale for the current harvester session; username
    resolve refreshes the peer and avoids PeerIdInvalidError on gifts/stories.
    """
    user_id = int(external_id)
    handle = (username or "").strip().lstrip("@")
    candidates: list[tuple[str, _PeerLookup]] = []
    if access_hash is not None:
        candidates.append(("access_hash", InputPeerUser(user_id, access_hash)))
    if handle:
        candidates.append(("username", handle))
    if not candidates:
        candidates.append(("user_id", user_id))
    return candidates


def _can_retry_with_next_peer(exc: BaseException) -> bool:
    if isinstance(exc, PeerIdInvalidError):
        return True
    if isinstance(exc, KeyError):
        # Telethon get_entity raises KeyError(user_id) when GetUsers returns empty.
        return True
    if is_user_unavailable_error(exc):
        return True
    if isinstance(exc, ValueError):
        msg = str(exc).lower()
        return "could not find the input entity" in msg or "cannot find any entity corresponding to" in msg
    return False


async def _with_peer_fallback(
    *,
    external_id: str,
    access_hash: int | None,
    username: str | None,
    call: Callable[[_PeerLookup], Awaitable[_T]],
) -> _T:
    candidates = _peer_candidates(
        external_id=external_id,
        access_hash=access_hash,
        username=username,
    )
    last_exc: BaseException | None = None
    for index, (label, peer) in enumerate(candidates):
        try:
            return await call(peer)
        except FloodWaitError:
            raise
        except Exception as exc:
            last_exc = exc
            if index + 1 < len(candidates) and _can_retry_with_next_peer(exc):
                lg.warning(
                    "Stories/gifts peer lookup %s failed for user external_id=%s: %s; trying %s",
                    label,
                    external_id,
                    exc,
                    candidates[index + 1][0],
                )
                continue
            raise
    assert last_exc is not None
    raise last_exc


def _is_definitive_denial(exc: BaseException) -> bool:
    return isinstance(exc, _DEFINITIVE_ERRORS) or is_user_unavailable_error(exc)


def _now() -> datetime:
    return datetime.now(UTC)


async def _persist_pinned_stories(
    conn: Any,
    *,
    external_id: str,
    data: dict[str, Any] | None,
    fetched_at: datetime,
) -> None:
    await update_user_stories_and_gifts(
        conn,
        external_id,
        pinned_stories=data,
        pinned_stories_fetched_at=fetched_at,
    )


async def _persist_saved_star_gifts(
    conn: Any,
    *,
    external_id: str,
    data: dict[str, Any] | None,
    fetched_at: datetime,
) -> None:
    await update_user_stories_and_gifts(
        conn,
        external_id,
        saved_star_gifts=data,
        star_gifts_fetched_at=fetched_at,
    )


async def fetch_pinned_stories_if_needed(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    access_hash: int | None,
    username: str | None,
    pinned_stories_fetched_at: datetime | None,
    existing_pinned_stories: dict[str, Any] | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> StoriesAndGiftsFetchResult:
    result = StoriesAndGiftsFetchResult()

    harvest_only = pinned_stories_fetched_at is not None
    if harvest_only:
        if not pinned_stories_need_harvest(existing_pinned_stories):
            lg.debug(
                "Skipping pinned stories fetch for user external_id=%s (already fetched at %s)",
                external_id,
                pinned_stories_fetched_at,
            )
            return result
        lg.info(
            "Harvesting pinned story media for user external_id=%s (fetched at %s)",
            external_id,
            pinned_stories_fetched_at,
        )
    else:
        lg.info(
            "Fetching pinned stories for user external_id=%s username=%r",
            external_id,
            username,
        )

    try:

        async def _fetch(peer: _PeerLookup):
            return await get_pinned_stories(client, peer)

        stories = await _with_peer_fallback(
            external_id=external_id,
            access_hash=access_hash,
            username=username,
            call=_fetch,
        )
        story_count = len(stories.stories or [])
        # Prefer already-persisted payload on harvest-only so we keep prior S3
        # harvests and only fill missing media; otherwise take the fresh TL dump.
        if harvest_only and isinstance(existing_pinned_stories, dict):
            payload = dict(existing_pinned_stories)
        else:
            payload = json_safe_value(from_telethon(stories))
            if not isinstance(payload, dict):
                payload = {}

        fetched_at = pinned_stories_fetched_at if harvest_only else _now()
        # Persist the list immediately so the client can render cards (thumbs /
        # placeholders) before S3 harvest finishes.
        await _persist_pinned_stories(
            conn,
            external_id=external_id,
            data=payload if (stories.stories or harvest_only) else None,
            fetched_at=fetched_at,
        )

        more_media = False
        if not harvest_only:
            # First list fetch: return the cards now; media harvest continues on
            # subsequent refresh=stories polls (one item per call).
            if story_count > 0 and pinned_stories_need_harvest(payload):
                if not s3_configured(s3_bucket, s3_region):
                    lg.error(
                        "Pinned story media not harvested for user external_id=%s: S3 not configured",
                        external_id,
                    )
                    result.pinned_stories_fetch_warnings.append("Media could not be loaded.")
                more_media = True
        elif s3_configured(s3_bucket, s3_region) and story_count > 0:
            payload, more_media = await harvest_pinned_stories_payload(
                client,
                stories,
                payload,
                user_external_id=external_id,
                bucket=s3_bucket,
                region=s3_region,
            )
            await _persist_pinned_stories(
                conn,
                external_id=external_id,
                data=payload,
                fetched_at=fetched_at,
            )
        elif story_count > 0 and pinned_stories_need_harvest(payload):
            lg.error(
                "Pinned story media not harvested for user external_id=%s: S3 not configured",
                external_id,
            )
            result.pinned_stories_fetch_warnings.append("Media could not be loaded.")
            more_media = True

        if more_media or pinned_stories_need_harvest(payload):
            result.pinned_stories_fetch_pending = True

        lg.info(
            "Fetched and upserted pinned stories for user external_id=%s: "
            "stories=%d telegram_count=%d chats=%d users=%d harvest_only=%s more_media=%s",
            external_id,
            story_count,
            stories.count,
            len(stories.chats or []),
            len(stories.users or []),
            harvest_only,
            more_media,
        )
    except FloodWaitError as exc:
        lg.error(
            "Pinned stories fetch failed for user external_id=%s: Telegram rate limit; retry in %s seconds",
            external_id,
            exc.seconds,
        )
        result.pinned_stories_fetch_pending = True
        result.pinned_stories_fetch_warnings.append(f"Too many requests. Try again in {exc.seconds} seconds.")
    except Exception as exc:
        if _is_definitive_denial(exc):
            lg.error(
                "Pinned stories fetch denied for user external_id=%s: %s",
                external_id,
                exc,
            )
            await _persist_pinned_stories(
                conn,
                external_id=external_id,
                data=None,
                fetched_at=_now(),
            )
            lg.info(
                "Upserted empty pinned stories for user external_id=%s after access denial",
                external_id,
            )
            result.pinned_stories_fetch_warnings.append("Stories could not be refreshed.")
        else:
            lg.error(
                "Pinned stories fetch failed for user external_id=%s",
                external_id,
                exc_info=True,
            )
            result.pinned_stories_fetch_pending = True
            result.pinned_stories_fetch_warnings.append("Stories could not be refreshed.")

    return result


async def fetch_saved_star_gifts_if_needed(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    access_hash: int | None,
    username: str | None,
    star_gifts_fetched_at: datetime | None,
    existing_saved_star_gifts: dict[str, Any] | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> StoriesAndGiftsFetchResult:
    result = StoriesAndGiftsFetchResult()
    harvest_only = star_gifts_fetched_at is not None
    if harvest_only:
        if not saved_star_gifts_need_harvest(existing_saved_star_gifts):
            lg.debug(
                "Skipping saved star gifts fetch for user external_id=%s (already fetched at %s)",
                external_id,
                star_gifts_fetched_at,
            )
            return result
        lg.info(
            "Harvesting saved star gift media for user external_id=%s (fetched at %s)",
            external_id,
            star_gifts_fetched_at,
        )
    else:
        lg.info(
            "Fetching saved star gifts for user external_id=%s username=%r",
            external_id,
            username,
        )

    try:

        async def _fetch(peer: _PeerLookup):
            return await get_saved_star_gifts(client, peer, exclude_unsaved=True)

        gifts = await _with_peer_fallback(
            external_id=external_id,
            access_hash=access_hash,
            username=username,
            call=_fetch,
        )
        gift_count = len(gifts.gifts or [])
        if harvest_only and isinstance(existing_saved_star_gifts, dict):
            payload = dict(existing_saved_star_gifts)
        else:
            payload = json_safe_value(from_telethon(gifts))
            if not isinstance(payload, dict):
                payload = {}

        fetched_at = star_gifts_fetched_at if harvest_only else _now()
        await _persist_saved_star_gifts(
            conn,
            external_id=external_id,
            data=payload if (gifts.gifts or harvest_only) else None,
            fetched_at=fetched_at,
        )

        more_media = False
        if not harvest_only:
            if gift_count > 0 and saved_star_gifts_need_harvest(payload):
                if not s3_configured(s3_bucket, s3_region):
                    lg.error(
                        "Saved star gift media not harvested for user external_id=%s: S3 not configured",
                        external_id,
                    )
                    result.saved_star_gifts_fetch_warnings.append("Media could not be loaded.")
                more_media = True
        elif s3_configured(s3_bucket, s3_region) and gift_count > 0:
            payload, more_media = await harvest_saved_star_gifts_payload(
                client,
                gifts,
                payload,
                user_external_id=external_id,
                bucket=s3_bucket,
                region=s3_region,
            )
            await _persist_saved_star_gifts(
                conn,
                external_id=external_id,
                data=payload,
                fetched_at=fetched_at,
            )
        elif gift_count > 0 and saved_star_gifts_need_harvest(payload):
            lg.error(
                "Saved star gift media not harvested for user external_id=%s: S3 not configured",
                external_id,
            )
            result.saved_star_gifts_fetch_warnings.append("Media could not be loaded.")
            more_media = True

        if more_media or saved_star_gifts_need_harvest(payload):
            result.saved_star_gifts_fetch_pending = True

        lg.info(
            "Fetched and upserted saved star gifts for user external_id=%s: "
            "gifts=%d telegram_count=%d chats=%d users=%d harvest_only=%s more_media=%s",
            external_id,
            gift_count,
            gifts.count,
            len(gifts.chats or []),
            len(gifts.users or []),
            harvest_only,
            more_media,
        )
    except FloodWaitError as exc:
        lg.error(
            "Saved star gifts fetch failed for user external_id=%s: Telegram rate limit; retry in %s seconds",
            external_id,
            exc.seconds,
        )
        result.saved_star_gifts_fetch_pending = True
        result.saved_star_gifts_fetch_warnings.append(f"Too many requests. Try again in {exc.seconds} seconds.")
    except Exception as exc:
        if _is_definitive_denial(exc):
            lg.error(
                "Saved star gifts fetch denied for user external_id=%s: %s",
                external_id,
                exc,
            )
            await _persist_saved_star_gifts(
                conn,
                external_id=external_id,
                data=None,
                fetched_at=_now(),
            )
            lg.info(
                "Upserted empty saved star gifts for user external_id=%s after access denial",
                external_id,
            )
            result.saved_star_gifts_fetch_warnings.append("Gifts could not be refreshed.")
        else:
            lg.error(
                "Saved star gifts fetch failed for user external_id=%s",
                external_id,
                exc_info=True,
            )
            result.saved_star_gifts_fetch_pending = True
            result.saved_star_gifts_fetch_warnings.append("Gifts could not be refreshed.")

    return result


def _merge_results(
    *parts: StoriesAndGiftsFetchResult,
) -> StoriesAndGiftsFetchResult:
    merged = StoriesAndGiftsFetchResult()
    for part in parts:
        merged.pinned_stories_fetch_pending = merged.pinned_stories_fetch_pending or part.pinned_stories_fetch_pending
        merged.saved_star_gifts_fetch_pending = (
            merged.saved_star_gifts_fetch_pending or part.saved_star_gifts_fetch_pending
        )
        merged.pinned_stories_fetch_warnings.extend(part.pinned_stories_fetch_warnings)
        merged.saved_star_gifts_fetch_warnings.extend(part.saved_star_gifts_fetch_warnings)
    return merged


async def fetch_user_stories_and_gifts_if_needed(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    access_hash: int | None,
    username: str | None,
    bot: bool,
    pinned_stories_fetched_at: datetime | None,
    star_gifts_fetched_at: datetime | None,
    existing_pinned_stories: dict[str, Any] | None = None,
    existing_saved_star_gifts: dict[str, Any] | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> StoriesAndGiftsFetchResult:
    if bot:
        lg.debug(
            "Skipping stories/gifts fetch for bot user external_id=%s",
            external_id,
        )
        return StoriesAndGiftsFetchResult()

    need_stories = pinned_stories_fetched_at is None or pinned_stories_need_harvest(existing_pinned_stories)
    need_gifts = star_gifts_fetched_at is None or saved_star_gifts_need_harvest(existing_saved_star_gifts)
    lg.info(
        "Starting stories/gifts fetch for user external_id=%s username=%r (need_stories=%s need_gifts=%s)",
        external_id,
        username,
        need_stories,
        need_gifts,
    )

    stories_result = await fetch_pinned_stories_if_needed(
        client,
        conn,
        external_id=external_id,
        access_hash=access_hash,
        username=username,
        pinned_stories_fetched_at=pinned_stories_fetched_at,
        existing_pinned_stories=existing_pinned_stories,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    gifts_result = await fetch_saved_star_gifts_if_needed(
        client,
        conn,
        external_id=external_id,
        access_hash=access_hash,
        username=username,
        star_gifts_fetched_at=star_gifts_fetched_at,
        existing_saved_star_gifts=existing_saved_star_gifts,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    merged = _merge_results(stories_result, gifts_result)
    if merged.pinned_stories_fetch_pending or merged.saved_star_gifts_fetch_pending:
        lg.error(
            "Stories/gifts fetch incomplete for user external_id=%s: "
            "stories_pending=%s gifts_pending=%s stories_warnings=%r gifts_warnings=%r",
            external_id,
            merged.pinned_stories_fetch_pending,
            merged.saved_star_gifts_fetch_pending,
            merged.pinned_stories_fetch_warnings,
            merged.saved_star_gifts_fetch_warnings,
        )
    else:
        lg.info(
            "Stories/gifts fetch completed for user external_id=%s",
            external_id,
        )
    return merged
