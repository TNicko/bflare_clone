"""On-demand username resolve for name search (saved DB row, else t.me gate + ResolveUsername)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from telethon import TelegramClient
from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerUser, User

from db.crud.telegram.harvester import get_harvester_by_id
from db.crud.telegram.peers import set_peer_unavailability
from services.api.src.config import ConfigFactory
from services.api.src.client_errors import (
    USERNAME_SEARCH_FAILED,
    USERNAME_SEARCH_UNAVAILABLE,
    rate_limited_retry,
)
from services.api.src.models.search import UsernameSearchResponse
from services.api.src.crud.s3.media import (
    presign_community_view,
    presign_user_partial,
)
from services.api.src.crud.supabase.peers import (
    get_community_view_by_external_id,
    get_community_view_by_username,
    get_partial_telegram_user_by_external_id,
    get_partial_telegram_user_by_username,
)
from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from telegram.tme import TmeClient
from telegram.api_session_pool import ApiSessionPool
from telegram.availability import (
    is_community_unavailable_error,
    is_forbidden_peer_entity,
    is_user_unavailable_error,
)
from telegram.client import HarvesterSessionTerminatedError
from telegram.materialize import materialize_channel, materialize_chat, materialize_user
from telegram.utils import channel_id_from_signed_peer_id, normalize_username, to_signed_peer_id

lg = logging.getLogger(__name__)

# Wait-and-retry once when Telegram asks for a short pause; longer waits become 429.
_FLOOD_WAIT_INLINE_RETRY_MAX_SECONDS = 5

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

OwnershipStatus = Literal["owned", "diverged", "unreachable", "no_access"]


@dataclass(slots=True, frozen=True)
class UsernameResolveResult:
    response: UsernameSearchResponse
    http_error: tuple[int, str] | None = None
    #: True when the peer was already stored and no Telegram call was made.
    from_cache: bool = False


def _username_matches(stored: str | None, expected: str) -> bool:
    if not stored:
        return False
    return stored.strip().lstrip("@").lower() == expected.lower()


def _username_not_found_error(exc: BaseException) -> bool:
    if isinstance(exc, UsernameNotOccupiedError):
        return True
    return isinstance(exc, ValueError) and isinstance(exc.__cause__, UsernameNotOccupiedError)


async def _presign_user(user: UserPartialView) -> UserPartialView:
    region = ConfigFactory.get_config().aws_region.strip()
    if not region:
        return user
    return await presign_user_partial(user, region=region)


async def _presign_community(community: CommunityView) -> CommunityView:
    region = ConfigFactory.get_config().aws_region.strip()
    if not region:
        return community
    return await presign_community_view(community, region=region)


async def _empty(query: str, username: str) -> UsernameResolveResult:
    return UsernameResolveResult(
        response=UsernameSearchResponse(
            query=query,
            username=username,
            found=False,
            user=None,
            community=None,
        )
    )


def _generic_unavailable() -> tuple[int, str]:
    return (503, USERNAME_SEARCH_UNAVAILABLE)


def _rate_limited_result(
    query: str,
    username: str,
    exc: FloodWaitError,
) -> UsernameResolveResult:
    seconds = max(1, int(exc.seconds))
    return UsernameResolveResult(
        response=UsernameSearchResponse(
            query=query,
            username=username,
            found=False,
            user=None,
            community=None,
        ),
        http_error=(429, rate_limited_retry(seconds)),
    )


def _unavailable_result(query: str, username: str) -> UsernameResolveResult:
    return UsernameResolveResult(
        response=UsernameSearchResponse(
            query=query,
            username=username,
            found=False,
            user=None,
            community=None,
        ),
        http_error=_generic_unavailable(),
    )


async def _sleep_if_short_flood_wait(
    exc: FloodWaitError,
    *,
    username: str,
    context: str,
) -> bool:
    """Sleep once when the wait is short. Return True if the caller should retry."""
    wait = max(0, int(exc.seconds))
    if wait > _FLOOD_WAIT_INLINE_RETRY_MAX_SECONDS:
        lg.warning(
            "Username resolve %s for @%s hit rate limit; retry in %s seconds",
            context,
            username,
            wait,
        )
        return False
    lg.warning(
        "Username resolve %s for @%s hit rate limit for %s seconds; waiting once then retrying",
        context,
        username,
        wait,
    )
    if wait > 0:
        await asyncio.sleep(wait)
    return True


async def _found_user(query: str, username: str, user: UserPartialView) -> UsernameResolveResult:
    return UsernameResolveResult(
        response=UsernameSearchResponse(
            query=query,
            username=username,
            found=True,
            user=await _presign_user(user),
            community=None,
        )
    )


async def _found_community(
    query: str,
    username: str,
    community: CommunityView,
) -> UsernameResolveResult:
    return UsernameResolveResult(
        response=UsernameSearchResponse(
            query=query,
            username=username,
            found=True,
            user=None,
            community=await _presign_community(community),
        )
    )


async def _mark_user_unreachable_if_permanent(
    conn: AsyncConnection[DictRow],
    external_id: str,
    exc: BaseException,
) -> None:
    if is_user_unavailable_error(exc):
        await set_peer_unavailability(conn, external_id, peer_type="user", unavailable=True)


async def _mark_community_unreachable_if_permanent(
    conn: AsyncConnection[DictRow],
    external_id: str,
    peer_type: Literal["channel", "megagroup", "chat"],
    exc: BaseException | None = None,
    *,
    forbidden_entity: bool = False,
) -> None:
    if forbidden_entity or (exc is not None and is_community_unavailable_error(exc)):
        await set_peer_unavailability(
            conn,
            external_id,
            peer_type=peer_type,
            unavailable=True,
        )


@asynccontextmanager
async def _get_pool_client(
    pool: ApiSessionPool,
    conn: AsyncConnection[DictRow],
    harvester_id: UUID | None,
    *,
    access_hash: int | None = None,
) -> AsyncIterator[tuple[TelegramClient | None, UUID | None, int | None]]:
    """Return a client safe to use with ``access_hash`` (per-harvester)."""
    async with pool.acquire_client_for_peer_role(
        conn,
        harvester_id=harvester_id,
        access_hash=access_hash,
    ) as result:
        yield result


async def _refresh_user_by_access_hash(
    client: TelegramClient,
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    access_hash: int,
    harvester_id: UUID,
    expected_username: str,
    s3_bucket: str,
    s3_region: str,
) -> OwnershipStatus:
    """Refresh user via InputPeerUser only (does not burn ResolveUsername quota)."""
    try:
        entity = await client.get_entity(InputPeerUser(int(external_id), access_hash))
    except FloodWaitError:
        raise
    except Exception as exc:
        lg.info("Ownership refresh failed for user %s: %s", external_id, exc)
        await _mark_user_unreachable_if_permanent(conn, external_id, exc)
        return "unreachable"
    if not isinstance(entity, User):
        return "unreachable"
    ok = await materialize_user(
        client,
        conn,
        entity,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    if not ok:
        return "unreachable"
    # Ownership from the live Telethon entity — no extra DB round-trip.
    if _username_matches(getattr(entity, "username", None), expected_username):
        return "owned"
    return "diverged"


async def _refresh_community_by_access_hash(
    client: TelegramClient,
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: Literal["channel", "megagroup"],
    access_hash: int,
    harvester_id: UUID,
    expected_username: str,
    s3_bucket: str,
    s3_region: str,
) -> OwnershipStatus:
    try:
        signed = int(external_id)
    except ValueError:
        return "unreachable"
    raw_channel_id = channel_id_from_signed_peer_id(signed)
    if raw_channel_id is None:
        return "unreachable"
    try:
        entity = await client.get_entity(InputPeerChannel(raw_channel_id, access_hash))
    except FloodWaitError:
        raise
    except Exception as exc:
        lg.info(
            "Ownership refresh failed for %s %s: %s",
            peer_type,
            external_id,
            exc,
        )
        await _mark_community_unreachable_if_permanent(conn, external_id, peer_type, exc)
        return "unreachable"
    if is_forbidden_peer_entity(entity):
        await _mark_community_unreachable_if_permanent(
            conn,
            external_id,
            peer_type,
            forbidden_entity=True,
        )
        return "unreachable"
    if not isinstance(entity, Channel):
        return "unreachable"
    full = await materialize_channel(
        client,
        conn,
        entity,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    if full is None:
        return "unreachable"
    if _username_matches(getattr(entity, "username", None), expected_username):
        return "owned"
    return "diverged"


async def _tme_username_occupied(username: str) -> bool | None:
    """
    Return True if t.me looks occupied, False if dead, None if ambiguous (skip Telegram).
    """
    async with TmeClient(
        user_agent=DEFAULT_USER_AGENT,
        timeout_seconds=20.0,
        concurrency=1,
        delay_ms=0,
        max_retries=2,
    ) as tme:
        profile = await tme.fetch_profile(username)
    classification = profile.classification
    if classification == "dead":
        return False
    if classification in {"user", "channel", "megagroup", "bot"}:
        return True
    return None


async def _materialize_entity(
    client: TelegramClient,
    conn: AsyncConnection[DictRow],
    entity: Any,
    *,
    harvester_id: UUID,
    s3_bucket: str,
    s3_region: str,
) -> tuple[UserPartialView | None, CommunityView | None]:
    """Materialize entity; return ``(user, None)`` or ``(None, community)``."""
    if isinstance(entity, User):
        ok = await materialize_user(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        if not ok:
            return None, None
        signed = to_signed_peer_id(entity)
        if signed is None:
            return None, None
        user = await get_partial_telegram_user_by_external_id(conn, str(signed))
        return user, None

    if isinstance(entity, Channel):
        full = await materialize_channel(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        if full is None:
            return None, None
        signed = to_signed_peer_id(entity)
        if signed is None:
            return None, None
        community = await get_community_view_by_external_id(conn, str(signed))
        return None, community

    if isinstance(entity, Chat):
        full = await materialize_chat(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        if full is None:
            return None, None
        signed = to_signed_peer_id(entity)
        if signed is None:
            return None, None
        community = await get_community_view_by_external_id(conn, str(signed))
        return None, community

    return None, None


async def resolve_username_for_search(
    *,
    query: str,
    conn: AsyncConnection[DictRow],
    pool: ApiSessionPool | None,
) -> UsernameResolveResult:
    """
    Resolve a username-shaped search query into a user/community partial.

    Flow: saved DB username (no Telegram) → t.me gate → ResolveUsername.
    """
    username = normalize_username(query)
    if username is None:
        return await _empty(query, query.strip().lstrip("@").strip().lower())

    try:
        return await _resolve_username_once(query=query, username=username, conn=conn, pool=pool)
    except FloodWaitError as exc:
        if await _sleep_if_short_flood_wait(exc, username=username, context="resolve"):
            try:
                return await _resolve_username_once(
                    query=query,
                    username=username,
                    conn=conn,
                    pool=pool,
                )
            except FloodWaitError as retry_exc:
                return _rate_limited_result(query, username, retry_exc)
        return _rate_limited_result(query, username, exc)


async def _resolve_username_once(
    *,
    query: str,
    username: str,
    conn: AsyncConnection[DictRow],
    pool: ApiSessionPool | None,
) -> UsernameResolveResult:
    cfg = ConfigFactory.get_config()
    s3_bucket = cfg.harvest_s3_bucket
    s3_region = cfg.aws_region

    # --- DB exact hit (user preferred; usernames are globally unique) ---
    # A saved username is enough for search. Do not refresh via Telegram on
    # every repeat. The lookup still costs one credit.
    user_hit = await get_partial_telegram_user_by_username(conn, username)
    if user_hit is not None:
        found = await _found_user(query, username, user_hit)
        return UsernameResolveResult(response=found.response, from_cache=True)
    community_hit = await get_community_view_by_username(conn, username)
    if community_hit is not None:
        found = await _found_community(query, username, community_hit)
        return UsernameResolveResult(response=found.response, from_cache=True)

    # --- t.me gate ---
    occupied = await _tme_username_occupied(username)
    if occupied is not True:
        return await _empty(query, username)

    # --- ResolveUsername + materialize ---
    if cfg.telegram_api_id <= 0 or not cfg.telegram_api_hash.strip():
        lg.error("Username resolve unavailable: API credentials not configured")
        return UsernameResolveResult(
            response=UsernameSearchResponse(query=query, username=username, found=False),
            http_error=_generic_unavailable(),
        )
    if pool is None:
        lg.error("Username resolve unavailable: session pool not configured")
        return UsernameResolveResult(
            response=UsernameSearchResponse(query=query, username=username, found=False),
            http_error=_generic_unavailable(),
        )

    try:
        async with pool.acquire_any_client(conn) as (client, harvester_id):
            if client is None or harvester_id is None:
                lg.error("Username resolve unavailable: no session available")
                return UsernameResolveResult(
                    response=UsernameSearchResponse(query=query, username=username, found=False),
                    http_error=_generic_unavailable(),
                )
            harvester = await get_harvester_by_id(conn, harvester_id)
            if harvester is None:
                lg.error("Username resolve unavailable: harvester missing for session")
                return UsernameResolveResult(
                    response=UsernameSearchResponse(query=query, username=username, found=False),
                    http_error=_generic_unavailable(),
                )

            try:
                entity = await client.get_entity(username)
            except FloodWaitError:
                raise
            except UsernameInvalidError:
                return await _empty(query, username)
            except (UsernameNotOccupiedError, ValueError) as exc:
                if _username_not_found_error(exc):
                    return await _empty(query, username)
                raise

            user, community = await _materialize_entity(
                client,
                conn,
                entity,
                harvester_id=harvester.id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
            if user is not None:
                return await _found_user(query, username, user)
            if community is not None:
                return await _found_community(query, username, community)
            lg.error("Username @%s resolved but was not saved", username)
            return UsernameResolveResult(
                response=UsernameSearchResponse(query=query, username=username, found=False),
                http_error=(502, USERNAME_SEARCH_FAILED),
            )

    except FloodWaitError:
        raise
    except HarvesterSessionTerminatedError as exc:
        lg.warning("Username resolve materialize for @%s session terminated: %s", username, exc)
        return _unavailable_result(query, username)
    except Exception:
        lg.exception("Username resolve failed for @%s", username)
        return UsernameResolveResult(
            response=UsernameSearchResponse(query=query, username=username, found=False),
            http_error=(502, USERNAME_SEARCH_FAILED),
        )
