"""Staged user page load: DB-first, optional Telethon refresh slices."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from fastapi import HTTPException, Request
from telethon import TelegramClient
from telegram.api_session_pool import ApiSessionPool, PooledClientLease
from telegram.profile_media import pinned_stories_need_harvest, saved_star_gifts_need_harvest
from telegram.refresh_peer import (
    is_peer_profile_stale,
    refresh_user_if_stale,
    resolve_user_harvester_role,
)
from telegram.stories_gifts import StoriesAndGiftsFetchResult, fetch_user_stories_and_gifts_if_needed

from services.api.src.affiliated_peer_resolve import fill_affiliated_peers
from services.api.src.config import ConfigFactory
from services.api.src.crud.s3.media import presign_user_profile_media
from services.api.src.crud.supabase import get_conn
from services.api.src.crud.supabase.peers import (
    UserStoriesAndGiftsRow,
    get_user_by_id,
    get_user_stories_and_gifts_row,
)
from services.api.src.crud.supabase.views.telegram.user import UserPeerDetailView

lg = logging.getLogger(__name__)

UserRefreshMode = Literal["none", "history", "db", "profile", "stories", "affiliated", "all"]


@dataclass
class _UserTelethonContext:
    lease: PooledClientLease
    client: TelegramClient | None
    harvester_id: UUID | None
    access_hash: int | None
    config_missing: bool
    profile_warnings: list[str]
    resolved: object | None
    missing_harvester_role: bool
    stories_row: UserStoriesAndGiftsRow


def _telegram_harvester_configured(cfg: object) -> bool:
    return getattr(cfg, "telegram_api_id", 0) > 0 and bool(getattr(cfg, "telegram_api_hash", "").strip())


def _get_telegram_pool(request: Request) -> ApiSessionPool | None:
    pool = getattr(request.app.state, "telegram_pool", None)
    return pool if isinstance(pool, ApiSessionPool) else None


async def _pool_client_for_peer_role(
    request: Request,
    conn: object,
    *,
    harvester_id: UUID | None,
    access_hash: int | None,
    exclude_external_ids: tuple[str, ...] | None = None,
) -> tuple[TelegramClient | None, UUID | None, int | None]:
    pool = _get_telegram_pool(request)
    if pool is None:
        return None, harvester_id, access_hash if access_hash is not None else None
    return await pool.get_client_for_peer_role(
        conn,
        harvester_id=harvester_id,
        access_hash=access_hash,
        exclude_external_ids=exclude_external_ids,
    )


def _needs_stories_or_gifts_fetch(row: UserStoriesAndGiftsRow) -> bool:
    if bool(row["bot"]):
        return False
    if row["pinned_stories_fetched_at"] is None or row["star_gifts_fetched_at"] is None:
        return True
    pinned = row["pinned_stories"]
    if isinstance(pinned, dict) and pinned_stories_need_harvest(pinned):
        return True
    gifts = row["saved_star_gifts"]
    if isinstance(gifts, dict) and saved_star_gifts_need_harvest(gifts):
        return True
    return False


def _profile_array_len(payload: object | None, key: str) -> int:
    if not isinstance(payload, dict):
        return 0
    items = payload.get(key)
    return len(items) if isinstance(items, list) else 0


def _user_media_counts(user: UserPeerDetailView) -> tuple[int, int]:
    return (
        _profile_array_len(user.pinned_stories if isinstance(user.pinned_stories, dict) else None, "stories"),
        _profile_array_len(user.saved_star_gifts if isinstance(user.saved_star_gifts, dict) else None, "gifts"),
    )


def _affiliated_counts(user: UserPeerDetailView) -> tuple[int, int]:
    unknown = sum(1 for item in user.affiliated_peers if item.peer.is_unknown)
    return len(user.affiliated_peers), unknown


def _apply_fetch_result(
    user: UserPeerDetailView,
    fetch_result: StoriesAndGiftsFetchResult,
    *,
    config_missing: bool = False,
    stories_row: UserStoriesAndGiftsRow | None = None,
) -> UserPeerDetailView:
    pending_stories = fetch_result.pinned_stories_fetch_pending
    pending_gifts = fetch_result.saved_star_gifts_fetch_pending
    if config_missing and stories_row is not None:
        if stories_row["pinned_stories_fetched_at"] is None:
            pending_stories = True
        if stories_row["star_gifts_fetched_at"] is None:
            pending_gifts = True

    return user.model_copy(
        update={
            "pinned_stories_fetch_pending": pending_stories,
            "saved_star_gifts_fetch_pending": pending_gifts,
            "pinned_stories_fetch_warnings": fetch_result.pinned_stories_fetch_warnings,
            "saved_star_gifts_fetch_warnings": fetch_result.saved_star_gifts_fetch_warnings,
        }
    )


def _pending_flags_from_row(
    stories_row: UserStoriesAndGiftsRow | None,
    cfg: object,
) -> StoriesAndGiftsFetchResult:
    if stories_row is None or not _telegram_harvester_configured(cfg):
        return StoriesAndGiftsFetchResult()
    if bool(stories_row["bot"]):
        return StoriesAndGiftsFetchResult()
    pinned = stories_row["pinned_stories"]
    gifts = stories_row["saved_star_gifts"]
    pinned_pending = stories_row["pinned_stories_fetched_at"] is None or (
        isinstance(pinned, dict) and pinned_stories_need_harvest(pinned)
    )
    gifts_pending = stories_row["star_gifts_fetched_at"] is None or (
        isinstance(gifts, dict) and saved_star_gifts_need_harvest(gifts)
    )
    return StoriesAndGiftsFetchResult(
        pinned_stories_fetch_pending=pinned_pending,
        saved_star_gifts_fetch_pending=gifts_pending,
    )


async def _presign_user(
    user: UserPeerDetailView,
    *,
    stories_gifts: bool = True,
    field_history: bool = True,
) -> UserPeerDetailView:
    cfg = ConfigFactory.get_config()
    if not cfg.aws_region.strip():
        return user
    return await presign_user_profile_media(
        user,
        region=cfg.aws_region,
        stories_gifts=stories_gifts,
        field_history=field_history,
    )


async def _load_user_header(user_id_str: str) -> UserPeerDetailView:
    async with get_conn() as conn:
        user = await get_user_by_id(
            conn,
            user_id_str,
            include_stories_gifts=False,
            include_affiliated_peers=False,
            include_field_history=False,
        )
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
    return user


async def _load_user_history(user_id_str: str) -> UserPeerDetailView:
    async with get_conn() as conn:
        user = await get_user_by_id(
            conn,
            user_id_str,
            include_stories_gifts=False,
            include_affiliated_peers=False,
            include_field_history=True,
        )
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
    return user


async def _load_user_db(
    user_id_str: str,
    *,
    include_field_history: bool = False,
) -> tuple[UserPeerDetailView, UserStoriesAndGiftsRow | None]:
    async with get_conn() as conn:
        user = await get_user_by_id(
            conn,
            user_id_str,
            include_field_history=include_field_history,
        )
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        stories_row = await get_user_stories_and_gifts_row(conn, user_id_str)
    return user, stories_row


async def _acquire_user_client(
    request: Request,
    conn: object,
    *,
    user_id_str: str,
    stories_row: UserStoriesAndGiftsRow,
) -> _UserTelethonContext:
    cfg = ConfigFactory.get_config()
    profile_warnings: list[str] = []
    config_missing = False
    harvester_id = stories_row.get("harvester_id")
    if harvester_id is not None and not isinstance(harvester_id, UUID):
        harvester_id = UUID(str(harvester_id))
    access_hash = stories_row["access_hash"]
    peer_external_id = str(stories_row["external_id"])
    exclude_self = (peer_external_id,) if access_hash is None else None
    missing_harvester_role = not stories_row["harvester_external_id"] or access_hash is None
    if missing_harvester_role:
        lg.warning(
            "User page Telethon: no harvester role with access_hash for user_id=%s external_id=%s; attempting resolve",
            user_id_str,
            peer_external_id,
        )

    lease = PooledClientLease(_get_telegram_pool(request))
    client: TelegramClient | None = None
    resolved = None
    try:
        client, harvester_id, access_hash = await _pool_client_for_peer_role(
            request,
            conn,
            harvester_id=harvester_id,
            access_hash=access_hash,
            exclude_external_ids=exclude_self,
        )
        lease.bind(client, harvester_id, access_hash)
        if client is None and not missing_harvester_role and stories_row.get("username"):
            lg.warning(
                "User page Telethon: owning harvester session unavailable "
                "for user_id=%s external_id=%s; resolving via username without stored access_hash",
                user_id_str,
                peer_external_id,
            )
            client, harvester_id, access_hash = await _pool_client_for_peer_role(
                request,
                conn,
                harvester_id=None,
                access_hash=None,
                exclude_external_ids=(peer_external_id,),
            )
            lease.bind(client, harvester_id, access_hash)
            missing_harvester_role = True
        if client is None or harvester_id is None:
            config_missing = True
            profile_warnings.append("Profile refresh is temporarily unavailable")
        elif missing_harvester_role:
            resolved = await resolve_user_harvester_role(
                client,
                conn,
                external_id=stories_row["external_id"],
                username=stories_row["username"],
                harvester_id=harvester_id,
                access_hash=access_hash,
                s3_bucket=cfg.harvest_s3_bucket,
                s3_region=cfg.aws_region,
            )
            if resolved is None:
                config_missing = True
                profile_warnings.append("Could not resolve user via Telegram; profile refresh skipped")
            else:
                access_hash = resolved.access_hash
                refreshed_row = await get_user_stories_and_gifts_row(conn, user_id_str)
                if refreshed_row is not None:
                    stories_row = refreshed_row
                    if refreshed_row.get("harvester_id") is not None:
                        role_hid = refreshed_row["harvester_id"]
                        harvester_id = role_hid if isinstance(role_hid, UUID) else UUID(str(role_hid))
                client, harvester_id, access_hash = await _pool_client_for_peer_role(
                    request,
                    conn,
                    harvester_id=harvester_id,
                    access_hash=access_hash,
                )
                lease.bind(client, harvester_id, access_hash)
                if client is None or harvester_id is None or access_hash is None:
                    config_missing = True
                    profile_warnings.append("Profile refresh is temporarily unavailable")
                    resolved = None
    except Exception:
        lease.release()
        raise

    return _UserTelethonContext(
        lease=lease,
        client=client,
        harvester_id=harvester_id,
        access_hash=access_hash,
        config_missing=config_missing,
        profile_warnings=profile_warnings,
        resolved=resolved,
        missing_harvester_role=missing_harvester_role,
        stories_row=stories_row,
    )


async def _refresh_user_profile_slice(
    request: Request,
    *,
    user_id_str: str,
    stories_row: UserStoriesAndGiftsRow,
) -> StoriesAndGiftsFetchResult:
    cfg = ConfigFactory.get_config()
    if not _telegram_harvester_configured(cfg):
        lg.error(
            "User profile refresh skipped user_id=%s external_id=%s: Telethon not configured",
            user_id_str,
            stories_row["external_id"],
        )
        return _pending_flags_from_row(stories_row, cfg)

    if not is_peer_profile_stale(stories_row["updated_at"], cfg.peer_profile_refresh_ttl_minutes):
        lg.info(
            "User profile refresh skipped user_id=%s external_id=%s: within TTL",
            user_id_str,
            stories_row["external_id"],
        )
        return StoriesAndGiftsFetchResult()

    fetch_result = StoriesAndGiftsFetchResult()
    profile_warnings: list[str] = []
    async with get_conn() as conn:
        ctx = await _acquire_user_client(
            request,
            conn,
            user_id_str=user_id_str,
            stories_row=stories_row,
        )
        profile_warnings = ctx.profile_warnings
        stories_row = ctx.stories_row
        try:
            if (
                not ctx.config_missing
                and ctx.client is not None
                and ctx.harvester_id is not None
                and stories_row["harvester_external_id"]
                and ctx.access_hash is not None
            ):
                user_is_deleted = ctx.missing_harvester_role and ctx.resolved is not None and ctx.resolved.deleted
                if not user_is_deleted and not (ctx.missing_harvester_role and ctx.resolved is not None):
                    refresh_result = await refresh_user_if_stale(
                        ctx.client,
                        conn,
                        external_id=stories_row["external_id"],
                        access_hash=ctx.access_hash,
                        username=stories_row["username"],
                        updated_at=stories_row["updated_at"],
                        ttl_minutes=cfg.peer_profile_refresh_ttl_minutes,
                        harvester_id=ctx.harvester_id,
                        s3_bucket=cfg.harvest_s3_bucket,
                        s3_region=cfg.aws_region,
                    )
                    profile_warnings.extend(refresh_result.warnings)
        except Exception:
            lg.error(
                "User profile refresh failed user_id=%s external_id=%s",
                user_id_str,
                stories_row["external_id"],
                exc_info=True,
            )
            fetch_result = StoriesAndGiftsFetchResult(
                pinned_stories_fetch_warnings=["Profile refresh failed"],
            )
        finally:
            ctx.lease.release()

    return fetch_result


async def _refresh_user_stories_slice(
    request: Request,
    *,
    user_id_str: str,
    stories_row: UserStoriesAndGiftsRow,
) -> StoriesAndGiftsFetchResult:
    cfg = ConfigFactory.get_config()
    if not _telegram_harvester_configured(cfg):
        lg.error(
            "User stories refresh skipped user_id=%s external_id=%s: Telethon not configured",
            user_id_str,
            stories_row["external_id"],
        )
        return _pending_flags_from_row(stories_row, cfg)

    if not _needs_stories_or_gifts_fetch(stories_row):
        lg.info(
            "User stories refresh skipped user_id=%s external_id=%s: already fetched",
            user_id_str,
            stories_row["external_id"],
        )
        return StoriesAndGiftsFetchResult()

    fetch_result = StoriesAndGiftsFetchResult()
    async with get_conn() as conn:
        ctx = await _acquire_user_client(
            request,
            conn,
            user_id_str=user_id_str,
            stories_row=stories_row,
        )
        stories_row = ctx.stories_row
        try:
            if (
                not ctx.config_missing
                and ctx.client is not None
                and ctx.harvester_id is not None
                and stories_row["harvester_external_id"]
                and ctx.access_hash is not None
            ):
                user_is_deleted = ctx.missing_harvester_role and ctx.resolved is not None and ctx.resolved.deleted
                if not user_is_deleted:
                    existing_pinned = stories_row["pinned_stories"]
                    existing_gifts = stories_row["saved_star_gifts"]
                    fetch_result = await fetch_user_stories_and_gifts_if_needed(
                        ctx.client,
                        conn,
                        external_id=stories_row["external_id"],
                        access_hash=ctx.access_hash,
                        username=stories_row["username"],
                        bot=bool(stories_row["bot"]),
                        pinned_stories_fetched_at=stories_row["pinned_stories_fetched_at"],
                        star_gifts_fetched_at=stories_row["star_gifts_fetched_at"],
                        existing_pinned_stories=existing_pinned if isinstance(existing_pinned, dict) else None,
                        existing_saved_star_gifts=existing_gifts if isinstance(existing_gifts, dict) else None,
                        s3_bucket=cfg.harvest_s3_bucket,
                        s3_region=cfg.aws_region,
                    )
        except Exception:
            lg.error(
                "User stories refresh failed user_id=%s external_id=%s",
                user_id_str,
                stories_row["external_id"],
                exc_info=True,
            )
            fetch_result = StoriesAndGiftsFetchResult(
                pinned_stories_fetch_pending=stories_row["pinned_stories_fetched_at"] is None,
                saved_star_gifts_fetch_pending=stories_row["star_gifts_fetched_at"] is None,
                pinned_stories_fetch_warnings=["Stories/gifts fetch failed"],
                saved_star_gifts_fetch_warnings=["Stories/gifts fetch failed"],
            )
        finally:
            ctx.lease.release()

    return fetch_result


async def _refresh_user_affiliated_slice(
    request: Request,
    *,
    user_id_str: str,
    user: UserPeerDetailView,
) -> UserPeerDetailView:
    cfg = ConfigFactory.get_config()
    if not user.affiliated_peers or not _telegram_harvester_configured(cfg):
        return user

    unknown_before = sum(1 for item in user.affiliated_peers if item.peer.is_unknown)
    if unknown_before == 0:
        lg.info(
            "User affiliated refresh skipped user_id=%s external_id=%s: none unknown",
            user_id_str,
            user.external_id,
        )
        return user

    try:
        async with get_conn() as conn:
            filled = await fill_affiliated_peers(
                _get_telegram_pool(request),
                conn,
                user.affiliated_peers,
                s3_bucket=cfg.harvest_s3_bucket,
                s3_region=cfg.aws_region,
            )
        user = user.model_copy(update={"affiliated_peers": filled})
    except Exception:
        lg.exception("User affiliated refresh failed user_id=%s", user_id_str)
        return user

    return user


async def load_user_page(
    user_id: UUID,
    request: Request,
    *,
    refresh: UserRefreshMode = "none",
) -> UserPeerDetailView:
    """Load user page data. ``refresh=none`` returns a fast header snapshot."""
    user_id_str = str(user_id)
    cfg = ConfigFactory.get_config()

    if refresh == "none":
        user = await _load_user_header(user_id_str)
        lg.info(
            "User page header user_id=%s external_id=%s",
            user_id_str,
            user.external_id,
        )
        return await _presign_user(user, stories_gifts=False, field_history=False)

    if refresh == "history":
        user = await _load_user_history(user_id_str)
        lg.info(
            "User page history user_id=%s external_id=%s history=%s",
            user_id_str,
            user.external_id,
            len(user.field_history),
        )
        return await _presign_user(user, stories_gifts=False, field_history=True)

    if refresh == "profile":
        async with get_conn() as conn:
            stories_row = await get_user_stories_and_gifts_row(conn, user_id_str)
        if stories_row is None:
            raise HTTPException(status_code=404, detail="User not found")
        await _refresh_user_profile_slice(
            request,
            user_id_str=user_id_str,
            stories_row=stories_row,
        )
        # Identity only: keep already-loaded history and affiliated peers on the client.
        user = await _load_user_header(user_id_str)
        lg.info(
            "User page load user_id=%s external_id=%s refresh=profile",
            user_id_str,
            user.external_id,
        )
        return await _presign_user(user, stories_gifts=False, field_history=False)

    if refresh == "db":
        user, stories_row = await _load_user_db(user_id_str)
        fetch_result = _pending_flags_from_row(stories_row, cfg)
        user = _apply_fetch_result(user, fetch_result, stories_row=stories_row)
        stories_count, gifts_count = _user_media_counts(user)
        affiliated_count, unknown_affiliated = _affiliated_counts(user)
        lg.info(
            "User page db user_id=%s external_id=%s stories=%s gifts=%s "
            "affiliated=%s unknown_affiliated=%s pending_stories=%s pending_gifts=%s",
            user_id_str,
            user.external_id,
            stories_count,
            gifts_count,
            affiliated_count,
            unknown_affiliated,
            user.pinned_stories_fetch_pending,
            user.saved_star_gifts_fetch_pending,
        )
        return await _presign_user(user)

    user, stories_row = await _load_user_db(user_id_str)
    fetch_result = StoriesAndGiftsFetchResult()
    config_missing = False

    if stories_row is None:
        if refresh in ("affiliated", "all"):
            user = await _refresh_user_affiliated_slice(
                request,
                user_id_str=user_id_str,
                user=user,
            )
        user = _apply_fetch_result(user, fetch_result, config_missing=config_missing, stories_row=stories_row)
        return await _presign_user(user)

    if refresh in ("profile", "all"):
        profile_fetch = await _refresh_user_profile_slice(
            request,
            user_id_str=user_id_str,
            stories_row=stories_row,
        )
        fetch_result = StoriesAndGiftsFetchResult(
            pinned_stories_fetch_warnings=profile_fetch.pinned_stories_fetch_warnings,
            saved_star_gifts_fetch_warnings=profile_fetch.saved_star_gifts_fetch_warnings,
        )
        user, stories_row = await _load_user_db(user_id_str)

    if refresh in ("stories", "all"):
        stories_fetch = await _refresh_user_stories_slice(
            request,
            user_id_str=user_id_str,
            stories_row=stories_row,
        )
        fetch_result = StoriesAndGiftsFetchResult(
            pinned_stories_fetch_pending=stories_fetch.pinned_stories_fetch_pending,
            saved_star_gifts_fetch_pending=stories_fetch.saved_star_gifts_fetch_pending,
            pinned_stories_fetch_warnings=[
                *fetch_result.pinned_stories_fetch_warnings,
                *stories_fetch.pinned_stories_fetch_warnings,
            ],
            saved_star_gifts_fetch_warnings=[
                *fetch_result.saved_star_gifts_fetch_warnings,
                *stories_fetch.saved_star_gifts_fetch_warnings,
            ],
        )
        user, stories_row = await _load_user_db(user_id_str)

    if refresh in ("affiliated", "all"):
        user = await _refresh_user_affiliated_slice(
            request,
            user_id_str=user_id_str,
            user=user,
        )

    if not _telegram_harvester_configured(cfg) and stories_row is not None:
        needs_profile = is_peer_profile_stale(
            stories_row["updated_at"],
            cfg.peer_profile_refresh_ttl_minutes,
        )
        needs_stories = _needs_stories_or_gifts_fetch(stories_row)
        if needs_profile or needs_stories:
            config_missing = True
            lg.error(
                "User page Telethon skipped user_id=%s external_id=%s: not configured",
                user_id_str,
                stories_row["external_id"],
            )

    if stories_row is not None:
        row_pending = _pending_flags_from_row(stories_row, cfg)
        fetch_result = StoriesAndGiftsFetchResult(
            pinned_stories_fetch_pending=(
                fetch_result.pinned_stories_fetch_pending or row_pending.pinned_stories_fetch_pending
            ),
            saved_star_gifts_fetch_pending=(
                fetch_result.saved_star_gifts_fetch_pending or row_pending.saved_star_gifts_fetch_pending
            ),
            pinned_stories_fetch_warnings=fetch_result.pinned_stories_fetch_warnings,
            saved_star_gifts_fetch_warnings=fetch_result.saved_star_gifts_fetch_warnings,
        )

    user = _apply_fetch_result(user, fetch_result, config_missing=config_missing, stories_row=stories_row)
    stories_count, gifts_count = _user_media_counts(user)
    affiliated_count, unknown_affiliated = _affiliated_counts(user)
    lg.info(
        "User page load user_id=%s external_id=%s refresh=%s stories=%s gifts=%s "
        "affiliated=%s unknown_affiliated=%s pending_stories=%s pending_gifts=%s",
        user_id_str,
        user.external_id,
        refresh,
        stories_count,
        gifts_count,
        affiliated_count,
        unknown_affiliated,
        user.pinned_stories_fetch_pending,
        user.saved_star_gifts_fetch_pending,
    )
    return await _presign_user(user)
