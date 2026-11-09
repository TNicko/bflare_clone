"""TTL-gated Telethon refresh of peer profile rows on page load."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, UsernameNotOccupiedError
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerUser, PeerChannel, User

from db.crud.telegram.harvester import (
    clear_harvester_peer_access_error,
    record_harvester_peer_access_error,
)
from db.crud.telegram.peers import set_peer_unavailability
from db.schemas.general.primitives import PeerTypeSchema, TelegramCommunityProfileTierSchema
from telegram.availability import (
    is_community_unavailable_error,
    is_forbidden_peer_entity,
    is_telethon_entity_cache_miss,
    is_user_unavailable_error,
)
from telegram.harvester_peer_access import (
    classify_harvester_access_error_code,
    deactivate_harvester_if_account_frozen,
    is_harvester_peer_inaccessible_error,
)
from telegram.materialize import materialize_channel, materialize_chat, materialize_user
from telegram.profile_photos import harvest_current_profile_photo
from telegram.utils import channel_id_from_signed_peer_id, to_signed_peer_id

logger = logging.getLogger(__name__)


def _is_stale_username_error(exc: BaseException) -> bool:
    """Username free/invalid — channel may still exist under another handle or as private."""
    if isinstance(exc, UsernameNotOccupiedError):
        return True
    cause = getattr(exc, "__cause__", None)
    if isinstance(cause, UsernameNotOccupiedError):
        return True
    msg = str(exc).lower()
    return "nobody is using this username" in msg or ("no user has" in msg and "as username" in msg)


def _is_definitive_community_gone(exc: BaseException) -> bool:
    """True when Telegram says the community itself is gone/forbidden (not a cache miss)."""
    if is_telethon_entity_cache_miss(exc):
        return False
    if _is_stale_username_error(exc):
        return False
    return is_community_unavailable_error(exc)


@dataclass(slots=True)
class RefreshPeerResult:
    refreshed: bool = False
    warnings: list[str] = field(default_factory=list)
    #: Local resolve paths exhausted (no usable entity). Caller should try
    #: forward-seed when possible, then mark the peer unavailable.
    unresolvable: bool = False


@dataclass(slots=True, frozen=True)
class ResolvedUserHarvesterRole:
    access_hash: int
    harvester_id: UUID
    deleted: bool = False


async def resolve_user_harvester_role(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    username: str | None,
    harvester_id: UUID,
    access_hash: int | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> ResolvedUserHarvesterRole | None:
    """Resolve a user via Telethon and upsert parent ``telegram_harvester_peer_roles``."""
    entity = await _resolve_user_entity(
        client,
        conn,
        external_id=external_id,
        username=username,
        access_hash=access_hash,
    )
    if entity is None:
        return None

    signed_peer_id = to_signed_peer_id(entity)
    if signed_peer_id is None or str(signed_peer_id) != external_id:
        logger.warning(
            "User %s resolved to mismatched external_id %s",
            external_id,
            signed_peer_id,
        )
        return None

    access_hash = entity.access_hash
    if access_hash is None:
        logger.warning("User %s resolved but Telethon returned no access_hash", external_id)
        return None

    if getattr(entity, "is_self", False):
        logger.warning(
            "User %s resolved as is_self for harvester_id=%s; refusing role upsert "
            "(need a different harvester session)",
            external_id,
            harvester_id,
        )
        return None

    if not await materialize_user(
        client,
        conn,
        entity,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    ):
        return None

    if entity.deleted:
        logger.info("Resolved deleted user %s with harvester id=%s", external_id, harvester_id)

    return ResolvedUserHarvesterRole(
        access_hash=access_hash,
        harvester_id=harvester_id,
        deleted=bool(entity.deleted),
    )


def is_peer_profile_stale(updated_at: datetime | None, ttl_minutes: int) -> bool:
    if ttl_minutes <= 0:
        return True
    if updated_at is None:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=UTC)
    return updated_at < datetime.now(UTC) - timedelta(minutes=ttl_minutes)


def community_profile_needs_refresh(
    profile_tier: TelegramCommunityProfileTierSchema | str | None,
    updated_at: datetime | None,
    ttl_minutes: int,
) -> bool:
    """Return whether a channel/megagroup row should be refreshed via Telethon."""
    if profile_tier in (None, "web", "tg_min"):
        return True
    return is_peer_profile_stale(updated_at, ttl_minutes)


def community_access_probe_due(access_checked_at: datetime | None, ttl_minutes: int) -> bool:
    """True when a flagged-inaccessible community should GetChannels again."""
    return is_peer_profile_stale(access_checked_at, ttl_minutes)


async def probe_inaccessible_community(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    access_hash: int | None,
    harvester_id: UUID,
    username: str | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> RefreshPeerResult:
    """One access check for a community already marked unavailable / CHANNEL_PRIVATE.

    Uses ``channels.GetChannels`` (or chat entity resolve). On success, materializes
    the peer and clears unavailability. On still-inaccessible, re-records the access
    error (updates ``access_checked_at``) and does not fetch history or photos.
    """
    if peer_type == "chat":
        try:
            chat_id = int(external_id)
        except ValueError:
            return RefreshPeerResult(warnings=[f"Invalid chat external_id={external_id!r}"])
        try:
            entity = await client.get_entity(chat_id)
        except FloodWaitError:
            raise
        except Exception as exc:
            logger.info("Inaccessible-community probe failed external_id=%s: %s", external_id, exc)
            if await deactivate_harvester_if_account_frozen(
                conn,
                harvester_id=harvester_id,
                exc=exc,
            ):
                return RefreshPeerResult(warnings=[f"Harvester account frozen: {exc}"])
            if is_harvester_peer_inaccessible_error(exc):
                await record_harvester_peer_access_error(
                    conn,
                    harvester_id=harvester_id,
                    external_id=external_id,
                    error_code=classify_harvester_access_error_code(exc),
                    peer_type="chat",
                )
            elif is_community_unavailable_error(exc):
                await set_peer_unavailability(conn, external_id, peer_type="chat", unavailable=True)
            return RefreshPeerResult(warnings=[f"Access probe failed: {exc}"])
        if is_forbidden_peer_entity(entity) or not isinstance(entity, Chat):
            await set_peer_unavailability(conn, external_id, peer_type="chat", unavailable=True)
            return RefreshPeerResult(warnings=["Access probe resolved a forbidden or non-chat entity"])
        await materialize_chat(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        logger.info("Inaccessible-community probe recovered chat external_id=%s", external_id)
        return RefreshPeerResult(refreshed=True)

    if peer_type not in ("channel", "megagroup"):
        return RefreshPeerResult(warnings=[f"Unsupported peer_type={peer_type!r}"])

    try:
        signed = int(external_id)
    except ValueError:
        return RefreshPeerResult(warnings=[f"Invalid channel external_id={external_id!r}"])
    raw_channel_id = channel_id_from_signed_peer_id(signed)
    if raw_channel_id is None:
        return RefreshPeerResult(warnings=[f"external_id={external_id!r} is not a channel signed id"])

    last_exc: Exception | None = None
    entity: Any = None
    if access_hash is not None:
        try:
            entity = await client.get_entity(InputPeerChannel(raw_channel_id, access_hash))
        except FloodWaitError:
            raise
        except Exception as exc:
            last_exc = exc
    else:
        handle = (username or "").strip().lstrip("@")
        if handle:
            try:
                entity = await client.get_entity(handle)
            except FloodWaitError:
                raise
            except Exception as exc:
                last_exc = exc

    if entity is None or is_forbidden_peer_entity(entity) or not isinstance(entity, Channel):
        if last_exc is not None and await deactivate_harvester_if_account_frozen(
            conn,
            harvester_id=harvester_id,
            exc=last_exc,
        ):
            return RefreshPeerResult(warnings=[f"Harvester account frozen: {last_exc}"])
        if last_exc is not None and is_harvester_peer_inaccessible_error(last_exc):
            await record_harvester_peer_access_error(
                conn,
                harvester_id=harvester_id,
                external_id=external_id,
                error_code=classify_harvester_access_error_code(last_exc),
                peer_type=peer_type,  # type: ignore[arg-type]
                access_hash=access_hash,
            )
        elif last_exc is not None and is_community_unavailable_error(last_exc):
            await set_peer_unavailability(
                conn,
                external_id,
                peer_type=peer_type,  # type: ignore[arg-type]
                unavailable=True,
            )
        elif entity is not None and is_forbidden_peer_entity(entity):
            await set_peer_unavailability(
                conn,
                external_id,
                peer_type=peer_type,  # type: ignore[arg-type]
                unavailable=True,
            )
        logger.info(
            "Inaccessible-community probe failed external_id=%s: %s",
            external_id,
            last_exc or type(entity).__name__,
        )
        return RefreshPeerResult(warnings=[f"Access probe failed: {last_exc or type(entity).__name__}"])

    await materialize_channel(
        client,
        conn,
        entity,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    logger.info("Inaccessible-community probe recovered external_id=%s", external_id)
    return RefreshPeerResult(refreshed=True)


async def refresh_user_if_stale(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    access_hash: int | None,
    updated_at: datetime | None,
    ttl_minutes: int,
    harvester_id: UUID,
    username: str | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> RefreshPeerResult:
    if not is_peer_profile_stale(updated_at, ttl_minutes):
        return RefreshPeerResult()
    if access_hash is None and not (username or "").strip():
        return RefreshPeerResult(warnings=["Missing access_hash; profile refresh skipped"])

    entity = await _resolve_user_entity(
        client,
        conn,
        external_id=external_id,
        username=username,
        access_hash=access_hash,
    )
    if entity is None:
        return RefreshPeerResult(warnings=["Profile refresh failed: user could not be resolved"])

    await materialize_user(
        client,
        conn,
        entity,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    return RefreshPeerResult(refreshed=True)


async def _resolve_user_entity(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    username: str | None,
    access_hash: int | None,
) -> User | None:
    """Resolve a user via InputPeerUser and/or @username; mark unavailable when unreachable."""
    handle = (username or "").strip().lstrip("@")
    has_username = bool(handle)
    if access_hash is None and not has_username:
        logger.info(
            "Skipping user resolve for %s: no access_hash or username to query Telegram",
            external_id,
        )
        await set_peer_unavailability(conn, external_id, peer_type="user", unavailable=True)
        return None

    last_exc: BaseException | None = None

    async def attempt(label: str, lookup: Any) -> User | None:
        nonlocal last_exc
        try:
            resolved = await client.get_entity(lookup)
        except FloodWaitError:
            raise
        except Exception as exc:
            last_exc = exc
            logger.debug("User resolve %s for %s failed: %s", label, external_id, exc)
            return None
        if not isinstance(resolved, User):
            logger.warning(
                "User %s resolve via %s returned unsupported entity type %s",
                external_id,
                label,
                type(resolved).__name__,
            )
            return None
        return resolved

    entity: User | None = None
    if access_hash is not None:
        entity = await attempt("InputPeerUser", InputPeerUser(int(external_id), access_hash))
    if entity is None and has_username:
        entity = await attempt("username", handle)

    if entity is not None:
        return entity

    logger.warning(
        "Failed to resolve user %s%s: %s",
        external_id,
        f" (@{handle})" if has_username else "",
        last_exc,
    )
    if last_exc is not None and is_user_unavailable_error(last_exc):
        await set_peer_unavailability(conn, external_id, peer_type="user", unavailable=True)
    return None


async def refresh_community_if_stale(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    access_hash: int | None,
    updated_at: datetime | None,
    ttl_minutes: int,
    harvester_id: UUID,
    profile_tier: TelegramCommunityProfileTierSchema | str | None = None,
    username: str | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> RefreshPeerResult:
    if not community_profile_needs_refresh(profile_tier, updated_at, ttl_minutes):
        return RefreshPeerResult()

    if peer_type == "chat":
        try:
            chat_id = int(external_id)
        except ValueError:
            return RefreshPeerResult(warnings=[f"Invalid chat external_id={external_id!r}"])
        try:
            entity = await client.get_entity(chat_id)
        except FloodWaitError:
            raise
        except Exception as exc:
            logger.warning("GetFullChat entity resolve failed external_id=%s: %s", external_id, exc)
            if is_community_unavailable_error(exc):
                await set_peer_unavailability(
                    conn,
                    external_id,
                    peer_type="chat",
                    unavailable=True,
                )
            return RefreshPeerResult(warnings=[f"Profile refresh failed: {exc}"])
        if is_forbidden_peer_entity(entity):
            await set_peer_unavailability(
                conn,
                external_id,
                peer_type="chat",
                unavailable=True,
            )
            return RefreshPeerResult(warnings=["Resolved entity is ChatForbidden"])
        if not isinstance(entity, Chat):
            return RefreshPeerResult(warnings=["Resolved entity is not a Chat"])
        await materialize_chat(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        return RefreshPeerResult(refreshed=True)

    if peer_type not in ("channel", "megagroup"):
        return RefreshPeerResult(warnings=[f"Unsupported peer_type={peer_type!r}"])

    try:
        signed = int(external_id)
    except ValueError:
        return RefreshPeerResult(warnings=[f"Invalid channel external_id={external_id!r}"])

    raw_channel_id = channel_id_from_signed_peer_id(signed)
    if raw_channel_id is None:
        return RefreshPeerResult(warnings=[f"external_id={external_id!r} is not a channel signed id"])

    if access_hash is None:
        handle = (username or "").strip().lstrip("@")
        entity: Any = None
        username_exc: Exception | None = None
        last_exc: Exception | None = None
        if handle:
            try:
                entity = await client.get_entity(handle)
            except FloodWaitError:
                raise
            except Exception as exc:
                username_exc = exc
                last_exc = exc
                logger.warning(
                    "GetFullChannel username resolve failed external_id=%s @%s: %s",
                    external_id,
                    handle,
                    exc,
                )
        if entity is None:
            # Session cache / recent dialogs often know the peer without a stored hash.
            try:
                entity = await client.get_entity(PeerChannel(raw_channel_id))
            except FloodWaitError:
                raise
            except Exception as exc:
                last_exc = exc
                log_fn = logger.info if is_telethon_entity_cache_miss(exc) else logger.warning
                log_fn(
                    "GetFullChannel PeerChannel resolve failed external_id=%s: %s",
                    external_id,
                    exc,
                )
        if entity is None:
            # Prefer a definitive Telegram "gone/forbidden" signal over a cache miss
            # (username failure must not be overwritten by a later PeerChannel miss).
            # Stale @username alone is not definitive — forward-seed may still recover.
            definitive_exc: Exception | None = None
            for candidate in (username_exc, last_exc):
                if candidate is None:
                    continue
                if _is_definitive_community_gone(candidate):
                    definitive_exc = candidate
                    break

            if definitive_exc is not None:
                await set_peer_unavailability(
                    conn,
                    external_id,
                    peer_type=peer_type,  # type: ignore[arg-type]
                    unavailable=True,
                )
                return RefreshPeerResult(
                    warnings=[f"Profile refresh failed: {definitive_exc}"],
                    unresolvable=True,
                )

            warnings: list[str] = []
            if username_exc is not None:
                warnings.append(f"Username resolve failed: {username_exc}")
            if last_exc is not None and is_telethon_entity_cache_miss(last_exc):
                warnings.append(
                    "Missing access_hash; peer not in Telethon entity cache (forward-seed resolve may recover)"
                )
            elif last_exc is not None and last_exc is not username_exc:
                warnings.append(f"Profile refresh failed: {last_exc}")
            if not handle:
                warnings.append("Missing access_hash and username; profile refresh skipped")
            elif not warnings:
                warnings.append(f"Profile refresh failed: {last_exc}")
            return RefreshPeerResult(warnings=warnings, unresolvable=True)
    else:
        entity = None
        last_exc: Exception | None = None
        try:
            entity = await client.get_entity(InputPeerChannel(raw_channel_id, access_hash))
        except FloodWaitError:
            raise
        except Exception as exc:
            last_exc = exc
            logger.info(
                "channels.GetChannels failed external_id=%s: %s; trying cache / half resolve",
                external_id,
                exc,
            )
            if await deactivate_harvester_if_account_frozen(
                conn,
                harvester_id=harvester_id,
                exc=exc,
            ):
                return RefreshPeerResult(warnings=[f"Harvester account frozen: {exc}"])
            if is_harvester_peer_inaccessible_error(exc):
                await record_harvester_peer_access_error(
                    conn,
                    harvester_id=harvester_id,
                    external_id=external_id,
                    error_code=classify_harvester_access_error_code(exc),
                    peer_type=peer_type,  # type: ignore[arg-type]
                    access_hash=access_hash,
                )
            # Forward-seed often left the Channel in session cache even when
            # GetChannels is denied for private peers.
            try:
                entity = await client.get_entity(PeerChannel(raw_channel_id))
            except FloodWaitError:
                raise
            except Exception as cache_exc:
                last_exc = cache_exc
                if not is_telethon_entity_cache_miss(cache_exc):
                    logger.info(
                        "PeerChannel cache resolve failed external_id=%s: %s",
                        external_id,
                        cache_exc,
                    )

        if entity is None:
            # Still try profile photo download with the stored access_hash.
            harvested = False
            try:
                harvested = await harvest_current_profile_photo(
                    client,
                    conn,
                    entity=InputPeerChannel(raw_channel_id, access_hash),
                    peer_type=peer_type,
                    external_id=external_id,
                    bucket=s3_bucket,
                    region=s3_region,
                    current_photo=None,
                )
            except FloodWaitError:
                raise
            except Exception:
                logger.exception(
                    "Profile photo harvest after GetChannels failure failed external_id=%s",
                    external_id,
                )
            # ChannelPrivateError means we can't join/get full — not that the
            # forward-discovered stub is gone. Keep existing tg_min row, but
            # record the harvester access error so the UI can show Unavailable.
            if last_exc is not None and await deactivate_harvester_if_account_frozen(
                conn,
                harvester_id=harvester_id,
                exc=last_exc,
            ):
                warnings = [f"Harvester account frozen: {last_exc}"]
                if harvested:
                    warnings.append("Profile photo harvested without full channel resolve")
                return RefreshPeerResult(refreshed=harvested, warnings=warnings)
            if last_exc is not None and is_harvester_peer_inaccessible_error(last_exc):
                await record_harvester_peer_access_error(
                    conn,
                    harvester_id=harvester_id,
                    external_id=external_id,
                    error_code=classify_harvester_access_error_code(last_exc),
                    peer_type=peer_type,  # type: ignore[arg-type]
                    access_hash=access_hash,
                )
            if (
                last_exc is not None
                and is_community_unavailable_error(last_exc)
                and not isinstance(last_exc, ChannelPrivateError)
            ):
                await set_peer_unavailability(
                    conn,
                    external_id,
                    peer_type=peer_type,  # type: ignore[arg-type]
                    unavailable=True,
                )
            warnings = [f"Profile refresh failed: {last_exc}"]
            if harvested:
                warnings.append("Profile photo harvested without full channel resolve")
            return RefreshPeerResult(refreshed=harvested, warnings=warnings)

    if is_forbidden_peer_entity(entity):
        await set_peer_unavailability(
            conn,
            external_id,
            peer_type=peer_type,  # type: ignore[arg-type]
            unavailable=True,
        )
        return RefreshPeerResult(warnings=["Resolved entity is ChannelForbidden"])

    if not isinstance(entity, Channel):
        return RefreshPeerResult(warnings=["Resolved entity is not a Channel"])

    await materialize_channel(
        client,
        conn,
        entity,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    # Only clear when GetChannels (or username resolve) succeeded — not when we
    # fell back to a private/forbidden cache stub after CHANNEL_PRIVATE.
    if last_exc is None or not is_harvester_peer_inaccessible_error(last_exc):
        await clear_harvester_peer_access_error(
            conn,
            harvester_id=harvester_id,
            external_id=external_id,
        )
    return RefreshPeerResult(refreshed=True)
