"""Detect when a Telegram peer account or community is gone or unreachable."""

from __future__ import annotations

from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChatForbiddenError,
    PeerIdInvalidError,
    UserIdInvalidError,
    UsernameNotOccupiedError,
)

from telegram.harvester_peer_access import is_harvester_peer_inaccessible_error


def _value_error_indicates_missing_entity(exc: ValueError) -> bool:
    if isinstance(exc.__cause__, UsernameNotOccupiedError):
        return True
    msg = str(exc).lower()
    if "no user has" in msg and "as username" in msg:
        return True
    if "cannot find any entity corresponding to" in msg:
        return True
    if "could not find the input entity" in msg:
        return True
    return False


def is_telethon_entity_cache_miss(exc: BaseException) -> bool:
    """True when Telethon has no InputPeer in the session entity cache.

    This is **not** the same as the peer being gone or private — often a
    forward-source seed (re-fetch a message that references the peer) is enough.
    """
    if not isinstance(exc, ValueError):
        return False
    return "could not find the input entity" in str(exc).lower()


def is_user_unavailable_error(exc: BaseException) -> bool:
    """True when Telethon indicates the user no longer exists or cannot be resolved."""
    if isinstance(exc, (UsernameNotOccupiedError, UserIdInvalidError, PeerIdInvalidError)):
        return True
    if isinstance(exc, ValueError):
        return _value_error_indicates_missing_entity(exc)
    return False


def is_community_unavailable_error(exc: BaseException) -> bool:
    """True when Telethon indicates a channel/megagroup/chat cannot be fetched."""
    if isinstance(
        exc,
        (
            ChannelInvalidError,
            ChannelPrivateError,
            ChatForbiddenError,
            PeerIdInvalidError,
            UsernameNotOccupiedError,
        ),
    ):
        return True
    if is_harvester_peer_inaccessible_error(exc):
        return True
    # Session cache miss ≠ unavailable; caller should try forward-seed resolve.
    if is_telethon_entity_cache_miss(exc):
        return False
    if isinstance(exc, ValueError):
        return _value_error_indicates_missing_entity(exc)
    return False


def is_forbidden_peer_entity(entity: object) -> bool:
    """True when Telethon returned a forbidden/minimal peer stub instead of full entity."""
    from telethon.tl.types import ChannelForbidden, ChatForbidden

    return isinstance(entity, (ChannelForbidden, ChatForbidden))
