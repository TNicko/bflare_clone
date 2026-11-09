"""Classify and persist harvester-scoped peer access failures from Telethon."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from telethon.errors import (
    ChannelPrivateError,
    ChatForbiddenError,
    FrozenParticipantMissingError,
    PeerIdInvalidError,
    RPCError,
    UserBannedInChannelError,
)

from db.crud.telegram.harvester import deactivate_harvester

lg = logging.getLogger(__name__)

INACCESSIBLE_HARVESTER_PEER_ERRORS: tuple[type[RPCError], ...] = (
    ChannelPrivateError,
    UserBannedInChannelError,
    ChatForbiddenError,
    PeerIdInvalidError,
)

_INACCESSIBLE_ERROR_CODES: dict[type[RPCError], str] = {
    ChannelPrivateError: "CHANNEL_PRIVATE",
    UserBannedInChannelError: "USER_BANNED_IN_CHANNEL",
    ChatForbiddenError: "CHAT_FORBIDDEN",
    PeerIdInvalidError: "PEER_ID_INVALID",
}

#: Account-level freezes — the harvester must be taken offline globally.
ACCOUNT_FROZEN_HARVESTER_ERRORS: tuple[type[RPCError], ...] = (FrozenParticipantMissingError,)


def is_harvester_peer_inaccessible_error(exc: BaseException) -> bool:
    return isinstance(exc, INACCESSIBLE_HARVESTER_PEER_ERRORS)


def is_harvester_account_frozen_error(exc: BaseException) -> bool:
    """True when Telegram reports the harvester account itself is frozen."""
    return isinstance(exc, ACCOUNT_FROZEN_HARVESTER_ERRORS)


def classify_harvester_access_error_code(exc: BaseException) -> str:
    for error_type, code in _INACCESSIBLE_ERROR_CODES.items():
        if isinstance(exc, error_type):
            return code
    if isinstance(exc, RPCError) and exc.message:
        return str(exc.message)
    return type(exc).__name__


async def deactivate_harvester_if_account_frozen(
    conn: Any,
    *,
    harvester_id: UUID | str | None,
    exc: BaseException,
    pool: Any | None = None,
) -> bool:
    """If ``exc`` is an account freeze, deactivate the harvester and drop it from the API pool.

    Returns ``True`` when the harvester was deactivated (or already inactive after a
    freeze match). Callers should treat a ``True`` result as "do not keep using this
    account"; subsequent peer refresh will prefer another active harvester.
    """
    if harvester_id is None or not is_harvester_account_frozen_error(exc):
        return False

    hid = harvester_id if isinstance(harvester_id, UUID) else UUID(str(harvester_id))
    changed = await deactivate_harvester(conn, hid)
    try:
        await conn.commit()
    except Exception:
        lg.exception("Failed to commit harvester deactivation harvester_id=%s", hid)
        raise

    if pool is not None:
        drop = getattr(pool, "drop_harvester", None)
        if callable(drop):
            try:
                await drop(hid)
            except Exception:
                lg.exception(
                    "Failed to drop frozen harvester from API pool harvester_id=%s",
                    hid,
                )

    if changed:
        lg.error(
            "Deactivated harvester_id=%s after Telegram account freeze (%s)",
            hid,
            type(exc).__name__,
        )
    else:
        lg.warning(
            "Harvester already inactive after account freeze harvester_id=%s (%s)",
            hid,
            type(exc).__name__,
        )
    return True
