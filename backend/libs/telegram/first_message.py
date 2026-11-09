"""Fetch oldest accessible message date for a Telegram community peer."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import (
    Chat,
    InputPeerChannel,
    PeerChannel,
)

from db.crud.telegram.peers import set_peer_first_message_at
from db.schemas.general.primitives import TelegramPeerTypeSchema
from telegram.harvester_peer_access import is_harvester_peer_inaccessible_error
from telegram.utils import channel_id_from_signed_peer_id
from psycopg import sql

logger = logging.getLogger(__name__)

_COMMUNITY_PEER_TYPES: frozenset[TelegramPeerTypeSchema] = frozenset({"channel", "megagroup", "chat"})

_PEER_TABLE: dict[TelegramPeerTypeSchema, str] = {
    "channel": "telegram_channels",
    "megagroup": "telegram_megagroups",
    "chat": "telegram_chats",
}


async def _first_message_at_missing(
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> bool:
    table = _PEER_TABLE.get(peer_type)
    if table is None:
        return False
    query = sql.SQL(
        """
        SELECT first_message_at IS NULL AS missing
        FROM {table}
        WHERE external_id = %s
        """
    ).format(table=sql.Identifier(table))
    async with conn.cursor() as cur:
        await cur.execute(query, (external_id,))
        row = await cur.fetchone()
    if row is None:
        return False
    return bool(row[0] if not isinstance(row, dict) else row.get("missing"))


async def fetch_oldest_message_at(
    client: TelegramClient,
    entity: Any,
) -> datetime | None:
    """Return date of the oldest accessible message via reverse GetHistory.

    Does not upsert the message or write coverage. Returns None when history is
    empty or the peer is inaccessible.
    """
    try:
        messages = await client.get_messages(entity, limit=1, reverse=True)
    except FloodWaitError:
        raise
    except Exception as exc:
        if is_harvester_peer_inaccessible_error(exc):
            logger.info("Oldest-message seek inaccessible: %s", exc)
            return None
        logger.warning("Oldest-message seek failed: %s", exc)
        return None

    if not messages:
        return None
    msg = messages[0]
    date = getattr(msg, "date", None)
    return date if isinstance(date, datetime) else None


async def ensure_peer_first_message_at(
    client: TelegramClient,
    conn: Any,
    entity: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> datetime | None:
    """Fetch oldest message date and persist when ``first_message_at`` is null.

    Returns the date written (or None if fetch failed / empty).
    """
    if peer_type not in _COMMUNITY_PEER_TYPES:
        return None
    if not await _first_message_at_missing(conn, external_id=external_id, peer_type=peer_type):
        return None
    try:
        first_at = await fetch_oldest_message_at(client, entity)
    except FloodWaitError:
        raise
    if first_at is None:
        return None
    await set_peer_first_message_at(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        first_message_at=first_at,
    )
    return first_at


async def resolve_community_entity_for_history(
    client: TelegramClient,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    access_hash: int | None,
    username: str | None = None,
) -> Any | None:
    """Best-effort resolve a Telethon entity usable for GetHistory."""
    if peer_type == "chat":
        try:
            chat_id = int(external_id)
        except ValueError:
            return None
        try:
            entity = await client.get_entity(chat_id)
        except FloodWaitError:
            raise
        except Exception as exc:
            logger.info(
                "Chat entity resolve for first_message_at failed external_id=%s: %s",
                external_id,
                exc,
            )
            return None
        return entity if isinstance(entity, Chat) else entity

    if peer_type not in ("channel", "megagroup"):
        return None

    try:
        signed = int(external_id)
    except ValueError:
        return None
    raw_channel_id = channel_id_from_signed_peer_id(signed)
    if raw_channel_id is None:
        return None

    if access_hash is not None:
        try:
            return await client.get_entity(InputPeerChannel(raw_channel_id, access_hash))
        except FloodWaitError:
            raise
        except Exception:
            try:
                return await client.get_entity(PeerChannel(raw_channel_id))
            except FloodWaitError:
                raise
            except Exception as exc:
                logger.info(
                    "Channel entity resolve for first_message_at failed external_id=%s: %s",
                    external_id,
                    exc,
                )
            return InputPeerChannel(raw_channel_id, access_hash)

    handle = (username or "").strip().lstrip("@")
    if handle:
        try:
            return await client.get_entity(handle)
        except FloodWaitError:
            raise
        except Exception as exc:
            logger.info(
                "Username resolve for first_message_at failed external_id=%s @%s: %s",
                external_id,
                handle,
                exc,
            )
    try:
        return await client.get_entity(PeerChannel(raw_channel_id))
    except FloodWaitError:
        raise
    except Exception as exc:
        logger.info(
            "PeerChannel resolve for first_message_at failed external_id=%s: %s",
            external_id,
            exc,
        )
        return None
