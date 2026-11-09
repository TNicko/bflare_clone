import logging

from typing import Any
from uuid import UUID
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat

from db.crud.telegram.harvester import clear_harvester_peer_access_error, upsert_harvester_peer_role
from db.schemas.general.primitives import TelegramPeerTypeSchema
from jobs.fetch_messages.db import PeerAssignmentRow, PeerSelectionRow, fetch_peer_username
from telegram.utils import to_signed_peer_id

lg = logging.getLogger(__name__)


def _peer_type_for_entity(entity: Channel | Chat) -> TelegramPeerTypeSchema:
    if isinstance(entity, Channel):
        return "megagroup" if entity.megagroup else "channel"
    return "chat"


async def resolve_peer_harvester_role(
    client: TelegramClient,
    conn: Any,
    *,
    row: PeerSelectionRow,
    harvester_id: UUID,
    harvester_external_id: str,
) -> PeerAssignmentRow | None:
    """Resolve a peer via Telethon and upsert ``telegram_harvester_peer_roles`` (``role=parent``).

    Does **not** call ``get_entity(bare_id)`` for channels/megagroups. If this harvester
    already knew the peer, the access_hash should be in DB; bare-id resolve only hits
    Telethon's session cache and is redundant with our role table. Discovery paths are:
    stored ``row.access_hash``, ``@username``, or (elsewhere) invite / via-forward seeding.
    """

    entity = None
    peer_type_hint = row.peer_type if row.peer_type in ("channel", "megagroup", "chat") else "channel"

    # access_hash is per Telegram account. Only use a stored hash when it belongs
    # to *this* harvester — foreign hashes cause ChannelInvalidError and get
    # re-upserted onto the wrong role row.
    owned_access_hash = row.access_hash
    if owned_access_hash is not None and row.harvester_id is not None and row.harvester_id != harvester_id:
        lg.info(
            "Ignoring foreign access_hash for peer %s (role harvester=%s ≠ %s)",
            row.peer_external_id,
            row.harvester_id,
            harvester_id,
        )
        owned_access_hash = None
    elif owned_access_hash is not None and row.harvester_id is None:
        # Ambiguous ownership (synthetic selection) — never trust the hash.
        owned_access_hash = None

    if owned_access_hash is not None and peer_type_hint in ("channel", "megagroup"):
        from telethon.tl.types import InputPeerChannel
        from telegram.utils import channel_id_from_signed_peer_id

        try:
            signed = int(row.peer_external_id)
        except (TypeError, ValueError):
            return None
        raw_channel_id = channel_id_from_signed_peer_id(signed)
        if raw_channel_id is None:
            return None
        try:
            entity = await client.get_entity(InputPeerChannel(raw_channel_id, int(owned_access_hash)))
        except FloodWaitError:
            raise
        except Exception as exc:
            lg.warning(
                "Failed to resolve peer %s with stored access_hash: %s",
                row.peer_external_id,
                exc,
            )
            return None
    elif peer_type_hint == "chat":
        # Basic groups have no access_hash; id resolve is valid.
        try:
            entity = await client.get_entity(int(row.peer_external_id))
        except FloodWaitError:
            raise
        except Exception as exc:
            lg.debug(
                "get_entity(chat %s) failed, will try @username if present: %s",
                row.peer_external_id,
                exc,
            )
            entity = None

    if entity is None or not isinstance(entity, (Channel, Chat)):
        username = await fetch_peer_username(conn, peer_external_id=row.peer_external_id)
        if not username:
            lg.info(
                "Skip bare-id resolve for %s (%s) — no DB access_hash and no username",
                row.peer_external_id,
                peer_type_hint,
            )
            return None
        try:
            entity = await client.get_entity(username)
        except FloodWaitError:
            raise
        except Exception as exc:
            lg.warning(
                "Failed to resolve peer %s (%s) with telegram: %s",
                row.peer_external_id,
                row.peer_type,
                exc,
            )
            return None

    if not isinstance(entity, (Channel, Chat)):
        lg.warning(
            "Peer %s resolved to unsupported entity type %s",
            row.peer_external_id,
            type(entity).__name__,
        )
        return None

    signed_peer_id = to_signed_peer_id(entity)
    if signed_peer_id is None or str(signed_peer_id) != row.peer_external_id:
        lg.warning(
            "Peer %s resolved to mismatched external_id %s",
            row.peer_external_id,
            signed_peer_id,
        )
        return None

    peer_type = _peer_type_for_entity(entity)
    access_hash = getattr(entity, "access_hash", None) if isinstance(entity, Channel) else None

    if peer_type in ("channel", "megagroup") and access_hash is None:
        lg.warning(
            "Peer %s (%s) resolved but Telethon returned no access_hash",
            row.peer_external_id,
            peer_type,
        )
        return None

    async with conn.transaction():
        await upsert_harvester_peer_role(
            conn,
            harvester_id=harvester_id,
            external_id=row.peer_external_id,
            peer_type=peer_type,
            access_hash=access_hash,
            role="parent",
        )
        await clear_harvester_peer_access_error(
            conn,
            harvester_id=harvester_id,
            external_id=row.peer_external_id,
        )
    await conn.commit()

    lg.info(
        "Resolved peer %s (%s) → harvester %s",
        row.peer_external_id,
        peer_type,
        harvester_external_id,
    )
    return PeerAssignmentRow(
        peer_external_id=row.peer_external_id,
        peer_type=peer_type,
        access_hash=access_hash,
        harvester_id=harvester_id,
        harvester_external_id=harvester_external_id,
    )
