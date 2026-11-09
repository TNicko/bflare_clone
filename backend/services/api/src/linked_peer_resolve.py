"""On-demand resolve of channel↔discussion linked communities."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from services.api.src.crud.supabase.peer_rows import community_view_from_peer_row
from services.api.src.crud.supabase.peers import _fetch_peer_row_by_external_id
from services.api.src.crud.supabase.views.general.community import CommunityView
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, InputPeerChannel, PeerChannel

from db.schemas.general.primitives import PeerTypeSchema
from telegram.availability import is_forbidden_peer_entity
from telegram.materialize import materialize_channel
from telegram.utils import channel_id_from_signed_peer_id, to_signed_peer_id

logger = logging.getLogger(__name__)


async def load_linked_community_view(
    conn: Any,
    *,
    linked_chat_id: int,
    linked_peer_type: PeerTypeSchema,
) -> CommunityView | None:
    linked_signed = to_signed_peer_id(PeerChannel(linked_chat_id))
    if linked_signed is None:
        return None
    table = "telegram_megagroups" if linked_peer_type == "megagroup" else "telegram_channels"
    linked_row = await _fetch_peer_row_by_external_id(conn, table=table, external_id=str(linked_signed))
    if not linked_row:
        return None
    return community_view_from_peer_row(linked_row, peer_type=linked_peer_type)


async def resolve_linked_community(
    client: TelegramClient,
    conn: Any,
    *,
    parent_external_id: str,
    parent_peer_type: PeerTypeSchema,
    parent_access_hash: int | None,
    linked_chat_id: int,
    linked_peer_type: PeerTypeSchema,
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> CommunityView | None:
    """Materialize a missing linked discussion/channel peer; return CommunityView or None.

    Re-materializes the parent via ``materialize_channel``, which also upserts the
    linked discussion/broadcast peer when Telegram returns it in GetFullChannel.
    """
    existing = await load_linked_community_view(conn, linked_chat_id=linked_chat_id, linked_peer_type=linked_peer_type)
    if existing is not None:
        return existing

    if parent_peer_type not in ("channel", "megagroup"):
        return None
    if parent_access_hash is None:
        logger.info(
            "Linked peer resolve skipped for parent=%s: missing parent access_hash",
            parent_external_id,
        )
        return None

    try:
        signed = int(parent_external_id)
    except ValueError:
        return None
    raw_channel_id = channel_id_from_signed_peer_id(signed)
    if raw_channel_id is None:
        return None

    try:
        parent_entity = await client.get_entity(InputPeerChannel(raw_channel_id, parent_access_hash))
    except FloodWaitError:
        raise
    except Exception as exc:
        logger.warning(
            "Linked peer parent resolve failed parent=%s: %s",
            parent_external_id,
            exc,
        )
        return None

    if is_forbidden_peer_entity(parent_entity) or not isinstance(parent_entity, Channel):
        return None

    try:
        await materialize_channel(
            client,
            conn,
            parent_entity,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
    except FloodWaitError:
        raise
    except Exception as exc:
        logger.warning(
            "Materialize parent for linked peer chat_id=%s failed: %s",
            linked_chat_id,
            exc,
        )
        return None

    return await load_linked_community_view(conn, linked_chat_id=linked_chat_id, linked_peer_type=linked_peer_type)
