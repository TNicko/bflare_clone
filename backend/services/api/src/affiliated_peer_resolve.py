"""Resolve missing affiliated community peers for a user page."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, InputPeerChannel

from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.crud.supabase.peer_rows import community_view_from_peer_row
from services.api.src.crud.supabase.peers import _fetch_peer_row_by_external_id
from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.user import UserAffiliatedPeerView
from telegram.api_session_pool import ApiSessionPool
from telegram.availability import is_forbidden_peer_entity
from telegram.materialize import materialize_channel, materialize_chat
from telegram.utils import channel_id_from_signed_peer_id

logger = logging.getLogger(__name__)


async def _role_access_for_peer(
    conn: Any,
    external_id: str,
) -> tuple[UUID | None, int | None]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT r.harvester_id, r.access_hash
            FROM telegram_harvester_peer_roles r
            INNER JOIN telegram_harvesters h ON h.id = r.harvester_id AND h.is_active = true
            WHERE r.external_id = %s
              AND r.role = 'parent'
              AND EXISTS (
                SELECT 1 FROM telegram_harvester_sessions hs
                WHERE hs.harvester_id = h.id AND hs.auth_state = 'healthy'
              )
            ORDER BY r.updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (external_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None, None
    harvester_id, access_hash = row[0], row[1]
    return (
        harvester_id if isinstance(harvester_id, UUID) else UUID(str(harvester_id)),
        int(access_hash) if access_hash is not None else None,
    )


async def _load_community_view(
    conn: Any,
    *,
    peer_type: PeerTypeSchema,
    external_id: str,
) -> CommunityView | None:
    table = {
        "channel": "telegram_channels",
        "megagroup": "telegram_megagroups",
        "chat": "telegram_chats",
    }.get(peer_type)
    if table is None:
        return None
    row = await _fetch_peer_row_by_external_id(conn, table=table, external_id=external_id)
    if not row:
        return None
    return community_view_from_peer_row(row, peer_type=peer_type)


async def resolve_affiliated_community(
    client: TelegramClient,
    conn: Any,
    *,
    peer_type: PeerTypeSchema,
    external_id: str,
    harvester_id: UUID,
    access_hash: int | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> CommunityView | None:
    existing = await _load_community_view(conn, peer_type=peer_type, external_id=external_id)
    if existing is not None:
        return existing

    try:
        if peer_type == "chat":
            entity = await client.get_entity(int(external_id))
            if is_forbidden_peer_entity(entity) or not isinstance(entity, Chat):
                return None
            await materialize_chat(
                client,
                conn,
                entity,
                harvester_id=harvester_id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
        elif peer_type in ("channel", "megagroup"):
            if access_hash is None:
                return None
            raw_id = channel_id_from_signed_peer_id(int(external_id))
            if raw_id is None:
                return None
            entity = await client.get_entity(InputPeerChannel(raw_id, access_hash))
            if is_forbidden_peer_entity(entity) or not isinstance(entity, Channel):
                return None
            await materialize_channel(
                client,
                conn,
                entity,
                harvester_id=harvester_id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
        else:
            return None
    except FloodWaitError:
        raise
    except Exception as exc:
        logger.info(
            "Affiliated peer resolve failed peer_type=%s external_id=%s: %s",
            peer_type,
            external_id,
            exc,
        )
        return None

    return await _load_community_view(conn, peer_type=peer_type, external_id=external_id)


async def fill_affiliated_peers(
    pool: ApiSessionPool | None,
    conn: Any,
    affiliated: list[UserAffiliatedPeerView],
    *,
    s3_bucket: str = "",
    s3_region: str = "",
) -> list[UserAffiliatedPeerView]:
    """Resolve unknown affiliated peers when possible; keep Unknown stubs on failure.

    Each peer's access_hash is only used with that role's owning harvester client.
    """
    out: list[UserAffiliatedPeerView] = []
    for entry in affiliated:
        peer = entry.peer
        if not peer.is_unknown:
            out.append(entry)
            continue

        resolved: CommunityView | None = None
        if pool is not None:
            try:
                if peer.peer_type == "chat":
                    async with pool.acquire_client_for_peer_role(
                        conn,
                        harvester_id=None,
                        access_hash=None,
                    ) as (client, harvester_id, _):
                        access_hash = None
                        if client is not None and harvester_id is not None:
                            resolved = await resolve_affiliated_community(
                                client,
                                conn,
                                peer_type=peer.peer_type,
                                external_id=entry.external_id,
                                harvester_id=harvester_id,
                                access_hash=access_hash,
                                s3_bucket=s3_bucket,
                                s3_region=s3_region,
                            )
                else:
                    role_hid, access_hash = await _role_access_for_peer(conn, entry.external_id)
                    async with pool.acquire_client_for_peer_role(
                        conn,
                        harvester_id=role_hid,
                        access_hash=access_hash,
                    ) as (client, harvester_id, access_hash):
                        if client is not None and harvester_id is not None:
                            resolved = await resolve_affiliated_community(
                                client,
                                conn,
                                peer_type=peer.peer_type,
                                external_id=entry.external_id,
                                harvester_id=harvester_id,
                                access_hash=access_hash,
                                s3_bucket=s3_bucket,
                                s3_region=s3_region,
                            )
            except FloodWaitError:
                raise
            except Exception:
                logger.exception(
                    "Affiliated peer resolve error external_id=%s",
                    entry.external_id,
                )
                resolved = None

        out.append(
            entry.model_copy(
                update={
                    "peer": resolved
                    or CommunityView.unknown(
                        peer_type=peer.peer_type,
                        external_id=entry.external_id,
                    )
                }
            )
        )
    return out
