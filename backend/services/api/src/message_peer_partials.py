"""Collect + resolve community peers referenced by chat messages (senders / forwards)."""

from __future__ import annotations

import logging
from typing import Any, Literal
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, InputPeerChannel, MessageEmpty, PeerChannel

from services.api.src.crud.supabase.peers import (
    get_partial_communities_by_external_ids,
)
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from telegram.availability import is_forbidden_peer_entity
from telegram.harvester_peer_access import is_harvester_peer_inaccessible_error
from telegram.materialize import materialize_channel, materialize_chat, light_upsert_telethon_channel
from telegram.utils import (
    channel_id_from_signed_peer_id,
    signed_peer_id_from_raw_channel_id,
    to_signed_peer_id,
)

lg = logging.getLogger(__name__)

ResolveAttempt = Literal["ok", "inaccessible", "unavailable"]


def is_channel_external_id(external_id: str | None) -> bool:
    if not external_id:
        return False
    try:
        return channel_id_from_signed_peer_id(int(external_id)) is not None
    except (TypeError, ValueError):
        return False


def signed_id_from_fwd_peer(from_id: Any) -> str | None:
    if not isinstance(from_id, dict):
        return None
    tl = from_id.get("tl")
    if tl == "PeerChannel" and isinstance(from_id.get("channel_id"), int):
        return str(signed_peer_id_from_raw_channel_id(int(from_id["channel_id"])))
    if tl == "PeerChat" and isinstance(from_id.get("chat_id"), int):
        chat_id = int(from_id["chat_id"])
        return str(-chat_id if chat_id > 0 else chat_id)
    return None


def community_external_ids_from_messages(
    messages: list[TelegramMessageView],
) -> list[str]:
    """Unique channel/megagroup/chat ids from ``from_external_id`` and ``fwd_from``."""
    ids: set[str] = set()
    for message in messages:
        if is_channel_external_id(message.from_external_id):
            ids.add(str(message.from_external_id))
        fwd = message.fwd_from
        if not isinstance(fwd, dict):
            continue
        for key in ("from_id", "saved_from_peer"):
            signed = signed_id_from_fwd_peer(fwd.get(key))
            if signed:
                ids.add(signed)
    return sorted(ids)


def _user_id_str(value: Any) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return str(value)
    if isinstance(value, str) and value.strip().isdigit():
        n = int(value.strip())
        return str(n) if n > 0 else None
    return None


def user_external_ids_from_action_data(action_data: Any) -> list[str]:
    """User ids from MessageAction payloads (add/leave/create/invite, etc.)."""
    if not isinstance(action_data, dict):
        return []
    ids: list[str] = []
    users = action_data.get("users")
    if isinstance(users, list):
        for item in users:
            uid = _user_id_str(item)
            if uid:
                ids.append(uid)
    uid = _user_id_str(action_data.get("user_id"))
    if uid:
        ids.append(uid)
    return ids


def _user_external_id_from_fwd(fwd: Any) -> str | None:
    """Telegram user id on ``fwd_from.from_id`` when the forward source is a user."""
    if not isinstance(fwd, dict):
        return None
    from_id = fwd.get("from_id")
    if not isinstance(from_id, dict) or from_id.get("tl") != "PeerUser":
        return None
    return _user_id_str(from_id.get("user_id"))


def user_external_ids_from_messages(
    messages: list[TelegramMessageView],
) -> list[str]:
    """Unique user ids from message senders, forward headers, and action payloads."""
    ids: set[str] = set()
    for message in messages:
        if message.from_external_id and not is_channel_external_id(message.from_external_id):
            ids.add(str(message.from_external_id))
        fwd_user = _user_external_id_from_fwd(message.fwd_from)
        if fwd_user:
            ids.add(fwd_user)
        for uid in user_external_ids_from_action_data(message.action_data):
            ids.add(uid)
    return sorted(ids)


async def _role_access_hash_for_harvester(
    conn: Any,
    external_id: str,
    harvester_id: UUID,
) -> int | None:
    """Return access_hash only for the harvester whose client will use it."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT r.access_hash
            FROM telegram_harvester_peer_roles r
            WHERE r.external_id = %s
              AND r.harvester_id = %s
              AND r.access_hash IS NOT NULL
            LIMIT 1
            """,
            (external_id, harvester_id),
        )
        row = await cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


async def _resolve_one_channel_peer(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> ResolveAttempt:
    """Telethon resolve + materialize one channel/megagroup."""
    try:
        signed = int(external_id)
    except (TypeError, ValueError):
        return "unavailable"
    raw_id = channel_id_from_signed_peer_id(signed)
    if raw_id is None:
        return "unavailable"
    entity: Channel | None = None
    try:
        access_hash = await _role_access_hash_for_harvester(conn, external_id, harvester_id)
        if access_hash is not None:
            entity = await client.get_entity(InputPeerChannel(raw_id, access_hash))
        else:
            # Often works right after get_messages (entity cache / peers in updates).
            entity = await client.get_entity(PeerChannel(raw_id))
    except FloodWaitError:
        raise
    except (RPCError, ValueError, TypeError) as exc:
        if is_harvester_peer_inaccessible_error(exc):
            lg.info(
                "Community peer inaccessible external_id=%s: %s",
                external_id,
                exc,
            )
            return "inaccessible"
        lg.info(
            "Could not resolve community peer external_id=%s: %s",
            external_id,
            exc,
        )
        return "unavailable"
    except Exception:
        lg.exception("Unexpected error resolving community peer external_id=%s", external_id)
        return "unavailable"

    if not isinstance(entity, Channel) or is_forbidden_peer_entity(entity):
        return "inaccessible" if is_forbidden_peer_entity(entity) else "unavailable"
    try:
        await materialize_channel(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            role="child",
            s3_bucket=s3_bucket,
            s3_region=s3_region,
            materialize_linked=False,
        )
        return "ok"
    except FloodWaitError:
        raise
    except Exception as exc:
        if is_harvester_peer_inaccessible_error(exc):
            lg.info(
                "Community peer materialize inaccessible external_id=%s: %s",
                external_id,
                exc,
            )
            return "inaccessible"
        lg.exception("Failed to materialize community peer external_id=%s", external_id)
        return "unavailable"


async def _resolve_one_chat_peer(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> ResolveAttempt:
    """Telethon resolve + materialize one basic group chat."""
    try:
        signed = int(external_id)
    except (TypeError, ValueError):
        return "unavailable"
    # Channel-shaped ids are handled separately; basic chats are negative without -100 mark.
    if channel_id_from_signed_peer_id(signed) is not None or signed >= 0:
        return "unavailable"
    try:
        entity = await client.get_entity(signed)
    except FloodWaitError:
        raise
    except (RPCError, ValueError, TypeError) as exc:
        if is_harvester_peer_inaccessible_error(exc):
            lg.info(
                "Chat peer inaccessible external_id=%s: %s",
                external_id,
                exc,
            )
            return "inaccessible"
        lg.info(
            "Could not resolve chat peer external_id=%s: %s",
            external_id,
            exc,
        )
        return "unavailable"
    except Exception:
        lg.exception("Unexpected error resolving chat peer external_id=%s", external_id)
        return "unavailable"

    if not isinstance(entity, Chat) or is_forbidden_peer_entity(entity):
        return "inaccessible" if is_forbidden_peer_entity(entity) else "unavailable"
    try:
        await materialize_chat(
            client,
            conn,
            entity,
            harvester_id=harvester_id,
            role="child",
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        return "ok"
    except FloodWaitError:
        raise
    except Exception as exc:
        if is_harvester_peer_inaccessible_error(exc):
            return "inaccessible"
        lg.exception("Failed to materialize chat peer external_id=%s", external_id)
        return "unavailable"


async def resolve_missing_community_peers(
    client: TelegramClient,
    conn: Any,
    *,
    missing_external_ids: list[str],
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> None:
    """Best-effort Telethon resolve + materialize for community ids not yet in DB."""
    for external_id in missing_external_ids:
        await _resolve_one_channel_peer(
            client,
            conn,
            external_id=external_id,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )


async def find_forward_sample_message_id(
    conn: Any,
    *,
    source_external_id: str,
    target_external_id: str,
) -> int | None:
    """Return a recent message id in ``source`` whose ``fwd_from`` points at ``target``."""
    try:
        target_signed = int(target_external_id)
    except (TypeError, ValueError):
        return None
    raw_channel_id = channel_id_from_signed_peer_id(target_signed)
    # PeerChat ids in TL are often positive; we store signed negative.
    chat_tl_id = -target_signed if target_signed < 0 else target_signed

    async with conn.cursor() as cur:
        if raw_channel_id is not None:
            await cur.execute(
                """
                SELECT message_id
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND COALESCE(deleted, false) = false
                  AND fwd_from IS NOT NULL
                  AND (
                    (
                      fwd_from->'from_id'->>'tl' = 'PeerChannel'
                      AND (fwd_from->'from_id'->>'channel_id') ~ '^[0-9]+$'
                      AND (fwd_from->'from_id'->>'channel_id')::bigint = %s
                    )
                    OR (
                      fwd_from->'saved_from_peer'->>'tl' = 'PeerChannel'
                      AND (fwd_from->'saved_from_peer'->>'channel_id') ~ '^[0-9]+$'
                      AND (fwd_from->'saved_from_peer'->>'channel_id')::bigint = %s
                    )
                  )
                ORDER BY date DESC NULLS LAST
                LIMIT 1
                """,
                (source_external_id, raw_channel_id, raw_channel_id),
            )
        else:
            await cur.execute(
                """
                SELECT message_id
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND COALESCE(deleted, false) = false
                  AND fwd_from IS NOT NULL
                  AND (
                    (
                      fwd_from->'from_id'->>'tl' = 'PeerChat'
                      AND (fwd_from->'from_id'->>'chat_id') ~ '^[0-9]+$'
                      AND (fwd_from->'from_id'->>'chat_id')::bigint = %s
                    )
                    OR (
                      fwd_from->'saved_from_peer'->>'tl' = 'PeerChat'
                      AND (fwd_from->'saved_from_peer'->>'chat_id') ~ '^[0-9]+$'
                      AND (fwd_from->'saved_from_peer'->>'chat_id')::bigint = %s
                    )
                  )
                ORDER BY date DESC NULLS LAST
                LIMIT 1
                """,
                (source_external_id, chat_tl_id, chat_tl_id),
            )
        row = await cur.fetchone()
    if not row:
        return None
    mid = row[0] if not isinstance(row, dict) else row.get("message_id")
    try:
        return int(mid) if mid is not None else None
    except (TypeError, ValueError):
        return None


async def find_any_forward_via_for_target(
    conn: Any,
    *,
    target_external_id: str,
) -> tuple[str, int] | None:
    """Deprecated: do not use on hot paths.

    Unscoped ``fwd_from`` scans over partitioned ``telegram_messages`` time out.
    Prefer ``find_forward_sample_message_id`` with a known ``source_external_id``.
    """
    _ = (conn, target_external_id)
    return None


async def resolve_community_peer_by_external_id(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> tuple[KeywordPeerView | None, ResolveAttempt]:
    """Resolve one community by signed external_id; return DB partial + attempt status."""
    existing = await get_partial_communities_by_external_ids(conn, [external_id])
    if external_id in existing:
        return existing[external_id], "ok"

    try:
        signed = int(external_id)
    except (TypeError, ValueError):
        return None, "unavailable"

    if channel_id_from_signed_peer_id(signed) is not None:
        attempt = await _resolve_one_channel_peer(
            client,
            conn,
            external_id=external_id,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
    elif signed < 0:
        attempt = await _resolve_one_chat_peer(
            client,
            conn,
            external_id=external_id,
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
    else:
        return None, "unavailable"

    if attempt != "ok":
        return None, attempt

    peers = await get_partial_communities_by_external_ids(conn, [external_id])
    peer = peers.get(external_id)
    return peer, ("ok" if peer is not None else "unavailable")


async def materialize_channels_from_telethon_messages(
    client: TelegramClient,
    conn: Any,
    messages: list[Any],
    *,
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> int:
    """Persist Channel entities attached to Telethon messages (sender / forward chat)."""
    seen: set[int] = set()
    count = 0
    for msg in messages:
        if msg is None or isinstance(msg, MessageEmpty):
            continue
        if getattr(msg, "id", None) is None:
            continue
        candidates: list[Any] = []
        sender = getattr(msg, "sender", None)
        if sender is not None:
            candidates.append(sender)
        forward = getattr(msg, "forward", None)
        if forward is not None:
            chat = getattr(forward, "chat", None)
            if chat is None and hasattr(forward, "get_chat"):
                try:
                    chat = await forward.get_chat()
                except FloodWaitError:
                    raise
                except Exception as exc:
                    lg.info("forward.get_chat failed: %s", exc)
                    chat = None
            if chat is not None:
                candidates.append(chat)
            sender_chat = getattr(forward, "sender", None)
            if sender_chat is None and hasattr(forward, "get_sender"):
                try:
                    sender_chat = await forward.get_sender()
                except FloodWaitError:
                    raise
                except Exception:
                    sender_chat = None
            if sender_chat is not None:
                candidates.append(sender_chat)

        for entity in candidates:
            if not isinstance(entity, Channel):
                continue
            signed = to_signed_peer_id(entity)
            if signed is None or signed in seen:
                continue
            seen.add(signed)
            existing = await get_partial_communities_by_external_ids(conn, [str(signed)])
            if str(signed) in existing:
                continue
            try:
                await materialize_channel(
                    client,
                    conn,
                    entity,
                    harvester_id=harvester_id,
                    role="child",
                    s3_bucket=s3_bucket,
                    s3_region=s3_region,
                    materialize_linked=False,
                )
                # Full profile or tg_min stub (private / no getFullChannel).
                count += 1
            except FloodWaitError:
                raise
            except Exception as exc:
                if is_harvester_peer_inaccessible_error(exc):
                    # Should be rare now (getFullChannel catches these); still keep stub.
                    await light_upsert_telethon_channel(
                        conn,
                        entity,
                        harvester_id=harvester_id,
                        role="child",
                    )
                    lg.info(
                        "Skip full materialize for inaccessible message-attached channel "
                        "id=%s title=%r username=%r: %s (light-upserted)",
                        signed,
                        getattr(entity, "title", None),
                        getattr(entity, "username", None),
                        type(exc).__name__,
                    )
                    count += 1
                    continue
                lg.exception(
                    "Failed to materialize message-attached channel id=%s title=%r",
                    signed,
                    getattr(entity, "title", None),
                )
    return count


async def load_community_partials_for_messages(
    conn: Any,
    messages: list[TelegramMessageView],
) -> dict[str, KeywordPeerView]:
    ids = community_external_ids_from_messages(messages)
    if not ids:
        return {}
    return await get_partial_communities_by_external_ids(conn, ids)
