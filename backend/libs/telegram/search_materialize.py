"""Materialize peers returned by Telegram ``contacts.search``.

Optional ``on_community`` runs after each successful channel/chat materialize
(e.g. seed first + latest messages on the same session) before the next hit.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, User

from db.connection import rollback_quiet
from telegram.materialize import materialize_channel, materialize_chat, materialize_user, peer_ref
from telegram.utils import to_signed_peer_id

lg = logging.getLogger(__name__)

CommunityHook = Callable[[str, str, int | None], Awaitable[None]]


class TelegramMaterializeStats(BaseModel):
    model_config = ConfigDict(frozen=True)

    peers_seen: int = 0
    peers_persisted: int = 0
    peers_skipped: int = 0
    peers_found: tuple[dict[str, object], ...] = ()


async def materialize_search_peers(
    client: TelegramClient,
    conn: Any,
    *,
    harvester_id: UUID,
    query: str,
    chats: list[Any],
    users: list[Any],
    delay_seconds: float = 1.5,
    s3_bucket: str = "",
    s3_region: str = "",
    on_community: CommunityHook | None = None,
) -> TelegramMaterializeStats:
    seen = 0
    persisted = 0
    skipped = 0
    peer_results: list[dict[str, object]] = []

    for user in users:
        if not isinstance(user, User):
            continue
        seen += 1
        ok = False
        try:
            ok = await materialize_user(client, conn, user, harvester_id=harvester_id)
            if ok:
                persisted += 1
            else:
                skipped += 1
        except FloodWaitError:
            raise
        except Exception:
            lg.exception("Failed materializing user id=%s for query %r", user.id, query)
            await rollback_quiet(conn, context=f"search materialize user id={user.id}")
            skipped += 1

        result = peer_ref(user, persisted=ok)
        if result is not None:
            peer_results.append(result)

        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    for chat in chats:
        seen += 1
        ok = False
        peer_type: str | None = None
        access_hash: int | None = None
        external_id: str | None = None
        try:
            if isinstance(chat, Channel):
                peer_type = "megagroup" if chat.megagroup else "channel"
                access_hash = int(chat.access_hash) if chat.access_hash is not None else None
                signed = to_signed_peer_id(chat)
                external_id = str(signed) if signed is not None else None
                ok = (
                    await materialize_channel(
                        client,
                        conn,
                        chat,
                        harvester_id=harvester_id,
                        s3_bucket=s3_bucket,
                        s3_region=s3_region,
                    )
                ) is not None
            elif isinstance(chat, Chat):
                peer_type = "chat"
                access_hash = None
                signed = to_signed_peer_id(chat)
                external_id = str(signed) if signed is not None else None
                ok = (
                    await materialize_chat(
                        client,
                        conn,
                        chat,
                        harvester_id=harvester_id,
                        s3_bucket=s3_bucket,
                        s3_region=s3_region,
                    )
                ) is not None
            else:
                ok = False
            if ok:
                persisted += 1
            else:
                skipped += 1
        except FloodWaitError:
            raise
        except Exception:
            lg.exception(
                "Failed materializing chat id=%s for query %r",
                getattr(chat, "id", None),
                query,
            )
            await rollback_quiet(
                conn,
                context=f"search materialize chat id={getattr(chat, 'id', None)}",
            )
            skipped += 1

        if ok and on_community is not None and peer_type is not None and external_id is not None:
            try:
                await on_community(external_id, peer_type, access_hash)
            except FloodWaitError:
                raise
            except Exception:
                lg.exception(
                    "Failed post-materialize hook for chat id=%s query %r",
                    external_id,
                    query,
                )
                await rollback_quiet(conn, context=f"search on_community id={external_id}")

        result = peer_ref(chat, persisted=ok)
        if result is not None:
            peer_results.append(result)

        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    return TelegramMaterializeStats(
        peers_seen=seen,
        peers_persisted=persisted,
        peers_skipped=skipped,
        peers_found=tuple(peer_results),
    )
