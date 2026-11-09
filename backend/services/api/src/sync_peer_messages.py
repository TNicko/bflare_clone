"""On-demand sync of latest peer messages from Telegram into Postgres."""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from telethon import TelegramClient, utils as tg_utils
from telethon.errors import FloodWaitError
from telethon.tl import functions
from telethon.tl.types import InputPeerChannel, MessageEmpty, PeerChat

from db.crud.telegram.forward_sources import persist_forward_sources_from_message_batch
from db.crud.telegram.message import mark_missing_messages_deleted, upsert_messages
from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.message import TelegramMessageSchema
from services.api.src.message_peer_partials import materialize_channels_from_telethon_messages
from telegram.mappers import message_schema_from_telethon_message
from telegram.materialize import light_upsert_telethon_channels, light_upsert_telethon_users
from telegram.utils import channel_id_from_signed_peer_id

lg = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PeerMessageSyncResult:
    upserted: int
    message_dates: list[datetime] = field(default_factory=list)
    #: True when this sync exhausted the requested tip (safe to claim coverage).
    tip_complete: bool = False
    #: Oldest Telegram message id from this older-page / refresh sync (for client loops).
    oldest_message_id: int | None = None
    #: True when an older-page sync reached Telegram's beginning for this peer.
    reached_beginning: bool = False
    #: True when a recent-refresh walk hit ``since`` (or the chat beginning).
    refresh_complete: bool = False
    #: Message ids newly marked ``deleted`` during a recent-refresh page.
    deleted_message_ids: list[int] = field(default_factory=list)


def entity_for_peer(
    *,
    peer_external_id: str,
    peer_type: str,
    access_hash: int | None,
) -> InputPeerChannel | PeerChat:
    signed = int(peer_external_id)
    if peer_type in ("channel", "megagroup"):
        raw_channel_id = channel_id_from_signed_peer_id(signed)
        if raw_channel_id is None:
            raise ValueError(f"Peer external_id={peer_external_id!r} is not a channel/megagroup")
        if access_hash is None:
            raise ValueError(f"Missing access_hash for peer {peer_external_id}")
        return InputPeerChannel(raw_channel_id, access_hash)
    if peer_type == "chat":
        chat_id = -signed if signed < 0 else signed
        return PeerChat(chat_id)
    raise ValueError(f"Unsupported peer_type={peer_type!r}")


@dataclass(frozen=True, slots=True)
class HistoryFetchPage:
    messages: list[Any]
    chats: list[Any] = field(default_factory=list)
    users: list[Any] = field(default_factory=list)


def _messages_from_history_result(
    client: TelegramClient,
    result: Any,
    entity: Any,
) -> list[Any]:
    if result is None or not hasattr(result, "messages"):
        return []
    chats = getattr(result, "chats", None) or []
    users = getattr(result, "users", None) or []
    entities = {tg_utils.get_peer_id(x): x for x in itertools.chain(users, chats)}
    out: list[Any] = []
    for message in result.messages:
        if message is None or isinstance(message, MessageEmpty):
            continue
        if getattr(message, "id", None) is None:
            continue
        message._finish_init(client, entities, entity)
        out.append(message)
    return out


async def _light_upsert_history_chats(
    conn: Any,
    chats: list[Any],
    *,
    harvester_id: UUID | None,
) -> int:
    if harvester_id is None or not chats:
        return 0
    try:
        return await light_upsert_telethon_channels(
            conn,
            chats,
            harvester_id=harvester_id,
        )
    except Exception:
        lg.exception("Failed light-upserting chats[] from message history")
        return 0


async def _light_upsert_history_users(
    conn: Any,
    users: list[Any],
    *,
    harvester_id: UUID | None,
) -> int:
    if harvester_id is None or not users:
        return 0
    try:
        return await light_upsert_telethon_users(
            conn,
            users,
            harvester_id=harvester_id,
        )
    except Exception:
        lg.exception("Failed light-upserting users[] from message history")
        return 0


async def _light_upsert_history_entities(
    conn: Any,
    *,
    chats: list[Any],
    users: list[Any],
    harvester_id: UUID | None,
) -> tuple[int, int]:
    linked_chats = await _light_upsert_history_chats(conn, chats, harvester_id=harvester_id)
    linked_users = await _light_upsert_history_users(conn, users, harvester_id=harvester_id)
    return linked_chats, linked_users


async def _get_history_messages(
    client: TelegramClient,
    entity: Any,
    *,
    limit: int,
    offset_id: int = 0,
    min_id: int = 0,
) -> HistoryFetchPage:
    """``messages.GetHistory`` including sibling ``chats[]`` / ``users[]`` stubs."""
    result = await client(
        functions.messages.GetHistoryRequest(
            peer=entity,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=min_id,
            hash=0,
        )
    )
    chats = list(getattr(result, "chats", None) or [])
    users = list(getattr(result, "users", None) or [])
    return HistoryFetchPage(
        messages=_messages_from_history_result(client, result, entity),
        chats=chats,
        users=users,
    )


def _usable_messages(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raw = [raw]
    out: list[Any] = []
    for msg in raw:
        if msg is None or isinstance(msg, MessageEmpty):
            continue
        if getattr(msg, "id", None) is None:
            continue
        out.append(msg)
    return out


async def _refresh_forward_sources_snapshot(
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    harvester_id: UUID | None,
    schemas: list[TelegramMessageSchema],
) -> None:
    if harvester_id is None or not schemas:
        return
    await persist_forward_sources_from_message_batch(
        conn,
        messages=schemas,
        source_external_id=external_id,
        from_peer_type=peer_type,
        harvester_id=harvester_id,
    )


async def fetch_and_upsert_latest_messages(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    access_hash: int | None,
    harvester_external_id: str | None,
    limit: int,
    after_message_id: int | None = None,
    harvester_id: UUID | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> PeerMessageSyncResult:
    """Pull latest (or newer-than) messages from Telegram and upsert."""
    entity = entity_for_peer(
        peer_external_id=external_id,
        peer_type=peer_type,
        access_hash=access_hash,
    )

    try:
        page = await _get_history_messages(
            client,
            entity,
            limit=limit,
            offset_id=0,
            min_id=after_message_id or 0,
        )
    except FloodWaitError:
        raise

    linked_chats, linked_users = await _light_upsert_history_entities(
        conn,
        chats=page.chats,
        users=page.users,
        harvester_id=harvester_id,
    )
    raw = _usable_messages(page.messages)
    if not raw:
        return PeerMessageSyncResult(upserted=0, tip_complete=True)

    schemas = []
    dates: list[datetime] = []
    for msg in raw:
        schemas.append(
            message_schema_from_telethon_message(
                msg,
                source_external_id=external_id,
                peer_type=peer_type,
                harvester_external_id=harvester_external_id,
            )
        )
        if getattr(msg, "date", None) is not None:
            dates.append(msg.date)

    if not schemas:
        # Empty tip (nothing newer, or empty chat) — treat as complete.
        return PeerMessageSyncResult(upserted=0, tip_complete=True)

    if harvester_id is not None:
        try:
            await materialize_channels_from_telethon_messages(
                client,
                conn,
                raw,
                harvester_id=harvester_id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
        except FloodWaitError:
            raise
        except Exception:
            lg.exception(
                "Failed materializing forward/sender channels for peer=%s",
                external_id,
            )

    await upsert_messages(conn, schemas)
    await _refresh_forward_sources_snapshot(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        harvester_id=harvester_id,
        schemas=schemas,
    )

    # Latest-N from the head is a contiguous tip. Incremental min_id pages are
    # complete only when fewer than ``limit`` rows came back.
    if after_message_id is None:
        tip_complete = True
    else:
        tip_complete = len(schemas) < limit

    lg.info(
        "Synced peer messages external_id=%s peer_type=%s upserted=%s "
        "after_message_id=%s tip_complete=%s linked_chats=%s linked_users=%s",
        external_id,
        peer_type,
        len(schemas),
        after_message_id,
        tip_complete,
        linked_chats,
        linked_users,
    )
    return PeerMessageSyncResult(
        upserted=len(schemas),
        message_dates=dates,
        tip_complete=tip_complete,
    )


TELEGRAM_HISTORY_PAGE = 100


async def fetch_and_upsert_older_messages(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    access_hash: int | None,
    harvester_external_id: str | None,
    limit: int,
    before_message_id: int,
    harvester_id: UUID | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> PeerMessageSyncResult:
    """Pull older pages (ids strictly below ``before_message_id``) and upsert.

    Telegram caps each GetHistory at 100 messages. When ``limit`` is larger, this
    loops multiple Telethon pages in one call so the live UI can backfill faster
    with fewer HTTP round-trips.
    """
    if before_message_id <= 0:
        return PeerMessageSyncResult(upserted=0, reached_beginning=True)

    entity = entity_for_peer(
        peer_external_id=external_id,
        peer_type=peer_type,
        access_hash=access_hash,
    )

    target = max(1, limit)
    before = before_message_id
    total_upserted = 0
    all_dates: list[datetime] = []
    oldest_id: int | None = None
    reached_beginning = False
    linked_chats = 0
    linked_users = 0

    while total_upserted < target:
        page_limit = min(TELEGRAM_HISTORY_PAGE, target - total_upserted)
        try:
            page = await _get_history_messages(
                client,
                entity,
                limit=page_limit,
                offset_id=before,
            )
        except FloodWaitError:
            # Keep any pages already upserted (autocommit); caller/client can
            # continue from oldest_message_id after backing off.
            if total_upserted > 0:
                lg.warning(
                    "Older sync flood-wait after partial progress peer=%s upserted=%s oldest=%s",
                    external_id,
                    total_upserted,
                    oldest_id,
                )
                break
            raise

        page_chats, page_users = await _light_upsert_history_entities(
            conn,
            chats=page.chats,
            users=page.users,
            harvester_id=harvester_id,
        )
        linked_chats += page_chats
        linked_users += page_users
        raw = _usable_messages(page.messages)
        if not raw:
            reached_beginning = True
            break

        schemas = []
        dates: list[datetime] = []
        ids: list[int] = []
        for msg in raw:
            mid = getattr(msg, "id", None)
            if mid is None:
                continue
            schemas.append(
                message_schema_from_telethon_message(
                    msg,
                    source_external_id=external_id,
                    peer_type=peer_type,
                    harvester_external_id=harvester_external_id,
                )
            )
            ids.append(int(mid))
            if getattr(msg, "date", None) is not None:
                dates.append(msg.date)

        if not schemas:
            reached_beginning = True
            break

        if harvester_id is not None:
            try:
                await materialize_channels_from_telethon_messages(
                    client,
                    conn,
                    raw,
                    harvester_id=harvester_id,
                    s3_bucket=s3_bucket,
                    s3_region=s3_region,
                )
            except FloodWaitError:
                # Prefer progress over channel enrichment if Telegram rate-limits.
                await upsert_messages(conn, schemas)
                await _refresh_forward_sources_snapshot(
                    conn,
                    external_id=external_id,
                    peer_type=peer_type,
                    harvester_id=harvester_id,
                    schemas=schemas,
                )
                total_upserted += len(schemas)
                all_dates.extend(dates)
                page_oldest = min(ids)
                oldest_id = page_oldest if oldest_id is None else min(oldest_id, page_oldest)
                lg.warning(
                    "Older sync flood-wait during channel materialize peer=%s upserted=%s oldest=%s",
                    external_id,
                    total_upserted,
                    oldest_id,
                )
                break
            except Exception:
                lg.exception(
                    "Failed materializing forward/sender channels for older sync peer=%s",
                    external_id,
                )

        await upsert_messages(conn, schemas)
        await _refresh_forward_sources_snapshot(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            harvester_id=harvester_id,
            schemas=schemas,
        )
        total_upserted += len(schemas)
        all_dates.extend(dates)
        page_oldest = min(ids)
        oldest_id = page_oldest if oldest_id is None else min(oldest_id, page_oldest)

        if len(schemas) < page_limit:
            reached_beginning = True
            break

        # Next page: strictly older than what we just fetched.
        if page_oldest >= before:
            break
        before = page_oldest

    lg.info(
        "Synced older peer messages external_id=%s peer_type=%s upserted=%s "
        "before_message_id=%s oldest_message_id=%s reached_beginning=%s "
        "linked_chats=%s linked_users=%s",
        external_id,
        peer_type,
        total_upserted,
        before_message_id,
        oldest_id,
        reached_beginning,
        linked_chats,
        linked_users,
    )
    return PeerMessageSyncResult(
        upserted=total_upserted,
        message_dates=all_dates,
        oldest_message_id=oldest_id,
        reached_beginning=reached_beginning,
    )


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def fetch_and_refresh_recent_messages(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    access_hash: int | None,
    harvester_external_id: str | None,
    limit: int,
    since_days: int = 30,
    before_message_id: int | None = None,
    harvester_id: UUID | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> PeerMessageSyncResult:
    """Re-fetch recent history for edit/delete freshness.

    Pages ``GetHistory`` newest→older until ``since`` (``now - since_days``), the
    chat beginning, or ``limit`` messages. Each page upserts live rows, then marks
    DB ids in that page's id span missing from Telegram as ``deleted``. The API
    router records fetch coverage from ``message_dates`` after each page (and
    seals ``[now − since_days, now)`` when ``refresh_complete``).
    """
    since = datetime.now(timezone.utc) - timedelta(days=max(1, since_days))
    entity = entity_for_peer(
        peer_external_id=external_id,
        peer_type=peer_type,
        access_hash=access_hash,
    )

    target = max(1, limit)
    # offset_id=0 = tip; else resume strictly older than before_message_id.
    before = before_message_id if before_message_id and before_message_id > 0 else 0
    total_upserted = 0
    all_dates: list[datetime] = []
    oldest_id: int | None = None
    deleted_ids: list[int] = []
    refresh_complete = False
    linked_chats = 0
    linked_users = 0

    while total_upserted < target:
        page_limit = min(TELEGRAM_HISTORY_PAGE, target - total_upserted)
        try:
            page = await _get_history_messages(
                client,
                entity,
                limit=page_limit,
                offset_id=before,
            )
        except FloodWaitError:
            if total_upserted > 0 or deleted_ids:
                lg.warning(
                    "Recent refresh flood-wait after partial progress peer=%s upserted=%s oldest=%s",
                    external_id,
                    total_upserted,
                    oldest_id,
                )
                break
            raise

        page_chats, page_users = await _light_upsert_history_entities(
            conn,
            chats=page.chats,
            users=page.users,
            harvester_id=harvester_id,
        )
        linked_chats += page_chats
        linked_users += page_users
        raw = _usable_messages(page.messages)
        if not raw:
            refresh_complete = True
            break

        in_window: list[Any] = []
        hit_since = False
        for msg in raw:
            msg_date = getattr(msg, "date", None)
            if msg_date is not None and _as_utc(msg_date) < since:
                hit_since = True
                continue
            in_window.append(msg)

        if not in_window:
            refresh_complete = True
            break

        schemas = []
        dates: list[datetime] = []
        ids: list[int] = []
        for msg in in_window:
            mid = getattr(msg, "id", None)
            if mid is None:
                continue
            schemas.append(
                message_schema_from_telethon_message(
                    msg,
                    source_external_id=external_id,
                    peer_type=peer_type,
                    harvester_external_id=harvester_external_id,
                )
            )
            ids.append(int(mid))
            if getattr(msg, "date", None) is not None:
                dates.append(msg.date)

        if not schemas:
            refresh_complete = True
            break

        if harvester_id is not None:
            try:
                await materialize_channels_from_telethon_messages(
                    client,
                    conn,
                    in_window,
                    harvester_id=harvester_id,
                    s3_bucket=s3_bucket,
                    s3_region=s3_region,
                )
            except FloodWaitError:
                await upsert_messages(conn, schemas)
                await _refresh_forward_sources_snapshot(
                    conn,
                    external_id=external_id,
                    peer_type=peer_type,
                    harvester_id=harvester_id,
                    schemas=schemas,
                )
                total_upserted += len(schemas)
                all_dates.extend(dates)
                page_oldest = min(ids)
                page_newest = max(ids)
                oldest_id = page_oldest if oldest_id is None else min(oldest_id, page_oldest)
                try:
                    page_deleted = await mark_missing_messages_deleted(
                        conn,
                        source_external_id=external_id,
                        id_lo=page_oldest,
                        id_hi=page_newest,
                        seen_message_ids=ids,
                        since=since,
                    )
                    deleted_ids.extend(page_deleted)
                except Exception:
                    lg.exception(
                        "Failed delete reconcile during refresh peer=%s",
                        external_id,
                    )
                lg.warning(
                    "Recent refresh flood-wait during channel materialize peer=%s upserted=%s oldest=%s",
                    external_id,
                    total_upserted,
                    oldest_id,
                )
                break
            except Exception:
                lg.exception(
                    "Failed materializing forward/sender channels for refresh peer=%s",
                    external_id,
                )

        await upsert_messages(conn, schemas)
        await _refresh_forward_sources_snapshot(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            harvester_id=harvester_id,
            schemas=schemas,
        )
        total_upserted += len(schemas)
        all_dates.extend(dates)
        page_oldest = min(ids)
        page_newest = max(ids)
        oldest_id = page_oldest if oldest_id is None else min(oldest_id, page_oldest)

        try:
            page_deleted = await mark_missing_messages_deleted(
                conn,
                source_external_id=external_id,
                id_lo=page_oldest,
                id_hi=page_newest,
                seen_message_ids=ids,
                since=since,
            )
            deleted_ids.extend(page_deleted)
        except Exception:
            lg.exception("Failed delete reconcile during refresh peer=%s", external_id)

        if hit_since or len(raw) < page_limit:
            refresh_complete = True
            break

        if page_oldest >= before and before != 0:
            refresh_complete = True
            break
        before = page_oldest

    lg.info(
        "Refreshed recent peer messages external_id=%s peer_type=%s upserted=%s "
        "deleted=%s oldest_message_id=%s since_days=%s refresh_complete=%s "
        "linked_chats=%s linked_users=%s",
        external_id,
        peer_type,
        total_upserted,
        len(deleted_ids),
        oldest_id,
        since_days,
        refresh_complete,
        linked_chats,
        linked_users,
    )
    return PeerMessageSyncResult(
        upserted=total_upserted,
        message_dates=all_dates,
        oldest_message_id=oldest_id,
        refresh_complete=refresh_complete,
        deleted_message_ids=deleted_ids,
    )


async def fetch_and_upsert_messages_by_id(
    client: TelegramClient,
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    access_hash: int | None,
    harvester_external_id: str | None,
    message_ids: list[int],
    harvester_id: UUID | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> PeerMessageSyncResult:
    """Fetch specific historical Telegram messages by id and upsert them."""
    ids = sorted({mid for mid in message_ids if isinstance(mid, int) and mid > 0})
    if not ids:
        return PeerMessageSyncResult(upserted=0, tip_complete=True)

    entity = entity_for_peer(
        peer_external_id=external_id,
        peer_type=peer_type,
        access_hash=access_hash,
    )

    try:
        if peer_type in ("channel", "megagroup"):
            result = await client(functions.channels.GetMessagesRequest(entity, ids))
        else:
            result = await client(functions.messages.GetMessagesRequest(ids))
    except FloodWaitError:
        raise

    linked_chats, linked_users = await _light_upsert_history_entities(
        conn,
        chats=list(getattr(result, "chats", None) or []),
        users=list(getattr(result, "users", None) or []),
        harvester_id=harvester_id,
    )
    raw = _messages_from_history_result(client, result, entity)
    raw = _usable_messages(raw)

    schemas = []
    dates: list[datetime] = []
    for msg in raw:
        schemas.append(
            message_schema_from_telethon_message(
                msg,
                source_external_id=external_id,
                peer_type=peer_type,
                harvester_external_id=harvester_external_id,
            )
        )
        if getattr(msg, "date", None) is not None:
            dates.append(msg.date)

    if not schemas:
        return PeerMessageSyncResult(upserted=0, tip_complete=True)

    if harvester_id is not None:
        try:
            await materialize_channels_from_telethon_messages(
                client,
                conn,
                raw,
                harvester_id=harvester_id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
        except FloodWaitError:
            raise
        except Exception:
            lg.exception(
                "Failed materializing reply/forward channels for peer=%s ids=%s",
                external_id,
                ids[:5],
            )

    await upsert_messages(conn, schemas)
    await _refresh_forward_sources_snapshot(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        harvester_id=harvester_id,
        schemas=schemas,
    )
    lg.info(
        "Resolved peer messages external_id=%s peer_type=%s requested=%s upserted=%s linked_chats=%s linked_users=%s",
        external_id,
        peer_type,
        len(ids),
        len(schemas),
        linked_chats,
        linked_users,
    )
    return PeerMessageSyncResult(
        upserted=len(schemas),
        message_dates=dates,
        tip_complete=True,
    )
