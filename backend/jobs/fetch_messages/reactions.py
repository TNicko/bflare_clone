"""Fetch full message reactor lists via ``messages.GetMessageReactionsList``."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter, ValidationError
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.functions.messages import GetMessageReactionsListRequest
from telethon.tl.types import User

from db.crud.telegram.harvester import upsert_harvester_peer_role
from db.crud.telegram.message_reactors import (
    fetch_existing_reaction_totals,
    fetch_message_row_ids,
    replace_message_reactors,
)
from db.crud.telegram.user import sync_user_deleted_observed_at, upsert_user
from db.schemas.telegram.message import TelegramMessageSchema
from db.schemas.telegram.message_reactor import TelegramMessageReactorSchema
from db.schemas.telegram.slim_reactions import SlimMessageReactionsSchema
from db.schemas.telegram.tl import TypeReactionSchema
from telegram.mappers import user_profile_fields_are_authoritative, user_schema_from_telethon_user
from telegram.slim_reactions import reaction_key, reaction_total
from telegram.utils import to_signed_peer_id, from_telethon, peer_ref_from_telethon

logger = logging.getLogger(__name__)

REACTIONS_LIST_LIMIT = 100
REACTIONS_LIST_CONCURRENCY = 2

_REACTION_ADAPTER = TypeAdapter(TypeReactionSchema)

MessageReactionKey = tuple[str, int, datetime]
PriorReactionState = dict[MessageReactionKey, tuple[UUID, int, bool]]


@dataclass(frozen=True, slots=True)
class ReactionHarvestStats:
    lists_fetched: int = 0
    lists_skipped: int = 0
    reactors_persisted: int = 0
    users_upserted: int = 0
    errors: int = 0


def can_see_reaction_list(reactions: SlimMessageReactionsSchema | None) -> bool:
    return bool(reactions and reactions.can_see_list and reactions.results)


def message_reaction_key(msg: TelegramMessageSchema) -> MessageReactionKey | None:
    if msg.date is None:
        return None
    date = msg.date if msg.date.tzinfo is not None else msg.date.replace(tzinfo=UTC)
    return (msg.source_external_id, msg.message_id, date)


async def snapshot_reaction_state(
    conn: Any,
    messages: list[TelegramMessageSchema],
) -> PriorReactionState:
    """Call **before** message upsert so totals reflect the previous DB state."""
    keys = [k for m in messages if can_see_reaction_list(m.reactions) for k in (message_reaction_key(m),) if k]
    return await fetch_existing_reaction_totals(conn, keys)


async def _sleep_flood(seconds: int) -> None:
    wait = max(1, int(seconds))
    logger.warning("FloodWait %ss while fetching message reactions list", wait)
    await asyncio.sleep(wait)


async def _fetch_all_peer_reactions(
    client: TelegramClient,
    *,
    peer: Any,
    telegram_message_id: int,
) -> tuple[list[Any], list[User]]:
    offset: str | None = None
    peer_reactions: list[Any] = []
    users_by_id: dict[int, User] = {}

    while True:
        while True:
            try:
                result = await client(
                    GetMessageReactionsListRequest(
                        peer=peer,
                        id=telegram_message_id,
                        limit=REACTIONS_LIST_LIMIT,
                        offset=offset,
                    )
                )
                break
            except FloodWaitError as exc:
                await _sleep_flood(int(exc.seconds))

        for u in getattr(result, "users", None) or []:
            if isinstance(u, User) and getattr(u, "id", None) is not None:
                users_by_id[int(u.id)] = u

        for pr in getattr(result, "reactions", None) or []:
            peer_reactions.append(pr)

        next_offset = getattr(result, "next_offset", None)
        if not next_offset:
            break
        offset = str(next_offset)

    return peer_reactions, list(users_by_id.values())


def _reactor_rows_from_peer_reactions(
    *,
    message_uuid: UUID,
    message_date: datetime,
    source_external_id: str,
    peer_reactions: list[Any],
) -> list[TelegramMessageReactorSchema]:
    rows: list[TelegramMessageReactorSchema] = []
    for pr in peer_reactions:
        signed = to_signed_peer_id(getattr(pr, "peer_id", None))
        ref = peer_ref_from_telethon(getattr(pr, "peer_id", None))
        if signed is None or ref is None:
            continue
        try:
            reaction = _REACTION_ADAPTER.validate_python(from_telethon(getattr(pr, "reaction", None)))
        except (ValidationError, TypeError):
            continue

        reacted_at = getattr(pr, "date", None)
        if isinstance(reacted_at, datetime) and reacted_at.tzinfo is None:
            reacted_at = reacted_at.replace(tzinfo=UTC)

        try:
            rows.append(
                TelegramMessageReactorSchema(
                    message_id=message_uuid,
                    message_date=message_date,
                    source_external_id=source_external_id,
                    reactor_external_id=str(signed),
                    peer_type=ref["peer_type"],
                    reaction_key=reaction_key(reaction),
                    reaction=reaction,
                    reacted_at=reacted_at if isinstance(reacted_at, datetime) else None,
                )
            )
        except ValidationError:
            continue
    return rows


async def _light_upsert_reactor_users(
    conn: Any,
    users: list[User],
    *,
    harvester_id: UUID,
) -> int:
    upserted = 0
    for user in users:
        try:
            row = user_schema_from_telethon_user(user)
        except ValueError:
            continue
        await upsert_user(
            conn,
            row,
            profile_fields_authoritative=user_profile_fields_are_authoritative(user),
        )
        await sync_user_deleted_observed_at(
            conn,
            row.external_id,
            deleted=bool(getattr(user, "deleted", False)),
        )
        access_hash = getattr(user, "access_hash", None)
        if access_hash is not None and not getattr(user, "is_self", False):
            await upsert_harvester_peer_role(
                conn,
                harvester_id=harvester_id,
                external_id=row.external_id,
                peer_type="user",
                access_hash=int(access_hash),
                role="child",
            )
        upserted += 1
    return upserted


async def harvest_reactors_for_messages(
    client: TelegramClient,
    conn: Any,
    messages: list[TelegramMessageSchema],
    *,
    peer_entity: Any,
    harvester_id: UUID,
    prior_state: PriorReactionState | None = None,
) -> ReactionHarvestStats:
    """Persist full reactor lists after messages have been upserted.

    Pass ``prior_state`` from :func:`snapshot_reaction_state` taken **before** upsert
    so we can detect count changes. Without it, only messages with no reactor rows yet
    are fetched.
    """
    candidates = [m for m in messages if can_see_reaction_list(m.reactions) and message_reaction_key(m)]
    if not candidates:
        return ReactionHarvestStats(lists_skipped=len(messages))

    keys_nn = [k for m in candidates for k in (message_reaction_key(m),) if k]
    existing = prior_state if prior_state is not None else {}
    ids = await fetch_message_row_ids(conn, keys_nn)

    to_fetch: list[tuple[TelegramMessageSchema, UUID, datetime]] = []
    skipped = 0
    for msg in candidates:
        key = message_reaction_key(msg)
        if key is None:
            skipped += 1
            continue
        message_date = key[2]
        mid_uuid = ids.get(key)
        if mid_uuid is None:
            skipped += 1
            continue
        new_total = reaction_total(msg.reactions)
        prev = existing.get(key)
        if prev is not None:
            _uid, old_total, has_reactors = prev
            if has_reactors and old_total == new_total:
                skipped += 1
                continue
        to_fetch.append((msg, mid_uuid, message_date))

    if not to_fetch:
        return ReactionHarvestStats(lists_skipped=skipped + (len(messages) - len(candidates)))

    sem = asyncio.Semaphore(REACTIONS_LIST_CONCURRENCY)
    fetched: list[tuple[TelegramMessageSchema, UUID, datetime, list[Any], list[User]]] = []
    errors = 0

    async def _fetch_one(msg: TelegramMessageSchema, mid_uuid: UUID, message_date: datetime) -> None:
        nonlocal errors
        async with sem:
            try:
                peer_reactions, users = await _fetch_all_peer_reactions(
                    client,
                    peer=peer_entity,
                    telegram_message_id=msg.message_id,
                )
                fetched.append((msg, mid_uuid, message_date, peer_reactions, users))
            except RPCError as exc:
                errors += 1
                logger.warning(
                    "GetMessageReactionsList failed message_id=%s peer=%s: %s",
                    msg.message_id,
                    msg.source_external_id,
                    exc,
                )
            except Exception:
                errors += 1
                logger.exception(
                    "Reactor harvest failed message_id=%s peer=%s",
                    msg.message_id,
                    msg.source_external_id,
                )

    await asyncio.gather(*[_fetch_one(m, uid, d) for m, uid, d in to_fetch])

    reactors_persisted = 0
    users_upserted = 0
    for msg, mid_uuid, message_date, peer_reactions, users in fetched:
        rows = _reactor_rows_from_peer_reactions(
            message_uuid=mid_uuid,
            message_date=message_date,
            source_external_id=msg.source_external_id,
            peer_reactions=peer_reactions,
        )
        reactors_persisted += await replace_message_reactors(
            conn,
            message_id=mid_uuid,
            message_date=message_date,
            rows=rows,
        )
        users_upserted += await _light_upsert_reactor_users(conn, users, harvester_id=harvester_id)

    await conn.commit()

    return ReactionHarvestStats(
        lists_fetched=len(fetched),
        lists_skipped=skipped + (len(messages) - len(candidates)),
        reactors_persisted=reactors_persisted,
        users_upserted=users_upserted,
        errors=errors,
    )
