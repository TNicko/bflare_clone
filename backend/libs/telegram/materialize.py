"""Shared Telethon peer materialization into canonical Postgres tables."""

from __future__ import annotations

import logging
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import ValidationError
from telethon.errors import FloodWaitError
from telethon import TelegramClient
from telethon.errors.common import TypeNotFoundError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    Channel,
    ChannelForbidden,
    Chat,
    ChatForbidden,
    InputUser,
    InputUserSelf,
    User,
)
from telethon.tl.types.messages import ChatFull as TelethonMessagesChatFull
from telethon.tl.types.users import UserFull as GetFullUserResult
from telegram.api import get_full_channel, get_full_chat
from telegram.harvester_peer_access import is_harvester_peer_inaccessible_error
from db.connection import rollback_quiet
from db.crud.system.peer_seed import enqueue_peer_seed_quiet
from db.crud.telegram.user import sync_user_deleted_observed_at, upsert_user
from db.crud.telegram.channel import upsert_channel
from db.crud.telegram.chat import upsert_chat
from db.crud.telegram.harvester import clear_harvester_peer_access_error, upsert_harvester_peer_role
from db.crud.telegram.peers import set_peer_unavailability
from db.schemas.general.primitives import TelegramPeerTypeSchema
from telegram.mappers import (
    channel_schema_from_telethon_channel,
    channel_schema_from_telethon_full_channel,
    chat_schema_from_telethon_full_chat,
    full_user_profile_fields_are_authoritative,
    user_profile_fields_are_authoritative,
    user_schema_from_telethon_full_user,
    user_schema_from_telethon_user,
)
from telegram.profile_photos import harvest_current_profile_photo, photo_dict_for_harvest
from telegram.utils import phone_digits, to_signed_peer_id

logger = logging.getLogger(__name__)

type EntityPeerTypeSchema = TelegramPeerTypeSchema | Literal["user"]


async def light_upsert_telethon_users(
    conn: Any,
    users: list[Any],
    *,
    harvester_id: UUID,
) -> int:
    """Persist minimal user rows from Telethon ``User`` objects (e.g. channel full.users bots)."""
    upserted = 0
    for user in users:
        if not isinstance(user, User):
            continue
        try:
            row = user_schema_from_telethon_user(user)
        except ValueError:
            continue
        await upsert_user(
            conn,
            row,
            preserve_updated_at_on_conflict=True,
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


async def light_upsert_telethon_channel(
    conn: Any,
    entity: Channel | ChannelForbidden,
    *,
    harvester_id: UUID,
    role: str = "child",
) -> bool:
    """Persist title/photo from a base Channel without getFullChannel.

    Stores ``access_hash`` on the harvester role when present. SQL keeps an
    existing ``parent`` role and never clobbers a known hash with NULL.
    """
    signed_peer_id = to_signed_peer_id(entity)
    if signed_peer_id is None:
        return False
    try:
        # Omit harvester_id so we do not overwrite the channel's owning harvester.
        row, access_hash = channel_schema_from_telethon_channel(
            entity,
            harvester_id=None,
            signed_peer_id=signed_peer_id,
        )
    except (ValueError, TypeError, ValidationError):
        return False

    peer_type: TelegramPeerTypeSchema = "megagroup" if bool(getattr(entity, "megagroup", False)) else "channel"
    table_name = "telegram_megagroups" if peer_type == "megagroup" else "telegram_channels"
    existed = await upsert_channel(conn, row, table_name=table_name)
    if access_hash is not None and not isinstance(entity, ChannelForbidden):
        await upsert_harvester_peer_role(
            conn,
            harvester_id=harvester_id,
            external_id=str(signed_peer_id),
            peer_type=peer_type,
            access_hash=int(access_hash),
            role=role,
        )
    if not existed:
        await enqueue_peer_seed_quiet(
            conn,
            external_id=str(signed_peer_id),
            peer_type=peer_type,
        )
    await conn.commit()
    return True


async def light_upsert_telethon_channels(
    conn: Any,
    chats: list[Any],
    *,
    harvester_id: UUID,
    role: str = "child",
) -> int:
    """Persist ``tg_min`` rows from Telethon ``chats[]`` (GetHistory / GetMessages)."""
    upserted = 0
    seen: set[int] = set()
    for chat in chats or []:
        if not isinstance(chat, (Channel, ChannelForbidden)):
            continue
        signed = to_signed_peer_id(chat)
        if signed is None or signed in seen:
            continue
        seen.add(signed)
        if await light_upsert_telethon_channel(
            conn,
            chat,
            harvester_id=harvester_id,
            role=role,
        ):
            upserted += 1
    return upserted


def peer_type_for_entity(entity: Any) -> EntityPeerTypeSchema | None:
    if isinstance(entity, User):
        return "user"
    if isinstance(entity, (Channel, ChannelForbidden)):
        return "megagroup" if bool(getattr(entity, "megagroup", False)) else "channel"
    if isinstance(entity, (Chat, ChatForbidden)):
        return "chat"
    return None


def peer_ref(entity: Any, *, persisted: bool) -> dict[str, object] | None:
    signed_peer_id = to_signed_peer_id(entity)
    peer_type = peer_type_for_entity(entity)
    if signed_peer_id is None or peer_type is None:
        return None
    return {
        "external_id": str(signed_peer_id),
        "peer_type": peer_type,
        "persisted": persisted,
    }


async def _try_harvest_profile_photo_after_materialize(
    client: TelegramClient,
    conn: Any,
    *,
    entity: User | Channel | Chat,
    peer_type: EntityPeerTypeSchema,
    external_id: str,
    current_photo: dict[str, Any] | None,
    s3_bucket: str,
    s3_region: str,
) -> None:
    try:
        await harvest_current_profile_photo(
            client,
            conn,
            entity=entity,
            peer_type=peer_type,
            external_id=external_id,
            bucket=s3_bucket,
            region=s3_region,
            current_photo=current_photo,
        )
    except Exception:
        logger.error(
            "Profile photo harvest failed peer_type=%s external_id=%s",
            peer_type,
            external_id,
            exc_info=True,
        )
        # Swallowing a SQL error without rollback leaves the connection aborted;
        # the next statement then fails with InFailedSqlTransaction.
        await rollback_quiet(
            conn,
            context=f"profile photo harvest peer_type={peer_type} external_id={external_id}",
        )


def _persist_log_verb(existed: bool) -> str:
    return "Refreshed" if existed else "Materialized"


async def materialize_user(
    client: TelegramClient,
    conn: Any,
    entity: User,
    *,
    harvester_id: UUID,
    phone: str | None = None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> bool:
    signed_peer_id = to_signed_peer_id(entity)
    if signed_peer_id is None:
        return False
    external_id = str(signed_peer_id)
    input_user: InputUser | InputUserSelf = (
        InputUserSelf() if entity.is_self else InputUser(entity.id, entity.access_hash or 0)
    )
    full = cast(GetFullUserResult, await client(GetFullUserRequest(input_user)))
    row_user = user_schema_from_telethon_full_user(full, signed_peer_id=signed_peer_id)
    if phone and not row_user.phone:
        digits = phone_digits(phone)
        if digits:
            row_user = row_user.model_copy(update={"phone": digits})
    existed = False
    async with conn.transaction():
        existed = await upsert_user(
            conn,
            row_user,
            profile_fields_authoritative=full_user_profile_fields_are_authoritative(full),
        )
        await sync_user_deleted_observed_at(
            conn,
            external_id,
            deleted=bool(entity.deleted),
        )
        await set_peer_unavailability(
            conn,
            external_id,
            peer_type="user",
            unavailable=False,
        )
        if not entity.is_self:
            await upsert_harvester_peer_role(
                conn,
                harvester_id=harvester_id,
                external_id=external_id,
                peer_type="user",
                access_hash=entity.access_hash,
                role="parent",
            )
    await _try_harvest_profile_photo_after_materialize(
        client,
        conn,
        entity=entity,
        peer_type="user",
        external_id=external_id,
        current_photo=photo_dict_for_harvest(row_user.photo),
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    await conn.commit()
    logger.info("%s user @%s id=%s", _persist_log_verb(existed), entity.username or entity.id, signed_peer_id)
    return True


async def _get_full_channel_or_none(
    client: TelegramClient,
    entity: Channel,
    *,
    external_id: str,
) -> TelethonMessagesChatFull | None:
    try:
        return cast(TelethonMessagesChatFull, await get_full_channel(client, entity))
    except TypeNotFoundError as exc:
        import telethon

        logger.error(
            "Telethon cannot decode channels.getFullChannel for external_id=%s "
            "(telethon %s): %s. Pin telethon to >=1.43.2,<1.44 if this mentions "
            "constructor 0xe4e0b29d.",
            external_id,
            telethon.__version__,
            exc,
        )
        return None
    except FloodWaitError:
        raise
    except Exception as exc:
        if is_harvester_peer_inaccessible_error(exc):
            logger.info(
                "getFullChannel inaccessible external_id=%s title=%r: %s",
                external_id,
                getattr(entity, "title", None),
                type(exc).__name__,
            )
            return None
        raise


def linked_channel_entity_from_full(
    full: TelethonMessagesChatFull,
    *,
    parent_channel_id: int,
) -> Channel | None:
    """Return the linked discussion/broadcast ``Channel`` from a GetFullChannel result, if any."""
    full_chat = getattr(full, "full_chat", None)
    linked_chat_id = getattr(full_chat, "linked_chat_id", None) if full_chat is not None else None
    if linked_chat_id is None:
        return None
    linked_id = int(linked_chat_id)
    if linked_id == int(parent_channel_id):
        return None
    for chat in getattr(full, "chats", None) or []:
        if isinstance(chat, Channel) and int(chat.id) == linked_id:
            return chat
    return None


async def materialize_channel(
    client: TelegramClient,
    conn: Any,
    entity: Channel,
    *,
    harvester_id: UUID,
    role: str = "parent",
    s3_bucket: str = "",
    s3_region: str = "",
    materialize_linked: bool = True,
) -> TelethonMessagesChatFull | None:
    """Upsert a channel/megagroup full profile; optionally also materialize its linked peer.

    When ``materialize_linked`` is true and ``channels.getFullChannel`` returns a
    ``linked_chat_id`` whose ``Channel`` is present in ``full.chats``, that peer is
    materialized too (discussion megagroup for a broadcast, or vice versa). Nested
    linked materialization uses ``materialize_linked=False`` to avoid A↔B recursion.
    """
    signed_peer_id = to_signed_peer_id(entity)
    if signed_peer_id is None:
        return None
    external_id = str(signed_peer_id)
    full = await _get_full_channel_or_none(client, entity, external_id=external_id)
    if full is None:
        # Private / banned / undecodable: keep chats[] stub (title + stripped thumb).
        await light_upsert_telethon_channel(
            conn,
            entity,
            harvester_id=harvester_id,
            role=role,
        )
        peer_type_min: TelegramPeerTypeSchema = "megagroup" if getattr(entity, "megagroup", False) else "channel"
        # Profile photos can still be downloadable with InputPeerChannel + access_hash
        # even when channels.getFullChannel is denied.
        row_min, _ = channel_schema_from_telethon_channel(
            entity,
            harvester_id=None,
            signed_peer_id=signed_peer_id,
        )
        await _try_harvest_profile_photo_after_materialize(
            client,
            conn,
            entity=entity,
            peer_type=peer_type_min,
            external_id=external_id,
            current_photo=photo_dict_for_harvest(row_min.photo),
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
        await conn.commit()
        logger.info(
            "Light-upserted %s id=%s title=%r (no getFullChannel)",
            peer_type_min,
            external_id,
            getattr(entity, "title", None),
        )
        return None
    row_channel, mapped_access_hash = channel_schema_from_telethon_full_channel(
        full,
        harvester_id=harvester_id,
        signed_peer_id=signed_peer_id,
    )
    peer_type: TelegramPeerTypeSchema = "megagroup" if entity.megagroup else "channel"
    table_name = "telegram_megagroups" if entity.megagroup else "telegram_channels"
    access_hash = mapped_access_hash if mapped_access_hash is not None else entity.access_hash
    existed = False
    async with conn.transaction():
        existed = await upsert_channel(conn, row_channel, table_name=table_name)
        await set_peer_unavailability(
            conn,
            external_id,
            peer_type=peer_type,
            unavailable=False,
        )
        await upsert_harvester_peer_role(
            conn,
            harvester_id=harvester_id,
            external_id=external_id,
            peer_type=peer_type,
            access_hash=access_hash,
            role=role,
        )
        await clear_harvester_peer_access_error(
            conn,
            harvester_id=harvester_id,
            external_id=external_id,
        )
    users = [u for u in (getattr(full, "users", None) or []) if isinstance(u, User)]
    if users:
        upserted = await light_upsert_telethon_users(
            conn,
            users,
            harvester_id=harvester_id,
        )
        if upserted:
            logger.info(
                "Upserted %s users from %s full for external_id=%s",
                upserted,
                peer_type,
                external_id,
            )
    await _try_harvest_profile_photo_after_materialize(
        client,
        conn,
        entity=entity,
        peer_type=peer_type,
        external_id=external_id,
        current_photo=photo_dict_for_harvest(row_channel.photo),
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    await conn.commit()
    if not existed:
        await enqueue_peer_seed_quiet(
            conn,
            external_id=external_id,
            peer_type=peer_type,
        )
    logger.info("%s %s %s id=%s", _persist_log_verb(existed), peer_type, entity.title, external_id)

    if materialize_linked:
        linked_entity = linked_channel_entity_from_full(full, parent_channel_id=entity.id)
        if linked_entity is not None:
            linked_type = "megagroup" if linked_entity.megagroup else "channel"
            try:
                linked_full = await materialize_channel(
                    client,
                    conn,
                    linked_entity,
                    harvester_id=harvester_id,
                    role=role,
                    s3_bucket=s3_bucket,
                    s3_region=s3_region,
                    materialize_linked=False,
                )
                if linked_full is not None:
                    logger.info(
                        "Materialized linked %s id=%s for parent %s %s",
                        linked_type,
                        to_signed_peer_id(linked_entity),
                        peer_type,
                        external_id,
                    )
                else:
                    logger.info(
                        "Linked %s chat_id=%s present but materialize returned None for parent %s",
                        linked_type,
                        linked_entity.id,
                        external_id,
                    )
            except FloodWaitError:
                raise
            except Exception:
                logger.warning(
                    "Failed materializing linked %s chat_id=%s for parent %s",
                    linked_type,
                    linked_entity.id,
                    external_id,
                    exc_info=True,
                )

    return full


async def materialize_chat(
    client: TelegramClient,
    conn: Any,
    entity: Chat,
    *,
    harvester_id: UUID,
    role: str = "parent",
    s3_bucket: str = "",
    s3_region: str = "",
) -> TelethonMessagesChatFull | None:
    signed_peer_id = to_signed_peer_id(entity)
    if signed_peer_id is None:
        return None
    external_id = str(signed_peer_id)
    full = await get_full_chat(client, entity.id)
    row_chat = chat_schema_from_telethon_full_chat(
        full,
        harvester_id=harvester_id,
        signed_peer_id=signed_peer_id,
    )
    existed = False
    async with conn.transaction():
        existed = await upsert_chat(conn, row_chat)
        await set_peer_unavailability(
            conn,
            external_id,
            peer_type="chat",
            unavailable=False,
        )
        await upsert_harvester_peer_role(
            conn,
            harvester_id=harvester_id,
            external_id=external_id,
            peer_type="chat",
            access_hash=None,
            role=role,
        )
        await clear_harvester_peer_access_error(
            conn,
            harvester_id=harvester_id,
            external_id=external_id,
        )
    users = [u for u in (getattr(full, "users", None) or []) if isinstance(u, User)]
    if users:
        upserted = await light_upsert_telethon_users(
            conn,
            users,
            harvester_id=harvester_id,
        )
        if upserted:
            logger.info(
                "Upserted %s users from chat full for external_id=%s",
                upserted,
                external_id,
            )
    await _try_harvest_profile_photo_after_materialize(
        client,
        conn,
        entity=entity,
        peer_type="chat",
        external_id=external_id,
        current_photo=photo_dict_for_harvest(row_chat.photo),
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    await conn.commit()
    if not existed:
        await enqueue_peer_seed_quiet(
            conn,
            external_id=external_id,
            peer_type="chat",
        )
    logger.info("%s chat %s id=%s", _persist_log_verb(existed), entity.title, external_id)
    return full
