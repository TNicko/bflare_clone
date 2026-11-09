"""Shared CRUD helpers for all canonical Telegram peer tables."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection, sql

from psycopg.rows import DictRow
from pydantic import BaseModel

from db.crud.telegram.field_history import (
    TABLE_PEER_TYPE,
    diff_peer_fields,
    sync_peer_field_history,
)
from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.utils import adapt_jsonb_param


def schema_upsert_values(
    row: BaseModel,
    *,
    explicit_null_fields: frozenset[str],
) -> dict[str, Any]:
    """Build upsert columns from a peer schema row.

    Most ``None`` fields are omitted so partial upserts leave existing DB values
    unchanged. Unset fields (``exclude_unset``) are omitted entirely so unknown
    booleans are not written as false. Fields in ``explicit_null_fields`` are
    included as SQL NULL when they were set on the schema (e.g. Telegram returned
    no username, bio, or profile photo).
    """
    fields_set = row.model_fields_set
    dump = row.model_dump(mode="json", exclude={"id"}, exclude_unset=True)
    values: dict[str, Any] = {}
    for key, value in dump.items():
        if value is not None:
            values[key] = value
        elif key in explicit_null_fields and key in fields_set:
            values[key] = None
    return values


async def record_field_history(
    conn: AsyncConnection,
    *,
    table_name: str,
    external_id: str,
    old_row: Mapping[str, Any] | None,
    new_row: Mapping[str, Any],
) -> None:
    peer_type = TABLE_PEER_TYPE[table_name]
    changes = diff_peer_fields(old_row, new_row, peer_type)
    await sync_peer_field_history(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        changes=changes,
    )


_PEER_TABLE_BY_TYPE: dict[TelegramPeerTypeSchema, str] = {
    "user": "telegram_users",
    "channel": "telegram_channels",
    "megagroup": "telegram_megagroups",
    "chat": "telegram_chats",
}


async def set_peer_unavailability(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    peer_type: TelegramPeerTypeSchema,
    unavailable: bool,
) -> None:
    """Set or clear ``unavailable_at`` on a canonical peer row."""
    table_name = _PEER_TABLE_BY_TYPE.get(peer_type)
    if table_name is None:
        raise ValueError(f"Unsupported peer_type={peer_type!r}")
    if unavailable:
        query = sql.SQL(
            """
            UPDATE {table}
            SET unavailable_at = now(), updated_at = now()
            WHERE external_id = %s
              AND unavailable_at IS NULL
            """
        ).format(table=sql.Identifier(table_name))
    else:
        query = sql.SQL(
            """
            UPDATE {table}
            SET unavailable_at = NULL, updated_at = now()
            WHERE external_id = %s
              AND unavailable_at IS NOT NULL
            """
        ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (external_id,))


async def update_peer_profile_photo_harvest(
    conn: AsyncConnection,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    photo: dict[str, Any] | None,
    profile_photo_s3_uri: str | None,
) -> None:
    table_name = _PEER_TABLE_BY_TYPE.get(peer_type)
    if table_name is None:
        raise ValueError(f"Unsupported peer_type={peer_type!r}")
    query = sql.SQL(
        """
        UPDATE {table}
        SET photo = %s,
            profile_photo_s3_uri = %s,
            updated_at = now()
        WHERE external_id = %s
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(
            query,
            (
                adapt_jsonb_param(photo) if photo is not None else None,
                profile_photo_s3_uri,
                external_id,
            ),
        )


_PARTICIPANT_FETCH_PEER_TYPES: frozenset[TelegramPeerTypeSchema] = frozenset({"megagroup", "chat"})


_FIRST_MESSAGE_PEER_TYPES: frozenset[TelegramPeerTypeSchema] = frozenset({"channel", "megagroup", "chat"})


async def set_peer_first_message_at(
    conn: AsyncConnection,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    first_message_at: datetime,
) -> bool:
    """Persist oldest-accessible message date when the column is still null.

    Returns True when a row was updated.
    """
    if peer_type not in _FIRST_MESSAGE_PEER_TYPES:
        return False
    table_name = _PEER_TABLE_BY_TYPE[peer_type]
    query = sql.SQL(
        """
        UPDATE {table}
        SET first_message_at = %s,
            updated_at = now()
        WHERE external_id = %s
          AND first_message_at IS NULL
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (first_message_at, external_id))
        return bool(cur.rowcount)


async def mark_last_participant_fetch(
    conn: AsyncConnection,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
) -> None:
    """Stamp ``last_participant_fetch_at`` on megagroups/chats after a successful harvest."""
    if peer_type not in _PARTICIPANT_FETCH_PEER_TYPES:
        return
    table_name = _PEER_TABLE_BY_TYPE[peer_type]
    query = sql.SQL(
        """
        UPDATE {table}
        SET last_participant_fetch_at = now(),
            updated_at = now()
        WHERE external_id = %s
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (external_id,))
