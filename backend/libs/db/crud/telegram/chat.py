"""Upserts for canonical Telegram basic chat tables."""

from __future__ import annotations

from psycopg import AsyncConnection

from db.crud.telegram.field_history import fetch_peer_row_snapshot
from db.crud.telegram.peers import record_field_history, schema_upsert_values
from db.crud.telegram.search_normalize import attach_chat_search_fields
from db.schemas.telegram.chat import TelegramChatSchema
from db.utils import adapt_param, build_upsert_sql

CHAT_EXPLICIT_NULL_FIELDS = frozenset({"title", "about", "photo"})


async def upsert_chat(
    conn: AsyncConnection,
    row: TelegramChatSchema,
    *,
    table_name: str = "telegram_chats",
    conflict_columns: tuple[str, ...] = ("harvester_id", "external_id"),
) -> bool:
    """Upsert a basic chat row.

    Returns True when a row already existed (this is a refresh), False on insert.
    """
    old_row = await fetch_peer_row_snapshot(
        conn,
        table_name=table_name,
        external_id=row.external_id,
    )
    values = schema_upsert_values(row, explicit_null_fields=CHAT_EXPLICIT_NULL_FIELDS)
    attach_chat_search_fields(values, old_row=old_row)
    # Managed by participant harvest, not profile upserts.
    values.pop("last_participant_fetch_at", None)
    # Filled by first-message seek; never clobber from profile upserts.
    values.pop("first_message_at", None)
    columns = list(values.keys())
    query = build_upsert_sql(
        table=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
    )
    params = tuple(adapt_param(values[c]) for c in columns)

    async with conn.cursor() as cur:
        await cur.execute(query, params)

    await record_field_history(
        conn,
        table_name=table_name,
        external_id=row.external_id,
        old_row=old_row,
        new_row=values,
    )
    return old_row is not None
