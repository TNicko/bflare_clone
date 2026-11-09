"""Upserts and partial updates for canonical Telegram users."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg import AsyncConnection, sql

from db.crud.telegram.field_history import drop_unauthoritative_profile_overwrites, fetch_peer_row_snapshot
from db.crud.telegram.peers import record_field_history, schema_upsert_values
from db.crud.telegram.search_normalize import attach_user_search_fields
from db.schemas.telegram.user import TelegramUserSchema
from db.utils import adapt_jsonb_param, adapt_param, build_upsert_sql

_TELEGRAM_USER_JSONB_SCALAR_COLUMNS = frozenset({"blocked_my_stories_from"})
USER_EXPLICIT_NULL_FIELDS = frozenset({"username", "first_name", "last_name", "about", "photo"})


async def upsert_user(
    conn: AsyncConnection,
    row: TelegramUserSchema,
    *,
    table_name: str = "telegram_users",
    conflict_columns: tuple[str, ...] = ("external_id",),
    preserve_created_at_on_conflict: bool = True,
    preserve_updated_at_on_conflict: bool = False,
    profile_fields_authoritative: bool = True,
) -> bool:
    """Upsert a user row.

    Returns True when a row already existed (this is a refresh), False on insert.
    Existence comes from the field-history snapshot already loaded for the upsert.
    """
    old_row = await fetch_peer_row_snapshot(
        conn,
        table_name=table_name,
        external_id=row.external_id,
    )
    values = schema_upsert_values(row, explicit_null_fields=USER_EXPLICIT_NULL_FIELDS)
    if not profile_fields_authoritative:
        drop_unauthoritative_profile_overwrites(values, old_row)
    attach_user_search_fields(values, old_row=old_row)
    columns = list(values.keys())
    exclude_from_update: list[str] = []
    if preserve_created_at_on_conflict:
        exclude_from_update.append("created_at")
    if preserve_updated_at_on_conflict:
        exclude_from_update.append("updated_at")
    query = build_upsert_sql(
        table=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
        exclude_from_update=exclude_from_update or None,
    )
    params = tuple(
        adapt_jsonb_param(values[c]) if c in _TELEGRAM_USER_JSONB_SCALAR_COLUMNS else adapt_param(values[c])
        for c in columns
    )

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


async def update_user_stories_and_gifts(
    conn: AsyncConnection,
    external_id: str,
    *,
    pinned_stories: dict[str, Any] | None | object = ...,
    pinned_stories_fetched_at: datetime | None | object = ...,
    saved_star_gifts: dict[str, Any] | None | object = ...,
    star_gifts_fetched_at: datetime | None | object = ...,
    table_name: str = "telegram_users",
) -> None:
    """Partial update for lazy-fetched pinned stories and saved star gifts."""
    updates: list[sql.Composable] = []
    params: list[Any] = []

    if pinned_stories is not ...:
        updates.append(sql.SQL("pinned_stories = %s"))
        params.append(adapt_jsonb_param(pinned_stories))
    if pinned_stories_fetched_at is not ...:
        updates.append(sql.SQL("pinned_stories_fetched_at = %s"))
        params.append(pinned_stories_fetched_at)
    if saved_star_gifts is not ...:
        updates.append(sql.SQL("saved_star_gifts = %s"))
        params.append(adapt_jsonb_param(saved_star_gifts))
    if star_gifts_fetched_at is not ...:
        updates.append(sql.SQL("star_gifts_fetched_at = %s"))
        params.append(star_gifts_fetched_at)

    if not updates:
        return

    updates.append(sql.SQL("updated_at = now()"))
    query = sql.SQL("UPDATE {table} SET {assignments} WHERE external_id = %s").format(
        table=sql.Identifier(table_name),
        assignments=sql.SQL(", ").join(updates),
    )
    params.append(external_id)
    async with conn.cursor() as cur:
        await cur.execute(query, tuple(params))


async def sync_user_deleted_observed_at(
    conn: AsyncConnection,
    external_id: str,
    *,
    deleted: bool,
    table_name: str = "telegram_users",
) -> None:
    """Stamp or clear ``deleted_observed_at`` after ``deleted`` was persisted."""
    if deleted:
        query = sql.SQL(
            """
            UPDATE {table}
            SET deleted_observed_at = COALESCE(deleted_observed_at, now()),
                updated_at = now()
            WHERE external_id = %s
              AND deleted = true
            """
        ).format(table=sql.Identifier(table_name))
    else:
        query = sql.SQL(
            """
            UPDATE {table}
            SET deleted_observed_at = NULL,
                updated_at = now()
            WHERE external_id = %s
              AND deleted = false
              AND deleted_observed_at IS NOT NULL
            """
        ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (external_id,))
