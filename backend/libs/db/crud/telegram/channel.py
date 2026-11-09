"""Upserts for canonical Telegram channel and megagroup tables."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg import AsyncConnection

from db.crud.telegram.field_history import fetch_peer_row_snapshot
from db.crud.telegram.peers import record_field_history, schema_upsert_values
from db.crud.telegram.search_normalize import attach_channel_search_fields
from db.schemas.telegram.channel import TelegramChannelSchema
from db.utils import adapt_param, build_upsert_sql

CHANNEL_EXPLICIT_NULL_FIELDS = frozenset({"title", "about", "photo", "username", "usernames"})
_PROFILE_TIER_RANK: dict[str, int] = {
    "web": 0,
    "tg_min": 1,
    "tg_full": 2,
}


def _ensure_channel_insert_date(
    values: dict[str, Any],
    row: TelegramChannelSchema,
) -> None:
    """Ensure the channel's required insertion date is always present."""
    if values.get("date") is not None:
        return
    if "date" in row.model_fields_set and row.date is not None:
        values["date"] = row.date
        return
    updated_at = values.get("updated_at")
    if updated_at is not None:
        values["date"] = updated_at
        return
    values["date"] = datetime.now(UTC)


def _should_apply_profile_tier_update(
    *,
    existing_tier: str | None,
    incoming_tier: str,
) -> bool:
    if existing_tier is None:
        return True
    return _PROFILE_TIER_RANK.get(incoming_tier, 0) >= _PROFILE_TIER_RANK.get(existing_tier, 0)


async def upsert_channel(
    conn: AsyncConnection,
    row: TelegramChannelSchema,
    *,
    table_name: str = "telegram_channels",
    conflict_columns: tuple[str, ...] = ("external_id",),
) -> bool:
    """Upsert a channel/megagroup row.

    Returns True when a row already existed (this is a refresh), False on insert.
    """
    old_row = await fetch_peer_row_snapshot(
        conn,
        table_name=table_name,
        external_id=row.external_id,
    )
    values = schema_upsert_values(row, explicit_null_fields=CHANNEL_EXPLICIT_NULL_FIELDS)
    attach_channel_search_fields(values, old_row=old_row)
    # Managed by participant harvest, not profile upserts.
    values.pop("last_participant_fetch_at", None)
    # Filled by first-message seek; never clobber from profile upserts.
    values.pop("first_message_at", None)
    _ensure_channel_insert_date(values, row)
    exclude_from_update: list[str] = []
    if old_row is not None:
        exclude_from_update.append("date")
    incoming_tier = values.get("profile_tier")
    if incoming_tier is not None and old_row is not None:
        existing_tier = old_row.get("profile_tier")
        if not _should_apply_profile_tier_update(
            existing_tier=str(existing_tier) if existing_tier is not None else None,
            incoming_tier=str(incoming_tier),
        ):
            values.pop("profile_tier", None)
    columns = list(values.keys())
    query = build_upsert_sql(
        table=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
        exclude_from_update=exclude_from_update or None,
    )
    params = tuple(adapt_param(values[c]) for c in columns)

    async with conn.cursor() as cur:
        await cur.execute(query, params)

    history_row = values
    if old_row is not None:
        history_row = {k: v for k, v in values.items() if k != "date"}
    await record_field_history(
        conn,
        table_name=table_name,
        external_id=row.external_id,
        old_row=old_row,
        new_row=history_row,
    )
    return old_row is not None
