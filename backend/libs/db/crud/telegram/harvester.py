"""CRUD for ``telegram_harvesters``, ``telegram_harvester_peer_roles``, and permissions."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row

from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.harvester import (
    SessionStateSchema,
    TelegramHarvesterSchema,
    TelegramHarvesterPeerPermissionsSchema,
)
from db.utils import adapt_param, build_upsert_sql


def _harvester_from_row(row: dict[str, Any]) -> TelegramHarvesterSchema:
    return TelegramHarvesterSchema(
        id=row["id"],
        external_id=str(row["external_id"]),
        label=row["label"],
        is_active=bool(row["is_active"]),
        session_state=row["session_state"],
        phone_number=row["phone_number"],
        username=row["username"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _uuid_param(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


async def list_eligible_harvesters(
    conn: AsyncConnection,
    *,
    session_type: str = "system",
    table_name: str = "telegram_harvesters",
) -> list[TelegramHarvesterSchema]:
    """Return harvesters with at least one healthy session of the given type."""
    query = sql.SQL(
        """
        SELECT DISTINCT h.id, h.external_id, h.label, h.is_active, h.session_state,
               h.phone_number, h.username, h.first_name, h.last_name,
               h.created_at, h.updated_at
        FROM {table} AS h
        INNER JOIN telegram_harvester_sessions AS s
          ON s.harvester_id = h.id
         AND s.session_type = %s
         AND s.auth_state = 'healthy'
        WHERE h.is_active = true
        ORDER BY h.external_id
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (session_type,))
        rows = await cur.fetchall()
    return [_harvester_from_row(row) for row in rows]


async def list_active_harvesters(
    conn: AsyncConnection[DictRow],
    *,
    table_name: str = "telegram_harvesters",
) -> list[TelegramHarvesterSchema]:
    query = sql.SQL(
        """
        SELECT id, external_id, label, is_active, session_state,
               phone_number, username, first_name, last_name,
               created_at, updated_at
        FROM {table}
        WHERE is_active = true
        ORDER BY external_id
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query)
        rows = await cur.fetchall()
    return [_harvester_from_row(row) for row in rows]


async def list_harvesters(
    conn: AsyncConnection,
    *,
    table_name: str = "telegram_harvesters",
) -> list[TelegramHarvesterSchema]:
    """Return all harvester rows ordered by ``external_id``."""
    query = sql.SQL(
        """
        SELECT id, external_id, label, is_active, session_state,
               phone_number, username, first_name, last_name,
               created_at, updated_at
        FROM {table}
        ORDER BY external_id
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query)
        rows = await cur.fetchall()
    return [_harvester_from_row(row) for row in rows]


async def get_harvester_by_id(
    conn: AsyncConnection[DictRow],
    harvester_id: Any,
    *,
    table_name: str = "telegram_harvesters",
) -> TelegramHarvesterSchema | None:
    query = sql.SQL(
        """
        SELECT id, external_id, label, is_active, session_state,
               phone_number, username, first_name, last_name,
               created_at, updated_at
        FROM {table}
        WHERE id = %s
        LIMIT 1
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (harvester_id,))
        row = await cur.fetchone()
    return _harvester_from_row(row) if row else None


async def get_harvester_by_external_id(
    conn: AsyncConnection,
    external_id: str,
    *,
    table_name: str = "telegram_harvesters",
) -> TelegramHarvesterSchema | None:
    query = sql.SQL(
        """
        SELECT id, external_id, label, is_active, session_state,
               phone_number, username, first_name, last_name,
               created_at, updated_at
        FROM {table}
        WHERE external_id = %s
        LIMIT 1
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id,))
        row = await cur.fetchone()
    return _harvester_from_row(row) if row else None


async def update_harvester_session_state(
    conn: AsyncConnection,
    harvester_id: Any,
    session_state: SessionStateSchema,
    *,
    table_name: str = "telegram_harvesters",
) -> None:
    """Set ``session_state`` for a harvester row."""
    query = sql.SQL(
        """
        UPDATE {table}
        SET session_state = %s,
            updated_at = NOW()
        WHERE id = %s
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (session_state, harvester_id))


async def deactivate_harvester(
    conn: AsyncConnection,
    harvester_id: UUID | str,
    *,
    table_name: str = "telegram_harvesters",
) -> bool:
    """Mark a harvester inactive (``is_active=false``, ``session_state=error``).

    Returns ``True`` when a previously-active row was updated.
    """
    hid = _uuid_param(harvester_id)
    query = sql.SQL(
        """
        UPDATE {table}
        SET is_active = false,
            session_state = 'error',
            updated_at = NOW()
        WHERE id = %s
          AND is_active = true
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (hid,))
        return bool(cur.rowcount)


async def upsert_harvester_peer_role(
    conn: AsyncConnection,
    *,
    harvester_id: UUID | str,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    access_hash: int | None = None,
    role: str = "child",
    table_name: str = "telegram_harvester_peer_roles",
) -> None:
    hid = _uuid_param(harvester_id)
    # Never downgrade parent→child or clobber a known access_hash with NULL.
    query = sql.SQL(
        """
        INSERT INTO {table} (
            external_id,
            harvester_id,
            peer_type,
            access_hash,
            role,
            assigned_at,
            updated_at
        )
        VALUES ({placeholders}, now(), now())
        ON CONFLICT (external_id, harvester_id)
        DO UPDATE SET
            peer_type = EXCLUDED.peer_type,
            access_hash = COALESCE(EXCLUDED.access_hash, {table}.access_hash),
            role = CASE
                WHEN {table}.role = 'parent' THEN 'parent'
                ELSE EXCLUDED.role
            END,
            updated_at = now()
        """
    ).format(
        table=sql.Identifier(table_name),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * 5),
    )
    params = (external_id, hid, peer_type, access_hash, role)
    async with conn.cursor() as cur:
        await cur.execute(query, params)


async def record_harvester_peer_access_error(
    conn: AsyncConnection,
    *,
    harvester_id: UUID | str,
    external_id: str,
    error_code: str,
    peer_type: TelegramPeerTypeSchema | None = None,
    access_hash: int | None = None,
    table_name: str = "telegram_harvester_peer_roles",
) -> None:
    """Mark a harvester+peer assignment as inaccessible (Telegram RPC error code)."""
    hid = _uuid_param(harvester_id)
    update_query = sql.SQL(
        """
        UPDATE {table}
        SET access_error_code = %s,
            access_checked_at = now(),
            updated_at = now()
        WHERE harvester_id = %s
          AND external_id = %s
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(update_query, (error_code, hid, external_id))
        updated = cur.rowcount

    if updated or peer_type is None:
        return

    await upsert_harvester_peer_role(
        conn,
        harvester_id=hid,
        external_id=external_id,
        peer_type=peer_type,
        access_hash=access_hash,
        role="parent",
        table_name=table_name,
    )
    mark_query = sql.SQL(
        """
        UPDATE {table}
        SET access_error_code = %s,
            access_checked_at = now(),
            updated_at = now()
        WHERE harvester_id = %s
          AND external_id = %s
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(mark_query, (error_code, hid, external_id))


async def clear_harvester_peer_access_error(
    conn: AsyncConnection,
    *,
    harvester_id: UUID | str,
    external_id: str,
    table_name: str = "telegram_harvester_peer_roles",
) -> None:
    """Clear a prior access error after a successful fetch/sync."""
    hid = _uuid_param(harvester_id)
    query = sql.SQL(
        """
        UPDATE {table}
        SET access_error_code = NULL,
            access_checked_at = now(),
            updated_at = now()
        WHERE harvester_id = %s
          AND external_id = %s
          AND access_error_code IS NOT NULL
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor() as cur:
        await cur.execute(query, (hid, external_id))


async def get_peer_access_error(
    conn: AsyncConnection,
    *,
    external_id: str,
    harvester_id: UUID | str | None = None,
    table_name: str = "telegram_harvester_peer_roles",
) -> tuple[str | None, datetime | None]:
    """Return the best harvester access error for a peer, if any.

    Prefers the peer's assigned harvester (when provided), then parent roles,
    then the most recently checked error.
    """
    hid = _uuid_param(harvester_id) if harvester_id is not None else None
    query = sql.SQL(
        """
        SELECT access_error_code, access_checked_at
        FROM {table}
        WHERE external_id = %s
          AND access_error_code IS NOT NULL
        ORDER BY
          CASE WHEN %s::uuid IS NOT NULL AND harvester_id = %s::uuid THEN 0 ELSE 1 END,
          CASE WHEN role = 'parent' THEN 0 ELSE 1 END,
          access_checked_at DESC NULLS LAST
        LIMIT 1
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id, hid, hid))
        row = await cur.fetchone()
    if not row:
        return None, None
    code = row.get("access_error_code")
    checked_at = row.get("access_checked_at")
    return (
        str(code) if code is not None else None,
        checked_at if isinstance(checked_at, datetime) else None,
    )


async def get_peers_access_error_codes(
    conn: AsyncConnection,
    external_ids: list[str],
    *,
    table_name: str = "telegram_harvester_peer_roles",
) -> dict[str, str]:
    """Map ``external_id`` → best non-null ``access_error_code`` for a batch of peers."""
    if not external_ids:
        return {}
    query = sql.SQL(
        """
        SELECT DISTINCT ON (external_id)
          external_id,
          access_error_code
        FROM {table}
        WHERE external_id = ANY(%s)
          AND access_error_code IS NOT NULL
        ORDER BY
          external_id,
          CASE WHEN role = 'parent' THEN 0 ELSE 1 END,
          access_checked_at DESC NULLS LAST
        """
    ).format(table=sql.Identifier(table_name))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_ids,))
        rows = await cur.fetchall()
    out: dict[str, str] = {}
    for row in rows:
        eid = row.get("external_id")
        code = row.get("access_error_code")
        if eid is not None and code is not None:
            out[str(eid)] = str(code)
    return out


async def upsert_harvester_peer_permissions(
    conn: AsyncConnection,
    row: TelegramHarvesterPeerPermissionsSchema,
    *,
    table_name: str = "telegram_harvester_peer_permissions",
    conflict_columns: tuple[str, ...] = ("harvester_id", "external_id"),
) -> None:
    values: Mapping[str, Any] = row.model_dump(mode="json", exclude_none=True)
    columns = list(values.keys())
    query = build_upsert_sql(
        table=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
    )
    params = tuple(adapt_param(values[c]) for c in columns)

    async with conn.cursor() as cur:
        await cur.execute(query, params)
