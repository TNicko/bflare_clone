from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple
from uuid import UUID

from db.schemas.telegram.harvester_session import (
    SessionAuthStateSchema,
    SessionStatusSchema,
    SessionTypeSchema,
    TelegramHarvesterSessionSchema,
)
from telegram.session_crypto import decrypt_bytes, decrypt_text, encrypt_bytes, encrypt_text
from telegram.session_env import LEASE_TTL_SECONDS

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row


_SESSION_COLUMNS = """
    id, harvester_id, label, session_type, auth_state, status,
    session_string, auth_key, dc_id, server_address, port,
    leased_by_location_id, leased_until, created_at, updated_at
"""


def _session_from_row(row: dict[str, Any]) -> TelegramHarvesterSessionSchema:
    auth_key = row.get("auth_key")
    if isinstance(auth_key, memoryview):
        auth_key = auth_key.tobytes()
    return TelegramHarvesterSessionSchema(
        id=row["id"],
        harvester_id=row["harvester_id"],
        label=row["label"],
        session_type=row["session_type"],
        auth_state=row["auth_state"],
        status=row["status"],
        session_string=row.get("session_string"),
        auth_key=auth_key,
        dc_id=row.get("dc_id"),
        server_address=row.get("server_address"),
        port=row.get("port"),
        leased_by_location_id=row.get("leased_by_location_id"),
        leased_until=row.get("leased_until"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


async def list_sessions_for_harvester(
    conn: AsyncConnection,
    harvester_id: UUID,
) -> list[TelegramHarvesterSessionSchema]:
    query = f"""
        SELECT {_SESSION_COLUMNS}
        FROM telegram_harvester_sessions
        WHERE harvester_id = %s
        ORDER BY session_type, label
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (harvester_id,))
        rows = await cur.fetchall()
    return [_session_from_row(row) for row in rows]


async def get_session_by_id(
    conn: AsyncConnection[DictRow],
    session_id: UUID,
) -> TelegramHarvesterSessionSchema | None:
    query = f"""
        SELECT {_SESSION_COLUMNS}
        FROM telegram_harvester_sessions
        WHERE id = %s
        LIMIT 1
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (session_id,))
        row = await cur.fetchone()
    return _session_from_row(row) if row else None


async def insert_session_row(
    conn: AsyncConnection[DictRow],
    *,
    harvester_id: UUID,
    label: str,
    session_type: SessionTypeSchema,
    auth_state: SessionAuthStateSchema = "healthy",
    status: SessionStatusSchema = "inactive",
) -> UUID:
    query = """
        INSERT INTO telegram_harvester_sessions (
            harvester_id, label, session_type, auth_state, status
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (harvester_id, label, session_type, auth_state, status))
        row = await cur.fetchone()
    assert row is not None
    return row["id"]


async def persist_session_auth(
    conn: AsyncConnection,
    session_id: UUID,
    *,
    session_string: str,
    auth_key: bytes,
    dc_id: int,
    server_address: str,
    port: int,
) -> None:
    query = """
        UPDATE telegram_harvester_sessions
        SET session_string = %s,
            auth_key = %s,
            dc_id = %s,
            server_address = %s,
            port = %s,
            auth_state = 'healthy',
            last_error = NULL,
            last_error_at = NULL,
            updated_at = NOW()
        WHERE id = %s
    """
    async with conn.cursor() as cur:
        await cur.execute(
            query,
            (
                encrypt_text(session_string),
                encrypt_bytes(auth_key),
                dc_id,
                server_address,
                port,
                session_id,
            ),
        )


async def load_session_auth_strings(
    conn: AsyncConnection,
    session_id: UUID,
) -> tuple[str, bytes]:
    query = """
        SELECT session_string, auth_key
        FROM telegram_harvester_sessions
        WHERE id = %s
        LIMIT 1
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (session_id,))
        row = await cur.fetchone()
    if row is None or not row.get("session_string") or row.get("auth_key") is None:
        raise ValueError(f"Session id={session_id} has no auth payload")
    auth_key_encrypted: bytes = row["auth_key"]
    if isinstance(auth_key_encrypted, memoryview):
        auth_key_encrypted = auth_key_encrypted.tobytes()
    return decrypt_text(row["session_string"]), decrypt_bytes(auth_key_encrypted)


async def clear_session_auth(
    conn: AsyncConnection[DictRow],
    session_id: UUID,
) -> None:
    query = """
        UPDATE telegram_harvester_sessions
        SET session_string = NULL,
            auth_key = NULL,
            dc_id = NULL,
            server_address = NULL,
            port = NULL,
            auth_state = 'reset_required',
            updated_at = NOW()
        WHERE id = %s
    """
    async with conn.cursor() as cur:
        await cur.execute(query, (session_id,))


async def mark_session_auth_reset_required(
    conn: AsyncConnection,
    session_id: UUID,
    *,
    error_message: str | None = None,
) -> None:
    query = """
        UPDATE telegram_harvester_sessions
        SET auth_state = 'reset_required',
            status = 'inactive',
            leased_by_location_id = NULL,
            leased_until = NULL,
            last_error = COALESCE(%s::text, last_error),
            last_error_at = CASE
              WHEN %s::text IS NOT NULL THEN NOW()
              ELSE last_error_at
            END,
            updated_at = NOW()
        WHERE id = %s
    """
    msg = (error_message or "").strip() or None
    async with conn.cursor() as cur:
        await cur.execute(query, (msg, msg, session_id))
    await delete_all_lease_holders(conn, session_id)


async def delete_all_lease_holders(conn: AsyncConnection, session_id: UUID) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM telegram_harvester_session_lease_holders WHERE session_id = %s",
            (session_id,),
        )


class LeaseExpiryResult(NamedTuple):
    """Result of sweeping stale per-holder heartbeats and empty/expired sessions."""

    stale_holders_removed: int
    sessions_released: int

    def __bool__(self) -> bool:
        return self.stale_holders_removed > 0 or self.sessions_released > 0


async def expire_stale_session_leases(conn: AsyncConnection[DictRow]) -> LeaseExpiryResult:
    """Expire individual lease holders by heartbeat age, then release empty sessions.

    1. Drop holders whose ``last_heartbeat_at`` is older than ``LEASE_TTL_SECONDS``
    2. Mark active sessions inactive when they have no holders left, or when
       session-level ``leased_until`` has passed.

    Concurrent callers are safe via FOR UPDATE SKIP LOCKED on session rows.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            DELETE FROM telegram_harvester_session_lease_holders
            WHERE last_heartbeat_at < NOW() - make_interval(secs => %s)
            RETURNING session_id, server_id
            """,
            (LEASE_TTL_SECONDS,),
        )
        stale_holders = await cur.fetchall()

        await cur.execute(
            """
            WITH to_expire AS (
                SELECT s.id
                FROM telegram_harvester_sessions s
                WHERE s.status = 'active'
                  AND (
                    (s.leased_until IS NOT NULL AND s.leased_until < NOW())
                    OR NOT EXISTS (
                        SELECT 1
                        FROM telegram_harvester_session_lease_holders h
                        WHERE h.session_id = s.id
                    )
                )
                FOR UPDATE SKIP LOCKED
            ),
            deleted_holders AS (
                DELETE FROM telegram_harvester_session_lease_holders
                WHERE session_id IN (SELECT id FROM to_expire)
            )
            UPDATE telegram_harvester_sessions s
            SET status = 'inactive',
                leased_by_location_id = NULL,
                leased_until = NULL,
                updated_at = NOW()
            FROM to_expire e
            WHERE s.id = e.id
            RETURNING s.id
            """
        )
        released_sessions = await cur.fetchall()

    return LeaseExpiryResult(
        stale_holders_removed=len(stale_holders),
        sessions_released=len(released_sessions),
    )


async def harvester_is_active(conn: AsyncConnection, harvester_id: UUID) -> bool:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT is_active FROM telegram_harvesters WHERE id = %s LIMIT 1",
            (harvester_id,),
        )
        row = await cur.fetchone()
    return bool(row and row["is_active"])


async def find_joinable_session(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    session_type: SessionTypeSchema,
    location_id: str,
    server_id: str,
) -> UUID | None:
    """Return a session this server already holds.

    Another process on the same machine must not attach. Two Telethon clients
    on one auth key make Telegram close both connections.
    """
    query = """
        SELECT s.id
        FROM telegram_harvester_sessions s
        WHERE s.harvester_id = %s
          AND s.session_type = %s
          AND s.auth_state = 'healthy'
          AND s.status = 'active'
          AND s.leased_by_location_id = %s
          AND (s.leased_until IS NULL OR s.leased_until > NOW())
          AND EXISTS (
            SELECT 1
            FROM telegram_harvester_session_lease_holders h
            WHERE h.session_id = s.id
              AND h.server_id = %s
          )
          AND NOT EXISTS (
            SELECT 1
            FROM telegram_harvester_session_lease_holders h
            WHERE h.session_id = s.id
              AND h.server_id <> %s
          )
        ORDER BY s.updated_at
        LIMIT 1
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            query,
            (harvester_id, session_type, location_id, server_id, server_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def claim_inactive_session(
    conn: AsyncConnection,
    *,
    harvester_id: UUID,
    session_type: SessionTypeSchema,
    location_id: str,
) -> UUID | None:
    leased_until = datetime.now(UTC) + timedelta(seconds=LEASE_TTL_SECONDS)
    query = """
       WITH claimed AS (
            UPDATE telegram_harvester_sessions
            SET status = 'active',
                leased_by_location_id = %s,
                leased_until = %s,
                updated_at = NOW()
            WHERE id = (
                SELECT id
                FROM telegram_harvester_sessions
                WHERE harvester_id = %s
                  AND session_type = %s
                  AND auth_state = 'healthy'
                  AND (
                    status = 'inactive'
                    OR (status = 'active' AND leased_until IS NOT NULL AND leased_until < NOW())
                  )
                ORDER BY
                  CASE WHEN status = 'inactive' THEN 0 ELSE 1 END,
                  updated_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        ),
        deleted_holders AS (
            DELETE FROM telegram_harvester_session_lease_holders
            WHERE session_id IN (SELECT id FROM claimed)
        )
        SELECT id FROM claimed
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            query,
            (location_id, leased_until, harvester_id, session_type),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def upsert_lease_holder(
    conn: AsyncConnection,
    *,
    session_id: UUID,
    server_id: str,
    location_id: str,
) -> bool:
    """Record/refresh a lease holder. Returns False if this location no longer owns the lease."""
    leased_until = datetime.now(UTC) + timedelta(seconds=LEASE_TTL_SECONDS)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE telegram_harvester_sessions
            SET leased_until = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'active'
              AND leased_by_location_id = %s
              AND (leased_until IS NULL OR leased_until > NOW())
            RETURNING id
            """,
            (leased_until, session_id, location_id),
        )
        row = await cur.fetchone()
        if row is None:
            return False
        await cur.execute(
            """
            INSERT INTO telegram_harvester_session_lease_holders (
                session_id, server_id, location_id, last_heartbeat_at, joined_at
            )
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (session_id, server_id)
            DO UPDATE SET
                location_id = EXCLUDED.location_id,
                last_heartbeat_at = NOW()
            """,
            (session_id, server_id, location_id),
        )
    return True


async def remove_lease_holder(
    conn: AsyncConnection,
    *,
    session_id: UUID,
    server_id: str,
    joined_before: datetime | None = None,
) -> bool:
    """Drop this server's holder row; mark the session inactive when none remain.

    ``joined_before`` (agent backup after cancel) only deletes a holder that
    existed before the job finished, so a later job cannot have its new lease
    stolen if it reused the same session + server_id.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        if joined_before is None:
            await cur.execute(
                """
                DELETE FROM telegram_harvester_session_lease_holders
                WHERE session_id = %s AND server_id = %s
                """,
                (session_id, server_id),
            )
        else:
            await cur.execute(
                """
                DELETE FROM telegram_harvester_session_lease_holders
                WHERE session_id = %s AND server_id = %s AND joined_at <= %s
                """,
                (session_id, server_id, joined_before),
            )
        await cur.execute(
            """
            SELECT COUNT(*)::int AS cnt
            FROM telegram_harvester_session_lease_holders
            WHERE session_id = %s
            """,
            (session_id,),
        )
        row = await cur.fetchone()
    remaining = int(row["cnt"]) if row else 0
    if remaining == 0:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE telegram_harvester_sessions
                SET status = 'inactive',
                    leased_by_location_id = NULL,
                    leased_until = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (session_id,),
            )
        return True
    return False


async def load_update_states(
    conn: AsyncConnection,
    session_id: UUID,
) -> list[dict[str, Any]]:
    query = """
        SELECT entity_id, pts, qts, seq, state_date
        FROM telegram_harvester_session_update_states
        WHERE session_id = %s
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (session_id,))
        return list(await cur.fetchall())


async def save_update_states(
    conn: AsyncConnection,
    session_id: UUID,
    states: list[tuple[int, int, int, int, datetime]],
) -> None:
    if not states:
        return
    async with conn.cursor() as cur:
        for entity_id, pts, qts, seq, state_date in states:
            await cur.execute(
                """
                INSERT INTO telegram_harvester_session_update_states (
                    session_id, entity_id, pts, qts, seq, state_date, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (session_id, entity_id)
                DO UPDATE SET
                    pts = EXCLUDED.pts,
                    qts = EXCLUDED.qts,
                    seq = EXCLUDED.seq,
                    state_date = EXCLUDED.state_date,
                    updated_at = NOW()
                """,
                (session_id, entity_id, pts, qts, seq, state_date),
            )


async def delete_update_states(conn: AsyncConnection, session_id: UUID) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM telegram_harvester_session_update_states WHERE session_id = %s",
            (session_id,),
        )
