"""Read/write user_messenger_accounts and user_messenger_sessions."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from telegram.session_crypto import encrypt_bytes, encrypt_text

SessionAuthState = Literal["healthy", "error", "reset_required"]
SessionStatus = Literal["inactive", "active"]


async def upsert_user_messenger_account(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    external_id: str,
    messenger: str = "telegram",
    phone_number: str | None = None,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    label: str | None = None,
) -> UUID:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO user_messenger_accounts (
                user_id, messenger, external_id, label, phone_number,
                username, first_name, last_name, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (user_id, messenger, external_id)
            DO UPDATE SET
                phone_number = EXCLUDED.phone_number,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                label = COALESCE(EXCLUDED.label, user_messenger_accounts.label),
                updated_at = NOW()
            RETURNING id
            """,
            (
                user_id,
                messenger,
                external_id,
                label,
                phone_number,
                username,
                first_name,
                last_name,
            ),
        )
        row = await cur.fetchone()
    assert row is not None
    return row["id"]


async def list_accounts_for_user(
    conn: AsyncConnection[DictRow],
    user_id: UUID,
) -> list[dict[str, Any]]:
    query = """
        SELECT id, user_id, messenger, external_id, label, phone_number,
               username, first_name, last_name, is_active, created_at, updated_at
        FROM user_messenger_accounts
        WHERE user_id = %s
        ORDER BY created_at DESC NULLS LAST, external_id
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (user_id,))
        return list(await cur.fetchall())


async def list_sessions_for_account(
    conn: AsyncConnection[DictRow],
    account_id: UUID,
) -> list[dict[str, Any]]:
    query = """
        SELECT id, account_id, label, session_type, auth_state, status,
               dc_id, server_address, port, created_at, updated_at
        FROM user_messenger_sessions
        WHERE account_id = %s
        ORDER BY label
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (account_id,))
        return list(await cur.fetchall())


async def get_account_for_user(
    conn: AsyncConnection[DictRow],
    *,
    account_id: UUID,
    user_id: UUID,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, user_id, messenger, external_id, label, phone_number,
                   username, first_name, last_name, is_active, created_at, updated_at
            FROM user_messenger_accounts
            WHERE id = %s AND user_id = %s
            """,
            (account_id, user_id),
        )
        return await cur.fetchone()


async def insert_user_session_row(
    conn: AsyncConnection[DictRow],
    *,
    account_id: UUID,
    user_id: UUID,
    label: str,
    auth_state: SessionAuthState = "healthy",
    status: SessionStatus = "inactive",
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO user_messenger_sessions (
                account_id, label, session_type, auth_state, status
            )
            SELECT a.id, %s, 'user', %s, %s
            FROM user_messenger_accounts a
            WHERE a.id = %s AND a.user_id = %s
            RETURNING id
            """,
            (label, auth_state, status, account_id, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def persist_user_session_auth(
    conn: AsyncConnection[DictRow],
    session_id: UUID,
    *,
    user_id: UUID,
    session_string: str,
    auth_key: bytes,
    dc_id: int,
    server_address: str,
    port: int,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE user_messenger_sessions s
            SET session_string = %s,
                auth_key = %s,
                dc_id = %s,
                server_address = %s,
                port = %s,
                auth_state = 'healthy',
                updated_at = NOW()
            FROM user_messenger_accounts a
            WHERE s.id = %s
              AND s.account_id = a.id
              AND a.user_id = %s
            """,
            (
                encrypt_text(session_string),
                encrypt_bytes(auth_key),
                dc_id,
                server_address,
                port,
                session_id,
                user_id,
            ),
        )
        return cur.rowcount > 0


async def persist_user_session_from_string(
    conn: AsyncConnection[DictRow],
    session_id: UUID,
    session_string: str,
    *,
    user_id: UUID,
) -> bool:
    from telegram.session import auth_payload_from_string_session

    auth_key, dc_id, server_address, port = auth_payload_from_string_session(session_string)
    return await persist_user_session_auth(
        conn,
        session_id,
        user_id=user_id,
        session_string=session_string,
        auth_key=auth_key,
        dc_id=dc_id,
        server_address=server_address,
        port=port,
    )


async def get_user_session_by_id(
    conn: AsyncConnection[DictRow],
    session_id: UUID,
    *,
    user_id: UUID,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT s.id, s.account_id, s.label, s.session_type, s.auth_state, s.status,
                   s.dc_id, s.server_address, s.port, s.created_at, s.updated_at,
                   a.user_id
            FROM user_messenger_sessions s
            INNER JOIN user_messenger_accounts a ON a.id = s.account_id
            WHERE s.id = %s AND a.user_id = %s
            """,
            (session_id, user_id),
        )
        return await cur.fetchone()


async def clear_user_session_auth(
    conn: AsyncConnection[DictRow],
    session_id: UUID,
    *,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE user_messenger_sessions s
            SET session_string = NULL,
                auth_key = NULL,
                dc_id = NULL,
                server_address = NULL,
                port = NULL,
                auth_state = 'reset_required',
                updated_at = NOW()
            FROM user_messenger_accounts a
            WHERE s.id = %s
              AND s.account_id = a.id
              AND a.user_id = %s
            """,
            (session_id, user_id),
        )
        return cur.rowcount > 0


async def list_sessions_for_user_account(
    conn: AsyncConnection[DictRow],
    *,
    account_id: UUID,
    user_id: UUID,
) -> list[dict[str, Any]]:
    account = await get_account_for_user(conn, account_id=account_id, user_id=user_id)
    if account is None:
        return []
    return await list_sessions_for_account(conn, account_id)
