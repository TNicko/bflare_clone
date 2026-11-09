"""WebSocket endpoint for user-owned Telegram session initialization."""

from __future__ import annotations

import logging
import shutil
import tempfile
from uuid import UUID

from fastapi import APIRouter, WebSocket
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.types import User

from psycopg.errors import UniqueViolation

from services.api.src.config import ConfigFactory
from services.api.src.auth.middleware import require_ws_user
from services.api.src.crud.supabase import get_conn
from services.api.src.crud.supabase.user_accounts import (
    insert_user_session_row,
    list_sessions_for_account,
    persist_user_session_from_string,
    upsert_user_messenger_account,
)
from telegram.client import session_string_from_telethon_client

lg = logging.getLogger(__name__)

router = APIRouter()


class SessionInit(BaseModel):
    phone_number: str
    label: str = "default"
    account_external_id: str | None = None


class SessionResetInit(BaseModel):
    phone_number: str
    session_id: str


async def _sign_in_user(
    client: TelegramClient,
    websocket: WebSocket,
    *,
    phone_number: str,
) -> User | None:
    await client.sign_in(phone_number)

    await websocket.send_json({"message": "Enter code"})
    data = await websocket.receive_json()
    code = data.get("code")
    if not code:
        await websocket.send_json({"error": "Missing 'code' in request"})
        return None

    user: User | None = None
    try:
        result = await client.sign_in(phone_number, code)
        if isinstance(result, User):
            user = result
    except SessionPasswordNeededError:
        await websocket.send_json({"message": "2FA is enabled. Enter 2FA password"})
        data = await websocket.receive_json()
        password = data.get("password")
        if not password:
            await websocket.send_json({"error": "Missing 'password' in request"})
            return None
        password_result = await client.sign_in(password=password)
        if isinstance(password_result, User):
            user = password_result

    if user is None:
        me = await client.get_me()
        if isinstance(me, User):
            user = me
    return user


async def _assert_session_label_available(
    conn,
    *,
    account_id: UUID,
    label: str,
) -> str:
    normalized_label = label.strip() or "default"
    existing = await list_sessions_for_account(conn, account_id)
    if any(s["label"] == normalized_label for s in existing):
        raise ValueError(
            f"Session label {normalized_label!r} already exists for this account; choose a different label."
        )
    return normalized_label


async def _cleanup_session_ws(
    *,
    client: TelegramClient | None,
    tmp_dir: str,
    websocket: WebSocket,
) -> None:
    if client is not None and client.is_connected():
        disconnect_result = client.disconnect()
        if disconnect_result is not None:
            await disconnect_result
    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        await websocket.close(code=1000)
    except Exception:
        pass


@router.websocket("/accounts/session/initialization")
async def initialize_user_session(websocket: WebSocket) -> None:
    await websocket.accept()
    platform_user = await require_ws_user(websocket)
    if platform_user is None:
        return

    config = ConfigFactory.get_config()
    client: TelegramClient | None = None
    tmp_dir = tempfile.mkdtemp(prefix="user_session_")
    try:
        data = await websocket.receive_json()
        session_init = SessionInit(**data)

        client = TelegramClient(
            StringSession(),
            config.telegram_api_id,
            config.telegram_api_hash,
        )
        await client.connect()
        user = await _sign_in_user(
            client,
            websocket,
            phone_number=session_init.phone_number,
        )
        if user is None or not user.id:
            return

        external_id = session_init.account_external_id or str(user.id)
        session_string = session_string_from_telethon_client(client)

        async with get_conn() as conn:
            account_id = await upsert_user_messenger_account(
                conn,
                user_id=platform_user.id,
                external_id=external_id,
                phone_number=user.phone or session_init.phone_number or None,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name,
            )
            normalized_label = await _assert_session_label_available(
                conn,
                account_id=account_id,
                label=session_init.label,
            )
            try:
                session_id = await insert_user_session_row(
                    conn,
                    account_id=account_id,
                    user_id=platform_user.id,
                    label=normalized_label,
                )
            except Exception as exc:
                if UniqueViolation is not None and isinstance(exc, UniqueViolation):
                    raise ValueError(f"Session label {normalized_label!r} already exists for this account.") from exc
                raise
            if session_id is None:
                raise ValueError("Account not found for this user")
            persisted = await persist_user_session_from_string(
                conn,
                session_id,
                session_string,
                user_id=platform_user.id,
            )
            if not persisted:
                raise ValueError("Failed to persist session for this user")
            await conn.commit()
            lg.info(
                "Persisted user session id=%s account=%s label=%s user=%s",
                session_id,
                account_id,
                normalized_label,
                platform_user.id,
            )
    except ValueError as e:
        lg.warning("User session initialization rejected: %s", e)
        try:
            await websocket.send_json({"error": "Session setup failed"})
            await websocket.close(code=1011, reason="Session initialization failed")
        except Exception:
            pass
    except Exception:
        lg.exception("User session initialization failed")
        try:
            await websocket.send_json({"error": "Session setup failed"})
            await websocket.close(code=1011, reason="Session initialization failed")
        except Exception:
            pass
        raise
    finally:
        await _cleanup_session_ws(client=client, tmp_dir=tmp_dir, websocket=websocket)
