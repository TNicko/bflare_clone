from __future__ import annotations

from typing import Any
from uuid import UUID

from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from db.crud.telegram.sessions import persist_session_auth


def auth_payload_from_string_session(session_string: str) -> tuple[bytes, int, str, int]:
    inner = StringSession(session_string)
    auth_key = inner.auth_key
    if not isinstance(auth_key, AuthKey):
        raise ValueError("StringSession has no auth key")

    dc_id = inner.dc_id
    server_address = inner.server_address
    port = inner.port
    if not dc_id or server_address is None or port is None:
        raise ValueError("StringSession is missing DC connection info")

    key_material = auth_key.key
    if not key_material:
        raise ValueError("StringSession auth key material is empty")

    return bytes(key_material), int(dc_id), str(server_address), int(port)


async def persist_session_from_string(
    conn: Any,
    session_id: UUID,
    session_string: str,
) -> None:
    auth_key, dc_id, server_address, port = auth_payload_from_string_session(session_string)
    await persist_session_auth(
        conn,
        session_id,
        session_string=session_string,
        auth_key=auth_key,
        dc_id=dc_id,
        server_address=server_address,
        port=port,
    )
