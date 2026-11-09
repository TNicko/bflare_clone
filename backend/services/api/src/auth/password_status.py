"""Detect whether a platform user still needs to choose a password.

Source of truth: ``auth.users.raw_user_meta_data.has_password``.

GoTrue may attach a temporary ``encrypted_password`` when an invite is
confirmed, so the DB hash alone is not reliable. We only treat the user as
set up when metadata explicitly has ``has_password: true`` (written after
/set-password or on a successful password login for legacy accounts).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row

# Must match client ``HAS_PASSWORD_KEY``.
_HAS_PASSWORD_KEY = "has_password"


def _meta_dict(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _metadata_has_password(raw_meta: Any) -> bool:
    return _meta_dict(raw_meta).get(_HAS_PASSWORD_KEY) is True


async def user_has_password(conn: AsyncConnection[DictRow], user_id: UUID) -> bool:
    """True when user_metadata.has_password is explicitly true."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT raw_user_meta_data
            FROM auth.users
            WHERE id = %s
            """,
            (user_id,),
        )
        row = await cur.fetchone()

    if not row:
        return False
    return _metadata_has_password(row.get("raw_user_meta_data"))


async def user_requires_password_setup(conn: AsyncConnection[DictRow], user_id: UUID) -> bool:
    """True when the user must finish /set-password before using the app."""
    return not await user_has_password(conn, user_id)
