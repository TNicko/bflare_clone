"""Pick a harvester session that can actually open a peer.

Access hashes belong to one Telegram account. When that account's API session
is unauthorized, try the next stored role and use that harvester's own hash.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from telethon import TelegramClient

from telegram.api_session_pool import ApiSessionPool

lg = logging.getLogger(__name__)


async def list_peer_access_candidates(
    conn: Any,
    external_id: str,
) -> list[tuple[UUID, int, str | None]]:
    """Active harvesters that store an access hash for this peer, best first."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT r.harvester_id, r.access_hash, h.external_id AS harvester_external_id
            FROM telegram_harvester_peer_roles r
            INNER JOIN telegram_harvesters h
                ON h.id = r.harvester_id
               AND h.is_active = true
            WHERE r.external_id = %s
              AND r.access_hash IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM telegram_harvester_sessions hs
                WHERE hs.harvester_id = h.id
                  AND hs.auth_state = 'healthy'
              )
            ORDER BY
                CASE WHEN r.role = 'parent' THEN 0 ELSE 1 END,
                r.updated_at DESC NULLS LAST
            """,
            (external_id,),
        )
        rows = await cur.fetchall()
    out: list[tuple[UUID, int, str | None]] = []
    for row in rows:
        raw_id = row["harvester_id"]
        hid = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
        ext = row.get("harvester_external_id")
        out.append((hid, int(row["access_hash"]), str(ext) if ext is not None else None))
    return out


async def connected_peer_role(
    pool: ApiSessionPool,
    conn: Any,
    *,
    peer_external_id: str,
    skip_harvester_ids: set[UUID],
) -> tuple[TelegramClient, UUID, int, str | None] | None:
    """First candidate whose API session connects, skipping ones already tried."""
    for hid, access_hash, external_id in await list_peer_access_candidates(conn, peer_external_id):
        if hid in skip_harvester_ids:
            continue
        client = await pool.get_client(conn, hid)
        if client is None:
            continue
        lg.info(
            "Peer access using alternate harvester external_id=%s harvester_id=%s skipped=%s",
            peer_external_id,
            hid,
            ",".join(str(item) for item in skip_harvester_ids) or "-",
        )
        return client, hid, access_hash, external_id
    return None
