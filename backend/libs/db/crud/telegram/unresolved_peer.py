"""Light web-tier stubs for peers we know only by external_id (e.g. forward graph)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row

from db.crud.system.peer_seed import enqueue_peer_seed_quiet
from db.crud.telegram.channel import upsert_channel
from db.crud.telegram.chat import upsert_chat
from db.crud.telegram.peers import set_peer_unavailability
from db.schemas.telegram.channel import TelegramChannelSchema
from db.schemas.telegram.chat import TelegramChatSchema
from db.schemas.general.primitives import TelegramPeerTypeSchema

_TERMINAL_RESOLVE_ERROR_PREFIX = "no harvester access"


def unknown_peer_label(external_id: str) -> str:
    return f"Unknown [{external_id}]"


def is_terminal_resolve_error(error: str | None) -> bool:
    if not error:
        return False
    return error.strip().lower().startswith(_TERMINAL_RESOLVE_ERROR_PREFIX)


def _as_peer_type(raw: object) -> TelegramPeerTypeSchema:
    if raw in ("channel", "megagroup", "chat"):
        return raw  # type: ignore[return-value]
    return "channel"


def _title_for_stub(external_id: str, title: str | None) -> str:
    cleaned = (title or "").strip()
    return cleaned or unknown_peer_label(external_id)


_FIND_PEER_SQL = """
SELECT peer_type, profile_tier, title, unavailable_at IS NOT NULL AS is_unavailable
FROM (
  SELECT 'channel'::text AS peer_type, profile_tier, title, unavailable_at
  FROM telegram_channels
  WHERE external_id = %s
  UNION ALL
  SELECT 'megagroup'::text, profile_tier, title, unavailable_at
  FROM telegram_megagroups
  WHERE external_id = %s
  UNION ALL
  SELECT 'chat'::text, 'tg_full'::text, title, unavailable_at
  FROM telegram_chats
  WHERE external_id = %s
) AS peers
LIMIT 1
"""

_UNAVAILABLE_PEERS_SQL = """
SELECT external_id
FROM telegram_channels
WHERE external_id = ANY(%s)
  AND unavailable_at IS NOT NULL
UNION
SELECT external_id
FROM telegram_megagroups
WHERE external_id = ANY(%s)
  AND unavailable_at IS NOT NULL
UNION
SELECT external_id
FROM telegram_chats
WHERE external_id = ANY(%s)
  AND unavailable_at IS NOT NULL
"""


async def fetch_unavailable_peer_ids(conn: Any, external_ids: list[str]) -> set[str]:
    if not external_ids:
        return set()
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _UNAVAILABLE_PEERS_SQL,
            (external_ids, external_ids, external_ids),
        )
        rows = await cur.fetchall()
    return {str(row["external_id"]).strip() for row in rows if row.get("external_id")}


async def is_peer_unavailable(conn: Any, external_id: str) -> bool:
    found = await fetch_unavailable_peer_ids(conn, [external_id])
    return external_id in found


async def ensure_web_peer_stub(
    conn: Any,
    *,
    external_id: str,
    peer_type: str | None,
    title: str | None = None,
    harvester_id: UUID | None = None,
    mark_unavailable: bool = False,
) -> None:
    """Insert a web-tier row when missing; optionally mark the peer unavailable.

    Never downgrades an existing ``tg_min`` / ``tg_full`` profile. Title is only
    written on insert (or when replacing a placeholder Unknown title on web tier).
    """
    eid = str(external_id).strip()
    if not eid:
        return
    preferred = _as_peer_type(peer_type)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_FIND_PEER_SQL, (eid, eid, eid))
        existing = await cur.fetchone()

    resolved_type = _as_peer_type(existing["peer_type"]) if existing else preferred
    stub_title = _title_for_stub(eid, title)
    if mark_unavailable:
        # Dead-end display label — prefer Unknown even when we had a from_name hint.
        stub_title = unknown_peer_label(eid)

    inserted = False
    if existing is None:
        if resolved_type == "chat":
            if harvester_id is None:
                return
            await upsert_chat(
                conn,
                TelegramChatSchema.model_construct(
                    external_id=eid,
                    title=stub_title,
                    harvester_id=harvester_id,
                    participants_count=0,
                    version=0,
                    updated_at=datetime.now(UTC),
                ),
            )
            inserted = True
        else:
            table = "telegram_megagroups" if resolved_type == "megagroup" else "telegram_channels"
            await upsert_channel(
                conn,
                TelegramChannelSchema.model_construct(
                    external_id=eid,
                    title=stub_title,
                    profile_tier="web",
                    updated_at=datetime.now(UTC),
                    date=datetime.now(UTC),
                ),
                table_name=table,
            )
            inserted = True
    elif mark_unavailable:
        tier = str(existing.get("profile_tier") or "")
        old_title = str(existing.get("title") or "").strip()
        if tier == "web" and (not old_title or old_title.startswith("Unknown [")):
            table = "telegram_megagroups" if resolved_type == "megagroup" else "telegram_channels"
            if resolved_type != "chat":
                await upsert_channel(
                    conn,
                    TelegramChannelSchema.model_construct(
                        external_id=eid,
                        title=stub_title,
                        profile_tier="web",
                        updated_at=datetime.now(UTC),
                    ),
                    table_name=table,
                )

    if mark_unavailable and (existing is None or not existing.get("is_unavailable")):
        await set_peer_unavailability(
            conn,
            eid,
            peer_type=resolved_type,
            unavailable=True,
        )
    if inserted and not mark_unavailable:
        await enqueue_peer_seed_quiet(
            conn,
            external_id=eid,
            peer_type=resolved_type,
        )
