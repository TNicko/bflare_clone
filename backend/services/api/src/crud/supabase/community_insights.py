"""Aggregations for the community overview insights panel.

Message-scoped queries use ``source_external_id`` first (``telegram_messages_source_date_idx``).
Forward sources read pre-aggregated ``telegram_forward_edges`` outbound rows.
Inbound ``forwarded_by`` rows (communities that forward this peer) come from the
same query.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row

from db.crud.telegram.forward_edges import ForwardEdgeRef, list_peer_forward_neighbors
from services.api.src.config import ConfigFactory
from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.crud.supabase.peers import (
    get_community_views_by_external_ids,
    get_partial_communities_by_external_ids,
    get_partial_telegram_users_by_external_ids,
)
from services.api.src.message_history_coverage import (
    gated_recent_volume_counts,
    gated_total_count,
    peer_latest_coverage_before,
)
from services.api.src.message_media import build_message_media_preview
from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.models.community_insights import (
    ActivitySnapshot,
    ActorCountInsight,
    CommunityInsightsResponse,
    ForwardSourceInsight,
    ParticipantsPage,
    RelatedChannelInsight,
    SimilarChannelsPage,
    TopViewedPostInsight,
)

_RELATED_PREVIEW_LIMIT = 5
_DEFAULT_FORWARDS_LIMIT = 5
_DEFAULT_PARTICIPANTS_LIMIT = 5
_DEFAULT_VIEWED_LIMIT = 3
_MAX_LIST_LIMIT = 20
_MAX_SIMILAR_PAGE_LIMIT = 100
_MAX_PARTICIPANTS_PAGE_LIMIT = 100
_SIMILAR_TABLES = {
    # Recommendations pipeline writes only to broadcast channels.
    "channel": "telegram_channels",
}


async def count_peer_recent_volumes(
    conn: AsyncConnection[DictRow],
    external_id: str,
) -> tuple[int, int]:
    """Raw 7d / 30d post counts (non-deleted, non-action) for a peer."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              COUNT(*) FILTER (
                WHERE date >= NOW() - INTERVAL '7 days'
              )::int AS messages_7d,
              COUNT(*)::int AS messages_30d
            FROM telegram_messages
            WHERE source_external_id = %s
              AND COALESCE(deleted, false) = false
              AND action_type IS NULL
              AND date >= NOW() - INTERVAL '30 days'
            """,
            (external_id,),
        )
        volume = await cur.fetchone() or {"messages_7d": 0, "messages_30d": 0}
    return int(volume.get("messages_7d") or 0), int(volume.get("messages_30d") or 0)


async def peer_activity_volume_counts(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
) -> tuple[int | None, int | None]:
    """Coverage-gated 7d / 30d post counts for activity widgets / sync responses."""
    raw_7d, raw_30d = await count_peer_recent_volumes(conn, external_id)
    return await gated_recent_volume_counts(
        conn,
        peer_external_id=external_id,
        peer_type=peer_type,
        messages_7d=raw_7d,
        messages_30d=raw_30d,
    )


def clamp_insights_limit(value: int | None, *, default: int) -> int:
    if value is None:
        return default
    return max(1, min(int(value), _MAX_LIST_LIMIT))


def clamp_similar_page_limit(value: int | None, *, default: int = 50) -> int:
    if value is None:
        return default
    return max(1, min(int(value), _MAX_SIMILAR_PAGE_LIMIT))


def clamp_participants_page_limit(value: int | None, *, default: int = 50) -> int:
    if value is None:
        return default
    return max(1, min(int(value), _MAX_PARTICIPANTS_PAGE_LIMIT))


async def _load_similar_channel_refs(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    table = _SIMILAR_TABLES.get(peer_type)
    if table is None:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            sql.SQL(
                """
                SELECT similar_channels
                FROM {}
                WHERE external_id = %s
                LIMIT 1
                """
            ).format(sql.Identifier(table)),
            (external_id,),
        )
        row = await cur.fetchone()
    if not row:
        return []
    raw = row.get("similar_channels")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        eid = item.get("external_id")
        if eid is None:
            continue
        out.append(
            {
                "external_id": str(eid),
                "peer_type": str(item.get("peer_type") or "channel"),
                "persisted": bool(item.get("persisted")),
            }
        )
        if limit is not None and len(out) >= limit:
            break
    return out


async def _related_channels(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
) -> tuple[list[RelatedChannelInsight], int]:
    refs = await _load_similar_channel_refs(conn, external_id=external_id, peer_type=peer_type, limit=None)
    total = len(refs)
    preview = refs[:_RELATED_PREVIEW_LIMIT]
    if not preview:
        return [], total
    ids = [r["external_id"] for r in preview]
    peers = await get_community_views_by_external_ids(conn, ids)
    return [
        RelatedChannelInsight(
            external_id=ref["external_id"],
            peer_type=(peers[ref["external_id"]].peer_type if ref["external_id"] in peers else ref["peer_type"]),
            persisted=ref["persisted"] or ref["external_id"] in peers,
            peer=peers.get(ref["external_id"]),
        )
        for ref in preview
    ], total


async def list_similar_channels(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    limit: int | None = None,
    offset: int = 0,
    q: str | None = None,
) -> SimilarChannelsPage:
    """Paginated similar channels as ``CommunityView`` rows for the list page."""
    page_limit = clamp_similar_page_limit(limit)
    page_offset = max(0, int(offset))
    refs = await _load_similar_channel_refs(conn, external_id=external_id, peer_type=peer_type, limit=None)
    if not refs:
        return SimilarChannelsPage(items=[], total=0, limit=page_limit, offset=page_offset)

    ids = [r["external_id"] for r in refs]
    peers = await get_community_views_by_external_ids(conn, ids)
    items: list[CommunityView] = []
    for ref in refs:
        peer = peers.get(ref["external_id"])
        if peer is not None:
            items.append(peer)
        else:
            pt = ref["peer_type"]
            if pt not in ("channel", "megagroup", "chat"):
                pt = "channel"
            items.append(
                CommunityView.unknown(
                    peer_type=pt,
                    external_id=ref["external_id"],
                )
            )

    needle = (q or "").strip().lower()
    if needle:
        filtered: list[CommunityView] = []
        for item in items:
            hay = " ".join(
                part
                for part in (
                    item.title,
                    item.username or "",
                    item.external_id,
                    *(c.label for c in item.categories),
                    *(c.slug for c in item.categories),
                )
                if part
            ).lower()
            if needle in hay:
                filtered.append(item)
        items = filtered

    total = len(items)
    page_items = items[page_offset : page_offset + page_limit]
    return SimilarChannelsPage(items=page_items, total=total, limit=page_limit, offset=page_offset)


async def _forward_neighbors(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    limit: int,
) -> tuple[list[ForwardSourceInsight], int, list[ForwardSourceInsight], int]:
    """Outbound sources and inbound amplifiers from ``telegram_forward_edges``.

    One query returns both directions (top-N each) plus full neighbor counts.
    """
    neighbors = await list_peer_forward_neighbors(conn, peer_external_id=external_id, limit=limit)
    ids: list[str] = []
    seen: set[str] = set()
    for edge in (*neighbors.outbound, *neighbors.inbound):
        if edge.external_id in seen:
            continue
        seen.add(edge.external_id)
        ids.append(edge.external_id)
    peers = await get_partial_communities_by_external_ids(conn, ids) if ids else {}

    def _insight(edge: ForwardEdgeRef, *, include_from_name: bool) -> ForwardSourceInsight:
        return ForwardSourceInsight(
            external_id=edge.external_id,
            forward_count=edge.count,
            from_name=edge.from_name if include_from_name else None,
            peer=peers.get(edge.external_id),
        )

    sources = [_insight(edge, include_from_name=True) for edge in neighbors.outbound]
    forwarded_by = [_insight(edge, include_from_name=False) for edge in neighbors.inbound]
    return sources, neighbors.outbound_total, forwarded_by, neighbors.inbound_total


async def _enrich_actor_rows(
    conn: AsyncConnection[DictRow],
    rows: list[dict[str, Any]],
) -> list[ActorCountInsight]:
    actor_ids = sorted({str(r["external_id"]) for r in rows if r.get("external_id")})
    if not actor_ids:
        return []
    users = await get_partial_telegram_users_by_external_ids(conn, actor_ids)
    missing = [eid for eid in actor_ids if eid not in users]
    peers = await get_partial_communities_by_external_ids(conn, missing)
    out: list[ActorCountInsight] = []
    for r in rows:
        eid = str(r["external_id"])
        raw_rank = r.get("rank")
        rank = str(raw_rank).strip() if raw_rank is not None and str(raw_rank).strip() else None
        out.append(
            ActorCountInsight(
                external_id=eid,
                count=int(r["count"] or 0),
                user=users.get(eid),
                peer=peers.get(eid),
                rank=rank,
            )
        )
    return out


async def _current_member_count(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    q: str = "",
) -> int:
    needle = q.strip()
    async with conn.cursor(row_factory=dict_row) as cur:
        if needle:
            like = f"%{needle}%"
            await cur.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM telegram_chat_participants tcp
                LEFT JOIN telegram_users tu ON tu.external_id = tcp.user_id
                WHERE tcp.external_id = %s
                  AND tcp.is_current_member = true
                  AND (
                    tcp.user_id ILIKE %s
                    OR COALESCE(tu.username, '') ILIKE %s
                    OR COALESCE(tu.first_name, '') ILIKE %s
                    OR COALESCE(tu.last_name, '') ILIKE %s
                    OR CONCAT_WS(' ', tu.first_name, tu.last_name) ILIKE %s
                  )
                """,
                (external_id, like, like, like, like, like),
            )
        else:
            await cur.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM telegram_chat_participants
                WHERE external_id = %s
                  AND is_current_member = true
                """,
                (external_id,),
            )
        row = await cur.fetchone()
    return int((row or {}).get("n") or 0)


async def _message_poster_total(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    q: str = "",
) -> int:
    needle = q.strip()
    async with conn.cursor(row_factory=dict_row) as cur:
        if needle:
            like = f"%{needle}%"
            await cur.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM (
                  SELECT DISTINCT m.from_external_id AS external_id
                  FROM telegram_messages m
                  LEFT JOIN telegram_users tu ON tu.external_id = m.from_external_id
                  LEFT JOIN telegram_channels tc ON tc.external_id = m.from_external_id
                  LEFT JOIN telegram_megagroups tm ON tm.external_id = m.from_external_id
                  LEFT JOIN telegram_chats th ON th.external_id = m.from_external_id
                  WHERE m.source_external_id = %s
                    AND COALESCE(m.deleted, false) = false
                    AND m.from_external_id IS NOT NULL
                    AND BTRIM(m.from_external_id) <> ''
                    AND (
                      m.from_external_id ILIKE %s
                      OR COALESCE(tu.username, '') ILIKE %s
                      OR COALESCE(tu.first_name, '') ILIKE %s
                      OR COALESCE(tu.last_name, '') ILIKE %s
                      OR CONCAT_WS(' ', tu.first_name, tu.last_name) ILIKE %s
                      OR COALESCE(tc.title, '') ILIKE %s
                      OR COALESCE(tc.username, '') ILIKE %s
                      OR COALESCE(tm.title, '') ILIKE %s
                      OR COALESCE(tm.username, '') ILIKE %s
                      OR COALESCE(th.title, '') ILIKE %s
                    )
                ) matched
                """,
                (
                    external_id,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                ),
            )
        else:
            await cur.execute(
                """
                SELECT COUNT(DISTINCT from_external_id)::int AS n
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND COALESCE(deleted, false) = false
                  AND from_external_id IS NOT NULL
                  AND BTRIM(from_external_id) <> ''
                """,
                (external_id,),
            )
        row = await cur.fetchone()
    return int((row or {}).get("n") or 0)


async def _list_membership_participants(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    limit: int,
    offset: int,
    q: str = "",
) -> list[dict[str, Any]]:
    """Current members ranked by all-time harvested post count (zeros included)."""
    needle = q.strip()
    like = f"%{needle}%" if needle else None
    async with conn.cursor(row_factory=dict_row) as cur:
        if like is not None:
            await cur.execute(
                """
                WITH post_counts AS (
                  SELECT from_external_id AS user_id, COUNT(*)::int AS count
                  FROM telegram_messages
                  WHERE source_external_id = %s
                    AND COALESCE(deleted, false) = false
                    AND from_external_id IS NOT NULL
                    AND BTRIM(from_external_id) <> ''
                  GROUP BY from_external_id
                )
                SELECT
                  tcp.user_id AS external_id,
                  COALESCE(pc.count, 0)::int AS count,
                  tcp.rank
                FROM telegram_chat_participants tcp
                LEFT JOIN post_counts pc ON pc.user_id = tcp.user_id
                LEFT JOIN telegram_users tu ON tu.external_id = tcp.user_id
                WHERE tcp.external_id = %s
                  AND tcp.is_current_member = true
                  AND (
                    tcp.user_id ILIKE %s
                    OR COALESCE(tu.username, '') ILIKE %s
                    OR COALESCE(tu.first_name, '') ILIKE %s
                    OR COALESCE(tu.last_name, '') ILIKE %s
                    OR CONCAT_WS(' ', tu.first_name, tu.last_name) ILIKE %s
                  )
                ORDER BY count DESC, tcp.joined_at DESC NULLS LAST, tcp.user_id
                LIMIT %s OFFSET %s
                """,
                (
                    external_id,
                    external_id,
                    like,
                    like,
                    like,
                    like,
                    like,
                    limit,
                    offset,
                ),
            )
        else:
            await cur.execute(
                """
                WITH post_counts AS (
                  SELECT from_external_id AS user_id, COUNT(*)::int AS count
                  FROM telegram_messages
                  WHERE source_external_id = %s
                    AND COALESCE(deleted, false) = false
                    AND from_external_id IS NOT NULL
                    AND BTRIM(from_external_id) <> ''
                  GROUP BY from_external_id
                )
                SELECT
                  tcp.user_id AS external_id,
                  COALESCE(pc.count, 0)::int AS count,
                  tcp.rank
                FROM telegram_chat_participants tcp
                LEFT JOIN post_counts pc ON pc.user_id = tcp.user_id
                WHERE tcp.external_id = %s
                  AND tcp.is_current_member = true
                ORDER BY count DESC, tcp.joined_at DESC NULLS LAST, tcp.user_id
                LIMIT %s OFFSET %s
                """,
                (external_id, external_id, limit, offset),
            )
        return list(await cur.fetchall())


async def _list_message_participants(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    limit: int,
    offset: int,
    q: str = "",
) -> list[dict[str, Any]]:
    """Message senders ranked by all-time post count (fallback when membership incomplete)."""
    needle = q.strip()
    like = f"%{needle}%" if needle else None
    async with conn.cursor(row_factory=dict_row) as cur:
        if like is not None:
            await cur.execute(
                """
                SELECT m.from_external_id AS external_id, COUNT(*)::int AS count
                FROM telegram_messages m
                LEFT JOIN telegram_users tu ON tu.external_id = m.from_external_id
                LEFT JOIN telegram_channels tc ON tc.external_id = m.from_external_id
                LEFT JOIN telegram_megagroups tm ON tm.external_id = m.from_external_id
                LEFT JOIN telegram_chats th ON th.external_id = m.from_external_id
                WHERE m.source_external_id = %s
                  AND COALESCE(m.deleted, false) = false
                  AND m.from_external_id IS NOT NULL
                  AND BTRIM(m.from_external_id) <> ''
                  AND (
                    m.from_external_id ILIKE %s
                    OR COALESCE(tu.username, '') ILIKE %s
                    OR COALESCE(tu.first_name, '') ILIKE %s
                    OR COALESCE(tu.last_name, '') ILIKE %s
                    OR CONCAT_WS(' ', tu.first_name, tu.last_name) ILIKE %s
                    OR COALESCE(tc.title, '') ILIKE %s
                    OR COALESCE(tc.username, '') ILIKE %s
                    OR COALESCE(tm.title, '') ILIKE %s
                    OR COALESCE(tm.username, '') ILIKE %s
                    OR COALESCE(th.title, '') ILIKE %s
                  )
                GROUP BY m.from_external_id
                ORDER BY count DESC, m.from_external_id
                LIMIT %s OFFSET %s
                """,
                (
                    external_id,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    like,
                    limit,
                    offset,
                ),
            )
        else:
            await cur.execute(
                """
                SELECT from_external_id AS external_id, COUNT(*)::int AS count
                FROM telegram_messages
                WHERE source_external_id = %s
                  AND COALESCE(deleted, false) = false
                  AND from_external_id IS NOT NULL
                  AND BTRIM(from_external_id) <> ''
                GROUP BY from_external_id
                ORDER BY count DESC, from_external_id
                LIMIT %s OFFSET %s
                """,
                (external_id, limit, offset),
            )
        return list(await cur.fetchall())


async def list_peer_participants(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    limit: int | None = None,
    offset: int = 0,
    q: str | None = None,
) -> ParticipantsPage:
    """Paginated participants sorted by all-time harvested post count.

    Uses harvested membership when current members exist; otherwise falls back to
    distinct message senders. Optional ``q`` filters by name / username / id.
    """
    del peer_type  # reserved for future peer-type-specific gates
    page_limit = clamp_participants_page_limit(limit)
    page_offset = max(0, int(offset))
    query = (q or "").strip()

    # Gate on unfiltered membership so a search miss does not flip to message mode.
    has_members = await _current_member_count(conn, external_id=external_id, q="")
    if has_members > 0:
        source = "membership"
        total = await _current_member_count(conn, external_id=external_id, q=query)
        rows = await _list_membership_participants(
            conn,
            external_id=external_id,
            limit=page_limit,
            offset=page_offset,
            q=query,
        )
    else:
        source = "messages"
        total = await _message_poster_total(conn, external_id=external_id, q=query)
        rows = await _list_message_participants(
            conn,
            external_id=external_id,
            limit=page_limit,
            offset=page_offset,
            q=query,
        )

    items = await _enrich_actor_rows(conn, rows)
    return ParticipantsPage(
        items=items,
        total=total,
        limit=page_limit,
        offset=page_offset,
        source=source,
    )


async def _activity_snapshot(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    participants_limit: int,
    viewed_limit: int,
) -> ActivitySnapshot:
    raw_7d, raw_30d = await count_peer_recent_volumes(conn, external_id)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, message_id, date, content, views, media,
                   from_external_id, source_external_id, fwd_from, reply_to,
                   archive_key, archive_row
            FROM telegram_messages
            WHERE source_external_id = %s
              AND COALESCE(deleted, false) = false
              AND action_type IS NULL
              AND views IS NOT NULL
              AND views > 0
            ORDER BY views DESC
            LIMIT %s
            """,
            (external_id, viewed_limit),
        )
        viewed_rows = await cur.fetchall()

    viewed_page = [dict(r) for r in viewed_rows]
    try:
        from msgstore import (
            get_message_archive_prefix,
            get_message_archive_sink,
            hydrate_message_rows_async,
        )

        # Sync S3/Parquet hydrate — must leave the event loop or the whole API stalls.
        await hydrate_message_rows_async(
            viewed_page,
            get_message_archive_sink(),
            prefix=get_message_archive_prefix(),
        )
    except Exception:
        pass

    participants_page = await list_peer_participants(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        limit=participants_limit,
        offset=0,
    )

    region = ConfigFactory.get_config().aws_region.strip()
    media_previews = await asyncio.gather(
        *[build_message_media_preview(r.get("media"), region=region) for r in viewed_page]
    )

    top_viewed: list[TopViewedPostInsight] = []
    for r, preview in zip(viewed_page, media_previews, strict=True):
        raw_id = r["id"]
        msg_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
        top_viewed.append(
            TopViewedPostInsight(
                id=msg_id,
                message_id=int(r["message_id"]),
                date=r.get("date"),
                content=(r.get("content") or None),
                views=int(r["views"] or 0),
                media=None if preview.status == "none" else preview,
                from_external_id=(r.get("from_external_id") or None),
                source_external_id=(r.get("source_external_id") or None),
                fwd_from=r.get("fwd_from"),
                reply_to=r.get("reply_to"),
            )
        )

    messages_total = await gated_total_count(
        conn,
        peer_external_id=external_id,
        peer_type=peer_type,
    )

    coverage_tip_at = await peer_latest_coverage_before(
        conn,
        peer_external_id=external_id,
        peer_type=peer_type,
    )

    # messages_last_at is only meaningful when history is complete from the
    # beginning through a tip within the last month (same gate as messages_total).
    # first_message_at for "Since" lives on the peer row (GET /peers/{id}), not here.
    messages_last_at = coverage_tip_at if messages_total is not None else None

    messages_7d, messages_30d = await gated_recent_volume_counts(
        conn,
        peer_external_id=external_id,
        peer_type=peer_type,
        messages_7d=raw_7d,
        messages_30d=raw_30d,
    )

    return ActivitySnapshot(
        messages_7d=messages_7d,
        messages_30d=messages_30d,
        messages_total=messages_total,
        messages_last_at=messages_last_at,
        coverage_tip_at=coverage_tip_at,
        messages_first_at=None,
        participants=participants_page.items,
        participants_total=participants_page.total,
        participants_source=participants_page.source,
        top_viewed=top_viewed,
    )


async def build_community_insights(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
    forwards_limit: int | None = None,
    participants_limit: int | None = None,
    posters_limit: int | None = None,
    viewed_limit: int | None = None,
) -> CommunityInsightsResponse:
    fwd_n = clamp_insights_limit(forwards_limit, default=_DEFAULT_FORWARDS_LIMIT)
    # ``posters_limit`` kept as a deprecated alias for older clients.
    part_n = clamp_insights_limit(
        participants_limit if participants_limit is not None else posters_limit,
        default=_DEFAULT_PARTICIPANTS_LIMIT,
    )
    view_n = clamp_insights_limit(viewed_limit, default=_DEFAULT_VIEWED_LIMIT)

    related, related_total = await _related_channels(conn, external_id=external_id, peer_type=peer_type)
    sources, sources_total, forwarded_by, forwarded_by_total = await _forward_neighbors(
        conn,
        external_id=external_id,
        limit=fwd_n,
    )
    activity = await _activity_snapshot(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        participants_limit=part_n,
        viewed_limit=view_n,
    )
    return CommunityInsightsResponse(
        related_channels=related,
        related_channels_total=related_total,
        forward_sources=sources,
        forward_sources_total=sources_total,
        forwarded_by=forwarded_by,
        forwarded_by_total=forwarded_by_total,
        activity=activity,
    )
