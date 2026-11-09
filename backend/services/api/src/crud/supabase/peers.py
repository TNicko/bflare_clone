"""Search and fetch telegram_channels, telegram_chats, and participants for graph."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any, TypedDict

from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row
from telethon.tl.types import PeerChannel

from db.schemas.general.primitives import PeerTypeSchema, TelegramPeerTypeSchema
from db.schemas.telegram.channel import TelegramChannelSchema
from db.schemas.telegram.chat import TelegramChatSchema
from db.crud.telegram.field_history import list_field_history_for_peer
from db.crud.telegram.harvester import get_peer_access_error, get_peers_access_error_codes
from db.crud.telegram.search_normalize import normalize_text
from db.utils import adapt_param, build_upsert_sql
from services.api.src.crud.supabase.peer_rows import (
    community_view_from_peer_row,
    enrich_telegram_peer_row,
)
from services.api.src.crud.supabase.views.general.category import CategoryView
from services.api.src.crud.supabase.views.general.community import CommunityView, _primary_username
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.peer_search import TelegramPeerSearchView
from services.api.src.crud.supabase.views.telegram.user import (
    UserAffiliatedPeersPage,
    UserAffiliatedPeerView,
    UserPartialView,
    UserPeerAffiliationDetailView,
    UserPeerAffiliationMessagesPage,
    UserPeerAffiliationReactionsPage,
    UserPeerAffiliationSummaryView,
    UserPeerDetailView,
    UserPeerReactionView,
    enrich_affiliated_peers,
)
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from services.api.src.crud.supabase.views.telegram.participant import PeerParticipantView
from telegram.mappers import get_b64_image_url_from_photo
from telegram.utils import CHANNEL_MARK, normalize_tl_keys, to_signed_peer_id
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelLinkView,
    TelegramChatView,
    TelegramMegagroupLinkView,
)
from uuid import UUID

logger = logging.getLogger(__name__)

# Present in the database; never returned by the client-facing API.
CLIENT_HIDDEN_CATEGORY_SLUGS: frozenset[str] = frozenset({"tl-cse-risk"})


def _ilike_contains_pattern(term: str) -> str:
    """Wrap ``term`` for a contains ILIKE, escaping LIKE metacharacters.

    Pair with ``ILIKE %s ESCAPE '\\'``.
    """
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


class UserStoriesAndGiftsRow(TypedDict):
    external_id: str
    access_hash: int | None
    username: str | None
    bot: bool | None
    updated_at: datetime | None
    harvester_id: Any
    harvester_external_id: str | None
    pinned_stories: Any
    pinned_stories_fetched_at: datetime | None
    saved_star_gifts: Any
    star_gifts_fetched_at: datetime | None


class PeerRefreshRow(TypedDict):
    external_id: str
    access_hash: int | None
    updated_at: datetime | None
    harvester_id: Any
    harvester_external_id: str | None
    profile_tier: str | None


def user_stories_and_gifts_row_from_db(row: Mapping[str, Any]) -> UserStoriesAndGiftsRow:
    raw_access_hash = row.get("access_hash")
    raw_username = row.get("username")
    raw_harvester_external_id = row.get("harvester_external_id")
    return UserStoriesAndGiftsRow(
        external_id=str(row["external_id"]),
        access_hash=int(raw_access_hash) if raw_access_hash is not None else None,
        username=str(raw_username) if raw_username is not None else None,
        bot=bool(row.get("bot")) if row.get("bot") is not None else None,
        updated_at=row.get("updated_at"),
        harvester_id=row.get("harvester_id"),
        harvester_external_id=(str(raw_harvester_external_id) if raw_harvester_external_id is not None else None),
        pinned_stories=row.get("pinned_stories"),
        pinned_stories_fetched_at=row.get("pinned_stories_fetched_at"),
        saved_star_gifts=row.get("saved_star_gifts"),
        star_gifts_fetched_at=row.get("star_gifts_fetched_at"),
    )


_CATEGORIES_SUBQUERY = """
    SELECT
        pc_all.community_id,
        jsonb_agg(
            jsonb_build_object('id', c_all.id, 'slug', c_all.slug, 'label', c_all.label)
            ORDER BY c_all.tier, c_all.label
        ) AS categories
    FROM peer_categories pc_all
    JOIN categories c_all
        ON c_all.id = pc_all.category_id
    WHERE c_all.slug NOT IN ('tl-cse-risk')
    GROUP BY pc_all.community_id
"""

_UNIFIED_COMMUNITIES_CTE = """
    unified AS (
      SELECT
        'channel'::text AS peer_type,
        id AS community_id,
        external_id,
        title,
        username,
        usernames,
        photo,
        participants_count,
        profile_photo_s3_uri,
        is_verified,
        language
      FROM telegram_channels
      UNION ALL
      SELECT
        'megagroup'::text AS peer_type,
        id AS community_id,
        external_id,
        title,
        username,
        usernames,
        photo,
        participants_count,
        profile_photo_s3_uri,
        is_verified,
        language
      FROM telegram_megagroups
      UNION ALL
      SELECT
        'chat'::text AS peer_type,
        id AS community_id,
        external_id,
        title,
        NULL::text AS username,
        '[]'::jsonb AS usernames,
        photo,
        participants_count,
        profile_photo_s3_uri,
        NULL::boolean AS is_verified,
        NULL::text AS language
      FROM telegram_chats
    )
"""

_PEER_TABLE_NAMES = frozenset({"telegram_channels", "telegram_megagroups", "telegram_chats"})


def _peer_row_with_categories_query(*, table: str, where_column: str) -> sql.Composed:
    if table not in _PEER_TABLE_NAMES:
        raise ValueError(f"Unsupported peer table: {table}")
    if where_column not in {"id", "external_id"}:
        raise ValueError(f"Unsupported where column: {where_column}")
    return sql.SQL(
        """
        SELECT tc.*, COALESCE(sub.categories, '[]'::jsonb) AS categories
        FROM {table} tc
        LEFT JOIN ({categories_subquery}) sub
          ON sub.community_id::uuid = tc.id
        WHERE tc.{where_col} = %s
        LIMIT 1
        """
    ).format(
        table=sql.Identifier(table),
        categories_subquery=sql.SQL(_CATEGORIES_SUBQUERY),
        where_col=sql.Identifier(where_column),
    )


_LIST_CATEGORY_COMMUNITIES_QUERY = sql.SQL(
    """
    WITH peer_set AS (
      SELECT DISTINCT
        pc.peer_type,
        pc.community_id
      FROM peer_categories pc
      JOIN categories c_filter
        ON c_filter.id = pc.category_id
      WHERE c_filter.slug = %s
    ),
    {unified_cte},
    grouped AS (
      SELECT
        ps.peer_type,
        ps.community_id,
        u.external_id,
        u.photo,
        u.title,
        u.username,
        u.usernames,
        u.participants_count,
        u.profile_photo_s3_uri,
        u.is_verified,
        u.language,
        jsonb_agg(
          jsonb_build_object('id', c_all.id, 'slug', c_all.slug, 'label', c_all.label)
          ORDER BY c_all.tier, c_all.label
        ) AS categories,
        COUNT(*) OVER()::int AS total
      FROM peer_set ps
      JOIN unified u
        ON u.peer_type = ps.peer_type
       AND u.community_id = ps.community_id
      JOIN peer_categories pc_all
        ON pc_all.peer_type = ps.peer_type
       AND pc_all.community_id::uuid = ps.community_id
      JOIN categories c_all
        ON c_all.id = pc_all.category_id
       AND c_all.slug NOT IN ('tl-cse-risk')
      WHERE (
        %s::text = ''
        OR u.external_id ILIKE %s ESCAPE '\\'
        OR COALESCE(u.title, '') ILIKE %s ESCAPE '\\'
        OR COALESCE(u.username, '') ILIKE %s ESCAPE '\\'
        OR COALESCE(u.usernames::text, '') ILIKE %s ESCAPE '\\'
      )
      GROUP BY
        ps.peer_type,
        ps.community_id,
        u.external_id,
        u.title,
        u.username,
        u.usernames,
        u.participants_count,
        u.profile_photo_s3_uri,
        u.photo,
        u.is_verified,
        u.language
    )
    SELECT
      peer_type,
      community_id,
      external_id,
      photo,
      title,
      username,
      usernames,
      participants_count,
      profile_photo_s3_uri,
      is_verified,
      language,
      categories,
      total
    FROM grouped
    ORDER BY
      peer_type, community_id
    LIMIT %s OFFSET %s;
    """
).format(unified_cte=sql.SQL(_UNIFIED_COMMUNITIES_CTE))


# Cap reported totals so short/common queries do not WindowAgg millions of hits.
_NAME_SEARCH_TOTAL_CAP = 1000

# Filter each peer table before UNION so pg_trgm GIN indexes on title/username/usernames can be used.
# Rank page with top-N heapsort; total is an early-stop count capped at ``_NAME_SEARCH_TOTAL_CAP``.
# usernames_search matches handles inside the JSONB array and uses *_usernames_trgm_idx.
_SEARCH_COMMUNITIES_BY_NAME_QUERY = """
    WITH hits AS (
      SELECT 'channel'::text AS peer_type, id AS community_id, participants_count
      FROM telegram_channels
      WHERE title_search ILIKE %s ESCAPE '\\'
         OR username_search ILIKE %s ESCAPE '\\'
         OR usernames_search ILIKE %s ESCAPE '\\'
      UNION ALL
      SELECT 'megagroup'::text, id, participants_count
      FROM telegram_megagroups
      WHERE title_search ILIKE %s ESCAPE '\\'
         OR username_search ILIKE %s ESCAPE '\\'
         OR usernames_search ILIKE %s ESCAPE '\\'
      UNION ALL
      SELECT 'chat'::text, id, participants_count
      FROM telegram_chats
      WHERE title_search ILIKE %s ESCAPE '\\'
    ),
    totals AS (
      SELECT COUNT(*)::int AS total
      FROM (SELECT 1 FROM hits LIMIT %s) capped
    ),
    page AS (
      SELECT
        peer_type,
        community_id,
        participants_count
      FROM hits
      ORDER BY participants_count DESC NULLS LAST, community_id
      LIMIT %s OFFSET %s
    ),
    matched AS (
      SELECT
        p.peer_type,
        p.community_id,
        p.participants_count,
        c.external_id,
        c.title,
        c.about,
        c.username,
        c.usernames,
        c.photo,
        c.profile_photo_s3_uri,
        c.unavailable_at,
        c.is_verified,
        c.is_scam,
        c.is_fake,
        c.is_restricted,
        c.restriction_reason,
        c.language
      FROM page p
      JOIN telegram_channels c
        ON p.peer_type = 'channel'
       AND c.id = p.community_id
      UNION ALL
      SELECT
        p.peer_type,
        p.community_id,
        p.participants_count,
        m.external_id,
        m.title,
        m.about,
        m.username,
        m.usernames,
        m.photo,
        m.profile_photo_s3_uri,
        m.unavailable_at,
        m.is_verified,
        m.is_scam,
        m.is_fake,
        m.is_restricted,
        m.restriction_reason,
        m.language
      FROM page p
      JOIN telegram_megagroups m
        ON p.peer_type = 'megagroup'
       AND m.id = p.community_id
      UNION ALL
      SELECT
        p.peer_type,
        p.community_id,
        p.participants_count,
        t.external_id,
        t.title,
        t.about,
        NULL::text AS username,
        '[]'::jsonb AS usernames,
        t.photo,
        t.profile_photo_s3_uri,
        t.unavailable_at,
        NULL::boolean AS is_verified,
        NULL::boolean AS is_scam,
        NULL::boolean AS is_fake,
        NULL::boolean AS is_restricted,
        '[]'::jsonb AS restriction_reason,
        NULL::text AS language
      FROM page p
      JOIN telegram_chats t
        ON p.peer_type = 'chat'
       AND t.id = p.community_id
    )
    SELECT
      m.peer_type,
      m.community_id,
      m.external_id,
      m.title,
      m.about,
      m.username,
      m.usernames,
      m.photo,
      m.participants_count,
      m.profile_photo_s3_uri,
      m.unavailable_at,
      m.is_verified,
      m.is_scam,
      m.is_fake,
      m.is_restricted,
      m.restriction_reason,
      m.language,
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'id', c_all.id,
            'slug', c_all.slug,
            'label', c_all.label
          )
          ORDER BY c_all.tier, c_all.label
        ) FILTER (
          WHERE c_all.id IS NOT NULL
            AND c_all.slug NOT IN ('tl-cse-risk')
        ),
        '[]'::jsonb
      ) AS categories,
      totals.total
    FROM matched m
    CROSS JOIN totals
    LEFT JOIN peer_categories pc_all
      ON pc_all.peer_type = m.peer_type
     AND pc_all.community_id::uuid = m.community_id
    LEFT JOIN categories c_all
      ON c_all.id = pc_all.category_id
    GROUP BY
      m.peer_type, m.community_id, m.external_id, m.title, m.about, m.username, m.usernames,
      m.participants_count, m.profile_photo_s3_uri, m.photo, m.unavailable_at,
      m.is_verified, m.is_scam, m.is_fake, m.is_restricted, m.restriction_reason, m.language,
      totals.total
    ORDER BY m.participants_count DESC NULLS LAST, m.community_id;
"""


async def _fetch_peer_row_by_uuid(
    conn: AsyncConnection[DictRow],
    *,
    table: str,
    peer_type: PeerTypeSchema,
    peer_id: str,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _peer_row_with_categories_query(table=table, where_column="id"),
            (peer_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return enrich_telegram_peer_row(dict(row), peer_type=peer_type)


async def get_telegram_channel_by_uuid(conn: AsyncConnection[DictRow], channel_id: str):
    return await _fetch_peer_row_by_uuid(conn, table="telegram_channels", peer_type="channel", peer_id=channel_id)


async def get_telegram_megagroup_by_uuid(conn: AsyncConnection[DictRow], megagroup_id: str):
    return await _fetch_peer_row_by_uuid(conn, table="telegram_megagroups", peer_type="megagroup", peer_id=megagroup_id)


async def get_telegram_chat_by_uuid(conn: AsyncConnection[DictRow], chat_id: str):
    return await _fetch_peer_row_by_uuid(conn, table="telegram_chats", peer_type="chat", peer_id=chat_id)


async def _fetch_peer_row_by_external_id(
    conn: AsyncConnection[DictRow],
    *,
    table: str,
    external_id: str,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _peer_row_with_categories_query(table=table, where_column="external_id"),
            (external_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_peer_by_id(
    conn: AsyncConnection[DictRow],
    external_id: str,
    peer_type: PeerTypeSchema,
) -> TelegramChannelLinkView | TelegramMegagroupLinkView | TelegramChatView | None:
    async def _with_access_error(out: dict[str, Any]) -> dict[str, Any]:
        code, checked_at = await get_peer_access_error(
            conn,
            external_id=external_id,
            harvester_id=out.get("harvester_id"),
        )
        out["access_error_code"] = code
        out["access_checked_at"] = checked_at
        return out

    if peer_type == "channel":
        row = await _fetch_peer_row_by_external_id(conn, table="telegram_channels", external_id=external_id)
        if not row:
            return None
        out = await _with_access_error(enrich_telegram_peer_row(row, peer_type="channel"))

        linked_peer_model = None
        linked_chat_id = row.get("linked_chat_id")
        if linked_chat_id is not None:
            linked_signed = str(to_signed_peer_id(PeerChannel(linked_chat_id)))
            linked_row = await _fetch_peer_row_by_external_id(
                conn, table="telegram_megagroups", external_id=linked_signed
            )
            if linked_row:
                linked_peer_model = community_view_from_peer_row(linked_row, peer_type="megagroup")

        out["linked_peer"] = linked_peer_model
        return TelegramChannelLinkView.model_validate(out)

    if peer_type == "chat":
        row = await _fetch_peer_row_by_external_id(conn, table="telegram_chats", external_id=external_id)
        if not row:
            return None
        return TelegramChatView.model_validate(
            await _with_access_error(enrich_telegram_peer_row(row, peer_type="chat"))
        )

    if peer_type == "megagroup":
        row = await _fetch_peer_row_by_external_id(conn, table="telegram_megagroups", external_id=external_id)
        if not row:
            return None
        out = await _with_access_error(enrich_telegram_peer_row(row, peer_type="megagroup"))

        linked_peer_model = None
        linked_chat_id = row.get("linked_chat_id")
        if linked_chat_id is not None:
            linked_signed = str(to_signed_peer_id(PeerChannel(linked_chat_id)))
            linked_row = await _fetch_peer_row_by_external_id(
                conn, table="telegram_channels", external_id=linked_signed
            )
            if linked_row:
                linked_peer_model = community_view_from_peer_row(linked_row, peer_type="channel")

        out["linked_peer"] = linked_peer_model
        return TelegramMegagroupLinkView.model_validate(out)

    return None


async def search_telegram_peers(
    conn: AsyncConnection[DictRow],
    q: str,
    *,
    limit: int = 20,
) -> list[TelegramPeerSearchView]:
    term = normalize_text(q) or ""
    if not term:
        return []

    pattern = _ilike_contains_pattern(term)
    result: list[TelegramPeerSearchView] = []
    half = max(1, limit // 2)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, external_id, title, username, is_megagroup
            FROM telegram_channels
            WHERE (title_search ILIKE %s ESCAPE '\\' OR username_search ILIKE %s ESCAPE '\\' OR usernames_search ILIKE %s ESCAPE '\\')
            ORDER BY participants_count DESC NULLS LAST, title NULLS LAST
            LIMIT %s
            """,
            (pattern, pattern, pattern, half),
        )
        for row in await cur.fetchall():
            result.append(TelegramPeerSearchView.from_peer_row(row, peer_type="channel", fallback_prefix="Channel"))

        remaining = limit - len(result)
        if remaining > 0:
            await cur.execute(
                """
                SELECT id, external_id, title, username, is_megagroup
                FROM telegram_megagroups
                WHERE (title_search ILIKE %s ESCAPE '\\' OR username_search ILIKE %s ESCAPE '\\' OR usernames_search ILIKE %s ESCAPE '\\')
                ORDER BY participants_count DESC NULLS LAST, title NULLS LAST
                LIMIT %s
                """,
                (pattern, pattern, pattern, remaining),
            )
            for row in await cur.fetchall():
                result.append(
                    TelegramPeerSearchView.from_peer_row(row, peer_type="megagroup", fallback_prefix="Megagroup")
                )

        remaining = limit - len(result)
        if remaining > 0:
            await cur.execute(
                """
                SELECT id, external_id, title
                FROM telegram_chats
                WHERE title_search ILIKE %s ESCAPE '\\'
                ORDER BY participants_count DESC NULLS LAST, title NULLS LAST
                LIMIT %s
                """,
                (pattern, remaining),
            )
            for row in await cur.fetchall():
                result.append(TelegramPeerSearchView.from_peer_row(row, peer_type="chat", fallback_prefix="Chat"))

    return result[:limit]


async def list_categories(conn: AsyncConnection[DictRow]) -> list[CategoryView]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                c.id,
                c.slug,
                c.label,
                c.parent_slug,
                p.id AS parent_id,
                p.slug AS parent_slug_resolved,
                p.label AS parent_label,
                c.tier,
                COALESCE(
                    (
                        SELECT COUNT(DISTINCT ROW(pc.peer_type, pc.community_id))
                        FROM peer_categories pc
                        WHERE pc.category_id = c.id
                    ),
                    0
                ) AS community_count
            FROM categories c
            LEFT JOIN categories p
                ON p.slug = c.parent_slug
            WHERE c.slug NOT IN ('tl-cse-risk')
            ORDER BY
                c.tier, c.label;
            """
        )
        rows = await cur.fetchall()
    return [CategoryView.from_row(row) for row in rows]


async def get_category(conn: AsyncConnection[DictRow], category_slug: str) -> CategoryView | None:
    if category_slug in CLIENT_HIDDEN_CATEGORY_SLUGS:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                c.id,
                c.slug,
                c.label,
                c.parent_slug,
                p.id AS parent_id,
                p.slug AS parent_slug_resolved,
                p.label AS parent_label,
                c.tier,
                COALESCE(
                    (
                        SELECT COUNT(DISTINCT ROW(pc.peer_type, pc.community_id))
                        FROM peer_categories pc
                        WHERE pc.category_id = c.id
                    ),
                    0
                ) AS community_count
            FROM categories c
            LEFT JOIN categories p
                ON p.slug = c.parent_slug
            WHERE c.slug = %s
            ORDER BY
                c.tier, c.label;
            """,
            (category_slug,),
        )
        row = await cur.fetchone()
    return CategoryView.from_row(row) if row else None


async def list_category_communities(
    conn: AsyncConnection[DictRow],
    category_slug: str,
    *,
    limit: int,
    offset: int,
    q: str | None = None,
) -> tuple[list[CommunityView], int]:
    needle = (q or "").strip()
    like = _ilike_contains_pattern(needle) if needle else "%"
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _LIST_CATEGORY_COMMUNITIES_QUERY,
            (
                category_slug,
                needle,
                like or "%",
                like or "%",
                like or "%",
                like or "%",
                limit,
                offset,
            ),
        )
        rows = await cur.fetchall()

    total = rows[0]["total"] if rows else 0
    items: list[CommunityView] = []
    for row in rows:
        view = CommunityView.from_row(row)
        if view is not None:
            items.append(view)
    items = await attach_community_link_privacy(conn, items)
    return items, total


async def search_communities_by_name(
    conn: AsyncConnection[DictRow],
    *,
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[CommunityView], int]:
    term = normalize_text(q) or ""
    if not term:
        return [], 0

    pattern = _ilike_contains_pattern(term)
    # channels: title/username/usernames; megagroups: same; chats: title; cap; limit; offset
    params = (
        pattern,
        pattern,
        pattern,
        pattern,
        pattern,
        pattern,
        pattern,
        _NAME_SEARCH_TOTAL_CAP,
        limit,
        offset,
    )

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_SEARCH_COMMUNITIES_BY_NAME_QUERY, params)
        rows = await cur.fetchall()

    total = rows[0]["total"] if rows else 0
    items: list[CommunityView] = []
    for row in rows:
        view = CommunityView.from_row(row)
        if view is not None:
            items.append(view)
    items = await _attach_community_access_errors(conn, items)
    items = await attach_community_link_privacy(conn, items)
    return items, total


async def attach_community_link_privacy(
    conn: AsyncConnection[DictRow],
    items: list[CommunityView],
) -> list[CommunityView]:
    """Attach linked public-channel username for username-less megagroups.

    Needed so list/search UIs can omit the Private badge for discussion groups
    that are reachable via a public channel's comments.
    """
    need_ids = [
        c.external_id
        for c in items
        if c.peer_type == "megagroup" and not c.is_unknown and not (c.username or "").strip()
    ]
    if not need_ids:
        return items

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              mg.external_id,
              mg.has_link,
              CASE
                WHEN linked.external_id IS NOT NULL THEN 'channel'::text
                ELSE NULL
              END AS linked_peer_type,
              linked.username AS linked_username,
              linked.usernames AS linked_usernames
            FROM telegram_megagroups mg
            LEFT JOIN telegram_channels linked
              ON mg.linked_chat_id IS NOT NULL
             AND linked.external_id = (-(%s::bigint + mg.linked_chat_id))::text
            WHERE mg.external_id = ANY(%s)
            """,
            (CHANNEL_MARK, need_ids),
        )
        by_eid = {str(row["external_id"]): row for row in await cur.fetchall()}

    out: list[CommunityView] = []
    for community in items:
        row = by_eid.get(community.external_id)
        if row is None:
            out.append(community)
            continue
        linked_username = _primary_username(
            {
                "username": row.get("linked_username"),
                "usernames": row.get("linked_usernames"),
            }
        )
        linked_peer_type = row.get("linked_peer_type")
        out.append(
            community.model_copy(
                update={
                    "has_link": bool(row["has_link"]) if row.get("has_link") is not None else None,
                    "linked_peer_type": linked_peer_type
                    if linked_peer_type in ("channel", "megagroup", "chat")
                    else None,
                    "linked_username": linked_username,
                }
            )
        )
    return out


async def _attach_affiliated_peers_link_privacy(
    conn: AsyncConnection[DictRow],
    entries: list[UserAffiliatedPeerView],
) -> list[UserAffiliatedPeerView]:
    """Enrich nested affiliated ``peer`` CommunityViews with link privacy fields."""
    if not entries:
        return entries
    peers = [entry.peer for entry in entries]
    enriched_peers = await attach_community_link_privacy(conn, peers)
    by_eid = {peer.external_id: peer for peer in enriched_peers}
    return [entry.model_copy(update={"peer": by_eid.get(entry.peer.external_id, entry.peer)}) for entry in entries]


async def _attach_community_access_errors(
    conn: AsyncConnection[DictRow],
    items: list[CommunityView],
) -> list[CommunityView]:
    if not items:
        return items
    codes = await get_peers_access_error_codes(conn, [c.external_id for c in items])
    if not codes:
        return items
    return [
        (c.model_copy(update={"access_error_code": codes[c.external_id]}) if c.external_id in codes else c)
        for c in items
    ]


async def _attach_community_access_error(
    conn: AsyncConnection[DictRow],
    view: CommunityView | None,
) -> CommunityView | None:
    if view is None:
        return None
    enriched = await _attach_community_access_errors(conn, [view])
    enriched = await attach_community_link_privacy(conn, enriched)
    return enriched[0]


_USER_PARTIAL_SELECT = """
    id,
    external_id,
    first_name,
    last_name,
    username,
    phone,
    about,
    engagement_count,
    profile_photo_s3_uri,
    verified,
    premium,
    bot,
    deleted,
    restricted,
    scam,
    fake,
    unavailable_at,
    restriction_reason,
    photo
"""


# Cheap exact handle → external_id (uses lower(username) btree indexes).
# Full CommunityView (categories + access) loads via get_community_view_by_external_id.
_COMMUNITY_EXTERNAL_ID_BY_USERNAME_QUERY = """
    SELECT external_id
    FROM telegram_channels
    WHERE lower(username) = %s
    UNION ALL
    SELECT external_id
    FROM telegram_megagroups
    WHERE lower(username) = %s
    LIMIT 1;
"""


_COMMUNITY_BY_EXTERNAL_ID_QUERY = """
    WITH filtered AS (
      SELECT
        'channel'::text AS peer_type,
        id AS community_id,
        external_id,
        title,
        username,
        usernames,
        photo,
        participants_count,
        profile_photo_s3_uri,
        unavailable_at,
        is_verified,
        is_scam,
        is_fake,
        is_restricted,
        restriction_reason,
        language
      FROM telegram_channels
      WHERE external_id = %s
      UNION ALL
      SELECT
        'megagroup'::text AS peer_type,
        id AS community_id,
        external_id,
        title,
        username,
        usernames,
        photo,
        participants_count,
        profile_photo_s3_uri,
        unavailable_at,
        is_verified,
        is_scam,
        is_fake,
        is_restricted,
        restriction_reason,
        language
      FROM telegram_megagroups
      WHERE external_id = %s
      UNION ALL
      SELECT
        'chat'::text AS peer_type,
        id AS community_id,
        external_id,
        title,
        NULL::text AS username,
        '[]'::jsonb AS usernames,
        photo,
        participants_count,
        profile_photo_s3_uri,
        unavailable_at,
        NULL::boolean AS is_verified,
        NULL::boolean AS is_scam,
        NULL::boolean AS is_fake,
        NULL::boolean AS is_restricted,
        '[]'::jsonb AS restriction_reason,
        NULL::text AS language
      FROM telegram_chats
      WHERE external_id = %s
    ),
    grouped AS (
      SELECT
        f.peer_type,
        f.community_id,
        f.external_id,
        f.title,
        f.username,
        f.usernames,
        f.photo,
        f.participants_count,
        f.profile_photo_s3_uri,
        f.unavailable_at,
        f.is_verified,
        f.is_scam,
        f.is_fake,
        f.is_restricted,
        f.restriction_reason,
        f.language,
        COALESCE(
          jsonb_agg(
            jsonb_build_object(
              'id', c_all.id,
              'slug', c_all.slug,
              'label', c_all.label
            )
            ORDER BY c_all.tier, c_all.label
          ) FILTER (
            WHERE c_all.id IS NOT NULL
              AND c_all.slug NOT IN ('tl-cse-risk')
          ),
          '[]'::jsonb
        ) AS categories
      FROM filtered f
      LEFT JOIN peer_categories pc_all
        ON pc_all.peer_type = f.peer_type
       AND pc_all.community_id::uuid = f.community_id
      LEFT JOIN categories c_all
        ON c_all.id = pc_all.category_id
      GROUP BY
        f.peer_type, f.community_id, f.external_id, f.title, f.username, f.usernames,
        f.participants_count, f.profile_photo_s3_uri, f.photo, f.unavailable_at,
        f.is_verified, f.is_scam, f.is_fake, f.is_restricted, f.restriction_reason,
        f.language
    )
    SELECT
      peer_type,
      community_id,
      external_id,
      title,
      username,
      usernames,
      photo,
      participants_count,
      profile_photo_s3_uri,
      unavailable_at,
      is_verified,
      is_scam,
      is_fake,
      is_restricted,
      restriction_reason,
      language,
      categories
    FROM grouped
    LIMIT 1;
"""


class UsernameOwnershipRow(TypedDict):
    external_id: str
    peer_type: TelegramPeerTypeSchema
    username: str | None
    access_hash: int | None
    harvester_id: Any


async def get_partial_telegram_user_by_username(
    conn: AsyncConnection[DictRow],
    username: str,
) -> UserPartialView | None:
    """Exact username match (case-insensitive) on ``telegram_users.username``."""
    handle = username.strip().lstrip("@").lower()
    if not handle:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_USER_PARTIAL_SELECT}
            FROM telegram_users
            WHERE lower(username) = %s
            LIMIT 1;
            """,
            (handle,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return UserPartialView.from_row(row)


async def get_community_view_by_username(
    conn: AsyncConnection[DictRow],
    username: str,
) -> CommunityView | None:
    """Exact username match on channels/megagroups (case-insensitive)."""
    handle = username.strip().lstrip("@").lower()
    if not handle:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_COMMUNITY_EXTERNAL_ID_BY_USERNAME_QUERY, (handle, handle))
        row = await cur.fetchone()
    if not row:
        return None
    external_id = str(row["external_id"] if isinstance(row, dict) else row[0])
    return await get_community_view_by_external_id(conn, external_id)


async def get_community_view_by_external_id(
    conn: AsyncConnection[DictRow],
    external_id: str,
) -> CommunityView | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _COMMUNITY_BY_EXTERNAL_ID_QUERY,
            (external_id, external_id, external_id),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return await _attach_community_access_error(conn, CommunityView.from_row(row))


async def get_user_username_ownership_row(
    conn: AsyncConnection[DictRow],
    external_id: str,
) -> UsernameOwnershipRow | None:
    """Load access_hash + healthy harvester for ownership refresh of a user."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                tu.external_id,
                tu.username,
                COALESCE(r.access_hash, tu.access_hash) AS access_hash,
                r.harvester_id
            FROM telegram_users tu
            LEFT JOIN LATERAL (
                SELECT r2.access_hash, r2.harvester_id
                FROM telegram_harvester_peer_roles r2
                INNER JOIN telegram_harvesters h2
                    ON h2.id = r2.harvester_id
                   AND h2.is_active = true
                   AND EXISTS (
                     SELECT 1 FROM telegram_harvester_sessions hs
                     WHERE hs.harvester_id = h2.id
                       AND hs.auth_state = 'healthy'
                   )
                WHERE r2.external_id = tu.external_id
                  AND r2.access_hash IS NOT NULL
                ORDER BY
                    CASE r2.role WHEN 'parent' THEN 0 ELSE 1 END,
                    r2.updated_at DESC NULLS LAST
                LIMIT 1
            ) r ON true
            WHERE tu.external_id = %s
            LIMIT 1;
            """,
            (external_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    raw_access_hash = row.get("access_hash")
    return UsernameOwnershipRow(
        external_id=str(row["external_id"]),
        peer_type="user",
        username=str(row["username"]) if row.get("username") is not None else None,
        access_hash=int(raw_access_hash) if raw_access_hash is not None else None,
        harvester_id=row.get("harvester_id"),
    )


async def get_community_username_ownership_row(
    conn: AsyncConnection[DictRow],
    *,
    external_id: str,
    peer_type: PeerTypeSchema,
) -> UsernameOwnershipRow | None:
    """Load parent access_hash + healthy harvester for a channel/megagroup."""
    if peer_type == "channel":
        table = "telegram_channels"
    elif peer_type == "megagroup":
        table = "telegram_megagroups"
    else:
        return None

    query = f"""
        SELECT
            p.external_id,
            p.username,
            r.access_hash,
            COALESCE(p.harvester_id, r.harvester_id) AS harvester_id
        FROM {table} p
        LEFT JOIN LATERAL (
            SELECT r2.access_hash, r2.harvester_id
            FROM telegram_harvester_peer_roles r2
            INNER JOIN telegram_harvesters h2
                ON h2.id = r2.harvester_id
               AND h2.is_active = true
               AND EXISTS (
                 SELECT 1 FROM telegram_harvester_sessions hs
                 WHERE hs.harvester_id = h2.id
                   AND hs.auth_state = 'healthy'
               )
            WHERE r2.external_id = p.external_id
              AND r2.role = 'parent'
              AND r2.access_hash IS NOT NULL
            ORDER BY
                CASE WHEN p.harvester_id IS NOT NULL AND r2.harvester_id = p.harvester_id
                     THEN 0 ELSE 1 END,
                r2.updated_at DESC NULLS LAST
            LIMIT 1
        ) r ON true
        WHERE p.external_id = %s
        LIMIT 1;
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id,))
        row = await cur.fetchone()
    if not row:
        return None
    raw_access_hash = row.get("access_hash")
    return UsernameOwnershipRow(
        external_id=str(row["external_id"]),
        peer_type=peer_type,
        username=str(row["username"]) if row.get("username") is not None else None,
        access_hash=int(raw_access_hash) if raw_access_hash is not None else None,
        harvester_id=row.get("harvester_id"),
    )


async def get_partial_telegram_user_by_phone(
    conn: AsyncConnection[DictRow],
    phone: str,
) -> UserPartialView | None:
    """Look up a user by phone (E.164); matches digit-only regardless of stored formatting."""
    digits = "".join(c for c in phone if c.isdigit())
    if not digits:
        return None

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_USER_PARTIAL_SELECT}
            FROM telegram_users
            WHERE regexp_replace(COALESCE(phone, ''), '[^0-9]', '', 'g') = %s
            LIMIT 1;
            """,
            (digits,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return UserPartialView.from_row(row)


async def get_partial_telegram_user_by_external_id(
    conn: AsyncConnection[DictRow],
    external_id: str,
) -> UserPartialView | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_USER_PARTIAL_SELECT}
            FROM telegram_users
            WHERE external_id = %s;
            """,
            (external_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return UserPartialView.from_row(row)


async def get_partial_telegram_users_by_external_ids(
    conn: AsyncConnection[DictRow],
    external_ids: list[str],
) -> dict[str, UserPartialView]:
    """Batch-load user partials keyed by ``external_id`` (e.g. message senders for a search page)."""
    if not external_ids:
        return {}

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_USER_PARTIAL_SELECT}
            FROM telegram_users
            WHERE external_id = ANY(%s);
            """,
            (external_ids,),
        )
        rows = await cur.fetchall()
    return {str(row["external_id"]): UserPartialView.from_row(row) for row in rows}


async def get_partial_communities_by_external_ids(
    conn: AsyncConnection[DictRow],
    external_ids: list[str],
) -> dict[str, KeywordPeerView]:
    """Batch-load channel/megagroup/chat partials keyed by ``external_id``.

    Looks up all three tables; first hit wins (channels before megagroups before chats).
    """
    if not external_ids:
        return {}

    out: dict[str, KeywordPeerView] = {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT external_id, title, username, photo, profile_photo_s3_uri, is_verified
            FROM telegram_channels
            WHERE external_id = ANY(%s);
            """,
            (external_ids,),
        )
        for row in await cur.fetchall():
            view = KeywordPeerView.from_row(row, peer_type="channel")
            out[view.external_id] = view

        missing = [eid for eid in external_ids if eid not in out]
        if missing:
            await cur.execute(
                """
                SELECT external_id, title, username, photo, profile_photo_s3_uri, is_verified
                FROM telegram_megagroups
                WHERE external_id = ANY(%s);
                """,
                (missing,),
            )
            for row in await cur.fetchall():
                view = KeywordPeerView.from_row(row, peer_type="megagroup")
                out[view.external_id] = view

        missing = [eid for eid in external_ids if eid not in out]
        if missing:
            await cur.execute(
                """
                SELECT external_id, title, photo, profile_photo_s3_uri
                FROM telegram_chats
                WHERE external_id = ANY(%s);
                """,
                (missing,),
            )
            for row in await cur.fetchall():
                view = KeywordPeerView.from_row(row, peer_type="chat")
                out[view.external_id] = view
    return out


async def get_community_views_by_external_ids(
    conn: AsyncConnection[DictRow],
    external_ids: list[str],
) -> dict[str, CommunityView]:
    """Batch-load ``CommunityView`` rows (with UUID) keyed by ``external_id``.

    Categories are omitted for strip/insights use. First hit wins across
    channels → megagroups → chats.
    """
    if not external_ids:
        return {}

    out: dict[str, CommunityView] = {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              'channel'::text AS peer_type,
              id AS community_id,
              external_id,
              title,
              username,
              usernames,
              photo,
              participants_count,
              profile_photo_s3_uri,
              unavailable_at,
              is_verified,
              is_scam,
              is_fake,
              is_restricted,
              restriction_reason,
              language
            FROM telegram_channels
            WHERE external_id = ANY(%s);
            """,
            (external_ids,),
        )
        for row in await cur.fetchall():
            view = CommunityView.from_row(row)
            if view is not None:
                out[view.external_id] = view

        missing = [eid for eid in external_ids if eid not in out]
        if missing:
            await cur.execute(
                """
                SELECT
                  'megagroup'::text AS peer_type,
                  id AS community_id,
                  external_id,
                  title,
                  username,
                  usernames,
                  photo,
                  participants_count,
                  profile_photo_s3_uri,
                  unavailable_at,
                  is_verified,
                  is_scam,
                  is_fake,
                  is_restricted,
                  restriction_reason,
                  language
                FROM telegram_megagroups
                WHERE external_id = ANY(%s);
                """,
                (missing,),
            )
            for row in await cur.fetchall():
                view = CommunityView.from_row(row)
                if view is not None:
                    out[view.external_id] = view

        missing = [eid for eid in external_ids if eid not in out]
        if missing:
            await cur.execute(
                """
                SELECT
                  'chat'::text AS peer_type,
                  id AS community_id,
                  external_id,
                  title,
                  NULL::text AS username,
                  '[]'::jsonb AS usernames,
                  photo,
                  participants_count,
                  profile_photo_s3_uri,
                  unavailable_at,
                  NULL::boolean AS is_verified,
                  NULL::boolean AS is_scam,
                  NULL::boolean AS is_fake,
                  NULL::boolean AS is_restricted,
                  '[]'::jsonb AS restriction_reason,
                  NULL::text AS language
                FROM telegram_chats
                WHERE external_id = ANY(%s);
                """,
                (missing,),
            )
            for row in await cur.fetchall():
                view = CommunityView.from_row(row)
                if view is not None:
                    out[view.external_id] = view
    if out:
        enriched = await _attach_community_access_errors(conn, list(out.values()))
        enriched = await attach_community_link_privacy(conn, enriched)
        out = {c.external_id: c for c in enriched}
    return out


async def load_community_bots(
    conn: AsyncConnection[DictRow],
    bot_external_ids: list[str],
) -> list[UserPartialView]:
    """Load bot user partials in ``bot_external_ids`` order (skips ids not in DB)."""
    if not bot_external_ids:
        return []
    found = await get_partial_telegram_users_by_external_ids(conn, bot_external_ids)
    return [found[eid] for eid in bot_external_ids if eid in found]


async def get_keyword_peers_by_keys(
    conn: AsyncConnection[DictRow],
    keys: list[tuple[str, str]],
) -> dict[str, KeywordPeerView]:
    """Batch-load community peer partials for ``(external_id, peer_type)`` pairs, keyed by ``external_id``.

    ``peer_type`` selects the source table (channels/megagroups/chats). Chats have no ``username``.
    Category labels are intentionally not joined - keyword results don't need them.
    """
    if not keys:
        return {}

    channel_ids = [k[0] for k in keys if k[1] == "channel"]
    megagroup_ids = [k[0] for k in keys if k[1] == "megagroup"]
    chat_ids = [k[0] for k in keys if k[1] == "chat"]

    out: dict[str, KeywordPeerView] = {}
    async with conn.cursor(row_factory=dict_row) as cur:
        if channel_ids:
            await cur.execute(
                """
                SELECT external_id, title, username, photo, profile_photo_s3_uri, is_verified
                FROM telegram_channels
                WHERE external_id = ANY(%s);
                """,
                (channel_ids,),
            )
            for row in await cur.fetchall():
                view = KeywordPeerView.from_row(row, peer_type="channel")
                out[view.external_id] = view
        if megagroup_ids:
            await cur.execute(
                """
                SELECT external_id, title, username, photo, profile_photo_s3_uri, is_verified
                FROM telegram_megagroups
                WHERE external_id = ANY(%s);
                """,
                (megagroup_ids,),
            )
            for row in await cur.fetchall():
                view = KeywordPeerView.from_row(row, peer_type="megagroup")
                out[view.external_id] = view
        if chat_ids:
            await cur.execute(
                """
                SELECT external_id, title, photo, profile_photo_s3_uri
                FROM telegram_chats
                WHERE external_id = ANY(%s);
                """,
                (chat_ids,),
            )
            for row in await cur.fetchall():
                view = KeywordPeerView.from_row(row, peer_type="chat")
                out[view.external_id] = view
    return out


async def get_partial_telegram_user_by_uuid(conn: AsyncConnection[DictRow], user_id: str) -> UserPartialView | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_USER_PARTIAL_SELECT}
            FROM telegram_users
            WHERE id = %s;
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return UserPartialView.from_row(row)


async def search_users_by_name(
    conn: AsyncConnection[DictRow],
    *,
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[UserPartialView], int]:
    term = normalize_text(q) or ""
    if not term:
        return [], 0

    pattern = _ilike_contains_pattern(term)

    sql = """
        WITH hits AS (
            SELECT id, external_id, engagement_count
            FROM telegram_users
            WHERE
                name_search ILIKE %s ESCAPE '\\'
                OR username_search ILIKE %s ESCAPE '\\'
        ),
        totals AS (
            SELECT COUNT(*)::int AS total
            FROM (SELECT 1 FROM hits LIMIT %s) capped
        ),
        page AS (
            SELECT id
            FROM hits
            ORDER BY engagement_count DESC NULLS LAST, external_id
            LIMIT %s OFFSET %s
        )
        SELECT
          u.id,
          u.external_id,
          u.first_name,
          u.last_name,
          u.username,
          u.phone,
          u.about,
          u.engagement_count,
          u.profile_photo_s3_uri,
          u.verified,
          u.premium,
          u.bot,
          u.deleted,
          u.restricted,
          u.scam,
          u.fake,
          u.unavailable_at,
          u.restriction_reason,
          u.photo,
          totals.total
        FROM page
        CROSS JOIN totals
        JOIN telegram_users u ON u.id = page.id
        ORDER BY u.engagement_count DESC NULLS LAST, u.external_id;
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            sql,
            (pattern, pattern, _NAME_SEARCH_TOTAL_CAP, limit, offset),
        )
        rows = await cur.fetchall()

    total = rows[0]["total"] if rows else 0
    return [UserPartialView.from_row(r) for r in rows], total


async def get_user_stories_and_gifts_row(
    conn: AsyncConnection[DictRow],
    user_id: str,
) -> UserStoriesAndGiftsRow | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                tu.external_id,
                r.access_hash,
                tu.username,
                tu.bot,
                tu.updated_at,
                r.harvester_id,
                h.external_id AS harvester_external_id,
                tu.pinned_stories,
                tu.pinned_stories_fetched_at,
                tu.saved_star_gifts,
                tu.star_gifts_fetched_at
            FROM telegram_users tu
            LEFT JOIN LATERAL (
                SELECT r.access_hash, r.harvester_id
                FROM telegram_harvester_peer_roles r
                INNER JOIN telegram_harvesters h2
                    ON h2.id = r.harvester_id
                   AND h2.is_active = true
                   AND EXISTS (
                     SELECT 1 FROM telegram_harvester_sessions hs
                     WHERE hs.harvester_id = h2.id
                       AND hs.auth_state = 'healthy'
                   )
                WHERE r.external_id = tu.external_id
                  AND r.access_hash IS NOT NULL
                ORDER BY
                    CASE r.role WHEN 'parent' THEN 0 ELSE 1 END,
                    r.updated_at DESC NULLS LAST
                LIMIT 1
            ) r ON true
            LEFT JOIN telegram_harvesters h
                ON h.id = r.harvester_id
               AND h.is_active = true
               AND EXISTS (
                 SELECT 1 FROM telegram_harvester_sessions hs
                 WHERE hs.harvester_id = h.id
                   AND hs.auth_state = 'healthy'
               )
            WHERE tu.id = %s
            LIMIT 1;
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    return user_stories_and_gifts_row_from_db(row) if row else None


async def get_peer_refresh_row(
    conn: AsyncConnection[DictRow],
    external_id: str,
    peer_type: PeerTypeSchema,
) -> PeerRefreshRow | None:
    if peer_type == "channel":
        table = "telegram_channels"
    elif peer_type == "megagroup":
        table = "telegram_megagroups"
    elif peer_type == "chat":
        table = "telegram_chats"
    else:
        return None

    profile_tier_select = "p.profile_tier" if peer_type in ("channel", "megagroup") else "NULL::text AS profile_tier"
    query = f"""
        SELECT
            p.external_id,
            p.updated_at,
            -- Prefer the role that owns access_hash; peer.harvester_id alone can
            -- point at a different account and must not be paired with r.access_hash.
            COALESCE(r.harvester_id, p.harvester_id) AS harvester_id,
            {profile_tier_select},
            r.access_hash,
            h.external_id AS harvester_external_id
        FROM {table} p
        LEFT JOIN LATERAL (
            SELECT r2.access_hash, r2.harvester_id
            FROM telegram_harvester_peer_roles r2
            INNER JOIN telegram_harvesters h2
                ON h2.id = r2.harvester_id
               AND h2.is_active = true
            WHERE r2.external_id = p.external_id
              AND r2.access_hash IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM telegram_harvester_sessions hs
                WHERE hs.harvester_id = h2.id
                  AND hs.auth_state = 'healthy'
              )
            ORDER BY
                CASE WHEN r2.role = 'parent' THEN 0 ELSE 1 END,
                CASE WHEN p.harvester_id IS NOT NULL AND r2.harvester_id = p.harvester_id THEN 0 ELSE 1 END,
                r2.updated_at DESC NULLS LAST
            LIMIT 1
        ) r ON true
        LEFT JOIN telegram_harvesters h
            ON h.id = COALESCE(r.harvester_id, p.harvester_id)
           AND h.is_active = true
           AND EXISTS (
             SELECT 1 FROM telegram_harvester_sessions hs
             WHERE hs.harvester_id = h.id
               AND hs.auth_state = 'healthy'
           )
        WHERE p.external_id = %s
        ORDER BY p.updated_at DESC NULLS LAST
        LIMIT 1
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id,))
        row = await cur.fetchone()
    if not row:
        return None
    raw_access_hash = row.get("access_hash")
    return PeerRefreshRow(
        external_id=str(row["external_id"]),
        access_hash=int(raw_access_hash) if raw_access_hash is not None else None,
        updated_at=row.get("updated_at"),
        harvester_id=row.get("harvester_id"),
        harvester_external_id=(
            str(row["harvester_external_id"]) if row.get("harvester_external_id") is not None else None
        ),
        profile_tier=(str(row["profile_tier"]) if row.get("profile_tier") is not None else None),
    )


def _field_history_old_value_for_api(field_key: str, old_value: Any) -> Any:
    """Return API-safe field history values (photo thumbs as data URLs for the client)."""
    if field_key != "photo" or old_value is None:
        return old_value

    photo = old_value
    if isinstance(photo, str):
        try:
            photo = json.loads(photo)
        except json.JSONDecodeError:
            return old_value
    if not isinstance(photo, dict):
        return old_value

    normalized = normalize_tl_keys(photo)
    thumb_url = get_b64_image_url_from_photo(normalized)
    if thumb_url is None:
        return normalized

    enriched = dict(normalized)
    enriched["stripped_thumb"] = thumb_url
    return enriched


def field_history_payload(history: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "field_key": entry.field_key,
            "old_value": _field_history_old_value_for_api(entry.field_key, entry.old_value),
            "recorded_at": entry.recorded_at,
        }
        for entry in history
    ]


# Affiliation lists load membership rows, then the matching community by external_id.
# Message totals come from user_peer_message_stats. "Reacted" is one index probe per
# community on telegram_message_reactors (reactor_external_id, source_external_id, reacted_at).
_MEMBERSHIP_COLUMNS_SQL = """
  tcp.id AS participant_id,
  tcp.peer_type,
  tcp.external_id,
  tcp.user_id AS user_external_id,
  tcp.is_current_member,
  tcp.left_at,
  tcp.first_seen_at,
  tcp.updated_at,
  tcp.rank,
  tcp.joined_at,
  tcp.created_at
"""

_COMMUNITY_RESOLVE_FROM_MEMBERSHIP_SQL = """
LEFT JOIN telegram_channels ch
  ON mem.peer_type = 'channel'
 AND ch.external_id = mem.external_id
LEFT JOIN telegram_megagroups mg
  ON mem.peer_type = 'megagroup'
 AND mg.external_id = mem.external_id
LEFT JOIN telegram_chats ct
  ON mem.peer_type = 'chat'
 AND ct.external_id = mem.external_id
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'id', c.id,
      'slug', c.slug,
      'label', c.label
    )
    ORDER BY c.tier, c.label
  ) AS categories
  FROM peer_categories pc
  JOIN categories c ON c.id = pc.category_id
  WHERE pc.peer_type = mem.peer_type
    AND pc.community_id = COALESCE(ch.id, mg.id, ct.id)
    AND c.slug NOT IN ('tl-cse-risk')
) cats ON COALESCE(ch.id, mg.id, ct.id) IS NOT NULL
LEFT JOIN LATERAL (
  SELECT
    TRUE AS has_reaction,
    COALESCE(r.reacted_at, r.message_date) AS last_reaction_at
  FROM telegram_message_reactors r
  WHERE r.reactor_external_id = mem.user_external_id
    AND r.source_external_id = mem.external_id
  ORDER BY r.reacted_at DESC NULLS LAST
  LIMIT 1
) rx ON true
LEFT JOIN user_peer_message_stats st
  ON st.user_external_id = mem.user_external_id
 AND st.peer_external_id = mem.external_id
"""

_RESOLVED_COMMUNITY_COLUMNS_SQL = """
  COALESCE(ch.id, mg.id, ct.id) AS community_id,
  COALESCE(ch.title, mg.title, ct.title) AS title,
  CASE mem.peer_type
    WHEN 'channel' THEN ch.username
    WHEN 'megagroup' THEN mg.username
    ELSE NULL::text
  END AS username,
  CASE mem.peer_type
    WHEN 'channel' THEN ch.usernames
    WHEN 'megagroup' THEN mg.usernames
    ELSE '[]'::jsonb
  END AS usernames,
  COALESCE(ch.participants_count, mg.participants_count, ct.participants_count) AS participants_count,
  COALESCE(ch.profile_photo_s3_uri, mg.profile_photo_s3_uri, ct.profile_photo_s3_uri) AS profile_photo_s3_uri,
  COALESCE(ch.photo, mg.photo, ct.photo) AS photo,
  CASE mem.peer_type
    WHEN 'channel' THEN ch.is_verified
    WHEN 'megagroup' THEN mg.is_verified
    ELSE NULL::boolean
  END AS is_verified,
  cats.categories,
  COALESCE(rx.has_reaction, false) AS has_reaction,
  rx.last_reaction_at,
  COALESCE(st.message_count, 0) AS message_count,
  st.last_message_at
"""

_AFFILIATED_PEER_OBJECT_SQL = """
jsonb_build_object(
  'external_id', row.external_id,
  'peer_type', row.peer_type,
  'participant_data', jsonb_build_object(
    'id', row.participant_id,
    'is_current_member', row.is_current_member,
    'left_at', row.left_at,
    'first_seen_at', row.first_seen_at,
    'updated_at', row.updated_at,
    'rank', row.rank,
    'joined_at', row.joined_at,
    'created_at', row.created_at
  ),
  'affiliation', jsonb_build_object(
    'membership', CASE
      WHEN row.is_current_member THEN 'current'
      ELSE 'former'
    END,
    'rank', row.rank,
    'joined_at', row.joined_at,
    'left_at', row.left_at,
    'message_count', COALESCE(row.message_count, 0),
    'reaction_count', 0,
    'last_message_at', row.last_message_at,
    'last_reaction_at', row.last_reaction_at,
    'engagement_kind', CASE
      WHEN COALESCE(row.message_count, 0) > 0 AND row.has_reaction THEN 'messaged_and_reacted'
      WHEN COALESCE(row.message_count, 0) > 0 THEN 'messaged'
      WHEN row.has_reaction THEN 'reacted'
      ELSE 'participant_only'
    END
  ),
  'peer', CASE
    WHEN row.community_id IS NULL THEN jsonb_build_object(
      'peer_type', row.peer_type,
      'community_id', '00000000-0000-0000-0000-000000000000'::uuid,
      'external_id', row.external_id,
      'title', 'Unknown',
      'is_unknown', true,
      'participants_count', 0,
      'categories', '[]'::jsonb
    )
    ELSE jsonb_build_object(
      'peer_type', row.peer_type,
      'community_id', row.community_id,
      'external_id', row.external_id,
      'title', row.title,
      'username', row.username,
      'usernames', row.usernames,
      'participants_count', row.participants_count,
      'profile_photo_s3_uri', row.profile_photo_s3_uri,
      'photo', row.photo,
      'is_verified', row.is_verified,
      'categories', COALESCE(row.categories, '[]'::jsonb),
      'is_unknown', false
    )
  END
)
"""


def _affiliated_peers_agg_sql(membership_source: str) -> str:
    """Aggregate one page of memberships after looking up each community by id."""
    return f"""
SELECT jsonb_agg(peer_row.obj ORDER BY peer_row.joined_at DESC NULLS LAST)
FROM (
  SELECT
    {_AFFILIATED_PEER_OBJECT_SQL} AS obj,
    row.joined_at
  FROM (
    SELECT
      mem.participant_id,
      mem.peer_type,
      mem.external_id,
      mem.user_external_id,
      mem.is_current_member,
      mem.left_at,
      mem.first_seen_at,
      mem.updated_at,
      mem.rank,
      mem.joined_at,
      mem.created_at,
      {_RESOLVED_COMMUNITY_COLUMNS_SQL}
    FROM (
      {membership_source}
    ) mem
    {_COMMUNITY_RESOLVE_FROM_MEMBERSHIP_SQL}
  ) row
) peer_row
"""


_USER_PAGE_MEMBERSHIPS_SQL = f"""
SELECT
{_MEMBERSHIP_COLUMNS_SQL}
FROM telegram_chat_participants tcp
WHERE tcp.user_id = tu.external_id
ORDER BY tcp.joined_at DESC NULLS LAST
LIMIT %s OFFSET %s
"""

_GET_USER_BY_ID_QUERY = f"""
    SELECT
      tu.id,
      tu.external_id,
      tu.first_name,
      tu.last_name,
      tu.username,
      tu.phone,
      tu.engagement_count,
      tu.profile_photo_s3_uri,
      tu.premium,
      tu.verified,
      tu.bot,
      tu.restricted,
      tu.scam,
      tu.fake,
      tu.restriction_reason,
      tu.about,
      tu.birthday,
      tu.deleted,
      tu.deleted_observed_at,
      tu.unavailable_at,
      tu.updated_at,
      tu.photo,
      tu.pinned_stories,
      tu.pinned_stories_fetched_at,
      tu.saved_star_gifts,
      tu.star_gifts_fetched_at,
      (
        SELECT COUNT(*)::int
        FROM telegram_chat_participants tcp
        WHERE tcp.user_id = tu.external_id
      ) AS affiliated_peers_total,
      COALESCE((
        {_affiliated_peers_agg_sql(_USER_PAGE_MEMBERSHIPS_SQL)}
      ), '[]'::jsonb) AS affiliated_peers
    FROM telegram_users tu
    WHERE tu.id = %s
    LIMIT 1
"""

# Header snapshot: user row only. Affiliated peers load on the later DB slice.
_GET_USER_HEADER_BY_ID_QUERY = """
    SELECT
      tu.id,
      tu.external_id,
      tu.first_name,
      tu.last_name,
      tu.username,
      tu.phone,
      tu.engagement_count,
      tu.profile_photo_s3_uri,
      tu.premium,
      tu.verified,
      tu.bot,
      tu.restricted,
      tu.scam,
      tu.fake,
      tu.restriction_reason,
      tu.about,
      tu.birthday,
      tu.deleted,
      tu.deleted_observed_at,
      tu.unavailable_at,
      tu.updated_at,
      tu.photo,
      NULL::jsonb AS pinned_stories,
      tu.pinned_stories_fetched_at,
      NULL::jsonb AS saved_star_gifts,
      tu.star_gifts_fetched_at,
      (tu.pinned_stories IS NOT NULL) AS has_pinned_stories_payload,
      (tu.saved_star_gifts IS NOT NULL) AS has_saved_star_gifts_payload,
      0 AS affiliated_peers_total,
      '[]'::jsonb AS affiliated_peers
    FROM telegram_users tu
    WHERE tu.id = %s
    LIMIT 1
"""

_LIST_UNFILTERED_MEMBERSHIPS_SQL = f"""
SELECT
{_MEMBERSHIP_COLUMNS_SQL}
FROM telegram_chat_participants tcp
WHERE tcp.user_id = %s
ORDER BY tcp.joined_at DESC NULLS LAST
LIMIT %s OFFSET %s
"""

_LIST_USER_AFFILIATED_PEERS_UNFILTERED_QUERY = f"""
SELECT
  (
    SELECT COUNT(*)::int
    FROM telegram_chat_participants tcp
    WHERE tcp.user_id = %s
  ) AS total,
  COALESCE((
    {_affiliated_peers_agg_sql(_LIST_UNFILTERED_MEMBERSHIPS_SQL)}
  ), '[]'::jsonb) AS affiliated_peers
"""

_FILTERED_PAGE_MEMBERSHIPS_SQL = """
SELECT
  matched.participant_id,
  matched.peer_type,
  matched.external_id,
  matched.user_external_id,
  matched.is_current_member,
  matched.left_at,
  matched.first_seen_at,
  matched.updated_at,
  matched.rank,
  matched.joined_at,
  matched.created_at
FROM matched
ORDER BY matched.joined_at DESC NULLS LAST
LIMIT %s OFFSET %s
"""

_LIST_USER_AFFILIATED_PEERS_QUERY = f"""
WITH memberships AS (
  SELECT
{_MEMBERSHIP_COLUMNS_SQL}
  FROM telegram_chat_participants tcp
  WHERE tcp.user_id = %s
),
resolved AS (
  SELECT
    mem.participant_id,
    mem.peer_type,
    mem.external_id,
    mem.user_external_id,
    mem.is_current_member,
    mem.left_at,
    mem.first_seen_at,
    mem.updated_at,
    mem.rank,
    mem.joined_at,
    mem.created_at,
    COALESCE(ch.id, mg.id, ct.id) AS community_id,
    COALESCE(ch.title, mg.title, ct.title) AS title,
    CASE mem.peer_type
      WHEN 'channel' THEN ch.username
      WHEN 'megagroup' THEN mg.username
      ELSE NULL::text
    END AS username,
    CASE mem.peer_type
      WHEN 'channel' THEN ch.usernames
      WHEN 'megagroup' THEN mg.usernames
      ELSE '[]'::jsonb
    END AS usernames
  FROM memberships mem
  LEFT JOIN telegram_channels ch
    ON mem.peer_type = 'channel'
   AND ch.external_id = mem.external_id
  LEFT JOIN telegram_megagroups mg
    ON mem.peer_type = 'megagroup'
   AND mg.external_id = mem.external_id
  LEFT JOIN telegram_chats ct
    ON mem.peer_type = 'chat'
   AND ct.external_id = mem.external_id
),
matched AS (
  SELECT *
  FROM resolved
  WHERE %s::text = ''
     OR external_id ILIKE %s ESCAPE '\\'
     OR COALESCE(title, '') ILIKE %s ESCAPE '\\'
     OR COALESCE(username, '') ILIKE %s ESCAPE '\\'
     OR COALESCE(usernames::text, '') ILIKE %s ESCAPE '\\'
)
SELECT
  (SELECT COUNT(*)::int FROM matched) AS total,
  COALESCE((
    {_affiliated_peers_agg_sql(_FILTERED_PAGE_MEMBERSHIPS_SQL)}
  ), '[]'::jsonb) AS affiliated_peers
"""


async def get_user_by_id(
    conn: AsyncConnection[DictRow],
    user_id: str,
    *,
    affiliated_limit: int = 10,
    include_stories_gifts: bool = True,
    include_affiliated_peers: bool = True,
    include_field_history: bool = True,
) -> UserPeerDetailView | None:
    light = not include_stories_gifts and not include_affiliated_peers
    query = _GET_USER_HEADER_BY_ID_QUERY if light else _GET_USER_BY_ID_QUERY
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (user_id,) if light else (affiliated_limit, 0, user_id))
        row = await cur.fetchone()
    if not row:
        return None
    out = dict(row)
    has_stories_payload = bool(out.pop("has_pinned_stories_payload", False))
    has_gifts_payload = bool(out.pop("has_saved_star_gifts_payload", False))
    out["affiliated_peers"] = enrich_affiliated_peers(out.get("affiliated_peers"))
    if include_field_history:
        history = await list_field_history_for_peer(
            conn,
            str(out["external_id"]),
            peer_type="user",
        )
        out["field_history"] = field_history_payload(history)
    else:
        out["field_history"] = []
    detail = UserPeerDetailView.from_row(out)
    affiliated = [] if light else await _attach_affiliated_peers_link_privacy(conn, detail.affiliated_peers)
    updates: dict[str, object] = {"affiliated_peers": affiliated}
    if light and not detail.bot:
        updates["pinned_stories_fetch_pending"] = detail.pinned_stories_fetched_at is None or has_stories_payload
        updates["saved_star_gifts_fetch_pending"] = detail.star_gifts_fetched_at is None or has_gifts_payload
    return detail.model_copy(update=updates)


async def list_user_affiliated_peers(
    conn: AsyncConnection[DictRow],
    user_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
    q: str | None = None,
) -> UserAffiliatedPeersPage | None:
    needle = (q or "").strip()
    like = _ilike_contains_pattern(needle) if needle else "%"
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT external_id
            FROM telegram_users
            WHERE id = %s
            LIMIT 1
            """,
            (user_id,),
        )
        user_row = await cur.fetchone()
        if not user_row:
            return None

        user_external_id = str(user_row["external_id"])
        if needle:
            await cur.execute(
                _LIST_USER_AFFILIATED_PEERS_QUERY,
                (
                    user_external_id,
                    needle,
                    like or "%",
                    like or "%",
                    like or "%",
                    like or "%",
                    limit,
                    offset,
                ),
            )
        else:
            await cur.execute(
                _LIST_USER_AFFILIATED_PEERS_UNFILTERED_QUERY,
                (user_external_id, user_external_id, limit, offset),
            )
        row = await cur.fetchone()

    if not row:
        return UserAffiliatedPeersPage(items=[], total=0, limit=limit, offset=offset)

    enriched = enrich_affiliated_peers(row.get("affiliated_peers"))
    parsed = UserPeerDetailView.from_row(
        {
            "id": user_id,
            "external_id": user_external_id,
            "engagement_count": 0,
            "premium": False,
            "verified": False,
            "bot": False,
            "affiliated_peers": enriched,
            "affiliated_peers_total": int(row.get("total") or 0),
        }
    )
    items = await _attach_affiliated_peers_link_privacy(conn, parsed.affiliated_peers)
    return UserAffiliatedPeersPage(
        items=items,
        total=int(row.get("total") or 0),
        limit=limit,
        offset=offset,
    )


async def _resolve_user_external_id(
    conn: AsyncConnection[DictRow],
    user_id: str,
) -> str | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT external_id
            FROM telegram_users
            WHERE id = %s
            LIMIT 1
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return str(row["external_id"])


async def get_user_peer_affiliation(
    conn: AsyncConnection[DictRow],
    user_id: str,
    *,
    peer_type: PeerTypeSchema,
    peer_external_id: str,
) -> UserPeerAffiliationDetailView | None:
    user_external_id = await _resolve_user_external_id(conn, user_id)
    if user_external_id is None:
        return None

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              tcp.id AS participant_id,
              tcp.peer_type,
              tcp.external_id,
              tcp.is_current_member,
              tcp.left_at,
              tcp.first_seen_at,
              tcp.updated_at,
              tcp.rank,
              tcp.joined_at,
              tcp.created_at,
              COALESCE(st.message_count, 0) AS message_count,
              0::int AS reaction_count,
              st.last_message_at,
              rx.last_reaction_at,
              COALESCE(rx.has_reaction, false) AS has_reaction
            FROM telegram_chat_participants tcp
            LEFT JOIN user_peer_message_stats st
              ON st.user_external_id = tcp.user_id
             AND st.peer_external_id = tcp.external_id
            LEFT JOIN LATERAL (
              SELECT
                TRUE AS has_reaction,
                COALESCE(r.reacted_at, r.message_date) AS last_reaction_at
              FROM telegram_message_reactors r
              WHERE r.reactor_external_id = tcp.user_id
                AND r.source_external_id = tcp.external_id
              ORDER BY r.reacted_at DESC NULLS LAST
              LIMIT 1
            ) rx ON true
            WHERE tcp.user_id = %s
              AND tcp.external_id = %s
              AND tcp.peer_type = %s
            LIMIT 1
            """,
            (user_external_id, peer_external_id, peer_type),
        )
        row = await cur.fetchone()

    if not row:
        return None

    participant = PeerParticipantView.model_validate(
        {
            "id": row["participant_id"],
            "is_current_member": row["is_current_member"],
            "left_at": row.get("left_at"),
            "first_seen_at": row.get("first_seen_at"),
            "updated_at": row.get("updated_at"),
            "rank": row.get("rank"),
            "joined_at": row.get("joined_at"),
            "created_at": row.get("created_at"),
        }
    )
    affiliation = UserPeerAffiliationSummaryView.from_participant(
        participant,
        message_count=int(row.get("message_count") or 0),
        reaction_count=int(row.get("reaction_count") or 0),
        last_message_at=row.get("last_message_at"),
        last_reaction_at=row.get("last_reaction_at"),
        has_reaction=bool(row.get("has_reaction")),
    )

    peer_view: CommunityView | None = None
    table = {
        "channel": "telegram_channels",
        "megagroup": "telegram_megagroups",
        "chat": "telegram_chats",
    }.get(peer_type)
    if table:
        peer_row = await _fetch_peer_row_by_external_id(conn, table=table, external_id=peer_external_id)
        if peer_row:
            peer_view = community_view_from_peer_row(peer_row, peer_type=peer_type)
    if peer_view is None:
        peer_view = CommunityView.unknown(
            peer_type=peer_type,  # type: ignore[arg-type]
            external_id=peer_external_id,
        )
    else:
        peer_view = (await attach_community_link_privacy(conn, [peer_view]))[0]

    return UserPeerAffiliationDetailView(
        user_id=UUID(user_id),
        user_external_id=user_external_id,
        peer=peer_view,
        participant_data=participant,
        affiliation=affiliation,
    )


async def list_user_peer_affiliation_messages(
    conn: AsyncConnection[DictRow],
    user_id: str,
    *,
    peer_external_id: str,
    limit: int = 50,
    offset: int = 0,
) -> UserPeerAffiliationMessagesPage | None:
    user_external_id = await _resolve_user_external_id(conn, user_id)
    if user_external_id is None:
        return None

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT COUNT(*)::int AS total
            FROM telegram_messages
            WHERE from_external_id = %s
              AND source_external_id = %s
              AND COALESCE(deleted, false) = false
            """,
            (user_external_id, peer_external_id),
        )
        total_row = await cur.fetchone()
        total = int((total_row or {}).get("total") or 0)

        await cur.execute(
            """
            SELECT *
            FROM telegram_messages
            WHERE from_external_id = %s
              AND source_external_id = %s
              AND COALESCE(deleted, false) = false
            ORDER BY date DESC, message_id DESC
            LIMIT %s OFFSET %s
            """,
            (user_external_id, peer_external_id, limit, offset),
        )
        rows = list(await cur.fetchall())

    messages = [
        TelegramMessageView.from_row(
            dict(row),
            edits=[],
            source_external_id=peer_external_id,
        )
        for row in rows
    ]
    return UserPeerAffiliationMessagesPage(
        items=messages,
        total=total,
        limit=limit,
        offset=offset,
    )


async def list_user_peer_affiliation_reactions(
    conn: AsyncConnection[DictRow],
    user_id: str,
    *,
    peer_external_id: str,
    limit: int = 50,
    offset: int = 0,
) -> UserPeerAffiliationReactionsPage | None:
    user_external_id = await _resolve_user_external_id(conn, user_id)
    if user_external_id is None:
        return None

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT COUNT(*)::int AS total
            FROM telegram_message_reactors r
            WHERE r.reactor_external_id = %s
              AND r.source_external_id = %s
            """,
            (user_external_id, peer_external_id),
        )
        total_row = await cur.fetchone()
        total = int((total_row or {}).get("total") or 0)

        await cur.execute(
            """
            SELECT
              r.reaction_key,
              r.reaction,
              r.reacted_at,
              m.message_id,
              m.id AS message_uuid,
              m.date AS message_date,
              m.content AS message_content,
              m.peer_type AS message_peer_type
            FROM (
              SELECT
                reaction_key,
                reaction,
                reacted_at,
                message_id,
                message_date
              FROM telegram_message_reactors
              WHERE reactor_external_id = %s
                AND source_external_id = %s
              ORDER BY reacted_at DESC NULLS LAST, message_date DESC, reaction_key
              LIMIT %s OFFSET %s
            ) r
            INNER JOIN telegram_messages m
              ON m.id = r.message_id
             AND m.date = r.message_date
            ORDER BY r.reacted_at DESC NULLS LAST, r.message_date DESC, r.reaction_key
            """,
            (user_external_id, peer_external_id, limit, offset),
        )
        rows = list(await cur.fetchall())

    items: list[UserPeerReactionView] = []
    for row in rows:
        reaction = row.get("reaction")
        items.append(
            UserPeerReactionView(
                reaction_key=str(row["reaction_key"]),
                reaction=dict(reaction) if isinstance(reaction, Mapping) else {},
                reacted_at=row.get("reacted_at"),
                message_id=int(row["message_id"]),
                message_uuid=row["message_uuid"]
                if isinstance(row["message_uuid"], UUID)
                else UUID(str(row["message_uuid"])),
                message_date=row.get("message_date"),
                message_content=row.get("message_content"),
                message_peer_type=row.get("message_peer_type"),
            )
        )
    return UserPeerAffiliationReactionsPage(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


async def upsert_community(
    conn: AsyncConnection[DictRow],
    row: TelegramChannelSchema | TelegramChatSchema,
    peer_type: PeerTypeSchema,
) -> None:
    values: Mapping[str, Any] = row.model_dump(mode="json", exclude_none=True)
    columns = list(values.keys())

    if isinstance(row, TelegramChannelSchema):
        if peer_type == "channel":
            sql = build_upsert_sql(
                table="telegram_channels",
                columns=columns,
                conflict_columns=["external_id"],
            )
        elif peer_type == "megagroup":
            sql = build_upsert_sql(
                table="telegram_megagroups",
                columns=columns,
                conflict_columns=["external_id"],
            )
        else:
            logger.error("[upsert_community] Channel schema with unexpected peer_type=%s", peer_type)
            return
    elif isinstance(row, TelegramChatSchema):
        sql = build_upsert_sql(
            table="telegram_chats",
            columns=columns,
            conflict_columns=["external_id"],
        )
    else:
        logger.error("[upsert_community] Row of type '%s' is not supported.", type(row))
        return

    params = tuple(adapt_param(values[c]) for c in columns)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)


async def get_categories(conn: AsyncConnection[DictRow]) -> list:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT *
            FROM public.categories
            ORDER BY slug DESC
            """,
        )
        rows = await cur.fetchall()

    if not rows:
        return []

    return [row for row in rows]


async def get_paged_telegram_channels(conn: AsyncConnection[DictRow], page_size: int):
    offset = 0
    async with conn.cursor(row_factory=dict_row) as cur:
        while True:
            await cur.execute(
                """
                SELECT id, title, username, about FROM telegram_channels
                LIMIT %s OFFSET %s
                """,
                (page_size, offset),
            )
            rows = await cur.fetchall()
            if not rows:
                break
            yield [dict(row) for row in rows]
            offset += page_size


async def get_paged_telegram_megagroups(conn: AsyncConnection[DictRow], page_size: int):
    offset = 0
    async with conn.cursor(row_factory=dict_row) as cur:
        while True:
            await cur.execute(
                """
                SELECT id, title, username, about FROM telegram_megagroups
                LIMIT %s OFFSET %s
                """,
                (page_size, offset),
            )
            rows = await cur.fetchall()
            if not rows:
                break
            yield [dict(row) for row in rows]
            offset += page_size


async def add_category(
    conn: AsyncConnection[DictRow],
    peer_type,
    category,
    community_id,
) -> None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO peer_categories (
                peer_type, category_id, community_id
            )
            VALUES (
                %s, %s, %s
            )
            """,
            (peer_type, category, community_id),
        )
