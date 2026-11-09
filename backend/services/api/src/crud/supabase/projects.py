from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row

from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase.peer_rows import enrich_telegram_peer_row
from services.api.src.crud.supabase.peers import (
    get_partial_communities_by_external_ids,
    get_partial_telegram_users_by_external_ids,
)
from services.api.src.crud.supabase.views.general.project import (
    ProjectAlertView,
    ProjectMessageMappingView,
    ProjectReportView,
    ProjectView,
)
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelView,
    TelegramChatView,
    TelegramMegagroupView,
)
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.message_media import build_message_media_preview
from services.api.src.message_peer_partials import (
    is_channel_external_id,
    signed_id_from_fwd_peer,
)
from services.api.src.models.message_media import MessageMediaPreview
from telegram.mappers import get_b64_image_url_from_photo, profile_photo_media_kind
from telegram.utils import channel_id_from_signed_peer_id

_PROJECT_MESSAGES_SELECT_SQL = """
SELECT
    ptm.id,
    ptm.project,
    ptm.message,
    tm.message_id,
    tm.source_external_id::text AS source_external_id,
    tm.peer_type,
    tm.message_type,
    tm.content,
    tm.from_external_id::text AS from_external_id,
    tm.date,
    tm.media AS media_raw,
    tm.fwd_from,
    tm.reply_to,
    COALESCE(tc.title, tmg.title, tch.title) AS peer_title,
    COALESCE(tc.username, tmg.username) AS peer_username,
    COALESCE(
        tc.profile_photo_s3_uri,
        tmg.profile_photo_s3_uri,
        tch.profile_photo_s3_uri
    ) AS peer_profile_photo_s3_uri,
    COALESCE(tc.photo, tmg.photo, tch.photo) AS peer_photo
FROM project_telegram_messages ptm
INNER JOIN telegram_messages tm ON tm.id = ptm.message
LEFT JOIN telegram_channels tc
    ON tm.peer_type = 'channel' AND tc.external_id::text = tm.source_external_id::text
LEFT JOIN telegram_megagroups tmg
    ON tm.peer_type = 'megagroup' AND tmg.external_id::text = tm.source_external_id::text
LEFT JOIN telegram_chats tch
    ON tm.peer_type = 'chat' AND tch.external_id::text = tm.source_external_id::text
"""

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
    GROUP BY pc_all.community_id
"""


async def project_belongs_to_user(
    conn: AsyncConnection[DictRow],
    *,
    project_id: str | UUID,
    user_id: UUID,
) -> bool:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT 1 FROM projects WHERE id = %s AND user_id = %s",
            (str(project_id), user_id),
        )
        return await cur.fetchone() is not None


async def count_user_pins(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
) -> dict[str, int]:
    """Pin counts across all of the user's projects."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              (
                SELECT COUNT(*)::int
                FROM project_telegram_users m
                JOIN projects p ON p.id = m.project
                WHERE p.user_id = %s
              ) AS users,
              (
                (SELECT COUNT(*)::int FROM project_telegram_channels m
                 JOIN projects p ON p.id = m.project WHERE p.user_id = %s)
                + (SELECT COUNT(*)::int FROM project_telegram_megagroups m
                   JOIN projects p ON p.id = m.project WHERE p.user_id = %s)
                + (SELECT COUNT(*)::int FROM project_telegram_chats m
                   JOIN projects p ON p.id = m.project WHERE p.user_id = %s)
              ) AS communities,
              (
                SELECT COUNT(*)::int
                FROM project_telegram_messages m
                JOIN projects p ON p.id = m.project
                WHERE p.user_id = %s
              ) AS messages
            """,
            (user_id, user_id, user_id, user_id, user_id),
        )
        row = await cur.fetchone()
    if row is None:
        return {"users": 0, "communities": 0, "messages": 0}
    return {
        "users": int(row["users"] or 0),
        "communities": int(row["communities"] or 0),
        "messages": int(row["messages"] or 0),
    }


def _project_peer_query(
    *, table: sql.Identifier, mapping_table: sql.Identifier, fk_column: sql.Identifier
) -> sql.Composed:
    return sql.SQL(
        """
        SELECT tc.*, COALESCE(sub.categories, '[]'::jsonb) AS categories
        FROM {table} tc
        LEFT JOIN ({categories_subquery}) sub
          ON sub.community_id::uuid = tc.id
        INNER JOIN {mapping_table} ptm
          ON ptm.{fk_column} = tc.id
        WHERE ptm.project = %s
        """
    ).format(
        table=table,
        categories_subquery=sql.SQL(_CATEGORIES_SUBQUERY),
        mapping_table=mapping_table,
        fk_column=fk_column,
    )


async def _fetch_project_peers(
    conn: AsyncConnection[DictRow],
    project_id: UUID,
    *,
    table: str,
    mapping_table: str,
    fk_column: str,
    peer_type: str,
    view_cls: type[TelegramChannelView] | type[TelegramMegagroupView] | type[TelegramChatView],
) -> list[Any]:
    query = _project_peer_query(
        table=sql.Identifier(table),
        mapping_table=sql.Identifier(mapping_table),
        fk_column=sql.Identifier(fk_column),
    )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (project_id,))
        rows = await cur.fetchall()
    return [view_cls.model_validate(enrich_telegram_peer_row(dict(row), peer_type=peer_type)) for row in rows]


async def _attach_project_details(conn: AsyncConnection[DictRow], project: Mapping[str, Any]) -> ProjectView:
    project_id = project["id"]
    if not isinstance(project_id, UUID):
        project_id = UUID(str(project_id))

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                tu.id,
                tu.external_id,
                tu.first_name,
                tu.last_name,
                tu.username,
                tu.phone,
                tu.about,
                tu.engagement_count,
                tu.profile_photo_s3_uri,
                tu.verified,
                tu.premium,
                tu.bot,
                tu.deleted,
                tu.photo
            FROM telegram_users tu
            INNER JOIN project_telegram_users ptu ON tu.id = ptu."user"
            WHERE ptu.project = %s
            """,
            (project_id,),
        )
        user_rows = await cur.fetchall()

        await cur.execute("SELECT * FROM project_alerts WHERE project = %s", (project_id,))
        alert_rows = await cur.fetchall()

        await cur.execute("SELECT * FROM project_reports WHERE project = %s", (project_id,))
        report_rows = await cur.fetchall()

    channels = await _fetch_project_peers(
        conn,
        project_id,
        table="telegram_channels",
        mapping_table="project_telegram_channels",
        fk_column="channel",
        peer_type="channel",
        view_cls=TelegramChannelView,
    )
    megagroups = await _fetch_project_peers(
        conn,
        project_id,
        table="telegram_megagroups",
        mapping_table="project_telegram_megagroups",
        fk_column="megagroup",
        peer_type="megagroup",
        view_cls=TelegramMegagroupView,
    )
    chats = await _fetch_project_peers(
        conn,
        project_id,
        table="telegram_chats",
        mapping_table="project_telegram_chats",
        fk_column="chat",
        peer_type="chat",
        view_cls=TelegramChatView,
    )
    telegram_messages, message_peers, message_users = await _fetch_project_messages(conn, project_id)

    return ProjectView(
        id=project_id,
        name=project["name"],
        description=project.get("description"),
        created_at=project.get("created_at"),
        telegram_channels=channels,
        telegram_megagroups=megagroups,
        telegram_chats=chats,
        telegram_users=[UserPartialView.from_row(row) for row in user_rows],
        telegram_messages=telegram_messages,
        telegram_message_peers=message_peers,
        telegram_message_users=message_users,
        alerts=[ProjectAlertView.model_validate(row) for row in alert_rows],
        reports=[ProjectReportView.model_validate(row) for row in report_rows],
    )


def _project_message_from_row(
    row: Mapping[str, Any],
    *,
    media: MessageMediaPreview | None = None,
) -> ProjectMessageMappingView:
    fwd = row.get("fwd_from")
    reply = row.get("reply_to")
    peer_photo = row.get("peer_photo")
    peer_title = row.get("peer_title")
    source = row.get("source_external_id")
    title = (str(peer_title).strip() if peer_title else "") or (str(source) if source is not None else None)
    return ProjectMessageMappingView(
        id=row["id"],
        project=row["project"],
        message=row["message"],
        message_id=int(row["message_id"]) if row.get("message_id") is not None else None,
        source_external_id=str(source) if source is not None else None,
        peer_type=row.get("peer_type"),
        message_type=row.get("message_type"),
        content=row.get("content"),
        from_external_id=(str(row["from_external_id"]) if row.get("from_external_id") is not None else None),
        date=row.get("date"),
        media=media,
        fwd_from=dict(fwd) if isinstance(fwd, Mapping) else None,
        reply_to=dict(reply) if isinstance(reply, Mapping) else None,
        peer_title=title,
        peer_username=row.get("peer_username"),
        peer_profile_photo_s3_uri=row.get("peer_profile_photo_s3_uri"),
        peer_profile_photo_media_kind=profile_photo_media_kind(
            photo=peer_photo,
            profile_photo_s3_uri=row.get("peer_profile_photo_s3_uri"),
        ),
        peer_stripped_profile_photo=get_b64_image_url_from_photo(peer_photo),
    )


async def _rows_to_project_messages(
    rows: list[Mapping[str, Any]],
) -> list[ProjectMessageMappingView]:
    if not rows:
        return []
    region = ConfigFactory.get_config().aws_region.strip()
    previews = await asyncio.gather(*[build_message_media_preview(row.get("media_raw"), region=region) for row in rows])
    return [_project_message_from_row(row, media=preview) for row, preview in zip(rows, previews, strict=True)]


def _as_record(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping) and not isinstance(value, (str, bytes)):
        return dict(value)
    return None


def _user_id_from_peer(peer: Any) -> str | None:
    record = _as_record(peer)
    if not record or record.get("tl") != "PeerUser":
        return None
    user_id = record.get("user_id")
    if isinstance(user_id, int) and user_id > 0:
        return str(user_id)
    return None


def _community_id_from_peer(peer: Any) -> str | None:
    return signed_id_from_fwd_peer(peer)


def _collect_context_ids(
    messages: list[ProjectMessageMappingView],
) -> tuple[set[str], set[str], list[tuple[str, int]]]:
    """Community ids, user ids, and (source, reply_msg_id) pairs for enrichment."""
    community_ids: set[str] = set()
    user_ids: set[str] = set()
    reply_keys: list[tuple[str, int]] = []

    for message in messages:
        if message.source_external_id and message.peer_type in ("channel", "megagroup", "chat"):
            community_ids.add(str(message.source_external_id))
        if message.from_external_id:
            from_id = str(message.from_external_id)
            if is_channel_external_id(from_id):
                community_ids.add(from_id)
            else:
                user_ids.add(from_id)

        fwd = _as_record(message.fwd_from)
        if fwd:
            for key in ("from_id", "saved_from_peer"):
                cid = _community_id_from_peer(fwd.get(key))
                if cid:
                    community_ids.add(cid)
                uid = _user_id_from_peer(fwd.get(key))
                if uid:
                    user_ids.add(uid)

        reply = _as_record(message.reply_to)
        if not reply:
            continue
        reply_from = _as_record(reply.get("reply_from"))
        from_id = reply_from.get("from_id") if reply_from else None
        cid = _community_id_from_peer(from_id) or _community_id_from_peer(reply.get("reply_to_peer_id"))
        if cid:
            community_ids.add(cid)
        uid = _user_id_from_peer(from_id)
        if uid:
            user_ids.add(uid)

        reply_msg_id = reply.get("reply_to_msg_id")
        if message.source_external_id and isinstance(reply_msg_id, int) and reply_msg_id > 0:
            reply_keys.append((str(message.source_external_id), reply_msg_id))

    # Deduplicate reply keys while preserving order.
    seen: set[tuple[str, int]] = set()
    unique_reply_keys: list[tuple[str, int]] = []
    for key in reply_keys:
        if key in seen:
            continue
        seen.add(key)
        unique_reply_keys.append(key)
    return community_ids, user_ids, unique_reply_keys


async def _load_reply_message_rows(
    conn: AsyncConnection[DictRow],
    reply_keys: list[tuple[str, int]],
) -> dict[tuple[str, int], Mapping[str, Any]]:
    if not reply_keys:
        return {}
    sources = [source for source, _ in reply_keys]
    msg_ids = [msg_id for _, msg_id in reply_keys]
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                source_external_id::text AS source_external_id,
                message_id,
                content,
                from_external_id::text AS from_external_id
            FROM telegram_messages
            WHERE (source_external_id::text, message_id) IN (
                SELECT * FROM unnest(%s::text[], %s::bigint[]) AS t(source_external_id, message_id)
            )
            """,
            (sources, msg_ids),
        )
        rows = await cur.fetchall()
    out: dict[tuple[str, int], Mapping[str, Any]] = {}
    for row in rows:
        source = row.get("source_external_id")
        msg_id = row.get("message_id")
        if source is None or msg_id is None:
            continue
        out[(str(source), int(msg_id))] = row
    return out


def _with_reply_snippet(
    message: ProjectMessageMappingView,
    reply_rows: Mapping[tuple[str, int], Mapping[str, Any]],
) -> ProjectMessageMappingView:
    reply = _as_record(message.reply_to)
    if not reply or not message.source_external_id:
        return message
    reply_msg_id = reply.get("reply_to_msg_id")
    if not isinstance(reply_msg_id, int):
        return message
    row = reply_rows.get((str(message.source_external_id), reply_msg_id))
    if not row:
        return message

    patched = dict(reply)
    existing_quote = patched.get("quote_text")
    if not (isinstance(existing_quote, str) and existing_quote.strip()):
        content = row.get("content")
        snippet = content.strip() if isinstance(content, str) and content.strip() else None
        if snippet:
            patched["quote_text"] = snippet

    # Ensure reply_from.from_id exists so the client can resolve author via peers/users.
    reply_from = _as_record(patched.get("reply_from")) or {}
    if not _as_record(reply_from.get("from_id")):
        from_eid = row.get("from_external_id")
        if from_eid is not None and str(from_eid).strip():
            from_id = str(from_eid)
            peer_from_id: dict[str, Any] | None = None
            if is_channel_external_id(from_id):
                try:
                    raw = channel_id_from_signed_peer_id(int(from_id))
                except (TypeError, ValueError):
                    raw = None
                if raw is not None:
                    peer_from_id = {"tl": "PeerChannel", "channel_id": raw}
            elif from_id.isdigit() and int(from_id) > 0:
                peer_from_id = {"tl": "PeerUser", "user_id": int(from_id)}
            if peer_from_id is not None:
                patched["reply_from"] = {**reply_from, "from_id": peer_from_id}

    if patched == reply:
        return message
    return message.model_copy(update={"reply_to": patched})


async def _enrich_project_messages_context(
    conn: AsyncConnection[DictRow],
    messages: list[ProjectMessageMappingView],
) -> tuple[
    list[ProjectMessageMappingView],
    dict[str, KeywordPeerView],
    dict[str, UserPartialView],
]:
    """Resolve forward/reply peers + reply snippets for MessageCard strips."""
    if not messages:
        return [], {}, {}

    community_ids, user_ids, reply_keys = _collect_context_ids(messages)
    reply_rows = await _load_reply_message_rows(conn, reply_keys)

    for row in reply_rows.values():
        from_eid = row.get("from_external_id")
        if from_eid is None or not str(from_eid).strip():
            continue
        from_id = str(from_eid)
        if is_channel_external_id(from_id):
            community_ids.add(from_id)
        else:
            user_ids.add(from_id)

    peers = await get_partial_communities_by_external_ids(conn, sorted(community_ids)) if community_ids else {}
    users = await get_partial_telegram_users_by_external_ids(conn, sorted(user_ids)) if user_ids else {}
    # Authors that aren't users may still be community peers.
    missing_users = [eid for eid in user_ids if eid not in users]
    if missing_users:
        author_peers = await get_partial_communities_by_external_ids(conn, missing_users)
        peers = {**peers, **author_peers}

    enriched = [_with_reply_snippet(message, reply_rows) for message in messages]
    return enriched, peers, users


async def _fetch_project_messages(
    conn: AsyncConnection[DictRow],
    project_id: UUID,
) -> tuple[
    list[ProjectMessageMappingView],
    dict[str, KeywordPeerView],
    dict[str, UserPartialView],
]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _PROJECT_MESSAGES_SELECT_SQL + " WHERE ptm.project = %s ORDER BY tm.date DESC NULLS LAST",
            (project_id,),
        )
        rows = await cur.fetchall()
    messages = await _rows_to_project_messages(rows)
    return await _enrich_project_messages_context(conn, messages)


async def get_project_telegram_message_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    message: str,
) -> ProjectMessageMappingView | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _PROJECT_MESSAGES_SELECT_SQL + " WHERE ptm.project = %s AND ptm.message = %s",
            (project, message),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    views = await _rows_to_project_messages([row])
    if not views:
        return None
    enriched, _, _ = await _enrich_project_messages_context(conn, views)
    return enriched[0] if enriched else views[0]


DEFAULT_PROJECT_NAME = "Pin Dashboard"
DEFAULT_PROJECT_DESCRIPTION = "Your pinned communities, users, and messages."


async def create_project(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
    name: str | None = None,
    description: str | None = None,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO projects (name, description, user_id)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (name, description, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def ensure_default_project(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
) -> UUID:
    """Return the user's pin dashboard project, creating one if needed."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id FROM projects
            WHERE user_id = %s
            ORDER BY
              CASE WHEN name = %s THEN 0 ELSE 1 END,
              created_at ASC NULLS LAST
            LIMIT 1
            """,
            (user_id, DEFAULT_PROJECT_NAME),
        )
        row = await cur.fetchone()
        if row:
            return row["id"]

        await cur.execute(
            """
            INSERT INTO projects (name, description, user_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, name) DO NOTHING
            RETURNING id
            """,
            (DEFAULT_PROJECT_NAME, DEFAULT_PROJECT_DESCRIPTION, user_id),
        )
        created = await cur.fetchone()
        if created:
            return created["id"]

        await cur.execute(
            """
            SELECT id FROM projects
            WHERE user_id = %s AND name = %s
            LIMIT 1
            """,
            (user_id, DEFAULT_PROJECT_NAME),
        )
        existing = await cur.fetchone()
    if existing is None:
        raise RuntimeError("Failed to ensure default project")
    return existing["id"]


async def create_project_telegram_channel_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    channel: str,
    user_id: UUID,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_telegram_channels (project, channel)
            SELECT p.id, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            RETURNING id
            """,
            (channel, project, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def create_project_telegram_megagroup_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    megagroup: str,
    user_id: UUID,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_telegram_megagroups (project, megagroup)
            SELECT p.id, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            RETURNING id
            """,
            (megagroup, project, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def create_project_telegram_chat_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    chat: str,
    user_id: UUID,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_telegram_chats (project, chat)
            SELECT p.id, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            RETURNING id
            """,
            (chat, project, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def create_project_telegram_user_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    user: str,
    user_id: UUID,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_telegram_users (project, "user")
            SELECT p.id, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            RETURNING id
            """,
            (user, project, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def create_project_telegram_message_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    message: str,
    user_id: UUID,
) -> ProjectMessageMappingView | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_telegram_messages (project, message)
            SELECT p.id, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            ON CONFLICT (project, message) DO NOTHING
            RETURNING id
            """,
            (message, project, user_id),
        )
        row = await cur.fetchone()
    if row is None:
        # Already pinned (conflict) or project not owned — resolve existing mapping.
        existing = await get_project_telegram_message_mapping(conn, project=project, message=message)
        if existing is not None:
            return existing
        return None
    return await get_project_telegram_message_mapping(conn, project=project, message=message)


async def create_project_alert(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    alert_name: str,
    alert_type: str,
    alert_phrase: str,
    alert_user: str,
    user_id: UUID,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_alerts (
                project, alert_name, alert_type, alert_phrase, alert_user
            )
            SELECT p.id, %s, %s, %s, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            RETURNING id
            """,
            (
                alert_name,
                alert_type,
                alert_phrase,
                alert_user or None,
                project,
                user_id,
            ),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def create_project_report(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    report_name: str,
    report_location: str,
    user_id: UUID,
) -> UUID | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO project_reports (project, report_name, report_location)
            SELECT p.id, %s, %s
            FROM projects p
            WHERE p.id = %s AND p.user_id = %s
            RETURNING id
            """,
            (report_name, report_location, project, user_id),
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def read_project(
    conn: AsyncConnection[DictRow],
    *,
    project_id: str,
    user_id: UUID,
) -> ProjectView | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM projects WHERE id = %s AND user_id = %s",
            (project_id, user_id),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return await _attach_project_details(conn, row)


async def read_projects(
    conn: AsyncConnection[DictRow],
    *,
    user_id: UUID,
) -> list[ProjectView]:
    await ensure_default_project(conn, user_id=user_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT * FROM projects
            WHERE user_id = %s
            ORDER BY
              CASE WHEN name = %s THEN 0 ELSE 1 END,
              created_at ASC NULLS LAST,
              name
            """,
            (user_id, DEFAULT_PROJECT_NAME),
        )
        rows = await cur.fetchall()
    projects: list[ProjectView] = []
    for row in rows:
        projects.append(await _attach_project_details(conn, row))
    return projects


async def delete_project(
    conn: AsyncConnection[DictRow],
    *,
    project_id: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM projects WHERE id = %s AND user_id = %s",
            (project_id, user_id),
        )
        return cur.rowcount > 0


async def update_project_description(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    description: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE projects SET description = %s WHERE id = %s AND user_id = %s",
            (description, project, user_id),
        )
        return cur.rowcount > 0


async def update_project_name(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    name: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE projects SET name = %s WHERE id = %s AND user_id = %s",
            (name, project, user_id),
        )
        return cur.rowcount > 0


async def delete_project_telegram_channel_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    channel: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM project_telegram_channels ptm
            USING projects p
            WHERE ptm.project = p.id
              AND ptm.project = %s
              AND ptm.channel = %s
              AND p.user_id = %s
            """,
            (project, channel, user_id),
        )
        return cur.rowcount > 0


async def delete_project_telegram_megagroup_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    megagroup: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM project_telegram_megagroups ptm
            USING projects p
            WHERE ptm.project = p.id
              AND ptm.project = %s
              AND ptm.megagroup = %s
              AND p.user_id = %s
            """,
            (project, megagroup, user_id),
        )
        return cur.rowcount > 0


async def delete_project_telegram_chat_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    chat: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM project_telegram_chats ptm
            USING projects p
            WHERE ptm.project = p.id
              AND ptm.project = %s
              AND ptm.chat = %s
              AND p.user_id = %s
            """,
            (project, chat, user_id),
        )
        return cur.rowcount > 0


async def delete_project_telegram_user_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    user: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM project_telegram_users ptm
            USING projects p
            WHERE ptm.project = p.id
              AND ptm.project = %s
              AND ptm."user" = %s
              AND p.user_id = %s
            """,
            (project, user, user_id),
        )
        return cur.rowcount > 0


async def delete_project_telegram_message_mapping(
    conn: AsyncConnection[DictRow],
    *,
    project: str,
    message: str,
    user_id: UUID,
) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM project_telegram_messages ptm
            USING projects p
            WHERE ptm.project = p.id
              AND ptm.project = %s
              AND ptm.message = %s
              AND p.user_id = %s
            """,
            (project, message, user_id),
        )
        return cur.rowcount > 0
