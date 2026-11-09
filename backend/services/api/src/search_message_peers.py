"""Shared peer/sender enrichment for keyword-hydrated message search rows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row

from services.api.src.crud.supabase.peers import (
    get_keyword_peers_by_keys,
    get_partial_communities_by_external_ids,
    get_partial_telegram_users_by_external_ids,
)
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.message_peer_partials import is_channel_external_id, signed_id_from_fwd_peer
from telegram.utils import channel_id_from_signed_peer_id


def source_peer_row_fields(peer: KeywordPeerView | None) -> dict[str, Any]:
    if peer is None:
        return {
            "peer_title": "",
            "peer_username": None,
            "peer_profile_photo_s3_uri": None,
            "peer_profile_photo_media_kind": None,
            "peer_stripped_profile_photo": None,
        }
    return {
        "peer_title": peer.title,
        "peer_username": peer.username,
        "peer_profile_photo_s3_uri": peer.profile_photo_s3_uri,
        "peer_profile_photo_media_kind": peer.profile_photo_media_kind,
        "peer_stripped_profile_photo": peer.stripped_profile_photo,
    }


def from_peer_row_fields(peer: KeywordPeerView | None) -> dict[str, Any]:
    if peer is None:
        return {
            "from_peer_title": None,
            "from_peer_type": None,
            "from_peer_username": None,
            "from_peer_profile_photo_s3_uri": None,
            "from_peer_profile_photo_media_kind": None,
            "from_peer_stripped_profile_photo": None,
        }
    return {
        "from_peer_title": peer.title,
        "from_peer_type": peer.peer_type,
        "from_peer_username": peer.username,
        "from_peer_profile_photo_s3_uri": peer.profile_photo_s3_uri,
        "from_peer_profile_photo_media_kind": peer.profile_photo_media_kind,
        "from_peer_stripped_profile_photo": peer.stripped_profile_photo,
    }


def from_user_row_fields(user: UserPartialView | None) -> dict[str, Any]:
    if user is None:
        return {
            "from_user_id": None,
            "from_user_first_name": None,
            "from_user_last_name": None,
            "from_user_username": None,
            "from_user_profile_photo_s3_uri": None,
            "from_user_profile_photo_media_kind": None,
            "from_user_stripped_thumb": None,
        }
    return {
        "from_user_id": str(user.id),
        "from_user_first_name": user.first_name,
        "from_user_last_name": user.last_name,
        "from_user_username": user.username,
        "from_user_profile_photo_s3_uri": user.profile_photo_s3_uri,
        "from_user_profile_photo_media_kind": user.profile_photo_media_kind,
        "from_user_stripped_thumb": user.stripped_thumb,
    }


async def resolve_source_and_from_partials(
    conn: AsyncConnection[DictRow],
    rows: Iterable[Mapping[str, Any]],
) -> tuple[
    dict[str, KeywordPeerView],
    dict[str, KeywordPeerView],
    dict[str, UserPartialView],
]:
    """Load source community peers + author users/communities for message rows.

    Returns ``(source_peers_by_eid, from_peers_by_eid, users_by_eid)``.
    """
    row_list = list(rows)
    peer_keys = list(
        {
            (str(row["source_external_id"]), str(row["peer_type"]))
            for row in row_list
            if row.get("source_external_id") and row.get("peer_type")
        }
    )
    from_ids = sorted(
        {
            str(row["from_external_id"])
            for row in row_list
            if row.get("from_external_id") is not None and str(row["from_external_id"]).strip() != ""
        }
    )

    source_peers = await get_keyword_peers_by_keys(conn, peer_keys)
    users: dict[str, UserPartialView] = {}
    from_peers: dict[str, KeywordPeerView] = {}
    if from_ids:
        users = await get_partial_telegram_users_by_external_ids(conn, from_ids)
        missing = [eid for eid in from_ids if eid not in users]
        if missing:
            from_peers = await get_partial_communities_by_external_ids(conn, missing)

    return source_peers, from_peers, users


def enrich_message_row_peers(
    row: Mapping[str, Any],
    *,
    source_peers: Mapping[str, KeywordPeerView],
    from_peers: Mapping[str, KeywordPeerView],
    users: Mapping[str, UserPartialView],
) -> dict[str, Any]:
    """Merge source + author peer/user display fields onto a hydrated message row."""
    source_id = str(row.get("source_external_id") or "")
    peer = source_peers.get(source_id)
    raw_from = row.get("from_external_id")
    from_id = str(raw_from) if raw_from is not None and str(raw_from).strip() else ""
    from_user = users.get(from_id) if from_id else None
    from_peer = from_peers.get(from_id) if from_id and from_user is None else None
    return {
        **row,
        **source_peer_row_fields(peer),
        **from_peer_row_fields(from_peer),
        **from_user_row_fields(from_user),
    }


def reply_target_key(
    source_external_id: str | None,
    reply_to: Mapping[str, Any] | None,
) -> tuple[str, int] | None:
    """Peer + Telegram message id of the message being replied to.

    Same-chat replies only store ``reply_to_msg_id``. A different chat sets
    ``reply_to_peer_id`` and the id belongs to that peer.
    """
    if not isinstance(reply_to, Mapping):
        return None
    raw_id = reply_to.get("reply_to_msg_id")
    if isinstance(raw_id, bool):
        return None
    if isinstance(raw_id, int):
        msg_id = raw_id
    elif isinstance(raw_id, str) and raw_id.strip().isdigit():
        msg_id = int(raw_id.strip())
    else:
        return None
    if msg_id <= 0:
        return None
    other = signed_id_from_fwd_peer(reply_to.get("reply_to_peer_id"))
    source = (other or (str(source_external_id).strip() if source_external_id else "")).strip()
    if not source:
        return None
    return source, msg_id


def tl_peer_from_external_id(external_id: str) -> dict[str, Any] | None:
    """Telegram peer dict the client already knows how to resolve."""
    if is_channel_external_id(external_id):
        try:
            raw = channel_id_from_signed_peer_id(int(external_id))
        except (TypeError, ValueError):
            raw = None
        if raw is None:
            return None
        return {"tl": "PeerChannel", "channel_id": int(raw)}
    if external_id.isdigit() and int(external_id) > 0:
        return {"tl": "PeerUser", "user_id": int(external_id)}
    if external_id.startswith("-") and external_id[1:].isdigit():
        return {"tl": "PeerChat", "chat_id": abs(int(external_id))}
    return None


def reply_author_external_id(row: Mapping[str, Any], lookup_source: str) -> str | None:
    """Author of the replied message, or the chat itself for an anonymous channel post."""
    from_eid = str(row.get("from_external_id") or "").strip()
    if from_eid:
        return from_eid
    source = lookup_source.strip()
    if source.startswith("-") or is_channel_external_id(source):
        return source
    return None


def _text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def media_reply_label(media: Any) -> str | None:
    """Short stand-in when the replied message has no text body."""
    from msgstore.reader import is_harvest_stub

    if not isinstance(media, dict) or not media or is_harvest_stub(media):
        return None
    tl = str(media.get("tl") or media.get("_") or "")
    if "Photo" in tl:
        return "Photo"
    if "WebPage" in tl:
        return "Link"
    if "Poll" in tl:
        return "Poll"
    if "Geo" in tl:
        return "Location"
    if "Video" in tl:
        return "Video"
    document = media.get("document")
    mime = str(document.get("mime_type") or "") if isinstance(document, dict) else ""
    if mime.startswith("audio") or "Audio" in tl:
        return "Audio"
    if mime.startswith("video"):
        return "Video"
    return "Attachment"


def reply_body_snippet(row: Mapping[str, Any]) -> str | None:
    text = _text(row.get("content"))
    if text:
        return text
    if row.get("deleted"):
        return "[deleted]"
    return media_reply_label(row.get("media"))


def patched_reply_to(
    reply_to: Mapping[str, Any],
    row: Mapping[str, Any],
    lookup_source: str,
) -> dict[str, Any]:
    """Fill quote text and author onto a reply header when Telegram omitted them."""
    patched = dict(reply_to)
    existing_quote = _text(patched.get("quote_text"))
    if not existing_quote:
        snippet = reply_body_snippet(row)
        if snippet:
            patched["quote_text"] = snippet

    reply_from = patched.get("reply_from")
    reply_from_record = dict(reply_from) if isinstance(reply_from, Mapping) else {}
    if not isinstance(reply_from_record.get("from_id"), Mapping):
        author = reply_author_external_id(row, lookup_source)
        peer = tl_peer_from_external_id(author) if author else None
        if peer is not None and peer.get("tl") == "PeerUser":
            patched["reply_from"] = {**reply_from_record, "from_id": peer}
        elif peer is not None and not isinstance(patched.get("reply_to_peer_id"), Mapping):
            patched["reply_to_peer_id"] = peer
    return patched


def _prefer_reply_row(candidate: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    cand_text = bool(_text(candidate.get("content")))
    curr_text = bool(_text(current.get("content")))
    if cand_text != curr_text:
        return cand_text
    cand_date = candidate.get("date")
    curr_date = current.get("date")
    if cand_date is not None and curr_date is not None and cand_date != curr_date:
        return cand_date > curr_date
    return False


def author_buckets(external_id: str) -> tuple[str | None, str | None]:
    """Split an external id into ``(user_id, community_id)``."""
    if is_channel_external_id(external_id) or (external_id.startswith("-") and external_id[1:].isdigit()):
        return None, external_id
    if external_id.isdigit() and int(external_id) > 0:
        return external_id, None
    return None, None


async def load_reply_targets(
    conn: AsyncConnection[DictRow],
    items: Sequence[Any],
) -> tuple[dict[int, dict[str, Any]], set[str], set[str]]:
    """One lookup for every replied-to message on this page.

    Returns patched ``reply_to`` dicts by item index, plus author user and
    community ids that should join the page's partials.
    """
    keys_by_index: dict[int, tuple[str, int]] = {}
    for index, item in enumerate(items):
        key = reply_target_key(getattr(item, "source_external_id", None), getattr(item, "reply_to", None))
        if key is not None:
            keys_by_index[index] = key
    if not keys_by_index:
        return {}, set(), set()

    unique = list(dict.fromkeys(keys_by_index.values()))
    sources = [source for source, _ in unique]
    msg_ids = [msg_id for _, msg_id in unique]
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                id,
                date,
                source_external_id::text AS source_external_id,
                message_id,
                content,
                from_external_id::text AS from_external_id,
                deleted,
                media,
                archive_key,
                archive_row
            FROM telegram_messages
            WHERE (source_external_id::text, message_id) IN (
                SELECT * FROM unnest(%s::text[], %s::bigint[]) AS t(source_external_id, message_id)
            )
            """,
            (sources, msg_ids),
        )
        fetched = [dict(row) for row in await cur.fetchall()]

    try:
        from msgstore import (
            get_message_archive_prefix,
            get_message_archive_sink,
            hydrate_message_rows_async,
        )

        await hydrate_message_rows_async(
            fetched,
            get_message_archive_sink(),
            prefix=get_message_archive_prefix(),
        )
    except Exception:
        pass

    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for row in fetched:
        source = row.get("source_external_id")
        msg_id = row.get("message_id")
        if source is None or msg_id is None:
            continue
        key = (str(source), int(msg_id))
        current = by_key.get(key)
        if current is None or _prefer_reply_row(row, current):
            by_key[key] = row

    patches: dict[int, dict[str, Any]] = {}
    user_ids: set[str] = set()
    community_ids: set[str] = set()
    for index, key in keys_by_index.items():
        row = by_key.get(key)
        if row is None:
            continue
        reply_to = getattr(items[index], "reply_to", None)
        if not isinstance(reply_to, Mapping):
            continue
        patched = patched_reply_to(reply_to, row, key[0])
        if patched != dict(reply_to):
            patches[index] = patched
        author = reply_author_external_id(row, key[0])
        if not author:
            continue
        user_id, community_id = author_buckets(author)
        if user_id:
            user_ids.add(user_id)
        if community_id:
            community_ids.add(community_id)
    return patches, user_ids, community_ids
