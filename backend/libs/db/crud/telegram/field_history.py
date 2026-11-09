from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import JsonValue

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row

from db.schemas.telegram.field_history import (
    PeerFieldHistoryTypeSchema,
    TelegramPeerFieldHistorySchema,
)
from db.utils import adapt_jsonb_param

UsernameHandleMap = dict[str, str]

USER_SCALAR_FIELDS: tuple[str, ...] = (
    "username",
    "first_name",
    "last_name",
    "about",
    "phone",
    "private_forward_name",
    "premium",
    "verified",
    "bot",
    "restricted",
    "scam",
    "fake",
    "deleted",
)
USER_JSON_FIELDS: tuple[str, ...] = ("birthday", "restriction_reason")

CHANNEL_SCALAR_FIELDS: tuple[str, ...] = (
    "title",
    "about",
    "is_verified",
    "is_scam",
    "is_fake",
    "is_restricted",
    "is_forum",
    "linked_chat_id",
    "linked_monoforum_id",
    "migrated_from_chat_id",
)
CHANNEL_JSON_FIELDS: tuple[str, ...] = (
    "restriction_reason",
    "location",
    "default_banned_rights",
)

CHAT_SCALAR_FIELDS: tuple[str, ...] = ("title", "about", "deactivated")
CHAT_JSON_FIELDS: tuple[str, ...] = ("migrated_to", "default_banned_rights")

TABLE_PEER_TYPE: dict[str, PeerFieldHistoryTypeSchema] = {
    "telegram_users": "user",
    "telegram_channels": "channel",
    "telegram_megagroups": "megagroup",
    "telegram_chats": "chat",
}

TABLE_TRACKED_COLUMNS: dict[str, tuple[str, ...]] = {
    "telegram_users": USER_SCALAR_FIELDS + USER_JSON_FIELDS + ("photo",),
    "telegram_channels": CHANNEL_SCALAR_FIELDS + CHANNEL_JSON_FIELDS + ("photo", "username", "usernames"),
    "telegram_megagroups": CHANNEL_SCALAR_FIELDS + CHANNEL_JSON_FIELDS + ("photo", "username", "usernames"),
    "telegram_chats": CHAT_SCALAR_FIELDS + CHAT_JSON_FIELDS + ("photo",),
}


@dataclass(frozen=True, slots=True)
class FieldChange:
    field_key: str
    old_value: JsonValue


def normalize_username(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lstrip("@")
    if not cleaned:
        return None
    return cleaned.lower()


def _canonical_username(value: str) -> str:
    return value.strip().lstrip("@")


def usernames_from_user(username: str | None) -> UsernameHandleMap:
    normalized = normalize_username(username)
    if normalized is None:
        return {}
    return {normalized: _canonical_username(username or "")}


def username_sets_equal(old: UsernameHandleMap, new: UsernameHandleMap) -> bool:
    return set(old.keys()) == set(new.keys())


def usernames_from_channel(
    username: str | None,
    usernames: list[Any] | None,
) -> UsernameHandleMap:
    handles: UsernameHandleMap = {}
    handles.update(usernames_from_user(username))
    if not usernames:
        return handles
    for entry in usernames:
        if not isinstance(entry, Mapping):
            continue
        raw_username = entry.get("username")
        if not isinstance(raw_username, str):
            continue
        if entry.get("active") is False:
            continue
        normalized = normalize_username(raw_username)
        if normalized is None:
            continue
        handles[normalized] = _canonical_username(raw_username)
    return handles


_PHOTO_EMPTY_TLS = frozenset({"PhotoEmpty", "ChatPhotoEmpty", "UserProfilePhotoEmpty"})


def extract_photo_id(photo: Any) -> str | None:
    """Return a stable Telegram photo id across ChatPhoto / UserProfilePhoto / Photo shapes."""
    if photo is None:
        return None
    if isinstance(photo, Mapping):
        tl = photo.get("tl") or photo.get("_")
        if tl in _PHOTO_EMPTY_TLS:
            return None
        if tl == "Photo":
            raw_id = photo.get("id")
        else:
            raw_id = photo.get("photo_id")
            if raw_id is None:
                raw_id = photo.get("id")
        if raw_id is not None:
            return str(raw_id)
    return None


def _canonical_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    except TypeError:
        return str(value)


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    text = value.strip()
    return text or None


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def _is_empty_photo(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, Mapping):
        tl = value.get("tl") or value.get("_")
        if tl in _PHOTO_EMPTY_TLS:
            return True
        if extract_photo_id(value) is None and tl in {"Photo", "ChatPhoto", "UserProfilePhoto"}:
            return True
    return _is_empty(value)


def _field_is_empty(field_key: str, value: Any) -> bool:
    if field_key == "photo":
        return _is_empty_photo(value)
    return _is_empty(value)


def _values_equal(field_key: str, old: Any, new: Any) -> bool:
    if field_key == "photo":
        if _is_empty_photo(old) and _is_empty_photo(new):
            return True
        return extract_photo_id(old) == extract_photo_id(new)
    if field_key == "username":
        return normalize_username(_normalize_string(old)) == normalize_username(_normalize_string(new))
    if field_key in USER_JSON_FIELDS or field_key in CHANNEL_JSON_FIELDS or field_key in CHAT_JSON_FIELDS:
        return _canonical_json(old) == _canonical_json(new)
    if isinstance(old, bool) or isinstance(new, bool):
        return bool(old) == bool(new)
    if isinstance(old, (int, float)) or isinstance(new, (int, float)):
        if old is None and new is None:
            return True
        try:
            return old == new
        except TypeError:
            return str(old) == str(new)
    return _normalize_string(old) == _normalize_string(new)


def _history_old_value(field_key: str, old: Any) -> Any:
    if field_key == "photo":
        return old
    if field_key in USER_JSON_FIELDS or field_key in CHANNEL_JSON_FIELDS or field_key in CHAT_JSON_FIELDS:
        return old
    if isinstance(old, bool):
        return old
    if isinstance(old, (int, float)) and not isinstance(old, bool):
        return old
    return _normalize_string(old)


def _should_record_field_change(field_key: str, old: Any, new: Any) -> bool:
    if _values_equal(field_key, old, new):
        return False
    # First observation (empty → value) is not a historical change. This matters when a
    # light stub row is later filled in by full materialization.
    if _field_is_empty(field_key, old):
        return False
    return True


UNAUTHORITATIVE_PROFILE_FIELDS: tuple[str, ...] = ("first_name", "last_name", "photo")


def drop_unauthoritative_profile_overwrites(
    values: dict[str, Any],
    old_row: Mapping[str, Any] | None,
) -> None:
    """Keep existing profile names/photo when the incoming User is min or a contact.

    Telegram returns locally saved contact-book names and personal photos when
    ``contact`` is set, and ``min`` constructors are cached/incomplete. Light
    upserts from those objects must still fill empty fields on first insert, but
    must not clobber a known public profile or emit oscillating field history.
    """
    if not old_row:
        return
    for field_key in UNAUTHORITATIVE_PROFILE_FIELDS:
        if field_key not in values:
            continue
        old_val = old_row.get(field_key)
        if _field_is_empty(field_key, old_val):
            continue
        values.pop(field_key, None)


def _diff_scalar_fields(
    *,
    old_row: Mapping[str, Any],
    new_row: Mapping[str, Any],
    scalar_fields: tuple[str, ...],
) -> list[FieldChange]:
    changes: list[FieldChange] = []
    for field_key in scalar_fields:
        if field_key not in new_row:
            continue
        old_val = old_row.get(field_key)
        new_val = new_row.get(field_key)
        if not _should_record_field_change(field_key, old_val, new_val):
            continue
        changes.append(FieldChange(field_key=field_key, old_value=_history_old_value(field_key, old_val)))
    return changes


def _diff_json_fields(
    *,
    old_row: Mapping[str, Any],
    new_row: Mapping[str, Any],
    json_fields: tuple[str, ...],
) -> list[FieldChange]:
    changes: list[FieldChange] = []
    for field_key in json_fields:
        if field_key not in new_row:
            continue
        old_val = old_row.get(field_key)
        new_val = new_row.get(field_key)
        if not _should_record_field_change(field_key, old_val, new_val):
            continue
        changes.append(FieldChange(field_key=field_key, old_value=_history_old_value(field_key, old_val)))
    return changes


def _diff_photo_field(
    *,
    old_row: Mapping[str, Any],
    new_row: Mapping[str, Any],
) -> list[FieldChange]:
    if "photo" not in new_row:
        return []
    old_photo = old_row.get("photo")
    new_photo = new_row.get("photo")
    if not _should_record_field_change("photo", old_photo, new_photo):
        return []
    return [FieldChange(field_key="photo", old_value=old_photo)]


def _diff_channel_usernames(
    *,
    old_row: Mapping[str, Any],
    new_row: Mapping[str, Any],
) -> list[FieldChange]:
    if "username" not in new_row and "usernames" not in new_row:
        return []
    username = new_row["username"] if "username" in new_row else old_row.get("username")
    usernames = new_row["usernames"] if "usernames" in new_row else old_row.get("usernames")
    old_handles = usernames_from_channel(old_row.get("username"), old_row.get("usernames"))
    new_handles = usernames_from_channel(username, usernames)
    # Only removals are historical. First-seen / newly added handles are not "previous".
    removed = set(old_handles.keys()) - set(new_handles.keys())
    return [FieldChange(field_key="username", old_value=old_handles[normalized]) for normalized in removed]


def diff_peer_fields(
    old_row: Mapping[str, Any] | None,
    new_row: Mapping[str, Any],
    peer_type: PeerFieldHistoryTypeSchema,
) -> list[FieldChange]:
    if not old_row:
        return []

    if peer_type == "user":
        user_scalars = tuple(f for f in USER_SCALAR_FIELDS if f != "username")
        changes = _diff_scalar_fields(old_row=old_row, new_row=new_row, scalar_fields=user_scalars)
        if "username" in new_row:
            username_old = old_row.get("username")
            username_new = new_row.get("username")
            if _should_record_field_change("username", username_old, username_new):
                changes.append(
                    FieldChange(
                        field_key="username",
                        old_value=_history_old_value("username", username_old),
                    )
                )
        changes.extend(_diff_json_fields(old_row=old_row, new_row=new_row, json_fields=USER_JSON_FIELDS))
        changes.extend(_diff_photo_field(old_row=old_row, new_row=new_row))
        return changes

    if peer_type in ("channel", "megagroup"):
        changes = _diff_scalar_fields(old_row=old_row, new_row=new_row, scalar_fields=CHANNEL_SCALAR_FIELDS)
        changes.extend(_diff_json_fields(old_row=old_row, new_row=new_row, json_fields=CHANNEL_JSON_FIELDS))
        changes.extend(_diff_channel_usernames(old_row=old_row, new_row=new_row))
        changes.extend(_diff_photo_field(old_row=old_row, new_row=new_row))
        return changes

    if peer_type == "chat":
        changes = _diff_scalar_fields(old_row=old_row, new_row=new_row, scalar_fields=CHAT_SCALAR_FIELDS)
        changes.extend(_diff_json_fields(old_row=old_row, new_row=new_row, json_fields=CHAT_JSON_FIELDS))
        changes.extend(_diff_photo_field(old_row=old_row, new_row=new_row))
        return changes

    return []


def diff_historical_peer_fields(
    current_row: Mapping[str, Any],
    historical_row: Mapping[str, Any],
    peer_type: PeerFieldHistoryTypeSchema,
) -> list[FieldChange]:
    """Record an older snapshot as previous values without touching the current row.

    Live upserts treat the DB row as outgoing and the Telegram payload as current.
    A historical dataset is older than live data, so non-empty historical values
    that differ from ``current_row`` become ``old_value`` entries instead.
    Empty historical cells are skipped (missing CSV data is not a known blank).
    """
    if peer_type not in ("channel", "megagroup"):
        return []

    changes: list[FieldChange] = []
    for field_key in CHANNEL_SCALAR_FIELDS:
        if field_key not in historical_row:
            continue
        hist_val = historical_row.get(field_key)
        if _field_is_empty(field_key, hist_val):
            continue
        curr_val = current_row.get(field_key)
        if _values_equal(field_key, hist_val, curr_val):
            continue
        changes.append(FieldChange(field_key=field_key, old_value=_history_old_value(field_key, hist_val)))

    if "username" in historical_row or "usernames" in historical_row:
        hist_username = historical_row.get("username") if "username" in historical_row else None
        hist_usernames = historical_row.get("usernames") if "usernames" in historical_row else None
        hist_handles = usernames_from_channel(hist_username, hist_usernames)
        curr_handles = usernames_from_channel(current_row.get("username"), current_row.get("usernames"))
        for normalized, canonical in hist_handles.items():
            if normalized in curr_handles:
                continue
            changes.append(FieldChange(field_key="username", old_value=canonical))

    return changes


def history_value_already_recorded(
    field_key: str,
    candidate_old_value: Any,
    existing_old_values: Sequence[Any],
) -> bool:
    """True when ``candidate_old_value`` is already stored for this field."""
    return any(_values_equal(field_key, candidate_old_value, existing) for existing in existing_old_values)


async def fetch_peer_row_snapshot(
    conn: AsyncConnection,
    *,
    table_name: str,
    external_id: str,
) -> dict[str, Any] | None:
    columns = TABLE_TRACKED_COLUMNS.get(table_name)
    if not columns:
        return None
    fields = ", ".join(columns)
    query = f"SELECT {fields} FROM {table_name} WHERE external_id = %s LIMIT 1"
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (external_id,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def sync_peer_field_history(
    conn: AsyncConnection,
    *,
    external_id: str,
    peer_type: PeerFieldHistoryTypeSchema,
    changes: list[FieldChange],
    recorded_at: datetime | None = None,
) -> None:
    if not changes:
        return
    at = recorded_at or datetime.now(UTC)
    async with conn.cursor() as cur:
        for change in changes:
            await cur.execute(
                """
                INSERT INTO telegram_peer_field_history (
                  external_id,
                  peer_type,
                  field_key,
                  old_value,
                  recorded_at
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    external_id,
                    peer_type,
                    change.field_key,
                    adapt_jsonb_param(change.old_value),
                    at,
                ),
            )


def harvest_s3_uri_from_photo_value(photo: Any) -> str | None:
    if not isinstance(photo, dict):
        return None
    harvest = photo.get("harvest")
    if not isinstance(harvest, dict):
        return None
    uri = harvest.get("s3_uri")
    return str(uri) if uri else None


def _photo_id_from_history_value(old_value: Any) -> int | None:
    if old_value is None:
        return None
    if hasattr(old_value, "model_dump"):
        try:
            old_value = old_value.model_dump(mode="json")
        except Exception:
            pass
    if not isinstance(old_value, dict):
        return None
    tl = old_value.get("tl")
    if tl == "Photo":
        raw = old_value.get("id")
    else:
        raw = old_value.get("photo_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _photo_ids_equal(left: int | None, right: int | None) -> bool:
    if left is None or right is None:
        return False
    return str(left) == str(right)


def photo_observed_at_from_tl(photo: Any) -> datetime | None:
    """Return when Telegram recorded the profile photo (``Photo.date``), if present."""
    if photo is None:
        return None

    raw_date: Any
    if isinstance(photo, Mapping):
        raw_date = photo.get("date")
    elif hasattr(photo, "date"):
        raw_date = photo.date
    else:
        return None

    if raw_date is None:
        return None
    if isinstance(raw_date, datetime):
        return raw_date.replace(tzinfo=UTC) if raw_date.tzinfo is None else raw_date
    if isinstance(raw_date, str):
        text = raw_date.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed
    return None


async def list_photo_harvest_index(
    conn: AsyncConnection,
    external_id: str,
    *,
    peer_type: PeerFieldHistoryTypeSchema,
    current_photo: Any | None = None,
) -> dict[int, str]:
    """Map Telegram ``photo_id`` to harvested ``s3_uri`` from peer photo + field history."""
    index: dict[int, str] = {}

    if isinstance(current_photo, dict):
        photo_id = _photo_id_from_history_value(current_photo)
        uri = harvest_s3_uri_from_photo_value(current_photo)
        if photo_id is not None and uri:
            index[photo_id] = uri

    history = await list_field_history_for_peer(
        conn,
        external_id,
        peer_type=peer_type,
        field_key="photo",
    )
    for entry in history:
        photo_id = _photo_id_from_history_value(entry.old_value)
        uri = harvest_s3_uri_from_photo_value(entry.old_value)
        if photo_id is not None and uri and photo_id not in index:
            index[photo_id] = uri
    return index


def _merge_harvest_into_photo_dict(
    photo_dict: dict[str, Any],
    *,
    s3_uri: str,
    content_type: str | None = None,
    media_kind: str | None = None,
) -> dict[str, Any]:
    out = dict(photo_dict)
    existing = out.get("harvest")
    harvest: dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}
    harvest["s3_uri"] = s3_uri
    if content_type:
        harvest["content_type"] = content_type
    if media_kind:
        harvest["media_kind"] = media_kind
    out["harvest"] = harvest
    return out


async def persist_profile_photo_harvest_to_field_history(
    conn: AsyncConnection,
    *,
    external_id: str,
    peer_type: PeerFieldHistoryTypeSchema,
    photo_id: int,
    photo_value: dict[str, Any],
    is_current: bool,
    observed_at: datetime | None = None,
) -> bool:
    """Attach harvest metadata to an existing photo history row, or insert one for backfilled photos.

    Returns ``True`` when a history row was updated or inserted.
    """
    harvest = photo_value.get("harvest")
    if not isinstance(harvest, dict):
        return False
    s3_uri = harvest.get("s3_uri")
    if not s3_uri:
        return False

    history = await list_field_history_for_peer(
        conn,
        external_id,
        peer_type=peer_type,
        field_key="photo",
    )
    matched = False
    async with conn.cursor() as cur:
        for entry in history:
            old_value = entry.old_value
            if not isinstance(old_value, dict):
                continue
            entry_photo_id = _photo_id_from_history_value(old_value)
            if not _photo_ids_equal(entry_photo_id, photo_id):
                continue
            matched = True
            updated = _merge_harvest_into_photo_dict(
                old_value,
                s3_uri=str(s3_uri),
                content_type=harvest.get("content_type"),
                media_kind=harvest.get("media_kind"),
            )
            await cur.execute(
                """
                UPDATE telegram_peer_field_history
                SET old_value = %s
                WHERE id = %s
                """,
                (adapt_jsonb_param(updated), entry.id),
            )

        if matched:
            return False
        if is_current:
            return False

        recorded_at = observed_at or photo_observed_at_from_tl(photo_value) or datetime.now(UTC)
        await cur.execute(
            """
            INSERT INTO telegram_peer_field_history (
              external_id,
              peer_type,
              field_key,
              old_value,
              recorded_at
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                external_id,
                peer_type,
                "photo",
                adapt_jsonb_param(photo_value),
                recorded_at,
            ),
        )
    return True


async def list_field_history_for_peer(
    conn: AsyncConnection[DictRow],
    external_id: str,
    *,
    peer_type: PeerFieldHistoryTypeSchema | None = None,
    field_key: str | None = None,
) -> list[TelegramPeerFieldHistorySchema]:
    params: list[Any] = [external_id]
    clauses = ["external_id = %s"]
    if peer_type is not None:
        clauses.append("peer_type = %s")
        params.append(peer_type)
    if field_key is not None:
        clauses.append("field_key = %s")
        params.append(field_key)

    query = f"""
        SELECT id, external_id, peer_type, field_key, old_value, recorded_at, created_at
        FROM telegram_peer_field_history
        WHERE {" AND ".join(clauses)}
        ORDER BY recorded_at DESC, created_at DESC
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, tuple(params))
        rows = await cur.fetchall()
    return [TelegramPeerFieldHistorySchema.model_validate(row) for row in rows]
