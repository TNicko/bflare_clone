"""Field history / diff tracking for Bluesky actors."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import JsonValue
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from db.schemas.bluesky.actor import normalize_handle
from db.utils import adapt_jsonb_param

ACTOR_SCALAR_FIELDS: tuple[str, ...] = (
    "handle",
    "handle_valid",
    "display_name",
    "description",
    "pronouns",
    "website",
    "active",
    "hosting_status",
    "pds_host",
    "profile_cid",
)
ACTOR_BLOB_FIELDS: tuple[str, ...] = ("avatar", "banner")
ACTOR_JSON_FIELDS: tuple[str, ...] = (
    "labels",
    "pinned_post",
    "joined_via_starter_pack",
)

TRACKED_COLUMNS: tuple[str, ...] = ACTOR_SCALAR_FIELDS + ACTOR_BLOB_FIELDS + ACTOR_JSON_FIELDS


@dataclass(frozen=True, slots=True)
class FieldChange:
    field_key: str
    old_value: JsonValue


def extract_blob_cid(blob: Any) -> str | None:
    """Stable content id for an AT Proto blob ref (avatar/banner)."""
    if blob is None:
        return None
    if isinstance(blob, Mapping):
        ref = blob.get("ref")
        if isinstance(ref, Mapping):
            link = ref.get("$link") or ref.get("link")
            if link is not None and str(link).strip():
                return str(link).strip()
        cid = blob.get("cid")
        if cid is not None and str(cid).strip():
            return str(cid).strip()
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


def _field_is_empty(field_key: str, value: Any) -> bool:
    if field_key in ACTOR_BLOB_FIELDS:
        return extract_blob_cid(value) is None and _is_empty(value)
    if field_key == "labels" and value == []:
        return True
    return _is_empty(value)


def _values_equal(field_key: str, old: Any, new: Any) -> bool:
    if field_key in ACTOR_BLOB_FIELDS:
        return extract_blob_cid(old) == extract_blob_cid(new)
    if field_key == "handle":
        return normalize_handle(_normalize_string(old)) == normalize_handle(_normalize_string(new))
    if field_key in ACTOR_JSON_FIELDS:
        return _canonical_json(old) == _canonical_json(new)
    if isinstance(old, bool) or isinstance(new, bool):
        return bool(old) == bool(new)
    return _normalize_string(old) == _normalize_string(new)


def _history_old_value(field_key: str, old: Any) -> Any:
    if field_key in ACTOR_BLOB_FIELDS or field_key in ACTOR_JSON_FIELDS:
        return old
    if isinstance(old, bool):
        return old
    return _normalize_string(old)


def _should_record_field_change(field_key: str, old: Any, new: Any) -> bool:
    if _values_equal(field_key, old, new):
        return False
    # First observation (empty → value) is not historical.
    if _field_is_empty(field_key, old):
        return False
    return True


def diff_actor_fields(
    old_row: Mapping[str, Any] | None,
    new_row: Mapping[str, Any],
) -> list[FieldChange]:
    if not old_row:
        return []
    changes: list[FieldChange] = []
    for field_key in ACTOR_SCALAR_FIELDS + ACTOR_BLOB_FIELDS + ACTOR_JSON_FIELDS:
        if field_key not in new_row:
            continue
        old_val = old_row.get(field_key)
        new_val = new_row.get(field_key)
        if not _should_record_field_change(field_key, old_val, new_val):
            continue
        changes.append(FieldChange(field_key=field_key, old_value=_history_old_value(field_key, old_val)))
    return changes


async def fetch_actor_row_snapshot(
    conn: AsyncConnection,
    *,
    did: str,
) -> dict[str, Any] | None:
    fields = ", ".join(TRACKED_COLUMNS)
    query = f"SELECT {fields} FROM bluesky_actors WHERE did = %s LIMIT 1"
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (did,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def sync_actor_field_history(
    conn: AsyncConnection,
    *,
    did: str,
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
                INSERT INTO bluesky_actor_field_history (
                  did,
                  field_key,
                  old_value,
                  recorded_at
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    did,
                    change.field_key,
                    adapt_jsonb_param(change.old_value),
                    at,
                ),
            )
