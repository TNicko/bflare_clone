"""Upserts and catalogue search for canonical Bluesky actors."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row

from db.crud.bluesky.field_history import (
    diff_actor_fields,
    fetch_actor_row_snapshot,
    sync_actor_field_history,
)
from db.crud.bluesky.search_normalize import attach_actor_search_fields, normalize_text
from db.schemas.bluesky.actor import BlueskyActorSchema, BlueskyHostingStatusSchema
from db.utils import adapt_jsonb_param, adapt_param, build_upsert_sql, schema_upsert_values


def _ilike_contains_pattern(term: str) -> str:
    """Wrap ``term`` for a contains ILIKE, escaping LIKE metacharacters.

    Pair with ``ILIKE %s ESCAPE '\\'``.
    """
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


async def search_actors_by_name(
    conn: AsyncConnection[DictRow],
    *,
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    """ILIKE search on NFKC ``display_name_search`` / ``handle_search`` (trgm-backed)."""
    term = normalize_text(q) or ""
    if not term:
        return [], 0

    pattern = _ilike_contains_pattern(term)
    query = """
        WITH filtered AS (
            SELECT
                id,
                did,
                handle,
                handle_valid,
                display_name,
                description,
                avatar_url,
                avatar_s3_uri,
                followers_count,
                posts_count,
                unavailable_at
            FROM bluesky_actors
            WHERE
                display_name_search ILIKE %s ESCAPE '\\'
                OR handle_search ILIKE %s ESCAPE '\\'
        ),
        paged AS (
            SELECT
                *,
                COUNT(*) OVER()::int AS total
            FROM filtered
            ORDER BY followers_count DESC NULLS LAST, did
            LIMIT %s OFFSET %s
        )
        SELECT
            id,
            did,
            handle,
            handle_valid,
            display_name,
            description,
            avatar_url,
            avatar_s3_uri,
            followers_count,
            posts_count,
            unavailable_at,
            total
        FROM paged;
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (pattern, pattern, limit, offset))
        rows = await cur.fetchall()

    total = int(rows[0]["total"]) if rows else 0
    return [dict(r) for r in rows], total


async def list_actors_by_dids(
    conn: AsyncConnection[DictRow],
    *,
    dids: list[str],
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    """Load catalogue rows for ``dids``, preserving input order."""
    if not dids:
        return [], 0
    total = len(dids)
    sliced = dids[offset : offset + limit]
    if not sliced:
        return [], total
    query = """
        SELECT
            id,
            did,
            handle,
            handle_valid,
            display_name,
            description,
            avatar_url,
            avatar_s3_uri,
            followers_count,
            posts_count,
            unavailable_at
        FROM bluesky_actors
        WHERE did = ANY(%s)
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (sliced,))
        rows = await cur.fetchall()
    by_did = {str(row["did"]): dict(row) for row in rows}
    return [by_did[did] for did in sliced if did in by_did], total


_JSONB_COLUMNS = frozenset(
    {
        "avatar",
        "banner",
        "labels",
        "pinned_post",
        "joined_via_starter_pack",
    }
)

# Clearing these when the API explicitly returns empty is a real change.
ACTOR_EXPLICIT_NULL_FIELDS = frozenset(
    {
        "handle",
        "display_name",
        "description",
        "pronouns",
        "website",
        "avatar",
        "banner",
        "avatar_url",
        "banner_url",
        "pinned_post",
        "joined_via_starter_pack",
        "hosting_status",
        "unavailable_at",
        "profile_cid",
    }
)


async def upsert_actor(
    conn: AsyncConnection,
    row: BlueskyActorSchema,
    *,
    preserve_created_at_on_conflict: bool = True,
    preserve_updated_at_on_conflict: bool = False,
) -> bool:
    """Upsert a Bluesky actor row and append field-history diffs.

    Returns True when a row already existed (refresh), False on insert.
    """
    old_row = await fetch_actor_row_snapshot(conn, did=row.did)
    values = schema_upsert_values(row, explicit_null_fields=ACTOR_EXPLICIT_NULL_FIELDS)
    attach_actor_search_fields(values, old_row=old_row)
    if "last_synced_at" not in values:
        values["last_synced_at"] = datetime.now(UTC)
    if "updated_at" not in values and not preserve_updated_at_on_conflict:
        values["updated_at"] = datetime.now(UTC)

    columns = list(values.keys())
    exclude_from_update: list[str] = []
    if preserve_created_at_on_conflict:
        exclude_from_update.append("created_at")
    if preserve_updated_at_on_conflict:
        exclude_from_update.append("updated_at")

    query = build_upsert_sql(
        table="bluesky_actors",
        columns=columns,
        conflict_columns=("did",),
        exclude_from_update=exclude_from_update or None,
    )
    params = tuple(adapt_jsonb_param(values[c]) if c in _JSONB_COLUMNS else adapt_param(values[c]) for c in columns)

    async with conn.cursor() as cur:
        await cur.execute(query, params)

    changes = diff_actor_fields(old_row, values)
    await sync_actor_field_history(conn, did=row.did, changes=changes)
    return old_row is not None


async def set_actor_hosting_status(
    conn: AsyncConnection,
    did: str,
    *,
    active: bool,
    hosting_status: BlueskyHostingStatusSchema | None,
    repo_rev: str | None = None,
    observed_at: datetime | None = None,
) -> None:
    """Update hosting lifecycle fields and record history when they change."""
    at = observed_at or datetime.now(UTC)
    old_row = await fetch_actor_row_snapshot(conn, did=did)
    new_row: dict[str, Any] = {
        "active": active,
        "hosting_status": hosting_status,
    }
    changes = diff_actor_fields(old_row, new_row)

    assignments = [
        sql.SQL("active = %s"),
        sql.SQL("hosting_status = %s"),
        sql.SQL("hosting_status_observed_at = %s"),
        sql.SQL("updated_at = now()"),
        sql.SQL("last_synced_at = now()"),
    ]
    params: list[Any] = [active, hosting_status, at]
    if repo_rev is not None:
        assignments.append(sql.SQL("repo_rev = %s"))
        params.append(repo_rev)
    params.append(did)

    query = sql.SQL("UPDATE bluesky_actors SET {assignments} WHERE did = %s").format(
        assignments=sql.SQL(", ").join(assignments),
    )
    async with conn.cursor() as cur:
        await cur.execute(query, tuple(params))

    await sync_actor_field_history(conn, did=did, changes=changes, recorded_at=at)


async def set_actor_unavailability(
    conn: AsyncConnection,
    did: str,
    *,
    unavailable: bool,
) -> None:
    """Set or clear ``unavailable_at`` on a Bluesky actor row."""
    if unavailable:
        query = """
            UPDATE bluesky_actors
            SET unavailable_at = COALESCE(unavailable_at, now()),
                updated_at = now()
            WHERE did = %s
        """
    else:
        query = """
            UPDATE bluesky_actors
            SET unavailable_at = NULL,
                updated_at = now()
            WHERE did = %s
        """
    async with conn.cursor() as cur:
        await cur.execute(query, (did,))
