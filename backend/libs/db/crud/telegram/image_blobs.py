"""Canonical image blob lookup, message map, and embed progress (exact pHash)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row

ZERO_PHASH = "0000000000000000"


def normalize_phash_hex(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if len(normalized) != 16 or any(ch not in "0123456789abcdef" for ch in normalized):
        return None
    return normalized


def is_dedupable_phash(value: str | None) -> bool:
    """True for a real 64-bit hex pHash. The all-zero hash is excluded."""
    normalized = normalize_phash_hex(value)
    return normalized is not None and normalized != ZERO_PHASH


def _as_uuid(value: UUID | str | None) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return UUID(value.strip())
        except ValueError:
            return None
    return None


async def lookup_canonical_s3_uri(conn: Any, phash_hex: str) -> str | None:
    """Return the keeper S3 URI for an exact pHash, or None."""
    normalized = normalize_phash_hex(phash_hex)
    if normalized is None or normalized == ZERO_PHASH:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT canonical_s3_uri
            FROM telegram_image_blobs
            WHERE phash = %s
            """,
            (normalized,),
        )
        found = await cur.fetchone()
    if found is None or not found["canonical_s3_uri"]:
        return None
    return str(found["canonical_s3_uri"])


async def link_blob_message(
    conn: Any,
    *,
    phash_hex: str | None,
    message_id: UUID | str | None,
    message_date: datetime | None,
    source_external_id: str | None,
    peer_type: str | None,
    telegram_message_id: int | None = None,
) -> None:
    """Upsert the exact-phash → message posting-list row."""
    normalized = normalize_phash_hex(phash_hex)
    message_uuid = _as_uuid(message_id)
    if (
        normalized is None
        or normalized == ZERO_PHASH
        or message_uuid is None
        or message_date is None
        or not source_external_id
        or not peer_type
    ):
        return
    await conn.execute(
        """
        INSERT INTO telegram_image_blob_messages (
          phash, message_id, message_date, source_external_id, peer_type, telegram_message_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (phash, message_id) DO UPDATE SET
          message_date = EXCLUDED.message_date,
          source_external_id = EXCLUDED.source_external_id,
          peer_type = EXCLUDED.peer_type,
          telegram_message_id = COALESCE(EXCLUDED.telegram_message_id, telegram_image_blob_messages.telegram_message_id)
        """,
        (
            normalized,
            message_uuid,
            message_date,
            str(source_external_id),
            str(peer_type),
            telegram_message_id,
        ),
    )


async def record_harvested_image_blob(
    conn: Any,
    *,
    phash_hex: str | None,
    s3_uri: str,
    display_s3_uri: str | None = None,
    reused_canonical: bool = False,
    message_id: UUID | str | None = None,
    message_date: datetime | None = None,
    source_external_id: str | None = None,
    peer_type: str | None = None,
    telegram_message_id: int | None = None,
) -> str:
    """Register a harvest against the canonical table and message map.

    Returns the URI that ``harvest.s3_uri`` should store. Existing canonical
    URIs are never replaced. A rare upload race returns the keeper URI and
    leaves the losing object unreferenced.
    """
    normalized = normalize_phash_hex(phash_hex)
    if normalized is None or normalized == ZERO_PHASH or not s3_uri.startswith("s3://"):
        return s3_uri

    if reused_canonical:
        await conn.execute(
            """
            UPDATE telegram_image_blobs
            SET refcount = refcount + 1,
                display_s3_uri = COALESCE(display_s3_uri, %s),
                updated_at = now()
            WHERE phash = %s
            """,
            (display_s3_uri, normalized),
        )
        canonical_uri = s3_uri
    else:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO telegram_image_blobs (phash, canonical_s3_uri, display_s3_uri, refcount)
                VALUES (%s, %s, %s, 1)
                ON CONFLICT (phash) DO UPDATE SET
                  refcount = telegram_image_blobs.refcount + 1,
                  display_s3_uri = COALESCE(telegram_image_blobs.display_s3_uri, EXCLUDED.display_s3_uri),
                  updated_at = now()
                RETURNING canonical_s3_uri
                """,
                (normalized, s3_uri, display_s3_uri),
            )
            stored = await cur.fetchone()
        canonical_uri = str(stored["canonical_s3_uri"]) if stored and stored["canonical_s3_uri"] else s3_uri

    await link_blob_message(
        conn,
        phash_hex=normalized,
        message_id=message_id,
        message_date=message_date,
        source_external_id=source_external_id,
        peer_type=peer_type,
        telegram_message_id=telegram_message_id,
    )
    return canonical_uri


async def mark_blob_embedded(conn: Any, *, phash_hex: str, model: str) -> None:
    normalized = normalize_phash_hex(phash_hex)
    if normalized is None or not model.strip():
        return
    await conn.execute(
        """
        UPDATE telegram_image_blobs
        SET image_embedded = COALESCE(image_embedded, '{}'::jsonb) || jsonb_build_object(%s::text, true),
            updated_at = now()
        WHERE phash = %s
        """,
        (model.strip(), normalized),
    )


async def list_blobs_needing_embed(
    conn: Any,
    *,
    model: str,
    limit: int = 250,
    after_phash: str | None = None,
) -> list[dict[str, Any]]:
    """Return canonical blobs not yet embedded for ``model``, ordered by phash."""
    model_key = model.strip()
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT phash, canonical_s3_uri, display_s3_uri, image_embedded, created_at
            FROM telegram_image_blobs
            WHERE NOT (COALESCE(image_embedded, '{}'::jsonb) ? %s)
              AND (%s::text IS NULL OR phash > %s)
            ORDER BY phash
            LIMIT %s
            """,
            (model_key, after_phash, after_phash, limit),
        )
        rows = await cur.fetchall()
    return [dict(row) for row in rows]


async def list_messages_for_phashes(
    conn: Any,
    phashes: list[str],
) -> list[dict[str, Any]]:
    """Return map rows for the given exact phashes (SigLIP search fan-out)."""
    normalized = [p for p in (normalize_phash_hex(x) for x in phashes) if p and p != ZERO_PHASH]
    if not normalized:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT phash, message_id, message_date, source_external_id, peer_type, telegram_message_id
            FROM telegram_image_blob_messages
            WHERE phash = ANY(%s)
            ORDER BY phash, message_date DESC
            """,
            (normalized,),
        )
        rows = await cur.fetchall()
    return [dict(row) for row in rows]


def s3_key_from_uri(uri: str | None) -> str:
    """Strip ``s3://bucket/`` prefix from a canonical URI (empty if malformed)."""
    if not uri:
        return ""
    text = str(uri).strip()
    if not text.startswith("s3://"):
        return text
    rest = text[5:]
    slash = rest.find("/")
    if slash < 0:
        return ""
    return rest[slash + 1 :]


async def search_near_duplicate_phashes(
    conn: Any,
    query_hex: str,
    *,
    max_hamming: int = 5,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Hamming near-dup search over ``telegram_image_blobs`` (replaces Milvus ANN).

    Returns ``[{phash, distance, s3_key, canonical_s3_uri}]`` ordered best-first
    (lowest Hamming distance), capped at ``limit``.
    """
    normalized = normalize_phash_hex(query_hex)
    if normalized is None or normalized == ZERO_PHASH:
        return []
    top_n = max(1, int(limit))
    max_dist = max(0, int(max_hamming))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              phash,
              canonical_s3_uri,
              bit_count((('x' || phash)::bit(64)) # (('x' || %s)::bit(64))) AS distance
            FROM telegram_image_blobs
            WHERE phash <> %s
              AND bit_count((('x' || phash)::bit(64)) # (('x' || %s)::bit(64))) <= %s
            ORDER BY distance ASC, phash ASC
            LIMIT %s
            """,
            (normalized, ZERO_PHASH, normalized, max_dist, top_n),
        )
        rows = await cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        uri = str(row.get("canonical_s3_uri") or "")
        out.append(
            {
                "phash": str(row["phash"]),
                "distance": float(row["distance"]),
                "s3_key": s3_key_from_uri(uri),
                "canonical_s3_uri": uri,
            }
        )
    return out
