"""Postgres catalog for S3 shards loaded into Milvus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from embstore.layout import is_micro_shard_key, month_from_shard_key


@dataclass(frozen=True, slots=True)
class MicroShardRef:
    shard_key: str
    month: str
    row_count: int


def list_completed_shard_keys(
    conn: Any,
    *,
    model: str,
    modality: str,
) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shard_key
            FROM embedding_store_shards
            WHERE model = %s
              AND modality = %s
              AND milvus_upserted_at IS NOT NULL
            """,
            (model, modality),
        )
        rows = cur.fetchall()
    return {str(row["shard_key"]) for row in rows}


def list_pending_shard_keys(
    conn: Any,
    *,
    model: str,
    modality: str,
) -> list[str]:
    """Shard keys not yet upserted to Milvus.

    Canonical parts first (post-compact ~100k-row objects), then micros, then by month/key.
    Backfill should drive off this list so it does not re-walk every already-done micro on S3.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shard_key
            FROM embedding_store_shards
            WHERE model = %s
              AND modality = %s
              AND milvus_upserted_at IS NULL
            ORDER BY
              CASE WHEN tier = 'canonical' THEN 0 ELSE 1 END,
              month ASC,
              shard_key ASC
            """,
            (model, modality),
        )
        rows = cur.fetchall()
    return [str(row["shard_key"]) for row in rows]


def list_s3vectors_completed_shard_keys(
    conn: Any,
    *,
    model: str,
    modality: str,
) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shard_key
            FROM embedding_store_shards
            WHERE model = %s
              AND modality = %s
              AND s3vectors_upserted_at IS NOT NULL
            """,
            (model, modality),
        )
        rows = cur.fetchall()
    return {str(row["shard_key"]) for row in rows}


def list_s3vectors_pending_shard_keys(
    conn: Any,
    *,
    model: str,
    modality: str,
    month: str | None = None,
) -> list[str]:
    """Shard keys not yet upserted to S3 Vectors (canonical first, then micro)."""
    sql = """
        SELECT shard_key
        FROM embedding_store_shards
        WHERE model = %s
          AND modality = %s
          AND s3vectors_upserted_at IS NULL
    """
    params: list[Any] = [model, modality]
    if month is not None:
        sql += " AND month = %s"
        params.append(month)
    sql += """
        ORDER BY
          CASE WHEN tier = 'canonical' THEN 0 ELSE 1 END,
          month ASC,
          shard_key ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [str(row["shard_key"]) for row in rows]


def list_catalog_shard_keys(
    conn: Any,
    *,
    model: str,
    modality: str,
    tier: str | None = None,
) -> set[str]:
    """All catalogued shard keys (any milvus status), optionally filtered by tier."""
    sql = """
        SELECT shard_key
        FROM embedding_store_shards
        WHERE model = %s
          AND modality = %s
    """
    params: list[Any] = [model, modality]
    if tier is not None:
        sql += " AND tier = %s"
        params.append(tier)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return {str(row["shard_key"]) for row in rows}


def mark_shard_upserted(
    conn: Any,
    *,
    model: str,
    modality: str,
    shard_key: str,
    row_count: int,
) -> None:
    month = month_from_shard_key(shard_key)
    if month is None:
        raise ValueError(f"cannot derive dt=YYYY-MM from shard key {shard_key!r}")
    tier = "micro" if is_micro_shard_key(shard_key) else "canonical"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO embedding_store_shards AS s (
              model,
              modality,
              shard_key,
              tier,
              month,
              row_count,
              milvus_upserted_at,
              updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (model, modality, shard_key)
            DO UPDATE SET
              tier = EXCLUDED.tier,
              month = EXCLUDED.month,
              row_count = EXCLUDED.row_count,
              milvus_upserted_at = EXCLUDED.milvus_upserted_at,
              updated_at = now()
            """,
            (model, modality, shard_key, tier, month, row_count),
        )


def mark_shard_s3vectors_upserted(
    conn: Any,
    *,
    model: str,
    modality: str,
    shard_key: str,
    row_count: int,
) -> None:
    """Mark a catalog shard as present in S3 Vectors (does not touch milvus_upserted_at)."""
    month = month_from_shard_key(shard_key)
    if month is None:
        raise ValueError(f"cannot derive dt=YYYY-MM from shard key {shard_key!r}")
    tier = "micro" if is_micro_shard_key(shard_key) else "canonical"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO embedding_store_shards AS s (
              model,
              modality,
              shard_key,
              tier,
              month,
              row_count,
              s3vectors_upserted_at,
              updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (model, modality, shard_key)
            DO UPDATE SET
              tier = EXCLUDED.tier,
              month = EXCLUDED.month,
              row_count = EXCLUDED.row_count,
              s3vectors_upserted_at = now(),
              updated_at = now()
            """,
            (model, modality, shard_key, tier, month, row_count),
        )


def register_micro_shards(
    conn: Any,
    *,
    model: str,
    modality: str,
    shards: list[MicroShardRef] | list[tuple[str, str, int]],
) -> int:
    """Insert micro shard catalog rows (pending Milvus). Returns rows inserted/updated."""
    if not shards:
        return 0
    refs: list[MicroShardRef] = []
    for item in shards:
        if isinstance(item, MicroShardRef):
            refs.append(item)
        else:
            key, month, row_count = item
            refs.append(MicroShardRef(shard_key=key, month=month, row_count=row_count))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO embedding_store_shards AS s (
              model,
              modality,
              shard_key,
              tier,
              month,
              row_count,
              milvus_upserted_at,
              updated_at
            )
            VALUES (%s, %s, %s, 'micro', %s, %s, NULL, now())
            ON CONFLICT (model, modality, shard_key)
            DO UPDATE SET
              tier = 'micro',
              month = EXCLUDED.month,
              row_count = EXCLUDED.row_count,
              updated_at = now()
            """,
            [(model, modality, ref.shard_key, ref.month, ref.row_count) for ref in refs],
        )
        return len(refs)


def list_micro_months_ready(
    conn: Any,
    *,
    model: str,
    modality: str,
    min_rows: int,
    month: str | None = None,
) -> list[tuple[str, int]]:
    """Months whose micro row_count sum is >= ``min_rows``. Returns ``(month, total_rows)``."""
    sql = """
        SELECT month, COALESCE(SUM(row_count), 0)::bigint AS total_rows
        FROM embedding_store_shards
        WHERE model = %s
          AND modality = %s
          AND tier = 'micro'
    """
    params: list[Any] = [model, modality]
    if month is not None:
        sql += " AND month = %s"
        params.append(month)
    sql += """
        GROUP BY month
        HAVING COALESCE(SUM(row_count), 0) >= %s
        ORDER BY month
    """
    params.append(min_rows)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [(str(row["month"]), int(row["total_rows"])) for row in rows]


def list_micro_shards_for_month(
    conn: Any,
    *,
    model: str,
    modality: str,
    month: str,
) -> list[MicroShardRef]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shard_key, month, COALESCE(row_count, 0) AS row_count
            FROM embedding_store_shards
            WHERE model = %s
              AND modality = %s
              AND tier = 'micro'
              AND month = %s
            ORDER BY created_at ASC, shard_key ASC
            """,
            (model, modality, month),
        )
        rows = cur.fetchall()
    return [
        MicroShardRef(
            shard_key=str(row["shard_key"]),
            month=str(row["month"]),
            row_count=int(row["row_count"]),
        )
        for row in rows
    ]


def reset_catalog(
    conn: Any,
    *,
    model: str,
    modality: str,
) -> int:
    """Clear ``milvus_upserted_at`` on catalog shards (keep rows + row_count)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE embedding_store_shards
            SET milvus_upserted_at = NULL,
                updated_at = now()
            WHERE model = %s
              AND modality = %s
              AND milvus_upserted_at IS NOT NULL
            """,
            (model, modality),
        )
        return int(cur.rowcount)


def reset_s3vectors_catalog(
    conn: Any,
    *,
    model: str,
    modality: str,
) -> int:
    """Clear ``s3vectors_upserted_at`` so the next S3 Vectors backfill re-uploads every shard."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE embedding_store_shards
            SET s3vectors_upserted_at = NULL,
                updated_at = now()
            WHERE model = %s
              AND modality = %s
              AND s3vectors_upserted_at IS NOT NULL
            """,
            (model, modality),
        )
        return int(cur.rowcount)


def replace_compacted_shards(
    conn: Any,
    *,
    model: str,
    modality: str,
    micro_keys: list[str],
    canonical_key: str,
    month: str,
    row_count: int,
) -> None:
    """Replace compacted micro catalog rows with one pending canonical row.

    The canonical remains pending until a subsequent Milvus backfill successfully
    upserts it.
    """
    with conn.cursor() as cur:
        if micro_keys:
            cur.execute(
                """
                DELETE FROM embedding_store_shards
                WHERE model = %s
                  AND modality = %s
                  AND shard_key = ANY(%s)
                """,
                (model, modality, micro_keys),
            )
        cur.execute(
            """
            INSERT INTO embedding_store_shards AS s (
              model,
              modality,
              shard_key,
              tier,
              month,
              row_count,
              milvus_upserted_at,
              s3vectors_upserted_at,
              updated_at
            )
            VALUES (%s, %s, %s, 'canonical', %s, %s, NULL, NULL, now())
            ON CONFLICT (model, modality, shard_key)
            DO UPDATE SET
              tier = 'canonical',
              month = EXCLUDED.month,
              row_count = EXCLUDED.row_count,
              milvus_upserted_at = NULL,
              s3vectors_upserted_at = NULL,
              updated_at = now()
            """,
            (model, modality, canonical_key, month, row_count),
        )


@dataclass(frozen=True, slots=True)
class EmbeddingStoreCatalogStats:
    modality: str
    in_milvus_rows: int
    in_s3vectors_rows: int = 0


def embedding_store_catalog_stats(
    conn: Any,
    *,
    modality: str = "text",
) -> EmbeddingStoreCatalogStats:
    """Rows marked upserted to S3 Vectors for one modality."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COALESCE(
                SUM(row_count) FILTER (WHERE milvus_upserted_at IS NOT NULL),
                0
              )::bigint AS in_milvus_rows,
              COALESCE(
                SUM(row_count) FILTER (WHERE s3vectors_upserted_at IS NOT NULL),
                0
              )::bigint AS in_s3vectors_rows
            FROM embedding_store_shards
            WHERE modality = %s
            """,
            (modality,),
        )
        row = cur.fetchone()
    if not row:
        return EmbeddingStoreCatalogStats(modality=modality, in_milvus_rows=0, in_s3vectors_rows=0)
    return EmbeddingStoreCatalogStats(
        modality=modality,
        in_milvus_rows=int(row["in_milvus_rows"]),
        in_s3vectors_rows=int(row["in_s3vectors_rows"]),
    )
