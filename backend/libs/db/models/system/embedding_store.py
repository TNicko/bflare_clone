"""SQLAlchemy Core models: embedding-store pipeline state."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)

from db.metadata import metadata


embedding_store_shards = Table(
    "embedding_store_shards",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("model", Text, nullable=False),
    Column("modality", Text, nullable=False),
    Column("shard_key", Text, nullable=False),
    Column("tier", Text, nullable=False),
    Column("month", Text, nullable=False),
    Column("row_count", Integer),
    Column("milvus_upserted_at", DateTime(True)),
    Column("s3vectors_upserted_at", DateTime(True)),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "tier = ANY (ARRAY['micro'::text, 'canonical'::text])",
        name="embedding_store_shards_tier_check",
    ),
    PrimaryKeyConstraint("id", name="embedding_store_shards_pkey"),
    UniqueConstraint(
        "model",
        "modality",
        "shard_key",
        name="embedding_store_shards_identity_key",
    ),
    Index(
        "embedding_store_shards_pending_idx",
        "model",
        "modality",
        postgresql_where=text("milvus_upserted_at IS NULL"),
    ),
    Index(
        "embedding_store_shards_s3vectors_pending_idx",
        "model",
        "modality",
        postgresql_where=text("s3vectors_upserted_at IS NULL"),
    ),
    Index(
        "embedding_store_shards_micro_month_idx",
        "model",
        "modality",
        "month",
        postgresql_where=text("tier = 'micro'"),
    ),
)
