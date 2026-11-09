"""SQLAlchemy Core models: peers/categories."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.metadata import metadata

categories = Table(
    "categories",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("slug", Text, nullable=False),
    Column("label", Text, nullable=False),
    Column("parent_slug", Text),
    Column("tier", Integer, nullable=False, server_default=text("1")),
    ForeignKeyConstraint(["parent_slug"], ["categories.slug"], name="categories_parent_slug_fkey"),
    PrimaryKeyConstraint("id", name="categories_pkey"),
    UniqueConstraint("slug", name="categories_slug_key"),
)

peer_categories = Table(
    "peer_categories",
    metadata,
    Column("community_id", Uuid, primary_key=True),
    Column("peer_type", String(50), primary_key=True),
    Column("category_id", Integer, primary_key=True),
    Column("confidence", Float),
    Column("labeled_at", DateTime(timezone=True), server_default=text("now()")),
    ForeignKeyConstraint(
        ["category_id"], ["categories.id"], ondelete="CASCADE", name="peer_categories_category_id_fkey"
    ),
    PrimaryKeyConstraint("community_id", "peer_type", "category_id", name="peer_categories_pkey"),
)

# Latest Jev topic+threat prediction per community (including abstains).
peer_jev_label_runs = Table(
    "peer_jev_label_runs",
    metadata,
    Column("community_id", Uuid, primary_key=True),
    Column("peer_type", String(50), primary_key=True),
    Column("external_id", Text, nullable=False),
    Column("taxonomy_version", Integer),
    Column("model", Text),
    Column("topic_choice", Text),
    Column("topic_confidence", Float),
    Column("topic_probabilities", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("threat_choice", Text),
    Column("threat_confidence", Float),
    Column("threat_probabilities", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("written_slugs", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("topic_written", Boolean, nullable=False, server_default=text("false")),
    Column("threat_written", Boolean, nullable=False, server_default=text("false")),
    Column("input_tokens", Integer, nullable=False, server_default=text("0")),
    Column("output_tokens", Integer, nullable=False, server_default=text("0")),
    Column("dry_run", Boolean, nullable=False, server_default=text("false")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("community_id", "peer_type", name="peer_jev_label_runs_pkey"),
    Index("peer_jev_label_runs_external_id_idx", "external_id"),
)
