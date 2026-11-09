"""SQLAlchemy Core models: Bluesky actors (DID-keyed profiles)."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
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
from sqlalchemy.dialects.postgresql import JSONB

from db.metadata import metadata

bluesky_actors = Table(
    "bluesky_actors",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("did", Text, nullable=False),
    Column("handle", Text),
    Column("handle_valid", Boolean, nullable=False, server_default=text("true")),
    Column("display_name", Text),
    Column("description", Text),
    Column("pronouns", Text),
    Column("website", Text),
    Column("avatar", JSONB),
    Column("banner", JSONB),
    Column("avatar_url", Text),
    Column("banner_url", Text),
    Column("avatar_s3_uri", Text),
    Column("banner_s3_uri", Text),
    Column("labels", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("pinned_post", JSONB),
    Column("joined_via_starter_pack", JSONB),
    Column("profile_created_at", DateTime(True)),
    Column("followers_count", Integer),
    Column("follows_count", Integer),
    Column("posts_count", Integer),
    Column("indexed_at", DateTime(True)),
    Column("repo_rev", Text),
    Column("pds_host", Text),
    Column("did_method", Text),
    Column("active", Boolean, nullable=False, server_default=text("true")),
    Column("hosting_status", Text),
    Column("hosting_status_observed_at", DateTime(True)),
    Column("unavailable_at", DateTime(True)),
    Column("display_name_search", Text),
    Column("handle_search", Text),
    Column("profile_cid", Text),
    Column("source", Text, nullable=False, server_default=text("'appview'")),
    Column("last_synced_at", DateTime(True)),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="bluesky_actors_pkey"),
    UniqueConstraint("did", name="bluesky_actors_did_key"),
    CheckConstraint(
        "hosting_status IS NULL OR hosting_status = ANY (ARRAY["
        "'takendown'::text, 'suspended'::text, 'deleted'::text, "
        "'deactivated'::text, 'desynchronized'::text, 'throttled'::text])",
        name="bluesky_actors_hosting_status_check",
    ),
    CheckConstraint(
        "source = ANY (ARRAY['appview'::text, 'firehose'::text, 'pds'::text, 'manual'::text])",
        name="bluesky_actors_source_check",
    ),
    CheckConstraint(
        "did ~ '^did:[a-z]+:[a-zA-Z0-9._:%-]*[a-zA-Z0-9._-]$'",
        name="bluesky_actors_did_syntax_check",
    ),
    Index(
        "bluesky_actors_handle_uidx",
        "handle",
        unique=True,
        postgresql_where=text("handle IS NOT NULL AND handle_valid"),
    ),
    Index(
        "bluesky_actors_handle_search_trgm_idx",
        "handle_search",
        postgresql_using="gin",
        postgresql_ops={"handle_search": "gin_trgm_ops"},
    ),
    Index(
        "bluesky_actors_display_name_search_trgm_idx",
        "display_name_search",
        postgresql_using="gin",
        postgresql_ops={"display_name_search": "gin_trgm_ops"},
    ),
    Index("bluesky_actors_active_idx", "active"),
    comment="Bluesky / AT Proto actor: id = our UUID, did = permanent account id",
)
