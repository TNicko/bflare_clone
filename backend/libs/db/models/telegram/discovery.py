"""SQLAlchemy Core models: telegram/discovery."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
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

telegram_banned_phones = Table(
    "telegram_banned_phones",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("phone", Text, nullable=False),
    Column("reason", Text),
    Column("metadata", JSONB),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint("phone ~ '^\\d{5,15}$'::text", name="telegram_banned_phones_phone_digits_check"),
    PrimaryKeyConstraint("id", name="telegram_banned_phones_pkey"),
    UniqueConstraint("phone", name="telegram_banned_phones_phone_key"),
)

telegram_channel_recommendation_runs = Table(
    "telegram_channel_recommendation_runs",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("source_external_id", Text, nullable=False),
    Column("harvester_id", Uuid, nullable=False),
    Column("status", Text, nullable=False, server_default=text("'running'::text")),
    Column("started_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("finished_at", DateTime(True)),
    Column("similar_channels", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("result_count", Integer),
    Column("peers_materialized", Integer),
    Column("peers_skipped", Integer),
    Column("error", Text),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_channel_recommendation_runs_pkey"),
    Index("telegram_channel_recommendation_runs_source_external_id_idx", "source_external_id"),
    Index(
        "telegram_channel_recommendation_runs_succeeded_result_count_idx",
        text("result_count DESC NULLS LAST"),
        text("finished_at DESC NULLS LAST"),
        postgresql_where=text("status = 'succeeded'"),
    ),
)

telegram_forward_edges = Table(
    "telegram_forward_edges",
    metadata,
    Column("from_external_id", Text, nullable=False),
    Column("from_peer_type", Text, nullable=False),
    Column("to_external_id", Text, nullable=False),
    Column("to_peer_type", Text, nullable=False),
    Column("forward_count", Integer, nullable=False, server_default=text("0")),
    Column("last_from_name", Text),
    Column("first_seen_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("last_seen_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "from_peer_type = ANY (ARRAY['channel'::text, 'megagroup'::text, 'chat'::text])",
        name="telegram_forward_edges_from_peer_type_check",
    ),
    CheckConstraint(
        "to_peer_type = ANY (ARRAY['channel'::text, 'megagroup'::text, 'chat'::text])",
        name="telegram_forward_edges_to_peer_type_check",
    ),
    CheckConstraint("forward_count >= 0", name="telegram_forward_edges_forward_count_nonneg_check"),
    CheckConstraint(
        "from_external_id <> to_external_id",
        name="telegram_forward_edges_no_self_loop_check",
    ),
    PrimaryKeyConstraint(
        "from_external_id",
        "to_external_id",
        name="telegram_forward_edges_pkey",
    ),
    Index("telegram_forward_edges_from_count_idx", "from_external_id", text("forward_count DESC")),
    Index("telegram_forward_edges_to_count_idx", "to_external_id", text("forward_count DESC")),
)

telegram_discovery_query_runs = Table(
    "telegram_discovery_query_runs",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("query_text", Text, nullable=False),
    Column("provider", Text, nullable=False),
    Column("notes", Text),
    Column("status", Text, nullable=False, server_default=text("'pending'::text")),
    Column("started_at", DateTime(True)),
    Column("finished_at", DateTime(True)),
    Column("result_count", Integer),
    Column("error", Text),
    Column("raw_metrics", JSONB, server_default=text("'{}'::jsonb")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "status = ANY (ARRAY['pending'::text, 'running'::text, 'succeeded'::text, 'failed'::text])",
        name="telegram_discovery_query_runs_status_check",
    ),
    PrimaryKeyConstraint("id", name="telegram_discovery_query_runs_pkey"),
    UniqueConstraint("query_text", "provider", name="telegram_discovery_query_runs_query_text_provider_key"),
    Index("idx_discovery_runs_provider_status", "provider", "status"),
    Index("idx_discovery_runs_status", "status"),
)

telegram_invite_discoveries = Table(
    "telegram_invite_discoveries",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("invite_hash", Text, nullable=False),
    Column("normalized_url", Text, nullable=False),
    Column("source_urls", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("source_queries", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("providers", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("status", Text, nullable=False, server_default=text("'candidate'::text")),
    Column("peer_type", Text),
    Column("external_id", Text),
    Column("access_hash", BigInteger),
    Column("title", Text),
    Column("raw", JSONB),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("first_seen_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("last_seen_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("resolved_at", DateTime(True)),
    PrimaryKeyConstraint("id", name="telegram_invite_discoveries_pkey"),
    UniqueConstraint("invite_hash", name="telegram_invite_discoveries_invite_hash_key"),
)

telegram_search_ingest = Table(
    "telegram_search_ingest",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("query_text", Text, nullable=False),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("claimed_at", DateTime(True)),
    CheckConstraint(
        "query_text = btrim(query_text) AND char_length(query_text) > 0",
        name="telegram_search_ingest_query_text_check",
    ),
    PrimaryKeyConstraint("id", name="telegram_search_ingest_pkey"),
    UniqueConstraint("query_text", name="telegram_search_ingest_query_text_key"),
    Index("telegram_search_ingest_created_at_idx", "created_at", "id"),
    comment=(
        "Queue of contacts.search queries. telegram_search claims a row, skips "
        "queries that already succeeded, searches the rest, then deletes the row."
    ),
)

telegram_search_query_runs = Table(
    "telegram_search_query_runs",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("query_text", Text, nullable=False),
    Column("harvester_id", Uuid, nullable=False),
    Column("status", Text, nullable=False, server_default=text("'running'::text")),
    Column("started_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("finished_at", DateTime(True)),
    Column("result_count", Integer),
    Column("peers_seen", Integer),
    Column("peers_persisted", Integer),
    Column("peers_skipped", Integer),
    Column("error", Text),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("peers_found", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    PrimaryKeyConstraint("id", name="telegram_search_query_runs_pkey"),
    Index("telegram_search_query_runs_query_text_idx", "query_text"),
)

telegram_username_discoveries = Table(
    "telegram_username_discoveries",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("username", Text, nullable=False),
    Column("normalized_url", Text, nullable=False),
    Column("source_urls", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("source_queries", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("providers", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("status", Text, nullable=False, server_default=text("'candidate'::text")),
    Column("peer_type", Text),
    Column("external_id", Text),
    Column("access_hash", BigInteger),
    Column("title", Text),
    Column("raw", JSONB),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("first_seen_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("last_seen_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("resolved_at", DateTime(True)),
    PrimaryKeyConstraint("id", name="telegram_username_discoveries_pkey"),
    UniqueConstraint("username", name="telegram_username_discoveries_username_key"),
    Index(
        "telegram_username_discoveries_candidate_id_idx",
        "id",
        postgresql_where=text("status = 'candidate'::text"),
    ),
)
