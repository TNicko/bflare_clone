"""SQLAlchemy Core models: Bluesky App View search query runs (dedup)."""

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
    Uuid,
    text,
)

from db.metadata import metadata

bluesky_search_query_runs = Table(
    "bluesky_search_query_runs",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("query_text", Text, nullable=False),
    Column("status", Text, nullable=False, server_default=text("'running'::text")),
    Column("started_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("finished_at", DateTime(True)),
    Column("result_count", Integer),
    Column("actors_seen", Integer),
    Column("actors_persisted", Integer),
    Column("actors_skipped", Integer),
    Column("next_cursor", Text),
    Column("pages_done", Integer, nullable=False, server_default=text("0")),
    Column("error", Text),
    Column("source", Text, nullable=False, server_default=text("'appview'")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="bluesky_search_query_runs_pkey"),
    CheckConstraint(
        "status = ANY (ARRAY['running'::text, 'succeeded'::text, 'failed'::text])",
        name="bluesky_search_query_runs_status_check",
    ),
    CheckConstraint(
        "source = ANY (ARRAY['appview'::text, 'manual'::text])",
        name="bluesky_search_query_runs_source_check",
    ),
    Index("bluesky_search_query_runs_query_text_idx", "query_text"),
    Index("bluesky_search_query_runs_status_idx", "status"),
    comment="App View actor search runs — skip re-search when succeeded / recently running",
)
