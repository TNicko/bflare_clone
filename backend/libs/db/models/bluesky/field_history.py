"""SQLAlchemy Core models: Bluesky actor field change history."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, Index, PrimaryKeyConstraint, Table, Text, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB

from db.metadata import metadata

bluesky_actor_field_history = Table(
    "bluesky_actor_field_history",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("did", Text, nullable=False),
    Column("field_key", Text, nullable=False),
    Column("old_value", JSONB),
    Column("recorded_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="bluesky_actor_field_history_pkey"),
    Index(
        "bluesky_actor_field_history_did_field_recorded_idx",
        "did",
        "field_key",
        "recorded_at",
    ),
)
