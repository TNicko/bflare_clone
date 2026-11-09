"""SQLAlchemy Core models: telegram peer field change history."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    PrimaryKeyConstraint,
    Table,
    Text,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.metadata import metadata

telegram_peer_field_history = Table(
    "telegram_peer_field_history",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    Column("field_key", Text, nullable=False),
    Column("old_value", JSONB),
    Column("recorded_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "peer_type = ANY (ARRAY['user'::text, 'channel'::text, 'megagroup'::text, 'chat'::text])",
        name="telegram_peer_field_history_peer_type_check",
    ),
    PrimaryKeyConstraint("id", name="telegram_peer_field_history_pkey"),
)
