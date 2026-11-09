"""SQLAlchemy Core models: telegram messages."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
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

telegram_messages = Table(
    "telegram_messages",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("message_id", BigInteger, nullable=False),
    Column("source_external_id", Text, nullable=False),
    Column("peer_type", String(50), nullable=False),
    Column("message_type", String(50)),
    Column("content", Text),
    Column("content_hash", String(64)),
    Column("out", Boolean, server_default=text("false")),
    Column("date", DateTime(True), primary_key=True),
    Column("edit_date", DateTime(True)),
    Column("created_at", DateTime(True), server_default=text("now()")),
    Column("updated_at", DateTime(True), server_default=text("now()")),
    Column("reactions", JSONB, server_default=text("'[]'::jsonb")),
    Column("views", Integer, server_default=text("0")),
    Column("media", JSONB, server_default=text("'{}'::jsonb")),
    Column("pinned", Boolean, server_default=text("false")),
    Column("deleted", Boolean, server_default=text("false")),
    Column("silent", Boolean, server_default=text("false")),
    Column("mentioned", Boolean, server_default=text("false")),
    Column("has_url", Boolean, server_default=text("false"), nullable=False),
    Column("has_mention", Boolean, server_default=text("false"), nullable=False),
    Column("grouped_id", BigInteger),
    Column("action_type", String(100)),
    Column("action_data", JSONB),
    Column("metadata", JSONB),
    Column("fwd_from", JSONB),
    Column("reply_to", JSONB),
    Column("from_external_id", Text),
    Column("harvester_external_id", Text),
    # Phase 3a cold Parquet pointers (full row stays in PG until strip).
    Column("archive_key", Text),
    Column("archive_row", Integer),
    Column("archived_at", DateTime(True)),
    PrimaryKeyConstraint("id", "date", name="telegram_messages_pkey"),
    UniqueConstraint(
        "source_external_id",
        "message_id",
        "date",
        name="telegram_messages_unique_message_source",
    ),
    Index("telegram_messages_date_idx", "date"),
    Index("telegram_messages_harvester_date_idx", "harvester_external_id", "date"),
    Index("telegram_messages_source_date_idx", "source_external_id", "date"),
    Index(
        "telegram_messages_from_source_date_idx",
        "from_external_id",
        "source_external_id",
        "date",
    ),
)

telegram_message_edits = Table(
    "telegram_message_edits",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("message_id", BigInteger, nullable=False),
    Column("source_external_id", Text, nullable=False),
    Column("content", Text),
    Column("edit_date", DateTime(True), nullable=False),
    Column("reactions", JSONB, server_default=text("'[]'::jsonb")),
    Column("media", JSONB, server_default=text("'{}'::jsonb")),
    Column("metadata", JSONB),
    Column("created_at", DateTime(True), server_default=text("now()")),
    Column("harvester_external_id", Text),
    PrimaryKeyConstraint("id", name="telegram_message_edits_pkey"),
    comment="Edit history for harvested messages",
)

telegram_message_reactors = Table(
    "telegram_message_reactors",
    metadata,
    Column("message_id", Uuid, nullable=False),
    Column("message_date", DateTime(True), nullable=False),
    Column("reactor_external_id", Text, nullable=False),
    Column("peer_type", String(50), nullable=False),
    Column("reaction_key", Text, nullable=False),
    Column("reaction", JSONB, nullable=False),
    Column("reacted_at", DateTime(True)),
    Column("source_external_id", Text),
    PrimaryKeyConstraint(
        "message_id",
        "message_date",
        "reactor_external_id",
        "reaction_key",
        name="telegram_message_reactors_pkey",
    ),
    ForeignKeyConstraint(
        ["message_id", "message_date"],
        ["telegram_messages.id", "telegram_messages.date"],
        name="telegram_message_reactors_message_fkey",
        ondelete="CASCADE",
    ),
    Index("telegram_message_reactors_message_idx", "message_id", "message_date"),
    Index(
        "telegram_message_reactors_reactor_source_idx",
        "reactor_external_id",
        "source_external_id",
        text("reacted_at DESC NULLS LAST"),
    ),
    comment="Full reactor list for messages where can_see_list is true",
)

user_peer_message_stats = Table(
    "user_peer_message_stats",
    metadata,
    Column("user_external_id", Text, nullable=False),
    Column("peer_external_id", Text, nullable=False),
    Column("message_count", BigInteger, nullable=False, server_default=text("0")),
    Column("last_message_at", DateTime(True)),
    PrimaryKeyConstraint(
        "user_external_id",
        "peer_external_id",
        name="user_peer_message_stats_pkey",
    ),
    comment="Recorded messages per user and community, including messages later marked deleted.",
)
