"""SQLAlchemy Core models: canonical image blobs keyed by exact pHash."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.metadata import metadata


telegram_image_blobs = Table(
    "telegram_image_blobs",
    metadata,
    Column("phash", Text, primary_key=True),
    Column("canonical_s3_uri", Text, nullable=False),
    Column("display_s3_uri", Text),
    Column("refcount", Integer, nullable=False),
    Column("image_embedded", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint("phash ~ '^[0-9a-f]{16}$'", name="telegram_image_blobs_phash_hex_chk"),
    CheckConstraint("refcount >= 1", name="telegram_image_blobs_refcount_chk"),
    CheckConstraint("canonical_s3_uri LIKE 's3://%'", name="telegram_image_blobs_canonical_uri_chk"),
)


telegram_image_blob_messages = Table(
    "telegram_image_blob_messages",
    metadata,
    Column("phash", Text, ForeignKey("telegram_image_blobs.phash", ondelete="CASCADE"), nullable=False),
    Column("message_id", UUID, nullable=False),
    Column("message_date", DateTime(True), nullable=False),
    Column("source_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    Column("telegram_message_id", BigInteger),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("phash", "message_id", name="telegram_image_blob_messages_pkey"),
    CheckConstraint("phash ~ '^[0-9a-f]{16}$'", name="telegram_image_blob_messages_phash_hex_chk"),
    Index("telegram_image_blob_messages_message_idx", "message_id", "message_date"),
    Index("telegram_image_blob_messages_peer_idx", "source_external_id", "peer_type"),
)
