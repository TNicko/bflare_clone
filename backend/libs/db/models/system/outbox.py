"""SQLAlchemy Core models: system/outbox."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Identity,
    Index,
    PrimaryKeyConstraint,
    Table,
    Text,
    Uuid,
    text,
)

from db.metadata import metadata

search_outbox = Table(
    "search_outbox",
    metadata,
    Column(
        "id",
        BigInteger,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1),
        primary_key=True,
        autoincrement=True,
    ),
    Column("entity", Text, nullable=False, server_default=text("'message'::text")),
    Column("entity_id", Uuid, nullable=False),
    Column("entity_date", DateTime(True), nullable=False),
    Column("op", Text, nullable=False),
    Column("enqueued_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint("op = ANY (ARRAY['upsert'::text, 'delete'::text])", name="search_outbox_op_check"),
    PrimaryKeyConstraint("id", name="search_outbox_pkey"),
    Index("search_outbox_enqueued_at_idx", "enqueued_at"),
    Index("search_outbox_entity_id_idx", "entity", "id"),
)
