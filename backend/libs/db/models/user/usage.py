"""SQLAlchemy Core models: per-user usage ledger.

``usage_events`` is the audit trail (one row per product action).
``usage_periods`` is the calendar-month rollup quota will read later.
Step 1 does not enforce limits; it only records.
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.metadata import metadata

usage_events = Table(
    "usage_events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="usage_events_user_id_fkey"),
        nullable=False,
    ),
    Column("request_id", Text, nullable=False),
    Column(
        "occurred_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("completed_at", DateTime(timezone=True)),
    Column("meter", Text, nullable=False),
    Column("route", Text, nullable=False),
    Column("method", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("http_status", Integer),
    Column("query_text", Text),
    Column("query_hash", Text),
    Column("params", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("result_count", Integer),
    Column("duration_ms", Integer),
    Column("units", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("error_code", Text),
    PrimaryKeyConstraint("id", name="usage_events_pkey"),
    CheckConstraint(
        "status IN ('started', 'succeeded', 'failed', 'rejected')",
        name="usage_events_status_chk",
    ),
    Index(
        "usage_events_user_request_id_uidx",
        "user_id",
        "request_id",
        unique=True,
    ),
    Index("usage_events_user_occurred_idx", "user_id", text("occurred_at DESC")),
    Index("usage_events_user_meter_occurred_idx", "user_id", "meter", text("occurred_at DESC")),
    Index("usage_events_query_hash_idx", "user_id", "query_hash"),
    Index(
        "usage_events_started_idx",
        "occurred_at",
        postgresql_where=text("status = 'started'"),
    ),
)

usage_periods = Table(
    "usage_periods",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="usage_periods_user_id_fkey"),
        nullable=False,
    ),
    Column("meter", Text, nullable=False),
    Column("period_start", DateTime(timezone=True), nullable=False),
    Column("period_end", DateTime(timezone=True), nullable=False),
    Column("event_count", Integer, nullable=False, server_default=text("0")),
    Column("units", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    PrimaryKeyConstraint("id", name="usage_periods_pkey"),
    UniqueConstraint("user_id", "meter", "period_start", name="usage_periods_user_meter_period_key"),
    Index("usage_periods_user_period_idx", "user_id", "period_start"),
)
