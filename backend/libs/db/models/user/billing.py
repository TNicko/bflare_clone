"""SQLAlchemy Core models: plans and manual billing grants.

``plans`` holds included units and feature flags. ``billing_overrides`` is the
manual grant history. Effective plan is computed, not stored on ``profiles``.
"""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.metadata import metadata

plans = Table(
    "plans",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("included_units", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("features", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("past_due_grace_days", Integer, nullable=False, server_default=text("7")),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
)

billing_overrides = Table(
    "billing_overrides",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="billing_overrides_user_id_fkey"),
        nullable=False,
    ),
    Column(
        "plan_id",
        Text,
        ForeignKey("plans.id", name="billing_overrides_plan_id_fkey"),
        nullable=False,
    ),
    Column("kind", Text, nullable=False),
    Column(
        "starts_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("expires_at", DateTime(timezone=True)),
    Column("granted_by", Text, nullable=False),
    Column("reason", Text),
    Column("revoked_at", DateTime(timezone=True)),
    Column("revoked_by", Text),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint("kind IN ('comp', 'trial')", name="billing_overrides_kind_chk"),
    CheckConstraint(
        "(kind = 'comp' AND expires_at IS NULL) OR (kind = 'trial' AND expires_at IS NOT NULL)",
        name="billing_overrides_expiry_chk",
    ),
    Index(
        "billing_overrides_one_current_idx",
        "user_id",
        unique=True,
        postgresql_where=text("revoked_at IS NULL"),
    ),
    Index("billing_overrides_user_created_idx", "user_id", "created_at"),
)

billing_accounts = Table(
    "billing_accounts",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="billing_accounts_user_id_fkey"),
        nullable=False,
        unique=True,
    ),
    Column("stripe_customer_id", Text, unique=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
)

subscriptions = Table(
    "subscriptions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "billing_account_id",
        UUID(as_uuid=True),
        ForeignKey("billing_accounts.id", ondelete="CASCADE", name="subscriptions_billing_account_id_fkey"),
        nullable=False,
    ),
    Column("stripe_subscription_id", Text, nullable=False, unique=True),
    Column("stripe_price_id", Text),
    Column("status", Text, nullable=False),
    Column(
        "plan_id",
        Text,
        ForeignKey("plans.id", name="subscriptions_plan_id_fkey"),
        nullable=False,
    ),
    Column("current_period_start", DateTime(timezone=True)),
    Column("current_period_end", DateTime(timezone=True)),
    Column("cancel_at_period_end", Boolean, nullable=False, server_default=text("false")),
    Column("canceled_at", DateTime(timezone=True)),
    Column("ended_at", DateTime(timezone=True)),
    Column("raw", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint(
        "status IN ('trialing','active','past_due','canceled','unpaid','incomplete','incomplete_expired')",
        name="subscriptions_status_chk",
    ),
    Index(
        "subscriptions_one_current_per_account_idx",
        "billing_account_id",
        unique=True,
        postgresql_where=text("ended_at IS NULL"),
    ),
    Index("subscriptions_account_updated_idx", "billing_account_id", "updated_at"),
)

stripe_events = Table(
    "stripe_events",
    metadata,
    Column("id", Text, primary_key=True),
    Column("type", Text, nullable=False),
    Column("payload", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "received_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("processed_at", DateTime(timezone=True)),
    Column("error", Text),
)
