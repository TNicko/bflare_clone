"""API keys and the programmatic credit ledger."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

from db.metadata import metadata

api_keys = Table(
    "api_keys",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="api_keys_user_id_fkey"),
        nullable=False,
    ),
    Column("name", Text, nullable=False),
    Column("key_prefix", Text, nullable=False),
    Column("key_last4", Text, nullable=False),
    Column("key_hash", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("last_used_at", DateTime(timezone=True)),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint("char_length(name) BETWEEN 1 AND 64", name="api_keys_name_len_chk"),
    Index("api_keys_key_hash_key", "key_hash", unique=True),
    Index("api_keys_user_active_idx", "user_id", postgresql_where=text("revoked_at IS NULL")),
)

api_credit_accounts = Table(
    "api_credit_accounts",
    metadata,
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="api_credit_accounts_user_id_fkey"),
        primary_key=True,
    ),
    Column("prepaid_credits", Integer, nullable=False, server_default=text("0")),
    Column("allowance_used", Integer, nullable=False, server_default=text("0")),
    Column("allowance_period_start", Date, nullable=False),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint("prepaid_credits >= 0", name="api_credit_accounts_prepaid_chk"),
    CheckConstraint("allowance_used >= 0", name="api_credit_accounts_allowance_chk"),
)

api_credit_ledger = Table(
    "api_credit_ledger",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="api_credit_ledger_user_id_fkey"),
        nullable=False,
    ),
    Column("kind", Text, nullable=False),
    Column("credits", Integer, nullable=False),
    Column("allowance_credits", Integer, nullable=False, server_default=text("0")),
    Column("prepaid_credits", Integer, nullable=False, server_default=text("0")),
    Column("period_start", Date),
    Column("request_id", Text),
    Column("stripe_checkout_session_id", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    CheckConstraint("kind IN ('spend', 'refund', 'topup')", name="api_credit_ledger_kind_chk"),
    CheckConstraint("credits >= 0", name="api_credit_ledger_credits_chk"),
    Index(
        "api_credit_ledger_request_spend_idx",
        "user_id",
        "request_id",
        unique=True,
        postgresql_where=text("kind = 'spend' AND request_id IS NOT NULL"),
    ),
    Index(
        "api_credit_ledger_stripe_session_idx",
        "stripe_checkout_session_id",
        unique=True,
        postgresql_where=text("stripe_checkout_session_id IS NOT NULL"),
    ),
)

api_usage_notices = Table(
    "api_usage_notices",
    metadata,
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="api_usage_notices_user_id_fkey"),
        primary_key=True,
    ),
    Column("period_start", Date, primary_key=True),
    Column("kind", Text, primary_key=True),
    Column(
        "sent_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint(
        "kind IN ('allowance_80', 'allowance_exhausted', 'prepaid_empty')",
        name="api_usage_notices_kind_chk",
    ),
)
