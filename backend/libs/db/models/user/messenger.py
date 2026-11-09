"""SQLAlchemy Core models: user/messenger."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

from db.metadata import metadata

user_messenger_accounts = Table(
    "user_messenger_accounts",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "user_id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="user_messenger_accounts_user_id_fkey"),
        nullable=False,
    ),
    Column("messenger", Text, nullable=False, server_default=text("'telegram'")),
    Column("external_id", Text, nullable=False),
    Column("label", Text),
    Column("phone_number", Text),
    Column("username", Text),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
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
    UniqueConstraint(
        "user_id",
        "messenger",
        "external_id",
        name="user_messenger_accounts_user_messenger_external_unique",
    ),
    Index("user_messenger_accounts_user_id_idx", "user_id"),
)

user_messenger_sessions = Table(
    "user_messenger_sessions",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "account_id",
        UUID(as_uuid=True),
        ForeignKey("user_messenger_accounts.id", ondelete="CASCADE", name="user_messenger_sessions_account_id_fkey"),
        nullable=False,
    ),
    Column("label", Text, nullable=False),
    Column("session_type", Text, nullable=False, server_default=text("'user'")),
    Column("auth_state", Text, nullable=False, server_default=text("'healthy'")),
    Column("status", Text, nullable=False, server_default=text("'inactive'")),
    Column("session_string", Text),
    Column("auth_key", LargeBinary),
    Column("dc_id", SmallInteger),
    Column("server_address", Text),
    Column("port", Integer),
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
    UniqueConstraint("account_id", "label", name="user_messenger_sessions_account_label_unique"),
    CheckConstraint(
        "session_type IN ('user')",
        name="user_messenger_sessions_session_type_check",
    ),
    CheckConstraint(
        "auth_state IN ('healthy', 'error', 'reset_required')",
        name="user_messenger_sessions_auth_state_check",
    ),
    CheckConstraint(
        "status IN ('inactive', 'active')",
        name="user_messenger_sessions_status_check",
    ),
    Index("user_messenger_sessions_account_id_idx", "account_id"),
)
