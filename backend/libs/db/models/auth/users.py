"""Supabase auth.users — reference only (not migrated by this repo).

Full schema reflected from Supabase for FK resolution and documentation.
Alembic ignores the auth schema via ``include_object`` in ``alembic/env.py``.
"""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    Index,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.metadata import metadata

users = Table(
    "users",
    metadata,
    Column("instance_id", Uuid),
    Column("id", Uuid, primary_key=True),
    Column("aud", String(255)),
    Column("role", String(255)),
    Column("email", String(255)),
    Column("encrypted_password", String(255)),
    Column("email_confirmed_at", DateTime(True)),
    Column("invited_at", DateTime(True)),
    Column("confirmation_token", String(255)),
    Column("confirmation_sent_at", DateTime(True)),
    Column("recovery_token", String(255)),
    Column("recovery_sent_at", DateTime(True)),
    Column("email_change_token_new", String(255)),
    Column("email_change", String(255)),
    Column("email_change_sent_at", DateTime(True)),
    Column("last_sign_in_at", DateTime(True)),
    Column("raw_app_meta_data", JSONB),
    Column("raw_user_meta_data", JSONB),
    Column("is_super_admin", Boolean),
    Column("created_at", DateTime(True)),
    Column("updated_at", DateTime(True)),
    Column("phone", Text, server_default=text("NULL::character varying")),
    Column("phone_confirmed_at", DateTime(True)),
    Column("phone_change", Text, server_default=text("''::character varying")),
    Column("phone_change_token", String(255), server_default=text("''::character varying")),
    Column("phone_change_sent_at", DateTime(True)),
    Column(
        "confirmed_at",
        DateTime(True),
        Computed("LEAST(email_confirmed_at, phone_confirmed_at)", persisted=True),
    ),
    Column("email_change_token_current", String(255), server_default=text("''::character varying")),
    Column("email_change_confirm_status", SmallInteger, server_default=text("0")),
    Column("banned_until", DateTime(True)),
    Column("reauthentication_token", String(255), server_default=text("''::character varying")),
    Column("reauthentication_sent_at", DateTime(True)),
    Column(
        "is_sso_user",
        Boolean,
        nullable=False,
        server_default=text("false"),
        comment="Auth: Set this column to true when the account comes from SSO. These accounts can have duplicate emails.",
    ),
    Column("deleted_at", DateTime(True)),
    Column("is_anonymous", Boolean, nullable=False, server_default=text("false")),
    CheckConstraint(
        "email_change_confirm_status >= 0 AND email_change_confirm_status <= 2",
        name="users_email_change_confirm_status_check",
    ),
    PrimaryKeyConstraint("id", name="users_pkey"),
    UniqueConstraint("phone", name="users_phone_key"),
    Index(
        "confirmation_token_idx",
        "confirmation_token",
        postgresql_where="((confirmation_token)::text !~ '^[0-9 ]*$'::text)",
        unique=True,
    ),
    Index(
        "email_change_token_current_idx",
        "email_change_token_current",
        postgresql_where="((email_change_token_current)::text !~ '^[0-9 ]*$'::text)",
        unique=True,
    ),
    Index(
        "email_change_token_new_idx",
        "email_change_token_new",
        postgresql_where="((email_change_token_new)::text !~ '^[0-9 ]*$'::text)",
        unique=True,
    ),
    Index("idx_users_created_at_desc", "created_at"),
    Index("idx_users_email", "email"),
    Index("idx_users_last_sign_in_at_desc", "last_sign_in_at"),
    Index(
        "idx_users_name",
        postgresql_where="((raw_user_meta_data ->> 'name'::text) IS NOT NULL)",
    ),
    Index(
        "reauthentication_token_idx",
        "reauthentication_token",
        postgresql_where="((reauthentication_token)::text !~ '^[0-9 ]*$'::text)",
        unique=True,
    ),
    Index(
        "recovery_token_idx",
        "recovery_token",
        postgresql_where="((recovery_token)::text !~ '^[0-9 ]*$'::text)",
        unique=True,
    ),
    Index("users_email_partial_key", "email", postgresql_where="(is_sso_user = false)", unique=True),
    Index("users_instance_id_email_idx", "instance_id"),
    Index("users_instance_id_idx", "instance_id"),
    Index("users_is_anonymous_idx", "is_anonymous"),
    schema="auth",
    comment="Auth: Stores user login data within a secure schema.",
)
