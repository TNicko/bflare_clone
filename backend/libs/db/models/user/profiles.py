"""SQLAlchemy Core models: user/profiles."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Table, Text, text
from sqlalchemy.dialects.postgresql import UUID

from db.metadata import metadata

profiles = Table(
    "profiles",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE", name="profiles_id_fkey"),
        primary_key=True,
    ),
    Column("display_name", Text),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("theme", Text, server_default=text("'dark'")),
    Column("sidebar_mode", Text),
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
