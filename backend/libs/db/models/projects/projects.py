"""SQLAlchemy Core models: projects/projects."""

from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)

from db.metadata import metadata

projects = Table(
    "projects",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column(
        "created_at",
        DateTime(True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column(
        "user_id",
        Uuid,
        comment="Owning platform user (auth.users.id). NULL only for legacy rows pending assignment.",
    ),
    ForeignKeyConstraint(
        ["user_id"],
        ["auth.users.id"],
        ondelete="CASCADE",
        name="projects_user_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="projects_pkey"),
    UniqueConstraint("user_id", "name", name="projects_user_id_name_key"),
    Index("projects_user_id_idx", "user_id"),
)
