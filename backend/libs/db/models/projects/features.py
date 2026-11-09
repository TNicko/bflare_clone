"""SQLAlchemy Core models: projects/features."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)

import enum

from db.metadata import metadata


class AlertType(str, enum.Enum):
    PHRASE = "phrase"
    USER = "user"


project_alerts = Table(
    "project_alerts",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("alert_name", Text, nullable=False),
    Column(
        "alert_type",
        Enum(AlertType, values_callable=lambda cls: [member.value for member in cls], name="alert_type"),
        nullable=False,
    ),
    Column("alert_phrase", Text),
    Column("alert_user", Uuid),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint("alert_phrase IS NULL) <> (alert_user IS NULL", name="either_phrase_or_user"),
    CheckConstraint(
        "alert_type <> 'phrase'::alert_type OR alert_phrase IS NOT NULL", name="phrase_present_when_phrase_alert_type"
    ),
    CheckConstraint(
        "alert_type <> 'user'::alert_type OR alert_user IS NOT NULL", name="user_present_when_user_alert_type"
    ),
    ForeignKeyConstraint(["project"], ["projects.id"], ondelete="CASCADE", name="project_alerts_project_fkey"),
    PrimaryKeyConstraint("id", name="project_alerts_pkey"),
)

project_reports = Table(
    "project_reports",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("report_name", Text, nullable=False),
    Column("report_location", Text, nullable=False),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    ForeignKeyConstraint(["project"], ["projects.id"], ondelete="CASCADE", name="project_reports_project_fkey"),
    PrimaryKeyConstraint("id", name="project_reports_pkey"),
    UniqueConstraint("project", "report_name", name="project_reports_project_report_name_key"),
)

project_telegram_channels = Table(
    "project_telegram_channels",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("channel", Uuid),
    ForeignKeyConstraint(
        ["channel"], ["telegram_channels.id"], ondelete="CASCADE", name="project_telegram_channels_channel_fkey"
    ),
    ForeignKeyConstraint(
        ["project"], ["projects.id"], ondelete="CASCADE", name="project_telegram_channels_project_fkey"
    ),
    PrimaryKeyConstraint("id", name="project_telegram_channels_pkey"),
    UniqueConstraint("project", "channel", name="project_telegram_channels_project_channel_key"),
)

project_telegram_chats = Table(
    "project_telegram_chats",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("chat", Uuid),
    ForeignKeyConstraint(["chat"], ["telegram_chats.id"], ondelete="CASCADE", name="project_telegram_chats_chat_fkey"),
    ForeignKeyConstraint(["project"], ["projects.id"], ondelete="CASCADE", name="project_telegram_chats_project_fkey"),
    PrimaryKeyConstraint("id", name="project_telegram_chats_pkey"),
    UniqueConstraint("project", "chat", name="project_telegram_chats_project_chat_key"),
)

project_telegram_megagroups = Table(
    "project_telegram_megagroups",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("megagroup", Uuid),
    ForeignKeyConstraint(
        ["megagroup"], ["telegram_megagroups.id"], ondelete="CASCADE", name="project_telegram_megagroups_megagroup_fkey"
    ),
    ForeignKeyConstraint(
        ["project"], ["projects.id"], ondelete="CASCADE", name="project_telegram_megagroups_project_fkey"
    ),
    PrimaryKeyConstraint("id", name="project_telegram_megagroups_pkey"),
    UniqueConstraint("project", "megagroup", name="project_telegram_megagroups_project_megagroup_key"),
)

project_telegram_messages = Table(
    "project_telegram_messages",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("message", Uuid),
    ForeignKeyConstraint(
        ["project"], ["projects.id"], ondelete="CASCADE", name="project_telegram_messages_project_fkey"
    ),
    PrimaryKeyConstraint("id", name="project_telegram_messages_pkey"),
    UniqueConstraint("project", "message", name="project_telegram_messages_project_message_key"),
)

project_telegram_users = Table(
    "project_telegram_users",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("project", Uuid),
    Column("user", Uuid),
    ForeignKeyConstraint(["project"], ["projects.id"], ondelete="CASCADE", name="project_telegram_users_project_fkey"),
    ForeignKeyConstraint(["user"], ["telegram_users.id"], ondelete="CASCADE", name="project_telegram_users_user_fkey"),
    PrimaryKeyConstraint("id", name="project_telegram_users_pkey"),
    UniqueConstraint("project", "user", name="project_telegram_users_project_user_key"),
)
