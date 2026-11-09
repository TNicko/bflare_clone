"""SQLAlchemy Core models: telegram/harvesters."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
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

telegram_harvesters = Table(
    "telegram_harvesters",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("external_id", Text, nullable=False),
    Column("label", Text),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("phone_number", Text),
    Column("username", Text),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("session_state", String(50), nullable=False, server_default=text("'missing'::character varying")),
    CheckConstraint(
        "session_state::text = ANY (ARRAY['missing'::character varying::text, 'active'::character varying::text, 'error'::character varying::text, 'reset_required'::character varying::text])",
        name="telegram_harvesters_session_state",
    ),
    PrimaryKeyConstraint("id", name="telegram_harvesters_pkey"),
    UniqueConstraint("external_id", name="telegram_harvesters_peer_id_key"),
    comment="Telegram accounts used for harvesting; id = our UUID, peer_id = Telegram user id",
)

telegram_harvester_peer_permissions = Table(
    "telegram_harvester_peer_permissions",
    metadata,
    Column("harvester_id", Uuid, nullable=False),
    Column("external_id", Text, nullable=False),
    Column("peer_type", String(50), nullable=False),
    Column("notify_settings", JSONB),
    Column("read_inbox_max_id", Integer),
    Column("read_outbox_max_id", Integer),
    Column("unread_count", Integer),
    Column("pts", Integer),
    Column("can_view_participants", Boolean),
    Column("can_set_username", Boolean),
    Column("can_set_stickers", Boolean),
    Column("can_set_location", Boolean),
    Column("can_view_stats", Boolean),
    Column("can_delete_channel", Boolean),
    Column("blocked", Boolean),
    Column("has_scheduled", Boolean),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("can_view_revenue", Boolean),
    Column("can_view_stars_revenue", Boolean),
    Column("admin_rights", JSONB),
    CheckConstraint(
        "peer_type::text = ANY (ARRAY['channel'::character varying::text, 'megagroup'::character varying::text, 'chat'::character varying::text])",
        name="telegram_harvester_peer_permissions_peer_type",
    ),
    ForeignKeyConstraint(
        ["harvester_id"],
        ["telegram_harvesters.id"],
        ondelete="CASCADE",
        name="telegram_harvester_peer_permissions_harvester_id_fkey",
    ),
    UniqueConstraint("harvester_id", "external_id", name="telegram_harvester_peer_permissions_unique"),
    comment="Per-harvester view of each channel/chat: notify settings, read position, and permission flags (updated on every full sync).",
)

telegram_harvester_peer_roles = Table(
    "telegram_harvester_peer_roles",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("external_id", Text, nullable=False),
    Column("harvester_id", Uuid, nullable=False),
    Column("role", String(50), nullable=False, server_default=text("'child'::character varying")),
    Column("assigned_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("peer_type", String(50)),
    Column("access_hash", BigInteger),
    Column("access_error_code", Text),
    Column("access_checked_at", DateTime(True)),
    CheckConstraint(
        "peer_type::text = ANY (ARRAY['user'::character varying::text, 'channel'::character varying::text, 'megagroup'::character varying::text, 'chat'::character varying::text])",
        name="telegram_harvester_peer_roles_peer_type_check",
    ),
    CheckConstraint(
        "role::text = ANY (ARRAY['parent'::character varying::text, 'child'::character varying::text])",
        name="valid_role",
    ),
    ForeignKeyConstraint(
        ["harvester_id"],
        ["telegram_harvesters.id"],
        ondelete="CASCADE",
        name="telegram_peer_harvester_roles_harvester_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="telegram_peer_harvester_roles_pkey"),
    UniqueConstraint("external_id", "harvester_id", name="unique_harvester_peer_roles_peer_harvester"),
)

telegram_harvester_sessions = Table(
    "telegram_harvester_sessions",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("harvester_id", Uuid, nullable=False),
    Column("label", Text, nullable=False),
    Column("session_type", Text, nullable=False),
    Column("auth_state", Text, nullable=False, server_default=text("'healthy'::text")),
    Column("status", Text, nullable=False, server_default=text("'inactive'::text")),
    Column("session_string", Text),
    Column("auth_key", LargeBinary),
    Column("dc_id", SmallInteger),
    Column("server_address", Text),
    Column("port", Integer),
    Column("leased_by_location_id", Text),
    Column("leased_until", DateTime(True)),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "auth_state = ANY (ARRAY['healthy'::text, 'error'::text, 'reset_required'::text])",
        name="telegram_harvester_sessions_auth_state_check",
    ),
    CheckConstraint(
        "session_type = ANY (ARRAY['api'::text, 'system'::text])", name="telegram_harvester_sessions_session_type_check"
    ),
    CheckConstraint(
        "status = ANY (ARRAY['inactive'::text, 'active'::text])", name="telegram_harvester_sessions_status_check"
    ),
    ForeignKeyConstraint(
        ["harvester_id"],
        ["telegram_harvesters.id"],
        ondelete="CASCADE",
        name="telegram_harvester_sessions_harvester_id_fkey",
    ),
    PrimaryKeyConstraint("id", name="telegram_harvester_sessions_pkey"),
    UniqueConstraint("harvester_id", "label", name="telegram_harvester_sessions_harvester_label_unique"),
)

telegram_harvester_session_lease_holders = Table(
    "telegram_harvester_session_lease_holders",
    metadata,
    Column("session_id", Uuid, primary_key=True),
    Column("server_id", Text, primary_key=True),
    Column("location_id", Text, nullable=False),
    Column("last_heartbeat_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("joined_at", DateTime(True), nullable=False, server_default=text("now()")),
    ForeignKeyConstraint(
        ["session_id"],
        ["telegram_harvester_sessions.id"],
        ondelete="CASCADE",
        name="telegram_harvester_session_lease_holders_session_id_fkey",
    ),
    PrimaryKeyConstraint("session_id", "server_id", name="telegram_harvester_session_lease_holders_pkey"),
)

telegram_harvester_session_update_states = Table(
    "telegram_harvester_session_update_states",
    metadata,
    Column("session_id", Uuid, primary_key=True),
    Column("entity_id", BigInteger, primary_key=True),
    Column("pts", Integer, nullable=False),
    Column("qts", Integer, nullable=False, server_default=text("0")),
    Column("seq", Integer, nullable=False, server_default=text("0")),
    Column("state_date", DateTime(True), nullable=False),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    ForeignKeyConstraint(
        ["session_id"],
        ["telegram_harvester_sessions.id"],
        ondelete="CASCADE",
        name="telegram_harvester_session_update_states_session_id_fkey",
    ),
    PrimaryKeyConstraint("session_id", "entity_id", name="telegram_harvester_session_update_states_pkey"),
)
