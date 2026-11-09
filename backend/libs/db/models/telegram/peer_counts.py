"""Maintained row counts for the large telegram peer tables."""

from __future__ import annotations

from sqlalchemy import BigInteger, CheckConstraint, Column, DateTime, PrimaryKeyConstraint, Table, Text, text

from db.metadata import metadata

telegram_peer_counts = Table(
    "telegram_peer_counts",
    metadata,
    Column("table_name", Text, nullable=False),
    Column("row_count", BigInteger, nullable=False, server_default=text("0")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "table_name = ANY (ARRAY["
        "'telegram_channels'::text, "
        "'telegram_megagroups'::text, "
        "'telegram_chats'::text, "
        "'telegram_users'::text"
        "])",
        name="telegram_peer_counts_table_name_check",
    ),
    PrimaryKeyConstraint("table_name", name="telegram_peer_counts_pkey"),
    comment="Maintained row counts for large telegram peer tables. Updated by trigger.",
)
