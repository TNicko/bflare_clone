"""SQLAlchemy Core models: telegram/pipelines."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Table,
    Text,
    Uuid,
    text,
)

from db.metadata import metadata

telegram_media_download_coverage = Table(
    "telegram_media_download_coverage",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    Column("covered_after", DateTime(True)),
    Column("covered_before", DateTime(True), nullable=False),
    Column("count_image", Integer, nullable=False, server_default=text("0")),
    Column("count_gif", Integer, nullable=False, server_default=text("0")),
    Column("count_video", Integer, nullable=False, server_default=text("0")),
    Column("count_audio", Integer, nullable=False, server_default=text("0")),
    Column("count_document", Integer, nullable=False, server_default=text("0")),
    Column("count_webpage", Integer, nullable=False, server_default=text("0")),
    Column("phashes_in_milvus", Boolean, nullable=False, server_default=text("false")),
    Column("videos_excluded", Boolean, nullable=False, server_default=text("false")),
    Column("large_excluded", Boolean, nullable=False, server_default=text("false")),
    Column("seals_gap", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_media_download_coverage_pkey"),
    Index("telegram_media_download_coverage_lookup_idx", "peer_external_id", "peer_type"),
)

telegram_media_download_cursors = Table(
    "telegram_media_download_cursors",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    # NULL window_after = open start (−∞); unique via NULLS NOT DISTINCT index
    Column("window_after", DateTime(True)),
    Column("window_before", DateTime(True), nullable=False),
    Column("last_message_id", BigInteger, nullable=False, server_default=text("0")),
    Column("is_complete", Boolean, nullable=False, server_default=text("false")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_media_download_cursors_pkey"),
    Index(
        "telegram_media_download_cursors_window_uidx",
        "peer_external_id",
        "peer_type",
        "window_before",
        "window_after",
        unique=True,
        postgresql_nulls_not_distinct=True,
    ),
)

telegram_image_embed_coverage = Table(
    "telegram_image_embed_coverage",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("model", Text, nullable=False, server_default=text("'siglip2-so400m-p16-384'")),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    Column("covered_after", DateTime(True)),
    Column("covered_before", DateTime(True), nullable=False),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_image_embed_coverage_pkey"),
    Index("telegram_image_embed_coverage_lookup_idx", "model", "peer_external_id", "peer_type"),
)

telegram_image_embed_cursors = Table(
    "telegram_image_embed_cursors",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("model", Text, nullable=False, server_default=text("'siglip2-so400m-p16-384'")),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    # NULL window_after = open start (−∞); unique via NULLS NOT DISTINCT index
    Column("window_after", DateTime(True)),
    Column("window_before", DateTime(True), nullable=False),
    Column("last_message_id", BigInteger, nullable=False, server_default=text("0")),
    Column("is_complete", Boolean, nullable=False, server_default=text("false")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_image_embed_cursors_pkey"),
    Index(
        "telegram_image_embed_cursors_window_uidx",
        "model",
        "peer_external_id",
        "peer_type",
        "window_before",
        "window_after",
        unique=True,
        postgresql_nulls_not_distinct=True,
    ),
)

telegram_message_embed_coverage = Table(
    "telegram_message_embed_coverage",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    Column("covered_after", DateTime(True)),
    Column("covered_before", DateTime(True), nullable=False),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_message_embed_coverage_pkey"),
    Index("telegram_message_embed_coverage_lookup_idx", "peer_external_id", "peer_type"),
)

telegram_message_embed_cursors = Table(
    "telegram_message_embed_cursors",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    # NULL window_after = open start (−∞); unique via NULLS NOT DISTINCT index
    Column("window_after", DateTime(True)),
    Column("window_before", DateTime(True), nullable=False),
    Column("last_message_id", BigInteger, nullable=False, server_default=text("0")),
    Column("is_complete", Boolean, nullable=False, server_default=text("false")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_message_embed_cursors_pkey"),
    Index(
        "telegram_message_embed_cursors_window_uidx",
        "peer_external_id",
        "peer_type",
        "window_before",
        "window_after",
        unique=True,
        postgresql_nulls_not_distinct=True,
    ),
)

telegram_message_fetch_coverage = Table(
    "telegram_message_fetch_coverage",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("harvester_id", Uuid, nullable=False),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    Column("covered_after", DateTime(True)),
    Column("covered_before", DateTime(True), nullable=False),
    Column("message_count", BigInteger, nullable=False, server_default=text("0")),
    # Per-kind inventory from message ``media`` JSON (fetch-time; not S3 harvest).
    Column("count_image", Integer, nullable=False, server_default=text("0")),
    Column("count_gif", Integer, nullable=False, server_default=text("0")),
    Column("count_video", Integer, nullable=False, server_default=text("0")),
    Column("count_audio", Integer, nullable=False, server_default=text("0")),
    Column("count_document", Integer, nullable=False, server_default=text("0")),
    Column("count_webpage", Integer, nullable=False, server_default=text("0")),
    Column("count_url", Integer, nullable=False, server_default=text("0")),
    Column("count_mention", Integer, nullable=False, server_default=text("0")),
    #: True for islands written by the synthetic ``web_tme`` harvester (t.me/s scrape).
    Column("is_web", Boolean, nullable=False, server_default=text("false")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_message_fetch_coverage_pkey"),
    Index("telegram_message_fetch_coverage_lookup_idx", "harvester_id", "peer_external_id", "peer_type"),
    Index("telegram_message_fetch_coverage_peer_external_id_idx", "peer_external_id"),
)

telegram_message_fetch_cursors = Table(
    "telegram_message_fetch_cursors",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("harvester_id", Uuid, nullable=False),
    Column("peer_external_id", Text, nullable=False),
    Column("peer_type", Text, nullable=False),
    # window_before is always concrete (config None → latest/now); NULL window_after = open start (−∞)
    Column("window_before", DateTime(True), nullable=False),
    Column("window_after", DateTime(True)),
    Column("next_offset_id", BigInteger),
    Column("is_complete", Boolean, nullable=False, server_default=text("false")),
    Column("last_message_id_seen", BigInteger),
    Column("last_message_date_seen", DateTime(True)),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id", name="telegram_message_fetch_cursors_pkey"),
    Index(
        "telegram_message_fetch_cursors_window_uidx",
        "harvester_id",
        "peer_external_id",
        "window_before",
        "window_after",
        unique=True,
        postgresql_nulls_not_distinct=True,
    ),
)
