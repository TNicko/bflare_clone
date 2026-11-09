"""drop aws metric tables

Revision ID: a8d3f1c6e9b2
Revises: c4e9a1b7d2f8
Create Date: 2026-11-09 20:10:00.000000

AWS usage snapshots are no longer collected.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "a8d3f1c6e9b2"
down_revision: Union[str, Sequence[str], None] = "c4e9a1b7d2f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("s3_storage_metrics_collected_idx", table_name="s3_storage_metrics")
    op.drop_table("s3_storage_metrics")
    op.drop_index("milvus_service_metrics_collected_idx", table_name="milvus_service_metrics")
    op.drop_table("milvus_service_metrics")
    op.drop_index("aws_metric_snapshots_component_collected_idx", table_name="aws_metric_snapshots")
    op.drop_index("aws_metric_snapshots_collected_idx", table_name="aws_metric_snapshots")
    op.drop_table("aws_metric_snapshots")


def downgrade() -> None:
    op.create_table(
        "aws_metric_snapshots",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("component", sa.Text(), nullable=False),
        sa.Column("region", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("instance_id", sa.Text(), nullable=True),
        sa.Column("instance_state", sa.Text(), nullable=True),
        sa.Column("ebs_total_gib", sa.Integer(), nullable=True),
        sa.Column("estimated_monthly_usd", sa.Float(), nullable=True),
        sa.CheckConstraint(
            "kind = ANY (ARRAY['s3'::text, 'ec2'::text])",
            name="aws_metric_snapshots_kind_check",
        ),
        sa.PrimaryKeyConstraint("id", name="aws_metric_snapshots_pkey"),
    )
    op.create_index("aws_metric_snapshots_collected_idx", "aws_metric_snapshots", ["collected_at"], unique=False)
    op.create_index(
        "aws_metric_snapshots_component_collected_idx",
        "aws_metric_snapshots",
        ["component", "collected_at"],
        unique=False,
    )
    op.create_table(
        "milvus_service_metrics",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("uri", sa.Text(), nullable=True),
        sa.Column("messages", sa.BigInteger(), nullable=True),
        sa.Column("images", sa.BigInteger(), nullable=True),
        sa.Column("image_phashes", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="milvus_service_metrics_pkey"),
    )
    op.create_index("milvus_service_metrics_collected_idx", "milvus_service_metrics", ["collected_at"], unique=False)
    op.create_table(
        "s3_storage_metrics",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("region", sa.Text(), nullable=False),
        sa.Column("harvest_bucket", sa.Text(), nullable=True),
        sa.Column("embeddings_bucket", sa.Text(), nullable=True),
        sa.Column("harvest_total_count", sa.BigInteger(), nullable=True),
        sa.Column("harvest_total_bytes", sa.BigInteger(), nullable=True),
        sa.Column("profile_photos_count", sa.BigInteger(), nullable=True),
        sa.Column("profile_photos_bytes", sa.BigInteger(), nullable=True),
        sa.Column("message_media_count", sa.BigInteger(), nullable=True),
        sa.Column("message_media_bytes", sa.BigInteger(), nullable=True),
        sa.Column("message_media_images_count", sa.BigInteger(), nullable=True),
        sa.Column("message_media_images_bytes", sa.BigInteger(), nullable=True),
        sa.Column("message_media_videos_count", sa.BigInteger(), nullable=True),
        sa.Column("message_media_videos_bytes", sa.BigInteger(), nullable=True),
        sa.Column("message_media_documents_count", sa.BigInteger(), nullable=True),
        sa.Column("message_media_documents_bytes", sa.BigInteger(), nullable=True),
        sa.Column("profile_stories_count", sa.BigInteger(), nullable=True),
        sa.Column("profile_stories_bytes", sa.BigInteger(), nullable=True),
        sa.Column("profile_gifts_count", sa.BigInteger(), nullable=True),
        sa.Column("profile_gifts_bytes", sa.BigInteger(), nullable=True),
        sa.Column("harvest_other_count", sa.BigInteger(), nullable=True),
        sa.Column("harvest_other_bytes", sa.BigInteger(), nullable=True),
        sa.Column("embeddings_total_count", sa.BigInteger(), nullable=True),
        sa.Column("embeddings_total_bytes", sa.BigInteger(), nullable=True),
        sa.Column("embedding_text_shards_count", sa.BigInteger(), nullable=True),
        sa.Column("embedding_text_shards_bytes", sa.BigInteger(), nullable=True),
        sa.Column("embedding_image_shards_count", sa.BigInteger(), nullable=True),
        sa.Column("embedding_image_shards_bytes", sa.BigInteger(), nullable=True),
        sa.Column("embedding_other_shards_count", sa.BigInteger(), nullable=True),
        sa.Column("embedding_other_shards_bytes", sa.BigInteger(), nullable=True),
        sa.Column("harvest_estimated_monthly_usd", sa.Float(), nullable=True),
        sa.Column("embeddings_estimated_monthly_usd", sa.Float(), nullable=True),
        sa.Column("estimated_monthly_usd", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="s3_storage_metrics_pkey"),
    )
    op.create_index("s3_storage_metrics_collected_idx", "s3_storage_metrics", ["collected_at"], unique=False)
