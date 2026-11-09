"""drop opensearch_service_metrics

Revision ID: c4e9a1b7d2f8
Revises: b68e4bcf80684
Create Date: 2026-11-09 18:30:00.000000

Keyword search is Quickwit. The OpenSearch metrics table is unused.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c4e9a1b7d2f8"
down_revision: Union[str, Sequence[str], None] = "b68e4bcf80684"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("opensearch_service_metrics_collected_idx", table_name="opensearch_service_metrics")
    op.drop_table("opensearch_service_metrics")


def downgrade() -> None:
    op.create_table(
        "opensearch_service_metrics",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("docs_count", sa.BigInteger(), nullable=True),
        sa.Column("store_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("heap_used_percent", sa.Float(), nullable=True),
        sa.Column("heap_max_bytes", sa.BigInteger(), nullable=True),
        sa.Column("messages_docs_count", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="opensearch_service_metrics_pkey"),
    )
    op.create_index(
        "opensearch_service_metrics_collected_idx",
        "opensearch_service_metrics",
        ["collected_at"],
        unique=False,
    )
