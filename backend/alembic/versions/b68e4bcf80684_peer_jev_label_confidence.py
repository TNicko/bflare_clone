"""peer_categories.confidence + peer_jev_label_runs

Revision ID: b68e4bcf80684
Revises:
Create Date: 2026-10-27

Example revision, kept with the two revisions after it. These are not a
database bootstrap; the statements alter tables that already exist.

Store topic/threat confidence on assigned category links, and a per-peer
prediction row (including abstains) for later evaluation.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "b68e4bcf80684"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "peer_categories",
        sa.Column("confidence", sa.Float(), nullable=True),
    )
    op.add_column(
        "peer_categories",
        sa.Column(
            "labeled_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "peer_jev_label_runs",
        sa.Column("community_id", sa.Uuid(), nullable=False),
        sa.Column("peer_type", sa.String(length=50), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("taxonomy_version", sa.Integer(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("topic_choice", sa.Text(), nullable=True),
        sa.Column("topic_confidence", sa.Float(), nullable=True),
        sa.Column(
            "topic_probabilities",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("threat_choice", sa.Text(), nullable=True),
        sa.Column("threat_confidence", sa.Float(), nullable=True),
        sa.Column(
            "threat_probabilities",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "written_slugs",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("topic_written", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("threat_written", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("input_tokens", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("output_tokens", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("community_id", "peer_type", name="peer_jev_label_runs_pkey"),
    )
    op.create_index(
        "peer_jev_label_runs_external_id_idx",
        "peer_jev_label_runs",
        ["external_id"],
    )

    # Match PostgREST lockdown pattern for new public tables.
    op.execute("REVOKE ALL ON TABLE public.peer_jev_label_runs FROM PUBLIC")
    op.execute("REVOKE ALL ON TABLE public.peer_jev_label_runs FROM anon")
    op.execute("REVOKE ALL ON TABLE public.peer_jev_label_runs FROM authenticated")
    op.execute("ALTER TABLE public.peer_jev_label_runs ENABLE ROW LEVEL SECURITY")


def downgrade() -> None:
    op.drop_index("peer_jev_label_runs_external_id_idx", table_name="peer_jev_label_runs")
    op.drop_table("peer_jev_label_runs")
    op.drop_column("peer_categories", "labeled_at")
    op.drop_column("peer_categories", "confidence")
