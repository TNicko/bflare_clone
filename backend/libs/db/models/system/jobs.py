"""SQLAlchemy Core models: fleet job workers + harvest job queue."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from db.metadata import metadata

_TASK_TYPES_SQL = (
    "ARRAY["
    "'embed_images'::text, "
    "'embed_messages'::text, "
    "'fetch_messages'::text, "
    "'telegram_search'::text"
    "]"
)

job_workers = Table(
    "job_workers",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("slug", Text, nullable=False),
    Column("label", Text, nullable=False),
    Column("capabilities", ARRAY(Text), nullable=False, server_default=text("'{}'::text[]")),
    Column("max_concurrent_jobs", Integer, nullable=False, server_default=text("1")),
    Column("status", Text, nullable=False, server_default=text("'offline'::text")),
    Column("hostname", Text),
    Column("tailscale_ip", Text),
    Column("agent_version", Text),
    Column("git_sha", Text),
    Column("last_heartbeat_at", DateTime(True)),
    Column("meta", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "status = ANY (ARRAY['online'::text, 'offline'::text, 'draining'::text])",
        name="job_workers_status_check",
    ),
    CheckConstraint(
        "max_concurrent_jobs >= 1 AND max_concurrent_jobs <= 32",
        name="job_workers_max_concurrent_jobs_check",
    ),
    PrimaryKeyConstraint("id", name="job_workers_pkey"),
    UniqueConstraint("slug", name="job_workers_slug_key"),
    Index("job_workers_status_heartbeat_idx", "status", "last_heartbeat_at"),
    comment="Fleet hosts (Pi / Hetzner) that claim harvest_jobs",
)

harvest_jobs = Table(
    "harvest_jobs",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("task_type", Text, nullable=False),
    Column("status", Text, nullable=False, server_default=text("'queued'::text")),
    Column("params", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("requested_worker_id", Uuid),
    Column("worker_id", Uuid),
    Column("worker_slug", Text),
    Column("actor_type", Text, nullable=False),
    Column("actor_id", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("progress", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("error", Text),
    Column("lease_expires_at", DateTime(True)),
    Column("created_at", DateTime(True), nullable=False, server_default=text("now()")),
    Column("started_at", DateTime(True)),
    Column("finished_at", DateTime(True)),
    Column("updated_at", DateTime(True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        f"task_type = ANY ({_TASK_TYPES_SQL})",
        name="harvest_jobs_task_type_check",
    ),
    CheckConstraint(
        "status = ANY (ARRAY['queued'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text])",
        name="harvest_jobs_status_check",
    ),
    CheckConstraint(
        "actor_type = ANY (ARRAY['user'::text, 'admin'::text, 'system'::text])",
        name="harvest_jobs_actor_type_check",
    ),
    ForeignKeyConstraint(
        ["requested_worker_id"],
        ["job_workers.id"],
        name="harvest_jobs_requested_worker_id_fkey",
        ondelete="SET NULL",
    ),
    ForeignKeyConstraint(
        ["worker_id"],
        ["job_workers.id"],
        name="harvest_jobs_worker_id_fkey",
        ondelete="SET NULL",
    ),
    PrimaryKeyConstraint("id", name="harvest_jobs_pkey"),
    Index("harvest_jobs_status_created_idx", "status", "created_at"),
    Index("harvest_jobs_worker_status_idx", "worker_id", "status"),
    Index("harvest_jobs_task_status_idx", "task_type", "status"),
    Index("harvest_jobs_claim_idx", "status", "requested_worker_id", "created_at"),
    comment="Generic pull queue for fleet job agents",
)
