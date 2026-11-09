"""CRUD for job_workers + harvest_jobs fleet queue."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Sequence
from uuid import UUID

from psycopg.rows import dict_row
from psycopg.types.json import Json

ActorType = Literal["user", "admin", "system"]
WorkerStatus = Literal["online", "offline", "draining"]
JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]

_ACTIVE_JOB_STATUSES = ("queued", "running")
_DEFAULT_LEASE_SECONDS = 120
_ONLINE_HEARTBEAT_SECONDS = 60

TASK_TYPES: tuple[str, ...] = (
    "embed_images",
    "embed_messages",
    "fetch_messages",
    "telegram_search",
)


def _row_worker(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "slug": row["slug"],
        "label": row["label"],
        "capabilities": list(row["capabilities"] or []),
        "max_concurrent_jobs": int(row["max_concurrent_jobs"]),
        "status": row["status"],
        "hostname": row.get("hostname"),
        "tailscale_ip": row.get("tailscale_ip"),
        "agent_version": row.get("agent_version"),
        "git_sha": row.get("git_sha"),
        "last_heartbeat_at": row.get("last_heartbeat_at"),
        "meta": dict(row.get("meta") or {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "running_jobs": int(row["running_jobs"]) if "running_jobs" in row else None,
    }


def _row_job(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "task_type": row["task_type"],
        "status": row["status"],
        "params": dict(row.get("params") or {}),
        "requested_worker_id": row.get("requested_worker_id"),
        "worker_id": row.get("worker_id"),
        "worker_slug": row.get("worker_slug"),
        "actor_type": row["actor_type"],
        "actor_id": row["actor_id"],
        "source": row["source"],
        "progress": dict(row.get("progress") or {}),
        "error": row.get("error"),
        "lease_expires_at": row.get("lease_expires_at"),
        "created_at": row["created_at"],
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "updated_at": row["updated_at"],
    }


async def upsert_worker(
    conn: Any,
    *,
    slug: str,
    label: str,
    capabilities: Sequence[str],
    max_concurrent_jobs: int | None = None,
    hostname: str | None = None,
    tailscale_ip: str | None = None,
    agent_version: str | None = None,
    git_sha: str | None = None,
    meta: dict[str, Any] | None = None,
    seed_max_concurrent: bool = False,
) -> dict[str, Any]:
    """Register or refresh a worker. Heartbeat sets status=online.

    ``max_concurrent_jobs`` is only written on insert, or when
    ``seed_max_concurrent`` is True (bootstrap). Later database updates otherwise win.
    """
    caps = [c for c in capabilities if c in TASK_TYPES]
    max_n = max(1, min(int(max_concurrent_jobs or 1), 32))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO job_workers AS w (
              slug, label, capabilities, max_concurrent_jobs,
              status, hostname, tailscale_ip, agent_version, git_sha,
              last_heartbeat_at, meta, updated_at
            )
            VALUES (
              %s, %s, %s, %s,
              'online', %s, %s, %s, %s,
              now(), %s, now()
            )
            ON CONFLICT (slug) DO UPDATE SET
              label = EXCLUDED.label,
              capabilities = EXCLUDED.capabilities,
              status = 'online',
              hostname = COALESCE(EXCLUDED.hostname, w.hostname),
              tailscale_ip = COALESCE(EXCLUDED.tailscale_ip, w.tailscale_ip),
              agent_version = EXCLUDED.agent_version,
              git_sha = EXCLUDED.git_sha,
              last_heartbeat_at = now(),
              meta = CASE
                WHEN EXCLUDED.meta = '{}'::jsonb THEN w.meta
                ELSE EXCLUDED.meta
              END,
              max_concurrent_jobs = CASE
                WHEN %s THEN EXCLUDED.max_concurrent_jobs
                ELSE w.max_concurrent_jobs
              END,
              updated_at = now()
            RETURNING *
            """,
            (
                slug,
                label,
                caps,
                max_n,
                hostname,
                tailscale_ip,
                agent_version,
                git_sha,
                Json(meta or {}),
                seed_max_concurrent,
            ),
        )
        row = await cur.fetchone()
    assert row is not None
    return _row_worker(row)


async def heartbeat_worker(conn: Any, *, worker_id: UUID) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE job_workers
            SET last_heartbeat_at = now(),
                status = CASE WHEN status = 'draining' THEN status ELSE 'online' END,
                updated_at = now()
            WHERE id = %s
            RETURNING *
            """,
            (worker_id,),
        )
        row = await cur.fetchone()
    return _row_worker(row) if row else None


async def mark_stale_workers_offline(
    conn: Any,
    *,
    stale_after_seconds: int = _ONLINE_HEARTBEAT_SECONDS,
) -> int:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE job_workers
            SET status = 'offline', updated_at = now()
            WHERE status = 'online'
              AND (
                last_heartbeat_at IS NULL
                OR last_heartbeat_at < now() - make_interval(secs => %s)
              )
            """,
            (stale_after_seconds,),
        )
        return int(cur.rowcount or 0)


async def list_workers(conn: Any) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              w.*,
              (
                SELECT COUNT(*)::int
                FROM harvest_jobs j
                WHERE j.worker_id = w.id AND j.status = 'running'
              ) AS running_jobs
            FROM job_workers w
            ORDER BY w.slug
            """
        )
        rows = await cur.fetchall()
    return [_row_worker(r) for r in rows]


async def get_worker_by_slug(conn: Any, slug: str) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM job_workers WHERE slug = %s", (slug,))
        row = await cur.fetchone()
    return _row_worker(row) if row else None


async def get_worker(conn: Any, worker_id: UUID) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM job_workers WHERE id = %s", (worker_id,))
        row = await cur.fetchone()
    return _row_worker(row) if row else None


async def set_worker_max_concurrent(
    conn: Any,
    *,
    worker_id: UUID,
    max_concurrent_jobs: int,
) -> dict[str, Any] | None:
    max_n = max(1, min(int(max_concurrent_jobs), 32))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE job_workers
            SET max_concurrent_jobs = %s, updated_at = now()
            WHERE id = %s
            RETURNING *
            """,
            (max_n, worker_id),
        )
        row = await cur.fetchone()
    return _row_worker(row) if row else None


async def count_running_for_worker(conn: Any, worker_id: UUID) -> int:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM harvest_jobs
            WHERE worker_id = %s AND status = 'running'
            """,
            (worker_id,),
        )
        row = await cur.fetchone()
    return int(row["n"] if row else 0)


async def enqueue_job(
    conn: Any,
    *,
    task_type: str,
    params: dict[str, Any] | None = None,
    requested_worker_id: UUID | None = None,
    actor_type: ActorType,
    actor_id: str,
    source: str,
) -> dict[str, Any]:
    if task_type not in TASK_TYPES:
        raise ValueError(f"unknown task_type: {task_type}")
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO harvest_jobs (
              task_type, params, requested_worker_id,
              actor_type, actor_id, source
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                task_type,
                Json(params or {}),
                requested_worker_id,
                actor_type,
                actor_id,
                source,
            ),
        )
        row = await cur.fetchone()
    assert row is not None
    return _row_job(row)


async def get_job(conn: Any, job_id: UUID) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM harvest_jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
    return _row_job(row) if row else None


async def list_jobs(
    conn: Any,
    *,
    statuses: Sequence[str] | None = None,
    task_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    status_filter = list(statuses) if statuses else list(_ACTIVE_JOB_STATUSES)
    limit = max(1, min(int(limit), 200))
    clauses = ["status = ANY(%s)"]
    args: list[Any] = [status_filter]
    if task_type:
        clauses.append("task_type = %s")
        args.append(task_type)
    args.append(limit)
    where = " AND ".join(clauses)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT *
            FROM harvest_jobs
            WHERE {where}
            ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, created_at DESC
            LIMIT %s
            """,
            tuple(args),
        )
        rows = await cur.fetchall()
    return [_row_job(r) for r in rows]


async def claim_next_job(
    conn: Any,
    *,
    worker_id: UUID,
    worker_slug: str,
    capabilities: Sequence[str],
    lease_seconds: int = _DEFAULT_LEASE_SECONDS,
) -> dict[str, Any] | None:
    """Claim one queued job if under this worker's max_concurrent_jobs."""
    caps = list(capabilities) or list(TASK_TYPES)
    lease_until = datetime.now(UTC) + timedelta(seconds=lease_seconds)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            WITH worker AS (
              SELECT id, max_concurrent_jobs, status
              FROM job_workers
              WHERE id = %s
              FOR UPDATE
            ),
            running AS (
              SELECT COUNT(*)::int AS n
              FROM harvest_jobs
              WHERE worker_id = %s AND status = 'running'
            ),
            picked AS (
              SELECT j.id
              FROM harvest_jobs j, worker w, running r
              WHERE j.status = 'queued'
                AND w.status IN ('online', 'draining')
                AND r.n < w.max_concurrent_jobs
                AND (j.requested_worker_id IS NULL OR j.requested_worker_id = w.id)
                AND j.task_type = ANY(%s)
              ORDER BY j.created_at ASC
              FOR UPDATE OF j SKIP LOCKED
              LIMIT 1
            )
            UPDATE harvest_jobs AS hj
            SET status = 'running',
                worker_id = %s,
                worker_slug = %s,
                started_at = COALESCE(hj.started_at, now()),
                lease_expires_at = %s,
                updated_at = now()
            FROM picked
            WHERE hj.id = picked.id
            RETURNING hj.*
            """,
            (worker_id, worker_id, caps, worker_id, worker_slug, lease_until),
        )
        row = await cur.fetchone()
    return _row_job(row) if row else None


async def renew_lease(
    conn: Any,
    *,
    job_id: UUID,
    worker_id: UUID,
    lease_seconds: int = _DEFAULT_LEASE_SECONDS,
) -> bool:
    lease_until = datetime.now(UTC) + timedelta(seconds=lease_seconds)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE harvest_jobs
            SET lease_expires_at = %s, updated_at = now()
            WHERE id = %s AND worker_id = %s AND status = 'running'
            """,
            (lease_until, job_id, worker_id),
        )
        return bool(cur.rowcount)


async def update_job_progress(
    conn: Any,
    *,
    job_id: UUID,
    progress: dict[str, Any],
    merge: bool = True,
) -> None:
    """Write progress for a running job.

    When ``merge`` is True (default), shallow-merge into existing JSONB so callers
    can patch counters without wiping earlier fields.
    """
    async with conn.cursor() as cur:
        if merge:
            await cur.execute(
                """
                UPDATE harvest_jobs
                SET progress = COALESCE(progress, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                WHERE id = %s AND status = 'running'
                """,
                (Json(progress), job_id),
            )
        else:
            await cur.execute(
                """
                UPDATE harvest_jobs
                SET progress = %s, updated_at = now()
                WHERE id = %s AND status = 'running'
                """,
                (Json(progress), job_id),
            )


async def finish_job(
    conn: Any,
    *,
    job_id: UUID,
    status: Literal["succeeded", "failed", "cancelled"],
    error: str | None = None,
    progress: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        if progress is not None:
            await cur.execute(
                """
                UPDATE harvest_jobs
                SET status = %s,
                    error = %s,
                    progress = COALESCE(progress, '{}'::jsonb) || %s::jsonb,
                    finished_at = now(),
                    lease_expires_at = NULL,
                    updated_at = now()
                WHERE id = %s
                  AND status IN ('queued', 'running')
                RETURNING *
                """,
                (status, error, Json(progress), job_id),
            )
        else:
            await cur.execute(
                """
                UPDATE harvest_jobs
                SET status = %s,
                    error = %s,
                    finished_at = now(),
                    lease_expires_at = NULL,
                    updated_at = now()
                WHERE id = %s
                  AND status IN ('queued', 'running')
                RETURNING *
                """,
                (status, error, job_id),
            )
        row = await cur.fetchone()
    return _row_job(row) if row else None


async def cancel_job(conn: Any, job_id: UUID) -> dict[str, Any] | None:
    return await finish_job(conn, job_id=job_id, status="cancelled", error="Cancelled")


async def count_active_jobs(conn: Any, *, task_type: str | None = None) -> int:
    clauses = ["status = ANY(%s)"]
    args: list[Any] = [list(_ACTIVE_JOB_STATUSES)]
    if task_type:
        clauses.append("task_type = %s")
        args.append(task_type)
    where = " AND ".join(clauses)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT COUNT(*)::int AS n FROM harvest_jobs WHERE {where}",
            tuple(args),
        )
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


async def find_active_job_for_seed(
    conn: Any,
    *,
    seed_external_id: str,
    task_type: str = "forward_graph",
) -> dict[str, Any] | None:
    """Return an active fleet job for this seed (params.seed_external_id / external_id)."""
    seed = str(seed_external_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT *
            FROM harvest_jobs
            WHERE task_type = %s
              AND status = ANY(%s)
              AND (
                params->>'seed_external_id' = %s
                OR params->>'external_id' = %s
              )
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (task_type, list(_ACTIVE_JOB_STATUSES), seed, seed),
        )
        row = await cur.fetchone()
    return _row_job(row) if row else None


async def list_reserved_harvester_external_ids(
    conn: Any,
    *,
    exclude_job_id: str | None = None,
) -> set[str]:
    """Telegram user ids pinned on a queued or running job.

    Jobs that name their harvesters keep those accounts. Other jobs must not
    open a second client on them. ``exclude_job_id`` is the caller, so a job
    does not treat its own pins as taken.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT DISTINCT btrim(ext) AS external_id
            FROM harvest_jobs j
            CROSS JOIN LATERAL (
              SELECT jsonb_array_elements_text(
                CASE
                  WHEN jsonb_typeof(j.params->'harvester_external_ids') = 'array'
                  THEN j.params->'harvester_external_ids'
                  ELSE '[]'::jsonb
                END
              ) AS ext
              UNION ALL
              SELECT NULLIF(btrim(j.params->>'harvester_external_id'), '')
            ) ids
            WHERE j.status = ANY(%s)
              AND (%s::uuid IS NULL OR j.id <> %s::uuid)
              AND ext IS NOT NULL
              AND btrim(ext) <> ''
            """,
            (list(_ACTIVE_JOB_STATUSES), exclude_job_id, exclude_job_id),
        )
        rows = await cur.fetchall()
    return {str(row["external_id"]) for row in rows if row.get("external_id")}


async def list_active_job_params(conn: Any, *, task_type: str) -> list[dict[str, Any]]:
    """Params of queued and running jobs for ``task_type``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT params
            FROM harvest_jobs
            WHERE task_type = %s
              AND status = ANY(%s)
            """,
            (task_type, list(_ACTIVE_JOB_STATUSES)),
        )
        rows = await cur.fetchall()
    return [dict(row["params"] or {}) for row in rows]


async def job_is_cancelled(conn: Any, job_id: UUID) -> bool:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT status FROM harvest_jobs WHERE id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
    return bool(row and row["status"] == "cancelled")


async def reclaim_expired_leases(conn: Any) -> int:
    """Return expired running jobs to queued so another worker can claim them."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE harvest_jobs
            SET status = 'queued',
                worker_id = NULL,
                worker_slug = NULL,
                started_at = NULL,
                lease_expires_at = NULL,
                error = COALESCE(error, 'lease expired; requeued'),
                updated_at = now()
            WHERE status = 'running'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < now()
            """
        )
        return int(cur.rowcount or 0)
