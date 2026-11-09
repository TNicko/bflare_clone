"""Fleet jobs agent: heartbeat + claim + run task CLIs as subprocesses."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any
from uuid import UUID

import psycopg

from db.connection import connect_async_with_retry
from db.crud.system.jobs import (
    cancel_job,
    claim_next_job,
    finish_job,
    get_job,
    heartbeat_worker,
    job_is_cancelled,
    mark_stale_workers_offline,
    reclaim_expired_leases,
    renew_lease,
    update_job_progress,
    upsert_worker,
)
from jobs.argv import TASK_CLI_OVERRIDE, params_to_argv
from jobs.agent.config import AgentConfig, ConfigFactory
from jobs.params_env import params_to_env
from jobs.task_types import TASK_CLI_MODULES
from utils.env import load_service_dotenv
from utils.logger import configure_logging

lg = logging.getLogger(__name__)


async def _terminate_process(proc: asyncio.subprocess.Process, *, job_id: UUID) -> None:
    """SIGTERM the task CLI, then SIGKILL if it ignores cancel."""
    if proc.returncode is not None:
        return
    lg.warning("terminating cancelled job %s pid=%s", job_id, proc.pid)
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=15.0)
        return
    except TimeoutError:
        pass
    except ProcessLookupError:
        return
    lg.warning("force-killing cancelled job %s pid=%s", job_id, proc.pid)
    try:
        proc.kill()
        await proc.wait()
    except ProcessLookupError:
        return


async def _run_task_subprocess(
    *,
    task_type: str,
    params: dict[str, Any],
    job_id: UUID,
    postgres_dsn: str,
    procs: dict[UUID, asyncio.subprocess.Process],
) -> int:
    import os

    module = TASK_CLI_OVERRIDE.get(task_type) or TASK_CLI_MODULES.get(task_type)
    if not module:
        raise ValueError(f"no CLI module for task_type={task_type}")
    argv = params_to_argv(task_type, params)
    cmd = [sys.executable, "-m", module, *argv]
    lg.info("job %s starting: %s", job_id, " ".join(cmd))
    env = os.environ.copy()
    env["HARVEST_JOB_ID"] = str(job_id)
    if postgres_dsn:
        env["POSTGRES_DSN"] = postgres_dsn
    env.update(params_to_env(task_type, params))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=None,
        stderr=None,
        env=env,
    )
    procs[job_id] = proc
    try:
        return int(await proc.wait())
    finally:
        procs.pop(job_id, None)


async def _release_job_session_leases(postgres_dsn: str, job_id: UUID) -> None:
    """Drop any session leases still listed on the job after the subprocess exits.

    Happy-path cancel lets the task CLI release in ``authorized_harvester_client``.
    This covers SIGKILL / hung Telethon disconnect where ``finally`` never ran.
    """
    from jobs.progress import session_leases_from_progress
    from telegram.lease import release_session_lease

    async with await connect_async_with_retry(postgres_dsn) as conn:
        row = await get_job(conn, job_id)
        leases = session_leases_from_progress((row or {}).get("progress"))
        if not leases:
            return
        finished_at = (row or {}).get("finished_at")
        lg.warning(
            "releasing %s session lease(s) for cancelled job %s",
            len(leases),
            job_id,
        )
        for session_id, server_id in leases:
            try:
                await release_session_lease(
                    conn,
                    session_id=session_id,
                    server_id=server_id,
                    joined_before=finished_at,
                )
            except Exception:
                lg.exception(
                    "failed to release session_id=%s for cancelled job %s",
                    session_id,
                    job_id,
                )
        await update_job_progress(
            conn,
            job_id=job_id,
            progress={"session_leases": []},
            merge=True,
        )
        await conn.commit()


async def _execute_job(
    cfg: AgentConfig,
    *,
    worker_id: UUID,
    job: dict[str, Any],
    procs: dict[UUID, asyncio.subprocess.Process],
) -> None:
    job_id: UUID = job["id"]
    task_type: str = job["task_type"]
    params: dict[str, Any] = dict(job.get("params") or {})

    async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
        await update_job_progress(
            conn,
            job_id=job_id,
            progress={"phase": "starting", "task_type": task_type},
            merge=True,
        )
        await conn.commit()

    try:
        if await _job_cancelled(cfg, job_id):
            await _release_job_session_leases(cfg.postgres_dsn, job_id)
            return
        code = await _run_task_subprocess(
            task_type=task_type,
            params=params,
            job_id=job_id,
            postgres_dsn=cfg.postgres_dsn,
            procs=procs,
        )
        if await _job_cancelled(cfg, job_id):
            await _release_job_session_leases(cfg.postgres_dsn, job_id)
            return
        async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
            if code == 0:
                await finish_job(
                    conn,
                    job_id=job_id,
                    status="succeeded",
                    progress={"phase": "done", "exit_code": 0},
                )
            else:
                error_msg = await _failure_message(conn, job_id=job_id, exit_code=code)
                await finish_job(
                    conn,
                    job_id=job_id,
                    status="failed",
                    error=error_msg,
                    progress={"phase": "failed", "exit_code": code},
                )
            await conn.commit()
    except Exception as exc:
        summary = str(exc).strip() or type(exc).__name__
        lg.error("job %s failed: %s", job_id, summary)
        if await _job_cancelled(cfg, job_id):
            await _release_job_session_leases(cfg.postgres_dsn, job_id)
            return
        async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
            await finish_job(
                conn,
                job_id=job_id,
                status="failed",
                error=summary[:2000],
                progress={"phase": "failed"},
            )
            await conn.commit()


async def _failure_message(conn: Any, *, job_id: UUID, exit_code: int) -> str:
    """Prefer last operational progress event over a bare exit code."""
    row = await get_job(conn, job_id)
    progress = (row or {}).get("progress") or {}
    last_event = progress.get("last_event")
    if isinstance(last_event, dict):
        msg = str(last_event.get("message") or "").strip()
        if msg:
            return msg[:2000]
    detail = progress.get("detail")
    if isinstance(detail, str) and detail.strip():
        detail_s = detail.strip()
        # Avoid tautologies like "finished seed … (exit 1)" without cause.
        if "exit" not in detail_s.lower():
            peers_failed = progress.get("peers_failed") or 0
            media_failed = progress.get("media_failed") or 0
            if peers_failed or media_failed:
                return (f"{detail_s} — {peers_failed} peer failure(s), {media_failed} media failure(s)")[:2000]
            return f"{detail_s} (exit {exit_code})"[:2000]
    peers_failed = progress.get("peers_failed")
    media_failed = progress.get("media_failed")
    if peers_failed or media_failed:
        return (
            f"Job failed with {peers_failed or 0} peer failure(s) and "
            f"{media_failed or 0} media failure(s) (exit {exit_code})"
        )[:2000]
    return f"task exited with code {exit_code}"


async def _job_cancelled(cfg: AgentConfig, job_id: UUID) -> bool:
    async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
        cancelled = await job_is_cancelled(conn, job_id)
        if cancelled:
            # Ensure terminal state if cancel raced mid-run.
            await cancel_job(conn, job_id)
            await conn.commit()
        return cancelled


async def _register(cfg: AgentConfig) -> dict[str, Any]:
    async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
        worker = await upsert_worker(
            conn,
            slug=cfg.worker_id,
            label=cfg.worker_label or cfg.worker_id,
            capabilities=cfg.worker_capabilities,
            max_concurrent_jobs=cfg.worker_max_concurrent_jobs,
            hostname=cfg.hostname,
            tailscale_ip=cfg.tailscale_ip,
            agent_version=cfg.agent_version,
            git_sha=cfg.git_sha,
            seed_max_concurrent=cfg.worker_seed_max_concurrent,
        )
        await conn.commit()
    return worker


async def run_loop(cfg: AgentConfig) -> None:
    cfg.validate_required()
    worker = await _register(cfg)
    worker_id: UUID = worker["id"]
    lg.info(
        "jobs agent online slug=%s id=%s max_concurrent=%s caps=%s",
        worker["slug"],
        worker_id,
        worker["max_concurrent_jobs"],
        worker["capabilities"],
    )

    running: dict[UUID, asyncio.Task[None]] = {}
    procs: dict[UUID, asyncio.subprocess.Process] = {}
    last_heartbeat = 0.0

    while True:
        claimed_any = False
        try:
            now = asyncio.get_running_loop().time()
            if now - last_heartbeat >= cfg.heartbeat_seconds:
                async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
                    await heartbeat_worker(conn, worker_id=worker_id)
                    await mark_stale_workers_offline(conn)
                    if cfg.reclaim_leases:
                        n = await reclaim_expired_leases(conn)
                        if n:
                            lg.warning("reclaimed %s expired job lease(s)", n)
                    # Refresh max_concurrent / capabilities from DB
                    from db.crud.system.jobs import get_worker

                    fresh = await get_worker(conn, worker_id)
                    await conn.commit()
                if fresh:
                    worker = fresh
                last_heartbeat = now

                # Renew leases for in-flight jobs; stop subprocesses when cancelled.
                for jid in list(running):
                    async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
                        ok = await renew_lease(
                            conn,
                            job_id=jid,
                            worker_id=worker_id,
                            lease_seconds=cfg.lease_seconds,
                        )
                        await conn.commit()
                    if not ok:
                        lg.warning("lease renew failed for job %s (cancelled?)", jid)
                        proc = procs.get(jid)
                        if proc is not None:
                            await _terminate_process(proc, job_id=jid)

            # Reap finished tasks
            done = [jid for jid, task in running.items() if task.done()]
            for jid in done:
                task = running.pop(jid)
                procs.pop(jid, None)
                try:
                    task.result()
                except Exception:
                    lg.exception("job task crashed %s", jid)

            slots = int(worker.get("max_concurrent_jobs") or 1) - len(running)
            for _ in range(max(0, slots)):
                async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
                    job = await claim_next_job(
                        conn,
                        worker_id=worker_id,
                        worker_slug=worker["slug"],
                        capabilities=worker.get("capabilities") or cfg.worker_capabilities,
                        lease_seconds=cfg.lease_seconds,
                    )
                    await conn.commit()
                if job is None:
                    break
                claimed_any = True
                jid = job["id"]
                lg.info("claimed job %s task=%s", jid, job["task_type"])
                running[jid] = asyncio.create_task(
                    _execute_job(cfg, worker_id=worker_id, job=job, procs=procs),
                    name=f"harvest-job-{jid}",
                )
        except (psycopg.OperationalError, psycopg.InterfaceError, OSError) as exc:
            lg.warning(
                "postgres unavailable after retries (%s); backing off %.1fs",
                exc,
                cfg.poll_idle_seconds,
            )
            await asyncio.sleep(cfg.poll_idle_seconds)
            continue

        await asyncio.sleep(0.5 if claimed_any else cfg.poll_idle_seconds)


def main() -> None:
    load_service_dotenv(__file__)
    parser = argparse.ArgumentParser(description="Harvest fleet jobs agent")
    parser.add_argument("--loop", action="store_true", help="run forever (required)")
    parser.add_argument(
        "--once",
        action="store_true",
        help="register + claim/run at most one job then exit",
    )
    args = parser.parse_args()
    cfg = ConfigFactory.get_config()
    configure_logging(level=cfg.log_level)

    if args.once:

        async def _once() -> None:
            cfg.validate_required()
            worker = await _register(cfg)
            worker_id: UUID = worker["id"]
            async with await connect_async_with_retry(cfg.postgres_dsn) as conn:
                if cfg.reclaim_leases:
                    await reclaim_expired_leases(conn)
                job = await claim_next_job(
                    conn,
                    worker_id=worker_id,
                    worker_slug=worker["slug"],
                    capabilities=worker.get("capabilities") or cfg.worker_capabilities,
                    lease_seconds=cfg.lease_seconds,
                )
                await conn.commit()
            if job is None:
                lg.info("no job to claim")
                return
            await _execute_job(cfg, worker_id=worker_id, job=job, procs={})

        asyncio.run(_once())
        return

    if not args.loop:
        parser.error("pass --loop (or --once)")
    asyncio.run(run_loop(cfg))


if __name__ == "__main__":
    main()
