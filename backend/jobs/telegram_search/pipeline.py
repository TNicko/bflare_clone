"""MTProto contacts.search across eligible harvesters.

Fleet runs drain ``telegram_search_ingest``. Each harvester claims one row at a
time. A query that already succeeded, or is still running, is deleted without
another search. Anything else is searched and then deleted. Passing ``queries``
keeps the in-memory path used by the search CLI.

For each community in a query result the worker materializes or refreshes it.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from db.connection import connect_async_tuple, rollback_quiet
from db.crud.telegram.search_ingest import (
    claim_search_ingest_query,
    count_search_ingest,
    delete_search_ingest,
    release_search_ingest,
)
from db.crud.telegram.search_query_run import (
    fetch_queries_to_skip,
    insert_search_query_run,
    mark_search_query_run_failed,
    mark_search_query_run_succeeded,
)
from db.schemas.telegram.harvester import TelegramHarvesterSchema
from jobs.progress import SharedProgress
from jobs.telegram_search.config import Config
from telegram.api import search_peers
from telegram.harvester_logging import HarvesterLoggerAdapter, format_harvester_prefix
from telegram.multi_harvester import (
    log_harvester_worker_failure,
    resolve_worker_harvesters,
    run_across_harvesters,
)
from telegram.search_materialize import TelegramMaterializeStats, materialize_search_peers

lg = logging.getLogger(__name__)

__all__ = [
    "TelegramMaterializeStats",
    "TelegramRunStats",
    "materialize_search_peers",
    "run_telegram_search",
]


class TelegramRunStats(BaseModel):
    model_config = ConfigDict(frozen=True)

    queries_processed: int = 0
    queries_skipped: int = 0
    queries_failed: int = 0
    peers_seen: int = 0
    peers_persisted: int = 0
    peers_skipped: int = 0
    peers_seeded: int = 0
    peers_seed_skipped: int = 0


def _merge_stats(*parts: TelegramRunStats) -> TelegramRunStats:
    return TelegramRunStats(
        queries_processed=sum(p.queries_processed for p in parts),
        queries_skipped=sum(p.queries_skipped for p in parts),
        queries_failed=sum(p.queries_failed for p in parts),
        peers_seen=sum(p.peers_seen for p in parts),
        peers_persisted=sum(p.peers_persisted for p in parts),
        peers_skipped=sum(p.peers_skipped for p in parts),
        peers_seeded=sum(p.peers_seeded for p in parts),
        peers_seed_skipped=sum(p.peers_seed_skipped for p in parts),
    )


def _partition_queries(
    queries: list[str],
    harvesters: list[TelegramHarvesterSchema],
) -> dict[str, list[str]]:
    """Round-robin queries across harvesters sorted by external_id."""
    ordered = sorted(harvesters, key=lambda h: h.external_id)
    buckets: dict[str, list[str]] = {h.external_id: [] for h in ordered}
    n = len(ordered)
    for i, query in enumerate(queries):
        buckets[ordered[i % n].external_id].append(query)
    return buckets


async def _record_failed_run(
    conn: Any,
    log: HarvesterLoggerAdapter,
    *,
    run_id: UUID | None,
    error: str,
    context: str,
) -> None:
    if run_id is None:
        return
    try:
        await mark_search_query_run_failed(conn, run_id, error)
    except Exception:
        log.exception("Failed marking search run failed (%s)", context)
        await rollback_quiet(conn, context=context)


async def _search_one_query(
    conn: Any,
    client: TelegramClient,
    harvester: TelegramHarvesterSchema,
    log: HarvesterLoggerAdapter,
    *,
    cfg: Config,
    query: str,
) -> TelegramRunStats:
    """Search one query and record the run. FloodWait is re-raised after the row is marked failed."""
    log.info("Telegram search query %r", query)
    run_id = None
    try:
        run_id = await insert_search_query_run(
            conn,
            query_text=query,
            harvester_id=harvester.id,
        )
        chats, users = await search_peers(client, query)
        result_count = len(chats) + len(users)
        mat_stats = await materialize_search_peers(
            client,
            conn,
            harvester_id=harvester.id,
            query=query,
            chats=chats,
            users=users,
            delay_seconds=cfg.telegram_delay_seconds,
            s3_bucket=cfg.s3_bucket,
            s3_region=cfg.aws_region,
        )
        await mark_search_query_run_succeeded(
            conn,
            run_id,
            result_count=result_count,
            peers_seen=mat_stats.peers_seen,
            peers_persisted=mat_stats.peers_persisted,
            peers_skipped=mat_stats.peers_skipped,
            peers_found=list(mat_stats.peers_found),
        )
        log.info(
            "Telegram query %r done | seen=%s persisted=%s skipped=%s",
            query,
            mat_stats.peers_seen,
            mat_stats.peers_persisted,
            mat_stats.peers_skipped,
        )
        return TelegramRunStats(
            queries_processed=1,
            peers_seen=mat_stats.peers_seen,
            peers_persisted=mat_stats.peers_persisted,
            peers_skipped=mat_stats.peers_skipped,
        )
    except FloodWaitError as exc:
        await rollback_quiet(conn, context=f"FloodWait query={query!r}")
        await _record_failed_run(
            conn,
            log,
            run_id=run_id,
            error=f"FloodWait: {exc.seconds}s",
            context="mark failed after FloodWait",
        )
        log.warning(
            "FloodWait for query %r: %s seconds — aborting this harvester worker",
            query,
            exc.seconds,
        )
        raise
    except Exception as exc:
        await rollback_quiet(conn, context=f"search query failed query={query!r}")
        await _record_failed_run(
            conn,
            log,
            run_id=run_id,
            error=str(exc),
            context="mark failed after query error",
        )
        log.exception("Telegram search failed for query %r: %s", query, exc)
        if run_id is None:
            raise
        return TelegramRunStats(queries_failed=1)


async def _run_harvester_queries(
    conn: Any,
    client: TelegramClient,
    harvester: TelegramHarvesterSchema,
    log: HarvesterLoggerAdapter,
    *,
    cfg: Config,
    queries: list[str],
) -> TelegramRunStats:
    if not queries:
        log.info("No queries assigned to this harvester")
        return TelegramRunStats()

    log.info("Assigned %s query(ies)", len(queries))
    parts: list[TelegramRunStats] = []
    for query in queries:
        try:
            parts.append(
                await _search_one_query(
                    conn,
                    client,
                    harvester,
                    log,
                    cfg=cfg,
                    query=query,
                )
            )
        except FloodWaitError:
            return _merge_stats(*parts, TelegramRunStats(queries_failed=1))
    return _merge_stats(*parts)


async def _note_query_progress(
    progress: SharedProgress | None,
    stats: TelegramRunStats,
    *,
    query: str,
) -> None:
    if progress is None:
        return
    done = stats.queries_processed + stats.queries_skipped + stats.queries_failed
    await progress.bump(
        queries_done=done,
        peers_seen=stats.peers_seen,
        peers_done=stats.peers_persisted,
        peers_skipped=stats.peers_skipped,
        peers_failed=stats.queries_failed,
    )
    await progress.set(detail=query)


async def _drain_harvester_ingest(
    conn: Any,
    client: TelegramClient,
    harvester: TelegramHarvesterSchema,
    log: HarvesterLoggerAdapter,
    *,
    cfg: Config,
    progress: SharedProgress | None,
) -> TelegramRunStats:
    parts: list[TelegramRunStats] = []
    while True:
        claimed = await claim_search_ingest_query(conn)
        if claimed is None:
            log.info("telegram_search_ingest is empty")
            return _merge_stats(*parts)

        ingest_id = claimed["id"]
        if not isinstance(ingest_id, UUID):
            raise RuntimeError("telegram_search_ingest claim did not return a uuid id")
        query = str(claimed["query_text"])
        try:
            skip = await fetch_queries_to_skip(conn, [query])
            await conn.commit()
            if query in skip:
                await delete_search_ingest(conn, ingest_id)
                log.info("Skipping Telegram search query %r (already succeeded or running)", query)
                outcome = TelegramRunStats(queries_skipped=1)
            else:
                outcome = await _search_one_query(
                    conn,
                    client,
                    harvester,
                    log,
                    cfg=cfg,
                    query=query,
                )
                await delete_search_ingest(conn, ingest_id)
        except FloodWaitError:
            await release_search_ingest(conn, ingest_id)
            outcome = TelegramRunStats(queries_failed=1)
            parts.append(outcome)
            await _note_query_progress(progress, outcome, query=query)
            return _merge_stats(*parts)
        except Exception:
            log.exception("Failed draining telegram_search_ingest query %r", query)
            await rollback_quiet(conn, context=f"ingest query {query!r}")
            # Leave claimed_at set so this worker does not immediately take the same row.
            # The claim goes stale after two hours and another run can retry it.
            outcome = TelegramRunStats(queries_failed=1)
            parts.append(outcome)
            await _note_query_progress(progress, outcome, query=query)
            return _merge_stats(*parts)

        parts.append(outcome)
        await _note_query_progress(progress, outcome, query=query)


def _resolve_harvester_pin(
    cfg: Config,
    harvester_external_id: str | None,
) -> str | None:
    pin = harvester_external_id
    if pin is None or not str(pin).strip():
        pin = cfg.harvester_external_id.strip() or None
    return pin


async def _load_harvesters(
    cfg: Config,
    *,
    harvester_external_id: str | None,
    harvester_external_ids: Sequence[str] | None,
) -> list[TelegramHarvesterSchema]:
    async with await connect_async_tuple(cfg.postgres_dsn) as conn:
        harvesters = await resolve_worker_harvesters(
            conn,
            harvester_external_id=harvester_external_id,
            harvester_external_ids=harvester_external_ids,
        )
        await conn.commit()
    return harvesters


def _finish_workers(
    harvesters: list[TelegramHarvesterSchema],
    results: list[TelegramRunStats | BaseException],
    *,
    preface: TelegramRunStats | None = None,
) -> TelegramRunStats:
    parts: list[TelegramRunStats] = []
    if preface is not None:
        parts.append(preface)
    for harvester, result in zip(harvesters, results, strict=True):
        if isinstance(result, BaseException):
            log_harvester_worker_failure(lg, harvester, result)
            parts.append(TelegramRunStats(queries_failed=1))
            continue
        parts.append(result)
        lg.info(
            "Worker finished %s | ok=%s skipped=%s failed=%s | peers seen=%s persisted=%s skipped=%s",
            format_harvester_prefix(harvester),
            result.queries_processed,
            result.queries_skipped,
            result.queries_failed,
            result.peers_seen,
            result.peers_persisted,
            result.peers_skipped,
        )
    stats = _merge_stats(*parts)
    lg.info(
        "Telegram search finished | queries ok=%s skipped=%s failed=%s | peers seen=%s "
        "persisted=%s skipped=%s",
        stats.queries_processed,
        stats.queries_skipped,
        stats.queries_failed,
        stats.peers_seen,
        stats.peers_persisted,
        stats.peers_skipped,
    )
    return stats


async def _run_ingest(
    *,
    cfg: Config,
    harvesters: list[TelegramHarvesterSchema],
) -> TelegramRunStats:
    async with await connect_async_tuple(cfg.postgres_dsn) as conn:
        queries_total = await count_search_ingest(conn)
        await conn.commit()

    progress = SharedProgress(
        phase="running",
        task_type="telegram_search",
        queries_total=queries_total,
        queries_done=0,
        detail=f"0/{queries_total} queries",
    )
    await progress.snapshot()
    if queries_total == 0:
        lg.info("telegram_search_ingest is empty")
        await progress.snapshot(phase="finishing", detail="ingest empty")
        return TelegramRunStats()

    lg.info(
        "Running telegram search across %s harvester(s): %s | ingest_rows=%s",
        len(harvesters),
        ", ".join(format_harvester_prefix(h, color=False) for h in harvesters),
        queries_total,
    )

    async def _worker(
        conn: Any,
        client: TelegramClient,
        harvester: TelegramHarvesterSchema,
        log: HarvesterLoggerAdapter,
    ) -> TelegramRunStats:
        return await _drain_harvester_ingest(
            conn,
            client,
            harvester,
            log,
            cfg=cfg,
            progress=progress,
        )

    results = await run_across_harvesters(
        postgres_dsn=cfg.postgres_dsn,
        telegram_api_id=cfg.telegram_api_id,
        telegram_api_hash=cfg.telegram_api_hash,
        service_name="telegram_search",
        harvesters=harvesters,
        worker=_worker,
        base_logger=lg,
    )
    stats = _finish_workers(harvesters, results)
    await progress.snapshot(
        phase="finishing",
        queries_done=stats.queries_processed + stats.queries_skipped + stats.queries_failed,
        peers_seen=stats.peers_seen,
        peers_done=stats.peers_persisted,
        peers_skipped=stats.peers_skipped,
        peers_failed=stats.queries_failed,
        detail=f"done · {stats.peers_persisted} peers",
    )
    return stats


async def _run_query_list(
    queries: list[str],
    *,
    cfg: Config,
    harvesters: list[TelegramHarvesterSchema],
) -> TelegramRunStats:
    async with await connect_async_tuple(cfg.postgres_dsn) as conn:
        skip_queries = await fetch_queries_to_skip(conn, queries)
        await conn.commit()

    queries_skipped = sum(1 for q in queries if q in skip_queries)
    remaining = [q for q in queries if q not in skip_queries]
    for q in queries:
        if q in skip_queries:
            lg.info("Skipping Telegram search query %r (already succeeded or running)", q)

    if not remaining:
        return _finish_workers(
            [],
            [],
            preface=TelegramRunStats(queries_skipped=queries_skipped),
        )

    partitions = _partition_queries(remaining, harvesters)
    lg.info(
        "Running telegram search across %s harvester(s): %s | remaining_queries=%s skipped=%s",
        len(harvesters),
        ", ".join(format_harvester_prefix(h, color=False) for h in harvesters),
        len(remaining),
        queries_skipped,
    )

    async def _worker(
        conn: Any,
        client: TelegramClient,
        harvester: TelegramHarvesterSchema,
        log: HarvesterLoggerAdapter,
    ) -> TelegramRunStats:
        return await _run_harvester_queries(
            conn,
            client,
            harvester,
            log,
            cfg=cfg,
            queries=partitions.get(harvester.external_id, []),
        )

    results = await run_across_harvesters(
        postgres_dsn=cfg.postgres_dsn,
        telegram_api_id=cfg.telegram_api_id,
        telegram_api_hash=cfg.telegram_api_hash,
        service_name="telegram_search",
        harvesters=harvesters,
        worker=_worker,
        base_logger=lg,
    )
    return _finish_workers(
        harvesters,
        results,
        preface=TelegramRunStats(queries_skipped=queries_skipped),
    )


async def run_telegram_search(
    queries: list[str] | None = None,
    *,
    cfg: Config,
    harvester_external_id: str | None = None,
    harvester_external_ids: Sequence[str] | None = None,
) -> TelegramRunStats:
    """Search Telegram.

    ``queries is None`` drains ``telegram_search_ingest``. A list runs those
    strings in memory and does not touch the ingest table. Each community from
    a result is materialized before the next.
    """
    cfg.validate_telegram_required()
    if not cfg.postgres_dsn.strip():
        raise ValueError("POSTGRES_DSN is required for telegram search")

    pin = _resolve_harvester_pin(cfg, harvester_external_id)
    harvesters = await _load_harvesters(
        cfg,
        harvester_external_id=pin,
        harvester_external_ids=harvester_external_ids,
    )
    if queries is None:
        return await _run_ingest(cfg=cfg, harvesters=harvesters)
    return await _run_query_list(queries, cfg=cfg, harvesters=harvesters)
