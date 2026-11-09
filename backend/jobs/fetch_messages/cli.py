from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Sequence
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from db.connection import connect_async_tuple
from db.crud.telegram.worker_session import worker_session_kwargs
from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.harvester import TelegramHarvesterSchema
from jobs.fetch_messages.config import Config, ConfigFactory
from jobs.fetch_messages.db import (
    PeerAssignmentRow,
    PeerSelectionRow,
    list_peer_assignments,
    list_peer_assignments_for_categories,
    list_peer_assignments_for_community_id,
    list_peer_assignments_for_external_id,
    list_peer_assignments_for_name_query,
    list_peer_assignments_pending,
    list_unassigned_peer_selections,
    peer_selection_skip_reason,
    to_peer_assignment,
)
from jobs.fetch_messages.fetch import (
    DebugCapture,
    FetchStats,
    fetch_messages_for_peers,
    merge_fetch_stats,
    write_debug_messages,
)
from jobs.fetch_messages.resolve import resolve_peer_harvester_role
from jobs.progress import SharedProgress
from telegram.client import authorized_harvester_client
from telegram.harvester_logging import HarvesterLoggerAdapter, format_harvester_prefix
from telegram.multi_harvester import (
    log_harvester_worker_failure,
    resolve_worker_harvesters,
    run_across_harvesters,
    worker_harvester_ids,
)
from utils.logger import configure_logging

logger = logging.getLogger(__name__)

PEER_TYPES: list[TelegramPeerTypeSchema] = ["channel", "megagroup", "chat"]


def _log_run_stats(log: logging.Logger, stats: FetchStats) -> None:
    log.info(
        "Fetch run finished | peers=%s processed=%s skipped_complete=%s "
        "skipped_unassigned=%s resolved=%s failed=%s | messages=%s | "
        "reaction_lists=%s reactors=%s reactor_users=%s reaction_errors=%s",
        stats.peers_seen,
        stats.peers_processed,
        stats.peers_skipped_complete,
        stats.peers_skipped_unassigned,
        stats.peers_resolved,
        stats.peers_failed,
        stats.messages_persisted,
        stats.reaction_lists_fetched,
        stats.reactors_persisted,
        stats.reactor_users_upserted,
        stats.reaction_list_errors,
    )


def _selection_mode(cfg: Config) -> str:
    if cfg.peer_external_id:
        return "external_id"
    if cfg.community_id:
        return "community_id"
    if cfg.category_slugs:
        return "categories"
    if cfg.peer_name_query:
        return "name"
    if cfg.fetch_pending_only:
        return "pending"
    return "all"


async def _list_assigned_for_assignee(
    conn: Any,
    *,
    cfg: Config,
    assignee_harvester_id: UUID,
    worker_ids: Sequence[UUID],
) -> list[PeerAssignmentRow]:
    mode = _selection_mode(cfg)
    if mode == "external_id":
        assert cfg.peer_external_id is not None
        rows = await list_peer_assignments_for_external_id(
            conn,
            peer_external_id=cfg.peer_external_id,
            assignee_harvester_id=assignee_harvester_id,
            worker_harvester_ids=worker_ids,
        )
    elif mode == "community_id":
        assert cfg.community_id is not None
        rows = await list_peer_assignments_for_community_id(
            conn,
            community_id=UUID(cfg.community_id),
            assignee_harvester_id=assignee_harvester_id,
            worker_harvester_ids=worker_ids,
        )
    elif mode == "categories":
        rows = await list_peer_assignments_for_categories(
            conn,
            category_slugs=cfg.category_slugs,
            assignee_harvester_id=assignee_harvester_id,
            worker_harvester_ids=worker_ids,
        )
    elif mode == "name":
        assert cfg.peer_name_query is not None
        rows = await list_peer_assignments_for_name_query(
            conn,
            peer_name_query=cfg.peer_name_query,
            assignee_harvester_id=assignee_harvester_id,
            worker_harvester_ids=worker_ids,
        )
    elif mode == "pending":
        rows = await list_peer_assignments_pending(
            conn,
            peer_types=PEER_TYPES,
            window_after=cfg.telegram_fetch_after,
            window_before=cfg.telegram_fetch_before,
            catchup_slack_minutes=cfg.catchup_slack_minutes,
            assignee_harvester_id=assignee_harvester_id,
            worker_harvester_ids=worker_ids,
            limit=cfg.peer_batch_size,
        )
    else:
        rows = await list_peer_assignments(
            conn,
            peer_types=PEER_TYPES,
            assignee_harvester_id=assignee_harvester_id,
            worker_harvester_ids=worker_ids,
            limit=cfg.peer_batch_size,
        )

    out: list[PeerAssignmentRow] = []
    for row in rows:
        reason = peer_selection_skip_reason(row)
        if reason is not None:
            continue
        out.append(to_peer_assignment(row))
    return out


async def _collect_unassigned_for_bootstrap(
    conn: Any,
    *,
    cfg: Config,
) -> list[tuple[PeerSelectionRow, str]]:
    """Peers that need role resolve. Pending mode has no unassigned path."""
    mode = _selection_mode(cfg)
    if mode == "pending":
        return []

    pending: list[tuple[PeerSelectionRow, str]] = []
    seen: set[str] = set()

    if mode == "external_id":
        assert cfg.peer_external_id is not None
        rows = await list_peer_assignments_for_external_id(conn, peer_external_id=cfg.peer_external_id)
    elif mode == "community_id":
        assert cfg.community_id is not None
        rows = await list_peer_assignments_for_community_id(conn, community_id=UUID(cfg.community_id))
    elif mode == "categories":
        rows = await list_peer_assignments_for_categories(conn, category_slugs=cfg.category_slugs)
    elif mode == "name":
        assert cfg.peer_name_query is not None
        rows = await list_peer_assignments_for_name_query(conn, peer_name_query=cfg.peer_name_query)
    else:
        rows = await list_unassigned_peer_selections(conn, peer_types=PEER_TYPES, limit=cfg.peer_batch_size)

    for row in rows:
        reason = peer_selection_skip_reason(row)
        if reason is None:
            continue
        if row.peer_external_id in seen:
            continue
        seen.add(row.peer_external_id)
        pending.append((row, reason))
    return pending


async def _resolve_unassigned_peers(
    conn: Any,
    *,
    cfg: Config,
    resolve_harvester: TelegramHarvesterSchema,
    pending: list[tuple[PeerSelectionRow, str]],
    lg: logging.Logger,
) -> FetchStats:
    if not pending:
        return FetchStats()

    await conn.commit()
    run_stats = FetchStats()

    async with authorized_harvester_client(
        conn,
        telegram_api_id=cfg.telegram_api_id,
        telegram_api_hash=cfg.telegram_api_hash,
        harvester_external_id=resolve_harvester.external_id,
        **worker_session_kwargs(service_name="fetch_messages"),
    ) as (client, leased):
        lg.info(
            "Bootstrap resolve with harvester %s for %s unassigned peer(s)",
            format_harvester_prefix(leased, color=False),
            len(pending),
        )
        for row, reason in pending:
            lg.warning(
                "Peer %s (%s) does not have a telegram harvester peer role (%s). "
                "Attempting to resolve peer with telegram ...",
                row.peer_external_id,
                row.peer_type,
                reason,
            )
            try:
                resolved = await resolve_peer_harvester_role(
                    client,
                    conn,
                    row=row,
                    harvester_id=leased.id,
                    harvester_external_id=leased.external_id,
                )
            except FloodWaitError as exc:
                lg.warning(
                    "FloodWait resolving peer %s: %s seconds — stopping resolve bootstrap",
                    row.peer_external_id,
                    exc.seconds,
                )
                break

            if resolved is None:
                run_stats = replace(
                    run_stats,
                    peers_skipped_unassigned=run_stats.peers_skipped_unassigned + 1,
                )
                continue

            run_stats = replace(run_stats, peers_resolved=run_stats.peers_resolved + 1)
            if cfg.delay_seconds > 0:
                await asyncio.sleep(cfg.delay_seconds)

    return run_stats


async def _run_embed_for_peers(
    conn: Any,
    *,
    by_harvester: dict[tuple[str, UUID], list[PeerAssignmentRow]],
    fetch_cfg: Config,
    lg: logging.Logger,
) -> None:
    from jobs.embed_messages.config import ConfigFactory as EmbedConfigFactory
    from jobs.embed_messages.process import embed_messages_for_peers

    embed_cfg = EmbedConfigFactory.get_config()
    embed_cfg.validate_required()
    embed_cfg = embed_cfg.model_copy(
        update={
            "telegram_fetch_before": fetch_cfg.telegram_fetch_before,
            "telegram_fetch_after": fetch_cfg.telegram_fetch_after,
            "db_batch_size": fetch_cfg.db_batch_size,
            "delay_seconds": fetch_cfg.delay_seconds,
        }
    )

    for (harvester_external_id, harvester_id), peers in sorted(by_harvester.items(), key=lambda kv: kv[0][0]):
        lg.info(
            "Text embed for harvester peer_id=%s (%s peer(s))",
            harvester_external_id,
            len(peers),
        )
        stats = await embed_messages_for_peers(
            conn,
            harvester_id=harvester_id,
            peers=peers,
            cfg=embed_cfg,
        )
        lg.info(
            "Text embed harvester %s: scanned=%s embedded=%s failed=%s",
            harvester_external_id,
            stats.messages_scanned,
            stats.embedded,
            stats.failed,
        )


class WorkerResult:
    __slots__ = ("stats", "peers")

    def __init__(self, stats: FetchStats, peers: list[PeerAssignmentRow]) -> None:
        self.stats = stats
        self.peers = peers


async def _harvester_worker(
    conn: Any,
    client: TelegramClient,
    harvester: TelegramHarvesterSchema,
    log: HarvesterLoggerAdapter,
    *,
    cfg: Config,
    worker_ids: Sequence[UUID],
    debug_capture: DebugCapture | None,
    progress: SharedProgress | None = None,
) -> WorkerResult:
    peers = await _list_assigned_for_assignee(
        conn,
        cfg=cfg,
        assignee_harvester_id=harvester.id,
        worker_ids=worker_ids,
    )
    await conn.commit()

    if not peers:
        log.info("No peers assigned to this harvester for current selection")
        return WorkerResult(FetchStats(), [])

    log.info("Fetching messages for %s peer(s)", len(peers))
    stats = await fetch_messages_for_peers(
        client,
        conn,
        harvester_id=harvester.id,
        harvester_external_id=harvester.external_id,
        peers=peers,
        window_before=cfg.telegram_fetch_before,
        window_after=cfg.telegram_fetch_after,
        db_batch_size=cfg.db_batch_size,
        delay_seconds=cfg.delay_seconds,
        debug_capture=debug_capture,
        progress=progress,
    )
    return WorkerResult(stats, peers)


async def _main(
    *,
    debug: bool,
    debug_output: str,
    embed: bool,
    harvester_external_id: str | None,
) -> int:
    cfg = ConfigFactory.get_config()
    pin = (harvester_external_id or cfg.harvester_external_id or "").strip() or None
    # DebugCapture is not concurrency-safe; force a single harvester.
    if debug and pin is None:
        logger.info("--debug forces single-harvester mode (will pin to first eligible)")

    configure_logging(level=cfg.log_level)
    lg = logging.getLogger(__name__)

    async with await connect_async_tuple(cfg.postgres_dsn) as conn:
        harvesters = await resolve_worker_harvesters(
            conn,
            harvester_external_id=pin,
        )
        await conn.commit()

        if debug and len(harvesters) > 1:
            harvesters = harvesters[:1]
            lg.info(
                "--debug: pinned to single harvester %s",
                format_harvester_prefix(harvesters[0], color=False),
            )

        worker_ids = worker_harvester_ids(harvesters)
        mode = _selection_mode(cfg)
        lg.info(
            "Running fetch_messages mode=%s across %s harvester(s): %s",
            mode,
            len(harvesters),
            ", ".join(format_harvester_prefix(h, color=False) for h in harvesters),
        )

        bootstrap_stats = FetchStats()
        unassigned = await _collect_unassigned_for_bootstrap(conn, cfg=cfg)
        if unassigned:
            bootstrap_stats = await _resolve_unassigned_peers(
                conn,
                cfg=cfg,
                resolve_harvester=harvesters[0],
                pending=unassigned,
                lg=lg,
            )

    debug_capture = DebugCapture() if debug else None
    open_ended = not bool(
        (cfg.peer_external_id or "").strip() or (cfg.community_id or "").strip() or (cfg.peer_name_query or "").strip()
    )
    progress = SharedProgress(
        phase="running",
        task_type="fetch_messages",
        open_ended=open_ended,
        finished_peers=[],
    )
    await progress.snapshot()

    async def _worker(
        conn: Any,
        client: TelegramClient,
        harvester: TelegramHarvesterSchema,
        log: HarvesterLoggerAdapter,
    ) -> WorkerResult:
        return await _harvester_worker(
            conn,
            client,
            harvester,
            log,
            cfg=cfg,
            worker_ids=worker_ids,
            debug_capture=debug_capture,
            progress=progress,
        )

    results = await run_across_harvesters(
        postgres_dsn=cfg.postgres_dsn,
        telegram_api_id=cfg.telegram_api_id,
        telegram_api_hash=cfg.telegram_api_hash,
        service_name="fetch_messages",
        harvesters=harvesters,
        worker=_worker,
        base_logger=logger,
    )

    parts: list[FetchStats] = [bootstrap_stats]
    by_harvester: dict[tuple[str, UUID], list[PeerAssignmentRow]] = {}
    for harvester, result in zip(harvesters, results, strict=True):
        if isinstance(result, BaseException):
            log_harvester_worker_failure(lg, harvester, result)
            parts.append(FetchStats(peers_failed=1))
            continue
        parts.append(result.stats)
        if result.peers:
            key = (harvester.external_id, harvester.id)
            by_harvester[key] = result.peers
        lg.info(
            "Worker finished %s | seen=%s processed=%s failed=%s messages=%s",
            format_harvester_prefix(harvester),
            result.stats.peers_seen,
            result.stats.peers_processed,
            result.stats.peers_failed,
            result.stats.messages_persisted,
        )

    run_stats = merge_fetch_stats(FetchStats(), *parts)

    if embed and by_harvester:
        async with await connect_async_tuple(cfg.postgres_dsn) as conn:
            await _run_embed_for_peers(
                conn,
                by_harvester=by_harvester,
                fetch_cfg=cfg,
                lg=lg,
            )

    await progress.snapshot(
        phase="finishing",
        peers_seen=run_stats.peers_seen,
        peers_done=(run_stats.peers_processed + run_stats.peers_skipped_complete + run_stats.peers_failed),
        peers_failed=run_stats.peers_failed,
        peers_skipped=run_stats.peers_skipped_complete,
        messages_persisted=run_stats.messages_persisted,
        messages_fetched=run_stats.messages_persisted,
        detail="finished",
    )
    _log_run_stats(lg, run_stats)

    if debug_capture is not None and debug_capture.messages:
        write_debug_messages(Path(debug_output), debug_capture.messages)

    return 1 if run_stats.peers_failed else 0


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Fetch Telegram messages into Postgres. Runs concurrently across all "
            "eligible harvesters by default (capacity-aware peer assignment)."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Fetch only the first 10 messages in the run and write raw Telethon "
            "message objects to a JSON file (forces single-harvester)"
        ),
    )
    parser.add_argument(
        "--debug-output",
        default="fetch_messages_debug.json",
        type=Path,
        help="Output path for --debug JSON (default: fetch_messages_debug.json)",
    )
    parser.add_argument(
        "--embed",
        action="store_true",
        help=(
            "After fetch (or for already-fetched messages), embed message text via "
            "EMBEDDING_SERVICE_URL into the embedding store (S3/local shards)"
        ),
    )
    parser.add_argument(
        "--harvester-external-id",
        default=None,
        help=("Only run this harvester (Telegram user id). Default: all eligible active harvesters concurrently"),
    )
    args = parser.parse_args()
    raise SystemExit(
        asyncio.run(
            _main(
                debug=args.debug,
                debug_output=str(args.debug_output),
                embed=args.embed,
                harvester_external_id=args.harvester_external_id,
            )
        )
    )


if __name__ == "__main__":
    main()
