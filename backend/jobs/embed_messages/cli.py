from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from db.schemas.general.primitives import TelegramPeerTypeSchema
from utils.logger import configure_logging
from jobs.embed_messages.config import ConfigFactory
from jobs.embed_messages.db import (
    EmbedPeerRow,
    list_peers_for_categories,
    list_peers_for_community_id,
    list_peers_for_external_id,
    list_peers_for_name_query,
    list_peers_pending_embed,
    list_peers_with_fetch_coverage,
)
from jobs.embed_messages.process import embed_messages_for_peers
from jobs.progress import SharedProgress
from utils.graceful_stop import install_graceful_stop


# Broadcast channels only. Megagroups/chats need a different embed strategy later.
_EMBED_PEER_TYPES: list[TelegramPeerTypeSchema] = ["channel"]


async def _resolve_peers(conn, cfg) -> list[EmbedPeerRow]:
    log = logging.getLogger(__name__)

    if cfg.peer_external_id:
        log.info("Peer selection: external_id=%s", cfg.peer_external_id)
        selections = await list_peers_for_external_id(conn, peer_external_id=cfg.peer_external_id)
    elif cfg.community_id:
        log.info("Peer selection: community_id=%s", cfg.community_id)
        selections = await list_peers_for_community_id(conn, community_id=UUID(cfg.community_id))
    elif cfg.peer_name_query:
        log.info("Peer selection: peer_name_query=%r", cfg.peer_name_query)
        selections = await list_peers_for_name_query(conn, peer_name_query=cfg.peer_name_query)
    elif cfg.category_slugs:
        log.info("Peer selection: category_slugs=%s", cfg.category_slugs)
        selections = await list_peers_for_categories(conn, category_slugs=cfg.category_slugs)
    elif cfg.embed_pending_only:
        log.info(
            "Peer selection: pending embed work (batch_size=%s, window_after=%s, window_before=%s)",
            cfg.peer_batch_size,
            cfg.telegram_fetch_after,
            cfg.telegram_fetch_before,
        )
        selections = await list_peers_pending_embed(
            conn,
            peer_types=_EMBED_PEER_TYPES,
            window_after=cfg.telegram_fetch_after,
            window_before=cfg.telegram_fetch_before,
            limit=cfg.peer_batch_size,
        )
    else:
        log.info(
            "Peer selection: all peers with fetch coverage (batch_size=%s)",
            cfg.peer_batch_size,
        )
        selections = await list_peers_with_fetch_coverage(
            conn,
            peer_types=_EMBED_PEER_TYPES,
            limit=cfg.peer_batch_size,
        )

    peers: list[EmbedPeerRow] = []
    for row in selections:
        if row.peer_type not in _EMBED_PEER_TYPES:
            log.warning(
                "Skipping peer %s (%s): embed_messages is channel-only for now",
                row.peer_external_id,
                row.peer_type,
            )
            continue
        peers.append(row)
    return peers


async def _main() -> int:
    try:
        cfg = ConfigFactory.get_config()
        cfg.validate_required()
    except ValueError as exc:
        raise SystemExit(exc) from exc

    configure_logging(level=cfg.log_level)
    log = logging.getLogger(__name__)
    stop = install_graceful_stop(name="embed_messages")

    try:
        from db.connection import connect_async_tuple
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("psycopg is required; pip install -e '.[embed-messages]'") from exc

    async with await connect_async_tuple(cfg.postgres_dsn) as conn:
        peers = await _resolve_peers(conn, cfg)
        if not peers:
            log.info("No peers matched selection")
            return 0

        await conn.commit()

        log.info(
            "Embedding text for %s peer(s) (Ctrl+C finishes current page, then exits)",
            len(peers),
        )
        progress = SharedProgress(
            phase="running",
            task_type="embed_messages",
            peers_total=len(peers),
        )
        await progress.snapshot()
        stats = await embed_messages_for_peers(conn, peers=peers, cfg=cfg, stop=stop, progress=progress)
        await progress.snapshot(
            phase="finishing",
            peers_seen=stats.peers_seen,
            peers_done=stats.peers_seen,
            messages_scanned=stats.messages_scanned,
            embedded=stats.embedded,
            skipped=stats.skipped,
            failed=stats.failed,
            detail="finished",
        )
        log.info(
            "Done: with_work=%s complete=%s scanned=%s embedded=%s failed=%s",
            stats.peers_with_work,
            stats.peers_complete,
            stats.messages_scanned,
            stats.embedded,
            stats.failed,
        )
        if stop.requested:
            log.info("Exited after graceful stop; re-run to resume from cursors")
            return 130

    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
