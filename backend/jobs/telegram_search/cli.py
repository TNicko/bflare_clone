"""CLI: python -m jobs.telegram_search.cli [--harvester-external-id ...]"""

from __future__ import annotations

import argparse
import asyncio
import logging

from jobs.harvester_ids import normalize_harvester_external_ids
from jobs.telegram_search.config import ConfigFactory
from jobs.telegram_search.pipeline import TelegramRunStats, run_telegram_search
from utils.logger import configure_logging


def _log_telegram_stats(stats: TelegramRunStats) -> None:
    logging.getLogger(__name__).info(
        "Telegram run finished | queries ok=%s skipped=%s failed=%s | peers seen=%s persisted=%s skipped=%s",
        stats.queries_processed,
        stats.queries_skipped,
        stats.queries_failed,
        stats.peers_seen,
        stats.peers_persisted,
        stats.peers_skipped,
    )


async def _main(args: argparse.Namespace) -> int:
    try:
        cfg = ConfigFactory.get_config()
        cfg.validate_required()
    except ValueError as exc:
        raise SystemExit(exc) from exc

    configure_logging(level=cfg.log_level)
    lg = logging.getLogger(__name__)
    selected = normalize_harvester_external_ids(args.harvester_external_ids)
    lg.info(
        "Starting telegram search from telegram_search_ingest harvesters=%s",
        ", ".join(selected) if selected else "all eligible",
    )
    stats = await run_telegram_search(
        cfg=cfg,
        harvester_external_ids=selected or None,
    )
    _log_telegram_stats(stats)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Drain telegram_search_ingest with MTProto contacts.search. "
            "Selected harvesters run concurrently in this process. "
            "Default: all eligible harvesters."
        ),
    )
    parser.add_argument(
        "--harvester-external-id",
        action="append",
        default=None,
        dest="harvester_external_ids",
        help=(
            "Telegram user id to include. Repeat for several accounts in this process. "
            "Default: all eligible active harvesters."
        ),
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
