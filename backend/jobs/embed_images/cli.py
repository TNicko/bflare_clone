from __future__ import annotations

import argparse
import asyncio
import logging

from utils.logger import configure_logging
from jobs.embed_images.config import ConfigFactory
from jobs.embed_images.process_blobs import embed_image_blobs
from jobs.progress import SharedProgress
from utils.graceful_stop import install_graceful_stop


async def _main() -> int:
    try:
        cfg = ConfigFactory.get_config()
        cfg.validate_required()
    except ValueError as exc:
        raise SystemExit(exc) from exc

    configure_logging(level=cfg.log_level)
    log = logging.getLogger(__name__)
    stop = install_graceful_stop(name="embed_images")

    try:
        from db.connection import connect_async_tuple
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("psycopg is required; pip install -e '.[embed-images]'") from exc

    async with await connect_async_tuple(cfg.postgres_dsn) as conn:
        log.info(
            "Embedding unique image blobs model=%s dim=%s (Ctrl+C finishes current batch, then exits)",
            cfg.embedding_model,
            cfg.vector_dim,
        )
        progress = SharedProgress(
            phase="running",
            task_type="embed_images",
        )
        await progress.snapshot()
        stats = await embed_image_blobs(conn, cfg=cfg, stop=stop, progress=progress)
        await progress.snapshot(
            phase="finishing",
            messages_scanned=stats.scanned,
            embedded=stats.embedded,
            skipped=stats.skipped,
            failed=stats.failed,
            detail="finished",
        )
        log.info(
            "Done: scanned=%s embedded=%s skipped=%s failed=%s",
            stats.scanned,
            stats.embedded,
            stats.skipped,
            stats.failed,
        )
        if stop.requested:
            log.info("Exited after graceful stop; re-run to resume from blob embed flags")
            return 130

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed unique harvested images by exact pHash (blob-first)")
    parser.add_argument(
        "--model",
        default=None,
        help="siglip / mobilenet (or the embstore slug). Default: IMAGE_EMBEDDING_MODEL or siglip.",
    )
    args = parser.parse_args()
    if args.model:
        from utils.env import override_service_env

        override_service_env("IMAGE_EMBEDDING_MODEL", args.model)
        ConfigFactory._config = None
    raise SystemExit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
