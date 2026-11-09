"""Blob-first image embed: one GPU encode per exact pHash.

Walks ``telegram_image_blobs`` missing ``image_embedded[model]``, downloads the
canonical S3 object once, writes embstore rows keyed by phash, and marks the
blob done. Message fan-out for search uses ``telegram_image_blob_messages``.

Downloads for page N+1 are overlapped with embedding page N so the GPU stays
fed while S3 transfers continue.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aws.s3 import download_s3_object_to_path, parse_s3_uri, s3_client
from db.crud.telegram.image_blobs import list_blobs_needing_embed, mark_blob_embedded
from embedding.image_client import embed_images_bytes
from embstore import ImageEmbeddingShardWriter, LocalSink, S3Sink, Sink
from embstore.layout import month_from_shard_key
from jobs.embed_images.config import Config
from jobs.embed_images.db import delete_micro_shard_catalog, register_micro_shards
from jobs.progress import SharedProgress
from utils.graceful_stop import GracefulStop

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BlobEmbedStats:
    scanned: int = 0
    embedded: int = 0
    skipped: int = 0
    failed: int = 0


@dataclass(slots=True)
class _DownloadedPage:
    encode_payloads: list[bytes]
    encode_meta: list[dict[str, Any]]
    scanned: int
    skipped: int
    failed_downloads: int
    after_phash: str | None
    tmp_root: Path


def _build_sink(cfg: Config) -> Sink:
    if cfg.store_backend == "local":
        return LocalSink(cfg.local_dir)
    return S3Sink(
        cfg.s3_bucket,
        region=cfg.aws_region,
        access_key=cfg.aws_access_key_id,
        secret_key=cfg.aws_secret_access_key,
        endpoint_url=cfg.s3_endpoint_url,
    )


def _blob_date(row: dict[str, Any]) -> datetime:
    created = row.get("created_at")
    if isinstance(created, datetime):
        return created if created.tzinfo is not None else created.replace(tzinfo=UTC)
    return datetime.now(tz=UTC)


async def _download_page(
    rows: list[dict[str, Any]],
    *,
    cfg: Config,
    client: Any,
    download_concurrency: int,
) -> _DownloadedPage:
    tmp_root = Path(tempfile.mkdtemp(prefix="embed_blobs_"))
    scanned = 0
    skipped = 0
    download_jobs: list[tuple[dict[str, Any], str, Path]] = []
    after_phash: str | None = None

    for row in rows:
        scanned += 1
        phash = str(row["phash"])
        after_phash = phash
        uri = str(row.get("canonical_s3_uri") or "")
        if not uri.startswith("s3://"):
            skipped += 1
            continue
        download_jobs.append((row, uri, tmp_root / f"{phash}.bin"))

    sem = asyncio.Semaphore(download_concurrency)

    async def _one(
        row: dict[str, Any],
        uri: str,
        local: Path,
    ) -> tuple[dict[str, Any], bytes] | None:
        async with sem:
            try:
                await download_s3_object_to_path(
                    uri,
                    local,
                    region=cfg.aws_region,
                    client=client,
                )
                return row, local.read_bytes()
            except Exception:
                logger.exception("download failed phash=%s uri=%s", row.get("phash"), uri)
                return None

    results = await asyncio.gather(*[_one(row, uri, local) for row, uri, local in download_jobs])
    encode_payloads: list[bytes] = []
    encode_meta: list[dict[str, Any]] = []
    failed_downloads = 0
    for result in results:
        if result is None:
            failed_downloads += 1
            continue
        row, payload = result
        encode_payloads.append(payload)
        encode_meta.append(row)

    return _DownloadedPage(
        encode_payloads=encode_payloads,
        encode_meta=encode_meta,
        scanned=scanned,
        skipped=skipped,
        failed_downloads=failed_downloads,
        after_phash=after_phash,
        tmp_root=tmp_root,
    )


async def embed_image_blobs(
    conn: Any,
    cfg: Config,
    *,
    progress: SharedProgress | None = None,
    stop: GracefulStop | None = None,
) -> BlobEmbedStats:
    """Embed pending canonical blobs for ``cfg.embedding_model``."""
    sink = _build_sink(cfg)
    writer = ImageEmbeddingShardWriter(
        sink,
        model=cfg.embedding_model,
        modality=cfg.embedding_modality,
        dim=cfg.vector_dim,
        prefix=cfg.store_prefix,
        auto_flush=False,
        tier="micro",
    )
    stats = BlobEmbedStats()
    after_phash: str | None = None
    download_concurrency = max(1, int(getattr(cfg, "download_concurrency", 32) or 32))
    logger.info(
        "blob embed start model=%s download_concurrency=%s db_batch=%s embed_batch=%s (pipelined)",
        cfg.embedding_model,
        download_concurrency,
        cfg.db_batch_size,
        cfg.embed_batch_size,
    )

    async def _flush_marks(pending_marks: list[str], keys_before: int) -> BlobEmbedStats:
        nonlocal stats
        if not pending_marks and keys_before == len(writer.shards_written):
            return stats
        try:
            written = await writer.aflush()
            if written:
                shard_rows: list[tuple[str, str, int]] = []
                for key, row_count in written:
                    month = month_from_shard_key(key)
                    if month is None:
                        raise ValueError(f"cannot derive dt=YYYY-MM from shard key {key!r}")
                    shard_rows.append((key, month, row_count))
                await register_micro_shards(
                    conn,
                    model=cfg.embedding_model,
                    modality=cfg.embedding_modality,
                    shards=shard_rows,
                )
            for phash in pending_marks:
                await mark_blob_embedded(conn, phash_hex=phash, model=cfg.embedding_model)
            await conn.commit()
            stats = BlobEmbedStats(
                scanned=stats.scanned,
                embedded=stats.embedded + len(pending_marks),
                skipped=stats.skipped,
                failed=stats.failed,
            )
            if progress is not None:
                await progress.snapshot(
                    embedded=stats.embedded,
                    messages_scanned=stats.scanned,
                    failed=stats.failed,
                    skipped=stats.skipped,
                )
            logger.info(
                "progress embedded=%s scanned=%s skipped=%s failed=%s",
                stats.embedded,
                stats.scanned,
                stats.skipped,
                stats.failed,
            )
        except Exception:
            await conn.rollback()
            keys = writer.shards_written[keys_before:]
            writer.discard()
            if keys:
                try:
                    await delete_micro_shard_catalog(
                        conn,
                        model=cfg.embedding_model,
                        modality=cfg.embedding_modality,
                        shard_keys=keys,
                    )
                    await conn.commit()
                except Exception:
                    logger.exception("catalog cleanup failed")
            for key in keys:
                try:
                    await asyncio.to_thread(sink.delete_key, key)
                except Exception:
                    logger.exception("S3 rollback failed key=%s", key)
            raise
        return stats

    async def _embed_page(page: _DownloadedPage) -> None:
        nonlocal stats
        stats = BlobEmbedStats(
            scanned=stats.scanned + page.scanned,
            embedded=stats.embedded,
            skipped=stats.skipped + page.skipped,
            failed=stats.failed + page.failed_downloads,
        )
        pending_marks: list[str] = []
        keys_before = len(writer.shards_written)
        try:
            for i in range(0, len(page.encode_payloads), max(1, cfg.embed_batch_size)):
                chunk_bytes = page.encode_payloads[i : i + cfg.embed_batch_size]
                chunk_meta = page.encode_meta[i : i + cfg.embed_batch_size]
                try:
                    vectors = await embed_images_bytes(
                        service_url=cfg.image_embedding_service_url,
                        images=chunk_bytes,
                        timeout=cfg.embedding_timeout_seconds,
                    )
                except Exception:
                    logger.exception("embed batch failed size=%s", len(chunk_bytes))
                    stats = BlobEmbedStats(
                        scanned=stats.scanned,
                        embedded=stats.embedded,
                        skipped=stats.skipped,
                        failed=stats.failed + len(chunk_bytes),
                    )
                    continue
                if len(vectors) != len(chunk_meta):
                    logger.error(
                        "embed returned %s vectors for %s images",
                        len(vectors),
                        len(chunk_meta),
                    )
                    stats = BlobEmbedStats(
                        scanned=stats.scanned,
                        embedded=stats.embedded,
                        skipped=stats.skipped,
                        failed=stats.failed + len(chunk_meta),
                    )
                    continue
                for row, vector in zip(chunk_meta, vectors, strict=True):
                    phash = str(row["phash"])
                    uri = str(row["canonical_s3_uri"])
                    _, key = parse_s3_uri(uri)
                    writer.add(
                        id=phash,
                        date=_blob_date(row),
                        vector=vector,
                        msg_id=phash,
                        s3_key=uri,
                        filename=Path(key).name,
                    )
                    pending_marks.append(phash)
            await _flush_marks(pending_marks, keys_before)
        finally:
            shutil.rmtree(page.tmp_root, ignore_errors=True)

    try:
        async with s3_client(
            cfg.aws_region,
            access_key=cfg.aws_access_key_id,
            secret_key=cfg.aws_secret_access_key,
        ) as client:
            first_rows = await list_blobs_needing_embed(
                conn,
                model=cfg.embedding_model,
                limit=cfg.db_batch_size,
                after_phash=after_phash,
            )
            if not first_rows:
                return stats

            prefetch = asyncio.create_task(
                _download_page(
                    first_rows,
                    cfg=cfg,
                    client=client,
                    download_concurrency=download_concurrency,
                )
            )

            while True:
                if stop is not None and stop.requested:
                    prefetch.cancel()
                    try:
                        page = await prefetch
                        shutil.rmtree(page.tmp_root, ignore_errors=True)
                    except (asyncio.CancelledError, Exception):
                        pass
                    break

                page = await prefetch
                after_phash = page.after_phash

                next_rows: list[dict[str, Any]] = []
                if stop is None or not stop.requested:
                    next_rows = await list_blobs_needing_embed(
                        conn,
                        model=cfg.embedding_model,
                        limit=cfg.db_batch_size,
                        after_phash=after_phash,
                    )
                if next_rows:
                    prefetch = asyncio.create_task(
                        _download_page(
                            next_rows,
                            cfg=cfg,
                            client=client,
                            download_concurrency=download_concurrency,
                        )
                    )
                else:
                    prefetch = None

                await _embed_page(page)

                if cfg.delay_seconds > 0:
                    await asyncio.sleep(cfg.delay_seconds)
                if prefetch is None:
                    break
    finally:
        await writer.aclose()

    logger.info(
        "blob embed done model=%s scanned=%s embedded=%s skipped=%s failed=%s",
        cfg.embedding_model,
        stats.scanned,
        stats.embedded,
        stats.skipped,
        stats.failed,
    )
    return stats
