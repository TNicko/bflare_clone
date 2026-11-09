from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from aws.s3 import download_s3_object_to_path, parse_s3_uri
from embedding.image_client import embed_images_bytes
from embstore import ImageEmbeddingShardWriter, LocalSink, S3Sink, Sink
from embstore.layout import month_from_shard_key
from jobs.embed_images.config import Config
from jobs.progress import SharedProgress
from jobs.embed_images.db import (
    EmbedPeerRow,
    MessageMediaRow,
    delete_micro_shard_catalog,
    fetch_embed_cursor,
    list_download_coverage_for_peer,
    list_embed_coverage,
    list_messages_needing_embed,
    register_micro_shards,
    replace_embed_coverage,
    update_message_media,
    upsert_embed_cursor,
)
from telegram.media import (
    harvest_image_embedded,
    harvest_s3_uri,
    is_embeddable_image_media,
    merge_harvest_into_media_dict,
)
from utils.coverage_intervals import (
    CoverageInterval,
    ensure_utc,
    merge_intervals,
    partition_envelope,
)
from utils.graceful_stop import GracefulStop

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EmbedStats:
    peers_seen: int = 0
    peers_with_work: int = 0
    peers_complete: int = 0
    peers_no_download: int = 0
    messages_scanned: int = 0
    embedded: int = 0
    skipped: int = 0
    failed: int = 0


@dataclass(frozen=True, slots=True)
class _EmbeddedRow:
    row: MessageMediaRow
    s3_uri: str
    filename: str


async def _complete_embed_gap(
    conn: Any,
    *,
    peer: EmbedPeerRow,
    gap: CoverageInterval,
    gap_before: datetime,
    last_message_id: int,
    ignore_coverage: bool,
    model: str,
) -> None:
    if ignore_coverage:
        return

    current = await list_embed_coverage(
        conn,
        model=model,
        peer_external_id=peer.peer_external_id,
        peer_type=peer.peer_type,
    )
    cursor = await fetch_embed_cursor(
        conn,
        model=model,
        peer_external_id=peer.peer_external_id,
        peer_type=peer.peer_type,
        window_after=gap.covered_after,
        window_before=gap_before,
    )
    if cursor.is_complete:
        return

    merged = merge_intervals(current + [gap])
    await replace_embed_coverage(
        conn,
        model=model,
        peer_external_id=peer.peer_external_id,
        peer_type=peer.peer_type,
        intervals=merged,
    )
    await upsert_embed_cursor(
        conn,
        model=model,
        peer_external_id=peer.peer_external_id,
        peer_type=peer.peer_type,
        window_after=gap.covered_after,
        window_before=gap_before,
        last_message_id=last_message_id,
        is_complete=True,
    )


async def _advance_embed_cursor(
    conn: Any,
    *,
    peer: EmbedPeerRow,
    gap: CoverageInterval,
    gap_before: datetime,
    last_message_id: int,
    ignore_coverage: bool,
    model: str,
) -> None:
    if ignore_coverage:
        return
    cursor = await fetch_embed_cursor(
        conn,
        model=model,
        peer_external_id=peer.peer_external_id,
        peer_type=peer.peer_type,
        window_after=gap.covered_after,
        window_before=gap_before,
    )
    if not cursor.is_complete:
        await upsert_embed_cursor(
            conn,
            model=model,
            peer_external_id=peer.peer_external_id,
            peer_type=peer.peer_type,
            window_after=gap.covered_after,
            window_before=gap_before,
            last_message_id=last_message_id,
            is_complete=False,
        )


def _chunked(items: list, size: int) -> list[list]:
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


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


async def _rollback_micros(
    writer: ImageEmbeddingShardWriter,
    keys: list[str],
    *,
    peer_external_id: str,
) -> None:
    if not keys:
        return
    logger.warning(
        "Rolling back %s micro shard(s) for peer %s after failed commit",
        len(keys),
        peer_external_id,
    )
    await writer.rollback_keys(keys)


async def _download_image_bytes(s3_uri: str, *, region: str) -> bytes:
    tmp_dir = tempfile.mkdtemp(prefix="embed_img_")
    try:
        path = Path(tmp_dir) / "image.bin"
        await download_s3_object_to_path(s3_uri, path, region=region)
        return path.read_bytes()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


async def _embed_page_to_buffer(
    *,
    rows: list[MessageMediaRow],
    cfg: Config,
    writer: ImageEmbeddingShardWriter,
) -> tuple[int, int, int, list[_EmbeddedRow]]:
    """Embed a DB page into the writer buffer only (no S3 flush).

    On any GPU/chunk failure, discards the buffer and returns ``failed > 0``.
    """
    work: list[_EmbeddedRow] = []
    skipped = 0
    for row in rows:
        if not is_embeddable_image_media(row.media):
            skipped += 1
            continue
        s3_uri = harvest_s3_uri(row.media)
        if not s3_uri:
            skipped += 1
            continue
        if not cfg.embed_ignore_cursor and harvest_image_embedded(row.media, cfg.embedding_model):
            skipped += 1
            continue
        if row.date is None:
            logger.warning("Skip message_id=%s: message date is required", row.message_id)
            skipped += 1
            continue
        _, key = parse_s3_uri(s3_uri)
        filename = key.rsplit("/", 1)[-1]
        work.append(_EmbeddedRow(row=row, s3_uri=s3_uri, filename=filename))

    if not work:
        return 0, skipped, 0, []

    embedded_meta: list[_EmbeddedRow] = []
    embed_chunk_size = max(1, cfg.embed_batch_size)

    for chunk in _chunked(work, embed_chunk_size):
        try:
            images: list[bytes] = []
            for item in chunk:
                images.append(await _download_image_bytes(item.s3_uri, region=cfg.aws_region))
            vectors = await embed_images_bytes(
                service_url=cfg.image_embedding_service_url,
                images=images,
                timeout=cfg.embedding_timeout_seconds,
            )
        except Exception:
            logger.exception(
                "Failed embedding batch of %s image(s) (ids %s..%s)",
                len(chunk),
                chunk[0].row.message_id,
                chunk[-1].row.message_id,
            )
            writer.discard()
            return 0, skipped, len(work), []

        try:
            for item, vector in zip(chunk, vectors, strict=True):
                row = item.row
                assert row.date is not None
                writer.add(
                    id=str(row.id),
                    date=row.date,
                    vector=vector,
                    msg_id=str(row.id),
                    s3_key=item.s3_uri,
                    filename=item.filename,
                    message_id=row.message_id,
                    source_external_id=row.source_external_id,
                    peer_type=str(row.peer_type),
                )
                embedded_meta.append(item)
        except Exception:
            logger.exception(
                "Failed buffering embeddings for %s image(s) (ids %s..%s)",
                len(chunk),
                chunk[0].row.message_id,
                chunk[-1].row.message_id,
            )
            writer.discard()
            return 0, skipped, len(work), []

        if cfg.delay_seconds > 0:
            await asyncio.sleep(cfg.delay_seconds)

    return len(embedded_meta), skipped, 0, embedded_meta


async def _mark_rows_embedded(conn: Any, items: list[_EmbeddedRow], *, model: str) -> None:
    for item in items:
        if item.row.media is None or item.row.date is None:
            continue
        updated = merge_harvest_into_media_dict(item.row.media, image_embedded_model=model)
        await update_message_media(conn, row_id=item.row.id, date=item.row.date, media=updated)


async def _commit_page(
    conn: Any,
    *,
    peer: EmbedPeerRow,
    gap: CoverageInterval,
    gap_before: datetime,
    last_message_id: int,
    ignore_coverage: bool,
    writer: ImageEmbeddingShardWriter,
    complete_gap: bool,
    model: str,
    modality: str,
    embedded_items: list[_EmbeddedRow],
) -> list[str]:
    """Flush micros to S3, catalog them, mark media, then advance cursor."""
    keys_before = len(writer.shards_written)
    keys: list[str] = []
    written: list[tuple[str, int]] = []
    catalogued = False
    try:
        written = await writer.aflush()
        keys = [key for key, _ in written]
    except Exception:
        keys = writer.shards_written[keys_before:]
        await _rollback_micros(writer, keys, peer_external_id=peer.peer_external_id)
        writer.discard()
        raise

    if not keys and not embedded_items:
        if complete_gap:
            await _complete_embed_gap(
                conn,
                peer=peer,
                gap=gap,
                gap_before=gap_before,
                last_message_id=last_message_id,
                ignore_coverage=ignore_coverage,
                model=model,
            )
        else:
            await _advance_embed_cursor(
                conn,
                peer=peer,
                gap=gap,
                gap_before=gap_before,
                last_message_id=last_message_id,
                ignore_coverage=ignore_coverage,
                model=model,
            )
        return []

    try:
        if keys:
            shard_rows: list[tuple[str, str, int]] = []
            for key, row_count in written:
                month = month_from_shard_key(key)
                if month is None:
                    raise ValueError(f"cannot derive dt=YYYY-MM from shard key {key!r}")
                shard_rows.append((key, month, row_count))
            await register_micro_shards(
                conn,
                model=model,
                modality=modality,
                shards=shard_rows,
            )
            catalogued = True

        if embedded_items:
            await _mark_rows_embedded(conn, embedded_items, model=model)

        await _advance_embed_cursor(
            conn,
            peer=peer,
            gap=gap,
            gap_before=gap_before,
            last_message_id=last_message_id,
            ignore_coverage=ignore_coverage,
            model=model,
        )
        if complete_gap:
            await _complete_embed_gap(
                conn,
                peer=peer,
                gap=gap,
                gap_before=gap_before,
                last_message_id=last_message_id,
                ignore_coverage=ignore_coverage,
                model=model,
            )
    except Exception:
        await _rollback_micros(writer, keys, peer_external_id=peer.peer_external_id)
        if catalogued:
            try:
                await delete_micro_shard_catalog(
                    conn,
                    model=model,
                    modality=modality,
                    shard_keys=keys,
                )
            except Exception:
                logger.exception(
                    "Failed deleting catalog rows after rollback for peer %s",
                    peer.peer_external_id,
                )
        raise

    return keys


async def _pipeline_gap_batches(
    conn: Any,
    *,
    peer: EmbedPeerRow,
    gap: CoverageInterval,
    gap_before: datetime,
    last_message_id: int,
    ignore_coverage: bool,
    cfg: Config,
    writer: ImageEmbeddingShardWriter,
    stop: GracefulStop | None = None,
) -> tuple[int, int, int, int]:
    """Commit protocol per DB page: embed → flush micros → catalog → media → cursor."""
    messages_scanned = 0
    embedded = 0
    skipped = 0
    failed = 0
    gap_scanned = False

    await conn.commit()
    batch = await list_messages_needing_embed(
        conn,
        peer_external_id=peer.peer_external_id,
        covered_after=gap.covered_after,
        covered_before=gap_before,
        last_message_id=last_message_id,
        batch_size=cfg.db_batch_size,
        ignore_embedding_id=ignore_coverage,
        model=cfg.embedding_model,
    )

    while batch:
        if stop is not None and stop.requested:
            logger.info(
                "Peer %s: stop requested before next page (resume message_id>%s)",
                peer.peer_external_id,
                last_message_id,
            )
            writer.discard()
            break

        gap_scanned = True
        batch_last_id = max(r.message_id for r in batch)
        more_pages = len(batch) >= cfg.db_batch_size

        embedded_n, skipped_n, failed_n, embedded_items = await _embed_page_to_buffer(
            rows=batch, cfg=cfg, writer=writer
        )

        messages_scanned += len(batch)
        embedded += embedded_n
        skipped += skipped_n
        failed += failed_n

        if failed_n > 0:
            writer.discard()
            logger.warning(
                "Peer %s: stopping gap after embed failure(s); resume message_id>%s",
                peer.peer_external_id,
                last_message_id,
            )
            break

        try:
            await _commit_page(
                conn,
                peer=peer,
                gap=gap,
                gap_before=gap_before,
                last_message_id=batch_last_id,
                ignore_coverage=ignore_coverage,
                writer=writer,
                complete_gap=not more_pages,
                model=cfg.embedding_model,
                modality=cfg.embedding_modality,
                embedded_items=embedded_items,
            )
        except Exception:
            logger.exception(
                "Peer %s: failed committing embed page ending message_id=%s",
                peer.peer_external_id,
                batch_last_id,
            )
            failed += 1
            break

        last_message_id = batch_last_id
        if not more_pages:
            break

        if stop is not None and stop.requested:
            logger.info(
                "Peer %s: stop requested after page commit (resume message_id>%s)",
                peer.peer_external_id,
                last_message_id,
            )
            break

        await conn.commit()
        batch = await list_messages_needing_embed(
            conn,
            peer_external_id=peer.peer_external_id,
            covered_after=gap.covered_after,
            covered_before=gap_before,
            last_message_id=last_message_id,
            batch_size=cfg.db_batch_size,
            ignore_embedding_id=ignore_coverage,
            model=cfg.embedding_model,
        )

    if not gap_scanned:
        if stop is not None and stop.requested:
            return messages_scanned, embedded, skipped, failed
        logger.warning(
            "No unembedded images in DB for peer %s gap [%s, %s); marking embed coverage without cursor progress",
            peer.peer_external_id,
            ensure_utc(gap.covered_after),
            gap_before,
        )
        await _complete_embed_gap(
            conn,
            peer=peer,
            gap=gap,
            gap_before=gap_before,
            last_message_id=last_message_id,
            ignore_coverage=ignore_coverage,
            model=cfg.embedding_model,
        )

    return messages_scanned, embedded, skipped, failed


async def _process_peer(
    conn: Any,
    *,
    peer: EmbedPeerRow,
    cfg: Config,
    writer: ImageEmbeddingShardWriter,
    stop: GracefulStop | None = None,
) -> tuple[str, int, int, int, int]:
    """Returns (status, scanned, embedded, skipped, failed). status: work|complete|no_download|stopped."""
    if stop is not None and stop.requested:
        return "stopped", 0, 0, 0, 0

    download_coverage = await list_download_coverage_for_peer(
        conn,
        peer_external_id=peer.peer_external_id,
        peer_type=peer.peer_type,
    )

    window_after = cfg.telegram_fetch_after
    window_before = cfg.telegram_fetch_before
    download_processable, download_uncovered = partition_envelope(
        download_coverage,
        window_after=window_after,
        window_before=window_before,
    )

    if not download_coverage or not download_processable:
        logger.debug(
            "No download coverage for peer %s; skipping embed",
            peer.peer_external_id,
        )
        return "no_download", 0, 0, 0, 0

    ignore_coverage = cfg.embed_ignore_cursor
    if ignore_coverage:
        work_gaps = download_processable
    else:
        embed_coverage = await list_embed_coverage(
            conn,
            model=cfg.embedding_model,
            peer_external_id=peer.peer_external_id,
            peer_type=peer.peer_type,
        )
        _, work_gaps = partition_envelope(embed_coverage, envelope=download_processable)

    logger.debug(
        "Peer %s image embed: %s work gap(s) in download-covered envelope",
        peer.peer_external_id,
        len(work_gaps),
    )

    if not work_gaps:
        if download_uncovered:
            logger.debug(
                "Peer %s image embed complete; %s download-uncovered gap(s) outside embed work "
                "(run download_message_media to extend)",
                peer.peer_external_id,
                len(download_uncovered),
            )
        return "complete", 0, 0, 0, 0

    messages_scanned = 0
    embedded = 0
    skipped = 0
    failed = 0

    for gap in work_gaps:
        if stop is not None and stop.requested:
            logger.info(
                "Peer %s: stop requested before next gap; exiting peer loop",
                peer.peer_external_id,
            )
            return "stopped", messages_scanned, embedded, skipped, failed

        gap_before = ensure_utc(gap.covered_before)
        if gap_before is None:
            continue

        if ignore_coverage:
            last_message_id = 0
        else:
            cursor = await fetch_embed_cursor(
                conn,
                model=cfg.embedding_model,
                peer_external_id=peer.peer_external_id,
                peer_type=peer.peer_type,
                window_after=gap.covered_after,
                window_before=gap_before,
            )
            if cursor.is_complete:
                logger.debug(
                    "Peer %s: skip gap [%s, %s) — embed cursor already complete",
                    peer.peer_external_id,
                    ensure_utc(gap.covered_after),
                    gap_before,
                )
                continue
            last_message_id = cursor.last_message_id

        logger.info(
            "Peer %s: image embed gap [%s, %s) resume message_id>%s",
            peer.peer_external_id,
            ensure_utc(gap.covered_after),
            gap_before,
            last_message_id,
        )

        scanned_n, embedded_n, skipped_n, failed_n = await _pipeline_gap_batches(
            conn,
            peer=peer,
            gap=gap,
            gap_before=gap_before,
            last_message_id=last_message_id,
            ignore_coverage=ignore_coverage,
            cfg=cfg,
            writer=writer,
            stop=stop,
        )
        messages_scanned += scanned_n
        embedded += embedded_n
        skipped += skipped_n
        failed += failed_n

        if stop is not None and stop.requested:
            return "stopped", messages_scanned, embedded, skipped, failed

    return "work", messages_scanned, embedded, skipped, failed


async def embed_images_for_peers(
    conn: Any,
    *,
    peers: list[EmbedPeerRow],
    cfg: Config,
    stop: GracefulStop | None = None,
    progress: SharedProgress | None = None,
) -> EmbedStats:
    peers_with_work = 0
    peers_complete = 0
    peers_no_download = 0
    messages_scanned = 0
    embedded = 0
    skipped = 0
    failed = 0
    stopped_early = False

    sink = _build_sink(cfg)
    if progress is not None:
        await progress.bump(peers_total=len(peers), peers_seen=len(peers))

    async with ImageEmbeddingShardWriter(
        sink,
        model=cfg.embedding_model,
        modality=cfg.embedding_modality,
        dim=cfg.vector_dim,
        prefix=cfg.store_prefix,
        auto_flush=False,
        tier="micro",
    ) as writer:
        for peer in peers:
            if stop is not None and stop.requested:
                stopped_early = True
                logger.info("Stop requested; skipping remaining peers")
                writer.discard()
                break
            if progress is not None:
                await progress.set(
                    current_peer=peer.peer_external_id,
                    detail=f"embed-images {peer.peer_external_id}",
                )
            try:
                status, scanned_n, embedded_n, skipped_n, failed_n = await _process_peer(
                    conn, peer=peer, cfg=cfg, writer=writer, stop=stop
                )
                if status == "no_download":
                    peers_no_download += 1
                elif status == "complete":
                    peers_complete += 1
                elif status == "stopped":
                    peers_with_work += 1
                    stopped_early = True
                    messages_scanned += scanned_n
                    embedded += embedded_n
                    skipped += skipped_n
                    failed += failed_n
                    writer.discard()
                    if progress is not None:
                        await progress.bump(
                            peers_done=1,
                            messages_scanned=scanned_n,
                            embedded=embedded_n,
                            skipped=skipped_n,
                            failed=failed_n,
                        )
                    break
                else:
                    peers_with_work += 1
                messages_scanned += scanned_n
                embedded += embedded_n
                skipped += skipped_n
                failed += failed_n
                if progress is not None:
                    await progress.bump(
                        peers_done=1,
                        messages_scanned=scanned_n,
                        embedded=embedded_n,
                        skipped=skipped_n,
                        failed=failed_n,
                    )
            except Exception:
                logger.exception("Failed embedding images for peer %s", peer.peer_external_id)
                failed += 1
                if progress is not None:
                    await progress.bump(peers_done=1, peers_failed=1, failed=1)

    stats = EmbedStats(
        peers_seen=len(peers),
        peers_with_work=peers_with_work,
        peers_complete=peers_complete,
        peers_no_download=peers_no_download,
        messages_scanned=messages_scanned,
        embedded=embedded,
        skipped=skipped,
        failed=failed,
    )
    logger.info(
        "Image embed done%s | peers=%s with_work=%s complete=%s no_download=%s "
        "scanned=%s embedded=%s skipped=%s failed=%s",
        " (stopped early)" if stopped_early else "",
        stats.peers_seen,
        stats.peers_with_work,
        stats.peers_complete,
        stats.peers_no_download,
        stats.messages_scanned,
        stats.embedded,
        stats.skipped,
        stats.failed,
    )
    return stats
