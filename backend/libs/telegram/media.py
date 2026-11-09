"""Telegram message media download/upload and embeddable-image helpers."""

from __future__ import annotations

import inspect
import logging
import os
import re
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import (
    FileReferenceExpiredError,
    FilerefUpgradeNeededError,
    FloodWaitError,
)
from telethon.tl.types import (
    DocumentAttributeFilename,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
    TypeDocument,
    TypeMessageMedia,
)

from aws.s3 import download_s3_object_to_path, parse_s3_uri, upload_file as upload_s3_file
from db.schemas.telegram.slim_media import SlimHarvestMediaStateSchema
from db.schemas.telegram.slim_message_media import (
    SlimMessageMediaDocumentSchema,
    SlimTypeMessageMediaSchema,
)
from phash.core import phash_hex_from_bytes
from telegram.media_kinds import (
    classify_downloadable_kind,
    is_downloadable_media,
    is_raster_image_mime,
    parse_slim_media,
)
from telegram.image_harvest import (
    display_key_for_object_key,
    is_heif_mime,
    prepare_heif_harvest_derivative,
)
from telegram.media_limits import is_large_media

logger = logging.getLogger(__name__)

DownloadProgressCallback = Callable[[int, int | None], Awaitable[None] | None]

CHATS_ATTACHMENTS_PREFIX = "chats"
CHATS_PLATFORM_SEGMENT = "telegram"
CHATS_TELEGRAM_PREFIX = f"{CHATS_ATTACHMENTS_PREFIX}/{CHATS_PLATFORM_SEGMENT}"

# Telethon ``Message.media`` is ``TypeMessageMedia | None`` (19 TL variants).
# We download Photo and Document attachments only (not WebPage link cards).
DownloadableTelethonMedia = MessageMediaPhoto | MessageMediaDocument
TelethonMediaToDownload = MessageMediaPhoto | MessageMediaDocument


class MessageWithMedia(Protocol):
    """Minimal message shape for media download (custom or TL ``Message``)."""

    id: int
    media: TypeMessageMedia | None
    date: datetime | None


MediaFailureReason = Literal["expired", "missing", "failed"]

_FILE_REF_ERRORS = (FileReferenceExpiredError, FilerefUpgradeNeededError)


class MessageMediaDownloadError(Exception):
    """Terminal media download failure with a stable reason for API / UI."""

    def __init__(self, reason: MediaFailureReason, message: str = "") -> None:
        self.reason: MediaFailureReason = reason
        super().__init__(message or reason)


def _prepare_download_target(
    msg: MessageWithMedia,
    *,
    source_peer_id: int,
) -> tuple[Any, TelethonMediaToDownload, str, bool] | None:
    """Return ``(iter_download_input, media_for_meta, filename, is_document)`` or None.

    For photo/document messages, ``iter_download_input`` is the full ``msg`` so Telethon
    can refresh expired file references mid-download.
    """
    media: TypeMessageMedia | None = msg.media
    message_id = msg.id
    media_type_name = type(media).__name__ if media else "None"
    if media is None:
        logger.warning(
            "Media skip for %s msg %s: no media (media_type=%s)",
            source_peer_id,
            message_id,
            media_type_name,
        )
        return None

    index = 0
    if isinstance(media, MessageMediaWebPage):
        # Link cards point at an external site; do not harvest preview thumbs.
        return None

    if isinstance(media, (MessageMediaPhoto, MessageMediaDocument)):
        media_to_download = media
        is_document = isinstance(media, MessageMediaDocument)
        if is_document:
            doc = getattr(media, "document", None)
            filename = _get_document_filename(doc) if doc else ""
            if not filename:
                filename = media_attachment_name(message_id, index, "bin")
        else:
            filename = media_attachment_name(message_id, index, "jpg")
        # Pass the Message so Telethon can refetch file_reference on expiry.
        return msg, media_to_download, filename, is_document

    logger.warning(
        "Media skip for %s msg %s: unsupported type (media_type=%s)",
        source_peer_id,
        message_id,
        media_type_name,
    )
    return None


async def _emit_download_progress(
    on_progress: DownloadProgressCallback | None,
    loaded: int,
    total: int | None,
) -> None:
    if on_progress is None:
        return
    result = on_progress(loaded, total)
    if inspect.isawaitable(result):
        await result


async def _iter_download_to_path(
    client: TelegramClient,
    download_input: Any,
    filepath: str,
    *,
    source_peer_id: int,
    message_id: int,
    filename: str,
    media_for_meta: TelethonMediaToDownload,
    on_progress: DownloadProgressCallback | None = None,
) -> int:
    """Stream ``download_input`` to ``filepath``. Returns bytes downloaded."""
    chunk_size = 262144
    expected_size: int | None = None
    doc = getattr(media_for_meta, "document", None)
    if doc is not None:
        raw_size = getattr(doc, "size", None)
        if isinstance(raw_size, int) and raw_size > 0:
            expected_size = raw_size
    logger.info(
        "Media download start peer=%s msg=%s filename=%s expected_bytes=%s type=%s",
        source_peer_id,
        message_id,
        filename,
        expected_size,
        type(media_for_meta).__name__,
    )
    downloaded = 0
    last_log_bytes = 0
    last_log_at = time.monotonic()
    last_emit_at = 0.0
    last_emit_bytes = 0
    progress_log_bytes = 25 * 1024 * 1024
    progress_log_secs = 15.0
    emit_interval = 0.15
    t_dl0 = time.perf_counter()

    async def emit_progress(*, force: bool = False) -> None:
        nonlocal last_emit_at, last_emit_bytes
        now = time.monotonic()
        if not force and downloaded - last_emit_bytes < chunk_size and now - last_emit_at < emit_interval:
            return
        last_emit_at = now
        last_emit_bytes = downloaded
        await _emit_download_progress(on_progress, downloaded, expected_size)

    with open(filepath, "wb") as f:
        async for chunk in client.iter_download(download_input, chunk_size=chunk_size, request_size=chunk_size):
            f.write(chunk)
            downloaded += len(chunk)
            now = time.monotonic()
            if downloaded - last_log_bytes >= progress_log_bytes or now - last_log_at >= progress_log_secs:
                pct = f"{(100.0 * downloaded / expected_size):.1f}%" if expected_size else "?"
                elapsed = time.perf_counter() - t_dl0
                rate = downloaded / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "Media download progress peer=%s msg=%s bytes=%s/%s (%s) "
                    "elapsed_s=%.1f rate_mib_s=%.2f filename=%s",
                    source_peer_id,
                    message_id,
                    downloaded,
                    expected_size if expected_size is not None else "?",
                    pct,
                    elapsed,
                    rate / (1024 * 1024),
                    filename,
                )
                last_log_bytes = downloaded
                last_log_at = now
            await emit_progress()
    await emit_progress(force=True)
    dl_elapsed = time.perf_counter() - t_dl0
    logger.info(
        "Media download finished peer=%s msg=%s bytes=%s elapsed_s=%.1f filename=%s",
        source_peer_id,
        message_id,
        downloaded,
        dl_elapsed,
        filename,
    )
    return downloaded


async def download_and_upload_message_media(
    client: TelegramClient,
    msg: MessageWithMedia,
    source_peer_id: int,
    bucket: str,
    region: str,
    *,
    entity: Any | None = None,
    on_progress: DownloadProgressCallback | None = None,
    canonical_s3_uri_for_phash: Callable[[str], Awaitable[str | None]] | None = None,
) -> dict[str, Any] | None:
    """Download message media via Telethon and upload to S3.

    Pass ``entity`` (the peer used for ``get_messages``) so expired file references
    can be refreshed once via a message refetch. Raises ``FloodWaitError`` and
    ``MessageMediaDownloadError``; returns ``None`` only for unsupported / empty media.
    """
    attempt_msg: MessageWithMedia = msg
    refreshed = False

    while True:
        prepared = _prepare_download_target(attempt_msg, source_peer_id=source_peer_id)
        if prepared is None:
            return None

        download_input, media_for_meta, filename, is_document = prepared
        message_id = attempt_msg.id
        index = 0
        tmp_dir = tempfile.mkdtemp(prefix=f"harvest_media_{source_peer_id}_{message_id}_")
        try:
            filepath = os.path.join(tmp_dir, filename)
            try:
                await _iter_download_to_path(
                    client,
                    download_input,
                    filepath,
                    source_peer_id=source_peer_id,
                    message_id=message_id,
                    filename=filename,
                    media_for_meta=media_for_meta,
                    on_progress=on_progress,
                )
            except FloodWaitError:
                raise
            except _FILE_REF_ERRORS as exc:
                if refreshed or entity is None:
                    logger.warning(
                        "File reference expired for peer=%s msg=%s (refreshed=%s entity=%s)",
                        source_peer_id,
                        message_id,
                        refreshed,
                        entity is not None,
                    )
                    raise MessageMediaDownloadError("expired", str(exc)) from exc
                logger.info(
                    "File reference expired for peer=%s msg=%s; refetching message",
                    source_peer_id,
                    message_id,
                )
                try:
                    fresh = await client.get_messages(entity, ids=msg.id)
                except FloodWaitError:
                    raise
                except Exception as refetch_exc:
                    logger.warning(
                        "Refetch after file-ref expiry failed peer=%s msg=%s: %s",
                        source_peer_id,
                        message_id,
                        refetch_exc,
                    )
                    raise MessageMediaDownloadError("expired", str(refetch_exc)) from refetch_exc
                if not isinstance(fresh, list):
                    fresh_msg = fresh
                else:
                    fresh_msg = fresh[0] if fresh else None
                if fresh_msg is None or getattr(fresh_msg, "media", None) is None:
                    raise MessageMediaDownloadError(
                        "missing",
                        f"Message {msg.id} missing or has no media after file-ref refresh",
                    )
                attempt_msg = fresh_msg
                refreshed = True
                continue

            if not os.path.isfile(filepath):
                logger.warning(
                    "Media skip for %s msg %s: download did not produce file",
                    source_peer_id,
                    message_id,
                )
                return None

            message_date = getattr(attempt_msg, "date", None) or getattr(msg, "date", None)
            if message_date is None:
                logger.warning(
                    "Media skip for %s msg %s: missing message date (required for S3 key)",
                    source_peer_id,
                    message_id,
                )
                return None
            mime = (
                getattr(getattr(media_for_meta, "document", None), "mime_type", None) if is_document else "image/jpeg"
            )
            size = os.path.getsize(filepath)
            name = os.path.basename(filepath)
            mime_str = str(mime) if mime else None
            phash_hex: str | None = None
            if is_raster_image_mime(mime_str):
                try:
                    phash_hex = phash_hex_from_bytes(Path(filepath).read_bytes())
                except Exception:
                    logger.exception(
                        "pHash failed for %s msg %s after download",
                        source_peer_id,
                        message_id,
                    )
            reused_canonical = False
            uri: str | None = None
            if phash_hex and canonical_s3_uri_for_phash is not None:
                try:
                    existing = await canonical_s3_uri_for_phash(phash_hex)
                except Exception:
                    logger.exception(
                        "Canonical pHash lookup failed for %s msg %s",
                        source_peer_id,
                        message_id,
                    )
                    existing = None
                if existing:
                    uri = existing
                    reused_canonical = True
                    logger.info(
                        "Reusing canonical image peer=%s msg=%s phash=%s uri=%s",
                        source_peer_id,
                        message_id,
                        phash_hex,
                        existing,
                    )
            if uri is None:
                uri = await upload_message_media(
                    bucket,
                    source_peer_id,
                    message_id,
                    index,
                    filepath,
                    region,
                    message_date=message_date,
                    content_type=mime_str,
                )
            display_uri: str | None = None
            display_size: int | None = None
            exif: dict[str, Any] | None = None
            if is_heif_mime(mime_str) and not reused_canonical:
                try:
                    display_uri, exif, display_size = await _upload_heif_display_derivative(
                        bucket=bucket,
                        region=region,
                        original_uri=uri,
                        source_path=Path(filepath),
                        tmp_dir=Path(tmp_dir),
                    )
                except Exception:
                    logger.exception(
                        "HEIC display derivative failed for %s msg %s",
                        source_peer_id,
                        message_id,
                    )
            return {
                "uri": uri,
                "display_uri": display_uri,
                "filename": name,
                "size_bytes": size,
                "display_size_bytes": display_size,
                "mime_type": mime or None,
                "display_content_type": "image/jpeg" if display_uri else None,
                "phash_hex": phash_hex,
                "exif": exif,
                "reused_canonical": reused_canonical,
            }
        except FloodWaitError:
            raise
        except MessageMediaDownloadError:
            raise
        except _FILE_REF_ERRORS:
            raise
        except Exception as e:
            logger.warning(
                "Media download/upload failed for %s msg %s: %s",
                source_peer_id,
                message_id,
                e,
                exc_info=True,
            )
            raise MessageMediaDownloadError("failed", str(e)) from e
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def is_embeddable_image_media(media: SlimTypeMessageMediaSchema | None) -> bool:
    """Return True when ``media`` is an image suitable for SigLIP embed.

    Photos and static raster ``image/*`` documents (including classic ``image/gif``) qualify.
    CAD drawings (``image/vnd.dwg``, …), animated Telegram GIFs (often silent MP4), and
    other downloadable kinds do not.
    """
    kind = classify_downloadable_kind(media)
    if kind == "image":
        return True
    if kind != "gif":
        return False
    if isinstance(media, SlimMessageMediaDocumentSchema):
        return is_raster_image_mime(media.mime_type)
    return False


def harvest_s3_uri(media: SlimTypeMessageMediaSchema | None) -> str | None:
    if media is None:
        return None
    harvest = getattr(media, "harvest", None)
    if harvest is None:
        return None
    return harvest.s3_uri or None


def harvest_display_s3_uri(media: SlimTypeMessageMediaSchema | None) -> str | None:
    """Presign target for in-browser image preview (JPEG when HEIC master exists)."""
    if media is None:
        return None
    harvest = getattr(media, "harvest", None)
    if harvest is None:
        return None
    display = getattr(harvest, "display_s3_uri", None)
    if display:
        return display
    return harvest.s3_uri or None


def harvest_original_content_type(media: SlimTypeMessageMediaSchema | None) -> str | None:
    if media is None:
        return None
    harvest = getattr(media, "harvest", None)
    if harvest is not None:
        ct = getattr(harvest, "content_type", None)
        if isinstance(ct, str) and ct.strip():
            return ct.strip()
    if isinstance(media, SlimMessageMediaDocumentSchema):
        mime = media.mime_type
        if isinstance(mime, str) and mime.strip():
            return mime.strip()
    return None


def harvest_needs_web_display(media: SlimTypeMessageMediaSchema | None) -> bool:
    if media is None:
        return False
    harvest = getattr(media, "harvest", None)
    if harvest is None or not harvest.s3_uri:
        return False
    if getattr(harvest, "display_s3_uri", None):
        return False
    mime = harvest_original_content_type(media)
    return is_heif_mime(mime)


async def _upload_heif_display_derivative(
    *,
    bucket: str,
    region: str,
    original_uri: str,
    source_path: Path,
    tmp_dir: Path,
) -> tuple[str, dict[str, Any], int]:
    display_path, exif, display_size = prepare_heif_harvest_derivative(source_path, tmp_dir=tmp_dir)
    _, original_key = parse_s3_uri(original_uri)
    display_key = display_key_for_object_key(original_key)
    display_uri = await upload_s3_file(
        bucket=bucket,
        key=display_key,
        filepath=display_path,
        region=region,
        content_type="image/jpeg",
    )
    return display_uri, exif, display_size


async def ensure_harvest_web_display(
    media: SlimTypeMessageMediaSchema | dict[str, Any],
    *,
    bucket: str,
    region: str,
) -> dict[str, Any]:
    """Build a JPEG display derivative + EXIF for legacy HEIC-only harvest rows."""
    slim = parse_slim_media(media)
    if slim is None or not harvest_needs_web_display(slim):
        if isinstance(media, dict):
            return media
        return slim.model_dump(mode="json", exclude_none=True)  # type: ignore[union-attr]

    original_uri = harvest_s3_uri(slim)
    if not original_uri:
        if isinstance(media, dict):
            return media
        return slim.model_dump(mode="json", exclude_none=True)

    tmp_dir = tempfile.mkdtemp(prefix="heif_display_")
    try:
        local = Path(tmp_dir) / "master.bin"
        await download_s3_object_to_path(original_uri, local, region=region)
        display_uri, exif, display_size = await _upload_heif_display_derivative(
            bucket=bucket,
            region=region,
            original_uri=original_uri,
            source_path=local,
            tmp_dir=Path(tmp_dir),
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    original_ct = harvest_original_content_type(slim)
    return merge_harvest_into_media_dict(
        slim,
        display_s3_uri=display_uri,
        display_content_type="image/jpeg",
        display_size_bytes=display_size,
        content_type=original_ct,
        exif=exif,
    )


def harvest_image_embedding_id(media: SlimTypeMessageMediaSchema | None) -> UUID | None:
    if media is None:
        return None
    harvest = getattr(media, "harvest", None)
    if harvest is None or harvest.image_embedding_id is None:
        return None
    return harvest.image_embedding_id


def harvest_image_embedded(media: SlimTypeMessageMediaSchema | None, model: str) -> bool:
    """True when this harvest is marked done for ``model``.

    Legacy SigLIP rows only have ``image_embedding_id``; that still counts as embedded
    for ``siglip2-so400m-p16-384``.
    """
    from embedding.image_models import SIGLIP_EMBED_MODEL

    if media is None:
        return False
    harvest = getattr(media, "harvest", None)
    if harvest is None:
        return False
    flags = getattr(harvest, "image_embedded", None)
    if isinstance(flags, dict) and flags.get(model) is True:
        return True
    if model == SIGLIP_EMBED_MODEL and harvest.image_embedding_id is not None:
        return True
    return False


def harvest_phash(media: SlimTypeMessageMediaSchema | None) -> str | None:
    if media is None:
        return None
    harvest = getattr(media, "harvest", None)
    if harvest is None or harvest.phash is None:
        return None
    return str(harvest.phash)


def needs_download(media: SlimTypeMessageMediaSchema | None) -> bool:
    if not is_downloadable_media(media):
        return False
    if harvest_s3_uri(media):
        return False
    return True


def requires_harvest_for_coverage(media: SlimTypeMessageMediaSchema | None) -> bool:
    """True when coverage completeness should require ``s3_uri`` (or large-defer).

    ``MessageMediaWebPage`` link cards are never harvested and never block sealed runs.
    """
    if not is_downloadable_media(media):
        return False
    if harvest_s3_uri(media):
        return True
    if is_large_media(media):
        return True
    return needs_download(media)


def needs_embed(media: SlimTypeMessageMediaSchema | None, model: str | None = None) -> bool:
    from embedding.image_models import SIGLIP_EMBED_MODEL

    if not is_embeddable_image_media(media):
        return False
    uri = harvest_s3_uri(media)
    if not uri:
        return False
    return not harvest_image_embedded(media, model or SIGLIP_EMBED_MODEL)


def needs_phash(media: SlimTypeMessageMediaSchema | None) -> bool:
    if not is_embeddable_image_media(media):
        return False
    if not harvest_s3_uri(media):
        return False
    return harvest_phash(media) is None


def media_month_key(date: datetime) -> str:
    """UTC ``YYYY-MM`` for the message date (S3 ``dt=`` segment / DB partition prune)."""
    if date.tzinfo is None:
        date_u = date.replace(tzinfo=UTC)
    else:
        date_u = date.astimezone(UTC)
    return f"{date_u.year:04d}-{date_u.month:02d}"


def _safe_attachment_name(filename: str, index: int) -> str:
    base = os.path.basename(filename) or f"file-{index}"
    base = re.sub(r"[^\w.\-]+", "_", base)
    return base[:200] if base else f"file-{index}"


def media_attachment_name(message_id: int, index: int, extension: str | None = None) -> str:
    """Attachment stem ``{message_id}-{index}``, optionally with ``.{extension}``.

    ``index`` distinguishes multiple media files under one message. Pass ``extension``
    for local temp filenames only; S3 keys omit extensions.
    """
    stem = f"{message_id}-{index}"
    if extension is None:
        return stem
    return f"{stem}.{extension.lstrip('.')}"


def _get_document_filename(doc: TypeDocument) -> str:
    for attr in getattr(doc, "attributes", []) or []:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name or ""
    return ""


def build_chat_media_key(
    source_peer_id: int,
    message_id: int,
    index: int,
    *,
    message_date: datetime,
) -> str:
    """Build ``chats/telegram/{peer}/dt=YYYY-MM/{message_id}-{index}``."""
    month = media_month_key(message_date)
    return f"{CHATS_TELEGRAM_PREFIX}/{source_peer_id}/dt={month}/{media_attachment_name(message_id, index)}"


def parse_chat_media_key(key: str) -> dict[str, Any] | None:
    """Parse canonical chat media key ``chats/telegram/{peer}/dt=YYYY-MM/{message_id}-{index}``.

    Returns ``{peer_external_id, month, message_id, index}`` or ``None``.
    """
    parts = key.strip("/").split("/")
    if (
        len(parts) != 5
        or parts[0] != CHATS_ATTACHMENTS_PREFIX
        or parts[1] != CHATS_PLATFORM_SEGMENT
        or not parts[3].startswith("dt=")
    ):
        return None
    month = parts[3][3:]
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        return None
    basename = parts[4]
    if not re.fullmatch(r"\d+-\d+", basename):
        return None
    message_id_s, index_s = basename.split("-", 1)
    try:
        return {
            "peer_external_id": parts[2],
            "month": month,
            "message_id": int(message_id_s),
            "index": int(index_s),
        }
    except ValueError:
        return None


def is_canonical_chat_media_key(key: str) -> bool:
    """True when ``key`` matches the canonical chat media layout."""
    return parse_chat_media_key(key) is not None


async def upload_message_media(
    bucket: str,
    source_peer_id: int,
    message_id: int,
    index: int,
    filepath: str | Path,
    region: str,
    *,
    message_date: datetime,
    content_type: str | None = None,
) -> str:
    """Upload a message media file to S3. Returns ``s3://bucket/key`` URI."""
    path = Path(filepath)
    key = build_chat_media_key(
        source_peer_id,
        message_id,
        index,
        message_date=message_date,
    )
    uri = await upload_s3_file(
        bucket=bucket,
        key=key,
        filepath=path,
        region=region,
        content_type=content_type,
    )
    logger.info("Uploaded chat media for msg %s", message_id)
    return uri


def merge_harvest_into_media_dict(
    media: SlimTypeMessageMediaSchema | dict[str, Any],
    *,
    s3_uri: str | None = None,
    display_s3_uri: str | None = None,
    image_embedding_id: UUID | None = None,
    image_embedded_model: str | None = None,
    phash: str | None = None,
    size_bytes: int | None = None,
    display_size_bytes: int | None = None,
    motion_s3_uri: str | None = None,
    motion_size_bytes: int | None = None,
    content_type: str | None = None,
    display_content_type: str | None = None,
    exif: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return media dict with ``harvest`` fields merged (for JSONB updates)."""
    if isinstance(media, dict):
        out = dict(media)
    else:
        out = media.model_dump(mode="json", exclude_none=True)
    existing = out.get("harvest")
    harvest: dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}
    if s3_uri is not None:
        harvest["s3_uri"] = s3_uri
    if display_s3_uri is not None:
        harvest["display_s3_uri"] = display_s3_uri
    if image_embedding_id is not None:
        harvest["image_embedding_id"] = str(image_embedding_id)
    if image_embedded_model is not None:
        current = harvest.get("image_embedded")
        flags = dict(current) if isinstance(current, dict) else {}
        flags[image_embedded_model] = True
        harvest["image_embedded"] = flags
    if phash is not None:
        harvest["phash"] = phash
    if size_bytes is not None:
        harvest["size_bytes"] = size_bytes
    if display_size_bytes is not None:
        harvest["display_size_bytes"] = display_size_bytes
    if motion_s3_uri is not None:
        harvest["motion_s3_uri"] = motion_s3_uri
    if motion_size_bytes is not None:
        harvest["motion_size_bytes"] = motion_size_bytes
    if content_type is not None:
        harvest["content_type"] = content_type
    if display_content_type is not None:
        harvest["display_content_type"] = display_content_type
    if exif is not None:
        harvest["exif"] = exif
    out["harvest"] = SlimHarvestMediaStateSchema.model_validate(harvest).model_dump(mode="json", exclude_none=True)
    return out
