"""Build message media previews and get-or-fetch missing harvests via Telethon."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from psycopg import sql
from psycopg.rows import dict_row
from telethon.errors import FloodWaitError
from telethon.tl.types import InputPeerChannel, PeerChat

from aws.s3 import presign_s3_uri
from db.crud.telegram.image_blobs import is_dedupable_phash, lookup_canonical_s3_uri, record_harvested_image_blob
from db.crud.telegram.message_media_write import persist_message_media
from phash.ensure_message import ensure_message_image_phash as ensure_message_image_phash  # noqa: F401
from services.api.src.models.message_media import (
    MessageMediaFailureReason,
    MessageMediaPreview,
    MessageMediaStatus,
)
from services.api.src.on_demand_coverage import record_on_demand_media_download_coverage
from services.api.src.peer_access import connected_peer_role
from usage.meters import Meter
from utils.request_spans import timed
from telegram.api_session_pool import ApiSessionPool
from telegram.media import (
    MessageMediaDownloadError,
    download_and_upload_message_media,
    ensure_harvest_web_display,
    harvest_display_s3_uri,
    harvest_needs_web_display,
    harvest_original_content_type,
    harvest_s3_uri,
    merge_harvest_into_media_dict,
    needs_download,
)
from telegram.media_coverage_sync import record_harvested_media
from telegram.media_kinds import classify_downloadable_kind, parse_slim_media
from telegram.utils import channel_id_from_signed_peer_id

lg = logging.getLogger(__name__)

_MAX_KEYS = 50

MediaDownloadProgress = Callable[[str, int, int | None], Awaitable[None]]


def _filename_from_slim(media: Any) -> str | None:
    name = getattr(media, "filename", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    title = getattr(media, "title", None)
    if isinstance(title, str) and title.strip():
        return title.strip()
    return None


def _dims_from_slim(media: Any) -> tuple[int | None, int | None]:
    w = getattr(media, "w", None)
    h = getattr(media, "h", None)
    if isinstance(w, int) and isinstance(h, int) and w > 0 and h > 0:
        return w, h
    return None, None


def _size_bytes_from_slim(media: Any) -> int | None:
    harvest = getattr(media, "harvest", None)
    if harvest is not None:
        harvested = getattr(harvest, "size_bytes", None)
        if isinstance(harvested, int) and harvested >= 0:
            return harvested
    size = getattr(media, "size", None)
    if isinstance(size, int) and size >= 0:
        return size
    return None


def _duration_seconds_from_slim(media: Any) -> float | None:
    duration = getattr(media, "duration", None)
    if isinstance(duration, (int, float)) and duration >= 0 and duration == duration:
        return float(duration)
    return None


def _content_type_from_slim(media: Any) -> str | None:
    harvest = getattr(media, "harvest", None)
    if harvest is not None:
        display_ct = getattr(harvest, "display_content_type", None)
        if isinstance(display_ct, str) and display_ct.strip():
            return display_ct.strip()
        ct = getattr(harvest, "content_type", None)
        if isinstance(ct, str) and ct.strip():
            return ct.strip()
    mime = getattr(media, "mime_type", None)
    if isinstance(mime, str) and mime.strip():
        return mime.strip()
    preview_mime = getattr(media, "preview_mime", None)
    if isinstance(preview_mime, str) and preview_mime.strip():
        return preview_mime.strip()
    kind = classify_downloadable_kind(media)
    if kind == "image":
        return "image/jpeg"
    return None


def _original_content_type_from_slim(media: Any) -> str | None:
    return harvest_original_content_type(media)


def _thumb_url_from_slim(media: Any) -> str | None:
    raw = getattr(media, "stripped_thumb", None)
    if isinstance(raw, str) and raw.startswith("data:image/"):
        return raw
    return None


async def build_message_media_preview(
    media_raw: Any,
    *,
    region: str,
    status_override: MessageMediaStatus | None = None,
    include_url: bool | None = None,
    failure_reason: MessageMediaFailureReason | None = None,
    bucket: str | None = None,
    persist_display: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> MessageMediaPreview:
    """Map persisted slim media JSONB to a preview (presign when already harvested).

    Documents/audio default to metadata-only (no presigned URL) so message lists do
    not push file URLs to the client; pass ``include_url=True`` from the on-demand
    media endpoint after the user requests a download.
    """
    slim = parse_slim_media(media_raw)
    if bucket and include_url is not False and slim is not None and harvest_needs_web_display(slim):
        updated = await ensure_harvest_web_display(slim, bucket=bucket, region=region)
        if persist_display is not None:
            await persist_display(updated)
        media_raw = updated
        slim = parse_slim_media(media_raw)

    kind = classify_downloadable_kind(slim)
    if kind is None or slim is None:
        return MessageMediaPreview(status="none")

    filename = _filename_from_slim(slim)
    content_type = _content_type_from_slim(slim)
    original_content_type = _original_content_type_from_slim(slim)
    width, height = _dims_from_slim(slim)
    meta = {
        "kind": kind,
        "filename": filename,
        "content_type": content_type,
        "original_content_type": original_content_type,
        "width": width,
        "height": height,
        "size_bytes": _size_bytes_from_slim(slim),
        "duration_seconds": _duration_seconds_from_slim(slim),
        "thumb_url": _thumb_url_from_slim(slim),
    }
    if kind == "webpage":
        page_url = getattr(slim, "url", None)
        site_name = getattr(slim, "site_name", None)
        description = getattr(slim, "description", None)
        if isinstance(page_url, str) and page_url.strip():
            meta["page_url"] = page_url.strip()
        if isinstance(site_name, str) and site_name.strip():
            meta["site_name"] = site_name.strip()
        if isinstance(description, str) and description.strip():
            meta["description"] = description.strip()
    if status_override == "unavailable":
        return MessageMediaPreview(
            status="unavailable",
            failure_reason=failure_reason,
            **meta,
        )

    want_url = (
        include_url
        if include_url is not None
        else kind
        not in (
            "document",
            "audio",
        )
    )

    s3_uri = harvest_display_s3_uri(slim)
    if s3_uri and region.strip():
        if not want_url:
            return MessageMediaPreview(status="ready", url=None, **meta)
        try:
            url = await presign_s3_uri(s3_uri, region=region)
        except Exception:
            lg.exception("Failed to presign message media uri=%s", s3_uri)
            return MessageMediaPreview(
                status="unavailable",
                failure_reason="failed",
                **meta,
            )
        return MessageMediaPreview(status="ready", url=url, **meta)
    if s3_uri:
        # Region missing: expose pending rather than a non-HTTPS s3 URI.
        return MessageMediaPreview(status="pending", **meta)
    if needs_download(slim):
        return MessageMediaPreview(status="pending", **meta)
    if kind == "webpage":
        return MessageMediaPreview(status="ready", url=None, **meta)
    return MessageMediaPreview(status="none")


def _entity_for_peer(
    *,
    peer_external_id: str,
    peer_type: str,
    access_hash: int | None,
) -> InputPeerChannel | PeerChat:
    signed = int(peer_external_id)
    if peer_type in ("channel", "megagroup"):
        raw_channel_id = channel_id_from_signed_peer_id(signed)
        if raw_channel_id is None:
            raise ValueError(f"Peer external_id={peer_external_id!r} is not a channel/megagroup")
        if access_hash is None:
            raise ValueError(f"Missing access_hash for peer {peer_external_id}")
        return InputPeerChannel(raw_channel_id, access_hash)
    if peer_type == "chat":
        chat_id = -signed if signed < 0 else signed
        return PeerChat(chat_id)
    raise ValueError(f"Unsupported peer_type={peer_type!r}")


async def _load_message_rows_by_keys(
    conn: Any,
    keys: list[tuple[str, str]],
) -> dict[str, dict[str, Any]]:
    if not keys:
        return {}
    values = sql.SQL(", ").join(
        sql.SQL("({}::uuid, {}::timestamptz)").format(sql.Placeholder(), sql.Placeholder()) for _ in keys
    )
    query = sql.SQL(
        """
        SELECT
          tm.id,
          tm.message_id,
          tm.source_external_id::text AS source_external_id,
          tm.peer_type,
          tm.media,
          tm.content,
          tm.date,
          tm.archive_key,
          tm.archive_row
        FROM telegram_messages tm
        JOIN (VALUES {}) AS k(id, date) ON tm.id = k.id AND tm.date = k.date
        """
    ).format(values)
    params: list[str] = [value for key in keys for value in key]
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()
    out_rows = [dict(row) for row in rows]
    # Cold-archive months strip fat fields from Postgres; harvest needs the
    # Parquet body (same as list_peer_messages / keyword hydrate).
    try:
        from msgstore import (
            get_message_archive_prefix,
            get_message_archive_sink,
            hydrate_message_rows_async,
        )

        await hydrate_message_rows_async(
            out_rows,
            get_message_archive_sink(),
            prefix=get_message_archive_prefix(),
        )
    except Exception:
        lg.warning("message archive hydrate failed for message_media keys", exc_info=True)
    return {str(row["id"]): row for row in out_rows}


async def _persist_media(
    conn: Any,
    *,
    row_id: UUID,
    date: datetime,
    media: dict[str, Any],
) -> None:
    await persist_message_media(conn, row_id=row_id, date=date, media=media)


async def _download_peer_group(
    *,
    conn: Any,
    pool: ApiSessionPool,
    peer_external_id: str,
    peer_type: str,
    rows: list[dict[str, Any]],
    bucket: str,
    region: str,
    on_progress: MediaDownloadProgress | None = None,
) -> dict[str, MessageMediaPreview]:
    out: dict[str, MessageMediaPreview] = {}
    picked = await connected_peer_role(
        pool,
        conn,
        peer_external_id=peer_external_id,
        skip_harvester_ids=set(),
    )
    if picked is None:
        for row in rows:
            out[str(row["id"])] = await build_message_media_preview(
                row.get("media"),
                region=region,
                status_override="unavailable",
                failure_reason="failed",
                include_url=True,
            )
        return out

    _client, harvester_id, access_hash, _harvester_external_id = picked
    async with pool.acquire_client_for_peer_role(
        conn,
        harvester_id=harvester_id,
        access_hash=access_hash,
    ) as (client, harvester_id, access_hash):
        if client is None:
            for row in rows:
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason="failed",
                    include_url=True,
                )
            return out

        try:
            entity = _entity_for_peer(
                peer_external_id=peer_external_id,
                peer_type=peer_type,
                access_hash=access_hash,
            )
        except ValueError:
            lg.exception("Cannot build entity for peer %s", peer_external_id)
            for row in rows:
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason="failed",
                    include_url=True,
                )
            return out

        id_to_row = {int(r["message_id"]): r for r in rows}
        message_ids = list(id_to_row.keys())
        try:
            messages = await client.get_messages(entity, ids=message_ids)
        except FloodWaitError:
            raise
        except Exception:
            lg.exception("get_messages failed for peer %s", peer_external_id)
            for row in rows:
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason="failed",
                    include_url=True,
                )
            return out

        if not isinstance(messages, list):
            messages = [messages]

        seen_ids: set[int] = set()
        harvested_dates: list[Any] = []
        for msg in messages:
            if msg is None:
                continue
            row = id_to_row.get(msg.id)
            if row is None:
                continue
            seen_ids.add(msg.id)
            slim = parse_slim_media(row.get("media"))
            if slim is None or not needs_download(slim):
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    include_url=True,
                )
                continue
            kind = classify_downloadable_kind(slim)
            size_bytes = _size_bytes_from_slim(slim)
            filename = _filename_from_slim(slim)
            lg.info(
                "message_media download start message_id=%s peer=%s kind=%s size_bytes=%s filename=%s",
                msg.id,
                peer_external_id,
                kind,
                size_bytes,
                filename,
            )
            try:
                row_id = str(row["id"])

                async def _progress(
                    loaded: int,
                    total: int | None,
                    *,
                    _id: str = row_id,
                ) -> None:
                    if on_progress is not None:
                        await on_progress(_id, loaded, total)

                async with timed("telegram", "download_media"):
                    result = await download_and_upload_message_media(
                        client,
                        msg,
                        int(peer_external_id),
                        bucket,
                        region,
                        entity=entity,
                        on_progress=_progress if on_progress is not None else None,
                        canonical_s3_uri_for_phash=lambda phash: lookup_canonical_s3_uri(conn, phash),
                    )
            except FloodWaitError:
                raise
            except MessageMediaDownloadError as exc:
                lg.warning(
                    "Download failed msg %s peer %s reason=%s: %s",
                    msg.id,
                    peer_external_id,
                    exc.reason,
                    exc,
                )
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason=exc.reason,
                    include_url=True,
                )
                lg.info(
                    "message_media download failed message_id=%s kind=%s reason=%s",
                    msg.id,
                    kind,
                    exc.reason,
                )
                continue
            except Exception:
                lg.exception(
                    "Download failed msg %s peer %s",
                    msg.id,
                    peer_external_id,
                )
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason="failed",
                    include_url=True,
                )
                lg.info(
                    "message_media download failed message_id=%s kind=%s",
                    msg.id,
                    kind,
                )
                continue

            if result is None:
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason="failed",
                    include_url=True,
                )
                lg.info(
                    "message_media download empty message_id=%s kind=%s",
                    msg.id,
                    kind,
                )
                continue

            phash_hex = result.get("phash_hex")
            harvest_uri = str(result["uri"])
            if is_dedupable_phash(str(phash_hex) if phash_hex else None):
                try:
                    harvest_uri = await record_harvested_image_blob(
                        conn,
                        phash_hex=str(phash_hex),
                        s3_uri=harvest_uri,
                        display_s3_uri=result.get("display_uri"),
                        reused_canonical=bool(result.get("reused_canonical")),
                        message_id=row["id"],
                        message_date=row.get("date"),
                        source_external_id=peer_external_id,
                        peer_type=peer_type,
                        telegram_message_id=int(row["message_id"]) if row.get("message_id") is not None else None,
                    )
                except Exception:
                    lg.exception("image blob register failed message_id=%s", row.get("id"))
            updated_media = merge_harvest_into_media_dict(
                slim,
                s3_uri=harvest_uri,
                display_s3_uri=result.get("display_uri"),
                size_bytes=result.get("size_bytes"),
                display_size_bytes=result.get("display_size_bytes"),
                content_type=result.get("mime_type"),
                display_content_type=result.get("display_content_type"),
                exif=result.get("exif"),
                phash=str(phash_hex) if phash_hex else None,
            )
            try:
                await _persist_media(
                    conn,
                    row_id=row["id"],
                    date=row["date"],
                    media=updated_media,
                )
            except Exception:
                lg.exception("Failed to persist media for message %s", row["id"])
                out[str(row["id"])] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    status_override="unavailable",
                    failure_reason="failed",
                    include_url=True,
                )
                lg.info(
                    "message_media persist failed message_id=%s kind=%s",
                    msg.id,
                    kind,
                )
                continue

            if row.get("date") is not None:
                harvested_dates.append(row["date"])
                harvest_kind = classify_downloadable_kind(parse_slim_media(updated_media))
                if harvest_kind is not None and peer_type in (
                    "channel",
                    "megagroup",
                    "chat",
                ):
                    try:
                        await record_harvested_media(
                            conn,
                            peer_external_id=peer_external_id,
                            peer_type=peer_type,  # type: ignore[arg-type]
                            message_date=row["date"],
                            kind=harvest_kind,
                        )
                    except Exception:
                        lg.exception(
                            "Failed to bump media coverage after on-demand harvest id=%s",
                            row["id"],
                        )
            out[str(row["id"])] = await build_message_media_preview(
                updated_media,
                region=region,
                include_url=True,
            )

        for mid, row in id_to_row.items():
            if mid in seen_ids:
                continue
            out[str(row["id"])] = await build_message_media_preview(
                row.get("media"),
                region=region,
                status_override="unavailable",
                failure_reason="missing",
                include_url=True,
            )

        if harvested_dates and peer_type in (
            "channel",
            "megagroup",
            "chat",
        ):
            await record_on_demand_media_download_coverage(
                conn,
                peer_external_id=peer_external_id,
                peer_type=peer_type,  # type: ignore[arg-type]
                harvested_dates=harvested_dates,
            )
        return out


async def get_or_fetch_message_media(
    conn: Any,
    *,
    keys: list[tuple[str, str]],
    pool: ApiSessionPool | None,
    bucket: str,
    region: str,
    telegram_configured: bool,
    on_progress: MediaDownloadProgress | None = None,
    allow_harvest: bool = True,
    unlock_count: int | None = None,
) -> dict[str, MessageMediaPreview]:
    """Return previews for ``(id, date)`` keys, downloading from Telegram when needed.

    When ``unlock_count`` is set (Free plan), only harvest messages among the
    newest ``unlock_count`` downloadable media rows per peer. Already-harvested
    S3 previews are always returned.
    """
    if len(keys) > _MAX_KEYS:
        keys = keys[:_MAX_KEYS]

    from services.api.src.crud.supabase.messages import peer_media_id_floor

    rows_by_id = await _load_message_rows_by_keys(conn, keys)
    result: dict[str, MessageMediaPreview] = {}
    pending_by_peer: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    unlock_floor_by_peer: dict[str, int | None] = {}

    for mid, date in keys:
        row = rows_by_id.get(mid)
        if row is None:
            result[mid] = MessageMediaPreview(
                status="unavailable",
                failure_reason="missing",
            )
            continue
        slim = parse_slim_media(row.get("media"))
        kind = classify_downloadable_kind(slim)
        if kind is None or slim is None:
            result[mid] = MessageMediaPreview(status="none")
            continue
        if harvest_s3_uri(slim):
            row_id = row["id"]
            row_date = row["date"]
            peer_external_id = str(row["source_external_id"])
            include_harvest_url = True
            if unlock_count is not None and kind in ("image", "gif", "video"):
                if peer_external_id not in unlock_floor_by_peer:
                    unlock_floor_by_peer[peer_external_id] = await peer_media_id_floor(
                        conn,
                        peer_external_id,
                        unlock_count,
                        kind="visual",
                    )
                floor = unlock_floor_by_peer[peer_external_id]
                if floor is not None and int(row["message_id"]) < floor:
                    include_harvest_url = False

            async def _persist_display(
                updated: dict[str, Any],
                *,
                _row_id: UUID | str = row_id,
                _row_date: datetime = row_date,
            ) -> None:
                await _persist_media(
                    conn,
                    row_id=_row_id,
                    date=_row_date,
                    media=updated,
                )

            result[mid] = await build_message_media_preview(
                row.get("media"),
                region=region,
                include_url=include_harvest_url,
                bucket=bucket if include_harvest_url else None,
                persist_display=_persist_display if include_harvest_url else None,
            )
            continue
        if not needs_download(slim):
            # Webpage link cards (and other non-harvest kinds) still need previews.
            result[mid] = await build_message_media_preview(
                row.get("media"),
                region=region,
                include_url=True,
            )
            continue
        if not telegram_configured or pool is None:
            result[mid] = await build_message_media_preview(
                row.get("media"),
                region=region,
                status_override="unavailable",
                failure_reason="failed",
                include_url=True,
            )
            continue
        peer_type = str(row.get("peer_type") or "")
        peer_external_id = str(row["source_external_id"])
        if unlock_count is not None:
            # Free unlock harvest is for visual previews only.
            if kind not in ("image", "gif", "video"):
                result[mid] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    include_url=False,
                )
                continue
            if peer_external_id not in unlock_floor_by_peer:
                unlock_floor_by_peer[peer_external_id] = await peer_media_id_floor(
                    conn,
                    peer_external_id,
                    unlock_count,
                    kind="visual",
                )
            floor = unlock_floor_by_peer[peer_external_id]
            message_id = int(row["message_id"])
            if floor is not None and message_id < floor:
                result[mid] = await build_message_media_preview(
                    row.get("media"),
                    region=region,
                    include_url=False,
                )
                continue
        pending_by_peer[(peer_external_id, peer_type)].append(row)

    if not pending_by_peer:
        return result

    if not allow_harvest:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "feature_not_in_plan",
                "detail": "Media download is not included in your plan. Upgrade to Pro to unlock it.",
                "meter": str(Meter.TELEGRAM_MEDIA_DOWNLOAD),
            },
        )

    assert pool is not None  # pending rows only queued when pool is available

    # One connection cannot run concurrent queries; process peers sequentially.
    # Telethon downloads within a peer stay sequential in `_download_peer_group`.
    for peer_key, peer_rows in pending_by_peer.items():
        peer_external_id, peer_type = peer_key
        try:
            part = await _download_peer_group(
                conn=conn,
                pool=pool,
                peer_external_id=peer_external_id,
                peer_type=peer_type,
                rows=peer_rows,
                bucket=bucket,
                region=region,
                on_progress=on_progress,
            )
            result.update(part)
        except FloodWaitError:
            raise
        except Exception:
            lg.exception("message_media peer download failed peer=%s", peer_external_id)
            for row in peer_rows:
                mid = str(row["id"])
                if mid not in result:
                    result[mid] = await build_message_media_preview(
                        row.get("media"),
                        region=region,
                        status_override="unavailable",
                        failure_reason="failed",
                        include_url=True,
                    )
    return result
