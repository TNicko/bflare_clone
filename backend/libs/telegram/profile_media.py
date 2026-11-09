from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
from typing import Any, cast

from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
from telethon import TelegramClient
from telethon.tl.types import (
    Document,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
    SavedStarGift,
    StoryItem,
    TypeStoryItem,
    TypeMessageMedia,
)
from telethon.tl.types.payments import SavedStarGifts
from telethon.tl.types.stories import Stories

from aws.s3 import upload_file as upload_s3_file
from telegram.media import (
    TelethonMediaToDownload,
    merge_harvest_into_media_dict,
    media_attachment_name,
)
from telegram.utils import json_safe_value, from_telethon

lg = logging.getLogger(__name__)

PROFILE_MEDIA_PREFIX = "users/telegram"


def _get_document_filename(doc: Document) -> str:
    for attr in getattr(doc, "attributes", []) or []:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name or ""
    return ""


def _safe_attachment_name(filename: str, index: int) -> str:
    base = os.path.basename(filename) or f"file-{index}"
    base = re.sub(r"[^\w.\-]+", "_", base)
    return base[:200] if base else f"file-{index}"


def build_profile_media_key(
    user_external_id: str,
    *,
    kind: str,
    item_id: int,
    index: int,
    filename: str,
) -> str:
    safe_name = _safe_attachment_name(filename, index)
    stem = media_attachment_name(item_id, index)
    return f"{PROFILE_MEDIA_PREFIX}/{user_external_id}/{kind}/{stem}-{safe_name}"


def harvest_s3_uri_from_dict(node: dict[str, Any] | None) -> str | None:
    if not isinstance(node, dict):
        return None
    harvest = node.get("harvest")
    if not isinstance(harvest, dict):
        return None
    uri = harvest.get("s3_uri")
    return str(uri) if uri else None


def _media_dict_needs_harvest(media: dict[str, Any] | None) -> bool:
    if not isinstance(media, dict):
        return False
    tl = media.get("tl")
    # Link cards are not harvested; only photo/document attachments.
    if tl not in ("MessageMediaPhoto", "MessageMediaDocument"):
        return False
    return harvest_s3_uri_from_dict(media) is None


def pinned_stories_need_harvest(data: dict[str, Any] | None) -> bool:
    if not isinstance(data, dict):
        return False
    for story in data.get("stories") or []:
        if not isinstance(story, dict):
            continue
        if _media_dict_needs_harvest(story.get("media")):
            return True
    return False


def saved_star_gifts_need_harvest(data: dict[str, Any] | None) -> bool:
    if not isinstance(data, dict):
        return False
    for entry in data.get("gifts") or []:
        if not isinstance(entry, dict):
            continue
        gift = entry.get("gift")
        if not isinstance(gift, dict):
            continue
        sticker = gift.get("sticker")
        if isinstance(sticker, dict) and harvest_s3_uri_from_dict(sticker) is None:
            return True
    return False


def _document_extension_and_content_type(
    doc: Document | None,
) -> tuple[str, str | None]:
    if doc is None:
        return "bin", None

    mime = (getattr(doc, "mime_type", None) or "").strip().lower()
    if mime.startswith("video/"):
        subtype = mime.split("/", 1)[1].split("+")[0].split(";")[0]
        ext = {"quicktime": "mov", "x-m4v": "m4v"}.get(subtype, subtype)
        return ext or "mp4", mime
    if mime.startswith("image/"):
        subtype = mime.split("/", 1)[1].split("+")[0]
        ext = {"jpeg": "jpg"}.get(subtype, subtype)
        return ext or "jpg", mime

    for attr in getattr(doc, "attributes", []) or []:
        if isinstance(attr, DocumentAttributeVideo):
            return "mp4", mime or "video/mp4"

    filename = _get_document_filename(doc)
    if filename and "." in filename:
        ext = filename.rsplit(".", 1)[-1]
        return ext, mime or None

    return "bin", mime or None


def _resolve_telethon_media(
    media: TypeMessageMedia | None,
) -> tuple[TelethonMediaToDownload, str] | None:
    if media is None:
        return None

    if isinstance(media, MessageMediaWebPage):
        # Link cards are not profile media attachments.
        return None

    if isinstance(media, MessageMediaPhoto):
        return media, "jpg"

    if isinstance(media, MessageMediaDocument):
        doc = getattr(media, "document", None)
        ext, _ = _document_extension_and_content_type(doc)
        return media, ext

    return None


async def _upload_local_file(
    *,
    bucket: str,
    region: str,
    key: str,
    filepath: str,
    content_type: str | None = None,
) -> str:
    uri = await upload_s3_file(
        bucket=bucket,
        key=key,
        filepath=filepath,
        region=region,
        content_type=content_type,
    )
    lg.info("Uploaded profile media to %s", uri)
    return uri


async def _download_telethon_media_to_file(
    client: TelegramClient,
    media: TelethonMediaToDownload,
    filepath: str,
) -> bool:
    chunk_size = 262144
    with open(filepath, "wb") as handle:
        async for chunk in client.iter_download(
            media,
            chunk_size=chunk_size,
            request_size=chunk_size,
        ):
            handle.write(chunk)
    return os.path.isfile(filepath) and os.path.getsize(filepath) > 0


async def _download_document_thumb_to_file(
    client: TelegramClient,
    document: Document,
    filepath: str,
) -> bool:
    thumb = await client.download_media(
        cast(Any, MessageMediaDocument(document=document)),
        file=filepath,
        thumb=-1,
    )
    return thumb is not None and os.path.isfile(filepath) and os.path.getsize(filepath) > 0


async def harvest_story_item_media(
    client: TelegramClient,
    story: StoryItem,
    *,
    user_external_id: str,
    bucket: str,
    region: str,
) -> dict[str, Any] | None:
    """Download story media and return the media dict with ``harvest`` set."""
    resolved = _resolve_telethon_media(story.media)
    if resolved is None:
        return None

    media_to_download, ext = resolved
    story_id = story.id
    content_type: str | None = None
    if isinstance(story.media, MessageMediaDocument):
        _, content_type = _document_extension_and_content_type(
            getattr(story.media, "document", None),
        )
    filename = f"story.{ext.lstrip('.')}"
    key = build_profile_media_key(
        user_external_id,
        kind="stories",
        item_id=story_id,
        index=0,
        filename=filename,
    )

    tmp_dir = tempfile.mkdtemp(prefix=f"profile_story_{user_external_id}_{story_id}_")
    try:
        filepath = os.path.join(tmp_dir, filename)
        if not await _download_telethon_media_to_file(client, media_to_download, filepath):
            lg.warning(
                "Profile story media download produced no file user=%s story_id=%s",
                user_external_id,
                story_id,
            )
            return None
        uri = await _upload_local_file(
            bucket=bucket,
            region=region,
            key=key,
            filepath=filepath,
            content_type=content_type,
        )
    except Exception:
        lg.exception(
            "Profile story media harvest failed user=%s story_id=%s",
            user_external_id,
            story_id,
        )
        return None
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    media_dict = json_safe_value(from_telethon(story.media))
    if not isinstance(media_dict, dict):
        return None
    return merge_harvest_into_media_dict(media_dict, s3_uri=uri)


async def harvest_saved_star_gift_media(
    client: TelegramClient,
    entry: SavedStarGift,
    *,
    user_external_id: str,
    bucket: str,
    region: str,
) -> dict[str, Any] | None:
    """Download gift sticker preview and return sticker dict with ``harvest``."""
    gift = entry.gift
    sticker = getattr(gift, "sticker", None) if gift is not None else None
    if not isinstance(sticker, Document):
        return None

    gift_id = getattr(gift, "id", None) or getattr(sticker, "id", 0)
    saved_id = getattr(entry, "saved_id", None)
    item_id = int(saved_id or gift_id or 0)
    filename = "gift-preview.jpg"
    key = build_profile_media_key(
        user_external_id,
        kind="gifts",
        item_id=item_id,
        index=0,
        filename=filename,
    )

    tmp_dir = tempfile.mkdtemp(prefix=f"profile_gift_{user_external_id}_{item_id}_")
    try:
        filepath = os.path.join(tmp_dir, filename)
        if not await _download_document_thumb_to_file(client, sticker, filepath):
            resolved = _resolve_telethon_media(MessageMediaDocument(document=sticker))
            if resolved is None:
                lg.warning(
                    "Profile gift media download failed user=%s gift_id=%s",
                    user_external_id,
                    item_id,
                )
                return None
            media_to_download, ext = resolved
            filepath = os.path.join(tmp_dir, f"gift.{ext}")
            if not await _download_telethon_media_to_file(client, media_to_download, filepath):
                lg.warning(
                    "Profile gift media download produced no file user=%s gift_id=%s",
                    user_external_id,
                    item_id,
                )
                return None
            key = build_profile_media_key(
                user_external_id,
                kind="gifts",
                item_id=item_id,
                index=0,
                filename=os.path.basename(filepath),
            )
        uri = await _upload_local_file(
            bucket=bucket,
            region=region,
            key=key,
            filepath=filepath,
        )
    except Exception:
        lg.exception(
            "Profile gift media harvest failed user=%s gift_id=%s",
            user_external_id,
            item_id,
        )
        return None
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    gift_dict = json_safe_value(from_telethon(gift))
    if not isinstance(gift_dict, dict):
        return None
    sticker_dict = gift_dict.get("sticker")
    if not isinstance(sticker_dict, dict):
        return None
    gift_dict["sticker"] = merge_harvest_into_media_dict(sticker_dict, s3_uri=uri)
    return gift_dict


def _merge_story_media(
    story_dict: dict[str, Any],
    harvested_media: dict[str, Any] | None,
) -> dict[str, Any]:
    out = dict(story_dict)
    if harvested_media is not None:
        out["media"] = harvested_media
    return out


def _merge_gift_sticker(
    entry_dict: dict[str, Any],
    harvested_gift: dict[str, Any] | None,
) -> dict[str, Any]:
    out = dict(entry_dict)
    if harvested_gift is not None:
        out["gift"] = harvested_gift
    return out


#: Cap Telegram→S3 downloads per API refresh so the client can show the list
#: early and fill media progressively across polls.
PROFILE_MEDIA_HARVEST_BATCH = 1


async def harvest_pinned_stories_payload(
    client: TelegramClient,
    stories: Stories,
    payload: dict[str, Any],
    *,
    user_external_id: str,
    bucket: str,
    region: str,
    max_harvest: int | None = PROFILE_MEDIA_HARVEST_BATCH,
) -> tuple[dict[str, Any], bool]:
    """Harvest story media. Returns ``(payload, more_remaining)``.

    When ``max_harvest`` is set, stops after that many new downloads so callers
    can return to the client and continue on the next refresh.
    """
    story_items = list(stories.stories or [])
    payload_stories = payload.get("stories")
    if not isinstance(payload_stories, list):
        return payload, False

    by_id: dict[int, TypeStoryItem] = {int(item.id): item for item in story_items}
    harvested = 0
    skipped = 0
    failed = 0
    more_remaining = False
    merged_stories: list[Any] = []

    for raw_story in payload_stories:
        if not isinstance(raw_story, dict):
            merged_stories.append(raw_story)
            continue
        story_id = raw_story.get("id")
        story_tl = by_id.get(int(story_id)) if story_id is not None else None
        media = raw_story.get("media")
        needs = _media_dict_needs_harvest(media if isinstance(media, dict) else None)
        if not isinstance(story_tl, StoryItem) or not needs:
            merged_stories.append(raw_story)
            if isinstance(media, dict) and harvest_s3_uri_from_dict(media):
                skipped += 1
            continue

        if max_harvest is not None and harvested >= max_harvest:
            more_remaining = True
            merged_stories.append(raw_story)
            continue

        harvested_media = await harvest_story_item_media(
            client,
            story_tl,
            user_external_id=user_external_id,
            bucket=bucket,
            region=region,
        )
        if harvested_media is None:
            failed += 1
            merged_stories.append(raw_story)
            continue
        harvested += 1
        merged_stories.append(_merge_story_media(raw_story, harvested_media))

    out = dict(payload)
    out["stories"] = merged_stories
    if not more_remaining:
        more_remaining = pinned_stories_need_harvest(out)
    lg.info(
        "Profile story media harvest user=%s harvested=%d skipped=%d failed=%d total=%d more_remaining=%s",
        user_external_id,
        harvested,
        skipped,
        failed,
        len(payload_stories),
        more_remaining,
    )
    return out, more_remaining


async def harvest_saved_star_gifts_payload(
    client: TelegramClient,
    gifts: SavedStarGifts,
    payload: dict[str, Any],
    *,
    user_external_id: str,
    bucket: str,
    region: str,
    max_harvest: int | None = PROFILE_MEDIA_HARVEST_BATCH,
) -> tuple[dict[str, Any], bool]:
    """Harvest gift sticker media. Returns ``(payload, more_remaining)``."""
    gift_items = list(gifts.gifts or [])
    payload_gifts = payload.get("gifts")
    if not isinstance(payload_gifts, list):
        return payload, False

    by_saved: list[SavedStarGift] = gift_items
    harvested = 0
    skipped = 0
    failed = 0
    more_remaining = False
    merged_gifts: list[Any] = []

    for index, raw_gift in enumerate(payload_gifts):
        if not isinstance(raw_gift, dict):
            merged_gifts.append(raw_gift)
            continue
        gift_inner = raw_gift.get("gift")
        sticker = gift_inner.get("sticker") if isinstance(gift_inner, dict) else None
        if isinstance(sticker, dict) and harvest_s3_uri_from_dict(sticker):
            skipped += 1
            merged_gifts.append(raw_gift)
            continue
        if index >= len(by_saved):
            merged_gifts.append(raw_gift)
            continue

        if max_harvest is not None and harvested >= max_harvest:
            more_remaining = True
            merged_gifts.append(raw_gift)
            continue

        harvested_gift = await harvest_saved_star_gift_media(
            client,
            by_saved[index],
            user_external_id=user_external_id,
            bucket=bucket,
            region=region,
        )
        if harvested_gift is None:
            failed += 1
            merged_gifts.append(raw_gift)
            continue
        harvested += 1
        merged_gifts.append(_merge_gift_sticker(raw_gift, harvested_gift))

    out = dict(payload)
    out["gifts"] = merged_gifts
    if not more_remaining:
        more_remaining = saved_star_gifts_need_harvest(out)
    lg.info(
        "Profile gift media harvest user=%s harvested=%d skipped=%d failed=%d total=%d more_remaining=%s",
        user_external_id,
        harvested,
        skipped,
        failed,
        len(payload_gifts),
        more_remaining,
    )
    return out, more_remaining
