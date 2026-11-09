"""Download and persist Telegram peer profile photos to S3."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ForbiddenError, RPCError, UserPrivacyRestrictedError
from telethon.tl.types import (
    Channel,
    Chat,
    InputPeerChannel,
    InputPeerUser,
    PeerChat,
    Photo,
    PhotoEmpty,
    User,
    VideoSize,
)

from aws.s3 import make_s3_uri, object_exists, s3_configured, upload_file
from db.crud.telegram.field_history import (
    list_photo_harvest_index,
    persist_profile_photo_harvest_to_field_history,
    photo_observed_at_from_tl,
)
from db.crud.telegram.peers import update_peer_profile_photo_harvest
from db.schemas.general.primitives import TelegramPeerTypeSchema
from phash.core import phash_hex_from_bytes
from telegram.utils import channel_id_from_signed_peer_id, json_safe_value, from_telethon

logger = logging.getLogger(__name__)

USER_PROFILE_PHOTOS_PREFIX = "users/telegram"
COMMUNITY_PROFILE_PHOTOS_PREFIX = "communities/telegram"

_DEFINITIVE_ERRORS: tuple[type[RPCError], ...] = (
    UserPrivacyRestrictedError,
    ForbiddenError,
)


@dataclass(slots=True)
class ProfilePhotoFetchResult:
    harvested: int = 0
    skipped: int = 0
    failed: int = 0
    fetch_pending: bool = False
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PeerProfilePhotoRow:
    external_id: str
    peer_type: TelegramPeerTypeSchema
    access_hash: int | None
    harvester_id: Any
    harvester_external_id: str
    photo: dict[str, Any] | None = None
    bot: bool = False


def profile_photo_s3_prefix(peer_type: TelegramPeerTypeSchema | str) -> str:
    if peer_type == "user":
        return USER_PROFILE_PHOTOS_PREFIX
    return COMMUNITY_PROFILE_PHOTOS_PREFIX


def build_profile_photo_s3_key(
    external_id: str,
    photo_id: int,
    *,
    peer_type: TelegramPeerTypeSchema | str,
    ext: str,
) -> str:
    safe_ext = ext.lstrip(".") or "bin"
    prefix = profile_photo_s3_prefix(peer_type)
    return f"{prefix}/{external_id}/profile_photos/{photo_id}.{safe_ext}"


def photo_id_from_tl(photo: Any) -> int | None:
    if photo is None:
        return None
    if isinstance(photo, Photo):
        return int(photo.id)
    if hasattr(photo, "model_dump"):
        try:
            dumped = photo.model_dump(mode="json")
            if isinstance(dumped, dict):
                return photo_id_from_tl(dumped)
        except Exception:
            pass
    if isinstance(photo, dict):
        tl = photo.get("tl")
        if tl == "Photo":
            raw = photo.get("id")
        else:
            raw = photo.get("photo_id")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    tl = getattr(photo, "tl", None)
    if tl == "Photo" and hasattr(photo, "id"):
        try:
            return int(photo.id)
        except (TypeError, ValueError):
            return None
    if hasattr(photo, "photo_id"):
        try:
            return int(photo.photo_id)
        except (TypeError, ValueError):
            return None
    return None


def photo_ids_equal(left: Any, right: Any) -> bool:
    """Compare Telegram photo ids safely (handles pydantic models and large ints)."""
    left_id = photo_id_from_tl(left) if not isinstance(left, int) else left
    right_id = photo_id_from_tl(right) if not isinstance(right, int) else right
    if left_id is None or right_id is None:
        return False
    return str(left_id) == str(right_id)


def photo_dict_for_harvest(photo: Any) -> dict[str, Any] | None:
    if photo is None:
        return None
    if isinstance(photo, dict):
        return photo
    if hasattr(photo, "model_dump"):
        dumped = photo.model_dump(mode="json")
        return dumped if isinstance(dumped, dict) else None
    return None


def _thumb_sort_key(thumb: Any) -> tuple[int, int]:
    tl = getattr(thumb, "tl", None) or getattr(thumb, "_", None)
    if tl in ("PhotoStrippedSize", "PhotoCachedSize"):
        data = getattr(thumb, "bytes", None) or getattr(thumb, "data", None) or b""
        return 1, len(data)
    if tl == "PhotoSize":
        return 1, int(getattr(thumb, "size", 0) or 0)
    if tl == "PhotoSizeProgressive":
        sizes = getattr(thumb, "sizes", None) or []
        return 1, max(sizes) if sizes else 0
    if tl == "VideoSize" or isinstance(thumb, VideoSize):
        return 2, int(getattr(thumb, "size", 0) or 0)
    return 0, 0


def _largest_thumb(photo: Photo) -> Any | None:
    thumbs = list(photo.sizes or []) + list(photo.video_sizes or [])
    if not thumbs:
        return None
    thumbs = sorted(thumbs, key=_thumb_sort_key)
    return thumbs[-1]


def resolve_profile_photo_variant(photo: Photo) -> tuple[str, str, str]:
    """Return ``(ext, content_type, media_kind)`` for the downloadable profile photo."""
    thumb = _largest_thumb(photo)
    if isinstance(thumb, VideoSize):
        return "mp4", "video/mp4", "video"

    video_sizes = list(photo.video_sizes or [])
    has_video_size = any(isinstance(v, VideoSize) for v in video_sizes)
    markup_only = video_sizes and not has_video_size
    if markup_only:
        logger.warning(
            "Profile photo id=%s has video markup without VideoSize; falling back to static image",
            photo.id,
        )

    return "jpg", "image/jpeg", "image"


def harvest_s3_uri_from_photo_dict(photo_dict: dict[str, Any] | None) -> str | None:
    if not isinstance(photo_dict, dict):
        return None
    harvest = photo_dict.get("harvest")
    if not isinstance(harvest, dict):
        return None
    uri = harvest.get("s3_uri")
    return str(uri) if uri else None


def harvest_phash_from_photo_dict(photo_dict: dict[str, Any] | None) -> str | None:
    if not isinstance(photo_dict, dict):
        return None
    harvest = photo_dict.get("harvest")
    if not isinstance(harvest, dict):
        return None
    phash = harvest.get("phash")
    return str(phash) if phash else None


def merge_harvest_into_photo_dict(
    photo_dict: dict[str, Any],
    *,
    s3_uri: str,
    content_type: str | None = None,
    media_kind: str | None = None,
    phash: str | None = None,
) -> dict[str, Any]:
    out = dict(photo_dict)
    existing = out.get("harvest")
    harvest: dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}
    harvest["s3_uri"] = s3_uri
    if content_type:
        harvest["content_type"] = content_type
    if media_kind:
        harvest["media_kind"] = media_kind
    if phash is not None:
        harvest["phash"] = phash
    out["harvest"] = harvest
    return out


def entity_for_peer(
    peer: PeerProfilePhotoRow,
) -> InputPeerUser | InputPeerChannel | PeerChat:
    signed = int(peer.external_id)
    if peer.peer_type == "user":
        if peer.access_hash is None:
            raise ValueError(f"Missing access_hash for user {peer.external_id}")
        return InputPeerUser(signed, peer.access_hash)
    if peer.peer_type in ("channel", "megagroup"):
        raw_channel_id = channel_id_from_signed_peer_id(signed)
        if raw_channel_id is None:
            raise ValueError(f"Peer external_id={peer.external_id!r} is not a channel/megagroup signed id")
        if peer.access_hash is None:
            raise ValueError(f"Missing access_hash for peer {peer.external_id}")
        return InputPeerChannel(raw_channel_id, peer.access_hash)
    if peer.peer_type == "chat":
        chat_id = -signed if signed < 0 else signed
        return PeerChat(chat_id)
    raise ValueError(f"Unsupported peer_type={peer.peer_type!r}")


def entity_for_materialize_entity(
    entity: User | Channel | Chat,
    *,
    peer_type: TelegramPeerTypeSchema | str,
    external_id: str,
) -> InputPeerUser | InputPeerChannel | PeerChat:
    signed = int(external_id)
    if peer_type == "user":
        if not isinstance(entity, User):
            raise TypeError("entity must be User")
        if entity.access_hash is None:
            raise ValueError(f"Missing access_hash for user {external_id}")
        return InputPeerUser(entity.id, entity.access_hash)
    if peer_type in ("channel", "megagroup"):
        if not isinstance(entity, Channel):
            raise TypeError("entity must be Channel")
        raw_channel_id = channel_id_from_signed_peer_id(signed)
        if raw_channel_id is None:
            raise ValueError(f"external_id={external_id!r} is not a channel signed id")
        if entity.access_hash is None:
            raise ValueError(f"Missing access_hash for channel {external_id}")
        return InputPeerChannel(raw_channel_id, entity.access_hash)
    if peer_type == "chat":
        if not isinstance(entity, Chat):
            raise TypeError("entity must be Chat")
        chat_id = -signed if signed < 0 else signed
        return PeerChat(chat_id)
    raise ValueError(f"Unsupported peer_type={peer_type!r}")


async def harvest_profile_photo(
    client: TelegramClient,
    photo: Photo,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema | str,
    bucket: str,
    region: str,
) -> tuple[str, str, str, str | None] | None:
    """Download a profile ``Photo`` and upload to S3.

    Returns ``(s3_uri, content_type, media_kind, phash_hex)`` for static images.
    """
    if isinstance(photo, PhotoEmpty):
        return None

    photo_id = photo_id_from_tl(photo)
    if photo_id is None:
        return None

    ext, content_type, media_kind = resolve_profile_photo_variant(photo)
    key = build_profile_photo_s3_key(external_id, photo_id, peer_type=peer_type, ext=ext)
    filename = f"{photo_id}.{ext}"
    tmp_dir = tempfile.mkdtemp(prefix=f"profile_photo_{external_id}_{photo_id}_")
    try:
        filepath = os.path.join(tmp_dir, filename)
        # Telethon accepts Photo TL at runtime; stubs only declare MessageLike.
        downloaded = await client.download_media(cast(Any, photo), file=filepath)
        if not downloaded or not os.path.isfile(filepath) or os.path.getsize(filepath) == 0:
            logger.warning(
                "Profile photo download produced no file external_id=%s photo_id=%s",
                external_id,
                photo_id,
            )
            return None
        uri = await upload_file(
            bucket=bucket,
            key=key,
            filepath=filepath,
            region=region,
            content_type=content_type,
        )
        phash_hex: str | None = None
        if media_kind == "image":
            try:
                phash_hex = phash_hex_from_bytes(Path(filepath).read_bytes())
            except Exception:
                logger.exception(
                    "Profile photo pHash failed external_id=%s photo_id=%s",
                    external_id,
                    photo_id,
                )
        return uri, content_type, media_kind, phash_hex
    except Exception:
        logger.exception(
            "Profile photo harvest failed external_id=%s photo_id=%s",
            external_id,
            photo_id,
        )
        return None
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


async def _maybe_skip_harvest(
    *,
    photo_id: int,
    harvest_index: dict[int, str],
    external_id: str,
    peer_type: TelegramPeerTypeSchema | str,
    ext: str,
    bucket: str,
    region: str,
) -> str | None:
    if photo_id in harvest_index:
        return harvest_index[photo_id]
    key = build_profile_photo_s3_key(external_id, photo_id, peer_type=peer_type, ext=ext)
    if await object_exists(bucket=bucket, key=key, region=region):
        return make_s3_uri(bucket, key)
    return None


async def _persist_harvest(
    conn: Any,
    *,
    external_id: str,
    peer_type: TelegramPeerTypeSchema,
    photo: Photo,
    s3_uri: str,
    content_type: str,
    media_kind: str,
    current_photo: dict[str, Any] | None,
    force_current: bool = False,
    phash: str | None = None,
) -> None:
    photo_id = photo_id_from_tl(photo)
    if photo_id is None:
        return

    photo_dict = json_safe_value(from_telethon(photo))
    if not isinstance(photo_dict, dict):
        return
    harvested_photo = merge_harvest_into_photo_dict(
        photo_dict,
        s3_uri=s3_uri,
        content_type=content_type,
        media_kind=media_kind,
        phash=phash,
    )

    is_current = force_current or photo_ids_equal(current_photo, photo_id)
    await persist_profile_photo_harvest_to_field_history(
        conn,
        external_id=external_id,
        peer_type=peer_type,
        photo_id=photo_id,
        photo_value=harvested_photo,
        is_current=is_current,
        observed_at=photo_observed_at_from_tl(photo),
    )

    if is_current:
        peer_photo: dict[str, Any] | Any = photo_dict_for_harvest(current_photo) or harvested_photo
        if isinstance(peer_photo, dict):
            peer_photo = merge_harvest_into_photo_dict(
                peer_photo,
                s3_uri=s3_uri,
                content_type=content_type,
                media_kind=media_kind,
                phash=phash,
            )
        else:
            peer_photo = harvested_photo
        safe_peer_photo = json_safe_value(peer_photo)
        if not isinstance(safe_peer_photo, dict):
            return
        await update_peer_profile_photo_harvest(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            photo=safe_peer_photo,
            profile_photo_s3_uri=s3_uri,
        )


async def harvest_current_profile_photo(
    client: TelegramClient,
    conn: Any,
    *,
    entity: User | Channel | Chat | InputPeerUser | InputPeerChannel,
    peer_type: TelegramPeerTypeSchema | str,
    external_id: str,
    bucket: str = "",
    region: str = "",
    current_photo: dict[str, Any] | None = None,
) -> bool:
    if not s3_configured(bucket, region):
        logger.debug(
            "Skipping profile photo harvest for %s external_id=%s: S3 not configured",
            peer_type,
            external_id,
        )
        return False
    try:
        if isinstance(entity, (InputPeerUser, InputPeerChannel)):
            telethon_entity: Any = entity
        else:
            telethon_entity = entity_for_materialize_entity(
                entity,
                peer_type=peer_type,  # type: ignore[arg-type]
                external_id=external_id,
            )
        photos = await client.get_profile_photos(telethon_entity, limit=1)
    except FloodWaitError:
        raise
    except Exception as exc:
        logger.warning(
            "get_profile_photos failed peer_type=%s external_id=%s: %s",
            peer_type,
            external_id,
            exc,
        )
        return False

    if not photos:
        logger.debug("No profile photos for peer_type=%s external_id=%s", peer_type, external_id)
        return False

    photo = photos[0]
    if isinstance(photo, PhotoEmpty):
        return False

    photo_id = photo_id_from_tl(photo)
    if photo_id is None:
        return False

    ext, content_type, media_kind = resolve_profile_photo_variant(photo)
    harvest_index = await list_photo_harvest_index(
        conn,
        external_id,
        peer_type=peer_type,  # type: ignore[arg-type]
        current_photo=current_photo,
    )
    existing_uri = await _maybe_skip_harvest(
        photo_id=photo_id,
        harvest_index=harvest_index,
        external_id=external_id,
        peer_type=peer_type,
        ext=ext,
        bucket=bucket,
        region=region,
    )
    if existing_uri:
        await _persist_harvest(
            conn,
            external_id=external_id,
            peer_type=peer_type,  # type: ignore[arg-type]
            photo=photo,
            s3_uri=existing_uri,
            content_type=content_type,
            media_kind=media_kind,
            current_photo=photo_dict_for_harvest(current_photo),
            force_current=True,
            phash=harvest_phash_from_photo_dict(photo_dict_for_harvest(current_photo)),
        )
        return True

    result = await harvest_profile_photo(
        client,
        photo,
        external_id=external_id,
        peer_type=peer_type,
        bucket=bucket,
        region=region,
    )
    if result is None:
        return False

    s3_uri, content_type, media_kind, phash_hex = result
    await _persist_harvest(
        conn,
        external_id=external_id,
        peer_type=peer_type,  # type: ignore[arg-type]
        photo=photo,
        s3_uri=s3_uri,
        content_type=content_type,
        media_kind=media_kind,
        current_photo=photo_dict_for_harvest(current_photo),
        force_current=True,
        phash=phash_hex,
    )
    return True


async def fetch_peer_profile_photos(
    client: TelegramClient,
    conn: Any,
    *,
    peer: PeerProfilePhotoRow,
    bucket: str = "",
    region: str = "",
) -> ProfilePhotoFetchResult:
    result = ProfilePhotoFetchResult()
    if not s3_configured(bucket, region):
        result.warnings.append("Media could not be loaded.")
        result.fetch_pending = True
        return result
    if peer.peer_type in ("channel", "megagroup", "user") and peer.access_hash is None:
        result.warnings.append("Profile photos could not be refreshed.")
        return result

    try:
        entity = entity_for_peer(peer)
        photos = await client.get_profile_photos(entity)
    except FloodWaitError:
        raise
    except _DEFINITIVE_ERRORS as exc:
        logger.error(
            "Profile photos fetch denied peer_type=%s external_id=%s: %s",
            peer.peer_type,
            peer.external_id,
            exc,
        )
        result.warnings.append("Profile photos could not be refreshed.")
        return result
    except Exception:
        logger.exception(
            "Profile photos fetch failed peer_type=%s external_id=%s",
            peer.peer_type,
            peer.external_id,
        )
        result.failed += 1
        result.warnings.append("Profile photos could not be refreshed.")
        return result

    harvest_index = await list_photo_harvest_index(
        conn,
        peer.external_id,
        peer_type=peer.peer_type,
        current_photo=photo_dict_for_harvest(peer.photo),
    )
    current_id = photo_id_from_tl(peer.photo)

    for photo in photos:
        if isinstance(photo, PhotoEmpty):
            continue
        photo_id = photo_id_from_tl(photo)
        if photo_id is None:
            result.failed += 1
            continue

        ext, content_type, media_kind = resolve_profile_photo_variant(photo)
        existing_uri = await _maybe_skip_harvest(
            photo_id=photo_id,
            harvest_index=harvest_index,
            external_id=peer.external_id,
            peer_type=peer.peer_type,
            ext=ext,
            bucket=bucket,
            region=region,
        )
        if existing_uri:
            await _persist_harvest(
                conn,
                external_id=peer.external_id,
                peer_type=peer.peer_type,
                photo=photo,
                s3_uri=existing_uri,
                content_type=content_type,
                media_kind=media_kind,
                current_photo=photo_dict_for_harvest(peer.photo),
                phash=harvest_phash_from_photo_dict(photo_dict_for_harvest(peer.photo)),
            )
            result.skipped += 1
            harvest_index[photo_id] = existing_uri
            continue

        harvested = await harvest_profile_photo(
            client,
            photo,
            external_id=peer.external_id,
            peer_type=peer.peer_type,
            bucket=bucket,
            region=region,
        )
        if harvested is None:
            result.failed += 1
            continue

        s3_uri, content_type, media_kind, phash_hex = harvested
        await _persist_harvest(
            conn,
            external_id=peer.external_id,
            peer_type=peer.peer_type,
            photo=photo,
            s3_uri=s3_uri,
            content_type=content_type,
            media_kind=media_kind,
            current_photo=photo_dict_for_harvest(peer.photo),
            phash=phash_hex,
        )
        harvest_index[photo_id] = s3_uri
        result.harvested += 1

        if photo_ids_equal(current_id, photo_id):
            peer = PeerProfilePhotoRow(
                external_id=peer.external_id,
                peer_type=peer.peer_type,
                access_hash=peer.access_hash,
                harvester_id=peer.harvester_id,
                harvester_external_id=peer.harvester_external_id,
                photo=peer.photo,
                bot=peer.bot,
            )

    logger.info(
        "Profile photos fetch peer_type=%s external_id=%s harvested=%d skipped=%d failed=%d",
        peer.peer_type,
        peer.external_id,
        result.harvested,
        result.skipped,
        result.failed,
    )
    return result
