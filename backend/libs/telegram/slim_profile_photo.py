"""Map full TL profile photos to slim persisted shapes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from db.schemas.telegram.slim_profile_photo import (
    SlimChatPhotoEmptySchema,
    SlimChatPhotoSchema,
    SlimPhotoEmptySchema,
    SlimPhotoSchema,
    SlimTypeProfilePhotoSchema,
    SlimUserProfilePhotoSchema,
)
from pydantic import TypeAdapter, ValidationError
from telegram.slim_harvest import slim_harvest_from_dict

if TYPE_CHECKING:
    from db.schemas.telegram.tl import TypePhotoSchema

_SLIM_ADAPTER = TypeAdapter(SlimTypeProfilePhotoSchema)


def _stripped_thumb_from_sizes(sizes: list[Any] | None) -> str | bytes | None:
    for size in sizes or []:
        if not isinstance(size, dict):
            continue
        if size.get("tl") == "PhotoStrippedSize":
            return size.get("data") or size.get("bytes")
    return None


def _photo_dims(sizes: list[Any] | None) -> tuple[int | None, int | None]:
    best_w: int | None = None
    best_h: int | None = None
    best_area = -1
    for size in sizes or []:
        if not isinstance(size, dict):
            continue
        tl = size.get("tl")
        if tl not in ("PhotoSize", "PhotoSizeProgressive"):
            continue
        w = size.get("w")
        h = size.get("h")
        if w is None or h is None:
            continue
        area = int(w) * int(h)
        if area > best_area:
            best_area = area
            best_w = int(w)
            best_h = int(h)
    return best_w, best_h


def _has_video_sizes(photo: dict[str, Any]) -> bool | None:
    video_sizes = photo.get("video_sizes") or []
    if not isinstance(video_sizes, list):
        return None
    return True if video_sizes else None


def _photo_tl(photo: dict[str, Any]) -> str | None:
    tl = photo.get("tl")
    if tl:
        return str(tl)
    legacy = photo.get("_")
    return str(legacy) if legacy else None


def is_fat_profile_photo(photo: dict[str, Any] | None) -> bool:
    """Return True when ``photo`` still carries full TL download metadata."""
    if not isinstance(photo, dict):
        return False
    tl = _photo_tl(photo)
    if tl == "Photo":
        return bool(
            photo.get("sizes")
            or photo.get("access_hash") is not None
            or photo.get("file_reference") is not None
            or photo.get("video_sizes")
        )
    if tl == "ChatPhoto":
        return photo.get("dc_id") is not None
    if tl == "UserProfilePhoto":
        return photo.get("dc_id") is not None or photo.get("personal_photo_id") is not None
    return False


def slim_profile_photo_from_full(
    photo: dict[str, Any] | "TypePhotoSchema" | None,
    *,
    preserve_harvest: bool = True,
) -> dict[str, Any] | None:
    """Convert a full TL profile photo dict/model to a slim JSON-serializable dict."""
    if photo is None:
        return None

    if isinstance(photo, dict):
        try:
            return validate_slim_profile_photo(photo)
        except ValidationError:
            pass
    else:
        photo = photo.model_dump(mode="json", exclude_none=True)

    tl = _photo_tl(photo)
    if not tl:
        return None

    existing_harvest = photo.get("harvest") if preserve_harvest else None
    harvest = slim_harvest_from_dict(existing_harvest)

    slim: Any
    match tl:
        case "PhotoEmpty":
            slim = SlimPhotoEmptySchema(id=int(photo.get("id") or 0))
        case "Photo":
            w, h = _photo_dims(photo.get("sizes"))
            stripped = photo.get("stripped_thumb") or _stripped_thumb_from_sizes(photo.get("sizes"))
            slim = SlimPhotoSchema(
                id=int(photo.get("id") or 0),
                date=photo.get("date"),
                w=w,
                h=h,
                stripped_thumb=stripped,
                has_video=_has_video_sizes(photo),
                harvest=harvest,
            )
        case "ChatPhotoEmpty":
            slim = SlimChatPhotoEmptySchema()
        case "ChatPhoto":
            has_video_raw = photo.get("has_video")
            has_video = True if str(has_video_raw).lower() in ("true", "1", "t", "yes") else None
            slim = SlimChatPhotoSchema(
                photo_id=int(photo.get("photo_id") or 0),
                stripped_thumb=photo.get("stripped_thumb"),
                has_video=has_video,
                harvest=harvest,
            )
        case "UserProfilePhoto":
            has_video_raw = photo.get("has_video")
            has_video = True if str(has_video_raw).lower() in ("true", "1", "t", "yes") else None
            slim = SlimUserProfilePhotoSchema(
                photo_id=int(photo.get("photo_id") or 0),
                stripped_thumb=photo.get("stripped_thumb"),
                has_video=has_video,
                harvest=harvest,
            )
        case _:
            return photo

    return slim.model_dump(mode="json", exclude_none=True)


def validate_slim_profile_photo(photo: dict[str, Any] | None) -> dict[str, Any] | None:
    if photo is None:
        return None
    return _SLIM_ADAPTER.validate_python(photo).model_dump(mode="json", exclude_none=True)
