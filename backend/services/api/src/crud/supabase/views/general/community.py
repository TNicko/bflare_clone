from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.crud.supabase.views.general.category import CategorySimpleView
from telegram.mappers import get_b64_image_url_from_photo, profile_photo_media_kind


def _primary_username(row: Mapping[str, Any]) -> str | None:
    """Prefer scalar ``username``, else first active entry in ``usernames`` JSONB."""
    raw = row.get("username")
    if raw is not None:
        username = str(raw).strip()
        if username:
            return username

    usernames = row.get("usernames")
    if not isinstance(usernames, list):
        return None
    for entry in usernames:
        if not isinstance(entry, dict):
            continue
        # Mirror CommunityPage: tl Username (or missing) and active !== false
        tl = entry.get("tl") or entry.get("_")
        if tl is not None and tl not in ("Username", "username"):
            continue
        if entry.get("active") is False:
            continue
        candidate = entry.get("username")
        if candidate is None:
            continue
        username = str(candidate).strip()
        if username:
            return username
    return None


class CommunityView(BaseModel):
    """Lightweight community row for category/search listings."""

    peer_type: PeerTypeSchema
    community_id: UUID
    external_id: str
    title: str
    about: str | None = None
    username: str | None = None
    stripped_profile_photo: str | None = None
    participants_count: int = 0
    profile_photo_s3_uri: str | None = None
    profile_photo_media_kind: Literal["image", "video"] | None = None
    categories: list[CategorySimpleView] = Field(default_factory=list)
    is_unknown: bool = False
    unavailable_at: datetime | None = None
    #: From ``telegram_harvester_peer_roles`` (e.g. CHANNEL_PRIVATE).
    access_error_code: str | None = None
    is_scam: bool | None = None
    is_verified: bool | None = None
    is_fake: bool | None = None
    is_restricted: bool | None = None
    restriction_reason: list[Any] = Field(default_factory=list)
    #: Discussion-group / linked-channel privacy (list payloads; not a nested peer).
    has_link: bool | None = None
    linked_peer_type: PeerTypeSchema | None = None
    linked_username: str | None = None
    #: Detected content language (ISO 639), or ``und`` when undetermined.
    language: str | None = None

    @classmethod
    def unknown(cls, *, peer_type: PeerTypeSchema, external_id: str) -> CommunityView:
        """Placeholder when a participant peer cannot be resolved."""
        return cls(
            peer_type=peer_type,
            community_id=UUID(int=0),
            external_id=str(external_id),
            title="Unknown",
            is_unknown=True,
        )

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> CommunityView | None:
        external_id = row.get("external_id")
        if not external_id:
            return None

        participants_count = row.get("participants_count")
        title = (row.get("title") or "").strip() or str(external_id)
        about_raw = row.get("about")
        about = str(about_raw).strip() if about_raw is not None else ""
        about = about or None
        categories_raw = row.get("categories")
        categories: list[CategorySimpleView] = []
        if isinstance(categories_raw, list):
            categories = [CategorySimpleView.model_validate(item) for item in categories_raw]

        community_id_raw = row.get("community_id", row.get("id"))
        if community_id_raw is None:
            return None

        restriction_raw = row.get("restriction_reason")
        restriction_reason: list[Any] = list(restriction_raw) if isinstance(restriction_raw, list) else []

        def _optional_bool(key: str) -> bool | None:
            if key not in row or row.get(key) is None:
                return None
            return bool(row.get(key))

        linked_username = _primary_username(
            {
                "username": row.get("linked_username"),
                "usernames": row.get("linked_usernames"),
            }
        )

        linked_peer_type_raw = row.get("linked_peer_type")
        linked_peer_type: PeerTypeSchema | None = None
        if linked_peer_type_raw in ("channel", "megagroup", "chat"):
            linked_peer_type = linked_peer_type_raw

        return cls(
            peer_type=row["peer_type"],
            community_id=UUID(str(community_id_raw)),
            external_id=str(external_id),
            title=title,
            about=about,
            username=_primary_username(row),
            stripped_profile_photo=(
                row.get("stripped_profile_photo")
                if row.get("stripped_profile_photo") is not None
                else get_b64_image_url_from_photo(row.get("photo"))
            ),
            participants_count=int(participants_count) if participants_count is not None else 0,
            profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            profile_photo_media_kind=profile_photo_media_kind(
                photo=row.get("photo"),
                profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            ),
            categories=categories,
            is_unknown=bool(row.get("is_unknown")),
            unavailable_at=row.get("unavailable_at"),
            access_error_code=(str(row["access_error_code"]) if row.get("access_error_code") is not None else None),
            is_scam=_optional_bool("is_scam"),
            is_verified=_optional_bool("is_verified"),
            is_fake=_optional_bool("is_fake"),
            is_restricted=_optional_bool("is_restricted"),
            restriction_reason=restriction_reason,
            has_link=_optional_bool("has_link"),
            linked_peer_type=linked_peer_type,
            linked_username=linked_username,
            language=(str(lang).strip() if (lang := row.get("language")) is not None and str(lang).strip() else None),
        )
