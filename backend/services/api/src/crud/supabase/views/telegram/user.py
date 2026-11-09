from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, JsonValue

from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.field_history import FieldHistoryEntryView, field_history_from_row
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from services.api.src.crud.supabase.views.telegram.participant import PeerParticipantView
from telegram.mappers import get_b64_image_url_from_photo, profile_photo_media_kind


EngagementKind = Literal["participant_only", "messaged", "reacted", "messaged_and_reacted"]
MembershipStatus = Literal["current", "former", "unknown"]


def _engagement_kind(
    *,
    message_count: int,
    reaction_count: int,
    has_reaction: bool = False,
) -> EngagementKind:
    has_msgs = message_count > 0
    has_rx = reaction_count > 0 or has_reaction
    if has_msgs and has_rx:
        return "messaged_and_reacted"
    if has_msgs:
        return "messaged"
    if has_rx:
        return "reacted"
    return "participant_only"


def _membership_status(participant: PeerParticipantView) -> MembershipStatus:
    if participant.is_current_member:
        return "current"
    if participant.left_at is not None or participant.is_current_member is False:
        return "former"
    return "unknown"


class UserPeerAffiliationSummaryView(BaseModel):
    membership: MembershipStatus
    rank: str | None = None
    joined_at: datetime | None = None
    left_at: datetime | None = None
    message_count: int = 0
    reaction_count: int = 0
    last_message_at: datetime | None = None
    last_reaction_at: datetime | None = None
    engagement_kind: EngagementKind = "participant_only"

    @classmethod
    def from_participant(
        cls,
        participant: PeerParticipantView,
        *,
        message_count: int = 0,
        reaction_count: int = 0,
        last_message_at: datetime | None = None,
        last_reaction_at: datetime | None = None,
        has_reaction: bool = False,
    ) -> UserPeerAffiliationSummaryView:
        return cls(
            membership=_membership_status(participant),
            rank=participant.rank,
            joined_at=participant.joined_at,
            left_at=participant.left_at,
            message_count=int(message_count or 0),
            reaction_count=int(reaction_count or 0),
            last_message_at=last_message_at,
            last_reaction_at=last_reaction_at,
            engagement_kind=_engagement_kind(
                message_count=int(message_count or 0),
                reaction_count=int(reaction_count or 0),
                has_reaction=has_reaction,
            ),
        )


class UserPartialView(BaseModel):
    id: UUID
    external_id: str
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    phone: str | None = None
    about: str | None = None
    engagement_count: int
    profile_photo_s3_uri: str | None = None
    profile_photo_media_kind: Literal["image", "video"] | None = None
    premium: bool
    verified: bool
    bot: bool
    deleted: bool = False
    restricted: bool = False
    scam: bool = False
    fake: bool = False
    unavailable_at: datetime | None = None
    restriction_reason: list[Any] = Field(default_factory=list)
    stripped_thumb: str | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> UserPartialView:
        raw_id = row["id"]
        user_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
        restriction_raw = row.get("restriction_reason")
        restriction_reason: list[Any] = list(restriction_raw) if isinstance(restriction_raw, list) else []
        return cls(
            id=user_id,
            external_id=str(row["external_id"]),
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            username=row.get("username"),
            phone=row.get("phone"),
            about=row.get("about"),
            engagement_count=int(row.get("engagement_count") or 0),
            profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            profile_photo_media_kind=profile_photo_media_kind(
                photo=row.get("photo"),
                profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            ),
            premium=bool(row.get("premium")),
            verified=bool(row.get("verified")),
            bot=bool(row.get("bot")),
            deleted=bool(row.get("deleted")),
            restricted=bool(row.get("restricted")),
            scam=bool(row.get("scam")),
            fake=bool(row.get("fake")),
            unavailable_at=row.get("unavailable_at"),
            restriction_reason=restriction_reason,
            stripped_thumb=get_b64_image_url_from_photo(row.get("photo")),
        )


class UserAffiliatedPeerView(BaseModel):
    external_id: str
    participant_data: PeerParticipantView
    peer: CommunityView
    affiliation: UserPeerAffiliationSummaryView


class UserAffiliatedPeersPage(BaseModel):
    items: list[UserAffiliatedPeerView]
    total: int
    limit: int
    offset: int


class UserPeerAffiliationDetailView(BaseModel):
    user_id: UUID
    user_external_id: str
    peer: CommunityView
    participant_data: PeerParticipantView
    affiliation: UserPeerAffiliationSummaryView


class UserPeerAffiliationMessagesPage(BaseModel):
    items: list[TelegramMessageView]
    total: int
    limit: int
    offset: int


class UserPeerReactionView(BaseModel):
    reaction_key: str
    reaction: dict[str, Any]
    reacted_at: datetime | None = None
    message_id: int
    message_uuid: UUID
    message_date: datetime | None = None
    message_content: str | None = None
    message_peer_type: str | None = None


class UserPeerAffiliationReactionsPage(BaseModel):
    items: list[UserPeerReactionView]
    total: int
    limit: int
    offset: int


class UserPeerDetailView(BaseModel):
    id: UUID
    external_id: str
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    phone: str | None = None
    engagement_count: int
    profile_photo_s3_uri: str | None = None
    profile_photo_media_kind: Literal["image", "video"] | None = None
    premium: bool
    verified: bool
    bot: bool
    restricted: bool = False
    scam: bool = False
    fake: bool = False
    stripped_thumb: str | None = None
    restriction_reason: JsonValue | None = None
    about: str | None = None
    birthday: JsonValue | None = None
    deleted: bool = False
    deleted_observed_at: datetime | None = None
    unavailable_at: datetime | None = None
    updated_at: datetime | None = None
    affiliated_peers: list[UserAffiliatedPeerView] = Field(default_factory=list)
    affiliated_peers_total: int = 0
    pinned_stories: JsonValue | None = None
    pinned_stories_fetched_at: datetime | None = None
    saved_star_gifts: JsonValue | None = None
    star_gifts_fetched_at: datetime | None = None
    pinned_stories_fetch_pending: bool = False
    saved_star_gifts_fetch_pending: bool = False
    pinned_stories_fetch_warnings: list[str] = Field(default_factory=list)
    saved_star_gifts_fetch_warnings: list[str] = Field(default_factory=list)
    field_history: list[FieldHistoryEntryView] = Field(default_factory=list)

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> UserPeerDetailView:
        raw_id = row["id"]
        user_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
        affiliated: list[UserAffiliatedPeerView] = []
        for entry in enrich_affiliated_peers(row.get("affiliated_peers")):
            peer_raw = entry.get("peer")
            peer_type = None
            external_id = str(entry.get("external_id") or "")
            if isinstance(peer_raw, dict):
                peer_type = peer_raw.get("peer_type")
                if not external_id:
                    external_id = str(peer_raw.get("external_id") or "")
                if peer_raw.get("is_unknown"):
                    peer_view = CommunityView.unknown(
                        peer_type=peer_type or "channel",
                        external_id=external_id,
                    )
                else:
                    peer_view = CommunityView.from_row(
                        {
                            **peer_raw,
                            "community_id": peer_raw.get("community_id"),
                            "participants_count": peer_raw.get("participants_count"),
                        }
                    )
            else:
                peer_view = None

            if peer_view is None:
                if not external_id or peer_type is None:
                    # Fall back to participant peer_type if present on entry
                    peer_type = entry.get("peer_type") or "channel"
                if not external_id:
                    continue
                peer_view = CommunityView.unknown(
                    peer_type=peer_type,  # type: ignore[arg-type]
                    external_id=external_id,
                )

            participant_raw = entry.get("participant_data")
            if not isinstance(participant_raw, dict):
                continue
            participant = PeerParticipantView.model_validate(participant_raw)
            affiliation_raw = entry.get("affiliation")
            if isinstance(affiliation_raw, dict):
                affiliation = UserPeerAffiliationSummaryView.model_validate(
                    {
                        **affiliation_raw,
                        "membership": affiliation_raw.get("membership") or _membership_status(participant),
                        "rank": affiliation_raw.get("rank", participant.rank),
                        "joined_at": affiliation_raw.get("joined_at", participant.joined_at),
                        "left_at": affiliation_raw.get("left_at", participant.left_at),
                        "engagement_kind": affiliation_raw.get("engagement_kind")
                        or _engagement_kind(
                            message_count=int(affiliation_raw.get("message_count") or 0),
                            reaction_count=int(affiliation_raw.get("reaction_count") or 0),
                        ),
                    }
                )
            else:
                affiliation = UserPeerAffiliationSummaryView.from_participant(
                    participant,
                    message_count=int(entry.get("message_count") or 0),
                    reaction_count=int(entry.get("reaction_count") or 0),
                    last_message_at=entry.get("last_message_at"),
                    last_reaction_at=entry.get("last_reaction_at"),
                )
            affiliated.append(
                UserAffiliatedPeerView(
                    external_id=external_id or peer_view.external_id,
                    participant_data=participant,
                    peer=peer_view,
                    affiliation=affiliation,
                )
            )

        return cls(
            id=user_id,
            external_id=str(row["external_id"]),
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            username=row.get("username"),
            phone=row.get("phone"),
            engagement_count=int(row.get("engagement_count") or 0),
            profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            profile_photo_media_kind=profile_photo_media_kind(
                photo=row.get("photo"),
                profile_photo_s3_uri=row.get("profile_photo_s3_uri"),
            ),
            premium=bool(row.get("premium")),
            verified=bool(row.get("verified")),
            bot=bool(row.get("bot")),
            restricted=bool(row.get("restricted")),
            scam=bool(row.get("scam")),
            fake=bool(row.get("fake")),
            stripped_thumb=row.get("stripped_thumb") or get_b64_image_url_from_photo(row.get("photo")),
            restriction_reason=row.get("restriction_reason"),
            about=row.get("about"),
            birthday=row.get("birthday"),
            deleted=bool(row.get("deleted")),
            deleted_observed_at=row.get("deleted_observed_at"),
            unavailable_at=row.get("unavailable_at"),
            updated_at=row.get("updated_at"),
            affiliated_peers=affiliated,
            affiliated_peers_total=int(row.get("affiliated_peers_total") or len(affiliated)),
            pinned_stories=row.get("pinned_stories"),
            pinned_stories_fetched_at=row.get("pinned_stories_fetched_at"),
            saved_star_gifts=row.get("saved_star_gifts"),
            star_gifts_fetched_at=row.get("star_gifts_fetched_at"),
            pinned_stories_fetch_pending=bool(row.get("pinned_stories_fetch_pending")),
            saved_star_gifts_fetch_pending=bool(row.get("saved_star_gifts_fetch_pending")),
            pinned_stories_fetch_warnings=list(row.get("pinned_stories_fetch_warnings") or []),
            saved_star_gifts_fetch_warnings=list(row.get("saved_star_gifts_fetch_warnings") or []),
            field_history=field_history_from_row(row.get("field_history")),
        )


def enrich_affiliated_peers(raw: Any) -> list[dict[str, Any]]:
    if not raw:
        return []
    if not isinstance(raw, list):
        return []

    enriched: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        item = dict(entry)
        peer = item.get("peer")
        if isinstance(peer, dict):
            peer_out = dict(peer)
            peer_out["stripped_profile_photo"] = get_b64_image_url_from_photo(peer_out.pop("photo", None))
            item["peer"] = peer_out
        enriched.append(item)
    return enriched
