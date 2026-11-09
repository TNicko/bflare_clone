"""Response models for community overview insights."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.models.message_media import MessageMediaPreview


class RelatedChannelInsight(BaseModel):
    """Telegram similar-channel recommendation enriched with a DB peer row when present."""

    external_id: str
    peer_type: str
    persisted: bool = False
    peer: CommunityView | None = None


class ForwardSourceInsight(BaseModel):
    """Aggregated forward origin (into this community)."""

    external_id: str
    forward_count: int
    from_name: str | None = None
    peer: KeywordPeerView | None = None


class ActorCountInsight(BaseModel):
    """Participant / poster ranked by message volume."""

    external_id: str
    count: int
    user: UserPartialView | None = None
    peer: KeywordPeerView | None = None
    #: Membership rank when ``participants_source`` is ``membership`` (e.g. admin).
    rank: str | None = None


class TopViewedPostInsight(BaseModel):
    id: UUID
    message_id: int
    date: datetime | None = None
    content: str | None = None
    views: int
    media: MessageMediaPreview | None = None
    from_external_id: str | None = None
    source_external_id: str | None = None
    fwd_from: dict | None = None
    reply_to: dict | None = None


class ActivitySnapshot(BaseModel):
    #: ``None`` when tip is calendar-stale or the 7d window behind the tip has gaps.
    messages_7d: int | None = None
    #: ``None`` when tip is calendar-stale or the 30d window behind the tip has gaps.
    messages_30d: int | None = None
    #: Total harvested messages when history coverage is complete from the beginning
    #: through a tip within the last month. ``None`` when coverage is incomplete.
    messages_total: int | None = None
    #: End of the newest fetch-coverage window when ``messages_total`` is present.
    #: ``None`` when total is gated off.
    messages_last_at: datetime | None = None
    #: Exclusive end of the newest fetch-coverage island when any coverage exists.
    #: Used by the client to plan tip-only vs window volume sync.
    coverage_tip_at: datetime | None = None
    #: Oldest message date when history coverage is complete from the beginning.
    messages_first_at: datetime | None = None
    #: Preview rows for the participants table (membership when available, else
    #: message-based top posters). Sorted by post count descending.
    participants: list[ActorCountInsight] = Field(default_factory=list)
    #: Full participant / distinct-poster count (may exceed ``participants`` preview).
    participants_total: int = 0
    #: ``membership`` when ``telegram_chat_participants`` has current members;
    #: ``messages`` when falling back to message senders.
    participants_source: str = "messages"
    top_viewed: list[TopViewedPostInsight] = Field(default_factory=list)


class CommunityInsightsResponse(BaseModel):
    related_channels: list[RelatedChannelInsight] = Field(default_factory=list)
    #: Full similar-channel ref count (may exceed ``related_channels`` preview length).
    related_channels_total: int = 0
    forward_sources: list[ForwardSourceInsight] = Field(default_factory=list)
    #: Full outbound neighbor count (may exceed ``forward_sources`` preview length).
    forward_sources_total: int = 0
    #: Communities that forward content from this peer (inbound edges).
    forwarded_by: list[ForwardSourceInsight] = Field(default_factory=list)
    #: Full inbound neighbor count (may exceed ``forwarded_by`` preview length).
    forwarded_by_total: int = 0
    activity: ActivitySnapshot = Field(default_factory=ActivitySnapshot)


class SimilarChannelsPage(BaseModel):
    """Paginated similar-channel recommendations for the full list page."""

    items: list[CommunityView]
    total: int
    limit: int
    offset: int


class ParticipantsPage(BaseModel):
    """Paginated community participants (or message-based top posters fallback)."""

    items: list[ActorCountInsight]
    total: int
    limit: int
    offset: int
    #: ``membership`` when listing harvested members; ``messages`` when incomplete.
    source: str = "messages"
