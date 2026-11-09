from pydantic import BaseModel, Field

from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelView,
    TelegramChatView,
    TelegramMegagroupView,
)
from services.api.src.crud.supabase.views.telegram.field_history import FieldHistoryEntryView
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView


class CommunityDetailResponse(BaseModel):
    """API envelope for a peer detail request (main peer + optional linked peer)."""

    peer_type: PeerTypeSchema
    channel: TelegramChannelView | TelegramMegagroupView | None = None
    chat: TelegramChatView | None = None
    linked_peer: CommunityView | None = None
    field_history: list[FieldHistoryEntryView] = Field(default_factory=list)
    bots: list[UserPartialView] = Field(default_factory=list)


class PeerMessagesResponse(BaseModel):
    """API envelope for a paginated peer messages request."""

    messages: list[TelegramMessageView]
    next_before_message_id: int | None = None
    #: ``None`` when message history is not yet fully scraped from Telegram.
    total_count: int | None = None
    #: 1-based page when the client requested offset paging via ``page``.
    page: int | None = None
    page_size: int | None = None
    synced_count: int | None = None
    #: Oldest Telegram id upserted by an older-history / refresh sync page.
    oldest_synced_message_id: int | None = None
    #: Set by ``mode=refresh_recent`` when the since-window walk finished.
    refresh_complete: bool | None = None
    #: Ids newly marked deleted during a recent-refresh page.
    deleted_message_ids: list[int] = Field(default_factory=list)
    #: Coverage-gated post counts (excludes service/action messages). ``None`` when
    #: fetch coverage does not yet span that window.
    messages_7d: int | None = None
    messages_30d: int | None = None
    users: dict[str, UserPartialView] = Field(default_factory=dict)
    peers: dict[str, KeywordPeerView] = Field(
        default_factory=dict,
        description="Community senders / forward sources keyed by signed external_id",
    )
    #: Set when the caller's plan only includes the latest N messages.
    upgrade_for_history: bool = False


class PeerMessageLocateResponse(BaseModel):
    """Which newest-first page contains a Telegram ``message_id``."""

    page: int
    page_size: int
    total_count: int
    newer_count: int
    message: TelegramMessageView
    users: dict[str, UserPartialView] = Field(default_factory=dict)
    peers: dict[str, KeywordPeerView] = Field(default_factory=dict)
    upgrade_for_history: bool = False


class PeerMediaKindCounts(BaseModel):
    """Per-kind inventory from ``telegram_message_fetch_coverage`` (may be partial)."""

    all: int = 0
    image: int = 0
    gif: int = 0
    video: int = 0
    audio: int = 0
    document: int = 0
    webpage: int = 0
    #: Messages with ``has_url`` (URL/TextUrl entity and/or webpage preview).
    url: int = 0
    #: Messages with ``has_mention`` (@username / MentionName entity).
    mention: int = 0


class PeerInventoryCountsResponse(BaseModel):
    """Cheap fetch-coverage-backed counts for chat view filter badges."""

    #: Fetch-coverage message total for what we have (ungated; may be partial).
    message_count: int = 0
    #: Fetch-coverage media-kind + entity-flag totals (not download/S3 harvest).
    media: PeerMediaKindCounts = Field(default_factory=PeerMediaKindCounts)
    #: True when fetch history has no gaps from −∞ through the tip slack window.
    history_complete: bool = False
    #: True when the last 30 days have no coverage gaps (older gaps are separate).
    recent_window_complete: bool = False
    #: Active ``fetch_messages`` fleet job for this peer (any source), if any.
    history_job_id: str | None = None
    history_job_status: str | None = None


class CoverageIntervalView(BaseModel):
    """Half-open ``[covered_after, covered_before)``; ``covered_after`` null = −∞."""

    covered_after: str | None = None
    covered_before: str


class PeerHistoryCoverageResponse(BaseModel):
    """Fetch-coverage islands + gaps for the history-completeness envelope."""

    complete: bool = False
    tip_slack_days: int = 30
    envelope_before: str | None = None
    covered: list[CoverageIntervalView] = Field(default_factory=list)
    gaps: list[CoverageIntervalView] = Field(default_factory=list)
    #: Active user-requested complete-history job, if any.
    job_id: str | None = None
    job_status: str | None = None


class CompletePeerHistoryResponse(BaseModel):
    """Result of enqueueing (or finding) a user complete-history fleet job."""

    job_id: str
    status: str
    created: bool = False
