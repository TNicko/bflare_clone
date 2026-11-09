from datetime import datetime
from typing import List, Literal
from uuid import UUID
from pydantic import BaseModel, Field

from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordMessageView, KeywordPeerView
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from services.api.src.crud.supabase.views.bluesky.actor import (
    BlueskyActorPagedResult,
)
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.crud.supabase.views.telegram.semantic_message import SemanticMessageView
from services.api.src.models.message_media import (  # noqa: F401 — re-export
    MessageMediaPreview,
    MessageMediaResponse,
)
from services.api.src.search_dates import DATE_FROM_DESCRIPTION, DATE_TO_DESCRIPTION, SearchDateFrom, SearchDateTo
from services.api.src.search_limits import SEMANTIC_QUERY_MAX_LENGTH

ALLOWED_CONTENT_TYPES = frozenset({"image/png", "image/jpeg"})

NameScope = Literal["all", "community", "user"]
NameKind = Literal["all", "community", "user"]


class ImageSearchMatch(BaseModel):
    id: UUID
    filename: str
    url: str
    score: float


class ImageMessageSearchMatch(ImageSearchMatch, TelegramMessageView):
    peer_title: str
    peer_username: str | None = None
    peer_profile_photo_s3_uri: str | None = None
    peer_profile_photo_media_kind: Literal["image", "video"] | None = None
    peer_stripped_profile_photo: str | None = None
    # Author when ``from_external_id`` is a community (channel/megagroup/chat).
    from_peer_title: str | None = None
    from_peer_type: Literal["channel", "megagroup", "chat"] | None = None
    from_peer_username: str | None = None
    from_peer_profile_photo_s3_uri: str | None = None
    from_peer_profile_photo_media_kind: Literal["image", "video"] | None = None
    from_peer_stripped_profile_photo: str | None = None
    # Author when ``from_external_id`` is a telegram user.
    from_user_id: str | None = None
    from_user_first_name: str | None = None
    from_user_last_name: str | None = None
    from_user_username: str | None = None
    from_user_profile_photo_s3_uri: str | None = None
    from_user_profile_photo_media_kind: Literal["image", "video"] | None = None
    from_user_stripped_thumb: str | None = None


class ImageMessageSearchResponse(BaseModel):
    """Paginated reverse-image / pHash copy search results."""

    items: list[ImageMessageSearchMatch]
    total: int
    limit: int
    offset: int = Field(ge=0)
    #: Opaque id for in-process session pagination (omit embed/Milvus on later pages).
    search_id: str | None = None


class CommunityPagedResult(BaseModel):
    items: List[CommunityView]
    total: int
    limit: int
    offset: int


class UserPagedResult(BaseModel):
    items: List[UserPartialView]
    total: int
    limit: int
    offset: int


class NameSearchResponse(BaseModel):
    query: str
    scope: NameScope
    communities: CommunityPagedResult
    users: UserPagedResult


class TelegramNameSearchResponse(BaseModel):
    """Live Telegram ``contacts.search`` results for a name query.

    When ``skipped`` is true, this query was already run (or is running) and
    no live search was performed — communities/users are empty.
    """

    query: str
    scope: NameScope
    skipped: bool = False
    communities: CommunityPagedResult
    users: UserPagedResult


class BlueskyNameSearchResponse(BaseModel):
    """Bluesky actor name search.

    A ``live`` request returns that call's App View page (in memory). Catalogue
    ILIKE is used when live is off or the live call was skipped.
    """

    query: str
    skipped: bool = False
    actors: BlueskyActorPagedResult


class MessagePartial(BaseModel):
    id: UUID
    message_id: int
    source_external_id: str
    peer_type: PeerTypeSchema | None = None
    message_type: str | None = None
    content: str | None = None
    from_external_id: str | None = None
    date: datetime | None = None


class SemanticMessage(MessagePartial):
    similarity: float


class SearchContentSemanticRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=SEMANTIC_QUERY_MAX_LENGTH)
    limit: int | None = None
    offset: int = Field(default=0, ge=0)
    # Optional message date window; applied in Milvus filtering and used for hydration ordering.
    date_from: SearchDateFrom | None = Field(default=None, description=DATE_FROM_DESCRIPTION)
    date_to: SearchDateTo | None = Field(default=None, description=DATE_TO_DESCRIPTION)


class SemanticMessageResponse(BaseModel):
    query: str
    items: list[SemanticMessageView]
    limit: int
    offset: int = Field(ge=0)
    # Deduped lookups for ``items`` (avoids repeating shared peers/senders per message).
    peers: dict[str, KeywordPeerView] = Field(default_factory=dict)
    users: dict[str, UserPartialView] = Field(default_factory=dict)


class KeywordMessageResponse(BaseModel):
    query: str
    items: list[KeywordMessageView]
    total: int
    limit: int
    offset: int = Field(ge=0)
    peers: dict[str, KeywordPeerView] = Field(default_factory=dict)
    users: dict[str, UserPartialView] = Field(default_factory=dict)


class PhoneSearchResponse(BaseModel):
    query: str
    phone: str
    found: bool
    user: UserPartialView | None = None


class UsernameSearchResponse(BaseModel):
    query: str
    username: str
    found: bool
    user: UserPartialView | None = None
    community: CommunityView | None = None
