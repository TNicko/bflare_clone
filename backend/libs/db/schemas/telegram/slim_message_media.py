"""Slim persisted shapes for ``telegram_messages.media`` (JSONB).

One model per ``TypeMessageMedia`` variant. Full TL blobs are normalized at
write time; harvest state is merged on download/embed pipelines.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from db.schemas.telegram.slim_media import SlimHarvestMediaStateSchema
from pydantic import BaseModel, ConfigDict, Field

# Re-export for callers that still import harvest from this module.
__all__ = [
    "SlimHarvestMediaStateSchema",
    "SlimMessageMediaBase",
    "SlimMessageMediaContactSchema",
    "SlimMessageMediaDiceSchema",
    "SlimMessageMediaDocumentSchema",
    "SlimMessageMediaEmptySchema",
    "SlimMessageMediaGameSchema",
    "SlimMessageMediaGeoLiveSchema",
    "SlimMessageMediaGeoSchema",
    "SlimMessageMediaGiveawayResultsSchema",
    "SlimMessageMediaGiveawaySchema",
    "SlimMessageMediaInvoiceSchema",
    "SlimMessageMediaPaidMediaSchema",
    "SlimMessageMediaPhotoSchema",
    "SlimMessageMediaPollSchema",
    "SlimMessageMediaStorySchema",
    "SlimMessageMediaToDoSchema",
    "SlimMessageMediaUnsupportedSchema",
    "SlimMessageMediaVenueSchema",
    "SlimMessageMediaVideoStreamSchema",
    "SlimMessageMediaWebPageSchema",
    "SlimPollAttachedMediaSchema",
    "SlimPollResultOptionSchema",
    "SlimTodoItemSchema",
    "SlimTypeMessageMediaSchema",
]


class SlimMessageMediaBase(BaseModel):
    """Persisted message media: only declared slim fields are allowed (no TL nesting)."""

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# Photo / document (harvest-capable) / webpage (link metadata only)
# ---------------------------------------------------------------------------


class SlimMessageMediaPhotoSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaPhoto"] = "MessageMediaPhoto"
    w: int | None = None
    h: int | None = None
    live_photo: bool | None = None
    harvest: SlimHarvestMediaStateSchema | None = None
    # Inline Telegram thumb as a data URL (stripped / cached JPEG).
    stripped_thumb: str | None = None
    #: Short-lived CDN URL from ``t.me/s`` scrape (v1 inventory only; not harvested).
    web_url: str | None = None


class SlimMessageMediaDocumentSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaDocument"] = "MessageMediaDocument"
    mime_type: str | None = None
    size: int | None = None
    filename: str | None = None
    duration: float | None = None
    w: int | None = None
    h: int | None = None
    video: bool | None = None
    voice: bool | None = None
    round: bool | None = None
    animated: bool | None = None
    sticker: bool | None = None
    harvest: SlimHarvestMediaStateSchema | None = None
    stripped_thumb: str | None = None
    #: Short-lived CDN URL from ``t.me/s`` scrape (v1 inventory only; not harvested).
    web_url: str | None = None


class SlimMessageMediaWebPageSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaWebPage"] = "MessageMediaWebPage"
    url: str | None = None
    title: str | None = None
    site_name: str | None = None
    description: str | None = None
    preview_mime: str | None = None
    harvest: SlimHarvestMediaStateSchema | None = None


# ---------------------------------------------------------------------------
# Location / contact
# ---------------------------------------------------------------------------


class SlimMessageMediaGeoSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaGeo"] = "MessageMediaGeo"
    lat: float
    long: float
    accuracy_radius: int | None = None


class SlimMessageMediaGeoLiveSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaGeoLive"] = "MessageMediaGeoLive"
    lat: float
    long: float
    period: int
    heading: int | None = None
    proximity_notification_radius: int | None = None


class SlimMessageMediaVenueSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaVenue"] = "MessageMediaVenue"
    lat: float
    long: float
    title: str
    address: str
    provider: str
    venue_id: str
    venue_type: str


class SlimMessageMediaContactSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaContact"] = "MessageMediaContact"
    phone_number: str
    first_name: str
    last_name: str
    user_id: int


# ---------------------------------------------------------------------------
# Interactive / service-like
# ---------------------------------------------------------------------------


class SlimPollResultOptionSchema(BaseModel):
    option: int
    voters: int
    chosen: bool | None = None
    correct: bool | None = None


class SlimPollAttachedMediaSchema(BaseModel):
    """Inline preview for ``MessageMediaPoll.attached_media`` (usually a photo)."""

    model_config = ConfigDict(extra="forbid")
    w: int | None = None
    h: int | None = None
    stripped_thumb: str | None = None


class SlimMessageMediaPollSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaPoll"] = "MessageMediaPoll"
    poll_id: int
    question: str
    answers: list[str]
    quiz: bool | None = None
    multiple_choice: bool | None = None
    closed: bool | None = None
    public_voters: bool | None = None
    total_voters: int | None = None
    results: list[SlimPollResultOptionSchema] | None = None
    solution: str | None = None
    attached: SlimPollAttachedMediaSchema | None = None


class SlimMessageMediaDiceSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaDice"] = "MessageMediaDice"
    value: int
    emoticon: str


class SlimMessageMediaGameSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaGame"] = "MessageMediaGame"
    id: int
    short_name: str
    title: str
    description: str


class SlimMessageMediaInvoiceSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaInvoice"] = "MessageMediaInvoice"
    title: str
    description: str
    currency: str
    total_amount: int
    start_param: str
    shipping_address_requested: bool | None = None
    test: bool | None = None
    receipt_msg_id: int | None = None


class SlimTodoItemSchema(BaseModel):
    id: int
    title: str
    completed: bool = False


class SlimMessageMediaToDoSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaToDo"] = "MessageMediaToDo"
    title: str
    items: list[SlimTodoItemSchema]
    others_can_append: bool | None = None
    others_can_complete: bool | None = None


# ---------------------------------------------------------------------------
# Story / giveaway / paid / stream
# ---------------------------------------------------------------------------


class SlimMessageMediaStorySchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaStory"] = "MessageMediaStory"
    peer_id: str
    story_id: int
    via_mention: bool | None = None


class SlimMessageMediaGiveawaySchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaGiveaway"] = "MessageMediaGiveaway"
    channels: list[int]
    quantity: int
    until_date: datetime | None = None
    only_new_subscribers: bool | None = None
    winners_are_visible: bool | None = None
    countries_iso2: list[str] | None = None
    prize_description: str | None = None
    months: int | None = None
    stars: int | None = None


class SlimMessageMediaGiveawayResultsSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaGiveawayResults"] = "MessageMediaGiveawayResults"
    channel_id: int
    launch_msg_id: int
    winners_count: int
    unclaimed_count: int
    winners: list[int]
    until_date: datetime | None = None
    only_new_subscribers: bool | None = None
    refunded: bool | None = None
    additional_peers_count: int | None = None
    months: int | None = None
    stars: int | None = None
    prize_description: str | None = None


class SlimMessageMediaPaidMediaSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaPaidMedia"] = "MessageMediaPaidMedia"
    stars_amount: int
    media_kinds: list[str] = Field(default_factory=list)


class SlimMessageMediaVideoStreamSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaVideoStream"] = "MessageMediaVideoStream"
    rtmp_stream: bool | None = None


# ---------------------------------------------------------------------------
# Empty / unsupported
# ---------------------------------------------------------------------------


class SlimMessageMediaEmptySchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaEmpty"] = "MessageMediaEmpty"


class SlimMessageMediaUnsupportedSchema(SlimMessageMediaBase):
    tl: Literal["MessageMediaUnsupported"] = "MessageMediaUnsupported"


SlimTypeMessageMediaSchema = Annotated[
    SlimMessageMediaEmptySchema
    | SlimMessageMediaPhotoSchema
    | SlimMessageMediaGeoSchema
    | SlimMessageMediaContactSchema
    | SlimMessageMediaUnsupportedSchema
    | SlimMessageMediaDocumentSchema
    | SlimMessageMediaWebPageSchema
    | SlimMessageMediaVenueSchema
    | SlimMessageMediaGameSchema
    | SlimMessageMediaInvoiceSchema
    | SlimMessageMediaGeoLiveSchema
    | SlimMessageMediaPollSchema
    | SlimMessageMediaDiceSchema
    | SlimMessageMediaStorySchema
    | SlimMessageMediaGiveawaySchema
    | SlimMessageMediaGiveawayResultsSchema
    | SlimMessageMediaPaidMediaSchema
    | SlimMessageMediaToDoSchema
    | SlimMessageMediaVideoStreamSchema,
    Field(discriminator="tl"),
]
