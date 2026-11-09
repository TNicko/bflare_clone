from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from telegram.slim_profile_photo import slim_profile_photo_from_full
from .slim_profile_photo import SlimTypeProfilePhotoSchema
from db.schemas.general.primitives import TelegramCommunityProfileTierSchema
from telegram.utils import normalize_tl_keys

from .tl import (
    BotInfoSchema,
    BotVerificationSchema,
    TypeChannelLocationSchema,
    ChatBannedRightsSchema,
    TypeChatReactionsSchema,
    TypePeerColorSchema as PeerColorSchema,
    RestrictionReasonSchema,
    StickerSetSchema,
    TypeEmojiStatusSchema,
    TypeWallPaperSchema,
    UsernameSchema,
)


class SimilarChannelRefSchema(BaseModel):
    external_id: str
    peer_type: str
    persisted: bool = False


class TelegramChannelSchema(BaseModel):
    model_config = ConfigDict(ser_json_bytes="base64")

    @model_validator(mode="before")
    @classmethod
    def _normalize_tl_discriminators(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return normalize_tl_keys(data)
        return data

    @field_validator("restriction_reason", mode="before")
    @classmethod
    def _restriction_reason_null(cls, v: Any) -> Any:
        if v is None:
            return []
        return v

    @field_validator("photo", mode="before")
    @classmethod
    def _slim_profile_photo(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        return slim_profile_photo_from_full(v) or v

    id: UUID | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    unavailable_at: datetime | None = None
    #: Set when public ``t.me/s`` history is not available for this channel.
    web_s_inaccessible_at: datetime | None = None
    # Megagroups only (broadcast channels cannot list participants).
    last_participant_fetch_at: datetime | None = None
    profile_photo_s3_uri: str | None = None
    harvester_id: UUID | None = None
    #: Detected content language (ISO 639), or ``und`` when undetermined.
    language: str | None = None

    similar_channels: list[SimilarChannelRefSchema] | None = None

    # --- Telegram Data ---

    # identity
    external_id: str  # telegram's signed peer id
    title: str  # display name
    username: str | None = None  # primary @username
    usernames: list[UsernameSchema] | None = None

    # photos
    photo: SlimTypeProfilePhotoSchema | None = None

    # type flags (null = unknown / not fetched)
    is_broadcast: bool | None = None  # true = one-way channel, false = group
    is_megagroup: bool | None = None  # true = supergroup
    is_gigagroup: bool | None = None  # true = broadcast group (large, admins post only)
    is_monoforum: bool | None = None  # true = per-user thread channel type
    is_forum: bool | None = None  # true = topics/form mode enabled

    # status flags (null = unknown / not fetched)
    is_verified: bool | None = None  # officially verified by Telegram
    is_scam: bool | None = None  # flagged as scam by Telegram
    is_fake: bool | None = None  # flagged as fake/impersonation by Telegram
    is_restricted: bool | None = None  # if true, see restriction_reason

    # profile provenance: tg_full | tg_min | web (null = omit on partial upsert)
    profile_tier: TelegramCommunityProfileTierSchema | None = None

    # stories
    stories_unavailable: bool | None = None  # stories feature unavailable for this channel
    stories_max_id: int | None = None  # id of the most recent story
    stories_pinned_available: bool | None = None  # channel has pinned stories available

    # participant info
    participants_count: int | None = None  # total subscriber/member count
    online_count: int | None = None  # currently online members
    admins_count: int | None = None  # number of admins
    kicked_count: int | None = None  # number of banned users
    banned_count: int | None = None  # number of restricted users

    # channel settings (null = unknown / not fetched)
    has_link: bool | None = None  # has a linked discussion group or channel
    has_geo: bool | None = None  # has a geographic location set
    slowmode_enabled: bool | None = None
    slowmode_seconds: int | None = None  # slowmode interval in seconds
    noforwards: bool | None = None  # content cannot be forwarded or saved
    join_to_send: bool | None = None  # must join before sending messages
    join_request: bool | None = None  # joining requires admin approval
    signatures: bool | None = None  # admin signatures shown on posts
    signature_profiles: bool | None = None
    antispam: bool | None = None  # telegram's automatic antispam enabled
    participants_hidden: bool | None = None  # member list is hidden
    translations_disabled: bool | None = None  # auto-translation disabled
    hidden_prehistory: bool | None = None  # messages before join are hidden
    autotranslation: bool | None = None  # auto translation enabled
    broadcast_messages_allowed: bool | None = None

    # rights
    default_banned_rights: ChatBannedRightsSchema | None = None  # default restrictions for all members

    # linked peers
    linked_chat_id: int | None = None  # discussion group linked to this channel
    linked_monoforum_id: int | None = None  # monoforum peer linked to this channel
    migrated_from_chat_id: int | None = None  # old chat id if migrated from basic group
    migrated_from_max_id: int | None = None  # last message id before migration

    # info
    about: str | None = None  # channel description/bio
    date: datetime  # date channel was created or last seen
    location: TypeChannelLocationSchema | None = None  # geographic location if has_geo is True
    pinned_msg_id: int | None = None  # id of the currently pinned message
    available_min_id: int | None = None  # messages before this id are unavailable
    first_message_at: datetime | None = None  # oldest accessible message date (not profile upsert)
    stats_dc: int | None = None  # data centre id for fetching channel statistics
    ttl_period: int | None = None  # auto-delete timer in seconds if set

    # reactions
    available_reactions: TypeChatReactionsSchema | None = None  # reactions allowed in this channel
    reactions_limit: int | None = None  # limit of unique reactions

    # paid/stars
    paid_media_allowed: bool | None = None  # paid media can be posted
    paid_reactions_available: bool | None = None  # paid reactions enabled
    paid_messages_available: bool | None = None  # paid messages enabled
    send_paid_messages_stars: int | None = None  # cost in stars to send a message
    stargifts_available: bool | None = None  # star gifts can be sent
    stargifts_count: int | None = None  # total star gifts received

    # boosts
    boosts_applied: int | None = None  # number of boosts currently applied
    boosts_unrestrict: int | None = None  # boosts needed to remove restrictions

    # emojis & stickers
    stickerset: StickerSetSchema | None = None  # channel's custom sticker set
    emojiset: StickerSetSchema | None = None  # custom emoji set for the channel

    # bots
    bot_info: list[BotInfoSchema] = Field(default_factory=list)
    bot_verification: BotVerificationSchema | None = None  # bot verification info

    # restriction
    restriction_reason: list[RestrictionReasonSchema] = Field(default_factory=list)
    restricted_sponsored: bool | None = None  # sponsored messages are restricted

    # forums
    view_forum_as_messages: bool | None = None  # view forum topics as a flat message list

    # appearance
    color: PeerColorSchema | None = None  # accent color for channel name and header
    profile_color: PeerColorSchema | None = None  # accent color for channel profile page
    emoji_status: TypeEmojiStatusSchema | None = None  # custom emoji shown next to channel name
    level: int | None = None  # channel boost level
    bot_verification_icon: int | None = None  # document id of bot verification badge icon
    theme_emoticon: str | None = None  # emoji used as the channel theme
    wallpaper: TypeWallPaperSchema | None = None  # channel wallpaper
