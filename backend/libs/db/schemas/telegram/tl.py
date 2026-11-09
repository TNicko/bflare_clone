from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from telegram.utils import normalize_tl_keys


class SchemaBase(BaseModel):
    """TL-shaped nested JSON: fields use ``tl``; input dicts may still use ``_`` (DB / Telethon)."""

    model_config = ConfigDict(populate_by_name=True, ser_json_bytes="base64")

    @model_validator(mode="before")
    @classmethod
    def _legacy_underscore_discriminator(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return normalize_tl_keys(data)
        return data


# ---------------------------------------------------------------------------
# TypePeer (PeerUser / PeerChat / PeerChannel) — normalized storage
# ---------------------------------------------------------------------------

# TL ``TypePeer`` kind (not ``TelegramPeerTypeSchema`` on harvester rows: megagroups are ``PeerChannel``).
TelegramTLPeerTypeSchema = Literal["user", "chat", "channel"]


class TelegramPeerRefSchema(BaseModel):
    """Telethon peer as a raw id plus kind — e.g. ``PeerUser`` → ``id=user_id``, ``peer_type=user``."""

    model_config = ConfigDict(frozen=True)

    id: int
    peer_type: TelegramTLPeerTypeSchema

    @model_validator(mode="before")
    @classmethod
    def _from_tl_peer(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw = normalize_tl_keys(data)
        if raw.get("peer_type") in ("user", "chat", "channel") and "id" in raw and "tl" not in raw:
            return raw
        match raw.get("tl"):
            case "PeerUser":
                return {"id": raw["user_id"], "peer_type": "user"}
            case "PeerChat":
                return {"id": raw["chat_id"], "peer_type": "chat"}
            case "PeerChannel":
                return {"id": raw["channel_id"], "peer_type": "channel"}
            case _:
                return raw


# ---------------------------------------------------------------------------
# Geo / channel location (ChannelLocation, ChannelLocationEmpty)
# ---------------------------------------------------------------------------


class GeoPointEmptySchema(SchemaBase):
    tl: Literal["GeoPointEmpty"] = "GeoPointEmpty"


class GeoPointSchema(SchemaBase):
    tl: Literal["GeoPoint"] = "GeoPoint"
    long: float
    lat: float
    access_hash: int
    accuracy_radius: int | None = None


TypeGeoPointSchema = Annotated[
    Union[GeoPointSchema, GeoPointEmptySchema],
    Field(discriminator="tl"),
]


class GeoPointAddressSchema(SchemaBase):
    tl: Literal["GeoPointAddress"] = "GeoPointAddress"
    country_iso2: str
    state: str | None = None
    city: str | None = None
    street: str | None = None


class MaskCoordsSchema(SchemaBase):
    tl: Literal["MaskCoords"] = "MaskCoords"
    n: int
    x: float
    y: float
    zoom: float


# ---------------------------------------------------------------------------
# InputUser / InputPeer (TL input-layer peers; distinct from Peer* / TelegramPeerRefSchema)
# ---------------------------------------------------------------------------


class InputUserEmptySchema(SchemaBase):
    tl: Literal["InputUserEmpty"] = "InputUserEmpty"


class InputUserSelfSchema(SchemaBase):
    tl: Literal["InputUserSelf"] = "InputUserSelf"


class InputUserSchema(SchemaBase):
    tl: Literal["InputUser"] = "InputUser"
    user_id: int
    access_hash: int


class InputUserFromMessageSchema(SchemaBase):
    tl: Literal["InputUserFromMessage"] = "InputUserFromMessage"
    peer: "TypeInputPeerSchema"
    msg_id: int
    user_id: int


TypeInputUserSchema = Annotated[
    InputUserEmptySchema | InputUserSelfSchema | InputUserSchema | InputUserFromMessageSchema,
    Field(discriminator="tl"),
]


class InputPeerEmptySchema(SchemaBase):
    tl: Literal["InputPeerEmpty"] = "InputPeerEmpty"


class InputPeerSelfSchema(SchemaBase):
    tl: Literal["InputPeerSelf"] = "InputPeerSelf"


class InputPeerChatSchema(SchemaBase):
    tl: Literal["InputPeerChat"] = "InputPeerChat"
    chat_id: int


class InputPeerUserSchema(SchemaBase):
    tl: Literal["InputPeerUser"] = "InputPeerUser"
    user_id: int
    access_hash: int


class InputPeerChannelSchema(SchemaBase):
    tl: Literal["InputPeerChannel"] = "InputPeerChannel"
    channel_id: int
    access_hash: int


class InputPeerUserFromMessageSchema(SchemaBase):
    tl: Literal["InputPeerUserFromMessage"] = "InputPeerUserFromMessage"
    peer: "TypeInputPeerSchema"
    msg_id: int
    user_id: int


class InputPeerChannelFromMessageSchema(SchemaBase):
    tl: Literal["InputPeerChannelFromMessage"] = "InputPeerChannelFromMessage"
    peer: "TypeInputPeerSchema"
    msg_id: int
    channel_id: int


TypeInputPeerSchema = Annotated[
    InputPeerEmptySchema
    | InputPeerSelfSchema
    | InputPeerChatSchema
    | InputPeerUserSchema
    | InputPeerChannelSchema
    | InputPeerUserFromMessageSchema
    | InputPeerChannelFromMessageSchema,
    Field(discriminator="tl"),
]


InputUserFromMessageSchema.model_rebuild()
InputPeerUserFromMessageSchema.model_rebuild()
InputPeerChannelFromMessageSchema.model_rebuild()


class ChannelLocationEmptySchema(SchemaBase):
    tl: Literal["ChannelLocationEmpty"] = "ChannelLocationEmpty"


class ChannelLocationGeoSchema(SchemaBase):
    tl: Literal["ChannelLocation"] = "ChannelLocation"
    geo_point: TypeGeoPointSchema
    address: str


TypeChannelLocationSchema = Annotated[
    Union[ChannelLocationGeoSchema, ChannelLocationEmptySchema],
    Field(discriminator="tl"),
]

# ---------------------------------------------------------------------------
# Reactions (TypeReaction, TypeChatReactions)
# ---------------------------------------------------------------------------


class ReactionEmptySchema(SchemaBase):
    tl: Literal["ReactionEmpty"] = "ReactionEmpty"


class ReactionEmojiSchema(SchemaBase):
    tl: Literal["ReactionEmoji"] = "ReactionEmoji"
    emoticon: str


class ReactionCustomEmojiSchema(SchemaBase):
    tl: Literal["ReactionCustomEmoji"] = "ReactionCustomEmoji"
    document_id: int


class ReactionPaidSchema(SchemaBase):
    tl: Literal["ReactionPaid"] = "ReactionPaid"


TypeReactionSchema = Annotated[
    Union[
        ReactionEmptySchema,
        ReactionEmojiSchema,
        ReactionCustomEmojiSchema,
        ReactionPaidSchema,
    ],
    Field(discriminator="tl"),
]


class ChatReactionsNoneSchema(SchemaBase):
    tl: Literal["ChatReactionsNone"] = "ChatReactionsNone"


class ChatReactionsAllSchema(SchemaBase):
    tl: Literal["ChatReactionsAll"] = "ChatReactionsAll"
    allow_custom: bool | None = None


class ChatReactionsSomeSchema(SchemaBase):
    tl: Literal["ChatReactionsSome"] = "ChatReactionsSome"
    reactions: list[TypeReactionSchema]


TypeChatReactionsSchema = Annotated[
    Union[
        ChatReactionsNoneSchema,
        ChatReactionsAllSchema,
        ChatReactionsSomeSchema,
    ],
    Field(discriminator="tl"),
]

# ---------------------------------------------------------------------------
# Rights / username / restriction
# ---------------------------------------------------------------------------


class ChatBannedRightsSchema(SchemaBase):
    tl: Literal["ChatBannedRights"] = "ChatBannedRights"
    until_date: datetime | None
    view_messages: bool | None = None
    send_messages: bool | None = None
    send_media: bool | None = None
    send_stickers: bool | None = None
    send_gifs: bool | None = None
    send_games: bool | None = None
    send_inline: bool | None = None
    embed_links: bool | None = None
    send_polls: bool | None = None
    change_info: bool | None = None
    invite_users: bool | None = None
    pin_messages: bool | None = None
    manage_topics: bool | None = None
    send_photos: bool | None = None
    send_videos: bool | None = None
    send_roundvideos: bool | None = None
    send_audios: bool | None = None
    send_voices: bool | None = None
    send_docs: bool | None = None
    send_plain: bool | None = None


class ChatAdminRightsSchema(SchemaBase):
    tl: Literal["ChatAdminRights"] = "ChatAdminRights"
    change_info: bool | None = None
    post_messages: bool | None = None
    edit_messages: bool | None = None
    delete_messages: bool | None = None
    ban_users: bool | None = None
    invite_users: bool | None = None
    pin_messages: bool | None = None
    add_admins: bool | None = None
    anonymous: bool | None = None
    manage_call: bool | None = None
    other: bool | None = None
    manage_topics: bool | None = None
    post_stories: bool | None = None
    edit_stories: bool | None = None
    delete_stories: bool | None = None
    manage_direct_messages: bool | None = None


class UsernameSchema(SchemaBase):
    tl: Literal["Username"] = "Username"
    username: str
    editable: bool | None = None
    active: bool | None = None


class RestrictionReasonSchema(SchemaBase):
    tl: Literal["RestrictionReason"] = "RestrictionReason"
    platform: str
    reason: str
    text: str


# ---------------------------------------------------------------------------
# Peer color / emoji status
# ---------------------------------------------------------------------------


class PeerColorSchema(SchemaBase):
    tl: Literal["PeerColor"] = "PeerColor"
    color: int | None = None
    background_emoji_id: int | None = None


class PeerColorCollectibleSchema(SchemaBase):
    tl: Literal["PeerColorCollectible"] = "PeerColorCollectible"
    collectible_id: int
    gift_emoji_id: int
    background_emoji_id: int
    accent_color: int
    colors: list[int]
    dark_accent_color: int | None = None
    dark_colors: list[int] | None = None


TypePeerColorSchema = Annotated[
    Union[PeerColorSchema, PeerColorCollectibleSchema],
    Field(discriminator="tl"),
]


class EmojiStatusEmptySchema(SchemaBase):
    tl: Literal["EmojiStatusEmpty"] = "EmojiStatusEmpty"


class EmojiStatusDataSchema(SchemaBase):
    tl: Literal["EmojiStatus"] = "EmojiStatus"
    document_id: int
    until: datetime | None = None


class EmojiStatusCollectibleSchema(SchemaBase):
    tl: Literal["EmojiStatusCollectible"] = "EmojiStatusCollectible"
    collectible_id: int
    document_id: int
    title: str
    slug: str
    pattern_document_id: int
    center_color: int
    edge_color: int
    pattern_color: int
    text_color: int
    until: datetime | None = None


TypeEmojiStatusSchema = Annotated[
    Union[
        EmojiStatusEmptySchema,
        EmojiStatusDataSchema,
        EmojiStatusCollectibleSchema,
    ],
    Field(discriminator="tl"),
]

# ---------------------------------------------------------------------------
# Photo sizes / video sizes (nested under Photo)
# ---------------------------------------------------------------------------


class PhotoSizeEmptySchema(SchemaBase):
    # ``tl`` = TL constructor; ``type`` matches Telethon (size variant key, e.g. "m")
    tl: Literal["PhotoSizeEmpty"] = "PhotoSizeEmpty"
    type: str


class PhotoSizeDataSchema(SchemaBase):
    tl: Literal["PhotoSize"] = "PhotoSize"
    type: str
    w: int
    h: int
    size: int


class PhotoCachedSizeSchema(SchemaBase):
    tl: Literal["PhotoCachedSize"] = "PhotoCachedSize"
    type: str
    w: int
    h: int
    data: bytes = Field(validation_alias="bytes")


class PhotoStrippedSizeSchema(SchemaBase):
    tl: Literal["PhotoStrippedSize"] = "PhotoStrippedSize"
    type: str
    data: bytes = Field(validation_alias="bytes")


class PhotoSizeProgressiveSchema(SchemaBase):
    tl: Literal["PhotoSizeProgressive"] = "PhotoSizeProgressive"
    type: str
    w: int
    h: int
    sizes: list[int]


class PhotoPathSizeSchema(SchemaBase):
    tl: Literal["PhotoPathSize"] = "PhotoPathSize"
    type: str
    data: bytes = Field(validation_alias="bytes")


TypePhotoSizeSchema = Annotated[
    Union[
        PhotoSizeEmptySchema,
        PhotoSizeDataSchema,
        PhotoCachedSizeSchema,
        PhotoStrippedSizeSchema,
        PhotoSizeProgressiveSchema,
        PhotoPathSizeSchema,
    ],
    Field(discriminator="tl"),
]


class VideoSizeSchema(SchemaBase):
    tl: Literal["VideoSize"] = "VideoSize"
    type: str
    w: int
    h: int
    size: int
    video_start_ts: float | None = None


class VideoSizeEmojiMarkupSchema(SchemaBase):
    tl: Literal["VideoSizeEmojiMarkup"] = "VideoSizeEmojiMarkup"
    emoji_id: int
    background_colors: list[int]


class VideoSizeStickerMarkupSchema(SchemaBase):
    tl: Literal["VideoSizeStickerMarkup"] = "VideoSizeStickerMarkup"
    stickerset: dict[str, Any]
    sticker_id: int
    background_colors: list[int]


TypeVideoSizeSchema = Annotated[
    Union[
        VideoSizeSchema,
        VideoSizeEmojiMarkupSchema,
        VideoSizeStickerMarkupSchema,
    ],
    Field(discriminator="tl"),
]

# ---------------------------------------------------------------------------
# ChatPhoto / Photo (full)
# ---------------------------------------------------------------------------


class ChatPhotoEmptySchema(SchemaBase):
    tl: Literal["ChatPhotoEmpty"] = "ChatPhotoEmpty"


class ChatPhotoSchema(SchemaBase):
    tl: Literal["ChatPhoto"] = "ChatPhoto"
    photo_id: int
    dc_id: int
    has_video: bool | None = None
    stripped_thumb: bytes | None = None


class PhotoEmptySchema(SchemaBase):
    tl: Literal["PhotoEmpty"] = "PhotoEmpty"
    id: int


class PhotoSchema(SchemaBase):
    tl: Literal["Photo"] = "Photo"
    id: int
    access_hash: int
    file_reference: bytes
    date: datetime | None
    sizes: list[TypePhotoSizeSchema]
    dc_id: int
    has_stickers: bool | None = None
    video_sizes: list[TypeVideoSizeSchema] | None = None


TypeChatPhotoSchema = Annotated[
    Union[ChatPhotoSchema, ChatPhotoEmptySchema],
    Field(discriminator="tl"),
]
TypePhotoFullSchema = Annotated[
    Union[PhotoSchema, PhotoEmptySchema],
    Field(discriminator="tl"),
]

TypePhotoSchema = Annotated[
    Union[
        ChatPhotoSchema,
        ChatPhotoEmptySchema,
        PhotoSchema,
        PhotoEmptySchema,
    ],
    Field(discriminator="tl"),
]

# ---------------------------------------------------------------------------
# Sticker set
# ---------------------------------------------------------------------------


class StickerSetSchema(SchemaBase):
    tl: Literal["StickerSet"] = "StickerSet"
    id: int
    access_hash: int
    title: str
    short_name: str
    count: int
    hash: int
    archived: bool | None = None
    official: bool | None = None
    masks: bool | None = None
    emojis: bool | None = None
    text_color: bool | None = None
    channel_emoji_status: bool | None = None
    creator: bool | None = None
    installed_date: datetime | None = None
    thumbs: list[TypePhotoSizeSchema] | None = None
    thumb_dc_id: int | None = None
    thumb_version: int | None = None
    thumb_document_id: int | None = None


# ---------------------------------------------------------------------------
# BotInfo graph
# ---------------------------------------------------------------------------


class BotCommandSchema(SchemaBase):
    tl: Literal["BotCommand"] = "BotCommand"
    command: str
    description: str


class BotMenuButtonDefaultSchema(SchemaBase):
    tl: Literal["BotMenuButtonDefault"] = "BotMenuButtonDefault"


class BotMenuButtonCommandsSchema(SchemaBase):
    tl: Literal["BotMenuButtonCommands"] = "BotMenuButtonCommands"


class BotMenuButtonUrlSchema(SchemaBase):
    tl: Literal["BotMenuButton"] = "BotMenuButton"
    text: str
    url: str


TypeBotMenuButtonSchema = Annotated[
    Union[
        BotMenuButtonDefaultSchema,
        BotMenuButtonCommandsSchema,
        BotMenuButtonUrlSchema,
    ],
    Field(discriminator="tl"),
]


class BotAppSettingsSchema(SchemaBase):
    tl: Literal["BotAppSettings"] = "BotAppSettings"
    placeholder_path: bytes | None = None
    background_color: int | None = None
    background_dark_color: int | None = None
    header_color: int | None = None
    header_dark_color: int | None = None


class BotVerifierSettingsSchema(SchemaBase):
    tl: Literal["BotVerifierSettings"] = "BotVerifierSettings"
    icon: int
    company: str
    can_modify_custom_description: bool | None = None
    custom_description: str | None = None


class BotInfoSchema(SchemaBase):
    tl: Literal["BotInfo"] = "BotInfo"
    has_preview_medias: bool | None = None
    user_id: int | None = None
    description: str | None = None
    description_photo: TypePhotoSchema | None = None
    description_document: TypeDocumentSchema | None = None
    commands: list[BotCommandSchema] | None = None
    menu_button: TypeBotMenuButtonSchema | None = None
    privacy_policy_url: str | None = None
    app_settings: BotAppSettingsSchema | None = None
    verifier_settings: BotVerifierSettingsSchema | None = None


class BotVerificationSchema(SchemaBase):
    tl: Literal["BotVerification"] = "BotVerification"
    bot_id: int
    icon: int
    description: str


# ---------------------------------------------------------------------------
# WallPaper
# ---------------------------------------------------------------------------


class WallPaperSettingsSchema(SchemaBase):
    tl: Literal["WallPaperSettings"] = "WallPaperSettings"
    blur: bool | None = None
    motion: bool | None = None
    background_color: int | None = None
    second_background_color: int | None = None
    third_background_color: int | None = None
    fourth_background_color: int | None = None
    intensity: int | None = None
    rotation: int | None = None
    emoticon: str | None = None


class WallPaperSchema(SchemaBase):
    tl: Literal["WallPaper"] = "WallPaper"
    id: int
    access_hash: int
    slug: str
    document: TypeDocumentSchema
    creator: bool | None = None
    default: bool | None = None
    pattern: bool | None = None
    dark: bool | None = None
    settings: WallPaperSettingsSchema | None = None


class WallPaperNoFileSchema(SchemaBase):
    tl: Literal["WallPaperNoFile"] = "WallPaperNoFile"
    id: int
    default: bool | None = None
    dark: bool | None = None
    settings: WallPaperSettingsSchema | None = None


TypeWallPaperSchema = Annotated[
    Union[WallPaperSchema, WallPaperNoFileSchema],
    Field(discriminator="tl"),
]


# ---------------------------------------------------------------------------
# Message reactions (MessageReactions, ReactionCount, MessagePeerReaction)
# ---------------------------------------------------------------------------


class ReactionCountSchema(SchemaBase):
    tl: Literal["ReactionCount"] = "ReactionCount"
    reaction: TypeReactionSchema
    count: int
    chosen_order: int | None = None


class MessagePeerReactionSchema(SchemaBase):
    tl: Literal["MessagePeerReaction"] = "MessagePeerReaction"
    peer_id: TelegramPeerRefSchema
    date: datetime | None
    reaction: TypeReactionSchema
    big: bool | None = None
    unread: bool | None = None
    my: bool | None = None


class MessageReactorSchema(SchemaBase):
    tl: Literal["MessageReactor"] = "MessageReactor"
    count: int
    top: bool | None = None
    my: bool | None = None
    anonymous: bool | None = None
    peer_id: TelegramPeerRefSchema | None = None


class MessageReactionsSchema(SchemaBase):
    tl: Literal["MessageReactions"] = "MessageReactions"
    results: list[ReactionCountSchema]
    min: bool | None = None
    can_see_list: bool | None = None
    reactions_as_tags: bool | None = None
    recent_reactions: list[MessagePeerReactionSchema] | None = None
    top_reactors: list[MessageReactorSchema] | None = None


from .tl_media import (  # noqa: E402
    TypeDocumentSchema,
)
