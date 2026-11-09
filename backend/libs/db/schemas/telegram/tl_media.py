"""Nested TL schemas for :class:`TypeMessageMedia` and dependencies."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from .tl import (
    ChatAdminRightsSchema,
    ChatBannedRightsSchema,
    GeoPointAddressSchema,
    MaskCoordsSchema,
    ReactionCountSchema,
    SchemaBase,
    TelegramPeerRefSchema,
    TypeChatPhotoSchema,
    TypeEmojiStatusSchema,
    TypeGeoPointSchema,
    TypeInputPeerSchema,
    TypeInputUserSchema,
    TypePhotoSchema,
    TypePhotoSizeSchema,
    TypeReactionSchema,
    TypeVideoSizeSchema,
    TypeWallPaperSchema,
)


class MessageEntityUnknownSchema(SchemaBase):
    tl: Literal["MessageEntityUnknown"] = "MessageEntityUnknown"
    offset: int
    length: int


class MessageEntityMentionSchema(SchemaBase):
    tl: Literal["MessageEntityMention"] = "MessageEntityMention"
    offset: int
    length: int


class MessageEntityHashtagSchema(SchemaBase):
    tl: Literal["MessageEntityHashtag"] = "MessageEntityHashtag"
    offset: int
    length: int


class MessageEntityBotCommandSchema(SchemaBase):
    tl: Literal["MessageEntityBotCommand"] = "MessageEntityBotCommand"
    offset: int
    length: int


class MessageEntityUrlSchema(SchemaBase):
    tl: Literal["MessageEntityUrl"] = "MessageEntityUrl"
    offset: int
    length: int


class MessageEntityEmailSchema(SchemaBase):
    tl: Literal["MessageEntityEmail"] = "MessageEntityEmail"
    offset: int
    length: int


class MessageEntityBoldSchema(SchemaBase):
    tl: Literal["MessageEntityBold"] = "MessageEntityBold"
    offset: int
    length: int


class MessageEntityItalicSchema(SchemaBase):
    tl: Literal["MessageEntityItalic"] = "MessageEntityItalic"
    offset: int
    length: int


class MessageEntityCodeSchema(SchemaBase):
    tl: Literal["MessageEntityCode"] = "MessageEntityCode"
    offset: int
    length: int


class MessageEntityPreSchema(SchemaBase):
    tl: Literal["MessageEntityPre"] = "MessageEntityPre"
    offset: int
    length: int
    language: str


class MessageEntityTextUrlSchema(SchemaBase):
    tl: Literal["MessageEntityTextUrl"] = "MessageEntityTextUrl"
    offset: int
    length: int
    url: str


class MessageEntityMentionNameSchema(SchemaBase):
    tl: Literal["MessageEntityMentionName"] = "MessageEntityMentionName"
    offset: int
    length: int
    user_id: int


class InputMessageEntityMentionNameSchema(SchemaBase):
    tl: Literal["InputMessageEntityMentionName"] = "InputMessageEntityMentionName"
    offset: int
    length: int
    user_id: TypeInputUserSchema


class MessageEntityPhoneSchema(SchemaBase):
    tl: Literal["MessageEntityPhone"] = "MessageEntityPhone"
    offset: int
    length: int


class MessageEntityCashtagSchema(SchemaBase):
    tl: Literal["MessageEntityCashtag"] = "MessageEntityCashtag"
    offset: int
    length: int


class MessageEntityUnderlineSchema(SchemaBase):
    tl: Literal["MessageEntityUnderline"] = "MessageEntityUnderline"
    offset: int
    length: int


class MessageEntityStrikeSchema(SchemaBase):
    tl: Literal["MessageEntityStrike"] = "MessageEntityStrike"
    offset: int
    length: int


class MessageEntityBankCardSchema(SchemaBase):
    tl: Literal["MessageEntityBankCard"] = "MessageEntityBankCard"
    offset: int
    length: int


class MessageEntitySpoilerSchema(SchemaBase):
    tl: Literal["MessageEntitySpoiler"] = "MessageEntitySpoiler"
    offset: int
    length: int


class MessageEntityCustomEmojiSchema(SchemaBase):
    tl: Literal["MessageEntityCustomEmoji"] = "MessageEntityCustomEmoji"
    offset: int
    length: int
    document_id: int


class MessageEntityBlockquoteSchema(SchemaBase):
    tl: Literal["MessageEntityBlockquote"] = "MessageEntityBlockquote"
    offset: int
    length: int
    collapsed: bool | None = None


class MessageEntityFormattedDateSchema(SchemaBase):
    tl: Literal["MessageEntityFormattedDate"] = "MessageEntityFormattedDate"
    offset: int
    length: int
    date: datetime | None | None
    relative: bool | None = None
    short_time: bool | None = None
    long_time: bool | None = None
    short_date: bool | None = None
    long_date: bool | None = None
    day_of_week: bool | None = None


class MessageEntityDiffInsertSchema(SchemaBase):
    tl: Literal["MessageEntityDiffInsert"] = "MessageEntityDiffInsert"
    offset: int
    length: int


class MessageEntityDiffReplaceSchema(SchemaBase):
    tl: Literal["MessageEntityDiffReplace"] = "MessageEntityDiffReplace"
    offset: int
    length: int
    old_text: str


class MessageEntityDiffDeleteSchema(SchemaBase):
    tl: Literal["MessageEntityDiffDelete"] = "MessageEntityDiffDelete"
    offset: int
    length: int


class TextWithEntitiesSchema(SchemaBase):
    tl: Literal["TextWithEntities"] = "TextWithEntities"
    text: str
    entities: list[TypeMessageEntitySchema]


class DocumentAttributeImageSizeSchema(SchemaBase):
    tl: Literal["DocumentAttributeImageSize"] = "DocumentAttributeImageSize"
    w: int
    h: int


class DocumentAttributeAnimatedSchema(SchemaBase):
    tl: Literal["DocumentAttributeAnimated"] = "DocumentAttributeAnimated"


class DocumentAttributeStickerSchema(SchemaBase):
    tl: Literal["DocumentAttributeSticker"] = "DocumentAttributeSticker"
    alt: str
    stickerset: TypeInputStickerSetSchema
    mask: bool | None = None
    mask_coords: MaskCoordsSchema | None = None


class DocumentAttributeVideoSchema(SchemaBase):
    tl: Literal["DocumentAttributeVideo"] = "DocumentAttributeVideo"
    duration: float
    w: int
    h: int
    round_message: bool | None = None
    supports_streaming: bool | None = None
    nosound: bool | None = None
    preload_prefix_size: int | None = None
    video_start_ts: float | None = None
    video_codec: str | None = None


class DocumentAttributeAudioSchema(SchemaBase):
    tl: Literal["DocumentAttributeAudio"] = "DocumentAttributeAudio"
    duration: int
    voice: bool | None = None
    title: str | None = None
    performer: str | None = None
    waveform: bytes | None = None


class DocumentAttributeFilenameSchema(SchemaBase):
    tl: Literal["DocumentAttributeFilename"] = "DocumentAttributeFilename"
    file_name: str


class DocumentAttributeHasStickersSchema(SchemaBase):
    tl: Literal["DocumentAttributeHasStickers"] = "DocumentAttributeHasStickers"


class DocumentAttributeCustomEmojiSchema(SchemaBase):
    tl: Literal["DocumentAttributeCustomEmoji"] = "DocumentAttributeCustomEmoji"
    alt: str
    stickerset: TypeInputStickerSetSchema
    free: bool | None = None
    text_color: bool | None = None


class WebDocumentSchema(SchemaBase):
    tl: Literal["WebDocument"] = "WebDocument"
    url: str
    access_hash: int
    size: int
    mime_type: str
    attributes: list[TypeDocumentAttributeSchema]


class WebDocumentNoProxySchema(SchemaBase):
    tl: Literal["WebDocumentNoProxy"] = "WebDocumentNoProxy"
    url: str
    size: int
    mime_type: str
    attributes: list[TypeDocumentAttributeSchema]


class GameSchema(SchemaBase):
    tl: Literal["Game"] = "Game"
    id: int
    access_hash: int
    short_name: str
    title: str
    description: str
    photo: TypePhotoSchema
    document: TypeDocumentSchema | None = None


class EmojiGameOutcomeSchema(SchemaBase):
    tl: Literal["EmojiGameOutcome"] = "EmojiGameOutcome"
    seed: bytes
    stake_ton_amount: int
    ton_amount: int


class EmojiGameUnavailableSchema(SchemaBase):
    tl: Literal["EmojiGameUnavailable"] = "EmojiGameUnavailable"


class PollAnswerSchema(SchemaBase):
    tl: Literal["PollAnswer"] = "PollAnswer"
    text: TextWithEntitiesSchema
    option: bytes
    media: TypeMessageMediaSchema | None = None
    added_by: TelegramPeerRefSchema | None = None
    date: datetime | None | None = None


class PollAnswerVotersSchema(SchemaBase):
    tl: Literal["PollAnswerVoters"] = "PollAnswerVoters"
    option: bytes
    chosen: bool | None = None
    correct: bool | None = None
    voters: int | None = None
    recent_voters: list[TelegramPeerRefSchema] | None = None


class PollSchema(SchemaBase):
    tl: Literal["Poll"] = "Poll"
    id: int
    question: TextWithEntitiesSchema
    answers: list[PollAnswerSchema]
    hash: int
    closed: bool | None = None
    public_voters: bool | None = None
    multiple_choice: bool | None = None
    quiz: bool | None = None
    open_answers: bool | None = None
    revoting_disabled: bool | None = None
    shuffle_answers: bool | None = None
    hide_results_until_close: bool | None = None
    creator: bool | None = None
    close_period: int | None = None
    close_date: datetime | None | None = None


class PollResultsSchema(SchemaBase):
    tl: Literal["PollResults"] = "PollResults"
    min: bool | None = None
    has_unread_votes: bool | None = None
    results: list[PollAnswerVotersSchema] | None = None
    total_voters: int | None = None
    recent_voters: list[TelegramPeerRefSchema] | None = None
    solution: str | None = None
    solution_entities: list[TypeMessageEntitySchema] | None = None
    solution_media: TypeMessageMediaSchema | None = None


class MessageExtendedMediaSchema(SchemaBase):
    tl: Literal["MessageExtendedMedia"] = "MessageExtendedMedia"
    media: TypeMessageMediaSchema


class MessageExtendedMediaPreviewSchema(SchemaBase):
    tl: Literal["MessageExtendedMediaPreview"] = "MessageExtendedMediaPreview"
    w: int | None = None
    h: int | None = None
    thumb: TypePhotoSizeSchema | None = None
    video_duration: int | None = None


class WebPageEmptySchema(SchemaBase):
    tl: Literal["WebPageEmpty"] = "WebPageEmpty"
    id: int
    url: str | None = None


class WebPagePendingSchema(SchemaBase):
    tl: Literal["WebPagePending"] = "WebPagePending"
    id: int
    date: datetime | None | None
    url: str | None = None


class WebPageNotModifiedSchema(SchemaBase):
    tl: Literal["WebPageNotModified"] = "WebPageNotModified"
    cached_page_views: int | None = None


class WebPageSchema(SchemaBase):
    tl: Literal["WebPage"] = "WebPage"
    id: int
    url: str
    display_url: str
    hash: int
    has_large_media: bool | None = None
    video_cover_photo: bool | None = None
    type: str | None = None
    site_name: str | None = None
    title: str | None = None
    description: str | None = None
    photo: TypePhotoSchema | None = None
    embed_url: str | None = None
    embed_type: str | None = None
    embed_width: int | None = None
    embed_height: int | None = None
    duration: int | None = None
    author: str | None = None
    document: TypeDocumentSchema | None = None
    cached_page: PageSchema | None = None
    attributes: list[TypeWebPageAttributeSchema] | None = None


class WebPageAttributeThemeSchema(SchemaBase):
    tl: Literal["WebPageAttributeTheme"] = "WebPageAttributeTheme"
    documents: list[DocumentSchema] | None = None
    settings: ThemeSettingsSchema | None = None


class WebPageAttributeStorySchema(SchemaBase):
    tl: Literal["WebPageAttributeStory"] = "WebPageAttributeStory"
    peer: TelegramPeerRefSchema
    id: int
    story: TypeStoryItemSchema | None = None


class WebPageAttributeStickerSetSchema(SchemaBase):
    tl: Literal["WebPageAttributeStickerSet"] = "WebPageAttributeStickerSet"
    stickers: list[DocumentSchema]
    emojis: bool | None = None
    text_color: bool | None = None


class WebPageAttributeUniqueStarGiftSchema(SchemaBase):
    tl: Literal["WebPageAttributeUniqueStarGift"] = "WebPageAttributeUniqueStarGift"
    gift: TypeStarGiftSchema


class WebPageAttributeStarGiftCollectionSchema(SchemaBase):
    tl: Literal["WebPageAttributeStarGiftCollection"] = "WebPageAttributeStarGiftCollection"
    icons: list[DocumentSchema]


class WebPageAttributeStarGiftAuctionSchema(SchemaBase):
    tl: Literal["WebPageAttributeStarGiftAuction"] = "WebPageAttributeStarGiftAuction"
    gift: TypeStarGiftSchema
    end_date: datetime | None | None


class PageSchema(SchemaBase):
    tl: Literal["Page"] = "Page"
    url: str
    blocks: list[TypePageBlockSchema]
    photos: list[TypePhotoSchema]
    documents: list[DocumentSchema]
    part: bool | None = None
    rtl: bool | None = None
    v2: bool | None = None
    views: int | None = None


class InputGroupCallSchema(SchemaBase):
    tl: Literal["InputGroupCall"] = "InputGroupCall"
    id: int
    access_hash: int


class InputGroupCallSlugSchema(SchemaBase):
    tl: Literal["InputGroupCallSlug"] = "InputGroupCallSlug"
    slug: str


class InputGroupCallInviteMessageSchema(SchemaBase):
    tl: Literal["InputGroupCallInviteMessage"] = "InputGroupCallInviteMessage"
    msg_id: int


class StoryItemSchema(SchemaBase):
    tl: Literal["StoryItem"] = "StoryItem"
    id: int
    date: datetime | None | None
    expire_date: datetime | None | None
    media: TypeMessageMediaSchema
    pinned: bool | None = None
    public: bool | None = None
    close_friends: bool | None = None
    min: bool | None = None
    noforwards: bool | None = None
    edited: bool | None = None
    contacts: bool | None = None
    selected_contacts: bool | None = None
    out: bool | None = None
    from_id: TelegramPeerRefSchema | None = None
    fwd_from: StoryFwdHeaderSchema | None = None
    caption: str | None = None
    entities: list[TypeMessageEntitySchema] | None = None
    media_areas: list[TypeMediaAreaSchema] | None = None
    privacy: list[TypePrivacyRuleSchema] | None = None
    views: StoryViewsSchema | None = None
    sent_reaction: TypeReactionSchema | None = None
    albums: list[int] | None = None
    music: DocumentSchema | None = None


class StoryItemDeletedSchema(SchemaBase):
    tl: Literal["StoryItemDeleted"] = "StoryItemDeleted"
    id: int


class StoryItemSkippedSchema(SchemaBase):
    tl: Literal["StoryItemSkipped"] = "StoryItemSkipped"
    id: int
    date: datetime | None | None
    expire_date: datetime | None | None
    close_friends: bool | None = None
    live: bool | None = None


class StoryFwdHeaderSchema(SchemaBase):
    tl: Literal["StoryFwdHeader"] = "StoryFwdHeader"
    modified: bool | None = None
    from_: TelegramPeerRefSchema | None = None
    from_name: str | None = None
    story_id: int | None = None


class StoryViewsSchema(SchemaBase):
    tl: Literal["StoryViews"] = "StoryViews"
    views_count: int
    has_viewers: bool | None = None
    forwards_count: int | None = None
    reactions: list[list[ReactionCountSchema]] | None = None
    reactions_count: int | None = None
    recent_viewers: list[int] | None = None


class MediaAreaCoordinatesSchema(SchemaBase):
    tl: Literal["MediaAreaCoordinates"] = "MediaAreaCoordinates"
    x: float
    y: float
    w: float
    h: float
    rotation: float
    radius: float | None = None


class MediaAreaVenueSchema(SchemaBase):
    tl: Literal["MediaAreaVenue"] = "MediaAreaVenue"
    coordinates: MediaAreaCoordinatesSchema
    geo: TypeGeoPointSchema
    title: str
    address: str
    provider: str
    venue_id: str
    venue_type: str


class MediaAreaGeoPointSchema(SchemaBase):
    tl: Literal["MediaAreaGeoPoint"] = "MediaAreaGeoPoint"
    coordinates: MediaAreaCoordinatesSchema
    geo: TypeGeoPointSchema
    address: GeoPointAddressSchema | None = None


class MediaAreaSuggestedReactionSchema(SchemaBase):
    tl: Literal["MediaAreaSuggestedReaction"] = "MediaAreaSuggestedReaction"
    coordinates: MediaAreaCoordinatesSchema
    reaction: TypeReactionSchema
    dark: bool | None = None
    flipped: bool | None = None


class MediaAreaChannelPostSchema(SchemaBase):
    tl: Literal["MediaAreaChannelPost"] = "MediaAreaChannelPost"
    coordinates: MediaAreaCoordinatesSchema
    channel_id: int
    msg_id: int


class MediaAreaUrlSchema(SchemaBase):
    tl: Literal["MediaAreaUrl"] = "MediaAreaUrl"
    coordinates: MediaAreaCoordinatesSchema
    url: str


class MediaAreaWeatherSchema(SchemaBase):
    tl: Literal["MediaAreaWeather"] = "MediaAreaWeather"
    coordinates: MediaAreaCoordinatesSchema
    emoji: str
    temperature_c: float
    color: int


class MediaAreaStarGiftSchema(SchemaBase):
    tl: Literal["MediaAreaStarGift"] = "MediaAreaStarGift"
    coordinates: MediaAreaCoordinatesSchema
    slug: str


class PrivacyValueAllowContactsSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowContacts"] = "PrivacyValueAllowContacts"


class PrivacyValueAllowAllSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowAll"] = "PrivacyValueAllowAll"


class PrivacyValueAllowUsersSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowUsers"] = "PrivacyValueAllowUsers"
    users: list[int]


class PrivacyValueDisallowContactsSchema(SchemaBase):
    tl: Literal["PrivacyValueDisallowContacts"] = "PrivacyValueDisallowContacts"


class PrivacyValueDisallowAllSchema(SchemaBase):
    tl: Literal["PrivacyValueDisallowAll"] = "PrivacyValueDisallowAll"


class PrivacyValueDisallowUsersSchema(SchemaBase):
    tl: Literal["PrivacyValueDisallowUsers"] = "PrivacyValueDisallowUsers"
    users: list[int]


class PrivacyValueAllowChatParticipantsSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowChatParticipants"] = "PrivacyValueAllowChatParticipants"
    chats: list[int]


class PrivacyValueDisallowChatParticipantsSchema(SchemaBase):
    tl: Literal["PrivacyValueDisallowChatParticipants"] = "PrivacyValueDisallowChatParticipants"
    chats: list[int]


class PrivacyValueAllowCloseFriendsSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowCloseFriends"] = "PrivacyValueAllowCloseFriends"


class PrivacyValueAllowPremiumSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowPremium"] = "PrivacyValueAllowPremium"


class PrivacyValueAllowBotsSchema(SchemaBase):
    tl: Literal["PrivacyValueAllowBots"] = "PrivacyValueAllowBots"


class PrivacyValueDisallowBotsSchema(SchemaBase):
    tl: Literal["PrivacyValueDisallowBots"] = "PrivacyValueDisallowBots"


class TodoItemSchema(SchemaBase):
    tl: Literal["TodoItem"] = "TodoItem"
    id: int
    title: TextWithEntitiesSchema


class TodoListSchema(SchemaBase):
    tl: Literal["TodoList"] = "TodoList"
    title: TextWithEntitiesSchema
    list: list[TodoItemSchema]
    others_can_append: bool | None = None
    others_can_complete: bool | None = None


class TodoCompletionSchema(SchemaBase):
    tl: Literal["TodoCompletion"] = "TodoCompletion"
    id: int
    completed_by: TelegramPeerRefSchema
    date: datetime | None | None


class InputStickerSetEmptySchema(SchemaBase):
    tl: Literal["InputStickerSetEmpty"] = "InputStickerSetEmpty"


class InputStickerSetIDSchema(SchemaBase):
    tl: Literal["InputStickerSetID"] = "InputStickerSetID"
    id: int
    access_hash: int


class InputStickerSetShortNameSchema(SchemaBase):
    tl: Literal["InputStickerSetShortName"] = "InputStickerSetShortName"
    short_name: str


class InputStickerSetAnimatedEmojiSchema(SchemaBase):
    tl: Literal["InputStickerSetAnimatedEmoji"] = "InputStickerSetAnimatedEmoji"


class InputStickerSetDiceSchema(SchemaBase):
    tl: Literal["InputStickerSetDice"] = "InputStickerSetDice"
    emoticon: str


class InputStickerSetAnimatedEmojiAnimationsSchema(SchemaBase):
    tl: Literal["InputStickerSetAnimatedEmojiAnimations"] = "InputStickerSetAnimatedEmojiAnimations"


class InputStickerSetPremiumGiftsSchema(SchemaBase):
    tl: Literal["InputStickerSetPremiumGifts"] = "InputStickerSetPremiumGifts"


class InputStickerSetEmojiGenericAnimationsSchema(SchemaBase):
    tl: Literal["InputStickerSetEmojiGenericAnimations"] = "InputStickerSetEmojiGenericAnimations"


class InputStickerSetEmojiDefaultStatusesSchema(SchemaBase):
    tl: Literal["InputStickerSetEmojiDefaultStatuses"] = "InputStickerSetEmojiDefaultStatuses"


class InputStickerSetEmojiDefaultTopicIconsSchema(SchemaBase):
    tl: Literal["InputStickerSetEmojiDefaultTopicIcons"] = "InputStickerSetEmojiDefaultTopicIcons"


class InputStickerSetEmojiChannelDefaultStatusesSchema(SchemaBase):
    tl: Literal["InputStickerSetEmojiChannelDefaultStatuses"] = "InputStickerSetEmojiChannelDefaultStatuses"


class InputStickerSetTonGiftsSchema(SchemaBase):
    tl: Literal["InputStickerSetTonGifts"] = "InputStickerSetTonGifts"


class InputChannelEmptySchema(SchemaBase):
    tl: Literal["InputChannelEmpty"] = "InputChannelEmpty"


class InputChannelSchema(SchemaBase):
    tl: Literal["InputChannel"] = "InputChannel"
    channel_id: int
    access_hash: int


class InputChannelFromMessageSchema(SchemaBase):
    tl: Literal["InputChannelFromMessage"] = "InputChannelFromMessage"
    peer: TypeInputPeerSchema
    msg_id: int
    channel_id: int


class BaseThemeClassicSchema(SchemaBase):
    tl: Literal["BaseThemeClassic"] = "BaseThemeClassic"


class BaseThemeDaySchema(SchemaBase):
    tl: Literal["BaseThemeDay"] = "BaseThemeDay"


class BaseThemeNightSchema(SchemaBase):
    tl: Literal["BaseThemeNight"] = "BaseThemeNight"


class BaseThemeTintedSchema(SchemaBase):
    tl: Literal["BaseThemeTinted"] = "BaseThemeTinted"


class BaseThemeArcticSchema(SchemaBase):
    tl: Literal["BaseThemeArctic"] = "BaseThemeArctic"


class TextEmptySchema(SchemaBase):
    tl: Literal["TextEmpty"] = "TextEmpty"


class TextPlainSchema(SchemaBase):
    tl: Literal["TextPlain"] = "TextPlain"
    text: str


class TextBoldSchema(SchemaBase):
    tl: Literal["TextBold"] = "TextBold"
    text: TypeRichTextSchema


class TextItalicSchema(SchemaBase):
    tl: Literal["TextItalic"] = "TextItalic"
    text: TypeRichTextSchema


class TextUnderlineSchema(SchemaBase):
    tl: Literal["TextUnderline"] = "TextUnderline"
    text: TypeRichTextSchema


class TextStrikeSchema(SchemaBase):
    tl: Literal["TextStrike"] = "TextStrike"
    text: TypeRichTextSchema


class TextFixedSchema(SchemaBase):
    tl: Literal["TextFixed"] = "TextFixed"
    text: TypeRichTextSchema


class TextUrlSchema(SchemaBase):
    tl: Literal["TextUrl"] = "TextUrl"
    text: TypeRichTextSchema
    url: str
    webpage_id: int


class TextEmailSchema(SchemaBase):
    tl: Literal["TextEmail"] = "TextEmail"
    text: TypeRichTextSchema
    email: str


class TextConcatSchema(SchemaBase):
    tl: Literal["TextConcat"] = "TextConcat"
    texts: list[TypeRichTextSchema]


class TextSubscriptSchema(SchemaBase):
    tl: Literal["TextSubscript"] = "TextSubscript"
    text: TypeRichTextSchema


class TextSuperscriptSchema(SchemaBase):
    tl: Literal["TextSuperscript"] = "TextSuperscript"
    text: TypeRichTextSchema


class TextMarkedSchema(SchemaBase):
    tl: Literal["TextMarked"] = "TextMarked"
    text: TypeRichTextSchema


class TextPhoneSchema(SchemaBase):
    tl: Literal["TextPhone"] = "TextPhone"
    text: TypeRichTextSchema
    phone: str


class TextImageSchema(SchemaBase):
    tl: Literal["TextImage"] = "TextImage"
    document_id: int
    w: int
    h: int


class TextAnchorSchema(SchemaBase):
    tl: Literal["TextAnchor"] = "TextAnchor"
    text: TypeRichTextSchema
    name: str


class PageBlockUnsupportedSchema(SchemaBase):
    tl: Literal["PageBlockUnsupported"] = "PageBlockUnsupported"


class PageBlockTitleSchema(SchemaBase):
    tl: Literal["PageBlockTitle"] = "PageBlockTitle"
    text: TypeRichTextSchema


class PageBlockSubtitleSchema(SchemaBase):
    tl: Literal["PageBlockSubtitle"] = "PageBlockSubtitle"
    text: TypeRichTextSchema


class PageBlockAuthorDateSchema(SchemaBase):
    tl: Literal["PageBlockAuthorDate"] = "PageBlockAuthorDate"
    author: TypeRichTextSchema
    published_date: datetime | None | None


class PageBlockHeaderSchema(SchemaBase):
    tl: Literal["PageBlockHeader"] = "PageBlockHeader"
    text: TypeRichTextSchema


class PageBlockSubheaderSchema(SchemaBase):
    tl: Literal["PageBlockSubheader"] = "PageBlockSubheader"
    text: TypeRichTextSchema


class PageBlockParagraphSchema(SchemaBase):
    tl: Literal["PageBlockParagraph"] = "PageBlockParagraph"
    text: TypeRichTextSchema


class PageBlockPreformattedSchema(SchemaBase):
    tl: Literal["PageBlockPreformatted"] = "PageBlockPreformatted"
    text: TypeRichTextSchema
    language: str


class PageBlockFooterSchema(SchemaBase):
    tl: Literal["PageBlockFooter"] = "PageBlockFooter"
    text: TypeRichTextSchema


class PageBlockDividerSchema(SchemaBase):
    tl: Literal["PageBlockDivider"] = "PageBlockDivider"


class PageBlockAnchorSchema(SchemaBase):
    tl: Literal["PageBlockAnchor"] = "PageBlockAnchor"
    name: str


class PageBlockListSchema(SchemaBase):
    tl: Literal["PageBlockList"] = "PageBlockList"
    items: list[TypePageListItemSchema]


class PageBlockBlockquoteSchema(SchemaBase):
    tl: Literal["PageBlockBlockquote"] = "PageBlockBlockquote"
    text: TypeRichTextSchema
    caption: TypeRichTextSchema


class PageBlockPullquoteSchema(SchemaBase):
    tl: Literal["PageBlockPullquote"] = "PageBlockPullquote"
    text: TypeRichTextSchema
    caption: TypeRichTextSchema


class PageBlockPhotoSchema(SchemaBase):
    tl: Literal["PageBlockPhoto"] = "PageBlockPhoto"
    photo_id: int
    caption: PageCaptionSchema
    url: str | None = None
    webpage_id: int | None = None


class PageBlockVideoSchema(SchemaBase):
    tl: Literal["PageBlockVideo"] = "PageBlockVideo"
    video_id: int
    caption: PageCaptionSchema
    autoplay: bool | None = None
    loop: bool | None = None


class PageBlockCoverSchema(SchemaBase):
    tl: Literal["PageBlockCover"] = "PageBlockCover"
    cover: TypePageBlockSchema


class PageBlockEmbedSchema(SchemaBase):
    tl: Literal["PageBlockEmbed"] = "PageBlockEmbed"
    caption: PageCaptionSchema
    full_width: bool | None = None
    allow_scrolling: bool | None = None
    url: str | None = None
    html: str | None = None
    poster_photo_id: int | None = None
    w: int | None = None
    h: int | None = None


class PageBlockEmbedPostSchema(SchemaBase):
    tl: Literal["PageBlockEmbedPost"] = "PageBlockEmbedPost"
    url: str
    webpage_id: int
    author_photo_id: int
    author: str
    date: datetime | None | None
    blocks: list[TypePageBlockSchema]
    caption: PageCaptionSchema


class PageBlockCollageSchema(SchemaBase):
    tl: Literal["PageBlockCollage"] = "PageBlockCollage"
    items: list[TypePageBlockSchema]
    caption: PageCaptionSchema


class PageBlockSlideshowSchema(SchemaBase):
    tl: Literal["PageBlockSlideshow"] = "PageBlockSlideshow"
    items: list[TypePageBlockSchema]
    caption: PageCaptionSchema


class PageBlockChannelSchema(SchemaBase):
    tl: Literal["PageBlockChannel"] = "PageBlockChannel"
    channel: TypeChatSchema


class PageBlockAudioSchema(SchemaBase):
    tl: Literal["PageBlockAudio"] = "PageBlockAudio"
    audio_id: int
    caption: PageCaptionSchema


class PageBlockKickerSchema(SchemaBase):
    tl: Literal["PageBlockKicker"] = "PageBlockKicker"
    text: TypeRichTextSchema


class PageBlockTableSchema(SchemaBase):
    tl: Literal["PageBlockTable"] = "PageBlockTable"
    title: TypeRichTextSchema
    rows: list[PageTableRowSchema]
    bordered: bool | None = None
    striped: bool | None = None


class PageBlockOrderedListSchema(SchemaBase):
    tl: Literal["PageBlockOrderedList"] = "PageBlockOrderedList"
    items: list[TypePageListOrderedItemSchema]


class PageBlockDetailsSchema(SchemaBase):
    tl: Literal["PageBlockDetails"] = "PageBlockDetails"
    blocks: list[TypePageBlockSchema]
    title: TypeRichTextSchema
    open: bool | None = None


class PageBlockRelatedArticlesSchema(SchemaBase):
    tl: Literal["PageBlockRelatedArticles"] = "PageBlockRelatedArticles"
    title: TypeRichTextSchema
    articles: list[PageRelatedArticleSchema]


class PageBlockMapSchema(SchemaBase):
    tl: Literal["PageBlockMap"] = "PageBlockMap"
    geo: TypeGeoPointSchema
    zoom: int
    w: int
    h: int
    caption: PageCaptionSchema


class PageListItemTextSchema(SchemaBase):
    tl: Literal["PageListItemText"] = "PageListItemText"
    text: TypeRichTextSchema


class PageListItemBlocksSchema(SchemaBase):
    tl: Literal["PageListItemBlocks"] = "PageListItemBlocks"
    blocks: list[TypePageBlockSchema]


class PageListOrderedItemTextSchema(SchemaBase):
    tl: Literal["PageListOrderedItemText"] = "PageListOrderedItemText"
    num: str
    text: TypeRichTextSchema


class PageListOrderedItemBlocksSchema(SchemaBase):
    tl: Literal["PageListOrderedItemBlocks"] = "PageListOrderedItemBlocks"
    num: str
    blocks: list[TypePageBlockSchema]


class ChatEmptySchema(SchemaBase):
    tl: Literal["ChatEmpty"] = "ChatEmpty"
    id: int


class ChatSchema(SchemaBase):
    tl: Literal["Chat"] = "Chat"
    id: int
    title: str
    photo: TypeChatPhotoSchema
    participants_count: int
    date: datetime | None | None
    version: int
    creator: bool | None = None
    left: bool | None = None
    deactivated: bool | None = None
    call_active: bool | None = None
    call_not_empty: bool | None = None
    noforwards: bool | None = None
    migrated_to: TypeInputChannelSchema | None = None
    admin_rights: ChatAdminRightsSchema | None = None
    default_banned_rights: ChatBannedRightsSchema | None = None


class ChatForbiddenSchema(SchemaBase):
    tl: Literal["ChatForbidden"] = "ChatForbidden"
    id: int
    title: str


class ChannelSchema(SchemaBase):
    tl: Literal["Channel"] = "Channel"
    id: int
    title: str
    photo: TypeChatPhotoSchema
    date: datetime | None | None
    creator: bool | None = None
    left: bool | None = None
    broadcast: bool | None = None
    verified: bool | None = None
    megagroup: bool | None = None
    restricted: bool | None = None
    signatures: bool | None = None
    min: bool | None = None
    scam: bool | None = None
    has_link: bool | None = None
    has_geo: bool | None = None
    slowmode_enabled: bool | None = None
    call_active: bool | None = None
    call_not_empty: bool | None = None
    fake: bool | None = None
    gigagroup: bool | None = None
    noforwards: bool | None = None
    join_to_send: bool | None = None
    join_request: bool | None = None
    forum: bool | None = None
    stories_hidden: bool | None = None
    stories_hidden_min: bool | None = None
    stories_unavailable: bool | None = None
    signature_profiles: bool | None = None
    autotranslation: bool | None = None
    broadcast_messages_allowed: bool | None = None
    monoforum: bool | None = None
    forum_tabs: bool | None = None
    access_hash: int | None = None
    username: str | None = None
    restriction_reason: list[RestrictionReasonSchema] | None = None
    admin_rights: ChatAdminRightsSchema | None = None
    banned_rights: ChatBannedRightsSchema | None = None
    default_banned_rights: ChatBannedRightsSchema | None = None
    participants_count: int | None = None
    usernames: list[UsernameSchema] | None = None
    stories_max_id: RecentStorySchema | None = None
    color: TypePeerColorSchema | None = None
    profile_color: TypePeerColorSchema | None = None
    emoji_status: TypeEmojiStatusSchema | None = None
    level: int | None = None
    subscription_until_date: datetime | None | None = None
    bot_verification_icon: int | None = None
    send_paid_messages_stars: int | None = None
    linked_monoforum_id: int | None = None


class ChannelForbiddenSchema(SchemaBase):
    tl: Literal["ChannelForbidden"] = "ChannelForbidden"
    id: int
    access_hash: int
    title: str
    broadcast: bool | None = None
    megagroup: bool | None = None
    monoforum: bool | None = None
    until_date: datetime | None | None = None


class StarGiftSchema(SchemaBase):
    tl: Literal["StarGift"] = "StarGift"
    id: int
    sticker: DocumentSchema
    stars: int
    convert_stars: int
    limited: bool | None = None
    sold_out: bool | None = None
    birthday: bool | None = None
    require_premium: bool | None = None
    limited_per_user: bool | None = None
    peer_color_available: bool | None = None
    auction: bool | None = None
    availability_remains: int | None = None
    availability_total: int | None = None
    availability_resale: int | None = None
    first_sale_date: datetime | None | None = None
    last_sale_date: datetime | None | None = None
    upgrade_stars: int | None = None
    resell_min_stars: int | None = None
    title: str | None = None
    released_by: TelegramPeerRefSchema | None = None
    per_user_total: int | None = None
    per_user_remains: int | None = None
    locked_until_date: datetime | None | None = None
    auction_slug: str | None = None
    gifts_per_round: int | None = None
    auction_start_date: datetime | None | None = None
    upgrade_variants: int | None = None
    background: StarGiftBackgroundSchema | None = None


class StarGiftUniqueSchema(SchemaBase):
    tl: Literal["StarGiftUnique"] = "StarGiftUnique"
    id: int
    gift_id: int
    title: str
    slug: str
    num: int
    attributes: list[TypeStarGiftAttributeSchema]
    availability_issued: int
    availability_total: int
    require_premium: bool | None = None
    resale_ton_only: bool | None = None
    theme_available: bool | None = None
    burned: bool | None = None
    crafted: bool | None = None
    owner_id: TelegramPeerRefSchema | None = None
    owner_name: str | None = None
    owner_address: str | None = None
    gift_address: str | None = None
    resell_amount: list[TypeStarsAmountSchema] | None = None
    released_by: TelegramPeerRefSchema | None = None
    value_amount: int | None = None
    value_currency: str | None = None
    value_usd_amount: int | None = None
    theme_peer: TelegramPeerRefSchema | None = None
    peer_color: TypePeerColorSchema | None = None
    host_id: TelegramPeerRefSchema | None = None
    offer_min_stars: int | None = None
    craft_chance_permille: int | None = None


class StarGiftAttributeModelSchema(SchemaBase):
    tl: Literal["StarGiftAttributeModel"] = "StarGiftAttributeModel"
    name: str
    document: DocumentSchema
    rarity: TypeStarGiftAttributeRaritySchema
    crafted: bool | None = None


class StarGiftAttributePatternSchema(SchemaBase):
    tl: Literal["StarGiftAttributePattern"] = "StarGiftAttributePattern"
    name: str
    document: DocumentSchema
    rarity: TypeStarGiftAttributeRaritySchema


class StarGiftAttributeBackdropSchema(SchemaBase):
    tl: Literal["StarGiftAttributeBackdrop"] = "StarGiftAttributeBackdrop"
    name: str
    backdrop_id: int
    center_color: int
    edge_color: int
    pattern_color: int
    text_color: int
    rarity: TypeStarGiftAttributeRaritySchema


class StarGiftAttributeOriginalDetailsSchema(SchemaBase):
    tl: Literal["StarGiftAttributeOriginalDetails"] = "StarGiftAttributeOriginalDetails"
    recipient_id: TelegramPeerRefSchema
    date: datetime | None | None
    sender_id: TelegramPeerRefSchema | None = None
    message: TextWithEntitiesSchema | None = None


class StarGiftAttributeRaritySchema(SchemaBase):
    tl: Literal["StarGiftAttributeRarity"] = "StarGiftAttributeRarity"
    permille: int


class StarGiftAttributeRarityUncommonSchema(SchemaBase):
    tl: Literal["StarGiftAttributeRarityUncommon"] = "StarGiftAttributeRarityUncommon"


class StarGiftAttributeRarityRareSchema(SchemaBase):
    tl: Literal["StarGiftAttributeRarityRare"] = "StarGiftAttributeRarityRare"


class StarGiftAttributeRarityEpicSchema(SchemaBase):
    tl: Literal["StarGiftAttributeRarityEpic"] = "StarGiftAttributeRarityEpic"


class StarGiftAttributeRarityLegendarySchema(SchemaBase):
    tl: Literal["StarGiftAttributeRarityLegendary"] = "StarGiftAttributeRarityLegendary"


class StarGiftAttributeIdModelSchema(SchemaBase):
    tl: Literal["StarGiftAttributeIdModel"] = "StarGiftAttributeIdModel"
    document_id: int


class StarGiftAttributeIdPatternSchema(SchemaBase):
    tl: Literal["StarGiftAttributeIdPattern"] = "StarGiftAttributeIdPattern"
    document_id: int


class StarGiftAttributeIdBackdropSchema(SchemaBase):
    tl: Literal["StarGiftAttributeIdBackdrop"] = "StarGiftAttributeIdBackdrop"
    backdrop_id: int


class StarsAmountSchema(SchemaBase):
    tl: Literal["StarsAmount"] = "StarsAmount"
    amount: int
    nanos: int


class StarsTonAmountSchema(SchemaBase):
    tl: Literal["StarsTonAmount"] = "StarsTonAmount"
    amount: int


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


class InputPeerColorCollectibleSchema(SchemaBase):
    tl: Literal["InputPeerColorCollectible"] = "InputPeerColorCollectible"
    collectible_id: int


class ThemeSettingsSchema(SchemaBase):
    tl: Literal["ThemeSettings"] = "ThemeSettings"
    base_theme: TypeBaseThemeSchema
    accent_color: int
    message_colors_animated: bool | None = None
    outbox_accent_color: int | None = None
    message_colors: list[int] | None = None
    wallpaper: TypeWallPaperSchema | None = None


class PageCaptionSchema(SchemaBase):
    tl: Literal["PageCaption"] = "PageCaption"
    text: TypeRichTextSchema
    credit: TypeRichTextSchema


class PageRelatedArticleSchema(SchemaBase):
    tl: Literal["PageRelatedArticle"] = "PageRelatedArticle"
    url: str
    webpage_id: int
    title: str | None = None
    description: str | None = None
    photo_id: int | None = None
    author: str | None = None
    published_date: datetime | None | None = None


class PageTableCellSchema(SchemaBase):
    tl: Literal["PageTableCell"] = "PageTableCell"
    header: bool | None = None
    align_center: bool | None = None
    align_right: bool | None = None
    valign_middle: bool | None = None
    valign_bottom: bool | None = None
    text: TypeRichTextSchema | None = None
    colspan: int | None = None
    rowspan: int | None = None


class PageTableRowSchema(SchemaBase):
    tl: Literal["PageTableRow"] = "PageTableRow"
    cells: list[PageTableCellSchema]


class RestrictionReasonSchema(SchemaBase):
    tl: Literal["RestrictionReason"] = "RestrictionReason"
    platform: str
    reason: str
    text: str


class UsernameSchema(SchemaBase):
    tl: Literal["Username"] = "Username"
    username: str
    editable: bool | None = None
    active: bool | None = None


class RecentStorySchema(SchemaBase):
    tl: Literal["RecentStory"] = "RecentStory"
    live: bool | None = None
    max_id: int | None = None


class StarGiftBackgroundSchema(SchemaBase):
    tl: Literal["StarGiftBackground"] = "StarGiftBackground"
    center_color: int
    edge_color: int
    text_color: int


class StarGiftAttributeCounterSchema(SchemaBase):
    tl: Literal["StarGiftAttributeCounter"] = "StarGiftAttributeCounter"
    attribute: TypeStarGiftAttributeIdSchema
    count: int


TypeMessageEntitySchema = Annotated[
    MessageEntityUnknownSchema
    | MessageEntityMentionSchema
    | MessageEntityHashtagSchema
    | MessageEntityBotCommandSchema
    | MessageEntityUrlSchema
    | MessageEntityEmailSchema
    | MessageEntityBoldSchema
    | MessageEntityItalicSchema
    | MessageEntityCodeSchema
    | MessageEntityPreSchema
    | MessageEntityTextUrlSchema
    | MessageEntityMentionNameSchema
    | InputMessageEntityMentionNameSchema
    | MessageEntityPhoneSchema
    | MessageEntityCashtagSchema
    | MessageEntityUnderlineSchema
    | MessageEntityStrikeSchema
    | MessageEntityBankCardSchema
    | MessageEntitySpoilerSchema
    | MessageEntityCustomEmojiSchema
    | MessageEntityBlockquoteSchema
    | MessageEntityFormattedDateSchema
    | MessageEntityDiffInsertSchema
    | MessageEntityDiffReplaceSchema
    | MessageEntityDiffDeleteSchema,
    Field(discriminator="tl"),
]

TypeDocumentAttributeSchema = Annotated[
    DocumentAttributeImageSizeSchema
    | DocumentAttributeAnimatedSchema
    | DocumentAttributeStickerSchema
    | DocumentAttributeVideoSchema
    | DocumentAttributeAudioSchema
    | DocumentAttributeFilenameSchema
    | DocumentAttributeHasStickersSchema
    | DocumentAttributeCustomEmojiSchema,
    Field(discriminator="tl"),
]


class DocumentEmptySchema(SchemaBase):
    tl: Literal["DocumentEmpty"] = "DocumentEmpty"
    id: int


class DocumentSchema(SchemaBase):
    """Telethon ``Document`` (``to_dict`` uses ``_``: ``Document``)."""

    tl: Literal["Document"] = "Document"
    id: int
    access_hash: int
    file_reference: bytes
    date: datetime | None
    mime_type: str
    size: int
    dc_id: int
    attributes: list[TypeDocumentAttributeSchema]
    thumbs: list[TypePhotoSizeSchema] | None = None
    video_thumbs: list[TypeVideoSizeSchema] | None = None


TypeDocumentSchema = Annotated[
    DocumentEmptySchema | DocumentSchema,
    Field(discriminator="tl"),
]

TypeWebDocumentSchema = Annotated[
    WebDocumentSchema | WebDocumentNoProxySchema,
    Field(discriminator="tl"),
]

TypeEmojiGameOutcomeSchema = Annotated[
    EmojiGameOutcomeSchema | EmojiGameUnavailableSchema,
    Field(discriminator="tl"),
]

TypeMessageExtendedMediaSchema = Annotated[
    MessageExtendedMediaPreviewSchema | MessageExtendedMediaSchema,
    Field(discriminator="tl"),
]

TypeWebPageSchema = Annotated[
    WebPageEmptySchema | WebPagePendingSchema | WebPageNotModifiedSchema | WebPageSchema,
    Field(discriminator="tl"),
]

TypeWebPageAttributeSchema = Annotated[
    WebPageAttributeThemeSchema
    | WebPageAttributeStorySchema
    | WebPageAttributeStickerSetSchema
    | WebPageAttributeUniqueStarGiftSchema
    | WebPageAttributeStarGiftCollectionSchema
    | WebPageAttributeStarGiftAuctionSchema,
    Field(discriminator="tl"),
]

TypeInputGroupCallSchema = Annotated[
    InputGroupCallSchema | InputGroupCallSlugSchema | InputGroupCallInviteMessageSchema,
    Field(discriminator="tl"),
]

TypeStoryItemSchema = Annotated[
    StoryItemDeletedSchema | StoryItemSkippedSchema | StoryItemSchema,
    Field(discriminator="tl"),
]

TypeMediaAreaSchema = Annotated[
    MediaAreaVenueSchema
    | MediaAreaGeoPointSchema
    | MediaAreaSuggestedReactionSchema
    | MediaAreaChannelPostSchema
    | MediaAreaUrlSchema
    | MediaAreaWeatherSchema
    | MediaAreaStarGiftSchema,
    Field(discriminator="tl"),
]

TypePrivacyRuleSchema = Annotated[
    PrivacyValueAllowContactsSchema
    | PrivacyValueAllowAllSchema
    | PrivacyValueAllowUsersSchema
    | PrivacyValueDisallowContactsSchema
    | PrivacyValueDisallowAllSchema
    | PrivacyValueDisallowUsersSchema
    | PrivacyValueAllowChatParticipantsSchema
    | PrivacyValueDisallowChatParticipantsSchema
    | PrivacyValueAllowCloseFriendsSchema
    | PrivacyValueAllowPremiumSchema
    | PrivacyValueAllowBotsSchema
    | PrivacyValueDisallowBotsSchema,
    Field(discriminator="tl"),
]

TypeInputStickerSetSchema = Annotated[
    InputStickerSetEmptySchema
    | InputStickerSetIDSchema
    | InputStickerSetShortNameSchema
    | InputStickerSetAnimatedEmojiSchema
    | InputStickerSetDiceSchema
    | InputStickerSetAnimatedEmojiAnimationsSchema
    | InputStickerSetPremiumGiftsSchema
    | InputStickerSetEmojiGenericAnimationsSchema
    | InputStickerSetEmojiDefaultStatusesSchema
    | InputStickerSetEmojiDefaultTopicIconsSchema
    | InputStickerSetEmojiChannelDefaultStatusesSchema
    | InputStickerSetTonGiftsSchema,
    Field(discriminator="tl"),
]

TypeInputChannelSchema = Annotated[
    InputChannelEmptySchema | InputChannelSchema | InputChannelFromMessageSchema,
    Field(discriminator="tl"),
]

TypeBaseThemeSchema = Annotated[
    BaseThemeClassicSchema | BaseThemeDaySchema | BaseThemeNightSchema | BaseThemeTintedSchema | BaseThemeArcticSchema,
    Field(discriminator="tl"),
]

TypeRichTextSchema = Annotated[
    TextEmptySchema
    | TextPlainSchema
    | TextBoldSchema
    | TextItalicSchema
    | TextUnderlineSchema
    | TextStrikeSchema
    | TextFixedSchema
    | TextUrlSchema
    | TextEmailSchema
    | TextConcatSchema
    | TextSubscriptSchema
    | TextSuperscriptSchema
    | TextMarkedSchema
    | TextPhoneSchema
    | TextImageSchema
    | TextAnchorSchema,
    Field(discriminator="tl"),
]

TypePageBlockSchema = Annotated[
    PageBlockUnsupportedSchema
    | PageBlockTitleSchema
    | PageBlockSubtitleSchema
    | PageBlockAuthorDateSchema
    | PageBlockHeaderSchema
    | PageBlockSubheaderSchema
    | PageBlockParagraphSchema
    | PageBlockPreformattedSchema
    | PageBlockFooterSchema
    | PageBlockDividerSchema
    | PageBlockAnchorSchema
    | PageBlockListSchema
    | PageBlockBlockquoteSchema
    | PageBlockPullquoteSchema
    | PageBlockPhotoSchema
    | PageBlockVideoSchema
    | PageBlockCoverSchema
    | PageBlockEmbedSchema
    | PageBlockEmbedPostSchema
    | PageBlockCollageSchema
    | PageBlockSlideshowSchema
    | PageBlockChannelSchema
    | PageBlockAudioSchema
    | PageBlockKickerSchema
    | PageBlockTableSchema
    | PageBlockOrderedListSchema
    | PageBlockDetailsSchema
    | PageBlockRelatedArticlesSchema
    | PageBlockMapSchema,
    Field(discriminator="tl"),
]

TypePageListItemSchema = Annotated[
    PageListItemTextSchema | PageListItemBlocksSchema,
    Field(discriminator="tl"),
]

TypePageListOrderedItemSchema = Annotated[
    PageListOrderedItemTextSchema | PageListOrderedItemBlocksSchema,
    Field(discriminator="tl"),
]

TypeChatSchema = Annotated[
    ChatEmptySchema | ChatSchema | ChatForbiddenSchema | ChannelSchema | ChannelForbiddenSchema,
    Field(discriminator="tl"),
]

TypeStarGiftSchema = Annotated[
    StarGiftSchema | StarGiftUniqueSchema,
    Field(discriminator="tl"),
]

TypeStarGiftAttributeSchema = Annotated[
    StarGiftAttributeModelSchema
    | StarGiftAttributePatternSchema
    | StarGiftAttributeBackdropSchema
    | StarGiftAttributeOriginalDetailsSchema,
    Field(discriminator="tl"),
]

TypeStarGiftAttributeRaritySchema = Annotated[
    StarGiftAttributeRaritySchema
    | StarGiftAttributeRarityUncommonSchema
    | StarGiftAttributeRarityRareSchema
    | StarGiftAttributeRarityEpicSchema
    | StarGiftAttributeRarityLegendarySchema,
    Field(discriminator="tl"),
]

TypeStarGiftAttributeIdSchema = Annotated[
    StarGiftAttributeIdModelSchema | StarGiftAttributeIdPatternSchema | StarGiftAttributeIdBackdropSchema,
    Field(discriminator="tl"),
]

TypeStarsAmountSchema = Annotated[
    StarsAmountSchema | StarsTonAmountSchema,
    Field(discriminator="tl"),
]

TypePeerColorSchema = Annotated[
    PeerColorSchema | PeerColorCollectibleSchema | InputPeerColorCollectibleSchema,
    Field(discriminator="tl"),
]


# ---------------------------------------------------------------------------
# Message media (TypeMessageMedia)
# ---------------------------------------------------------------------------


class HarvestMediaStateSchema(SchemaBase):
    """State for downloadable & embeddable message media ."""

    s3_uri: str | None = None
    image_embedding_id: UUID | None = None
    image_embedded: dict[str, bool] | None = None
    phash: str | None = None


class MessageMediaEmptySchema(SchemaBase):
    tl: Literal["MessageMediaEmpty"] = "MessageMediaEmpty"


class MessageMediaPhotoSchema(SchemaBase):
    tl: Literal["MessageMediaPhoto"] = "MessageMediaPhoto"
    spoiler: bool | None = None
    live_photo: bool | None = None
    photo: TypePhotoSchema | None = None
    ttl_seconds: int | None = None
    video: TypeDocumentSchema | None = None
    harvest: HarvestMediaStateSchema | None = None


class MessageMediaGeoSchema(SchemaBase):
    tl: Literal["MessageMediaGeo"] = "MessageMediaGeo"
    geo: TypeGeoPointSchema


class MessageMediaContactSchema(SchemaBase):
    tl: Literal["MessageMediaContact"] = "MessageMediaContact"
    phone_number: str
    first_name: str
    last_name: str
    vcard: str
    user_id: int


class MessageMediaUnsupportedSchema(SchemaBase):
    tl: Literal["MessageMediaUnsupported"] = "MessageMediaUnsupported"


class MessageMediaDocumentSchema(SchemaBase):
    tl: Literal["MessageMediaDocument"] = "MessageMediaDocument"
    nopremium: bool | None = None
    spoiler: bool | None = None
    video: bool | None = None
    round: bool | None = None
    voice: bool | None = None
    document: TypeDocumentSchema | None = None
    alt_documents: list[TypeDocumentSchema] | None = None
    video_cover: TypePhotoSchema | None = None
    video_timestamp: int | None = None
    ttl_seconds: int | None = None
    harvester: HarvestMediaStateSchema | None = None


class MessageMediaWebPageSchema(SchemaBase):
    tl: Literal["MessageMediaWebPage"] = "MessageMediaWebPage"
    webpage: TypeWebPageSchema
    force_large_media: bool | None = None
    force_small_media: bool | None = None
    manual: bool | None = None
    safe: bool | None = None
    harvest: HarvestMediaStateSchema | None = None


class MessageMediaVenueSchema(SchemaBase):
    tl: Literal["MessageMediaVenue"] = "MessageMediaVenue"
    geo: TypeGeoPointSchema
    title: str
    address: str
    provider: str
    venue_id: str
    venue_type: str


class MessageMediaGameSchema(SchemaBase):
    tl: Literal["MessageMediaGame"] = "MessageMediaGame"
    game: GameSchema


class MessageMediaInvoiceSchema(SchemaBase):
    tl: Literal["MessageMediaInvoice"] = "MessageMediaInvoice"
    title: str
    description: str
    currency: str
    total_amount: int
    start_param: str
    shipping_address_requested: bool | None = None
    test: bool | None = None
    photo: TypeWebDocumentSchema | None = None
    receipt_msg_id: int | None = None
    extended_media: TypeMessageExtendedMediaSchema | None = None


class MessageMediaGeoLiveSchema(SchemaBase):
    tl: Literal["MessageMediaGeoLive"] = "MessageMediaGeoLive"
    geo: TypeGeoPointSchema
    period: int
    heading: int | None = None
    proximity_notification_radius: int | None = None


class MessageMediaPollSchema(SchemaBase):
    tl: Literal["MessageMediaPoll"] = "MessageMediaPoll"
    poll: PollSchema
    results: PollResultsSchema
    attached_media: TypeMessageMediaSchema | None = None


class MessageMediaDiceSchema(SchemaBase):
    tl: Literal["MessageMediaDice"] = "MessageMediaDice"
    value: int
    emoticon: str
    game_outcome: TypeEmojiGameOutcomeSchema | None = None


class MessageMediaStorySchema(SchemaBase):
    tl: Literal["MessageMediaStory"] = "MessageMediaStory"
    peer: TelegramPeerRefSchema
    id: int
    via_mention: bool | None = None
    story: TypeStoryItemSchema | None = None


class MessageMediaGiveawaySchema(SchemaBase):
    tl: Literal["MessageMediaGiveaway"] = "MessageMediaGiveaway"
    channels: list[int]
    quantity: int
    until_date: datetime | None | None
    only_new_subscribers: bool | None = None
    winners_are_visible: bool | None = None
    countries_iso2: list[str] | None = None
    prize_description: str | None = None
    months: int | None = None
    stars: int | None = None


class MessageMediaGiveawayResultsSchema(SchemaBase):
    tl: Literal["MessageMediaGiveawayResults"] = "MessageMediaGiveawayResults"
    channel_id: int
    launch_msg_id: int
    winners_count: int
    unclaimed_count: int
    winners: list[int]
    until_date: datetime | None | None
    only_new_subscribers: bool | None = None
    refunded: bool | None = None
    additional_peers_count: int | None = None
    months: int | None = None
    stars: int | None = None
    prize_description: str | None = None


class MessageMediaPaidMediaSchema(SchemaBase):
    tl: Literal["MessageMediaPaidMedia"] = "MessageMediaPaidMedia"
    stars_amount: int
    extended_media: list[TypeMessageExtendedMediaSchema]


class MessageMediaToDoSchema(SchemaBase):
    tl: Literal["MessageMediaToDo"] = "MessageMediaToDo"
    todo: TodoListSchema
    completions: list[TodoCompletionSchema] | None = None


class MessageMediaVideoStreamSchema(SchemaBase):
    tl: Literal["MessageMediaVideoStream"] = "MessageMediaVideoStream"
    call: TypeInputGroupCallSchema
    rtmp_stream: bool | None = None


TypeMessageMediaSchema = Annotated[
    MessageMediaEmptySchema
    | MessageMediaPhotoSchema
    | MessageMediaGeoSchema
    | MessageMediaContactSchema
    | MessageMediaUnsupportedSchema
    | MessageMediaDocumentSchema
    | MessageMediaWebPageSchema
    | MessageMediaVenueSchema
    | MessageMediaGameSchema
    | MessageMediaInvoiceSchema
    | MessageMediaGeoLiveSchema
    | MessageMediaPollSchema
    | MessageMediaDiceSchema
    | MessageMediaStorySchema
    | MessageMediaGiveawaySchema
    | MessageMediaGiveawayResultsSchema
    | MessageMediaPaidMediaSchema
    | MessageMediaToDoSchema
    | MessageMediaVideoStreamSchema,
    Field(discriminator="tl"),
]

PollAnswerSchema.model_rebuild()
PollResultsSchema.model_rebuild()
MessageExtendedMediaSchema.model_rebuild()
StoryItemSchema.model_rebuild()
PageSchema.model_rebuild()
WebPageSchema.model_rebuild()
TextWithEntitiesSchema.model_rebuild()
PollSchema.model_rebuild()
TodoListSchema.model_rebuild()
PageCaptionSchema.model_rebuild()
ThemeSettingsSchema.model_rebuild()
TextConcatSchema.model_rebuild()
PageBlockCollageSchema.model_rebuild()
PageBlockDetailsSchema.model_rebuild()
PageBlockListSchema.model_rebuild()
PageBlockOrderedListSchema.model_rebuild()
PageBlockRelatedArticlesSchema.model_rebuild()
PageBlockSlideshowSchema.model_rebuild()
StarGiftSchema.model_rebuild()
StarGiftUniqueSchema.model_rebuild()
StarGiftAttributeBackdropSchema.model_rebuild()
StarGiftAttributeModelSchema.model_rebuild()
StarGiftAttributePatternSchema.model_rebuild()
StarGiftAttributeCounterSchema.model_rebuild()
ChatSchema.model_rebuild()
ChannelSchema.model_rebuild()
MessageMediaPollSchema.model_rebuild()
MessageMediaInvoiceSchema.model_rebuild()
MessageMediaPaidMediaSchema.model_rebuild()
PollAnswerSchema.model_rebuild()
