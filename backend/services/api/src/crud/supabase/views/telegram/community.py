from typing import Literal
from datetime import datetime
from pydantic import Field
from db.schemas.telegram.channel import TelegramChannelSchema
from db.schemas.telegram.chat import TelegramChatSchema
from services.api.src.crud.supabase.views.general.category import CategorySimpleView
from services.api.src.crud.supabase.views.general.community import CommunityView


class TelegramChannelView(TelegramChannelSchema):
    peer_type: Literal["channel"] = "channel"
    categories: list[CategorySimpleView] = Field(default_factory=list)
    stripped_profile_photo: str | None = None
    profile_photo_media_kind: Literal["image", "video"] | None = None
    #: From ``telegram_harvester_peer_roles`` (e.g. CHANNEL_PRIVATE); not a peer-table column.
    access_error_code: str | None = None
    access_checked_at: datetime | None = None


class TelegramMegagroupView(TelegramChannelSchema):
    peer_type: Literal["megagroup"] = "megagroup"
    stripped_profile_photo: str | None = None
    categories: list[CategorySimpleView] = Field(default_factory=list)
    profile_photo_media_kind: Literal["image", "video"] | None = None
    access_error_code: str | None = None
    access_checked_at: datetime | None = None


class TelegramChatView(TelegramChatSchema):
    peer_type: Literal["chat"] = "chat"
    stripped_profile_photo: str | None = None
    categories: list[CategorySimpleView] = Field(default_factory=list)
    profile_photo_media_kind: Literal["image", "video"] | None = None
    access_error_code: str | None = None
    access_checked_at: datetime | None = None


class TelegramChannelLinkView(TelegramChannelView):
    linked_peer: CommunityView | None = None


class TelegramMegagroupLinkView(TelegramMegagroupView):
    linked_peer: CommunityView | None = None
