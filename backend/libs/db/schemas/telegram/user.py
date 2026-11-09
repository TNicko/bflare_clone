from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from telegram.utils import normalize_tl_keys


class TelegramUserSchema(BaseModel):
    id: UUID | None = None
    unavailable_at: datetime | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True, ser_json_bytes="base64")

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

    @field_validator("blocked_my_stories_from", mode="before")
    @classmethod
    def _blocked_my_stories_from_coerce(cls, v: Any) -> Any:
        # Telethon UserFull exposes this as a bool flag; ignore other TL shapes.
        if v is None or isinstance(v, bool):
            return v
        return None

    external_id: str
    access_hash: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    phone: str | None = None
    photo: dict[str, Any] | list[Any] | None = None
    profile_photo_s3_uri: str | None = None
    bot: bool = False
    verified: bool = False
    restricted: bool = False
    scam: bool = False
    fake: bool = False
    premium: bool = False
    restriction_reason: dict[str, Any] | list[Any] | None = None
    lang_code: str | None = None
    emoji_status: dict[str, Any] | None = None
    settings: dict[str, Any] | None = None
    notify_settings: dict[str, Any] | None = None
    common_chats_count: int | None = None
    blocked: bool | None = None
    phone_calls_available: bool | None = None
    phone_calls_private: bool | None = None
    can_pin_message: bool | None = None
    has_scheduled: bool | None = None
    video_calls_available: bool | None = None
    voice_messages_forbidden: bool | None = None
    translations_disabled: bool | None = None
    stories_pinned_available: bool | None = None
    blocked_my_stories_from: bool | None = None
    wallpaper_overridden: bool | None = None
    contact_require_premium: bool | None = None
    read_dates_private: bool | None = None
    sponsored_enabled: bool | None = None
    can_view_revenue: bool | None = None
    bot_can_manage_emoji_status: bool | None = None
    display_gifts_button: bool | None = None
    about: str | None = None
    personal_photo: dict[str, Any] | None = None
    profile_photo: dict[str, Any] | None = None
    fallback_photo: dict[str, Any] | None = None
    bot_info: dict[str, Any] | list[Any] | None = None
    pinned_msg_id: int | None = None
    folder_id: int | None = None
    ttl_period: int | None = None
    theme: dict[str, Any] | None = None
    private_forward_name: str | None = None
    bot_group_admin_rights: dict[str, Any] | None = None
    bot_broadcast_admin_rights: dict[str, Any] | None = None
    wallpaper: dict[str, Any] | None = None
    stories: dict[str, Any] | list[Any] | None = None
    business_work_hours: dict[str, Any] | None = None
    business_location: dict[str, Any] | None = None
    business_greeting_message: str | None = None
    business_away_message: str | None = None
    business_intro: dict[str, Any] | None = None
    birthday: dict[str, Any] | None = None
    personal_channel_id: int | None = None
    personal_channel_message: str | None = None
    stargifts_count: int | None = None
    starref_program: dict[str, Any] | None = None
    bot_verification: dict[str, Any] | None = None
    send_paid_messages_stars: int | None = None
    disallowed_gifts: dict[str, Any] | list[Any] | None = None
    stars_rating: dict[str, Any] | None = None
    stars_my_pending_rating: dict[str, Any] | None = None
    stars_my_pending_rating_date: int | None = None
    main_tab: dict[str, Any] | None = None
    saved_music: dict[str, Any] | list[Any] | None = None
    note: str | None = None

    pinned_stories: dict[str, Any] | None = None
    pinned_stories_fetched_at: datetime | None = None
    saved_star_gifts: dict[str, Any] | None = None
    star_gifts_fetched_at: datetime | None = None

    deleted: bool = False
    deleted_observed_at: datetime | None = None

    created_at: datetime | None = None
    updated_at: datetime | None = None
