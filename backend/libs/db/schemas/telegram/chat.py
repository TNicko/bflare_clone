from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from telegram.slim_profile_photo import slim_profile_photo_from_full
from telegram.utils import normalize_tl_keys

from .slim_profile_photo import SlimTypeProfilePhotoSchema
from .tl import ChatBannedRightsSchema, TypeChatReactionsSchema


class ChatMigratedToSchema(BaseModel):
    channel_id: int
    access_hash: int | None = None


class TelegramChatSchema(BaseModel):
    model_config = ConfigDict(ser_json_bytes="base64")

    @model_validator(mode="before")
    @classmethod
    def _normalize_tl_discriminators(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return normalize_tl_keys(data)
        return data

    @field_validator("photo", mode="before")
    @classmethod
    def _slim_profile_photo(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        return slim_profile_photo_from_full(v) or v

    id: UUID | None = None

    profile_photo_s3_uri: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    unavailable_at: datetime | None = None
    last_participant_fetch_at: datetime | None = None
    harvester_id: UUID

    # --- Telegram Data ---

    # identity
    external_id: str  # telegram's signed peer id
    title: str  # display name

    # photos
    photo: SlimTypeProfilePhotoSchema | None = None

    participants_count: int | None = None  # current member count
    date: datetime | None = None  # chat creation date
    version: int | None = None  # participant list version (increments on changes)
    deactivated: bool | None = None  # whether the group was migrated
    noforwards: bool | None = None  # whether message forwarding is allowed
    migrated_to: ChatMigratedToSchema | None = None  # if upgraded to supergroup
    default_banned_rights: ChatBannedRightsSchema | None = None

    # full chat info (from telethon.tl.types.ChatFull)
    about: str | None = None  # about string for this chat
    has_scheduled: bool | None = None  # whether scheduled messages are available
    translations_disabled: bool | None = None  # whether the real-time chat translation popup should be hidden.
    requests_pending: int | None = None  # no. of pending join requests
    pinned_msg_id: int | None = None  # currently pinned message id
    first_message_at: datetime | None = None  # oldest accessible message date (not profile upsert)
    available_reactions: TypeChatReactionsSchema | None = None  # allowed message reactions
    reactions_limit: int | None = None  # limit of unique reactions
