"""Slim persisted shapes for peer profile photos (JSONB on peers + field history)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from db.schemas.telegram.slim_media import SlimHarvestMediaStateSchema
from pydantic import BaseModel, ConfigDict, Field


class SlimProfilePhotoBase(BaseModel):
    model_config = ConfigDict(extra="forbid", ser_json_bytes="base64")


class SlimPhotoEmptySchema(SlimProfilePhotoBase):
    tl: Literal["PhotoEmpty"] = "PhotoEmpty"
    id: int


class SlimPhotoSchema(SlimProfilePhotoBase):
    tl: Literal["Photo"] = "Photo"
    id: int
    date: datetime | None = None
    w: int | None = None
    h: int | None = None
    stripped_thumb: str | bytes | None = None
    has_video: bool | None = None
    harvest: SlimHarvestMediaStateSchema | None = None


class SlimChatPhotoEmptySchema(SlimProfilePhotoBase):
    tl: Literal["ChatPhotoEmpty"] = "ChatPhotoEmpty"


class SlimChatPhotoSchema(SlimProfilePhotoBase):
    tl: Literal["ChatPhoto"] = "ChatPhoto"
    photo_id: int
    stripped_thumb: str | bytes | None = None
    has_video: bool | None = None
    harvest: SlimHarvestMediaStateSchema | None = None


class SlimUserProfilePhotoSchema(SlimProfilePhotoBase):
    tl: Literal["UserProfilePhoto"] = "UserProfilePhoto"
    photo_id: int
    stripped_thumb: str | bytes | None = None
    has_video: bool | None = None
    harvest: SlimHarvestMediaStateSchema | None = None


SlimTypeProfilePhotoSchema = Annotated[
    SlimPhotoEmptySchema
    | SlimPhotoSchema
    | SlimChatPhotoEmptySchema
    | SlimChatPhotoSchema
    | SlimUserProfilePhotoSchema,
    Field(discriminator="tl"),
]
