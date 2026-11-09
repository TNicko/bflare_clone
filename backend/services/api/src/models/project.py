from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelView,
    TelegramChatView,
    TelegramMegagroupView,
)
from services.api.src.crud.supabase.views.telegram.user import UserPartialView


class AddProjectPayload(BaseModel):
    name: str
    description: str | None = None


class AddProjectResponse(BaseModel):
    id: UUID
    name: str
    description: str | None = None


class AddTelegramChannelPayload(BaseModel):
    project: str
    channel: str


class AddTelegramChannelResponse(BaseModel):
    id: UUID
    project: str
    channel: TelegramChannelView


class AddTelegramMegagroupPayload(BaseModel):
    project: str
    megagroup: str


class AddTelegramMegagroupResponse(BaseModel):
    id: UUID
    project: str
    megagroup: TelegramMegagroupView


class AddTelegramChatPayload(BaseModel):
    project: str
    chat: str


class AddTelegramChatResponse(BaseModel):
    id: UUID
    project: str
    chat: TelegramChatView


class AddTelegramUserPayload(BaseModel):
    project: str
    user: str


class AddTelegramUserResponse(BaseModel):
    id: UUID
    project: str
    user: UserPartialView


class AddTelegramMessagePayload(BaseModel):
    project: str
    message: str


class AddProjectAlertPayload(BaseModel):
    project: str
    alert_name: str
    alert_type: str
    alert_phrase: str
    alert_user: str


class AddProjectAlertResponse(BaseModel):
    id: UUID
    project: str
    alert_name: str
    alert_type: str
    alert_phrase: str
    alert_user: str


class AddProjectReportPayload(BaseModel):
    project: str
    report_name: str
    report_location: str


class AddProjectReportResponse(BaseModel):
    id: UUID
    project: str
    report_name: str
    report_location: str


class UpdateProjectDescriptionPayload(BaseModel):
    project: str
    description: str


class UpdateProjectDescriptionResponse(BaseModel):
    project: str
    description: str


class UpdateProjectNamePayload(BaseModel):
    project: str
    name: str


class UpdateProjectNameResponse(BaseModel):
    project: str
    name: str


class DeleteTelegramChannelPayload(BaseModel):
    project: str
    channel: str


class DeleteTelegramMegagroupPayload(BaseModel):
    project: str
    megagroup: str


class DeleteTelegramChatPayload(BaseModel):
    project: str
    chat: str


class DeleteTelegramUserPayload(BaseModel):
    project: str
    user: str


class DeleteTelegramMessagePayload(BaseModel):
    project: str
    message: str
