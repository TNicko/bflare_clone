from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from services.api.src.entitlements import assert_pin_kind_allowed

from services.api.src.auth.deps import get_current_user
from services.api.src.auth.models import PlatformUser
from services.api.src.config import ConfigFactory
from services.api.src.crud.s3.media import (
    presign_model_profile_photo,
    presign_project_message_mapping,
    presign_project_view,
    presign_project_views,
    presign_user_partial,
)
from services.api.src.crud.supabase import get_conn
from services.api.src.crud.supabase.peers import (
    get_partial_telegram_user_by_uuid,
    get_telegram_channel_by_uuid,
    get_telegram_chat_by_uuid,
    get_telegram_megagroup_by_uuid,
)
from services.api.src.crud.supabase.projects import (
    create_project,
    create_project_alert,
    create_project_report,
    create_project_telegram_channel_mapping,
    create_project_telegram_chat_mapping,
    create_project_telegram_message_mapping,
    create_project_telegram_megagroup_mapping,
    create_project_telegram_user_mapping,
    delete_project,
    delete_project_telegram_channel_mapping,
    delete_project_telegram_chat_mapping,
    delete_project_telegram_message_mapping,
    delete_project_telegram_megagroup_mapping,
    delete_project_telegram_user_mapping,
    project_belongs_to_user,
    read_project,
    read_projects,
    update_project_description,
    update_project_name,
)
from services.api.src.models.project import (
    AddProjectAlertPayload,
    AddProjectAlertResponse,
    AddProjectPayload,
    AddProjectReportPayload,
    AddProjectReportResponse,
    AddProjectResponse,
    AddTelegramChannelPayload,
    AddTelegramChannelResponse,
    AddTelegramChatPayload,
    AddTelegramChatResponse,
    AddTelegramMegagroupPayload,
    AddTelegramMegagroupResponse,
    AddTelegramMessagePayload,
    AddTelegramUserPayload,
    AddTelegramUserResponse,
    DeleteTelegramChannelPayload,
    DeleteTelegramChatPayload,
    DeleteTelegramMegagroupPayload,
    DeleteTelegramMessagePayload,
    DeleteTelegramUserPayload,
    UpdateProjectDescriptionPayload,
    UpdateProjectDescriptionResponse,
    UpdateProjectNamePayload,
    UpdateProjectNameResponse,
)
from services.api.src.crud.supabase.views.general.project import (
    ProjectMessageMappingView,
    ProjectView,
)
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelView,
    TelegramChatView,
    TelegramMegagroupView,
)

router = APIRouter(tags=["project"])


def _aws_region() -> str:
    return ConfigFactory.get_config().aws_region.strip()


async def _require_project_access(conn, *, project_id: str, user_id: UUID) -> None:
    if not await project_belongs_to_user(conn, project_id=project_id, user_id=user_id):
        raise HTTPException(status_code=404, detail="Project not found")


@router.post("/", response_model=AddProjectResponse)
async def add_project(
    payload: AddProjectPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddProjectResponse:
    async with get_conn() as conn:
        project_id = await create_project(
            conn,
            user_id=user.id,
            name=payload.name,
            description=payload.description,
        )
    if project_id is None:
        raise HTTPException(status_code=500, detail="Failed to create project")
    return AddProjectResponse(id=project_id, name=payload.name, description=payload.description)


@router.post("/telegram/channel", response_model=AddTelegramChannelResponse)
async def add_telegram_channel(
    payload: AddTelegramChannelPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddTelegramChannelResponse:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        await assert_pin_kind_allowed(conn, user_id=user.id, kind="communities")
        mapping_id = await create_project_telegram_channel_mapping(
            conn,
            project=payload.project,
            channel=payload.channel,
            user_id=user.id,
        )
        channel_row = await get_telegram_channel_by_uuid(conn, payload.channel)
    if mapping_id is None:
        raise HTTPException(status_code=500, detail="Failed to create channel mapping")
    if channel_row is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    channel = await presign_model_profile_photo(
        TelegramChannelView.model_validate(channel_row),
        region=_aws_region(),
    )
    return AddTelegramChannelResponse(
        id=mapping_id,
        project=payload.project,
        channel=channel,
    )


@router.post("/telegram/megagroup", response_model=AddTelegramMegagroupResponse)
async def add_telegram_megagroup(
    payload: AddTelegramMegagroupPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddTelegramMegagroupResponse:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        await assert_pin_kind_allowed(conn, user_id=user.id, kind="communities")
        mapping_id = await create_project_telegram_megagroup_mapping(
            conn,
            project=payload.project,
            megagroup=payload.megagroup,
            user_id=user.id,
        )
        megagroup_row = await get_telegram_megagroup_by_uuid(conn, payload.megagroup)
    if mapping_id is None:
        raise HTTPException(status_code=500, detail="Failed to create megagroup mapping")
    if megagroup_row is None:
        raise HTTPException(status_code=404, detail="Megagroup not found")
    megagroup = await presign_model_profile_photo(
        TelegramMegagroupView.model_validate(megagroup_row),
        region=_aws_region(),
    )
    return AddTelegramMegagroupResponse(
        id=mapping_id,
        project=payload.project,
        megagroup=megagroup,
    )


@router.post("/telegram/chat", response_model=AddTelegramChatResponse)
async def add_telegram_chat(
    payload: AddTelegramChatPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddTelegramChatResponse:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        await assert_pin_kind_allowed(conn, user_id=user.id, kind="communities")
        mapping_id = await create_project_telegram_chat_mapping(
            conn,
            project=payload.project,
            chat=payload.chat,
            user_id=user.id,
        )
        chat_row = await get_telegram_chat_by_uuid(conn, payload.chat)
    if mapping_id is None:
        raise HTTPException(status_code=500, detail="Failed to create chat mapping")
    if chat_row is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    chat = await presign_model_profile_photo(
        TelegramChatView.model_validate(chat_row),
        region=_aws_region(),
    )
    return AddTelegramChatResponse(
        id=mapping_id,
        project=payload.project,
        chat=chat,
    )


@router.post("/telegram/user", response_model=AddTelegramUserResponse)
async def add_telegram_user(
    payload: AddTelegramUserPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddTelegramUserResponse:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        await assert_pin_kind_allowed(conn, user_id=user.id, kind="users")
        mapping_id = await create_project_telegram_user_mapping(
            conn,
            project=payload.project,
            user=payload.user,
            user_id=user.id,
        )
        user_row = await get_partial_telegram_user_by_uuid(conn, payload.user)
    if mapping_id is None:
        raise HTTPException(status_code=500, detail="Failed to create user mapping")
    if user_row is None:
        raise HTTPException(status_code=404, detail="User not found")
    pinned_user = await presign_user_partial(user_row, region=_aws_region())
    return AddTelegramUserResponse(id=mapping_id, project=payload.project, user=pinned_user)


@router.post("/telegram/message", response_model=ProjectMessageMappingView)
async def add_telegram_message(
    payload: AddTelegramMessagePayload,
    user: PlatformUser = Depends(get_current_user),
) -> ProjectMessageMappingView:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        await assert_pin_kind_allowed(conn, user_id=user.id, kind="messages")
        mapping = await create_project_telegram_message_mapping(
            conn,
            project=payload.project,
            message=payload.message,
            user_id=user.id,
        )
    if mapping is None:
        raise HTTPException(status_code=500, detail="Failed to create message mapping")
    return await presign_project_message_mapping(mapping, region=_aws_region())


@router.post("/alert", response_model=AddProjectAlertResponse)
async def add_alert(
    payload: AddProjectAlertPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddProjectAlertResponse:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        alert_id = await create_project_alert(
            conn,
            project=payload.project,
            alert_name=payload.alert_name,
            alert_type=payload.alert_type,
            alert_phrase=payload.alert_phrase,
            alert_user=payload.alert_user,
            user_id=user.id,
        )
    if alert_id is None:
        raise HTTPException(status_code=500, detail="Failed to create alert")
    return AddProjectAlertResponse(
        id=alert_id,
        project=payload.project,
        alert_name=payload.alert_name,
        alert_type=payload.alert_type,
        alert_phrase=payload.alert_phrase,
        alert_user=payload.alert_user,
    )


@router.post("/report", response_model=AddProjectReportResponse)
async def add_report(
    payload: AddProjectReportPayload,
    user: PlatformUser = Depends(get_current_user),
) -> AddProjectReportResponse:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        report_id = await create_project_report(
            conn,
            project=payload.project,
            report_name=payload.report_name,
            report_location=payload.report_location,
            user_id=user.id,
        )
    if report_id is None:
        raise HTTPException(status_code=500, detail="Failed to create report")
    return AddProjectReportResponse(
        id=report_id,
        project=payload.project,
        report_name=payload.report_name,
        report_location=payload.report_location,
    )


@router.get("/", response_model=list[ProjectView])
async def get_projects(user: PlatformUser = Depends(get_current_user)) -> list[ProjectView]:
    async with get_conn() as conn:
        projects = await read_projects(conn, user_id=user.id)
    return await presign_project_views(projects, region=_aws_region())


@router.get("/{project_id}", response_model=ProjectView)
async def get_project(
    project_id: UUID,
    user: PlatformUser = Depends(get_current_user),
) -> ProjectView:
    async with get_conn() as conn:
        project = await read_project(
            conn,
            project_id=str(project_id),
            user_id=user.id,
        )
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return await presign_project_view(project, region=_aws_region())


@router.delete("/{project_id}", status_code=204)
async def remove_project(
    project_id: UUID,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        deleted = await delete_project(
            conn,
            project_id=str(project_id),
            user_id=user.id,
        )
    if not deleted:
        raise HTTPException(status_code=404, detail="Project not found")


@router.patch("/description", response_model=UpdateProjectDescriptionResponse)
async def update_description(
    payload: UpdateProjectDescriptionPayload,
    user: PlatformUser = Depends(get_current_user),
) -> UpdateProjectDescriptionResponse:
    async with get_conn() as conn:
        updated = await update_project_description(
            conn,
            project=payload.project,
            description=payload.description,
            user_id=user.id,
        )
    if not updated:
        raise HTTPException(status_code=404, detail="Project not found")
    return UpdateProjectDescriptionResponse(
        project=payload.project,
        description=payload.description,
    )


@router.patch("/name", response_model=UpdateProjectNameResponse)
async def update_name(
    payload: UpdateProjectNamePayload,
    user: PlatformUser = Depends(get_current_user),
) -> UpdateProjectNameResponse:
    async with get_conn() as conn:
        updated = await update_project_name(
            conn,
            project=payload.project,
            name=payload.name,
            user_id=user.id,
        )
    if not updated:
        raise HTTPException(status_code=404, detail="Project not found")
    return UpdateProjectNameResponse(project=payload.project, name=payload.name)


@router.delete("/telegram/channel", status_code=204)
async def remove_telegram_channel(
    payload: DeleteTelegramChannelPayload,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        deleted = await delete_project_telegram_channel_mapping(
            conn,
            project=payload.project,
            channel=payload.channel,
            user_id=user.id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Channel mapping not found")


@router.delete("/telegram/megagroup", status_code=204)
async def remove_telegram_megagroup(
    payload: DeleteTelegramMegagroupPayload,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        deleted = await delete_project_telegram_megagroup_mapping(
            conn,
            project=payload.project,
            megagroup=payload.megagroup,
            user_id=user.id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Megagroup mapping not found")


@router.delete("/telegram/chat", status_code=204)
async def remove_telegram_chat(
    payload: DeleteTelegramChatPayload,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        deleted = await delete_project_telegram_chat_mapping(
            conn,
            project=payload.project,
            chat=payload.chat,
            user_id=user.id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Chat mapping not found")


@router.delete("/telegram/user", status_code=204)
async def remove_telegram_user(
    payload: DeleteTelegramUserPayload,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        deleted = await delete_project_telegram_user_mapping(
            conn,
            project=payload.project,
            user=payload.user,
            user_id=user.id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="User mapping not found")


@router.delete("/telegram/message", status_code=204)
async def remove_telegram_message(
    payload: DeleteTelegramMessagePayload,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        await _require_project_access(conn, project_id=payload.project, user_id=user.id)
        deleted = await delete_project_telegram_message_mapping(
            conn,
            project=payload.project,
            message=payload.message,
            user_id=user.id,
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Message mapping not found")
