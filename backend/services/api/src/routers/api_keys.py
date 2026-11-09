"""Create, list, and revoke API keys. JWT only."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from db.crud.user.api_access import (
    MAX_ACTIVE_API_KEYS,
    count_active_api_keys,
    generate_api_key,
    insert_api_key,
    list_active_api_keys,
    revoke_api_key,
)
from services.api.src.auth.deps import get_current_user
from services.api.src.auth.models import PlatformUser
from services.api.src.crud.supabase import get_conn

router = APIRouter(tags=["api-keys"])


class ApiKeyOut(BaseModel):
    id: UUID
    name: str
    key_prefix: str
    key_last4: str
    created_at: datetime
    last_used_at: datetime | None = None


class ApiKeyCreatedOut(ApiKeyOut):
    secret: str


class ApiKeyCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)


@router.get("", response_model=list[ApiKeyOut])
async def list_api_keys(user: PlatformUser = Depends(get_current_user)) -> list[ApiKeyOut]:
    async with get_conn() as conn:
        rows = await list_active_api_keys(conn, user.id)
    return [ApiKeyOut.model_validate(row) for row in rows]


@router.post("", response_model=ApiKeyCreatedOut, status_code=201)
async def create_api_key(
    body: ApiKeyCreateIn,
    user: PlatformUser = Depends(get_current_user),
) -> ApiKeyCreatedOut:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail={"code": "invalid_name", "detail": "Name is required"})
    secret, prefix, last4, key_hash = generate_api_key()
    async with get_conn() as conn:
        async with conn.transaction():
            active = await count_active_api_keys(conn, user.id)
            if active >= MAX_ACTIVE_API_KEYS:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "api_key_limit",
                        "detail": f"You can have {MAX_ACTIVE_API_KEYS} active API keys",
                    },
                )
            row = await insert_api_key(
                conn,
                user_id=user.id,
                name=name,
                key_prefix=prefix,
                key_last4=last4,
                key_hash=key_hash,
            )
    return ApiKeyCreatedOut(secret=secret, **row)


@router.delete("/{key_id}", status_code=204)
async def delete_api_key(
    key_id: UUID,
    user: PlatformUser = Depends(get_current_user),
) -> None:
    async with get_conn() as conn:
        revoked = await revoke_api_key(conn, user_id=user.id, key_id=key_id)
    if not revoked:
        raise HTTPException(status_code=404, detail={"code": "api_key_not_found", "detail": "API key not found"})
