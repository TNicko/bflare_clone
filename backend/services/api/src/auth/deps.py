from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException, Request

from services.api.src.auth.models import PlatformUser


def get_optional_user(request: Request) -> PlatformUser | None:
    return getattr(request.state, "platform_user", None)


def get_current_user(request: Request) -> PlatformUser:
    user = get_optional_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def current_user_id(request: Request) -> UUID:
    return get_current_user(request).id
