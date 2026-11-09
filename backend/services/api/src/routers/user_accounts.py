"""User-owned messenger account HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from services.api.src.auth.deps import get_current_user
from services.api.src.auth.models import PlatformUser
from services.api.src.crud.supabase import get_conn
from services.api.src.crud.supabase.user_accounts import (
    list_accounts_for_user,
    list_sessions_for_account,
)
from services.api.src.crud.supabase.views.user.messenger_account import (
    UserMessengerAccountView,
    UserMessengerSessionView,
)
from services.api.src.routers import user_telegram_session_ws as user_telegram_session_ws_router
from services.api.src.routers.api_keys import router as api_keys_router

router = APIRouter(tags=["user-accounts"])
router.include_router(user_telegram_session_ws_router.router)
router.include_router(api_keys_router, prefix="/api-keys")


@router.get("/accounts", response_model=list[UserMessengerAccountView])
async def list_user_accounts(
    user: PlatformUser = Depends(get_current_user),
) -> list[UserMessengerAccountView]:
    async with get_conn() as conn:
        account_rows = await list_accounts_for_user(conn, user.id)
        out: list[UserMessengerAccountView] = []
        for row in account_rows:
            sessions = await list_sessions_for_account(conn, row["id"])
            out.append(
                UserMessengerAccountView.from_row(
                    row,
                    sessions=[UserMessengerSessionView.from_row(s) for s in sessions],
                )
            )
    return out
