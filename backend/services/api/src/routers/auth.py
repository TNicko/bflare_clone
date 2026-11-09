from __future__ import annotations

import logging
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from psycopg import sql
from psycopg.rows import dict_row

from services.api.src.auth.deps import get_current_user
from services.api.src.auth.models import PlatformUser
from services.api.src.auth.password_status import user_requires_password_setup
from services.api.src.crud.supabase import get_conn

router = APIRouter(tags=["auth"])
lg = logging.getLogger(__name__)

ThemePreference = Literal["system", "light", "dark", "classic"]
SidebarMode = Literal["expanded", "collapsed", "expand-on-hover"]
_ALLOWED_THEMES = frozenset({"system", "light", "dark", "classic"})
_ALLOWED_SIDEBAR_MODES = frozenset({"expanded", "collapsed", "expand-on-hover"})


class ProfileView(BaseModel):
    id: UUID
    email: str | None = None
    display_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    theme: ThemePreference | None = None
    sidebar_mode: SidebarMode | None = None
    requires_password_setup: bool = False


class UpdateProfilePayload(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    display_name: str | None = None
    theme: ThemePreference | None = None
    sidebar_mode: SidebarMode | None = None

    @field_validator("theme")
    @classmethod
    def validate_theme(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in _ALLOWED_THEMES:
            raise ValueError(f"Invalid theme: {value}")
        return value

    @field_validator("sidebar_mode")
    @classmethod
    def validate_sidebar_mode(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in _ALLOWED_SIDEBAR_MODES:
            raise ValueError(f"Invalid sidebar mode: {value}")
        return value


_ALLOWED_PROFILE_COLUMNS = frozenset(UpdateProfilePayload.model_fields)


def _build_profile_update_query(columns: list[str]) -> sql.Composed:
    set_clause = sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder()) for column in columns
    )
    return sql.SQL(
        """
        UPDATE profiles
        SET {set_clause}, updated_at = NOW()
        WHERE id = {user_id}
        RETURNING display_name, first_name, last_name, theme, sidebar_mode
        """
    ).format(set_clause=set_clause, user_id=sql.Placeholder())


def _profile_view(
    user: PlatformUser,
    row: dict | None,
    *,
    requires_password_setup: bool = False,
) -> ProfileView:
    theme = row.get("theme") if row else None
    if theme not in _ALLOWED_THEMES:
        theme = None
    sidebar_mode = row.get("sidebar_mode") if row else None
    if sidebar_mode not in _ALLOWED_SIDEBAR_MODES:
        sidebar_mode = None
    return ProfileView(
        id=user.id,
        email=user.email,
        display_name=row.get("display_name") if row else None,
        first_name=row.get("first_name") if row else None,
        last_name=row.get("last_name") if row else None,
        theme=theme,
        sidebar_mode=sidebar_mode,
        requires_password_setup=requires_password_setup,
    )


async def _fetch_profile_row(conn, user_id: UUID) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT display_name, first_name, last_name, theme, sidebar_mode
            FROM profiles
            WHERE id = %s
            """,
            (user_id,),
        )
        return await cur.fetchone()


@router.get("/me", response_model=ProfileView)
async def get_me(user: PlatformUser = Depends(get_current_user)) -> ProfileView:
    async with get_conn() as conn:
        row = await _fetch_profile_row(conn, user.id)
        needs_setup = await user_requires_password_setup(conn, user.id)

    return _profile_view(user, row, requires_password_setup=needs_setup)


@router.patch("/me", response_model=ProfileView)
async def update_me(
    payload: UpdateProfilePayload,
    user: PlatformUser = Depends(get_current_user),
) -> ProfileView:
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        async with get_conn() as conn:
            row = await _fetch_profile_row(conn, user.id)
            needs_setup = await user_requires_password_setup(conn, user.id)
        return _profile_view(user, row, requires_password_setup=needs_setup)

    invalid_columns = set(updates) - _ALLOWED_PROFILE_COLUMNS
    if invalid_columns:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid profile fields: {', '.join(sorted(invalid_columns))}",
        )

    columns = list(updates)
    values = [updates[column] for column in columns] + [user.id]
    query = _build_profile_update_query(columns)

    try:
        async with get_conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, values)
                row = await cur.fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Profile not found")
                needs_setup = await user_requires_password_setup(conn, user.id)
            await conn.commit()
    except HTTPException:
        raise
    except Exception:
        lg.exception("Failed to update profile for user %s", user.id)
        raise

    return _profile_view(user, row, requires_password_setup=needs_setup)


@router.delete("/me", status_code=204)
async def delete_me(user: PlatformUser = Depends(get_current_user)) -> None:
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM auth.users WHERE id = %s", (user.id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="User not found")
        await conn.commit()
