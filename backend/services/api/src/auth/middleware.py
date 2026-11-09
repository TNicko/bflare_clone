from __future__ import annotations

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocket

from db.crud.user.api_access import is_api_key_token, resolve_api_key_user
from services.api.src.auth.api_route import is_hidden_public_route, requires_api_key
from services.api.src.auth.channel import current_usage_channel
from services.api.src.auth.jwt import AuthError, verify_supabase_access_token
from services.api.src.auth.models import PlatformUser
from services.api.src.auth.password_status import user_requires_password_setup
from services.api.src.crud.supabase import get_conn
from utils.user_log import reset_log_user, set_log_user

# Health probes only — all other routes require a valid Supabase JWT.
PUBLIC_PATHS = frozenset({"/health", "/health/telegram", "/metrics", "/billing/stripe/webhook"})

# Invite/recovery sessions may exist before a password is chosen. Allow profile
# reads so the client can detect incomplete setup; block everything else.
_PASSWORD_SETUP_EXEMPT_GET_PATHS = frozenset({"/auth/me"})


def _bearer_token_from_headers(headers: list[tuple[bytes, bytes]]) -> str | None:
    for key, value in headers:
        if key.lower() == b"authorization":
            text = value.decode("latin-1")
            if text.startswith("Bearer "):
                token = text[7:].strip()
                return token or None
            return None
    return None


def _bearer_token(request: Request) -> str | None:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    token = header[7:].strip()
    return token or None


def _resolve_user_from_token(token: str | None) -> PlatformUser | None:
    if not token:
        return None
    try:
        user_id, email = verify_supabase_access_token(token)
    except (AuthError, ValueError):
        return None
    return PlatformUser(id=user_id, email=email)


def _resolve_user(request: Request) -> PlatformUser | None:
    return _resolve_user_from_token(_bearer_token(request))


def _password_setup_exempt(method: str, path: str) -> bool:
    return method == "GET" and path in _PASSWORD_SETUP_EXEMPT_GET_PATHS


class PlatformAuthMiddleware:
    """Pure ASGI auth middleware so request ContextVars (log user) survive into handlers."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path") or ""
        method = scope.get("method") or "GET"
        log_token = set_log_user(None)  # system until JWT resolves
        try:
            if method == "OPTIONS" or path in PUBLIC_PATHS:
                await self.app(scope, receive, send)
                return

            if is_hidden_public_route(path):
                response = JSONResponse(status_code=404, content={"detail": "Not found"})
                await response(scope, receive, send)
                return

            token = _bearer_token_from_headers(scope.get("headers") or [])
            usage_channel = "app"
            if requires_api_key(path):
                usage_channel = current_usage_channel("api")
                user = None
                if is_api_key_token(token):
                    async with get_conn() as conn:
                        resolved = await resolve_api_key_user(conn, token or "")
                    if resolved is not None:
                        user_id, email = resolved
                        user = PlatformUser(id=user_id, email=email)
            else:
                user = _resolve_user_from_token(token)
            if user is None:
                response = JSONResponse(status_code=401, content={"detail": "Not authenticated"})
                await response(scope, receive, send)
                return

            reset_log_user(log_token)
            log_token = set_log_user(user.email)

            if not _password_setup_exempt(method, path):
                async with get_conn() as conn:
                    needs_setup = await user_requires_password_setup(conn, user.id)
                if needs_setup:
                    response = JSONResponse(
                        status_code=403,
                        content={
                            "detail": "Password setup required",
                            "code": "password_setup_required",
                        },
                    )
                    await response(scope, receive, send)
                    return

            state = scope.setdefault("state", {})
            state["platform_user"] = user
            state["usage_channel"] = usage_channel
            await self.app(scope, receive, send)
        finally:
            reset_log_user(log_token)


def verify_ws_token(token: str | None) -> PlatformUser | None:
    return _resolve_user_from_token(token)


async def require_ws_user(websocket: WebSocket) -> PlatformUser | None:
    token = websocket.query_params.get("access_token")
    user = verify_ws_token(token)
    if user is None:
        await websocket.close(code=4401, reason="Not authenticated")
        return None

    async with get_conn() as conn:
        needs_setup = await user_requires_password_setup(conn, user.id)
    if needs_setup:
        await websocket.close(code=4403, reason="Password setup required")
        return None

    set_log_user(user.email)
    return user
