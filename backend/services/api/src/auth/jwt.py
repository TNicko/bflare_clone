from __future__ import annotations

from uuid import UUID

import jwt
from jwt import PyJWKClient
from jwt.exceptions import InvalidTokenError, PyJWKClientError

from services.api.src.config import ConfigFactory

# Supabase asymmetric signing keys (ECC P-256 / RSA). JWKS is cached ~10 min per Supabase edge guidance.
_JWKS_CLIENTS: dict[str, PyJWKClient] = {}
_JWKS_CACHE_SECONDS = 600
_JWT_ALGORITHMS = ("ES256", "RS256")


class AuthError(Exception):
    pass


def _supabase_jwks_client(supabase_url: str) -> PyJWKClient:
    base = supabase_url.rstrip("/")
    jwks_url = f"{base}/auth/v1/.well-known/jwks.json"
    client = _JWKS_CLIENTS.get(jwks_url)
    if client is None:
        client = PyJWKClient(
            jwks_url,
            cache_keys=True,
            lifespan=_JWKS_CACHE_SECONDS,
        )
        _JWKS_CLIENTS[jwks_url] = client
    return client


def verify_supabase_access_token(token: str) -> tuple[UUID, str | None]:
    """Verify a Supabase user JWT via JWKS and return (user_id, email)."""
    cfg = ConfigFactory.get_config()
    supabase_url = cfg.supabase_url.strip()
    if not supabase_url:
        raise AuthError("SUPABASE_URL is not configured")

    issuer = f"{supabase_url.rstrip('/')}/auth/v1"

    try:
        jwks_client = _supabase_jwks_client(supabase_url)
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=list(_JWT_ALGORITHMS),
            audience="authenticated",
            issuer=issuer,
        )
    except PyJWKClientError as exc:
        raise AuthError("Unable to load Supabase signing keys") from exc
    except InvalidTokenError as exc:
        raise AuthError("Invalid or expired token") from exc

    sub = payload.get("sub")
    if not sub:
        raise AuthError("Token missing sub claim")

    email = payload.get("email")
    if email is not None and not isinstance(email, str):
        email = None

    return UUID(str(sub)), email
