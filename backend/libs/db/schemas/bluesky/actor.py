from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

BlueskyActorSourceSchema = Literal["appview", "firehose", "pds", "manual"]
BlueskyHostingStatusSchema = Literal[
    "takendown",
    "suspended",
    "deleted",
    "deactivated",
    "desynchronized",
    "throttled",
]


# App View placeholder when a DID has no resolvable handle (not unique per actor).
INVALID_HANDLE = "handle.invalid"


def normalize_handle(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lstrip("@").lower()
    return cleaned or None


def is_handle_valid(handle: str | None) -> bool:
    """True when the handle is a real AT Proto handle (not missing / placeholder)."""
    return bool(handle) and handle != INVALID_HANDLE


def did_method_from_did(did: str) -> str | None:
    parts = did.split(":", 2)
    if len(parts) < 2 or parts[0] != "did":
        return None
    method = parts[1].strip().lower()
    return method or None


class BlueskyActorSchema(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: UUID | None = None
    did: str
    handle: str | None = None
    handle_valid: bool = True
    display_name: str | None = None
    description: str | None = None
    pronouns: str | None = None
    website: str | None = None
    avatar: dict[str, Any] | None = None
    banner: dict[str, Any] | None = None
    avatar_url: str | None = None
    banner_url: str | None = None
    avatar_s3_uri: str | None = None
    banner_s3_uri: str | None = None
    labels: list[Any] | dict[str, Any] | None = None
    pinned_post: dict[str, Any] | None = None
    joined_via_starter_pack: dict[str, Any] | None = None
    profile_created_at: datetime | None = None
    followers_count: int | None = None
    follows_count: int | None = None
    posts_count: int | None = None
    indexed_at: datetime | None = None
    repo_rev: str | None = None
    pds_host: str | None = None
    did_method: str | None = None
    active: bool = True
    hosting_status: BlueskyHostingStatusSchema | None = None
    hosting_status_observed_at: datetime | None = None
    unavailable_at: datetime | None = None
    display_name_search: str | None = None
    handle_search: str | None = None
    profile_cid: str | None = None
    source: BlueskyActorSourceSchema = "appview"
    last_synced_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("did")
    @classmethod
    def _validate_did(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned.startswith("did:"):
            raise ValueError("did must start with 'did:'")
        return cleaned

    @field_validator("handle", mode="before")
    @classmethod
    def _normalize_handle(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        return normalize_handle(value)

    @model_validator(mode="before")
    @classmethod
    def _default_did_method(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        did = data.get("did")
        if data.get("did_method") or not isinstance(did, str):
            return data
        method = did_method_from_did(did)
        if method:
            data = {**data, "did_method": method}
        return data

    @model_validator(mode="after")
    def _sync_handle_valid(self) -> BlueskyActorSchema:
        # Many DIDs share App View's handle.invalid; only real handles are unique.
        valid = is_handle_valid(self.handle)
        if self.handle_valid != valid:
            self.handle_valid = valid
        return self
