from __future__ import annotations
from typing import Any
from telethon import utils
import base64
from datetime import UTC, datetime

# Telegram encoding: signed peer_id for channel/megagroup = -(1000000000 + raw_channel_id)
CHANNEL_MARK = 1_000_000_000_000


def to_signed_peer_id(peer: Any) -> int | None:
    """
    Return signed peer ID for storage in Postgres/S3 (same format as messenger and bot API).
    Accepts Peer, Entity (User/Chat/Channel), or int. If peer is a plain int (e.g. resolved
    sender_id), it is returned as-is—correct for users; if it is a channel id, from_id would
    be unsigned, which is incorrect.
    """
    if peer is None:
        return None
    return utils.get_peer_id(peer)


def channel_id_from_signed_peer_id(signed_peer_id: int) -> int | None:
    """
    Return the raw Telegram channel ID from a signed peer ID (e.g. for InputChannel).
    """
    if signed_peer_id >= 0:
        return None
    marked = -signed_peer_id
    if marked <= CHANNEL_MARK:
        return None
    return marked - CHANNEL_MARK


def signed_peer_id_from_raw_channel_id(raw_channel_id: int) -> int:
    """Return signed channel/megagroup peer ID from raw Telegram channel ID."""
    return -(CHANNEL_MARK + raw_channel_id)


def normalize_tl_keys(value: Any) -> Any:
    """Recursively copy ``value``, turning each dict's ``_`` key into ``tl`` when ``tl`` is absent.

    Does not mutate the input. Nested dicts and list elements are processed.
    If both ``_`` and ``tl`` are present, ``tl`` is kept and ``_`` is dropped.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            if key == "_":
                continue
            out[key] = normalize_tl_keys(child)
        if "_" in value and "tl" not in out:
            out["tl"] = value["_"]
        return out
    if isinstance(value, list):
        return [normalize_tl_keys(item) for item in value]
    if isinstance(value, tuple):
        return tuple(normalize_tl_keys(item) for item in value)
    return value


def json_safe_value(value: Any) -> Any:
    """Recursively coerce Telethon ``to_dict()`` values into Pydantic ``JsonValue``-safe data."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        dt = value.replace(tzinfo=UTC) if value.tzinfo is None else value
        return dt.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {k: json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe_value(v) for v in value]
    return str(value)


def phone_digits(raw: str) -> str:
    """Return digits-only phone (Telegram ``User.phone`` storage format)."""
    return "".join(c for c in raw if c.isdigit())


def normalize_phone(raw: str) -> str | None:
    """Normalize user input to E.164-style ``+{digits}`` for Telegram ``contacts.resolvePhone``."""
    stripped = raw.strip()
    if not stripped:
        return None
    digits = phone_digits(stripped)
    if not digits:
        return None
    return f"+{digits}"


def normalize_username(raw: str) -> str | None:
    """
    Normalize a Telegram @username for lookup / resolve.

    Accepts an optional leading ``@``. Returns lowercase handle when it matches
    Telegram's public username shape (5–32 chars, starts with a letter, then
    letters / digits / underscore); otherwise ``None``.
    """
    handle = raw.strip().lstrip("@").strip()
    if not handle:
        return None
    if len(handle) < 5 or len(handle) > 32:
        return None
    if not handle[0].isalpha():
        return None
    for ch in handle:
        if not (ch.isalnum() or ch == "_"):
            return None
    return handle.lower()


def peer_ref_from_telethon(peer: Any) -> dict[str, Any] | None:
    """Map Telethon ``TypePeer`` to ``{id, peer_type}`` for :class:`~db.schemas.telegram.tl.TelegramPeerRefSchema`."""
    if peer is None:
        return None
    from telethon.tl import types

    if isinstance(peer, types.PeerUser):
        return {"id": peer.user_id, "peer_type": "user"}
    if isinstance(peer, types.PeerChat):
        return {"id": peer.chat_id, "peer_type": "chat"}
    if isinstance(peer, types.PeerChannel):
        return {"id": peer.channel_id, "peer_type": "channel"}
    if isinstance(peer, dict):
        from db.schemas.telegram.tl import TelegramPeerRefSchema

        return TelegramPeerRefSchema.model_validate(normalize_tl_keys(peer)).model_dump()
    return None


def from_telethon(obj: Any) -> Any:
    """Normalize a Telethon TLObject or an already-built dict from ``to_dict()``."""
    if hasattr(obj, "to_dict"):
        raw = obj.to_dict()
    else:
        raw = obj
    return normalize_tl_keys(raw)
