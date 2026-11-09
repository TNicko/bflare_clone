from __future__ import annotations

from typing import Any, Iterable, Literal
import ast
from datetime import UTC, datetime
from telethon import utils
import base64
from pydantic import TypeAdapter, ValidationError
from telethon.tl.custom import Message as TelethonMessage
from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.chat import TelegramChatSchema
from db.schemas.telegram.harvester import TelegramHarvesterPeerPermissionsSchema
from db.schemas.telegram.message import TelegramMessageSchema
from db.schemas.telegram.participant import TelegramChatParticipantSchema
from db.schemas.telegram.tl import (
    ChatAdminRightsSchema,
    ChatBannedRightsSchema,
    ChatPhotoSchema,
    PhotoSchema,
    TypePhotoSchema,
)
from telethon.tl.types import InputChannel, InputChannelFromMessage, User, UserFull as TLUserFull
from telethon.tl.types.users import UserFull as GetFullUserResult
from uuid import UUID

from telethon.tl.types.messages import ChatFull

from db.schemas.telegram.channel import TelegramChannelSchema
from db.schemas.telegram.tl_media import TypeMessageMediaSchema
from db.schemas.telegram.user import TelegramUserSchema
from telegram.slim_message_media import slim_message_media_from_full
from telegram.slim_profile_photo import slim_profile_photo_from_full
from db.schemas.telegram.slim_reactions import SlimMessageReactionsSchema
from utils.coverage_intervals import ensure_utc
from .message_entity_flags import message_entity_flags
from .utils import from_telethon, json_safe_value, normalize_tl_keys, to_signed_peer_id
from .slim_reactions import slim_reactions_from_full
from .slim_reply_to import slim_reply_to_from_full


def _slim_peer_photo(photo: Any) -> dict[str, Any] | None:
    """Normalize Telethon / DB photo JSON to the slim persisted shape."""
    if photo is None:
        return None
    if not isinstance(photo, dict):
        return None
    return slim_profile_photo_from_full(photo)


def _first(iterable: Iterable[Any], pred) -> Any | None:
    for x in iterable:
        if pred(x):
            return x
    return None


def channel_schema_from_telethon_full_channel(
    result: ChatFull,
    *,
    harvester_id: UUID,
    signed_peer_id: int,
    profile_photo_s3_uri: str | None = None,
) -> tuple[TelegramChannelSchema, int]:
    """Build a :class:`~schema.channel.TelegramChannelSchema` from Telethon GetFullChannelRequest result.

    Telethon typically returns ``telethon.tl.types.messages.ChatFull`` with:
    - ``full_chat``: ``ChannelFull`` (for channels/supergroups) or ``ChatFull`` (for basic groups)
    - ``chats``: list of ``Channel``/``Chat`` objects (base peer objects)

    This mapper:
    - Normalizes TL discriminators (``_`` -> ``tl``) for nested TL blobs
    - Merges data from the base ``Channel`` object and ``ChannelFull``
    - Produces the exact fields expected by :class:`~schema.channel.TelegramChannelSchema`

    Notes:
    - ``external_id`` is required explicitly because it is a derived value (e.g. ``-100...``)
      and depends on how you compute/store peer ids.
    """
    # Extract the two key pieces from Telethon's wrapper.
    full_chat = result.full_chat
    chats = list(result.chats)

    # Find the matching base peer object by id.
    full_id = full_chat.id
    base = _first(chats, lambda c: c.id == full_id)

    access_hash = base.access_hash

    raw_full = from_telethon(full_chat)
    raw_base = from_telethon(base)

    # Prefer Channel.photo (ChatPhoto) when available; fall back to ChannelFull.chat_photo (Photo).
    photo = raw_full.get("chat_photo")
    if photo is None:
        photo = raw_base.get("photo")

    payload = {
        "profile_photo_s3_uri": profile_photo_s3_uri,
        "harvester_id": harvester_id,
        "external_id": str(signed_peer_id),
        "title": raw_base["title"],
        "username": raw_base.get("username"),
        "usernames": raw_base.get("usernames"),
        "photo": _slim_peer_photo(photo),
        # Flags from base Channel (best source).
        "is_broadcast": bool(raw_base["broadcast"]),
        "is_megagroup": bool(raw_base["megagroup"]),
        "is_gigagroup": bool(raw_base["gigagroup"]),
        "is_monoforum": bool(raw_base["monoforum"]),
        "is_forum": bool(raw_base["forum"]),
        "is_verified": bool(raw_base["verified"]),
        "is_scam": bool(raw_base["scam"]),
        "is_fake": bool(raw_base["fake"]),
        "is_restricted": bool(raw_base["restricted"]),
        "profile_tier": "tg_full",
        # Stories fields: from base Channel when present (Telethon varies by layer).
        "stories_unavailable": bool(raw_base["stories_unavailable"]),
        "stories_max_id": _stories_max_id_as_int(raw_base.get("stories_max_id")),
        "stories_pinned_available": raw_full.get("stories_pinned_available"),
        # Counts: ChannelFull is usually the source.
        "participants_count": raw_full.get("participants_count"),
        "online_count": raw_full.get("online_count"),
        "admins_count": raw_full.get("admins_count"),
        "kicked_count": raw_full.get("kicked_count"),
        "banned_count": raw_full.get("banned_count"),
        # Settings/flags: split between base and full.
        "has_link": bool(raw_base["has_link"]),
        "has_geo": bool(raw_base["has_geo"]),
        "slowmode_enabled": bool(raw_base["slowmode_enabled"]),
        "slowmode_seconds": raw_full.get("slowmode_seconds"),
        "noforwards": bool(raw_base["noforwards"]),
        "join_to_send": bool(raw_base["join_to_send"]),
        "join_request": bool(raw_base["join_request"]),
        "signatures": bool(raw_base["signatures"]),
        "signature_profiles": bool(raw_base["signature_profiles"]),
        "antispam": raw_full.get("antispam"),
        "participants_hidden": raw_full.get("participants_hidden"),
        "translations_disabled": raw_full.get("translations_disabled"),
        "hidden_prehistory": raw_full.get("hidden_prehistory"),
        "autotranslation": bool(raw_base["autotranslation"]),
        "broadcast_messages_allowed": bool(raw_base["broadcast_messages_allowed"]),
        # Rights & links.
        "default_banned_rights": raw_base.get("default_banned_rights"),
        "linked_chat_id": raw_full.get("linked_chat_id"),
        "linked_monoforum_id": raw_base.get("linked_monoforum_id"),
        "migrated_from_chat_id": raw_full.get("migrated_from_chat_id"),
        "migrated_from_max_id": raw_full.get("migrated_from_max_id"),
        # Info fields.
        "about": raw_full.get("about"),
        "date": getattr(base, "date", None),
        "location": raw_full.get("location"),
        "pinned_msg_id": raw_full.get("pinned_msg_id"),
        "available_min_id": raw_full.get("available_min_id"),
        "stats_dc": raw_full.get("stats_dc"),
        "ttl_period": raw_full.get("ttl_period"),
        "requests_pending": raw_full.get("requests_pending"),
        # Reactions.
        "available_reactions": raw_full.get("available_reactions"),
        "reactions_limit": raw_full.get("reactions_limit"),
        # Paid/stars.
        "paid_media_allowed": raw_full.get("paid_media_allowed"),
        "paid_reactions_available": raw_full.get("paid_reactions_available"),
        "paid_messages_available": raw_full.get("paid_messages_available"),
        "send_paid_messages_stars": raw_full.get("send_paid_messages_stars"),
        "stargifts_available": raw_full.get("stargifts_available"),
        "stargifts_count": raw_full.get("stargifts_count"),
        # Boosts.
        "boosts_applied": raw_full.get("boosts_applied"),
        "boosts_unrestrict": raw_full.get("boosts_unrestrict"),
        # Emojis/stickers/bots.
        "stickerset": raw_full.get("stickerset"),
        "emojiset": raw_full.get("emojiset"),
        "bot_info": raw_full.get("bot_info") or [],
        "bot_verification": raw_full.get("bot_verification"),
        # Restriction.
        "restriction_reason": raw_base.get("restriction_reason"),
        "restricted_sponsored": raw_full.get("restricted_sponsored"),
        # Forums & appearance.
        "view_forum_as_messages": raw_full.get("view_forum_as_messages"),
        "color": raw_base.get("color"),
        "profile_color": raw_base.get("profile_color"),
        "emoji_status": raw_base.get("emoji_status"),
        "level": raw_base.get("level"),
        "bot_verification_icon": raw_base.get("bot_verification_icon"),
        "theme_emoticon": raw_full.get("theme_emoticon"),
        "wallpaper": raw_full.get("wallpaper"),
    }

    return TelegramChannelSchema.model_validate(payload), access_hash


def channel_schema_from_telethon_channel(
    entity: Any,
    *,
    harvester_id: UUID | None = None,
    signed_peer_id: int | None = None,
) -> tuple[TelegramChannelSchema, int | None]:
    """Build a ``tg_min`` channel row from a base ``Channel`` / ``ChannelForbidden``.

    Used when ``channels.getFullChannel`` is unavailable (private / banned) but
    Telegram already sent title/photo in a sibling ``chats[]`` list.
    """
    peer_id = signed_peer_id if signed_peer_id is not None else to_signed_peer_id(entity)
    if peer_id is None:
        raise ValueError("Cannot derive signed peer id for channel entity")

    raw = from_telethon(entity)
    title = raw.get("title")
    if not isinstance(title, str) or not title.strip():
        title = f"channel {getattr(entity, 'id', peer_id)}"

    payload = {
        "harvester_id": harvester_id,
        "external_id": str(peer_id),
        "title": title.strip(),
        "username": raw.get("username"),
        "usernames": raw.get("usernames"),
        "photo": _slim_peer_photo(raw.get("photo")),
        "is_broadcast": bool(raw["broadcast"]) if "broadcast" in raw else None,
        "is_megagroup": bool(raw["megagroup"]) if "megagroup" in raw else None,
        "is_gigagroup": bool(raw["gigagroup"]) if "gigagroup" in raw else None,
        "is_monoforum": bool(raw["monoforum"]) if "monoforum" in raw else None,
        "is_forum": bool(raw["forum"]) if "forum" in raw else None,
        "is_verified": bool(raw["verified"]) if "verified" in raw else None,
        "is_scam": bool(raw["scam"]) if "scam" in raw else None,
        "is_fake": bool(raw["fake"]) if "fake" in raw else None,
        "is_restricted": bool(raw["restricted"]) if "restricted" in raw else None,
        "profile_tier": "tg_min",
        "date": raw.get("date"),
        "restriction_reason": raw.get("restriction_reason"),
        "color": raw.get("color"),
        "profile_color": raw.get("profile_color"),
        "emoji_status": raw.get("emoji_status"),
        "noforwards": bool(raw["noforwards"]) if "noforwards" in raw else None,
        "has_link": bool(raw["has_link"]) if "has_link" in raw else None,
        "has_geo": bool(raw["has_geo"]) if "has_geo" in raw else None,
        "slowmode_enabled": bool(raw["slowmode_enabled"]) if "slowmode_enabled" in raw else None,
        "join_to_send": bool(raw["join_to_send"]) if "join_to_send" in raw else None,
        "join_request": bool(raw["join_request"]) if "join_request" in raw else None,
        "signatures": bool(raw["signatures"]) if "signatures" in raw else None,
        "default_banned_rights": raw.get("default_banned_rights"),
        "linked_monoforum_id": raw.get("linked_monoforum_id"),
        "stories_unavailable": (bool(raw["stories_unavailable"]) if "stories_unavailable" in raw else None),
        "stories_max_id": _stories_max_id_as_int(raw.get("stories_max_id")),
    }
    access_hash = raw.get("access_hash")
    return TelegramChannelSchema.model_validate(payload), (int(access_hash) if access_hash is not None else None)


def chat_schema_from_telethon_full_chat(
    result: ChatFull,
    *,
    harvester_id: UUID,
    signed_peer_id: int,
    profile_photo_s3_uri: str | None = None,
) -> TelegramChatSchema:
    """Build a :class:`~schema.chat.TelegramChatSchema` from Telethon GetFullChatRequest result.

    Telethon typically returns ``telethon.tl.types.messages.ChatFull`` with:
    - ``full_chat``: ``telethon.tl.types.ChatFull`` (for basic groups)
    - ``chats``: list of ``telethon.tl.types.Chat`` objects (base peer objects)

    This mapper:
    - Normalizes TL discriminators (``_`` -> ``tl``) for nested TL blobs
    - Merges data from the base ``Chat`` object and ``ChatFull``
    - Excludes account-dependent fields (e.g. `left`, `creator`, `admin_rights`, `notify_settings`)
    """
    full_chat = result.full_chat
    chats = list(result.chats)

    full_id = full_chat.id
    base = _first(chats, lambda c: c.id == full_id)
    if base is None:
        base = chats[0]

    migrated = getattr(base, "migrated_to", None)
    migrated_to = None
    if isinstance(migrated, InputChannel):
        migrated_to = {"channel_id": migrated.channel_id, "access_hash": migrated.access_hash}
    if isinstance(migrated, InputChannelFromMessage):
        migrated_to = {"channel_id": migrated.channel_id, "access_hash": None}

    raw_full = from_telethon(full_chat)
    raw_base = from_telethon(base)

    # Prefer Chat.photo (ChatPhoto). If missing, fall back to ChatFull.chat_photo (Photo).
    photo = raw_base.get("photo")
    if photo is None:
        photo = raw_full.get("chat_photo")

    payload = {
        "profile_photo_s3_uri": profile_photo_s3_uri,
        "harvester_id": harvester_id,
        "external_id": str(signed_peer_id),
        "title": raw_base.get("title") or "",
        "photo": _slim_peer_photo(photo),
        # Base Chat fields (exclude account-dependent flags like `left`, `creator`, `admin_rights`)
        "participants_count": raw_base.get("participants_count"),
        "date": raw_base.get("date"),
        "version": raw_base.get("version"),
        "deactivated": raw_base.get("deactivated"),
        "noforwards": raw_base.get("noforwards"),
        "migrated_to": migrated_to,
        "default_banned_rights": raw_base.get("default_banned_rights"),
        # Full ChatFull fields (exclude per-account `notify_settings`)
        "about": raw_full.get("about"),
        "has_scheduled": raw_full.get("has_scheduled"),
        "translations_disabled": raw_full.get("translations_disabled"),
        "pinned_msg_id": raw_full.get("pinned_msg_id"),
        "requests_pending": raw_full.get("requests_pending"),
        "available_reactions": raw_full.get("available_reactions"),
        "reactions_limit": raw_full.get("reactions_limit"),
    }

    return TelegramChatSchema.model_validate(payload)


def _pending_rating_date_as_int(value: Any) -> int | None:
    if value is None or value is False:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.timestamp())
    if isinstance(value, int):
        return value
    return None


def _stories_max_id_as_int(value: Any) -> int | None:
    """Telethon encodes ``stories_max_id`` as a ``RecentStory`` TL object, not an int."""
    if value is None or value is False:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        max_id = value.get("max_id")
        return max_id if isinstance(max_id, int) else None
    max_id = getattr(value, "max_id", None)
    return max_id if isinstance(max_id, int) else None


def _text_field_from_tl_blob(value: Any) -> str | None:
    if value is None or value is False:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        t = value.get("text")
        if isinstance(t, str):
            return t
    return None


def _user_from_get_full_user_result(result: GetFullUserResult) -> User:
    """Return the ``User`` row matching ``result.full_user.id``."""
    full_user = result.full_user
    fallback: User | None = None
    for candidate in result.users:
        if not isinstance(candidate, User):
            continue
        if candidate.id == full_user.id:
            return candidate
        if fallback is None:
            fallback = candidate
    if fallback is not None:
        return fallback
    raise ValueError(f"GetFullUser result has no User rows for full_user id={full_user.id}")


def user_schema_from_telethon_full_user(
    result: GetFullUserResult,
    *,
    signed_peer_id: int,
    profile_photo_s3_uri: str | None = None,
) -> TelegramUserSchema:
    """Build :class:`~db.schemas.telegram.user.TelegramUserSchema` from a ``users.UserFull`` TL result."""
    full_user: TLUserFull = result.full_user
    base_user: User = _user_from_get_full_user_result(result)

    raw_full = from_telethon(full_user)
    raw_base = from_telethon(base_user)

    pcm = full_user.personal_channel_message
    pcm_str = str(pcm) if pcm is not None else None

    now = datetime.now(UTC)
    payload = {
        "profile_photo_s3_uri": profile_photo_s3_uri,
        "created_at": now,
        "updated_at": now,
        "external_id": str(signed_peer_id),
        "access_hash": base_user.access_hash,
        "first_name": base_user.first_name,
        "last_name": base_user.last_name,
        "username": base_user.username,
        "phone": base_user.phone,
        "bot": raw_base.get("bot"),
        "verified": raw_base.get("verified"),
        "restricted": raw_base.get("restricted"),
        "scam": raw_base.get("scam"),
        "fake": raw_base.get("fake"),
        "deleted": bool(raw_base.get("deleted")),
        "premium": raw_base.get("premium"),
        "restriction_reason": raw_base.get("restriction_reason"),
        "lang_code": base_user.lang_code,
        "emoji_status": raw_base.get("emoji_status"),
        "settings": raw_full.get("settings"),
        "notify_settings": raw_full.get("notify_settings"),
        "common_chats_count": full_user.common_chats_count,
        "blocked": full_user.blocked,
        "phone_calls_available": full_user.phone_calls_available,
        "phone_calls_private": full_user.phone_calls_private,
        "can_pin_message": full_user.can_pin_message,
        "has_scheduled": full_user.has_scheduled,
        "video_calls_available": full_user.video_calls_available,
        "voice_messages_forbidden": full_user.voice_messages_forbidden,
        "translations_disabled": full_user.translations_disabled,
        "stories_pinned_available": full_user.stories_pinned_available,
        "blocked_my_stories_from": full_user.blocked_my_stories_from,
        "wallpaper_overridden": full_user.wallpaper_overridden,
        "contact_require_premium": full_user.contact_require_premium,
        "read_dates_private": full_user.read_dates_private,
        "sponsored_enabled": full_user.sponsored_enabled,
        "can_view_revenue": full_user.can_view_revenue,
        "bot_can_manage_emoji_status": full_user.bot_can_manage_emoji_status,
        "display_gifts_button": full_user.display_gifts_button,
        "about": full_user.about,
        "personal_photo": raw_full.get("personal_photo"),
        "profile_photo": raw_full.get("profile_photo"),
        "fallback_photo": raw_full.get("fallback_photo"),
        "bot_info": raw_full.get("bot_info"),
        "pinned_msg_id": full_user.pinned_msg_id,
        "folder_id": full_user.folder_id,
        "ttl_period": full_user.ttl_period,
        "theme": raw_full.get("theme"),
        "private_forward_name": full_user.private_forward_name,
        "bot_group_admin_rights": raw_full.get("bot_group_admin_rights"),
        "bot_broadcast_admin_rights": raw_full.get("bot_broadcast_admin_rights"),
        "wallpaper": raw_full.get("wallpaper"),
        "stories": raw_full.get("stories"),
        "business_work_hours": raw_full.get("business_work_hours"),
        "business_location": raw_full.get("business_location"),
        "business_greeting_message": _text_field_from_tl_blob(raw_full.get("business_greeting_message")),
        "business_away_message": _text_field_from_tl_blob(raw_full.get("business_away_message")),
        "business_intro": raw_full.get("business_intro"),
        "birthday": raw_full.get("birthday"),
        "personal_channel_id": full_user.personal_channel_id,
        "personal_channel_message": pcm_str,
        "stargifts_count": full_user.stargifts_count,
        "starref_program": raw_full.get("starref_program"),
        "bot_verification": raw_full.get("bot_verification"),
        "send_paid_messages_stars": full_user.send_paid_messages_stars,
        "disallowed_gifts": raw_full.get("disallowed_gifts"),
        "stars_rating": raw_full.get("stars_rating"),
        "stars_my_pending_rating": raw_full.get("stars_my_pending_rating"),
        "stars_my_pending_rating_date": _pending_rating_date_as_int(full_user.stars_my_pending_rating_date),
        "main_tab": raw_full.get("main_tab"),
        "saved_music": raw_full.get("saved_music"),
        "note": _text_field_from_tl_blob(raw_full.get("note")),
    }

    # Canonical avatar is the public profile photo. User.photo is the locally
    # assigned contact photo when Telegram sets ``personal``.
    raw_profile_photo = raw_full.get("profile_photo")
    contact_photo_untrusted = bool(getattr(base_user, "contact", False)) and not bool(
        getattr(base_user, "is_self", False)
    )
    if raw_profile_photo is not None:
        payload["photo"] = raw_profile_photo
    elif not contact_photo_untrusted:
        payload["photo"] = raw_base.get("photo")

    return TelegramUserSchema.model_validate(payload)


def message_schema_from_telethon_message(
    msg: TelethonMessage,
    *,
    source_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    harvester_external_id: str | None = None,
    now: datetime | None = None,
) -> TelegramMessageSchema:
    when = now or datetime.now(UTC)

    sender_signed = to_signed_peer_id(msg.from_id)
    from_external_id = str(sender_signed) if sender_signed is not None else None

    # Structured TL blobs
    raw_media = msg.media
    raw_action = msg.action
    raw_reactions = msg.reactions
    raw_fwd = msg.fwd_from
    raw_reply_to = msg.reply_to

    media = (
        slim_message_media_from_full(TypeAdapter(TypeMessageMediaSchema).validate_python(from_telethon(raw_media)))
        if raw_media is not None
        else None
    )
    action_data = json_safe_value(from_telethon(raw_action)) if raw_action is not None else None

    reactions: SlimMessageReactionsSchema | None = None
    if raw_reactions is not None:
        reactions = slim_reactions_from_full(from_telethon(raw_reactions))

    fwd_from = json_safe_value(from_telethon(raw_fwd)) if raw_fwd is not None else None
    reply_to = slim_reply_to_from_full(from_telethon(raw_reply_to)) if raw_reply_to is not None else None

    action_type = None
    if isinstance(action_data, dict):
        action_type = action_data.get("tl")

    content = msg.message
    if content is not None and not isinstance(content, str):
        content = str(content)

    message_type = "MessageService" if msg.action is not None else "Message"

    raw_views = getattr(msg, "views", None)
    views = raw_views if isinstance(raw_views, int) else None

    has_url, has_mention = message_entity_flags(entities=getattr(msg, "entities", None), media=media)

    payload = TelegramMessageSchema(
        message_id=msg.id,
        source_external_id=str(source_external_id),
        peer_type=peer_type,
        message_type=message_type,
        content=content,
        from_external_id=from_external_id,
        # Never treat the harvester session as the product "self" user.
        # Analysts view harvested chats; harvester-sent messages must not be
        # stored/served as outgoing ("ours").
        out=False,
        date=msg.date,
        edit_date=msg.edit_date,
        created_at=when,
        updated_at=when,
        reactions=reactions,
        media=media,
        views=views,
        pinned=bool(msg.pinned),
        deleted=bool(getattr(msg, "deleted", False)),
        silent=bool(msg.silent),
        mentioned=bool(msg.mentioned),
        has_url=has_url,
        has_mention=has_mention,
        grouped_id=getattr(msg, "grouped_id", None),
        action_type=action_type,
        action_data=action_data,
        fwd_from=fwd_from,
        reply_to=reply_to,
        harvester_external_id=harvester_external_id,
    )
    return payload


def stripped_thumb_to_data_url(value: str | bytes | bytearray | None) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.startswith(("b'", 'b"')):
            stripped = ast.literal_eval(value)
        elif isinstance(value, (bytes, bytearray)) and bytes(value).startswith((b"b'", b'b"')):
            stripped = ast.literal_eval(bytes(value).decode())
        elif isinstance(value, (bytes, bytearray)):
            stripped = bytes(value)

        else:
            s = str(value).strip()
            stripped = base64.urlsafe_b64decode(s + "===")

        jpg_bytes = utils.stripped_photo_to_jpg(stripped)
        b64 = base64.b64encode(jpg_bytes).decode()
        return f"data:image/jpeg;base64,{b64}"

    except Exception:
        return None


def _stripped_thumb_from_photo_dict(photo: dict[str, Any]) -> str | bytes | bytearray | None:
    tl = photo.get("tl")
    # Slim Photo / ChatPhoto / UserProfilePhoto keep the thumb at the top level.
    top = photo.get("stripped_thumb")
    if top is not None:
        return top
    if tl in ("ChatPhoto", "UserProfilePhoto"):
        return photo.get("stripped_thumb")
    if tl == "Photo":
        for size in photo.get("sizes") or []:
            if not isinstance(size, dict):
                continue
            if size.get("tl") == "PhotoStrippedSize":
                return size.get("data") or size.get("bytes")
    return None


def profile_photo_media_kind(
    *,
    photo: TypePhotoSchema | dict[str, Any] | None = None,
    profile_photo_s3_uri: str | None = None,
) -> Literal["image", "video"] | None:
    """Infer whether a harvested profile photo is a static image or animated/video avatar."""
    if isinstance(photo, dict):
        harvest = photo.get("harvest")
        if isinstance(harvest, dict):
            kind = harvest.get("media_kind")
            if kind in ("image", "video"):
                return kind
            content_type = harvest.get("content_type")
            if isinstance(content_type, str) and content_type.lower().startswith("video/"):
                return "video"

    uri = (profile_photo_s3_uri or "").strip()
    if not uri:
        return None
    path = uri.split("?", 1)[0].lower()
    if path.endswith(".mp4"):
        return "video"
    return "image"


def get_b64_image_url_from_photo(photo: TypePhotoSchema | dict[str, Any] | None) -> str | None:
    if photo is None:
        return None

    if isinstance(photo, dict):
        normalized = normalize_tl_keys(photo)
        thumb = _stripped_thumb_from_photo_dict(normalized)
        if thumb is not None:
            return stripped_thumb_to_data_url(thumb)
        photo = normalized

    try:
        validated = TypeAdapter(TypePhotoSchema).validate_python(photo)
    except ValidationError:
        return None

    if isinstance(validated, ChatPhotoSchema):
        return stripped_thumb_to_data_url(validated.stripped_thumb)

    if isinstance(validated, PhotoSchema):
        for size in validated.sizes:
            if size.tl == "PhotoStrippedSize":
                return stripped_thumb_to_data_url(size.data)
    return None


def harvester_peer_permissions_from_channel_full(
    *,
    harvester_id: UUID,
    signed_peer_id: int,
    peer_type: TelegramPeerTypeSchema,
    full: ChatFull,
    admin_rights: dict[str, Any] | None,
) -> TelegramHarvesterPeerPermissionsSchema:
    """Map ``channels.getFullChannel`` + ``channels.getParticipant`` into permissions."""
    full_chat = full.full_chat
    notify_raw = getattr(full_chat, "notify_settings", None)

    payload: dict[str, Any] = {
        "harvester_id": harvester_id,
        "external_id": str(signed_peer_id),
        "peer_type": peer_type,
        "notify_settings": from_telethon(notify_raw) if notify_raw is not None else None,
        "read_inbox_max_id": getattr(full_chat, "read_inbox_max_id", None),
        "read_outbox_max_id": getattr(full_chat, "read_outbox_max_id", None),
        "unread_count": getattr(full_chat, "unread_count", None),
        "pts": getattr(full_chat, "pts", None),
        "can_view_participants": getattr(full_chat, "can_view_participants", None),
        "can_set_username": getattr(full_chat, "can_set_username", None),
        "can_set_stickers": getattr(full_chat, "can_set_stickers", None),
        "can_set_location": getattr(full_chat, "can_set_location", None),
        "can_view_stats": getattr(full_chat, "can_view_stats", None),
        "can_delete_channel": getattr(full_chat, "can_delete_channel", None),
        "blocked": getattr(full_chat, "blocked", None),
        "has_scheduled": getattr(full_chat, "has_scheduled", None),
        "can_view_revenue": getattr(full_chat, "can_view_revenue", None),
        "can_view_stars_revenue": getattr(full_chat, "can_view_stars_revenue", None),
        "admin_rights": admin_rights,
        "updated_at": datetime.now(UTC),
    }
    return TelegramHarvesterPeerPermissionsSchema.model_validate(payload)


def harvester_peer_permissions_from_chat_full(
    *,
    harvester_id: UUID,
    signed_peer_id: int,
    full: ChatFull,
    tl_dialog: Any | None,
    admin_rights: dict[str, Any] | None,
) -> TelegramHarvesterPeerPermissionsSchema:
    """Map ``messages.getFullChat`` + ``messages.getPeerDialogs`` into permissions."""
    full_chat = full.full_chat
    notify_raw = getattr(full_chat, "notify_settings", None)

    payload: dict[str, Any] = {
        "harvester_id": harvester_id,
        "external_id": str(signed_peer_id),
        "peer_type": "chat",
        "notify_settings": from_telethon(notify_raw) if notify_raw is not None else None,
        "read_inbox_max_id": getattr(tl_dialog, "read_inbox_max_id", None) if tl_dialog else None,
        "read_outbox_max_id": getattr(tl_dialog, "read_outbox_max_id", None) if tl_dialog else None,
        "unread_count": getattr(tl_dialog, "unread_count", None) if tl_dialog else None,
        "pts": getattr(tl_dialog, "pts", None) if tl_dialog else None,
        "can_set_username": getattr(full_chat, "can_set_username", None),
        "has_scheduled": getattr(full_chat, "has_scheduled", None),
        "admin_rights": admin_rights,
        "updated_at": datetime.now(UTC),
    }
    return TelegramHarvesterPeerPermissionsSchema.model_validate(payload)


_ADMIN_RIGHTS_ADAPTER = TypeAdapter(ChatAdminRightsSchema)
_BANNED_RIGHTS_ADAPTER = TypeAdapter(ChatBannedRightsSchema)


def _tl_rights_schema(
    value: Any,
    *,
    adapter: TypeAdapter[Any],
) -> Any | None:
    if value is None:
        return None
    raw = from_telethon(value)
    if not isinstance(raw, dict):
        return None
    try:
        return adapter.validate_python(normalize_tl_keys(raw))
    except ValidationError:
        return None


def user_profile_fields_are_authoritative(user: User) -> bool:
    """Whether name/photo on this User are the public profile, not a local view.

    ``User.self`` is the account's real profile. ``min`` constructors are cached
    and incomplete. When ``contact`` is set, Telegram returns the locally saved
    address-book name and any personal contact photo instead of the public
    profile — different harvesters otherwise overwrite ``telegram_users`` and
    emit oscillating field history.
    """
    if bool(getattr(user, "is_self", False)):
        return True
    if bool(getattr(user, "min", False)):
        return False
    if bool(getattr(user, "contact", False)):
        return False
    return True


def full_user_profile_fields_are_authoritative(result: GetFullUserResult) -> bool:
    return user_profile_fields_are_authoritative(_user_from_get_full_user_result(result))


def _user_display_title(user: User) -> str | None:
    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    name = " ".join(part for part in (first, last) if part).strip()
    if name:
        return name
    username = (user.username or "").strip()
    if username:
        return f"@{username.lstrip('@')}"
    return None


def user_schema_from_telethon_user(
    user: User,
    *,
    now: datetime | None = None,
) -> TelegramUserSchema:
    """Build a minimal :class:`~db.schemas.telegram.user.TelegramUserSchema` from a Telethon ``User``."""
    signed_peer_id = to_signed_peer_id(user)
    if signed_peer_id is None:
        raise ValueError("User has no signed peer id")

    when = now or datetime.now(UTC)
    raw = from_telethon(user)
    raw_dict = raw if isinstance(raw, dict) else {}

    payload: dict[str, Any] = {
        "external_id": str(signed_peer_id),
        "access_hash": user.access_hash,
        "bot": raw_dict.get("bot"),
        "verified": raw_dict.get("verified"),
        "restricted": raw_dict.get("restricted"),
        "scam": raw_dict.get("scam"),
        "fake": raw_dict.get("fake"),
        "deleted": bool(raw_dict.get("deleted")),
        "premium": raw_dict.get("premium"),
        "restriction_reason": raw_dict.get("restriction_reason") or [],
        "lang_code": user.lang_code,
        "emoji_status": raw_dict.get("emoji_status"),
        "created_at": when,
        "updated_at": when,
    }
    if user.first_name is not None:
        payload["first_name"] = user.first_name
    if user.last_name is not None:
        payload["last_name"] = user.last_name
    if user.username is not None:
        payload["username"] = user.username
    if user.phone is not None:
        payload["phone"] = user.phone
    photo = raw_dict.get("photo")
    if photo is not None:
        payload["photo"] = photo
    return TelegramUserSchema.model_validate(payload)


def participant_schema_from_telethon(
    *,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema,
    user: User,
    title: str | None = None,
    now: datetime | None = None,
) -> TelegramChatParticipantSchema:
    """Map a Telethon ``User`` (with optional ``.participant``) to a participant row."""
    signed_peer_id = to_signed_peer_id(user)
    if signed_peer_id is None:
        raise ValueError("User has no signed peer id")

    when = now or datetime.now(UTC)
    participant = getattr(user, "participant", None)
    payload: dict[str, Any] = {
        "external_id": peer_external_id,
        "user_id": str(signed_peer_id),
        "peer_type": peer_type,
        "is_current_member": True,
        "first_seen_at": when,
        "updated_at": when,
        "title": title or _user_display_title(user),
    }

    if participant is None:
        return TelegramChatParticipantSchema.model_validate(payload)

    tl_name = type(participant).__name__

    if tl_name == "ChatParticipantCreator":
        payload["rank"] = getattr(participant, "rank", None)
    elif tl_name in ("ChatParticipant", "ChatParticipantAdmin"):
        payload.update(
            {
                "inviter_id": getattr(participant, "inviter_id", None),
                "joined_at": ensure_utc(getattr(participant, "date", None)),
                "rank": getattr(participant, "rank", None),
            }
        )
    elif tl_name == "ChannelParticipant":
        payload.update(
            {
                "joined_at": ensure_utc(getattr(participant, "date", None)),
                "rank": getattr(participant, "rank", None),
                "subscription_until_at": ensure_utc(getattr(participant, "subscription_until_date", None)),
            }
        )
    elif tl_name == "ChannelParticipantSelf":
        payload.update(
            {
                "inviter_id": getattr(participant, "inviter_id", None),
                "joined_at": ensure_utc(getattr(participant, "date", None)),
                "rank": getattr(participant, "rank", None),
                "via_request": getattr(participant, "via_request", None),
                "subscription_until_at": ensure_utc(getattr(participant, "subscription_until_date", None)),
            }
        )
    elif tl_name == "ChannelParticipantAdmin":
        payload.update(
            {
                "inviter_id": getattr(participant, "inviter_id", None),
                "promoted_by": getattr(participant, "promoted_by", None),
                "joined_at": ensure_utc(getattr(participant, "date", None)),
                "rank": getattr(participant, "rank", None),
                "admin_rights": _tl_rights_schema(
                    getattr(participant, "admin_rights", None),
                    adapter=_ADMIN_RIGHTS_ADAPTER,
                ),
            }
        )
    elif tl_name == "ChannelParticipantCreator":
        payload.update(
            {
                "rank": getattr(participant, "rank", None),
                "admin_rights": _tl_rights_schema(
                    getattr(participant, "admin_rights", None),
                    adapter=_ADMIN_RIGHTS_ADAPTER,
                ),
            }
        )
    elif tl_name == "ChannelParticipantBanned":
        payload.update(
            {
                "joined_at": ensure_utc(getattr(participant, "date", None)),
                "rank": getattr(participant, "rank", None),
                "kicked_by": getattr(participant, "kicked_by", None),
                "banned_rights": _tl_rights_schema(
                    getattr(participant, "banned_rights", None),
                    adapter=_BANNED_RIGHTS_ADAPTER,
                ),
                "is_current_member": not bool(getattr(participant, "left", False)),
            }
        )

    return TelegramChatParticipantSchema.model_validate(payload)
