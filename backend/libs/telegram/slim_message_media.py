"""Map full TL message media to slim persisted shapes."""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any

from db.schemas.telegram.slim_message_media import (
    SlimMessageMediaContactSchema,
    SlimMessageMediaDiceSchema,
    SlimMessageMediaDocumentSchema,
    SlimMessageMediaEmptySchema,
    SlimMessageMediaGameSchema,
    SlimMessageMediaGeoLiveSchema,
    SlimMessageMediaGeoSchema,
    SlimMessageMediaGiveawayResultsSchema,
    SlimMessageMediaGiveawaySchema,
    SlimMessageMediaInvoiceSchema,
    SlimMessageMediaPaidMediaSchema,
    SlimMessageMediaPhotoSchema,
    SlimMessageMediaPollSchema,
    SlimMessageMediaStorySchema,
    SlimMessageMediaToDoSchema,
    SlimMessageMediaUnsupportedSchema,
    SlimMessageMediaVenueSchema,
    SlimMessageMediaVideoStreamSchema,
    SlimMessageMediaWebPageSchema,
    SlimPollAttachedMediaSchema,
    SlimPollResultOptionSchema,
    SlimTodoItemSchema,
    SlimTypeMessageMediaSchema,
)
from pydantic import TypeAdapter, ValidationError
from telethon import utils

from telegram.slim_harvest import slim_harvest_from_dict

if TYPE_CHECKING:
    from db.schemas.telegram.tl_media import TypeMessageMediaSchema

_SLIM_ADAPTER = TypeAdapter(SlimTypeMessageMediaSchema)
_MAX_THUMB_JPEG_BYTES = 100_000


def _photo_dims(photo: dict[str, Any] | None) -> tuple[int | None, int | None]:
    if not photo:
        return None, None
    sizes = photo.get("sizes") or []
    best: dict[str, Any] | None = None
    best_area = -1
    for s in sizes:
        if not isinstance(s, dict):
            continue
        w = s.get("w")
        h = s.get("h")
        if w is None or h is None:
            continue
        area = int(w) * int(h)
        if area > best_area:
            best_area = area
            best = s
    if best is None:
        return None, None
    return int(best["w"]), int(best["h"])


def _as_thumb_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        s = value.strip()
        if not s or s.startswith("data:"):
            return None
        try:
            return base64.b64decode(s, validate=False)
        except Exception:
            return None
    return None


def _jpeg_data_url(jpeg: bytes) -> str | None:
    if not jpeg or jpeg[:2] != b"\xff\xd8":
        return None
    if len(jpeg) > _MAX_THUMB_JPEG_BYTES:
        return None
    return f"data:image/jpeg;base64,{base64.b64encode(jpeg).decode('ascii')}"


def _inline_thumb_data_url(sizes: Any) -> str | None:
    """Best inline Telegram thumb: cached JPEG if present, else stripped size."""
    if not isinstance(sizes, list):
        return None
    cached: tuple[int, bytes] | None = None
    stripped: bytes | None = None
    for size in sizes:
        if not isinstance(size, dict):
            continue
        tl = size.get("tl")
        raw = _as_thumb_bytes(size.get("data") or size.get("bytes"))
        if raw is None:
            continue
        if tl == "PhotoCachedSize":
            area = int(size.get("w") or 0) * int(size.get("h") or 0)
            if cached is None or area >= cached[0]:
                cached = (area, raw)
        elif tl == "PhotoStrippedSize" and stripped is None:
            stripped = raw
    if cached is not None:
        url = _jpeg_data_url(cached[1])
        if url:
            return url
    if stripped is None:
        return None
    try:
        jpeg = utils.stripped_photo_to_jpg(stripped)
    except Exception:
        return None
    return _jpeg_data_url(bytes(jpeg))


def _document_attrs(doc: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not doc:
        return out
    attrs = doc.get("attributes") or []
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        tl = attr.get("tl")
        if tl == "DocumentAttributeFilename":
            out["filename"] = attr.get("file_name") or attr.get("filename")
        elif tl == "DocumentAttributeVideo":
            out["duration"] = attr.get("duration")
            out["video"] = True
            if attr.get("w") is not None:
                out["w"] = attr.get("w")
            if attr.get("h") is not None:
                out["h"] = attr.get("h")
            if attr.get("round_message"):
                out["round"] = True
        elif tl == "DocumentAttributeAudio":
            if attr.get("voice"):
                out["voice"] = True
            else:
                out["audio"] = True
            out["duration"] = attr.get("duration")
        elif tl == "DocumentAttributeAnimated":
            out["animated"] = True
        elif tl == "DocumentAttributeSticker":
            out["sticker"] = True
    return out


def _peer_id_str(peer: Any) -> str | None:
    if peer is None:
        return None
    if isinstance(peer, dict):
        return peer.get("peer_id") or peer.get("channel_id") or peer.get("user_id")
    return str(peer)


def _poll_question_text(poll: dict[str, Any] | None) -> str:
    if not poll:
        return ""
    q = poll.get("question")
    if isinstance(q, dict):
        return str(q.get("text") or "")
    return str(q or "")


def _poll_answers(poll: dict[str, Any] | None) -> list[str]:
    if not poll:
        return []
    answers = poll.get("answers") or []
    out: list[str] = []
    for a in answers:
        if not isinstance(a, dict):
            continue
        text = a.get("text")
        if isinstance(text, dict):
            out.append(str(text.get("text") or ""))
        else:
            out.append(str(text or ""))
    return out


def _poll_option_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=False)
        except Exception:
            return None
    return None


def _poll_option_index(
    poll_answers: list[Any],
    option_raw: Any,
    *,
    fallback: int,
) -> int:
    """Map TL poll option bytes to a 0-based answer index for slim storage."""
    if isinstance(option_raw, int):
        return option_raw
    opt_bytes = _poll_option_bytes(option_raw)
    if opt_bytes is not None:
        for i, answer in enumerate(poll_answers):
            if not isinstance(answer, dict):
                continue
            if _poll_option_bytes(answer.get("option")) == opt_bytes:
                return i
    return fallback


def _slim_poll_attached(attached: dict[str, Any] | None) -> SlimPollAttachedMediaSchema | None:
    """Keep a displayable thumb for poll-attached photo/document media."""
    if not attached:
        return None
    tl = attached.get("tl")
    w: int | None = None
    h: int | None = None
    thumb: str | None = None
    if tl == "MessageMediaPhoto":
        photo = attached.get("photo")
        if isinstance(photo, dict):
            w, h = _photo_dims(photo)
            thumb = _inline_thumb_data_url(photo.get("sizes"))
        else:
            raw_w, raw_h = attached.get("w"), attached.get("h")
            w = int(raw_w) if isinstance(raw_w, int) else None
            h = int(raw_h) if isinstance(raw_h, int) else None
            raw_thumb = attached.get("stripped_thumb")
            thumb = raw_thumb if isinstance(raw_thumb, str) else None
    elif tl == "MessageMediaDocument":
        doc = attached.get("document")
        if isinstance(doc, dict):
            attrs = _document_attrs(doc)
            raw_w, raw_h = attrs.get("w"), attrs.get("h")
            w = int(raw_w) if isinstance(raw_w, int) else None
            h = int(raw_h) if isinstance(raw_h, int) else None
            thumb = _inline_thumb_data_url(doc.get("thumbs"))
        else:
            raw_w, raw_h = attached.get("w"), attached.get("h")
            w = int(raw_w) if isinstance(raw_w, int) else None
            h = int(raw_h) if isinstance(raw_h, int) else None
            raw_thumb = attached.get("stripped_thumb")
            thumb = raw_thumb if isinstance(raw_thumb, str) else None
    else:
        return None
    if w is None and h is None and not thumb:
        return None
    return SlimPollAttachedMediaSchema(w=w, h=h, stripped_thumb=thumb)


def slim_message_media_from_full(
    media: dict[str, Any] | "TypeMessageMediaSchema" | None,
    *,
    preserve_harvest: bool = True,
) -> dict[str, Any] | None:
    """Convert full TL media (dict or validated model) to a slim JSON-serializable dict."""
    if media is None:
        return None

    if isinstance(media, dict):
        try:
            return validate_slim_message_media(media)
        except ValidationError:
            pass
    else:
        media = media.model_dump(mode="json", exclude_none=True)

    tl = media.get("tl")
    if not tl:
        return None

    existing_harvest = media.get("harvest") if preserve_harvest else None
    harvest = slim_harvest_from_dict(existing_harvest)

    slim: Any
    match tl:
        case "MessageMediaEmpty":
            slim = SlimMessageMediaEmptySchema()
        case "MessageMediaUnsupported":
            slim = SlimMessageMediaUnsupportedSchema()
        case "MessageMediaPhoto":
            w, h = _photo_dims(media.get("photo"))
            photo = media.get("photo") or {}
            slim = SlimMessageMediaPhotoSchema(
                w=w,
                h=h,
                live_photo=True if media.get("live_photo") else None,
                harvest=harvest,
                stripped_thumb=_inline_thumb_data_url(photo.get("sizes") if isinstance(photo, dict) else None),
            )
        case "MessageMediaDocument":
            doc = media.get("document") or {}
            attrs = _document_attrs(doc)
            slim = SlimMessageMediaDocumentSchema(
                mime_type=doc.get("mime_type"),
                size=doc.get("size"),
                filename=attrs.get("filename"),
                duration=attrs.get("duration") or media.get("duration"),
                w=attrs.get("w"),
                h=attrs.get("h"),
                video=True if media.get("video") or attrs.get("video") else None,
                voice=True if media.get("voice") or attrs.get("voice") else None,
                round=True if media.get("round") or attrs.get("round") else None,
                animated=True if media.get("animated") or attrs.get("animated") else None,
                sticker=True if media.get("sticker") or attrs.get("sticker") else None,
                harvest=harvest,
                stripped_thumb=_inline_thumb_data_url(doc.get("thumbs") if isinstance(doc, dict) else None),
            )
        case "MessageMediaWebPage":
            wp = media.get("webpage") or {}
            doc = wp.get("document") or {}
            preview_mime = doc.get("mime_type")
            if not preview_mime:
                photo = wp.get("photo")
                if photo:
                    preview_mime = "image/jpeg"
            slim = SlimMessageMediaWebPageSchema(
                url=wp.get("url"),
                title=wp.get("title"),
                site_name=wp.get("site_name"),
                description=wp.get("description"),
                preview_mime=preview_mime,
                harvest=harvest,
            )
        case "MessageMediaGeo":
            geo = media.get("geo") or {}
            slim = SlimMessageMediaGeoSchema(
                lat=float(geo.get("lat", 0)),
                long=float(geo.get("long", 0)),
                accuracy_radius=geo.get("accuracy_radius"),
            )
        case "MessageMediaGeoLive":
            geo = media.get("geo") or {}
            slim = SlimMessageMediaGeoLiveSchema(
                lat=float(geo.get("lat", 0)),
                long=float(geo.get("long", 0)),
                period=int(media.get("period") or 0),
                heading=media.get("heading"),
                proximity_notification_radius=media.get("proximity_notification_radius"),
            )
        case "MessageMediaVenue":
            geo = media.get("geo") or {}
            slim = SlimMessageMediaVenueSchema(
                lat=float(geo.get("lat", 0)),
                long=float(geo.get("long", 0)),
                title=str(media.get("title") or ""),
                address=str(media.get("address") or ""),
                provider=str(media.get("provider") or ""),
                venue_id=str(media.get("venue_id") or ""),
                venue_type=str(media.get("venue_type") or ""),
            )
        case "MessageMediaContact":
            slim = SlimMessageMediaContactSchema(
                phone_number=str(media.get("phone_number") or ""),
                first_name=str(media.get("first_name") or ""),
                last_name=str(media.get("last_name") or ""),
                user_id=int(media.get("user_id") or 0),
            )
        case "MessageMediaPoll":
            poll = media.get("poll") or {}
            results_raw = media.get("results") or {}
            result_options = results_raw.get("results") or []
            poll_answers = poll.get("answers") or []
            results = [
                SlimPollResultOptionSchema(
                    option=_poll_option_index(
                        poll_answers,
                        r.get("option"),
                        fallback=i,
                    ),
                    voters=int(r.get("voters") or 0),
                    chosen=True if r.get("chosen") else None,
                    correct=True if r.get("correct") else None,
                )
                for i, r in enumerate(result_options)
                if isinstance(r, dict)
            ]
            solution_raw = results_raw.get("solution")
            solution = str(solution_raw).strip() if isinstance(solution_raw, str) else ""
            attached_raw = media.get("attached_media")
            slim = SlimMessageMediaPollSchema(
                poll_id=int(poll.get("id") or 0),
                question=_poll_question_text(poll),
                answers=_poll_answers(poll),
                quiz=True if poll.get("quiz") else None,
                multiple_choice=True if poll.get("multiple_choice") else None,
                closed=True if poll.get("closed") else None,
                public_voters=True if poll.get("public_voters") else None,
                total_voters=results_raw.get("total_voters"),
                results=results or None,
                solution=solution or None,
                attached=_slim_poll_attached(attached_raw if isinstance(attached_raw, dict) else None),
            )
        case "MessageMediaDice":
            slim = SlimMessageMediaDiceSchema(
                value=int(media.get("value") or 0),
                emoticon=str(media.get("emoticon") or ""),
            )
        case "MessageMediaGame":
            game = media.get("game") or {}
            slim = SlimMessageMediaGameSchema(
                id=int(game.get("id") or 0),
                short_name=str(game.get("short_name") or ""),
                title=str(game.get("title") or ""),
                description=str(game.get("description") or ""),
            )
        case "MessageMediaInvoice":
            slim = SlimMessageMediaInvoiceSchema(
                title=str(media.get("title") or ""),
                description=str(media.get("description") or ""),
                currency=str(media.get("currency") or ""),
                total_amount=int(media.get("total_amount") or 0),
                start_param=str(media.get("start_param") or ""),
                shipping_address_requested=True if media.get("shipping_address_requested") else None,
                test=True if media.get("test") else None,
                receipt_msg_id=media.get("receipt_msg_id"),
            )
        case "MessageMediaToDo":
            todo = media.get("todo") or {}
            items = [
                SlimTodoItemSchema(
                    id=int(i.get("id", 0)),
                    title=str(i.get("title") or ""),
                    completed=bool(i.get("completed")),
                )
                for i in (todo.get("items") or [])
                if isinstance(i, dict)
            ]
            slim = SlimMessageMediaToDoSchema(
                title=str(todo.get("title") or ""),
                items=items,
                others_can_append=True if todo.get("others_can_append") else None,
                others_can_complete=True if todo.get("others_can_complete") else None,
            )
        case "MessageMediaStory":
            peer_id = _peer_id_str(media.get("peer"))
            slim = SlimMessageMediaStorySchema(
                peer_id=str(peer_id or ""),
                story_id=int(media.get("id") or 0),
                via_mention=True if media.get("via_mention") else None,
            )
        case "MessageMediaGiveaway":
            slim = SlimMessageMediaGiveawaySchema(
                channels=[int(c) for c in (media.get("channels") or [])],
                quantity=int(media.get("quantity") or 0),
                until_date=media.get("until_date"),
                only_new_subscribers=True if media.get("only_new_subscribers") else None,
                winners_are_visible=True if media.get("winners_are_visible") else None,
                countries_iso2=media.get("countries_iso2"),
                prize_description=media.get("prize_description"),
                months=media.get("months"),
                stars=media.get("stars"),
            )
        case "MessageMediaGiveawayResults":
            slim = SlimMessageMediaGiveawayResultsSchema(
                channel_id=int(media.get("channel_id") or 0),
                launch_msg_id=int(media.get("launch_msg_id") or 0),
                winners_count=int(media.get("winners_count") or 0),
                unclaimed_count=int(media.get("unclaimed_count") or 0),
                winners=[int(w) for w in (media.get("winners") or [])],
                until_date=media.get("until_date"),
                only_new_subscribers=True if media.get("only_new_subscribers") else None,
                refunded=True if media.get("refunded") else None,
                additional_peers_count=media.get("additional_peers_count"),
                months=media.get("months"),
                stars=media.get("stars"),
                prize_description=media.get("prize_description"),
            )
        case "MessageMediaPaidMedia":
            extended = media.get("extended_media") or []
            kinds: list[str] = []
            for item in extended:
                if isinstance(item, dict):
                    kinds.append(str(item.get("tl") or "unknown"))
            slim = SlimMessageMediaPaidMediaSchema(
                stars_amount=int(media.get("stars_amount") or 0),
                media_kinds=kinds,
            )
        case "MessageMediaVideoStream":
            slim = SlimMessageMediaVideoStreamSchema(
                rtmp_stream=True if media.get("rtmp_stream") else None,
            )
        case _:
            slim = SlimMessageMediaUnsupportedSchema()

    return slim.model_dump(mode="json", exclude_none=True)


def validate_slim_message_media(media: dict[str, Any] | None) -> dict[str, Any] | None:
    if media is None:
        return None
    return _SLIM_ADAPTER.validate_python(media).model_dump(mode="json", exclude_none=True)
