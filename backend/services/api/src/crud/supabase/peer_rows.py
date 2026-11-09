"""Helpers to enrich raw SQL rows into API view payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.api.src.crud.supabase.views.general.category import CategorySimpleView
from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelLinkView,
    TelegramChannelView,
    TelegramChatView,
    TelegramMegagroupLinkView,
    TelegramMegagroupView,
)
from services.api.src.crud.supabase.views.telegram.field_history import FieldHistoryEntryView
from services.api.src.models.telegram import CommunityDetailResponse
from telegram.mappers import get_b64_image_url_from_photo, profile_photo_media_kind


def coerce_categories(raw: Any) -> list[CategorySimpleView]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [CategorySimpleView.model_validate(item) for item in raw]
    return []


def enrich_telegram_peer_row(row: Mapping[str, Any], *, peer_type: str) -> dict[str, Any]:
    out = dict(row)
    out["peer_type"] = peer_type
    out["stripped_profile_photo"] = get_b64_image_url_from_photo(out.get("photo"))
    out["profile_photo_media_kind"] = profile_photo_media_kind(
        photo=out.get("photo"),
        profile_photo_s3_uri=out.get("profile_photo_s3_uri"),
    )
    out["categories"] = coerce_categories(out.get("categories"))
    return out


def community_view_from_peer_row(
    row: Mapping[str, Any],
    *,
    peer_type: str,
) -> CommunityView | None:
    enriched = enrich_telegram_peer_row(row, peer_type=peer_type)
    return CommunityView.from_row(
        {
            **enriched,
            "community_id": enriched.get("id"),
        }
    )


def community_detail_from_peer(
    detail: TelegramChannelLinkView | TelegramMegagroupLinkView | TelegramChatView,
    *,
    field_history: list[FieldHistoryEntryView] | None = None,
) -> CommunityDetailResponse:
    history = field_history or []
    peer_type = detail.peer_type
    if peer_type == "chat":
        return CommunityDetailResponse(
            peer_type=peer_type,
            chat=detail,
            channel=None,
            linked_peer=None,
            field_history=history,
        )

    payload = detail.model_dump()
    linked_peer = payload.pop("linked_peer", None)
    channel_model = (
        TelegramMegagroupView.model_validate(payload)
        if peer_type == "megagroup"
        else TelegramChannelView.model_validate(payload)
    )
    linked_model: CommunityView | None = None
    if linked_peer is not None:
        linked_model = (
            linked_peer if isinstance(linked_peer, CommunityView) else CommunityView.model_validate(linked_peer)
        )
    return CommunityDetailResponse(
        peer_type=peer_type,
        channel=channel_model,
        chat=None,
        linked_peer=linked_model,
        field_history=history,
    )
