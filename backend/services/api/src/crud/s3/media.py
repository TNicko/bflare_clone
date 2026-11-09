from __future__ import annotations

import asyncio
from typing import Any, TypeVar, cast

from pydantic import BaseModel


from services.api.src.crud.supabase.views.general.community import CommunityView
from services.api.src.crud.supabase.views.general.project import (
    ProjectMessageMappingView,
    ProjectView,
)
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.models.telegram import (
    CommunityDetailResponse,
)
from aws.s3 import presign_harvest_urls_in_json, presign_s3_uri, presign_s3_uri_map
from services.api.src.crud.supabase.views.telegram.field_history import FieldHistoryEntryView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView, UserPeerDetailView

_TModel = TypeVar("_TModel", bound=BaseModel)


async def _presign_profile_photo_s3_uri(
    profile_photo_s3_uri: str | None,
    *,
    region: str,
) -> str | None:
    if not profile_photo_s3_uri or not profile_photo_s3_uri.startswith("s3://"):
        return profile_photo_s3_uri
    return await presign_s3_uri(profile_photo_s3_uri, region=region)


async def _presign_model_profile_photo(model: _TModel, *, region: str) -> _TModel:
    updates: dict[str, object] = {}

    uri = getattr(model, "profile_photo_s3_uri", None)
    presigned = await _presign_profile_photo_s3_uri(uri, region=region)
    if presigned != uri:
        updates["profile_photo_s3_uri"] = presigned

    photo = getattr(model, "photo", None)
    if photo is not None:
        presigned_photo = await presign_harvest_urls_in_json(photo, region=region)
        if presigned_photo is not photo:
            updates["photo"] = presigned_photo

    if not updates:
        return model
    return cast(_TModel, model.model_copy(update=updates))


async def presign_model_profile_photo(model: _TModel, *, region: str) -> _TModel:
    """Public wrapper used by routers that return a single peer payload."""
    return await _presign_model_profile_photo(model, region=region)


async def presign_field_history_entries(
    entries: list[FieldHistoryEntryView],
    *,
    region: str,
) -> list[FieldHistoryEntryView]:
    if not entries:
        return entries

    out: list[FieldHistoryEntryView] = []
    for entry in entries:
        old_value = entry.old_value
        if entry.field_key == "photo" and old_value is not None:
            old_value = await presign_harvest_urls_in_json(old_value, region=region)
        if old_value is entry.old_value:
            out.append(entry)
        else:
            out.append(entry.model_copy(update={"old_value": old_value}))
    return out


async def presign_user_partial(user: UserPartialView, *, region: str) -> UserPartialView:
    presigned = await _presign_profile_photo_s3_uri(user.profile_photo_s3_uri, region=region)
    if presigned == user.profile_photo_s3_uri:
        return user
    return user.model_copy(update={"profile_photo_s3_uri": presigned})


async def presign_user_partials(
    users: list[UserPartialView],
    *,
    region: str,
) -> list[UserPartialView]:
    """Presign profile photos for a page of users (one S3 client for the batch)."""
    if not region or not users:
        return users
    uris = [uri for user in users if (uri := user.profile_photo_s3_uri) and uri.startswith("s3://")]
    if not uris:
        return users
    signed = await presign_s3_uri_map(uris, region=region)
    out: list[UserPartialView] = []
    for user in users:
        uri = user.profile_photo_s3_uri
        https = signed.get(uri) if uri else None
        if https is None or https == uri:
            out.append(user)
        else:
            out.append(user.model_copy(update={"profile_photo_s3_uri": https}))
    return out


async def presign_keyword_peer(peer: KeywordPeerView, *, region: str) -> KeywordPeerView:
    presigned = await _presign_profile_photo_s3_uri(peer.profile_photo_s3_uri, region=region)
    if presigned == peer.profile_photo_s3_uri:
        return peer
    return peer.model_copy(update={"profile_photo_s3_uri": presigned})


async def presign_keyword_peers(
    peers: list[KeywordPeerView],
    *,
    region: str,
) -> list[KeywordPeerView]:
    """Presign profile photos for keyword/semantic peer partials (one S3 client)."""
    if not region or not peers:
        return peers
    uris = [uri for peer in peers if (uri := peer.profile_photo_s3_uri) and uri.startswith("s3://")]
    if not uris:
        return peers
    signed = await presign_s3_uri_map(uris, region=region)
    out: list[KeywordPeerView] = []
    for peer in peers:
        uri = peer.profile_photo_s3_uri
        https = signed.get(uri) if uri else None
        if https is None or https == uri:
            out.append(peer)
        else:
            out.append(peer.model_copy(update={"profile_photo_s3_uri": https}))
    return out


async def presign_user_partials_by_id(
    users: dict[str, UserPartialView],
    *,
    region: str,
) -> dict[str, UserPartialView]:
    if not region or not users:
        return users
    keys = list(users.keys())
    signed_list = await presign_user_partials([users[k] for k in keys], region=region)
    return dict(zip(keys, signed_list, strict=True))


async def presign_keyword_peers_by_id(
    peers: dict[str, KeywordPeerView],
    *,
    region: str,
) -> dict[str, KeywordPeerView]:
    if not region or not peers:
        return peers
    keys = list(peers.keys())
    signed_list = await presign_keyword_peers([peers[k] for k in keys], region=region)
    return dict(zip(keys, signed_list, strict=True))


async def presign_community_view(community: CommunityView, *, region: str) -> CommunityView:
    presigned = await _presign_profile_photo_s3_uri(community.profile_photo_s3_uri, region=region)
    if presigned == community.profile_photo_s3_uri:
        return community
    return community.model_copy(update={"profile_photo_s3_uri": presigned})


async def presign_community_views(
    communities: list[CommunityView],
    *,
    region: str,
) -> list[CommunityView]:
    """Presign profile photos for a page of communities (one S3 client for the batch)."""
    if not region or not communities:
        return communities
    uris = [uri for community in communities if (uri := community.profile_photo_s3_uri) and uri.startswith("s3://")]
    if not uris:
        return communities
    signed = await presign_s3_uri_map(uris, region=region)
    out: list[CommunityView] = []
    for community in communities:
        uri = community.profile_photo_s3_uri
        https = signed.get(uri) if uri else None
        if https is None or https == uri:
            out.append(community)
        else:
            out.append(community.model_copy(update={"profile_photo_s3_uri": https}))
    return out


async def presign_project_message_mapping(
    mapping: ProjectMessageMappingView,
    *,
    region: str,
) -> ProjectMessageMappingView:
    """Presign peer profile photo URI on a pinned message mapping."""
    if not region:
        return mapping
    uri = mapping.peer_profile_photo_s3_uri
    presigned = await _presign_profile_photo_s3_uri(uri, region=region)
    if presigned == uri:
        return mapping
    return mapping.model_copy(update={"peer_profile_photo_s3_uri": presigned})


async def presign_project_view(project: ProjectView, *, region: str) -> ProjectView:
    """Presign profile photo URIs on a pin-dashboard project payload."""
    if not region:
        return project

    channels = [await _presign_model_profile_photo(channel, region=region) for channel in project.telegram_channels]
    megagroups = [
        await _presign_model_profile_photo(megagroup, region=region) for megagroup in project.telegram_megagroups
    ]
    chats = [await _presign_model_profile_photo(chat, region=region) for chat in project.telegram_chats]
    users = [await presign_user_partial(user, region=region) for user in project.telegram_users]
    messages = [await presign_project_message_mapping(message, region=region) for message in project.telegram_messages]
    message_peers = {
        key: await presign_keyword_peer(peer, region=region) for key, peer in project.telegram_message_peers.items()
    }
    message_users = {
        key: await presign_user_partial(user, region=region) for key, user in project.telegram_message_users.items()
    }

    if (
        channels == project.telegram_channels
        and megagroups == project.telegram_megagroups
        and chats == project.telegram_chats
        and users == project.telegram_users
        and messages == project.telegram_messages
        and message_peers == project.telegram_message_peers
        and message_users == project.telegram_message_users
    ):
        return project

    return project.model_copy(
        update={
            "telegram_channels": channels,
            "telegram_megagroups": megagroups,
            "telegram_chats": chats,
            "telegram_users": users,
            "telegram_messages": messages,
            "telegram_message_peers": message_peers,
            "telegram_message_users": message_users,
        }
    )


async def presign_project_views(
    projects: list[ProjectView],
    *,
    region: str,
) -> list[ProjectView]:
    if not region or not projects:
        return projects
    return [await presign_project_view(project, region=region) for project in projects]


async def presign_user_profile_media(
    user: UserPeerDetailView,
    *,
    region: str,
    stories_gifts: bool = True,
    field_history: bool = True,
) -> UserPeerDetailView:

    updates: dict[str, object] = {}

    presigned_photo = await _presign_profile_photo_s3_uri(user.profile_photo_s3_uri, region=region)
    if presigned_photo != user.profile_photo_s3_uri:
        updates["profile_photo_s3_uri"] = presigned_photo

    if stories_gifts:
        if user.pinned_stories is not None:
            updates["pinned_stories"] = await presign_harvest_urls_in_json(
                user.pinned_stories,
                region=region,
            )
        if user.saved_star_gifts is not None:
            updates["saved_star_gifts"] = await presign_harvest_urls_in_json(
                user.saved_star_gifts,
                region=region,
            )
    if field_history and user.field_history:
        presigned_history = await presign_field_history_entries(user.field_history, region=region)
        if presigned_history is not user.field_history:
            updates["field_history"] = presigned_history
    if user.affiliated_peers:
        peers = await asyncio.gather(
            *(presign_community_view(entry.peer, region=region) for entry in user.affiliated_peers)
        )
        affiliated = []
        changed = False
        for entry, peer in zip(user.affiliated_peers, peers, strict=True):
            if peer is not entry.peer:
                changed = True
                affiliated.append(entry.model_copy(update={"peer": peer}))
            else:
                affiliated.append(entry)
        if changed:
            updates["affiliated_peers"] = affiliated
    if not updates:
        return user
    return user.model_copy(update=updates)


async def presign_community_detail_media(
    response: CommunityDetailResponse,
    *,
    region: str,
) -> CommunityDetailResponse:
    updates: dict[str, Any] = {}

    if response.channel is not None:
        channel = await _presign_model_profile_photo(response.channel, region=region)
        if channel is not response.channel:
            updates["channel"] = channel
    if response.chat is not None:
        chat = await _presign_model_profile_photo(response.chat, region=region)
        if chat is not response.chat:
            updates["chat"] = chat
    if response.linked_peer is not None:
        linked_peer = await presign_community_view(response.linked_peer, region=region)
        if linked_peer is not response.linked_peer:
            updates["linked_peer"] = linked_peer
    if response.field_history:
        field_history = await presign_field_history_entries(response.field_history, region=region)
        if field_history is not response.field_history:
            updates["field_history"] = field_history
    if response.bots:
        bots = [await presign_user_partial(bot, region=region) for bot in response.bots]
        updates["bots"] = bots

    if not updates:
        return response
    return response.model_copy(update=updates)
