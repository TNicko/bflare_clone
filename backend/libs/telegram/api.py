import logging
from typing import Any, cast
from telethon import utils
from telethon import TelegramClient, functions
from telethon.tl.types.contacts import Found, ResolvedPeer
from telethon.tl.types.messages import Chats, ChatsSlice
from telethon.errors import FloodWaitError, PhoneNotOccupiedError, PhoneNumberInvalidError, UserNotParticipantError
from telethon.tl.types import (
    Channel,
    ChannelParticipant,
    Chat,
    Dialog,
    InputChannel,
    InputDialogPeer,
    InputPeerSelf,
    TypeChat,
    TypeInputChannel,
    TypeInputPeer,
    TypeUser,
    User,
)
from telethon.tl.types.messages import ChatFull, PeerDialogs
from telethon.tl.types.payments import SavedStarGifts
from telethon.tl.types.stories import Stories

from telegram.utils import channel_id_from_signed_peer_id

lg = logging.getLogger(__name__)

PINNED_STORIES_PAGE_LIMIT = 100
PINNED_STORIES_MAX_ITEMS = 500
SAVED_STAR_GIFTS_PAGE_LIMIT = 100
SAVED_STAR_GIFTS_MAX_ITEMS = 500


async def search_peers(
    client: TelegramClient,
    query: str,
    limit: int | None = None,
) -> tuple[list[TypeChat], list[TypeUser]]:
    """Search for chats and users by keyword using contacts.SearchRequest.

    Propagates ``FloodWaitError`` (and other RPC errors) so callers can mark the
    query run failed and stop instead of treating an empty result as success.
    """
    limit = limit if limit else 100
    found = cast(
        Found,
        await client(
            functions.contacts.SearchRequest(
                q=query,
                limit=limit,
            )
        ),
    )
    return list(found.chats), list(found.users)


async def get_recommendations_for_channel(
    client: TelegramClient,
    channel_id: int,
    access_hash: int | None = None,
) -> list[Channel]:
    """Get similar channel recommendations for a given raw Telegram channel id."""
    if access_hash is not None:
        channel_input: TypeInputChannel = InputChannel(channel_id, access_hash or 0)
    else:
        entity = await client.get_input_entity(channel_id)
        channel_input = cast(TypeInputChannel, utils.get_input_channel(entity))
    result = cast(
        Chats | ChatsSlice,
        await client(functions.channels.GetChannelRecommendationsRequest(channel=channel_input)),
    )
    return [chat for chat in result.chats if isinstance(chat, Channel)]


async def get_recommendations_for_signed_channel(
    client: TelegramClient,
    external_id: str | int,
    access_hash: int,
) -> list[Channel]:
    """Get similar channels for a signed peer id (e.g. ``-100…``) stored in Postgres."""
    signed = int(external_id)
    raw_channel_id = channel_id_from_signed_peer_id(signed)
    if raw_channel_id is None:
        raise ValueError(f"external_id={external_id!r} is not a channel/megagroup signed peer id")
    return await get_recommendations_for_channel(
        client,
        raw_channel_id,
        access_hash=access_hash,
    )


def input_channel_for_entity(channel: Channel) -> InputChannel:
    return InputChannel(channel.id, channel.access_hash or 0)


async def get_full_channel(
    client: TelegramClient,
    channel: Channel | InputChannel,
) -> ChatFull:
    """``channels.getFullChannel``."""
    if isinstance(channel, Channel):
        channel = input_channel_for_entity(channel)
    return cast(ChatFull, await client(functions.channels.GetFullChannelRequest(channel)))


async def get_full_chat(client: TelegramClient, chat_id: int) -> ChatFull:
    """``messages.getFullChat``."""
    return cast(ChatFull, await client(functions.messages.GetFullChatRequest(chat_id)))


async def get_channel_participant_self(
    client: TelegramClient,
    channel: Channel | InputChannel,
) -> ChannelParticipant | None:
    """``channels.getParticipant`` for the logged-in harvester account."""
    if isinstance(channel, Channel):
        channel = input_channel_for_entity(channel)
    try:
        result = await client(functions.channels.GetParticipantRequest(channel, InputPeerSelf()))
    except UserNotParticipantError:
        return None
    return cast(ChannelParticipant, result)


async def get_peer_dialog(
    client: TelegramClient,
    entity: Channel | Chat,
) -> Dialog | None:
    """``messages.getPeerDialogs`` for one peer's dialog row."""
    input_peer = await client.get_input_entity(entity)
    result = cast(PeerDialogs, await client(functions.messages.GetPeerDialogsRequest([InputDialogPeer(input_peer)])))
    dialogs = result.dialogs or []
    return dialogs[0] if dialogs else None


async def resolve_phone(client: TelegramClient, phone: str) -> User | None:
    """Resolve a phone number to a Telegram user via ``contacts.resolvePhone``."""
    try:
        resolved = cast(
            ResolvedPeer,
            await client(functions.contacts.ResolvePhoneRequest(phone=phone)),
        )
    except PhoneNotOccupiedError:
        lg.info("No Telegram user for phone %r", phone)
        return None
    except PhoneNumberInvalidError:
        lg.info("Invalid phone number %r", phone)
        return None
    except FloodWaitError:
        raise
    except Exception:
        lg.exception("Error resolving phone %r", phone)
        return None

    for user in resolved.users:
        if isinstance(user, User):
            return user
    return None


async def get_pinned_stories(
    client: TelegramClient,
    peer: TypeInputPeer | int | str,
    *,
    offset_id: int = 0,
    page_limit: int = PINNED_STORIES_PAGE_LIMIT,
    max_pages: int | None = None,
    max_items: int = PINNED_STORIES_MAX_ITEMS,
) -> Stories:
    """``stories.getPinnedStories``.

    ``max_pages=None`` fetches every page (up to ``max_items``). ``max_pages=1`` returns a
    single page starting at ``offset_id``.
    """
    input_peer = await client.get_input_entity(peer)

    async def _fetch_page(page_offset_id: int, limit: int) -> Stories:
        return cast(
            Stories,
            await client(
                functions.stories.GetPinnedStoriesRequest(
                    peer=input_peer,
                    offset_id=page_offset_id,
                    limit=limit,
                )
            ),
        )

    if max_pages == 1:
        return await _fetch_page(offset_id, page_limit)

    merged_stories: list[Any] = []
    merged_chats: dict[int, Any] = {}
    merged_users: dict[int, Any] = {}
    pinned_to_top: list[int] | None = None
    total_count = 0
    page_offset_id = offset_id
    pages_fetched = 0

    while (max_pages is None or pages_fetched < max_pages) and len(merged_stories) < max_items:
        limit = min(page_limit, max_items - len(merged_stories))
        page = await _fetch_page(page_offset_id, limit)
        pages_fetched += 1
        total_count = page.count
        if pinned_to_top is None and page.pinned_to_top:
            pinned_to_top = list(page.pinned_to_top)

        page_stories = page.stories or []
        if not page_stories:
            break

        for story in page_stories:
            merged_stories.append(story)
            if len(merged_stories) >= max_items:
                break

        for chat in page.chats or []:
            merged_chats[chat.id] = chat
        for user in page.users or []:
            merged_users[user.id] = user

        if len(page_stories) < limit:
            break
        page_offset_id = page_stories[-1].id

    return Stories(
        count=total_count,
        stories=merged_stories,
        chats=list(merged_chats.values()),
        users=list(merged_users.values()),
        pinned_to_top=pinned_to_top,
    )


async def get_saved_star_gifts(
    client: TelegramClient,
    peer: TypeInputPeer | int | str,
    *,
    offset: str = "",
    page_limit: int = SAVED_STAR_GIFTS_PAGE_LIMIT,
    max_pages: int | None = None,
    max_items: int = SAVED_STAR_GIFTS_MAX_ITEMS,
    exclude_unsaved: bool = True,
) -> SavedStarGifts:
    """``payments.getSavedStarGifts``.

    ``max_pages=None`` fetches every page (up to ``max_items``). ``max_pages=1`` returns a
    single page starting at ``offset``.
    """
    input_peer = await client.get_input_entity(peer)

    async def _fetch_page(page_offset: str, limit: int) -> SavedStarGifts:
        return cast(
            SavedStarGifts,
            await client(
                functions.payments.GetSavedStarGiftsRequest(
                    peer=input_peer,
                    offset=page_offset,
                    limit=limit,
                    exclude_unsaved=exclude_unsaved,
                )
            ),
        )

    if max_pages == 1:
        return await _fetch_page(offset, page_limit)

    merged_gifts: list[Any] = []
    merged_chats: dict[int, Any] = {}
    merged_users: dict[int, Any] = {}
    total_count = 0
    chat_notifications_enabled: bool | None = None
    page_offset = offset
    pages_fetched = 0

    while (max_pages is None or pages_fetched < max_pages) and len(merged_gifts) < max_items:
        limit = min(page_limit, max_items - len(merged_gifts))
        page = await _fetch_page(page_offset, limit)
        pages_fetched += 1
        total_count = page.count
        if chat_notifications_enabled is None:
            chat_notifications_enabled = page.chat_notifications_enabled

        page_gifts = page.gifts or []
        if not page_gifts:
            break

        for gift in page_gifts:
            merged_gifts.append(gift)
            if len(merged_gifts) >= max_items:
                break

        for chat in page.chats or []:
            merged_chats[chat.id] = chat
        for user in page.users or []:
            merged_users[user.id] = user

        next_offset = page.next_offset
        if not next_offset or len(page_gifts) < limit:
            break
        page_offset = next_offset

    return SavedStarGifts(
        count=total_count,
        gifts=merged_gifts,
        chats=list(merged_chats.values()),
        users=list(merged_users.values()),
        chat_notifications_enabled=chat_notifications_enabled,
        next_offset=None,
    )
