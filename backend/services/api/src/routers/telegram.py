import logging
import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from db.schemas.general.primitives import PeerTypeSchema
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from telegram.api_session_pool import ApiSessionPool, PooledClientLease
from telegram.community_bots import bot_external_ids_from_bot_info
from telegram.harvester_peer_access import deactivate_harvester_if_account_frozen
from telegram.refresh_peer import (
    community_access_probe_due,
    community_profile_needs_refresh,
    probe_inaccessible_community,
    refresh_community_if_stale,
)
from db.crud.telegram.unresolved_peer import ensure_web_peer_stub
from telegram.first_message import (
    ensure_peer_first_message_at,
    resolve_community_entity_for_history,
)

from services.api.src.usage import Meter, current_usage, metered
from services.api.src.entitlements import (
    assert_meter_allowed,
    current_plan_id_and_limits,
    current_plan_limits,
    raise_history_not_in_plan,
)
from billing.windows import clamp_tip_sync_limit, free_history_sync_forbidden, page_is_past_window
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase import get_conn
from services.api.src.peer_access import connected_peer_role
from services.api.src.peer_language import PeerLanguageResponse, peer_language_status
from services.api.src.client_errors import rate_limited_retry
from services.api.src.crud.supabase.messages import (
    get_peer_message_by_message_id,
    list_peer_media_messages,
    list_peer_messages,
    locate_peer_message_page,
    peer_media_id_floor,
    peer_message_id_floor,
)
from services.api.src.crud.supabase.peer_rows import community_detail_from_peer
from services.api.src.crud.s3.media import (
    presign_community_detail_media,
    presign_community_view,
    presign_community_views,
    presign_keyword_peers_by_id,
    presign_user_partials_by_id,
)
from services.api.src.forward_source_resolve import seed_community_from_stored_forward
from services.api.src.crud.supabase.community_insights import (
    build_community_insights,
    count_peer_recent_volumes,
    list_peer_participants,
    list_similar_channels,
    peer_activity_volume_counts,
)
from services.api.src.crud.supabase.peers import (
    field_history_payload,
    get_partial_communities_by_external_ids,
    get_partial_telegram_users_by_external_ids,
    get_peer_by_id,
    get_peer_refresh_row,
    get_user_peer_affiliation,
    list_user_affiliated_peers,
    list_user_peer_affiliation_messages,
    list_user_peer_affiliation_reactions,
    load_community_bots,
    search_telegram_peers,
)
from db.crud.telegram.field_history import list_field_history_for_peer
from services.api.src.models.community_insights import (
    CommunityInsightsResponse,
    ParticipantsPage,
    SimilarChannelsPage,
)
from services.api.src.models.telegram import (
    CommunityDetailResponse,
    CompletePeerHistoryResponse,
    CoverageIntervalView,
    PeerHistoryCoverageResponse,
    PeerInventoryCountsResponse,
    PeerMediaKindCounts,
    PeerMessageLocateResponse,
    PeerMessagesResponse,
)
from services.api.src.models.message_media import MessageMediaPreview, MessageMediaResponse
from services.api.src.message_media import get_or_fetch_message_media, build_message_media_preview
from aws.s3 import presign_s3_uris
from telegram.media import harvest_display_s3_uri
from telegram.media_kinds import parse_slim_media
from services.api.src.message_peer_partials import (
    community_external_ids_from_messages,
    resolve_missing_community_peers,
    user_external_ids_from_messages,
)
from services.api.src.forward_source_resolve import enrich_forward_sources
from services.api.src.search_limits import PEER_QUERY_MAX_LENGTH
from services.api.src.sync_peer_messages import (
    fetch_and_refresh_recent_messages,
    fetch_and_upsert_latest_messages,
    fetch_and_upsert_messages_by_id,
    fetch_and_upsert_older_messages,
)
from services.api.src.on_demand_coverage import (
    record_on_demand_message_fetch_coverage,
    record_on_demand_older_message_fetch_coverage,
    record_on_demand_recent_window_coverage,
)
from services.api.src.message_history_coverage import (
    gated_total_count,
    is_history_complete,
    is_recent_window_complete,
    list_peer_message_coverage,
    peer_history_coverage_report,
    peer_media_inventory_counts,
    peer_message_history_complete,
    peer_message_inventory_count,
)
from services.api.src.auth.deps import get_current_user
from services.api.src.auth.models import PlatformUser
from db.crud.system.jobs import enqueue_job
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordPeerView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.crud.supabase.views.telegram.peer_search import TelegramPeerSearchView
from services.api.src.crud.supabase.views.telegram.community import (
    TelegramChannelLinkView,
    TelegramChatView,
    TelegramMegagroupLinkView,
)
from services.api.src.crud.supabase.views.telegram.field_history import FieldHistoryEntryView
from services.api.src.crud.supabase.views.telegram.user import (
    UserAffiliatedPeersPage,
    UserPeerAffiliationDetailView,
    UserPeerAffiliationMessagesPage,
    UserPeerAffiliationReactionsPage,
    UserPeerDetailView,
)
from services.api.src.affiliated_peer_resolve import fill_affiliated_peers
from services.api.src.user_page_load import UserRefreshMode, load_user_page
from services.api.src.linked_peer_resolve import resolve_linked_community

lg = logging.getLogger(__name__)

router = APIRouter()


def _telegram_harvester_configured(cfg: object) -> bool:
    return getattr(cfg, "telegram_api_id", 0) > 0 and bool(getattr(cfg, "telegram_api_hash", "").strip())


def _community_on_demand_skip_reason(
    detail: TelegramChannelLinkView | TelegramMegagroupLinkView | TelegramChatView,
) -> str | None:
    """Why heavier Telegram on-demand work (history, sync, linked) must not run.

    ``unavailable_at`` means the peer is gone. ``access_error_code`` (e.g.
    CHANNEL_PRIVATE) means a harvester already failed to access it. Page-load
    still TTL-probes with GetChannels; this flag skips GetHistory / photos /
    message sync until that probe recovers access.
    """
    if getattr(detail, "unavailable_at", None) is not None:
        return "unavailable_at is set"
    access_error = getattr(detail, "access_error_code", None)
    if access_error:
        return f"access_error_code={access_error}"
    return None


def _community_refresh_username(
    detail: TelegramChannelLinkView | TelegramMegagroupLinkView | TelegramChatView,
) -> str | None:
    if isinstance(detail, TelegramChatView):
        return None
    primary = (detail.username or "").strip()
    if primary:
        return primary
    for entry in detail.usernames or []:
        if getattr(entry, "active", None) is False:
            continue
        candidate = getattr(entry, "username", None)
        if candidate is None:
            continue
        username = str(candidate).strip()
        if username:
            return username
    return None


def _get_telegram_pool(request: Request) -> ApiSessionPool | None:
    pool = getattr(request.app.state, "telegram_pool", None)
    return pool if isinstance(pool, ApiSessionPool) else None


async def _pool_client_for_peer_role(
    request: Request,
    conn: Any,
    *,
    harvester_id: UUID | None,
    access_hash: int | None,
    exclude_external_ids: Collection[str] | None = None,
    peer_external_id: str | None = None,
    refresh_row: dict[str, Any] | None = None,
) -> tuple[TelegramClient | None, UUID | None, int | None]:
    """Pick a Telethon client for ``access_hash`` without marking occupancy.

    When the owning session cannot connect, try another harvester that stores
    its own access hash for ``peer_external_id``.
    """
    pool = _get_telegram_pool(request)
    if pool is None:
        return None, harvester_id, access_hash if access_hash is not None else None
    client, resolved_id, resolved_hash = await pool.get_client_for_peer_role(
        conn,
        harvester_id=harvester_id,
        access_hash=access_hash,
        exclude_external_ids=exclude_external_ids,
    )
    if client is not None or not peer_external_id:
        return client, resolved_id, resolved_hash
    skip = {harvester_id} if harvester_id is not None else set()
    alternate = await connected_peer_role(
        pool,
        conn,
        peer_external_id=peer_external_id,
        skip_harvester_ids=skip,
    )
    if alternate is None:
        return client, resolved_id, resolved_hash
    alt_client, alt_id, alt_hash, alt_external_id = alternate
    if refresh_row is not None:
        refresh_row["harvester_id"] = alt_id
        refresh_row["access_hash"] = alt_hash
        if alt_external_id is not None:
            refresh_row["harvester_external_id"] = alt_external_id
    return alt_client, alt_id, alt_hash


@asynccontextmanager
async def _acquire_pool_client_for_peer_role(
    request: Request,
    conn: Any,
    *,
    harvester_id: UUID | None,
    access_hash: int | None,
    exclude_external_ids: Collection[str] | None = None,
    peer_external_id: str | None = None,
    refresh_row: dict[str, Any] | None = None,
) -> AsyncIterator[tuple[TelegramClient | None, UUID | None, int | None]]:
    """Pick a client and hold in-flight occupancy until the context exits."""
    pool = _get_telegram_pool(request)
    client, resolved_id, resolved_hash = await _pool_client_for_peer_role(
        request,
        conn,
        harvester_id=harvester_id,
        access_hash=access_hash,
        exclude_external_ids=exclude_external_ids,
        peer_external_id=peer_external_id,
        refresh_row=refresh_row,
    )
    if pool is None or client is None or resolved_id is None:
        yield client, resolved_id, resolved_hash
        return
    async with pool.acquire_client_for_peer_role(
        conn,
        harvester_id=resolved_id,
        access_hash=resolved_hash,
    ) as result:
        yield result


async def _sender_partials_for_messages(
    conn: Any,
    messages: list[TelegramMessageView],
    *,
    presign: bool = True,
) -> dict[str, UserPartialView]:
    """Batch-load + optionally presign user partials for senders and action targets."""
    sender_ids = user_external_ids_from_messages(messages)
    if not sender_ids:
        return {}
    users = await get_partial_telegram_users_by_external_ids(conn, sender_ids)
    if not presign:
        return users
    region = ConfigFactory.get_config().aws_region.strip()
    if not region:
        return users
    return await presign_user_partials_by_id(users, region=region)


async def _presign_community_insights(
    insights: CommunityInsightsResponse,
    *,
    region: str,
) -> CommunityInsightsResponse:
    if not region.strip():
        return insights

    related_peers = [item.peer for item in insights.related_channels if item.peer is not None]
    forward_peers = [item.peer for item in insights.forward_sources if item.peer is not None]
    forwarded_by_peers = [item.peer for item in insights.forwarded_by if item.peer is not None]
    participant_users = {
        str(i): item.user for i, item in enumerate(insights.activity.participants) if item.user is not None
    }
    participant_peers = {
        str(i): item.peer for i, item in enumerate(insights.activity.participants) if item.peer is not None
    }

    (
        related_signed,
        forward_signed,
        forwarded_by_signed,
        users_signed,
        peers_signed,
    ) = await asyncio.gather(
        presign_community_views(related_peers, region=region),
        presign_keyword_peers_by_id(
            {p.external_id: p for p in forward_peers},
            region=region,
        ),
        presign_keyword_peers_by_id(
            {p.external_id: p for p in forwarded_by_peers},
            region=region,
        ),
        presign_user_partials_by_id(participant_users, region=region),
        presign_keyword_peers_by_id(participant_peers, region=region),
    )
    related_by_eid = {p.external_id: p for p in related_signed}

    related = []
    for item in insights.related_channels:
        if item.peer is None:
            related.append(item)
            continue
        peer = related_by_eid.get(item.peer.external_id, item.peer)
        related.append(item.model_copy(update={"peer": peer}))

    forwards = []
    for item in insights.forward_sources:
        if item.peer is None:
            forwards.append(item)
            continue
        peer = forward_signed.get(item.peer.external_id, item.peer)
        forwards.append(item.model_copy(update={"peer": peer}))

    forwarded_by = []
    for item in insights.forwarded_by:
        if item.peer is None:
            forwarded_by.append(item)
            continue
        peer = forwarded_by_signed.get(item.peer.external_id, item.peer)
        forwarded_by.append(item.model_copy(update={"peer": peer}))

    participants = []
    for i, item in enumerate(insights.activity.participants):
        key = str(i)
        updates: dict[str, Any] = {}
        if key in users_signed:
            updates["user"] = users_signed[key]
        if key in peers_signed:
            updates["peer"] = peers_signed[key]
        participants.append(item.model_copy(update=updates) if updates else item)

    return insights.model_copy(
        update={
            "related_channels": related,
            "forward_sources": forwards,
            "forwarded_by": forwarded_by,
            "activity": insights.activity.model_copy(update={"participants": participants}),
        }
    )


async def _presign_participants_page(
    page: ParticipantsPage,
    *,
    region: str,
) -> ParticipantsPage:
    if not region.strip():
        return page
    users = {str(i): item.user for i, item in enumerate(page.items) if item.user is not None}
    peers = {str(i): item.peer for i, item in enumerate(page.items) if item.peer is not None}
    users_signed, peers_signed = await asyncio.gather(
        presign_user_partials_by_id(users, region=region),
        presign_keyword_peers_by_id(peers, region=region),
    )
    items = []
    for i, item in enumerate(page.items):
        key = str(i)
        updates: dict[str, Any] = {}
        if key in users_signed:
            updates["user"] = users_signed[key]
        if key in peers_signed:
            updates["peer"] = peers_signed[key]
        items.append(item.model_copy(update=updates) if updates else item)
    return page.model_copy(update={"items": items})


async def _community_partials_for_messages(
    conn: Any,
    messages: list[TelegramMessageView],
    *,
    client: TelegramClient | None = None,
    harvester_id: UUID | None = None,
    presign: bool = True,
) -> dict[str, KeywordPeerView]:
    """Load community peers for channel senders / forwards; optionally resolve missing."""
    ids = community_external_ids_from_messages(messages)
    if not ids:
        return {}

    peers = await get_partial_communities_by_external_ids(conn, ids)
    missing = [eid for eid in ids if eid not in peers]
    if missing and client is not None and harvester_id is not None:
        cfg = ConfigFactory.get_config()
        try:
            await resolve_missing_community_peers(
                client,
                conn,
                missing_external_ids=missing,
                harvester_id=harvester_id,
                s3_bucket=cfg.harvest_s3_bucket,
                s3_region=cfg.aws_region,
            )
            peers = await get_partial_communities_by_external_ids(conn, ids)
        except FloodWaitError:
            raise
        except Exception:
            lg.exception("Failed resolving missing community peers for chat messages")

    if not presign:
        return peers
    region = ConfigFactory.get_config().aws_region.strip()
    if not region:
        return peers
    return await presign_keyword_peers_by_id(peers, region=region)


async def _with_media_previews(
    messages: list[TelegramMessageView],
    *,
    redact_below_message_id: int | None = None,
) -> list[TelegramMessageView]:
    """Attach lightweight media_preview (presign-ready / pending) like search results.

    When ``redact_below_message_id`` is set (Free unlock window), visual media older
    than that id keep ``thumb_url`` but never receive a full S3 ``url``.

    Presigns are batched through a single S3 client — list pages used to open one
    client per row, which dominated Media-tab latency on large peers.
    """
    if not messages:
        return messages
    region = ConfigFactory.get_config().aws_region.strip()

    # Metadata only first (no per-row S3 clients).
    previews = await asyncio.gather(
        *[build_message_media_preview(m.media, region=region, include_url=False) for m in messages]
    )

    need_idx: list[int] = []
    uris: list[str] = []
    for i, (message, preview) in enumerate(zip(messages, previews, strict=True)):
        if preview.status == "none":
            continue
        if preview.kind in ("document", "audio", "webpage"):
            continue
        if (
            redact_below_message_id is not None
            and preview.kind in ("image", "gif", "video")
            and message.message_id < redact_below_message_id
        ):
            continue
        slim = parse_slim_media(message.media)
        s3_uri = harvest_display_s3_uri(slim) if slim is not None else None
        if s3_uri and region:
            need_idx.append(i)
            uris.append(s3_uri)

    urls_by_idx: dict[int, str] = {}
    if uris and region:
        try:
            urls = await presign_s3_uris(uris, region=region)
            urls_by_idx = dict(zip(need_idx, urls, strict=True))
        except Exception:
            lg.exception("Failed to batch-presign %s media preview URLs", len(uris))

    out: list[TelegramMessageView] = []
    for i, (message, preview) in enumerate(zip(messages, previews, strict=True)):
        if preview.status == "none":
            out.append(message.model_copy(update={"media_preview": None}))
            continue
        url = urls_by_idx.get(i)
        if url:
            preview = preview.model_copy(update={"url": url, "status": "ready"})
        elif (
            redact_below_message_id is not None
            and preview.kind in ("image", "gif", "video")
            and message.message_id < redact_below_message_id
        ):
            # Explicit lock: ready metadata + thumb, no full URL.
            preview = preview.model_copy(update={"url": None})
        out.append(message.model_copy(update={"media_preview": preview}))
    return out


async def _free_media_unlock_floor(
    conn: Any,
    *,
    request: Request,
    external_id: str,
    limits: dict[str, Any] | None = None,
) -> int | None:
    """Message-id floor for Free visual unlock; None means no redaction/harvest cap."""
    plan_limits = limits if limits is not None else await current_plan_limits(request)
    if bool(plan_limits.get("media_download")):
        return None
    unlocked = plan_limits.get("media_unlocked_count")
    if not isinstance(unlocked, int) or unlocked <= 0:
        return None
    return await peer_media_id_floor(conn, external_id, unlocked, kind="visual")


async def _chat_window_floor(
    conn: Any,
    *,
    request: Request,
    external_id: str,
    limits: dict[str, Any] | None = None,
) -> tuple[int | None, bool]:
    """Return (min_message_id, upgrade_for_history) for the caller's plan."""
    plan_limits = limits if limits is not None else await current_plan_limits(request)
    window = plan_limits.get("chat_message_window")
    if window is None:
        return None, False
    floor = await peer_message_id_floor(conn, external_id, int(window))
    return floor, True


def _field_history_views(history: list) -> list[FieldHistoryEntryView]:
    if not history:
        return []
    first = history[0]
    if hasattr(first, "field_key") and hasattr(first, "recorded_at"):
        return [
            FieldHistoryEntryView(
                field_key=entry.field_key,
                old_value=entry.old_value,
                recorded_at=entry.recorded_at,
            )
            for entry in history
        ]
    payload = field_history_payload(history)
    return [FieldHistoryEntryView.model_validate(item) for item in payload]


@router.get("/peers", response_model=list[TelegramPeerSearchView])
async def get_telegram_peers(
    q: str = Query(
        "",
        max_length=PEER_QUERY_MAX_LENGTH,
        description="Search channels and chats by title or username",
    ),
    limit: int = Query(20, ge=1, le=50),
) -> list[TelegramPeerSearchView]:
    """Search channels and chats from the database for adding as graph data sources."""
    async with get_conn() as conn:
        try:
            return await search_telegram_peers(conn, q, limit=limit)
        except Exception as e:
            lg.exception(f"Failed to search telegram peers: {e}")
            return []


async def _bill_live_peer_resolve(request: Request, *, kind: str) -> None:
    """Record a live Telegram peer resolve. Cache hits stay omitted."""
    await assert_meter_allowed(request, Meter.TELEGRAM_RESOLVE, check_plan=True)
    note = current_usage(request)
    if note is None:
        return
    note.omit = False
    note.params["kind"] = kind
    note.result_count = 1
    note.units["telegram_calls"] = int(note.units.get("telegram_calls", 0)) + 1


@router.get("/peers/{external_id}", response_model=CommunityDetailResponse)
@metered(Meter.TELEGRAM_RESOLVE, enforce=False)
async def fetch_peer(
    external_id: str,
    request: Request,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
    via_external_id: str | None = Query(
        None,
        description="Optional community that forwarded this peer (scopes forward-seed lookup)",
    ),
    via_peer_type: PeerTypeSchema | None = Query(
        None,
        description="Peer type for via_external_id",
    ),
) -> CommunityDetailResponse:
    cfg = ConfigFactory.get_config()
    # Saved profile reads are not usage. The materialize "Refreshed channel" path
    # is a live Telegram resolve and is recorded below.
    note = current_usage(request)
    if note is not None:
        note.omit = True

    """Fetch a single peer by id and type. Returns peer detail (channel or chat) and linked_peer when present."""
    async with get_conn() as conn:
        detail = await get_peer_by_id(conn, external_id, peer_type)

        if detail is None:
            raise HTTPException(status_code=404, detail="Peer not found")

        refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
        needs_refresh = refresh_row is not None and community_profile_needs_refresh(
            refresh_row.get("profile_tier"),
            refresh_row["updated_at"],
            cfg.peer_profile_refresh_ttl_minutes,
        )

        skip_reason = _community_on_demand_skip_reason(detail)
        ttl_minutes = cfg.peer_profile_refresh_ttl_minutes
        should_probe = bool(skip_reason) and community_access_probe_due(
            getattr(detail, "access_checked_at", None),
            ttl_minutes,
        )
        if skip_reason and not should_probe:
            lg.info(
                "Community on-demand Telegram skipped external_id=%s: %s (access check within TTL)",
                external_id,
                skip_reason,
            )

        client: TelegramClient | None = None
        harvester_id = refresh_row.get("harvester_id") if refresh_row else None
        if harvester_id is not None and not isinstance(harvester_id, UUID):
            harvester_id = UUID(str(harvester_id))
        access_hash = refresh_row["access_hash"] if refresh_row else None
        linked_peer = getattr(detail, "linked_peer", None)

        lease = PooledClientLease(_get_telegram_pool(request))
        try:

            async def _ensure_client() -> tuple[TelegramClient | None, Any, int | None]:
                nonlocal client, harvester_id, access_hash
                if client is not None and harvester_id is not None:
                    return client, harvester_id, access_hash
                client, harvester_id, access_hash = await _pool_client_for_peer_role(
                    request,
                    conn,
                    harvester_id=harvester_id,
                    access_hash=access_hash,
                    peer_external_id=external_id,
                    refresh_row=refresh_row,
                )
                lease.bind(client, harvester_id, access_hash)
                return client, harvester_id, access_hash

            if (should_probe or (not skip_reason and needs_refresh)) and _telegram_harvester_configured(cfg):
                client, harvester_id, access_hash = await _ensure_client()

                if client is None or harvester_id is None:
                    lg.warning(
                        "Community profile refresh skipped external_id=%s: no Telethon session available",
                        external_id,
                    )
                elif should_probe:
                    await _bill_live_peer_resolve(request, kind="access_probe")
                    try:
                        await probe_inaccessible_community(
                            client,
                            conn,
                            external_id=external_id,
                            peer_type=peer_type,
                            access_hash=access_hash,
                            harvester_id=harvester_id,
                            username=_community_refresh_username(detail),
                            s3_bucket=cfg.harvest_s3_bucket,
                            s3_region=cfg.aws_region,
                        )
                    except FloodWaitError as exc:
                        lg.info(
                            "Community access probe FloodWait external_id=%s seconds=%s",
                            external_id,
                            exc.seconds,
                        )
                    except Exception:
                        lg.exception(
                            "Community access probe failed external_id=%s peer_type=%s",
                            external_id,
                            peer_type,
                        )
                    await conn.commit()
                    detail = await get_peer_by_id(conn, external_id, peer_type)
                    if detail is None:
                        raise HTTPException(status_code=404, detail="Peer not found")
                    refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
                    skip_reason = _community_on_demand_skip_reason(detail)
                else:
                    try:
                        result = await refresh_community_if_stale(
                            client,
                            conn,
                            external_id=external_id,
                            peer_type=peer_type,
                            access_hash=access_hash,
                            updated_at=refresh_row["updated_at"] if refresh_row else None,
                            ttl_minutes=cfg.peer_profile_refresh_ttl_minutes,
                            harvester_id=harvester_id,
                            profile_tier=refresh_row.get("profile_tier") if refresh_row else None,
                            username=_community_refresh_username(detail),
                            s3_bucket=cfg.harvest_s3_bucket,
                            s3_region=cfg.aws_region,
                        )
                        if result.refreshed:
                            await _bill_live_peer_resolve(request, kind="peer_profile")
                        # Private forward-source stubs often lack access_hash; re-fetch a
                        # stored forward from a known via peer (indexed by source_external_id).
                        if (
                            not result.refreshed
                            and peer_type in ("channel", "megagroup")
                            and access_hash is None
                            and via_external_id
                            and via_peer_type in ("channel", "megagroup", "chat")
                            and result.unresolvable
                        ):
                            try:
                                seeded = await seed_community_from_stored_forward(
                                    client,
                                    conn,
                                    target_external_id=external_id,
                                    via_external_id=via_external_id,
                                    via_peer_type=via_peer_type,
                                    harvester_id=harvester_id,
                                    s3_bucket=cfg.harvest_s3_bucket,
                                    s3_region=cfg.aws_region,
                                )
                            except FloodWaitError:
                                raise
                            except Exception:
                                lg.exception(
                                    "Forward-seed resolve failed external_id=%s via=%s",
                                    external_id,
                                    via_external_id,
                                )
                                seeded = False
                            if seeded:
                                await _bill_live_peer_resolve(request, kind="forward_seed")
                                refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
                                role_hid = refresh_row.get("harvester_id") if refresh_row else None
                                if role_hid is not None and not isinstance(role_hid, UUID):
                                    role_hid = UUID(str(role_hid))
                                client, harvester_id, access_hash = await _pool_client_for_peer_role(
                                    request,
                                    conn,
                                    harvester_id=role_hid,
                                    access_hash=(refresh_row["access_hash"] if refresh_row else None),
                                    peer_external_id=external_id,
                                    refresh_row=refresh_row,
                                )
                                lease.bind(client, harvester_id, access_hash)
                                if client is None or harvester_id is None:
                                    lg.warning(
                                        "Community profile refresh skipped after seed "
                                        "external_id=%s: owning harvester session unavailable",
                                        external_id,
                                    )
                                else:
                                    result = await refresh_community_if_stale(
                                        client,
                                        conn,
                                        external_id=external_id,
                                        peer_type=peer_type,
                                        access_hash=access_hash,
                                        updated_at=(refresh_row["updated_at"] if refresh_row else None),
                                        ttl_minutes=cfg.peer_profile_refresh_ttl_minutes,
                                        harvester_id=harvester_id,
                                        profile_tier=(refresh_row.get("profile_tier") if refresh_row else None),
                                        username=_community_refresh_username(detail),
                                        s3_bucket=cfg.harvest_s3_bucket,
                                        s3_region=cfg.aws_region,
                                    )
                                    if result.refreshed:
                                        await _bill_live_peer_resolve(request, kind="peer_profile")
                        if (
                            not result.refreshed
                            and result.unresolvable
                            and peer_type in ("channel", "megagroup", "chat")
                        ):
                            await ensure_web_peer_stub(
                                conn,
                                external_id=external_id,
                                peer_type=peer_type,
                                title=getattr(detail, "title", None),
                                harvester_id=harvester_id,
                                mark_unavailable=True,
                            )
                            lg.info(
                                "Marked peer unavailable after exhausted resolve external_id=%s peer_type=%s",
                                external_id,
                                peer_type,
                            )
                        if result.warnings:
                            lg.warning(
                                "Community profile refresh warnings external_id=%s: %s",
                                external_id,
                                result.warnings,
                            )
                    except Exception:
                        lg.exception(
                            "Community profile refresh failed external_id=%s peer_type=%s",
                            external_id,
                            peer_type,
                        )

                    detail = await get_peer_by_id(conn, external_id, peer_type)
                    if detail is None:
                        raise HTTPException(status_code=404, detail="Peer not found")
                    refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
                    skip_reason = _community_on_demand_skip_reason(detail)
                    if skip_reason:
                        lg.info(
                            "Community on-demand Telegram skipped after refresh external_id=%s: %s",
                            external_id,
                            skip_reason,
                        )
            elif should_probe or (not skip_reason and needs_refresh):
                lg.info(
                    "Community profile refresh skipped external_id=%s: harvester not configured",
                    external_id,
                )

            # Oldest-accessible message date for "Since" (peer column, not insights).
            if (
                not skip_reason
                and getattr(detail, "first_message_at", None) is None
                and peer_type in ("channel", "megagroup", "chat")
                and _telegram_harvester_configured(cfg)
            ):
                client, harvester_id, access_hash = await _ensure_client()
                if client is not None and harvester_id is not None:
                    try:
                        entity = await resolve_community_entity_for_history(
                            client,
                            external_id=external_id,
                            peer_type=peer_type,
                            access_hash=access_hash,
                            username=_community_refresh_username(detail),
                        )
                        if entity is not None:
                            await ensure_peer_first_message_at(
                                client,
                                conn,
                                entity,
                                external_id=external_id,
                                peer_type=peer_type,
                            )
                            await conn.commit()
                            detail = await get_peer_by_id(conn, external_id, peer_type)
                            if detail is None:
                                raise HTTPException(status_code=404, detail="Peer not found")
                    except FloodWaitError as exc:
                        lg.info(
                            "first_message_at FloodWait external_id=%s seconds=%s",
                            external_id,
                            exc.seconds,
                        )
                    except Exception:
                        lg.exception(
                            "first_message_at fill failed external_id=%s peer_type=%s",
                            external_id,
                            peer_type,
                        )

            linked_chat_id = getattr(detail, "linked_chat_id", None)
            linked_peer = getattr(detail, "linked_peer", None)
            if (
                not skip_reason
                and linked_chat_id is not None
                and linked_peer is None
                and peer_type in ("channel", "megagroup")
                and _telegram_harvester_configured(cfg)
            ):
                linked_peer_type: PeerTypeSchema = "megagroup" if peer_type == "channel" else "channel"
                client, harvester_id, access_hash = await _ensure_client()
                if client is not None and harvester_id is not None:
                    try:
                        linked_peer = await resolve_linked_community(
                            client,
                            conn,
                            parent_external_id=external_id,
                            parent_peer_type=peer_type,
                            parent_access_hash=access_hash,
                            linked_chat_id=int(linked_chat_id),
                            linked_peer_type=linked_peer_type,
                            harvester_id=harvester_id,
                            s3_bucket=cfg.harvest_s3_bucket,
                            s3_region=cfg.aws_region,
                        )
                    except Exception:
                        lg.exception(
                            "Linked peer resolve failed parent=%s linked_chat_id=%s",
                            external_id,
                            linked_chat_id,
                        )
                        linked_peer = None

        finally:
            lease.release()

        bot_ids = bot_external_ids_from_bot_info(
            None if isinstance(detail, TelegramChatView) else getattr(detail, "bot_info", None),
        )
        bots = await load_community_bots(conn, bot_ids)

        history = await list_field_history_for_peer(
            conn,
            external_id,
            peer_type=peer_type,
        )
        field_history = _field_history_views(history)

    response = community_detail_from_peer(detail, field_history=field_history)
    response = response.model_copy(update={"bots": bots, "linked_peer": linked_peer if peer_type != "chat" else None})
    return await presign_community_detail_media(response, region=cfg.aws_region)


@router.get("/peers/{external_id}/language", response_model=PeerLanguageResponse)
async def fetch_peer_language(
    external_id: str,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
) -> PeerLanguageResponse:
    """Stored content language. Missing channel/megagroup languages are filled in the background."""
    return await peer_language_status(external_id, peer_type)


@router.get("/peers/{external_id}/insights", response_model=CommunityInsightsResponse)
async def fetch_peer_insights(
    external_id: str,
    request: Request,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
    forwards_limit: int = Query(
        5,
        ge=1,
        le=20,
        description="Max forward-source / forwarded-by rows (preview 5, see-more 20)",
    ),
    participants_limit: int = Query(
        5,
        ge=1,
        le=20,
        description="Max participant rows (preview 5)",
    ),
    posters_limit: int | None = Query(
        None,
        ge=1,
        le=20,
        description="Deprecated alias for participants_limit",
    ),
    viewed_limit: int = Query(
        3,
        ge=1,
        le=20,
        description="Max top-viewed rows (preview 3, see-more 20)",
    ),
) -> CommunityInsightsResponse:
    """Overview analytics: related channels, forward neighbors, activity snapshot.

    Related channels come from harvested ``similar_channels``; forward sources
    and forwarded-by communities from ``telegram_forward_edges`` (one query);
    activity from ``telegram_messages``.
    Missing forward-source peers are best-effort resolved via Telethon before
    the response is returned.
    """
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Unsupported peer_type")

    cfg = ConfigFactory.get_config()
    async with get_conn() as conn:
        detail = await get_peer_by_id(conn, external_id, peer_type)
        if detail is None:
            raise HTTPException(status_code=404, detail="Peer not found")
        insights = await build_community_insights(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            forwards_limit=forwards_limit,
            participants_limit=participants_limit,
            posters_limit=posters_limit,
            viewed_limit=viewed_limit,
        )

        missing = [s for s in insights.forward_sources if s.peer is None]
        skip_reason = _community_on_demand_skip_reason(detail)
        if missing and skip_reason:
            lg.info(
                "Forward-source enrich skipped external_id=%s: %s",
                external_id,
                skip_reason,
            )
        elif missing and _telegram_harvester_configured(cfg):
            refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
            role_hid = refresh_row.get("harvester_id") if refresh_row else None
            if role_hid is not None and not isinstance(role_hid, UUID):
                role_hid = UUID(str(role_hid))
            async with _acquire_pool_client_for_peer_role(
                request,
                conn,
                harvester_id=role_hid,
                access_hash=refresh_row["access_hash"] if refresh_row else None,
                peer_external_id=external_id,
                refresh_row=refresh_row,
            ) as (client, hid, via_access_hash):
                if client is not None and hid is not None:
                    try:
                        forwards = await enrich_forward_sources(
                            client,
                            conn,
                            via_external_id=external_id,
                            via_peer_type=peer_type,
                            sources=insights.forward_sources,
                            harvester_id=hid,
                            via_access_hash=via_access_hash,
                            via_harvester_external_id=(
                                refresh_row.get("harvester_external_id") if refresh_row else None
                            ),
                            s3_bucket=cfg.harvest_s3_bucket,
                            s3_region=cfg.aws_region,
                        )
                        insights = insights.model_copy(update={"forward_sources": forwards})
                    except FloodWaitError as exc:
                        lg.info(
                            "Forward-source enrich FloodWait external_id=%s seconds=%s",
                            external_id,
                            exc.seconds,
                        )
                    except Exception:
                        lg.exception(
                            "Forward-source enrich failed external_id=%s",
                            external_id,
                        )
                else:
                    lg.warning(
                        "Forward-source enrich skipped external_id=%s: no Telethon session",
                        external_id,
                    )

    return await _presign_community_insights(insights, region=cfg.aws_region)


@router.get(
    "/peers/{external_id}/similar-channels",
    response_model=SimilarChannelsPage,
)
async def fetch_peer_similar_channels(
    external_id: str,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = Query(
        None,
        max_length=100,
        description="Optional title / username / id filter",
    ),
) -> SimilarChannelsPage:
    """Paginated Telegram similar-channel recommendations for a community."""
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Unsupported peer_type")

    cfg = ConfigFactory.get_config()
    async with get_conn() as conn:
        detail = await get_peer_by_id(conn, external_id, peer_type)
        if detail is None:
            raise HTTPException(status_code=404, detail="Peer not found")
        page = await list_similar_channels(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            limit=limit,
            offset=offset,
            q=q,
        )

    if not cfg.aws_region.strip() or not page.items:
        return page
    items = await presign_community_views(page.items, region=cfg.aws_region)
    return page.model_copy(update={"items": items})


@router.get(
    "/peers/{external_id}/participants",
    response_model=ParticipantsPage,
)
async def fetch_peer_participants(
    external_id: str,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = Query(
        None,
        max_length=100,
        description="Optional name / username / id filter",
    ),
) -> ParticipantsPage:
    """Paginated participants sorted by all-time harvested post count.

    Uses harvested membership when available; otherwise message senders.
    """
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Unsupported peer_type")

    cfg = ConfigFactory.get_config()
    async with get_conn() as conn:
        detail = await get_peer_by_id(conn, external_id, peer_type)
        if detail is None:
            raise HTTPException(status_code=404, detail="Peer not found")
        page = await list_peer_participants(
            conn,
            external_id=external_id,
            peer_type=peer_type,
            limit=limit,
            offset=offset,
            q=q,
        )

    return await _presign_participants_page(page, region=cfg.aws_region)


@router.get("/users/{user_id}", response_model=UserPeerDetailView)
async def fetch_user(
    user_id: UUID,
    request: Request,
    refresh: UserRefreshMode = Query(
        "none",
        description=(
            "Load slice: none (header), history, db (affiliated snapshot), profile, stories, affiliated, or all."
        ),
    ),
) -> UserPeerDetailView:
    return await load_user_page(user_id, request, refresh=refresh)


@router.get("/users/{user_id}/affiliated-peers", response_model=UserAffiliatedPeersPage)
async def fetch_user_affiliated_peers(
    user_id: UUID,
    request: Request,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = Query(
        None,
        max_length=100,
        description="Optional title / username / id filter",
    ),
) -> UserAffiliatedPeersPage:
    user_id_str = str(user_id)
    cfg = ConfigFactory.get_config()

    async with get_conn() as conn:
        page = await list_user_affiliated_peers(conn, user_id_str, limit=limit, offset=offset, q=q)
        if page is None:
            raise HTTPException(status_code=404, detail="User not found")

        if _telegram_harvester_configured(cfg) and any(item.peer.is_unknown for item in page.items):
            try:
                filled = await fill_affiliated_peers(
                    _get_telegram_pool(request),
                    conn,
                    page.items,
                    s3_bucket=cfg.harvest_s3_bucket,
                    s3_region=cfg.aws_region,
                )
                page = page.model_copy(update={"items": filled})
            except Exception:
                lg.exception(
                    "Affiliated peer page resolve failed for user_id=%s",
                    user_id_str,
                )

    if cfg.aws_region:
        peers = await presign_community_views(
            [entry.peer for entry in page.items],
            region=cfg.aws_region,
        )
        items = [entry.model_copy(update={"peer": peer}) for entry, peer in zip(page.items, peers, strict=True)]
        page = page.model_copy(update={"items": items})
    return page


@router.get(
    "/users/{user_id}/peers/{peer_type}/{peer_external_id}/affiliation",
    response_model=UserPeerAffiliationDetailView,
)
async def fetch_user_peer_affiliation(
    user_id: UUID,
    peer_type: PeerTypeSchema,
    peer_external_id: str,
) -> UserPeerAffiliationDetailView:
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Invalid peer type")
    cfg = ConfigFactory.get_config()
    async with get_conn() as conn:
        detail = await get_user_peer_affiliation(
            conn,
            str(user_id),
            peer_type=peer_type,
            peer_external_id=peer_external_id,
        )
        if detail is None:
            raise HTTPException(status_code=404, detail="Affiliation not found")
    if cfg.aws_region:
        peer = await presign_community_view(detail.peer, region=cfg.aws_region)
        detail = detail.model_copy(update={"peer": peer})
    return detail


@router.get(
    "/users/{user_id}/peers/{peer_type}/{peer_external_id}/affiliation/messages",
    response_model=UserPeerAffiliationMessagesPage,
)
async def fetch_user_peer_affiliation_messages(
    user_id: UUID,
    peer_type: PeerTypeSchema,
    peer_external_id: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> UserPeerAffiliationMessagesPage:
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Invalid peer type")
    async with get_conn() as conn:
        # Ensure the affiliation edge exists (membership-based).
        detail = await get_user_peer_affiliation(
            conn,
            str(user_id),
            peer_type=peer_type,
            peer_external_id=peer_external_id,
        )
        if detail is None:
            raise HTTPException(status_code=404, detail="Affiliation not found")
        page = await list_user_peer_affiliation_messages(
            conn,
            str(user_id),
            peer_external_id=peer_external_id,
            limit=limit,
            offset=offset,
        )
    if page is None:
        raise HTTPException(status_code=404, detail="User not found")
    messages = await _with_media_previews(page.items)
    return page.model_copy(update={"items": messages})


@router.get(
    "/users/{user_id}/peers/{peer_type}/{peer_external_id}/affiliation/reactions",
    response_model=UserPeerAffiliationReactionsPage,
)
async def fetch_user_peer_affiliation_reactions(
    user_id: UUID,
    peer_type: PeerTypeSchema,
    peer_external_id: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> UserPeerAffiliationReactionsPage:
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Invalid peer type")
    async with get_conn() as conn:
        detail = await get_user_peer_affiliation(
            conn,
            str(user_id),
            peer_type=peer_type,
            peer_external_id=peer_external_id,
        )
        if detail is None:
            raise HTTPException(status_code=404, detail="Affiliation not found")
        page = await list_user_peer_affiliation_reactions(
            conn,
            str(user_id),
            peer_external_id=peer_external_id,
            limit=limit,
            offset=offset,
        )
    if page is None:
        raise HTTPException(status_code=404, detail="User not found")
    return page


@router.get(
    "/peers/{external_id}/inventory-counts",
    response_model=PeerInventoryCountsResponse,
)
async def fetch_peer_inventory_counts(
    external_id: str,
    peer_type: PeerTypeSchema = Query(
        ...,
        description="Peer type (required to scope coverage rows).",
    ),
) -> PeerInventoryCountsResponse:
    """Return coverage-backed message + media-kind counts for chat filter badges.

    Both message and media totals are ungated inventory from **message-fetch**
    coverage (message ``media`` JSON at fetch time — not download/S3). Totals
    may be partial until history is complete.
    """
    if peer_type not in ("channel", "megagroup", "chat"):
        return PeerInventoryCountsResponse()
    async with get_conn() as conn:
        message_count = await peer_message_inventory_count(
            conn,
            peer_external_id=external_id,
            peer_type=peer_type,
        )
        media_raw = await peer_media_inventory_counts(
            conn,
            peer_external_id=external_id,
            peer_type=peer_type,
        )
        intervals = await list_peer_message_coverage(
            conn,
            peer_external_id=external_id,
            peer_type=peer_type,
        )
        history_complete = is_history_complete(intervals)
        recent_window_complete = is_recent_window_complete(intervals)
        active = await _active_peer_fetch_messages_job(conn, peer_external_id=external_id)
    return PeerInventoryCountsResponse(
        message_count=message_count,
        media=PeerMediaKindCounts(**media_raw),
        history_complete=history_complete,
        recent_window_complete=recent_window_complete,
        history_job_id=str(active["id"]) if active else None,
        history_job_status=str(active["status"]) if active else None,
    )


_USER_COMPLETE_HISTORY_SOURCE = "user_complete_history"
#: Cap concurrent user-requested history jobs per account (fleet abuse).
_USER_COMPLETE_HISTORY_MAX_ACTIVE = 1
#: Cap new user-requested history jobs per account per rolling day.
_USER_COMPLETE_HISTORY_MAX_PER_DAY = 10


async def _active_peer_fetch_messages_job(
    conn: Any,
    *,
    peer_external_id: str,
) -> dict[str, Any] | None:
    """Return any active ``fetch_messages`` job for ``peer_external_id``, if any.

    Source-agnostic so UI (and enqueue) treat fleet and user jobs the same —
    one peer cannot stack concurrent history fetches.
    """
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, status, source
            FROM harvest_jobs
            WHERE task_type = 'fetch_messages'
              AND status IN ('queued', 'running')
              AND params->>'peer_external_id' = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (peer_external_id,),
        )
        return await cur.fetchone()


async def _user_complete_history_quota(
    conn: Any,
    *,
    user_id: UUID,
) -> tuple[int, int]:
    """Return ``(active_jobs, jobs_created_last_24h)`` for this user."""
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
              COUNT(*) FILTER (
                WHERE status IN ('queued', 'running')
              )::int AS active,
              COUNT(*) FILTER (
                WHERE created_at >= now() - interval '24 hours'
              )::int AS last_day
            FROM harvest_jobs
            WHERE source = %s
              AND actor_type = 'user'
              AND actor_id = %s
            """,
            (_USER_COMPLETE_HISTORY_SOURCE, str(user_id)),
        )
        row = await cur.fetchone()
    if row is None:
        return 0, 0
    return int(row["active"] or 0), int(row["last_day"] or 0)


@router.get(
    "/peers/{external_id}/history-coverage",
    response_model=PeerHistoryCoverageResponse,
)
async def fetch_peer_history_coverage(
    external_id: str,
    peer_type: PeerTypeSchema = Query(
        ...,
        description="Peer type (required to scope coverage rows).",
    ),
) -> PeerHistoryCoverageResponse:
    """Return covered islands and gaps for the history-completeness envelope."""
    if peer_type not in ("channel", "megagroup", "chat"):
        return PeerHistoryCoverageResponse()
    async with get_conn() as conn:
        report = await peer_history_coverage_report(
            conn,
            peer_external_id=external_id,
            peer_type=peer_type,
        )
        active = await _active_peer_fetch_messages_job(conn, peer_external_id=external_id)
    return PeerHistoryCoverageResponse(
        complete=bool(report["complete"]),
        tip_slack_days=int(report["tip_slack_days"]),
        envelope_before=report.get("envelope_before"),
        covered=[CoverageIntervalView(**row) for row in report["covered"]],
        gaps=[CoverageIntervalView(**row) for row in report["gaps"]],
        job_id=str(active["id"]) if active else None,
        job_status=str(active["status"]) if active else None,
    )


@router.post(
    "/peers/{external_id}/complete-history",
    response_model=CompletePeerHistoryResponse,
)
async def enqueue_peer_complete_history(
    request: Request,
    external_id: str,
    peer_type: PeerTypeSchema = Query(
        ...,
        description="Peer type (required to scope coverage rows).",
    ),
    user: PlatformUser = Depends(get_current_user),
) -> CompletePeerHistoryResponse:
    """Enqueue a fleet ``fetch_messages`` job to fill missing history for this peer.

    Prefer a harvester that already has an access hash (handled inside the
    ``fetch_messages`` worker). Idempotent while any queued/running
    ``fetch_messages`` job already targets this peer.
    """
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Unsupported peer type")

    limits = await current_plan_limits(request)
    if not limits.get("live_updates"):
        raise HTTPException(
            status_code=403,
            detail="Upgrade required to complete historical coverage",
        )

    async with get_conn() as conn:
        peer = await get_peer_by_id(conn, external_id, peer_type)
        if peer is None:
            raise HTTPException(status_code=404, detail="Peer not found")

        if await peer_message_history_complete(
            conn,
            peer_external_id=external_id,
            peer_type=peer_type,
        ):
            raise HTTPException(status_code=409, detail="History coverage is already complete")

        # Idempotent join: any active fetch for this peer (any actor) is fine.
        active = await _active_peer_fetch_messages_job(conn, peer_external_id=external_id)
        if active:
            return CompletePeerHistoryResponse(
                job_id=str(active["id"]),
                status=str(active["status"]),
                created=False,
            )

        active_n, last_day_n = await _user_complete_history_quota(conn, user_id=user.id)
        if active_n >= _USER_COMPLETE_HISTORY_MAX_ACTIVE:
            raise HTTPException(
                status_code=429,
                detail=(
                    "You already have a history sync in progress. Wait for it to finish before requesting another."
                ),
            )
        if last_day_n >= _USER_COMPLETE_HISTORY_MAX_PER_DAY:
            raise HTTPException(
                status_code=429,
                detail=(f"History sync limit reached ({_USER_COMPLETE_HISTORY_MAX_PER_DAY}/day). Try again tomorrow."),
            )

        try:
            job = await enqueue_job(
                conn,
                task_type="fetch_messages",
                params={
                    "peer_external_id": external_id,
                    "peer_type": peer_type,
                    "pending_only": False,
                },
                actor_type="user",
                actor_id=str(user.id),
                source=_USER_COMPLETE_HISTORY_SOURCE,
            )
        except Exception as exc:
            # Unique index race: another request won the insert.
            active = await _active_peer_fetch_messages_job(conn, peer_external_id=external_id)
            if active:
                return CompletePeerHistoryResponse(
                    job_id=str(active["id"]),
                    status=str(active["status"]),
                    created=False,
                )
            raise HTTPException(status_code=500, detail="Failed to enqueue coverage job") from exc

    return CompletePeerHistoryResponse(
        job_id=str(job["id"]),
        status=str(job["status"]),
        created=True,
    )


@router.get("/peers/{external_id}/messages", response_model=PeerMessagesResponse)
async def fetch_peer_messages(
    request: Request,
    external_id: str,
    limit: int = Query(50, ge=1, le=100),
    before_message_id: int | None = Query(None, description="Load messages older than this id"),
    page: int | None = Query(
        None,
        ge=1,
        description="1-based newest-first page (OFFSET). Do not combine with before_message_id.",
    ),
    peer_type: PeerTypeSchema | None = Query(None, description="Optional peer type for history-completeness gating"),
    date_from: datetime | None = Query(
        None,
        description="Inclusive lower bound on message date (ISO 8601).",
    ),
    date_to: datetime | None = Query(
        None,
        description="Inclusive upper bound on message date (ISO 8601).",
    ),
) -> PeerMessagesResponse:
    """Fetch messages for a peer (channel/chat), newest first.

    Paginate with ``before_message_id`` (cursor) or ``page`` (offset, page_size=limit).
    Optional ``date_from`` / ``date_to`` filter by ``date`` (indexed with peer).
    """
    if page is not None and before_message_id is not None:
        raise HTTPException(
            status_code=400,
            detail="Pass only one of page or before_message_id",
        )
    limits = await current_plan_limits(request)
    date_filtered = date_from is not None or date_to is not None
    async with get_conn() as conn:
        floor, upgrade_for_history = await _chat_window_floor(
            conn, request=request, external_id=external_id, limits=limits
        )
        media_unlock_floor = await _free_media_unlock_floor(
            conn, request=request, external_id=external_id, limits=limits
        )
        db_total = 0
        if page_is_past_window(floor_id=floor, before_message_id=before_message_id):
            messages, next_before = [], None
        else:
            messages, next_before, db_total = await list_peer_messages(
                conn,
                external_id,
                limit=limit,
                before_message_id=before_message_id,
                min_message_id=floor,
                page=page,
                date_from=date_from,
                date_to=date_to,
            )
        if page is not None or date_filtered:
            # OFFSET / date-filtered pages must use the same row count as the
            # list query (includes plan floor). Coverage inventory can be larger
            # when history is incomplete — that must not drive the pager.
            total_count = db_total
        else:
            total_count = await gated_total_count(
                conn,
                peer_external_id=external_id,
                peer_type=peer_type if peer_type in ("channel", "megagroup", "chat") else None,
            )
            if total_count is None:
                total_count = db_total
        users = await _sender_partials_for_messages(conn, messages)
        peers = await _community_partials_for_messages(conn, messages)
    messages = await _with_media_previews(messages, redact_below_message_id=media_unlock_floor)
    return PeerMessagesResponse(
        messages=messages,
        next_before_message_id=next_before,
        total_count=total_count,
        page=page,
        page_size=limit if page is not None else None,
        users=users,
        peers=peers,
        upgrade_for_history=upgrade_for_history,
    )


@router.get(
    "/peers/{external_id}/messages/locate",
    response_model=PeerMessageLocateResponse,
)
async def locate_peer_message(
    request: Request,
    external_id: str,
    message_id: int = Query(..., ge=1, description="Telegram message_id to locate"),
    page_size: int = Query(100, ge=1, le=100),
    peer_type: PeerTypeSchema | None = Query(None),
) -> PeerMessageLocateResponse:
    """Return which newest-first page contains ``message_id`` (for deep links / reply jumps)."""
    limits = await current_plan_limits(request)
    async with get_conn() as conn:
        floor, upgrade_for_history = await _chat_window_floor(
            conn, request=request, external_id=external_id, limits=limits
        )
        media_unlock_floor = await _free_media_unlock_floor(
            conn, request=request, external_id=external_id, limits=limits
        )
        located = await locate_peer_message_page(
            conn,
            external_id,
            message_id,
            page_size=page_size,
            min_message_id=floor,
        )
        if located is None:
            raise HTTPException(status_code=404, detail="Message not found")
        # Prefer coverage-gated total when history is complete; else raw window count.
        gated = await gated_total_count(
            conn,
            peer_external_id=external_id,
            peer_type=peer_type if peer_type in ("channel", "megagroup", "chat") else None,
        )
        if gated is not None:
            located["total_count"] = gated
        message = located["message"]
        users = await _sender_partials_for_messages(conn, [message])
        peers = await _community_partials_for_messages(conn, [message])
    messages = await _with_media_previews([message], redact_below_message_id=media_unlock_floor)
    return PeerMessageLocateResponse(
        page=int(located["page"]),
        page_size=int(located["page_size"]),
        total_count=int(located["total_count"]),
        newer_count=int(located["newer_count"]),
        message=messages[0],
        users=users,
        peers=peers,
        upgrade_for_history=upgrade_for_history,
    )


@router.get("/peers/{external_id}/media", response_model=PeerMessagesResponse)
async def fetch_peer_media(
    request: Request,
    external_id: str,
    limit: int = Query(50, ge=1, le=100),
    before_message_id: int | None = Query(None, description="Load media messages older than this Telegram message id"),
    after_message_id: int | None = Query(
        None, description="Only return media messages newer than this Telegram message id"
    ),
    kind: str = Query(
        "all",
        description="Media kind filter: all | image | gif | video | audio | document | webpage",
    ),
    peer_type: PeerTypeSchema | None = Query(None, description="Optional peer type for history-completeness gating"),
) -> PeerMessagesResponse:
    """Fetch downloadable media messages for a peer, newest first (Media tab grid)."""
    kind_norm = (kind or "all").strip().lower()
    if kind_norm not in ("all", "image", "gif", "video", "audio", "document", "webpage"):
        raise HTTPException(
            status_code=400,
            detail="Unsupported kind; expected all|image|gif|video|audio|document|webpage",
        )
    # Media tab lists the full DB inventory; Free unlock/blur is client-side (and
    # harvest / S3 URLs capped server-side). Do not apply the chat message window floor.
    limits = await current_plan_limits(request)
    async with get_conn() as conn:
        # Free older pages are entirely past the unlock window — skip the expensive
        # OFFSET floor query and redact every visual URL on the page.
        if before_message_id is not None and not bool(limits.get("media_download")):
            media_unlock_floor = before_message_id
        else:
            media_unlock_floor = await _free_media_unlock_floor(
                conn, request=request, external_id=external_id, limits=limits
            )
        messages, next_before, _ = await list_peer_media_messages(
            conn,
            external_id,
            limit=limit,
            before_message_id=before_message_id,
            after_message_id=after_message_id,
            kind=kind_norm,  # type: ignore[arg-type]
        )
        # Coverage total is expensive (history-completeness scan) and only affects
        # the empty-state copy. Older pages never need it; first page can wait for
        # backfill/realtime to publish a count.
        total_count = None
        # Skip avatar S3 presigns on the media list — tiles use the community page
        # identity; lightbox details still get titles / stripped thumbs from DB.
        users = await _sender_partials_for_messages(conn, messages, presign=False)
        peers = await _community_partials_for_messages(conn, messages, presign=False)
    messages = await _with_media_previews(messages, redact_below_message_id=media_unlock_floor)
    return PeerMessagesResponse(
        messages=messages,
        next_before_message_id=next_before,
        total_count=total_count,
        users=users,
        peers=peers,
        upgrade_for_history=False,
    )


@router.post("/peers/{external_id}/messages/sync", response_model=PeerMessagesResponse)
@metered(Meter.TELEGRAM_SYNC)
async def sync_peer_messages(
    external_id: str,
    request: Request,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
    limit: int = Query(
        50,
        ge=1,
        le=1000,
        description=(
            "Max messages to sync. Tip sync typically uses ≤100; older history "
            "backfill / recent refresh may request up to 1000 (Telegram pages of 100)."
        ),
    ),
    after_message_id: int | None = Query(
        None,
        description="Only sync/return messages newer than this Telegram message id",
    ),
    before_message_id: int | None = Query(
        None,
        description=(
            "Older sync: page with ids strictly below this id. "
            "refresh_recent: resume cursor (oldest id from prior page)."
        ),
    ),
    mode: str | None = Query(
        None,
        description="Optional sync mode. Use refresh_recent for edit/delete freshness.",
    ),
    since_days: int = Query(
        30,
        ge=1,
        le=90,
        description="For mode=refresh_recent: re-fetch messages newer than now−N days.",
    ),
) -> PeerMessagesResponse:
    """Fetch latest or older messages from Telegram, upsert into DB, return a DB page.

    Live UI catch-up for the Community Chat/Media tabs. Tip sync merges a tip
    coverage island; older sync extends coverage toward −∞. ``mode=refresh_recent``
    re-walks a recent window for edits/deletes and extends coverage from each
    page's message dates; when the walk finishes (``refresh_complete``) it also
    seals ``[now − since_days, now)`` so activity volume widgets can gate 7d /
    30d counts. ``total_count`` is null until history coverage is complete from
    the beginning through a tip within the last month.
    """
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Unsupported peer_type for message sync")
    mode_norm = (mode or "").strip().lower() or None
    if mode_norm is not None and mode_norm != "refresh_recent":
        raise HTTPException(
            status_code=400,
            detail="Unsupported mode; expected refresh_recent or omit",
        )
    refresh_recent = mode_norm == "refresh_recent"
    if after_message_id is not None and before_message_id is not None:
        raise HTTPException(
            status_code=400,
            detail="Pass only one of after_message_id or before_message_id",
        )
    if refresh_recent and after_message_id is not None:
        raise HTTPException(
            status_code=400,
            detail="after_message_id is not valid with mode=refresh_recent",
        )

    plan_id, plan_limits = await current_plan_id_and_limits(request)
    live_updates = bool(plan_limits.get("live_updates"))
    if free_history_sync_forbidden(
        live_updates=live_updates,
        before_message_id=before_message_id,
        after_message_id=after_message_id,
        refresh_recent=refresh_recent,
    ):
        raise_history_not_in_plan(plan_id=plan_id)
    sync_limit = clamp_tip_sync_limit(
        limit,
        live_updates=live_updates,
        window=plan_limits.get("chat_message_window"),
    )

    cfg = ConfigFactory.get_config()
    synced_count = 0
    sync_client: TelegramClient | None = None
    sync_harvester_id: UUID | None = None
    oldest_synced_message_id: int | None = None
    refresh_complete: bool | None = None
    deleted_message_ids: list[int] = []
    upgrade_for_history = False
    media_unlock_floor: int | None = None

    async with get_conn() as conn:
        detail = await get_peer_by_id(conn, external_id, peer_type)
        if detail is None:
            raise HTTPException(status_code=404, detail="Peer not found")

        skip_reason = _community_on_demand_skip_reason(detail)
        refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
        lease = PooledClientLease(_get_telegram_pool(request))
        try:
            if skip_reason:
                lg.info(
                    "Message sync skipped external_id=%s: %s",
                    external_id,
                    skip_reason,
                )
            elif _telegram_harvester_configured(cfg):
                role_hid = refresh_row.get("harvester_id") if refresh_row else None
                if role_hid is not None and not isinstance(role_hid, UUID):
                    role_hid = UUID(str(role_hid))
                client, harvester_id, access_hash = await _pool_client_for_peer_role(
                    request,
                    conn,
                    harvester_id=role_hid,
                    access_hash=refresh_row["access_hash"] if refresh_row else None,
                    peer_external_id=external_id,
                    refresh_row=refresh_row,
                )
                lease.bind(client, harvester_id, access_hash)

                if client is None:
                    lg.warning(
                        "Message sync skipped external_id=%s: no Telethon session available",
                        external_id,
                    )
                else:
                    sync_client = client
                    if harvester_id is not None:
                        sync_harvester_id = harvester_id
                    try:
                        if refresh_recent:
                            sync_result = await fetch_and_refresh_recent_messages(
                                client,
                                conn,
                                external_id=external_id,
                                peer_type=peer_type,
                                access_hash=access_hash,
                                harvester_external_id=(
                                    refresh_row.get("harvester_external_id") if refresh_row else None
                                ),
                                limit=sync_limit,
                                since_days=since_days,
                                before_message_id=before_message_id,
                                harvester_id=sync_harvester_id,
                                s3_bucket=cfg.harvest_s3_bucket,
                                s3_region=cfg.aws_region,
                            )
                            synced_count = sync_result.upserted
                            oldest_synced_message_id = sync_result.oldest_message_id
                            refresh_complete = sync_result.refresh_complete
                            deleted_message_ids = list(sync_result.deleted_message_ids)
                            # Extend coverage every page (not only when the walk finishes)
                            # so inventory badges track batches already saved mid-sync.
                            if sync_harvester_id is not None and (sync_result.message_dates or refresh_complete):
                                await record_on_demand_recent_window_coverage(
                                    conn,
                                    harvester_id=sync_harvester_id,
                                    peer_external_id=external_id,
                                    peer_type=peer_type,
                                    since_days=since_days,
                                    message_dates=sync_result.message_dates,
                                    refresh_complete=refresh_complete,
                                )
                        elif before_message_id is not None:
                            history_complete = await peer_message_history_complete(
                                conn,
                                peer_external_id=external_id,
                                peer_type=peer_type,
                            )
                            if history_complete:
                                lg.info(
                                    "Older message sync skipped external_id=%s: "
                                    "peer-wide fetch coverage already complete",
                                    external_id,
                                )
                            else:
                                sync_result = await fetch_and_upsert_older_messages(
                                    client,
                                    conn,
                                    external_id=external_id,
                                    peer_type=peer_type,
                                    access_hash=access_hash,
                                    harvester_external_id=(
                                        refresh_row.get("harvester_external_id") if refresh_row else None
                                    ),
                                    limit=sync_limit,
                                    before_message_id=before_message_id,
                                    harvester_id=sync_harvester_id,
                                    s3_bucket=cfg.harvest_s3_bucket,
                                    s3_region=cfg.aws_region,
                                )
                                synced_count = sync_result.upserted
                                oldest_synced_message_id = sync_result.oldest_message_id
                                if sync_harvester_id is not None:
                                    await record_on_demand_older_message_fetch_coverage(
                                        conn,
                                        harvester_id=sync_harvester_id,
                                        peer_external_id=external_id,
                                        peer_type=peer_type,
                                        message_dates=sync_result.message_dates,
                                        reached_beginning=sync_result.reached_beginning,
                                    )
                        else:
                            sync_result = await fetch_and_upsert_latest_messages(
                                client,
                                conn,
                                external_id=external_id,
                                peer_type=peer_type,
                                access_hash=access_hash,
                                harvester_external_id=(
                                    refresh_row.get("harvester_external_id") if refresh_row else None
                                ),
                                limit=sync_limit,
                                after_message_id=after_message_id,
                                harvester_id=sync_harvester_id,
                                s3_bucket=cfg.harvest_s3_bucket,
                                s3_region=cfg.aws_region,
                            )
                            synced_count = sync_result.upserted
                            if sync_harvester_id is not None and sync_result.tip_complete:
                                await record_on_demand_message_fetch_coverage(
                                    conn,
                                    harvester_id=sync_harvester_id,
                                    peer_external_id=external_id,
                                    peer_type=peer_type,
                                    message_dates=sync_result.message_dates,
                                    tip_complete=sync_result.tip_complete,
                                    empty_chat=after_message_id is None and not sync_result.message_dates,
                                    after_message_id=after_message_id,
                                )
                    except FloodWaitError as exc:
                        raise HTTPException(
                            status_code=429,
                            detail=rate_limited_retry(exc.seconds),
                        ) from exc
                    except ValueError as exc:
                        lg.warning(
                            "Message sync entity error external_id=%s: %s",
                            external_id,
                            exc,
                        )
                    except Exception as exc:
                        await deactivate_harvester_if_account_frozen(
                            conn,
                            harvester_id=sync_harvester_id,
                            exc=exc,
                            pool=_get_telegram_pool(request),
                        )
                        lg.exception(
                            "Message sync failed external_id=%s peer_type=%s",
                            external_id,
                            peer_type,
                        )
            else:
                lg.info(
                    "Message sync skipped external_id=%s: harvester not configured",
                    external_id,
                )

            # Tip / newer polls return the matching DB window; older/refresh return the tip page.
            list_after = after_message_id if before_message_id is None and not refresh_recent else None
            floor, upgrade_for_history = await _chat_window_floor(
                conn, request=request, external_id=external_id, limits=plan_limits
            )
            media_unlock_floor = await _free_media_unlock_floor(
                conn, request=request, external_id=external_id, limits=plan_limits
            )
            messages, next_before, _ = await list_peer_messages(
                conn,
                external_id,
                limit=min(sync_limit, 100) if refresh_recent else sync_limit,
                after_message_id=list_after,
                min_message_id=floor,
            )
            total_count = await gated_total_count(
                conn,
                peer_external_id=external_id,
                peer_type=peer_type,
            )
            messages_7d, messages_30d = await peer_activity_volume_counts(
                conn,
                external_id=external_id,
                peer_type=peer_type,
            )
            # A completed refresh_recent walk just filled that since-window — trust the
            # DB count even if coverage gating races the tip by a few ms.
            if refresh_recent and refresh_complete:
                raw_7d, raw_30d = await count_peer_recent_volumes(conn, external_id)
                if since_days >= 7 and messages_7d is None:
                    messages_7d = raw_7d
                if since_days >= 30 and messages_30d is None:
                    messages_30d = raw_30d
            users = await _sender_partials_for_messages(conn, messages)
            peers = await _community_partials_for_messages(
                conn,
                messages,
                client=sync_client,
                harvester_id=sync_harvester_id,
            )

        finally:
            lease.release()
    messages = await _with_media_previews(messages, redact_below_message_id=media_unlock_floor)
    return PeerMessagesResponse(
        messages=messages,
        next_before_message_id=next_before,
        total_count=total_count,
        synced_count=synced_count,
        oldest_synced_message_id=oldest_synced_message_id,
        refresh_complete=refresh_complete,
        deleted_message_ids=deleted_message_ids,
        messages_7d=messages_7d,
        messages_30d=messages_30d,
        users=users,
        peers=peers,
        upgrade_for_history=upgrade_for_history,
    )


@metered(Meter.TELEGRAM_RESOLVE)
async def resolve_peer_message(
    external_id: str,
    request: Request,
    peer_type: PeerTypeSchema = Query(..., description="channel | megagroup | chat"),
    message_id: int = Query(..., ge=1, description="Specific Telegram message id to fetch"),
) -> PeerMessagesResponse:
    """Fetch one historical Telegram message by id, upsert it, and return it if found."""
    if peer_type not in ("channel", "megagroup", "chat"):
        raise HTTPException(status_code=400, detail="Unsupported peer_type for message resolve")

    cfg = ConfigFactory.get_config()
    synced_count = 0
    sync_client: TelegramClient | None = None
    sync_harvester_id: UUID | None = None
    media_unlock_floor: int | None = None

    async with get_conn() as conn:
        detail = await get_peer_by_id(conn, external_id, peer_type)
        if detail is None:
            raise HTTPException(status_code=404, detail="Peer not found")

        skip_reason = _community_on_demand_skip_reason(detail)
        refresh_row = await get_peer_refresh_row(conn, external_id, peer_type)
        lease = PooledClientLease(_get_telegram_pool(request))
        try:
            if skip_reason:
                lg.info(
                    "Message resolve skipped external_id=%s: %s",
                    external_id,
                    skip_reason,
                )
            elif _telegram_harvester_configured(cfg):
                role_hid = refresh_row.get("harvester_id") if refresh_row else None
                if role_hid is not None and not isinstance(role_hid, UUID):
                    role_hid = UUID(str(role_hid))
                client, harvester_id, access_hash = await _pool_client_for_peer_role(
                    request,
                    conn,
                    harvester_id=role_hid,
                    access_hash=refresh_row["access_hash"] if refresh_row else None,
                    peer_external_id=external_id,
                    refresh_row=refresh_row,
                )
                lease.bind(client, harvester_id, access_hash)

                if client is not None:
                    sync_client = client
                    if harvester_id is not None:
                        sync_harvester_id = harvester_id
                    try:
                        sync_result = await fetch_and_upsert_messages_by_id(
                            client,
                            conn,
                            external_id=external_id,
                            peer_type=peer_type,
                            access_hash=access_hash,
                            harvester_external_id=(refresh_row.get("harvester_external_id") if refresh_row else None),
                            message_ids=[message_id],
                            harvester_id=sync_harvester_id,
                            s3_bucket=cfg.harvest_s3_bucket,
                            s3_region=cfg.aws_region,
                        )
                        synced_count = sync_result.upserted
                    except FloodWaitError as exc:
                        raise HTTPException(
                            status_code=429,
                            detail=rate_limited_retry(exc.seconds),
                        ) from exc
                    except ValueError as exc:
                        lg.warning(
                            "Message resolve entity error external_id=%s message_id=%s: %s",
                            external_id,
                            message_id,
                            exc,
                        )
                    except Exception as exc:
                        await deactivate_harvester_if_account_frozen(
                            conn,
                            harvester_id=sync_harvester_id,
                            exc=exc,
                            pool=_get_telegram_pool(request),
                        )
                        lg.exception(
                            "Message resolve failed external_id=%s peer_type=%s message_id=%s",
                            external_id,
                            peer_type,
                            message_id,
                        )

            message = await get_peer_message_by_message_id(conn, external_id, message_id)
            if message is None:
                return PeerMessagesResponse(messages=[], total_count=None, synced_count=synced_count)

            total_count = await gated_total_count(
                conn,
                peer_external_id=external_id,
                peer_type=peer_type,
            )
            media_unlock_floor = await _free_media_unlock_floor(conn, request=request, external_id=external_id)
            users = await _sender_partials_for_messages(conn, [message])
            peers = await _community_partials_for_messages(
                conn,
                [message],
                client=sync_client,
                harvester_id=sync_harvester_id,
            )

        finally:
            lease.release()
    messages = await _with_media_previews([message], redact_below_message_id=media_unlock_floor)
    return PeerMessagesResponse(
        messages=messages,
        next_before_message_id=None,
        total_count=total_count,
        synced_count=synced_count,
        users=users,
        peers=peers,
    )


@router.get("/message_media", response_model=None)
async def fetch_message_media(
    request: Request,
    id: list[str] = Query(..., description="Message UUID(s)"),
    date: list[str] = Query(..., description="Matching message date(s) as ISO timestamptz"),
) -> MessageMediaResponse | StreamingResponse:
    """Get-or-fetch message media previews (presign existing harvests; download missing via Telethon).

    Send ``Accept: application/x-ndjson`` to receive download progress lines, then a final
    ``{"type":"done","items":...}`` envelope. JSON remains the default.
    """
    if len(id) != len(date):
        raise HTTPException(
            status_code=400,
            detail="Query params id and date must have the same length",
        )
    if not id:
        raise HTTPException(status_code=400, detail="At least one id/date pair is required")
    if len(id) > 50:
        raise HTTPException(status_code=400, detail="At most 50 message keys per request")

    keys = list(zip(id, date, strict=True))
    accept = (request.headers.get("accept") or "").lower()
    want_stream = "application/x-ndjson" in accept
    if want_stream:
        return StreamingResponse(
            _stream_message_media(request, keys),
            media_type="application/x-ndjson",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    items = await _load_message_media_items(request, keys)
    return MessageMediaResponse(items=items)


async def _load_message_media_items(
    request: Request,
    keys: list[tuple[str, str]],
    *,
    on_progress: Any | None = None,
) -> dict[str, MessageMediaPreview]:
    cfg = ConfigFactory.get_config()
    pool = _get_telegram_pool(request)
    limits = await current_plan_limits(request)
    allow_full_download = bool(limits.get("media_download"))
    unlocked_raw = limits.get("media_unlocked_count")
    unlock_count = (
        int(unlocked_raw) if not allow_full_download and isinstance(unlocked_raw, int) and unlocked_raw > 0 else None
    )
    # Free may harvest previews inside the unlock window; Pro/Internal harvest all.
    allow_harvest = allow_full_download or unlock_count is not None
    try:
        async with get_conn() as conn:
            items = await get_or_fetch_message_media(
                conn,
                keys=keys,
                pool=pool,
                bucket=cfg.harvest_s3_bucket,
                region=cfg.aws_region.strip(),
                telegram_configured=_telegram_harvester_configured(cfg),
                on_progress=on_progress,
                allow_harvest=allow_harvest,
                unlock_count=unlock_count,
            )
    except FloodWaitError as exc:
        raise HTTPException(
            status_code=429,
            detail=f"Telegram rate limited; retry after {exc.seconds}s",
        ) from exc
    lg.info(
        "message_media request keys=%s items=%s",
        len(keys),
        len(items),
    )
    return items


async def _stream_message_media(
    request: Request,
    keys: list[tuple[str, str]],
) -> AsyncIterator[str]:
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def on_progress(message_id: str, loaded: int, total: int | None) -> None:
        await queue.put(
            {"type": "progress", "id": message_id, "loaded": loaded, "total": total},
        )

    async def work() -> None:
        try:
            items = await _load_message_media_items(request, keys, on_progress=on_progress)
            payload = MessageMediaResponse(items=items).model_dump(mode="json")
            await queue.put({"type": "done", "items": payload["items"]})
        except HTTPException as exc:
            await queue.put(
                {"type": "error", "status": exc.status_code, "detail": exc.detail},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            lg.exception("message_media stream failed")
            await queue.put({"type": "error", "status": 500, "detail": str(exc)})

    task = asyncio.create_task(work())
    try:
        while True:
            if await request.is_disconnected():
                task.cancel()
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            yield json.dumps(event) + "\n"
            if event.get("type") in ("done", "error"):
                break
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
