import hashlib
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, Self
import asyncio
from datetime import UTC, datetime
from uuid import UUID
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartException
from telethon.errors import FloodWaitError
from aws.s3 import presign_s3_uri, presign_s3_uri_map
from psycopg.rows import dict_row

from bluesky.appview_ingest import DEFAULT_APPVIEW, ingest_search_query
from db.crud.bluesky.actor import list_actors_by_dids, search_actors_by_name
from db.crud.telegram.harvester import get_harvester_by_id
from db.crud.telegram.image_blobs import is_dedupable_phash, normalize_phash_hex
from db.crud.telegram.message_keyword import hydrate_keyword_messages_by_keys, search_messages_keyword
from services.api.src.embedding_http import embed_search_query
from embedding.preprocess import prepare_text_for_embedding
from services.api.src.image_embedding_http import embed_search_image
from services.api.src.search_message_peers import load_reply_targets
from services.api.src.semantic_search import semantic_search_items
from services.api.src.client_errors import (
    HASH_IMAGE_SEARCH_FAILED,
    IMAGE_SEARCH_FAILED,
    IMAGE_SEARCH_UNAVAILABLE,
    KEYWORD_SEARCH_FAILED,
    KEYWORD_SEARCH_UNAVAILABLE,
    PHONE_SEARCH_FAILED,
    PHONE_SEARCH_UNAVAILABLE,
    SEMANTIC_FAILED,
    TELEGRAM_NAME_SEARCH_FAILED,
    TELEGRAM_NAME_SEARCH_UNAVAILABLE,
    rate_limited_retry,
)
from services.api.src.config import ConfigFactory
from services.api.src.crud.s3.media import (
    presign_community_views,
    presign_keyword_peers_by_id,
    presign_user_partials,
    presign_user_partials_by_id,
)
from services.api.src.crud.supabase import get_conn
from services.api.src.auth.channel import is_billed_channel
from services.api.src.auth.deps import get_optional_user
from services.api.src.image_hash_search import (
    build_image_hash_search_plan,
    build_image_hash_search_plan_from_phash,
)
from services.api.src.image_search import build_image_search_plan
from services.api.src.image_search_session import (
    ImageSearchPlanHit,
    clamp_page,
    get_image_search_session,
    materialize_image_search_page,
    store_image_search_session,
)
from services.api.src.crud.supabase.peers import (
    get_community_views_by_external_ids,
    get_keyword_peers_by_keys,
    get_partial_communities_by_external_ids,
    get_partial_telegram_user_by_external_id,
    get_partial_telegram_user_by_phone,
    get_partial_telegram_users_by_external_ids,
    search_communities_by_name,
    search_users_by_name,
)
from services.api.src.crud.supabase.views.bluesky.actor import (
    BlueskyActorPagedResult,
    BlueskyActorPartialView,
)
from services.api.src.crud.supabase.views.telegram.keyword_message import KeywordMessageView, KeywordPeerView
from services.api.src.crud.supabase.views.telegram.message import TelegramMessageView
from services.api.src.crud.supabase.views.telegram.user import UserPartialView
from services.api.src.message_media import (
    build_message_media_preview,
    ensure_message_image_phash,
)
from services.api.src.models.search import (
    ALLOWED_CONTENT_TYPES,
    BlueskyNameSearchResponse,
    CommunityPagedResult,
    ImageMessageSearchMatch,
    ImageMessageSearchResponse,
    KeywordMessageResponse,
    NameKind,
    NameScope,
    NameSearchResponse,
    PhoneSearchResponse,
    SearchContentSemanticRequest,
    TelegramNameSearchResponse,
    UserPagedResult,
    SemanticMessageResponse,
    UsernameSearchResponse,
)
from services.api.src.search_dates import (
    DATE_EXACT_DESCRIPTION,
    DATE_FROM_DESCRIPTION,
    DATE_TO_DESCRIPTION,
    SearchDateExact,
    SearchDateFrom,
    SearchDateTo,
)
from services.api.src.search_limits import (
    IMAGE_UPLOAD_MAX_BYTES,
    API_IMAGE_UPLOAD_MAX_COUNT,
    IMAGE_UPLOAD_MAX_COUNT,
    IMAGE_UPLOAD_MAX_LABEL,
    KEYWORD_QUERY_MAX_LENGTH,
    NAME_QUERY_MAX_LENGTH,
    PHONE_QUERY_MAX_LENGTH,
    USERNAME_QUERY_MAX_LENGTH,
)
from services.api.src.username_resolve import resolve_username_for_search
from quickwit.async_client import AsyncQuickwitClient, search_message_keyword as search_message_keyword_qw
from telegram.api import resolve_phone, search_peers
from telegram.client import HarvesterSessionTerminatedError
from telegram.materialize import materialize_user
from telegram.search_materialize import materialize_search_peers
from telegram.utils import normalize_phone, phone_digits, to_signed_peer_id
from services.api.src.usage import Meter, current_usage, metered
from db.crud.telegram.search_query_run import (
    fetch_latest_succeeded_search_query_peers,
    fetch_queries_to_skip,
    insert_search_query_run,
    mark_search_query_run_failed,
    mark_search_query_run_succeeded,
)


lg = logging.getLogger(__name__)

router = APIRouter()


def _image_too_large_detail(filename: str | None) -> str:
    label = filename if filename else "upload"
    return f"File too large for {label!r}: max {IMAGE_UPLOAD_MAX_LABEL} per image."


async def _collect_search_image_uploads(request: Request) -> list[UploadFile]:
    """Parse multipart image uploads with count/type/size guards."""
    try:
        form = await request.form(max_part_size=IMAGE_UPLOAD_MAX_BYTES)
    except MultiPartException as exc:
        raise HTTPException(
            status_code=400,
            detail=f"File too large: max {IMAGE_UPLOAD_MAX_LABEL} per image.",
        ) from exc

    billed = is_billed_channel(getattr(request.state, "usage_channel", "app"))
    field = "image" if billed else "images"
    images: list[UploadFile] = []
    for key, value in form.multi_items():
        if str(key).strip().lower() == field and isinstance(value, UploadFile):
            images.append(value)

    if not images:
        raise HTTPException(
            status_code=400,
            detail="No image uploaded." if billed else "No images uploaded.",
        )
    max_count = API_IMAGE_UPLOAD_MAX_COUNT if billed else IMAGE_UPLOAD_MAX_COUNT
    if len(images) > max_count:
        raise HTTPException(
            status_code=400,
            detail="Send one image." if billed else f"At most {IMAGE_UPLOAD_MAX_COUNT} images allowed.",
        )

    for upload in images:
        content_type = (upload.content_type or "").split(";")[0].strip().lower()
        if content_type not in ALLOWED_CONTENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid type for {upload.filename!r}: only PNG and JPEG allowed.",
            )
        if upload.size is not None and upload.size > IMAGE_UPLOAD_MAX_BYTES:
            raise HTTPException(
                status_code=400,
                detail=_image_too_large_detail(upload.filename),
            )

    return images


async def _read_search_image_upload(upload: UploadFile) -> bytes:
    """Read one upload, rejecting payloads over the per-file size limit."""
    if upload.size is not None and upload.size > IMAGE_UPLOAD_MAX_BYTES:
        raise HTTPException(
            status_code=400,
            detail=_image_too_large_detail(upload.filename),
        )
    raw = await upload.read(IMAGE_UPLOAD_MAX_BYTES + 1)
    if len(raw) > IMAGE_UPLOAD_MAX_BYTES:
        raise HTTPException(
            status_code=400,
            detail=_image_too_large_detail(upload.filename),
        )
    return raw


async def _presign_user_partials(users: list[UserPartialView]) -> list[UserPartialView]:
    region = ConfigFactory.get_config().aws_region.strip()
    if not region:
        return users
    return await presign_user_partials(users, region=region)


async def _presign_bluesky_actor_partials(
    actors: list[BlueskyActorPartialView],
) -> list[BlueskyActorPartialView]:
    region = ConfigFactory.get_config().aws_region.strip()
    if not region or not actors:
        return actors
    uris = [uri for actor in actors if (uri := actor.avatar_s3_uri) and uri.startswith("s3://")]
    if not uris:
        return actors
    signed = await presign_s3_uri_map(uris, region=region)
    out: list[BlueskyActorPartialView] = []
    for actor in actors:
        uri = actor.avatar_s3_uri
        https = signed.get(uri) if uri else None
        if https is None or https == uri:
            out.append(actor)
        else:
            out.append(actor.model_copy(update={"avatar_s3_uri": https}))
    return out


async def _presign_user_partial(user: UserPartialView | None) -> UserPartialView | None:
    if user is None:
        return None
    presigned = await _presign_user_partials([user])
    return presigned[0]


_IMAGE_SEARCH_PHOTO_URI_FIELDS = (
    "peer_profile_photo_s3_uri",
    "from_peer_profile_photo_s3_uri",
    "from_user_profile_photo_s3_uri",
)


async def _presign_peer_profile_photo(
    profile_photo_s3_uri: str | None,
    *,
    region: str,
    url_by_uri: Mapping[str, str] | None = None,
) -> str | None:
    if not profile_photo_s3_uri or not profile_photo_s3_uri.startswith("s3://"):
        return profile_photo_s3_uri
    if url_by_uri is not None and profile_photo_s3_uri in url_by_uri:
        return url_by_uri[profile_photo_s3_uri]
    return await presign_s3_uri(profile_photo_s3_uri, region=region)


def _normalize_harvest_s3_key(s3_key_full: str, *, harvest_s3_bucket: str) -> str:
    """Strip optional ``s3://bucket/`` prefix to a bare object key."""
    key = str(s3_key_full or "").strip()
    if not key:
        return ""
    prefix = f"s3://{harvest_s3_bucket}/"
    if key.startswith(prefix):
        return key[len(prefix) :]
    if key.startswith("s3://"):
        # Wrong/other bucket — keep path after s3://bucket/
        without = key[len("s3://") :]
        slash = without.find("/")
        return without[slash + 1 :] if slash >= 0 else ""
    return key


async def _image_message_search_match_from_row(
    *,
    match_id: Any,
    filename: str,
    url: str,
    score: float,
    row: Mapping[str, Any],
    region: str,
    photo_url_by_uri: Mapping[str, str] | None = None,
) -> ImageMessageSearchMatch:
    source_external_id = str(row["source_external_id"])
    message = TelegramMessageView.from_row(
        row,
        edits=[],
        source_external_id=source_external_id,
    ).model_dump(exclude={"id"})
    peer_photo, from_peer_photo, from_user_photo = await asyncio.gather(
        _presign_peer_profile_photo(
            row.get("peer_profile_photo_s3_uri"),
            region=region,
            url_by_uri=photo_url_by_uri,
        ),
        _presign_peer_profile_photo(
            row.get("from_peer_profile_photo_s3_uri"),
            region=region,
            url_by_uri=photo_url_by_uri,
        ),
        _presign_peer_profile_photo(
            row.get("from_user_profile_photo_s3_uri"),
            region=region,
            url_by_uri=photo_url_by_uri,
        ),
    )
    return ImageMessageSearchMatch(
        id=match_id,
        filename=filename,
        url=url,
        score=score,
        peer_title=row.get("peer_title") or "",
        peer_username=row.get("peer_username"),
        peer_profile_photo_s3_uri=peer_photo,
        peer_profile_photo_media_kind=row.get("peer_profile_photo_media_kind"),
        peer_stripped_profile_photo=row.get("peer_stripped_profile_photo"),
        from_peer_title=row.get("from_peer_title"),
        from_peer_type=row.get("from_peer_type"),
        from_peer_username=row.get("from_peer_username"),
        from_peer_profile_photo_s3_uri=from_peer_photo,
        from_peer_profile_photo_media_kind=row.get("from_peer_profile_photo_media_kind"),
        from_peer_stripped_profile_photo=row.get("from_peer_stripped_profile_photo"),
        from_user_id=row.get("from_user_id"),
        from_user_first_name=row.get("from_user_first_name"),
        from_user_last_name=row.get("from_user_last_name"),
        from_user_username=row.get("from_user_username"),
        from_user_profile_photo_s3_uri=from_user_photo,
        from_user_profile_photo_media_kind=row.get("from_user_profile_photo_media_kind"),
        from_user_stripped_thumb=row.get("from_user_stripped_thumb"),
        **message,
    )


async def _image_search_matches_to_response(
    matches: list[dict[str, Any]],
    *,
    region: str,
    harvest_s3_bucket: str,
) -> list[ImageMessageSearchMatch]:
    """Build image-search API rows, signing each unique S3 object once.

    Hash and reverse image search expand a few phashes into many message rows that
    share the same harvest object. Reusing one presigned URL per key (and per
    profile photo URI) cuts S3 GET egress when the client renders copies.
    """
    harvest_keys = [
        _normalize_harvest_s3_key(str(match.get("s3_key") or ""), harvest_s3_bucket=harvest_s3_bucket)
        for match in matches
    ]
    harvest_uris = [f"s3://{harvest_s3_bucket}/{key}" for key in harvest_keys if key]

    photo_uris: list[str] = []
    for match in matches:
        row = match.get("row") or {}
        if not isinstance(row, Mapping):
            continue
        for field in _IMAGE_SEARCH_PHOTO_URI_FIELDS:
            uri = row.get(field)
            if isinstance(uri, str) and uri.startswith("s3://"):
                photo_uris.append(uri)

    url_by_uri = await presign_s3_uri_map([*harvest_uris, *photo_uris], region=region)
    lg.info(
        "image search: presigned %s result rows (%s unique media, %s unique photo URIs)",
        len(matches),
        len(set(harvest_uris)),
        len(set(photo_uris)),
    )

    results: list[ImageMessageSearchMatch] = []
    for match, key in zip(matches, harvest_keys, strict=True):
        harvest_uri = f"s3://{harvest_s3_bucket}/{key}" if key else ""
        signed_url = url_by_uri.get(harvest_uri, "")
        filename = match.get("filename") or (key.rsplit("/", 1)[-1] if key else "")
        results.append(
            await _image_message_search_match_from_row(
                match_id=match["id"],
                filename=filename,
                url=signed_url,
                score=float(match["score"]),
                row=match["row"],
                region=region,
                photo_url_by_uri=url_by_uri,
            )
        )
    return results


def _user_with_phone_fallback(user: UserPartialView, phone: str) -> UserPartialView:
    if user.phone:
        return user
    digits = phone_digits(phone)
    return user.model_copy(update={"phone": digits}) if digits else user


async def _keyword_views_from_rows(rows: list) -> list[KeywordMessageView]:
    region = ConfigFactory.get_config().aws_region.strip()
    previews = await asyncio.gather(*[build_message_media_preview(row.get("media"), region=region) for row in rows])
    return [KeywordMessageView.from_row(row, media=preview) for row, preview in zip(rows, previews, strict=True)]


def _note_image_uploads(
    request: Request,
    raws: list[bytes],
    *,
    embedding_calls: int,
    milvus_searches: int,
) -> None:
    note = current_usage(request)
    if note is None:
        return
    note.params.update(
        {
            "image_count": len(raws),
            "upload_bytes": sum(len(raw) for raw in raws),
            "content_hashes": [hashlib.sha256(raw).hexdigest() for raw in raws],
        }
    )
    note.units.update(
        {
            "searches": 1,
            "upload_bytes": sum(len(raw) for raw in raws),
            "embedding_calls": embedding_calls,
            "milvus_searches": milvus_searches,
        }
    )


@router.get("/name", response_model=NameSearchResponse)
@metered(Meter.SEARCH_NAME)
async def search_by_name(
    request: Request,
    q: str = Query(..., min_length=1, max_length=NAME_QUERY_MAX_LENGTH),
    scope: NameScope = Query("all"),
    kind: NameKind = Query("all"),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    live: bool = Query(
        False,
        description="On API and MCP, also run a live Telegram community lookup and add 3 credits.",
    ),
) -> NameSearchResponse:
    want_communities = (scope in ("all", "community")) and (kind in ("all", "community"))
    want_users = (scope in ("all", "user")) and (kind in ("all", "user"))

    communities_items: list[Any] = []
    users_items: list[Any] = []
    communities_total = 0
    users_total = 0

    async def _load_communities() -> tuple[list[Any], int]:
        if not want_communities:
            return [], 0
        async with get_conn() as conn:
            return await search_communities_by_name(conn, q=q, limit=limit, offset=offset)

    async def _load_users() -> tuple[list[Any], int]:
        if not want_users:
            return [], 0
        async with get_conn() as conn:
            return await search_users_by_name(conn, q=q, limit=limit, offset=offset)

    (communities_items, communities_total), (users_items, users_total) = await asyncio.gather(
        _load_communities(),
        _load_users(),
    )

    region = ConfigFactory.get_config().aws_region.strip()
    users_items, communities_items = await asyncio.gather(
        _presign_user_partials(users_items),
        presign_community_views(communities_items, region=region),
    )

    if live and is_billed_channel(getattr(request.state, "usage_channel", "app")) and want_communities:
        telegram = await _live_telegram_name_search(request, q=q, scope="community", limit=limit)
        if telegram.skipped or len(q.strip()) < 2:
            note = current_usage(request)
            if note is not None:
                note.params["live_omitted"] = True
        seen = {item.external_id for item in communities_items}
        added = 0
        for item in telegram.communities.items:
            if item.external_id in seen:
                continue
            communities_items.append(item)
            seen.add(item.external_id)
            added += 1
        if added:
            communities_items = communities_items[:limit]
            communities_total += added

    return NameSearchResponse(
        query=q,
        scope=scope,
        communities=CommunityPagedResult(
            items=communities_items,
            total=communities_total,
            limit=limit,
            offset=offset,
        ),
        users=UserPagedResult(
            items=users_items,
            total=users_total,
            limit=limit,
            offset=offset,
        ),
    )


BLUESKY_LIVE_SEARCH_LIMIT = 100
BLUESKY_LIVE_SEARCH_PAGES = 1


async def _ingest_bluesky_live_search_page(conn: Any, query: str) -> tuple[bool, list[str]]:
    """Advance one App View ``searchActors`` page on the query's run row.

    Returns ``(skipped, hit_dids)``. ``hit_dids`` are this request's live page
    (App View order); they are not stored on the run.
    """
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "blackflare-api-bluesky-search/0.1"},
    ) as client:
        result = await ingest_search_query(
            conn,
            client,
            query=query,
            base_url=DEFAULT_APPVIEW,
            limit=BLUESKY_LIVE_SEARCH_LIMIT,
            pages=BLUESKY_LIVE_SEARCH_PAGES,
            skip_hydrate=True,
            record_run=True,
            skip_if_known=True,
            dry_run=False,
        )
    if result.skipped:
        lg.info("Bluesky live name search skipped for %r (succeeded or in-flight)", query)
        return True, []
    lg.info(
        "Bluesky live name search %s for %r pages_done=%s complete=%s next_cursor=%s seen=%s hits=%s",
        "resumed" if result.resumed else "started",
        query,
        result.pages_done,
        result.complete,
        (result.next_cursor[:48] + "…") if result.next_cursor and len(result.next_cursor) > 48 else result.next_cursor,
        result.actors_seen,
        len(result.hit_dids),
    )
    return False, result.hit_dids


@router.get(
    "/name/bluesky",
    response_model=BlueskyNameSearchResponse,
    summary="Bluesky search (beta)",
    description=(
        "Beta. Search Bluesky actors by name. Coverage is still partial while this index is expanded. "
        "The call still spends 1 credit. live=true does not change the cost."
    ),
)
@metered(Meter.SEARCH_BLUESKY)
async def search_by_name_bluesky(
    request: Request,
    q: str = Query(..., min_length=1, max_length=NAME_QUERY_MAX_LENGTH),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    live: bool = Query(
        False,
        description=(
            "When true, ingest one App View searchActors page (limit 100) and "
            "return that page's hits. Repeats advance the same run row."
        ),
    ),
) -> BlueskyNameSearchResponse:
    """Bluesky actor name search: live App View hits for this request, else ILIKE."""
    query = q.strip()
    skipped = True
    live_dids: list[str] = []
    async with get_conn() as conn:
        if live and query:
            try:
                skipped, live_dids = await _ingest_bluesky_live_search_page(conn, query)
            except Exception:
                lg.exception("Bluesky live name search ingest failed for %r", query)
                skipped = True
                live_dids = []
        if live_dids:
            rows, total = await list_actors_by_dids(
                conn,
                dids=live_dids,
                limit=len(live_dids),
                offset=0,
            )
        else:
            rows, total = await search_actors_by_name(conn, q=query, limit=limit, offset=offset)

    items = [BlueskyActorPartialView.from_row(row) for row in rows]
    items = await _presign_bluesky_actor_partials(items)

    return BlueskyNameSearchResponse(
        query=query,
        skipped=skipped,
        actors=BlueskyActorPagedResult(
            items=items,
            total=total,
            limit=limit,
            offset=offset,
        ),
    )


@router.get("/username", response_model=UsernameSearchResponse)
@metered(Meter.SEARCH_USERNAME)
async def search_by_username(
    request: Request,
    q: str = Query(
        ...,
        min_length=1,
        max_length=USERNAME_QUERY_MAX_LENGTH,
        description="Telegram @username",
    ),
) -> UsernameSearchResponse:
    """Look up a Telegram username."""
    pool = getattr(request.app.state, "telegram_pool", None)
    async with get_conn() as conn:
        result = await resolve_username_for_search(query=q, conn=conn, pool=pool)
    if result.http_error is not None:
        status, detail = result.http_error
        raise HTTPException(status_code=status, detail=detail)
    return result.response


def _empty_telegram_name_response(
    *,
    query: str,
    scope: NameScope,
    skipped: bool,
    limit: int,
) -> TelegramNameSearchResponse:
    return TelegramNameSearchResponse(
        query=query,
        scope=scope,
        skipped=skipped,
        communities=CommunityPagedResult(items=[], total=0, limit=limit, offset=0),
        users=UserPagedResult(items=[], total=0, limit=limit, offset=0),
    )


def _ids_from_peers_found(
    peers_found: Sequence[Mapping[str, Any]],
    *,
    want_communities: bool,
    want_users: bool,
) -> tuple[list[str], list[str]]:
    community_ids: list[str] = []
    user_ids: list[str] = []
    for ref in peers_found:
        if not ref.get("persisted"):
            continue
        ext = str(ref.get("external_id") or "")
        if not ext:
            continue
        peer_type = str(ref.get("peer_type") or "")
        if peer_type == "user":
            if want_users:
                user_ids.append(ext)
        elif peer_type in ("channel", "megagroup", "chat"):
            if want_communities:
                community_ids.append(ext)
    return community_ids, user_ids


async def _telegram_name_response_from_peers_found(
    conn: Any,
    *,
    query: str,
    scope: NameScope,
    skipped: bool,
    limit: int,
    peers_found: Sequence[Mapping[str, Any]],
    want_communities: bool,
    want_users: bool,
) -> TelegramNameSearchResponse:
    community_ids, user_ids = _ids_from_peers_found(
        peers_found,
        want_communities=want_communities,
        want_users=want_users,
    )
    communities_by_id = await get_community_views_by_external_ids(conn, community_ids) if community_ids else {}
    users_by_id = await get_partial_telegram_users_by_external_ids(conn, user_ids) if user_ids else {}
    communities_items = [communities_by_id[ext] for ext in community_ids if ext in communities_by_id]
    users_items = [users_by_id[ext] for ext in user_ids if ext in users_by_id]
    region = ConfigFactory.get_config().aws_region.strip()
    users_items, communities_items = await asyncio.gather(
        _presign_user_partials(users_items),
        presign_community_views(communities_items, region=region),
    )
    return TelegramNameSearchResponse(
        query=query,
        scope=scope,
        skipped=skipped,
        communities=CommunityPagedResult(
            items=communities_items,
            total=len(communities_items),
            limit=limit,
            offset=0,
        ),
        users=UserPagedResult(
            items=users_items,
            total=len(users_items),
            limit=limit,
            offset=0,
        ),
    )


async def _telegram_name_response_from_succeeded_run(
    conn: Any,
    *,
    query: str,
    scope: NameScope,
    limit: int,
    want_communities: bool,
    want_users: bool,
) -> TelegramNameSearchResponse | None:
    """Hydrate a prior succeeded contacts.search run. None if none exists (in-flight skip)."""
    peers_found = await fetch_latest_succeeded_search_query_peers(conn, query)
    if peers_found is None:
        return None
    return await _telegram_name_response_from_peers_found(
        conn,
        query=query,
        scope=scope,
        skipped=True,
        limit=limit,
        peers_found=peers_found,
        want_communities=want_communities,
        want_users=want_users,
    )


@router.get("/name/telegram", response_model=TelegramNameSearchResponse)
@metered(Meter.SEARCH_NAME_TELEGRAM)
async def search_by_name_via_telegram(
    request: Request,
    q: str = Query(..., min_length=1, max_length=NAME_QUERY_MAX_LENGTH),
    scope: NameScope = Query("all"),
    limit: int = Query(50, ge=1, le=100),
) -> TelegramNameSearchResponse:
    """Live Telegram ``contacts.search`` for the website. Hidden on ``/v1``."""
    return await _live_telegram_name_search(request, q=q, scope=scope, limit=limit)


async def _live_telegram_name_search(
    request: Request,
    *,
    q: str,
    scope: NameScope,
    limit: int,
) -> TelegramNameSearchResponse:
    """Live Telegram ``contacts.search`` for a name query.

    Skips a live Telegram call when this query already succeeded or is actively
    running in ``telegram_search_query_runs``. Succeeded runs are rehydrated from
    saved ``peers_found``; in-flight runs return empty ``skipped: true``.
    """
    query = q.strip()
    if len(query) < 2:
        return _empty_telegram_name_response(query=query, scope=scope, skipped=False, limit=limit)

    want_communities = scope in ("all", "community")
    want_users = scope in ("all", "user")

    async with get_conn() as conn:
        skip = await fetch_queries_to_skip(conn, [query])
        if query in skip:
            hydrated = await _telegram_name_response_from_succeeded_run(
                conn,
                query=query,
                scope=scope,
                limit=limit,
                want_communities=want_communities,
                want_users=want_users,
            )
            if hydrated is not None:
                lg.info("Telegram name search rehydrated for %r from a prior succeeded run", query)
                return hydrated
            lg.info("Telegram name search skipped for %r (already running)", query)
            return _empty_telegram_name_response(query=query, scope=scope, skipped=True, limit=limit)

    cfg = ConfigFactory.get_config()
    if cfg.telegram_api_id <= 0 or not cfg.telegram_api_hash.strip():
        lg.error("Telegram name search blocked: Telegram API credentials are not configured")
        raise HTTPException(status_code=503, detail=TELEGRAM_NAME_SEARCH_UNAVAILABLE)

    pool = getattr(request.app.state, "telegram_pool", None)
    if pool is None:
        lg.error("Telegram name search blocked: telegram session pool is not configured")
        raise HTTPException(status_code=503, detail=TELEGRAM_NAME_SEARCH_UNAVAILABLE)

    run_id = None
    run_finished = False
    try:
        async with get_conn() as conn:
            async with pool.acquire_any_client(conn) as (client, harvester_id):
                if client is None or harvester_id is None:
                    lg.warning("Telegram name search blocked: no Telethon session available")
                    raise HTTPException(status_code=503, detail=TELEGRAM_NAME_SEARCH_UNAVAILABLE)
                harvester = await get_harvester_by_id(conn, harvester_id)
                if harvester is None:
                    lg.error(
                        "Telegram name search blocked: harvester not found for session harvester_id=%s",
                        harvester_id,
                    )
                    raise HTTPException(status_code=503, detail=TELEGRAM_NAME_SEARCH_UNAVAILABLE)

                # Re-check skip under the same connection after acquiring a session
                # so concurrent requests for the same query don't both hit Telegram.
                skip = await fetch_queries_to_skip(conn, [query])
                if query in skip:
                    hydrated = await _telegram_name_response_from_succeeded_run(
                        conn,
                        query=query,
                        scope=scope,
                        limit=limit,
                        want_communities=want_communities,
                        want_users=want_users,
                    )
                    if hydrated is not None:
                        lg.info(
                            "Telegram name search rehydrated for %r after session acquire",
                            query,
                        )
                        return hydrated
                    lg.info(
                        "Telegram name search skipped for %r after session acquire",
                        query,
                    )
                    return _empty_telegram_name_response(query=query, scope=scope, skipped=True, limit=limit)

                run_id = await insert_search_query_run(
                    conn,
                    query_text=query,
                    harvester_id=harvester.id,
                )
                try:
                    chats, users = await search_peers(client, query, limit=limit)
                    result_count = len(chats) + len(users)
                    mat_stats = await materialize_search_peers(
                        client,
                        conn,
                        harvester_id=harvester.id,
                        query=query,
                        chats=chats,
                        users=users,
                        # Interactive path: no inter-peer delay (batch worker keeps its own).
                        delay_seconds=0,
                        s3_bucket=cfg.harvest_s3_bucket,
                        s3_region=cfg.aws_region,
                    )
                except FloodWaitError as exc:
                    await mark_search_query_run_failed(conn, run_id, f"FloodWait: {exc.seconds}s")
                    run_finished = True
                    raise HTTPException(
                        status_code=429,
                        detail=rate_limited_retry(exc.seconds),
                    ) from exc

                await mark_search_query_run_succeeded(
                    conn,
                    run_id,
                    result_count=result_count,
                    peers_seen=mat_stats.peers_seen,
                    peers_persisted=mat_stats.peers_persisted,
                    peers_skipped=mat_stats.peers_skipped,
                    peers_found=list(mat_stats.peers_found),
                )
                run_finished = True

                return await _telegram_name_response_from_peers_found(
                    conn,
                    query=query,
                    scope=scope,
                    skipped=False,
                    limit=limit,
                    peers_found=list(mat_stats.peers_found),
                    want_communities=want_communities,
                    want_users=want_users,
                )

    except HTTPException:
        raise
    except HarvesterSessionTerminatedError as exc:
        lg.warning("Telegram name search session terminated for %r: %s", query, exc)
        if run_id is not None and not run_finished:
            try:
                async with get_conn() as conn:
                    await mark_search_query_run_failed(conn, run_id, str(exc))
            except Exception:
                lg.exception("Failed to mark telegram name search run failed for %r", query)
        raise HTTPException(status_code=503, detail=TELEGRAM_NAME_SEARCH_UNAVAILABLE) from exc
    except ValueError as exc:
        lg.warning("Telegram name search rejected for %r: %s", query, exc)
        if run_id is not None and not run_finished:
            try:
                async with get_conn() as conn:
                    await mark_search_query_run_failed(conn, run_id, str(exc))
            except Exception:
                lg.exception("Failed to mark telegram name search run failed for %r", query)
        raise HTTPException(status_code=503, detail=TELEGRAM_NAME_SEARCH_UNAVAILABLE) from exc
    except Exception as exc:
        lg.exception("Telegram name search failed for %r", query)
        if run_id is not None and not run_finished:
            try:
                async with get_conn() as conn:
                    await mark_search_query_run_failed(conn, run_id, str(exc))
            except Exception:
                lg.exception("Failed to mark telegram name search run failed for %r", query)
        raise HTTPException(status_code=502, detail=TELEGRAM_NAME_SEARCH_FAILED) from exc


@router.post(
    "/content/semantic",
    response_model=SemanticMessageResponse,
    summary="Semantic search (beta)",
    description=(
        "Beta. Search messages by meaning. Coverage is still partial while this index is expanded. "
        "The call still spends 2 credits."
    ),
)
@metered(Meter.SEARCH_SEMANTIC)
async def search_content_semantic(request: Request, body: SearchContentSemanticRequest) -> SemanticMessageResponse:
    limit = max(1, min(body.limit, 100)) if body.limit else 100
    offset = body.offset
    lg.info("Embedding query: %s...", body.query[:80])

    # Convert optional date window to epoch ms for Milvus filtering.
    date_from_epoch_ms = None
    date_to_epoch_ms = None
    if body.date_from is not None:
        d = body.date_from
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        date_from_epoch_ms = int(d.timestamp() * 1000)
    if body.date_to is not None:
        d = body.date_to
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        date_to_epoch_ms = int(d.timestamp() * 1000)

    prepared_query = prepare_text_for_embedding(body.query)
    if prepared_query is None:
        raise HTTPException(status_code=400, detail="Query is empty after preprocessing")

    query_dense = await embed_search_query(prepared_query)

    lg.info(
        "Performing semantic search on query: %s... (limit=%s offset=%s)",
        body.query[:80],
        limit,
        offset,
    )
    try:
        items = await semantic_search_items(
            query_dense,
            limit,
            offset,
            date_from_epoch_ms=date_from_epoch_ms,
            date_to_epoch_ms=date_to_epoch_ms,
        )
    except HTTPException:
        raise
    except Exception as exc:
        lg.exception("S3 Vectors semantic search failed")
        raise HTTPException(status_code=502, detail=SEMANTIC_FAILED) from exc

    lg.info("Returned %s search result(s).", len(items))
    items, peers, users = await _build_message_partials(items)
    return SemanticMessageResponse(
        query=body.query,
        items=items,
        limit=limit,
        offset=offset,
        peers=peers,
        users=users,
    )


async def _keyword_via_pg(
    query: str,
    limit: int,
    offset: int,
    *,
    date_from: datetime | None,
    date_to: datetime | None,
    source_external_id: str | None = None,
) -> tuple[list[KeywordMessageView], int]:
    """Legacy path: Postgres ``ILIKE`` over content."""
    async with get_conn() as conn:
        rows, total = await search_messages_keyword(
            conn,
            query,
            limit=limit,
            offset=offset,
            date_from=date_from,  # datetime validated by FastAPI
            date_to=date_to,
            source_external_id=source_external_id,
        )
    return await _keyword_views_from_rows(rows), total


async def _keyword_via_hits(
    hits: list[dict[str, Any]],
    total: int,
) -> tuple[list[KeywordMessageView], int]:
    """Postgres-hydrate ranked keyword hits from Quickwit."""
    keys = [(h["id"], h["date"]) for h in hits if h.get("id") and h.get("date")]
    if not keys:
        return [], total

    async with get_conn() as conn:
        # Skip cold Parquet pulls on the interactive path (can stall for minutes on
        # multi‑MB parts). Indexed hit content fills stripped bodies below.
        rows_by_id = await hydrate_keyword_messages_by_keys(conn, keys, archive_hydrate=False)

    # Cold-stripped PG rows may lack content without archive hydrate.
    # Prefer the indexed body so keyword cards still render.
    for h in hits:
        row = rows_by_id.get(str(h.get("id") or ""))
        if row is None:
            continue
        if str(row.get("content") or "").strip():
            continue
        source = h.get("source") if isinstance(h.get("source"), dict) else {}
        qw_content = source.get("content") if isinstance(source, dict) else None
        if isinstance(qw_content, str) and qw_content.strip():
            row["content"] = qw_content

    # Preserve index ranking; drop any hit whose PG row is gone (e.g. deleted since indexing).
    ordered_rows = [rows_by_id[str(h["id"])] for h in hits if str(h["id"]) in rows_by_id]
    items = await _keyword_views_from_rows(ordered_rows)
    return items, total


async def _keyword_via_quickwit(
    qw_client: AsyncQuickwitClient,
    query: str,
    limit: int,
    offset: int,
    *,
    date_from: str | None,
    date_to: str | None,
    peer_type: str | list[str] | None,
    source_external_id: str | None = None,
) -> tuple[list[KeywordMessageView], int]:
    """Quickwit matches + ranks; Postgres hydrates authoritative rows (pruned by (id, date))."""
    hits, total = await search_message_keyword_qw(
        qw_client,
        query,
        limit=limit,
        offset=offset,
        date_from=date_from,
        date_to=date_to,
        peer_type=peer_type,
        source_external_id=source_external_id,
    )
    return await _keyword_via_hits(hits, total)


_KEYWORD_PEER_TYPES = ("channel", "megagroup", "chat")


class _MessagePartialLike(Protocol):
    """Minimal shape needed to resolve peer/sender partials (keyword + semantic share it).

    Declared as read-only properties so concrete views with narrower (Literal) ``peer_type``
    types satisfy the protocol covariantly.
    """

    @property
    def from_external_id(self) -> str | None: ...
    @property
    def source_external_id(self) -> str: ...
    @property
    def peer_type(self) -> str | None: ...
    @property
    def reply_to(self) -> Mapping[str, Any] | None: ...

    def model_copy(self, *, update: Mapping[str, Any] | None = None) -> Self: ...


async def _build_message_partials(
    items: Sequence[_MessagePartialLike],
) -> tuple[list[_MessagePartialLike], dict[str, KeywordPeerView], dict[str, UserPartialView]]:
    """Resolve reply targets plus deduped, presigned peer and sender partials."""
    async with get_conn() as conn:
        reply_patches, reply_users, reply_communities = await load_reply_targets(conn, items)
        from_ids = sorted({m.from_external_id for m in items if m.from_external_id} | reply_users)
        peer_keys = sorted(
            {
                (m.source_external_id, m.peer_type)
                for m in items
                if m.source_external_id and m.peer_type in _KEYWORD_PEER_TYPES
            }
        )
        if not from_ids and not peer_keys and not reply_communities and not reply_patches:
            return list(items), {}, {}

        users = await get_partial_telegram_users_by_external_ids(conn, list(from_ids))
        peers = await get_keyword_peers_by_keys(conn, list(peer_keys))
        # Messages may be authored by a channel/megagroup/chat (not a user).
        # Reply targets can be a community the result page has not loaded yet.
        missing_from = [eid for eid in from_ids if eid not in users]
        missing_communities = [eid for eid in reply_communities if eid not in peers]
        extra_communities = sorted(set(missing_from) | set(missing_communities))
        if extra_communities:
            from_peers = await get_partial_communities_by_external_ids(conn, extra_communities)
            peers = {**peers, **from_peers}

    enriched: list[_MessagePartialLike] = []
    for index, item in enumerate(items):
        patched = reply_patches.get(index)
        if patched is None:
            enriched.append(item)
            continue
        enriched.append(item.model_copy(update={"reply_to": patched}))

    region = ConfigFactory.get_config().aws_region.strip()
    if region:
        users, peers = await asyncio.gather(
            presign_user_partials_by_id(users, region=region),
            presign_keyword_peers_by_id(peers, region=region),
        )
    return enriched, peers, users


@router.get("/content/keyword", response_model=KeywordMessageResponse)
@metered(Meter.SEARCH_KEYWORD)
async def search_content_keyword(
    request: Request,
    q: str = Query(..., min_length=1, max_length=KEYWORD_QUERY_MAX_LENGTH),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    date_from: SearchDateFrom | None = Query(None, description=DATE_FROM_DESCRIPTION),
    date_to: SearchDateTo | None = Query(None, description=DATE_TO_DESCRIPTION),
    # Index backends only. `group` maps to megagroups + chats.
    peer_type: str | None = Query(
        None,
        description="Optional peer type filter: channel or group.",
    ),
    source_external_id: str | None = Query(
        None,
        description="Optional peer scope: only messages from this signed external id.",
    ),
) -> KeywordMessageResponse:
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is empty")
    peer_scope = (source_external_id or "").strip() or None

    cfg = ConfigFactory.get_config()
    backend = cfg.search_keyword_backend
    qw_client: AsyncQuickwitClient | None = getattr(request.app.state, "quickwit", None)

    lg.info(
        "Keyword search (%s): %s... (limit=%s offset=%s peer=%s)",
        backend,
        query[:80],
        limit,
        offset,
        peer_scope,
    )

    index_peer_type: str | list[str] | None = None
    if peer_type:
        pt = peer_type.strip().lower()
        if pt == "channel":
            index_peer_type = "channel"
        elif pt == "group":
            index_peer_type = ["megagroup", "chat"]
        else:
            raise HTTPException(status_code=400, detail="peer_type must be 'channel' or 'group'")

    # Prepare index range params (string form) + Postgres params (datetime validated by FastAPI).
    index_date_from: str | None = None
    index_date_to: str | None = None
    pg_date_from = date_from
    pg_date_to = date_to
    if pg_date_from is not None:
        d = pg_date_from
        if getattr(d, "tzinfo", None) is None:
            d = d.replace(tzinfo=UTC)
        index_date_from = d.isoformat()
    if pg_date_to is not None:
        d = pg_date_to
        if getattr(d, "tzinfo", None) is None:
            d = d.replace(tzinfo=UTC)
        index_date_to = d.isoformat()

    if backend == "quickwit":
        if qw_client is None:
            lg.error("Keyword search blocked: Quickwit client is unavailable")
            raise HTTPException(status_code=503, detail=KEYWORD_SEARCH_UNAVAILABLE)
        try:
            items, total = await _keyword_via_quickwit(
                qw_client,
                query,
                limit,
                offset,
                date_from=index_date_from,
                date_to=index_date_to,
                peer_type=index_peer_type,
                source_external_id=peer_scope,
            )
        except HTTPException:
            raise
        except Exception as exc:
            lg.exception("Quickwit keyword search failed")
            raise HTTPException(status_code=502, detail=KEYWORD_SEARCH_FAILED) from exc
    else:
        items, total = await _keyword_via_pg(
            query,
            limit,
            offset,
            date_from=pg_date_from,
            date_to=pg_date_to,
            source_external_id=peer_scope,
        )

    items, peers, users = await _build_message_partials(items)

    lg.info("Returned %s keyword result(s) (total=%s).", len(items), total)
    return KeywordMessageResponse(
        query=query,
        items=items,
        total=total,
        limit=limit,
        offset=offset,
        peers=peers,
        users=users,
    )


_IMAGE_PAGE_DETAIL = (
    "Pass search_id from a previous response, with offset, to read another page of that search. "
    "That call does not upload or search again and costs 0 credits. The id lasts 20 minutes."
)


async def _page_image_search(
    request: Request,
    search_id: str,
    *,
    kind: str,
    limit: int,
    offset: int,
) -> ImageMessageSearchResponse:
    """Return one page of a stored image search. Does not run the search again."""
    user = get_optional_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    session = get_image_search_session(search_id, owner_user_id=str(user.id))
    if session is None:
        raise HTTPException(status_code=410, detail="Image search session expired")
    if session.kind != kind:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "search_id_mismatch",
                "detail": "search_id is for a different image search",
            },
        )

    cfg = ConfigFactory.get_config()
    if not cfg.harvest_s3_bucket:
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)
    region = cfg.aws_region.strip()
    if not region:
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    page_limit, page_offset = clamp_page(limit=limit, offset=offset)
    lg.info(
        "image search page id=%s… kind=%s limit=%s offset=%s",
        search_id[:8],
        session.kind,
        page_limit,
        page_offset,
    )
    matches, total = await materialize_image_search_page(
        session.hits,
        limit=page_limit,
        offset=page_offset,
    )
    items = await _image_search_matches_to_response(
        matches,
        region=region,
        harvest_s3_bucket=cfg.harvest_s3_bucket,
    )
    return ImageMessageSearchResponse(
        items=items,
        total=total,
        limit=page_limit,
        offset=page_offset,
        search_id=search_id,
    )


async def _respond_image_search_page(
    *,
    request: Request,
    kind: str,
    planned: Sequence[ImageSearchPlanHit],
    limit: int,
    offset: int,
    harvest_s3_bucket: str,
    region: str,
) -> ImageMessageSearchResponse:
    """Store plan (if possible), hydrate + presign one page, return response."""
    page_limit, page_offset = clamp_page(limit=limit, offset=offset)
    user = get_optional_user(request)
    search_id: str | None = None
    if user is not None:
        search_id = store_image_search_session(
            owner_user_id=str(user.id),
            kind=kind,
            hits=planned,
        )
    matches, total = await materialize_image_search_page(
        planned,
        limit=page_limit,
        offset=page_offset,
    )
    if not matches and total == 0:
        return ImageMessageSearchResponse(
            items=[],
            total=0,
            limit=page_limit,
            offset=page_offset,
            search_id=search_id,
        )
    lg.info("image search: presigning %s results…", len(matches))
    items = await _image_search_matches_to_response(
        matches,
        region=region,
        harvest_s3_bucket=harvest_s3_bucket,
    )
    return ImageMessageSearchResponse(
        items=items,
        total=total,
        limit=page_limit,
        offset=page_offset,
        search_id=search_id,
    )


@router.post(
    "/image/similarity",
    response_model=ImageMessageSearchResponse,
    summary="Image similarity (beta)",
    description=(
        "Beta. Find messages with a visually similar image. Coverage is still partial while this index "
        "is expanded. A new search spends 2 credits. Send one PNG or JPEG in the image field. " + _IMAGE_PAGE_DETAIL
    ),
)
@metered(Meter.SEARCH_IMAGE)
async def search_images(
    request: Request,
    date_from: SearchDateFrom | None = Query(None, description=DATE_FROM_DESCRIPTION),
    date_to: SearchDateTo | None = Query(None, description=DATE_TO_DESCRIPTION),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    search_id: str | None = Query(None, description=_IMAGE_PAGE_DETAIL),
) -> ImageMessageSearchResponse:
    if search_id and search_id.strip():
        return await _page_image_search(
            request,
            search_id.strip(),
            kind="reverse",
            limit=limit,
            offset=offset,
        )
    lg.info(
        "reverse image search: start (limit=%s offset=%s date_from=%s date_to=%s)",
        limit,
        offset,
        date_from,
        date_to,
    )
    images = await _collect_search_image_uploads(request)

    cfg = ConfigFactory.get_config()
    page_limit = limit or cfg.image_search_page_size
    if not cfg.harvest_s3_bucket:
        lg.error("Reverse image search blocked: harvest_s3_bucket is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    date_from_epoch_ms = None
    date_to_epoch_ms = None
    if date_from is not None:
        d = date_from
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        date_from_epoch_ms = int(d.timestamp() * 1000)
    if date_to is not None:
        d = date_to
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        date_to_epoch_ms = int(d.timestamp() * 1000)

    region = cfg.aws_region.strip()
    if not region:
        lg.error("Reverse image search blocked: aws_region is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    response = ImageMessageSearchResponse(items=[], total=0, limit=page_limit, offset=offset)
    uploaded: list[bytes] = []

    # Multi-upload: page applies to the first image's result set (primary query).
    for upload in images:
        raw = await _read_search_image_upload(upload)
        uploaded.append(raw)
        lg.info("reverse image search: read upload %s (%s bytes)", len(uploaded), len(raw))
        _note_image_uploads(request, uploaded, embedding_calls=len(uploaded), milvus_searches=len(uploaded))
        vector = await embed_search_image(raw)
        lg.info("reverse image search: waiting on S3 Vectors ANN + expand…")
        try:
            planned = await build_image_search_plan(
                vector,
                top_n=cfg.image_search_top_n,
                max_messages_per_phash=cfg.image_max_messages_per_phash,
                date_from_epoch_ms=date_from_epoch_ms,
                date_to_epoch_ms=date_to_epoch_ms,
            )
        except ValueError as exc:
            lg.error("reverse image search config error: %s", exc)
            raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE) from exc
        except Exception as exc:
            lg.exception("reverse image search S3 Vectors failed")
            raise HTTPException(status_code=502, detail=IMAGE_SEARCH_FAILED) from exc
        if not planned:
            continue
        response = await _respond_image_search_page(
            request=request,
            kind="reverse",
            planned=planned,
            limit=page_limit,
            offset=offset,
            harvest_s3_bucket=cfg.harvest_s3_bucket,
            region=region,
        )
        break

    lg.info(
        "reverse image search: finished (items=%s total=%s search_id=%s)",
        len(response.items),
        response.total,
        (response.search_id or "")[:8] or None,
    )
    return response


@router.get("/phone", response_model=PhoneSearchResponse)
@metered(Meter.SEARCH_PHONE)
async def search_by_phone(
    request: Request,
    q: str = Query(
        ...,
        min_length=1,
        max_length=PHONE_QUERY_MAX_LENGTH,
        description="Phone number (E.164, e.g. +14155552671)",
    ),
) -> PhoneSearchResponse:
    query = q.strip()
    phone = normalize_phone(query)
    note = current_usage(request)
    if note is not None:
        note.params["phone"] = phone or query
    if not phone:
        raise HTTPException(status_code=400, detail="Invalid phone number")

    async with get_conn() as conn:
        existing = await get_partial_telegram_user_by_phone(conn, phone)
        if existing is not None:
            user = _user_with_phone_fallback(existing, phone)
            user = await _presign_user_partial(user)
            if note is not None:
                note.units["telegram_calls"] = 0
            return PhoneSearchResponse(
                query=query,
                phone=phone,
                found=True,
                user=user,
            )

    cfg = ConfigFactory.get_config()
    if cfg.telegram_api_id <= 0 or not cfg.telegram_api_hash.strip():
        lg.error("Phone search blocked: Telegram API credentials are not configured")
        raise HTTPException(status_code=503, detail=PHONE_SEARCH_UNAVAILABLE)

    pool = getattr(request.app.state, "telegram_pool", None)
    if pool is None:
        lg.error("Phone search blocked: telegram session pool is not configured")
        raise HTTPException(status_code=503, detail=PHONE_SEARCH_UNAVAILABLE)

    try:
        async with get_conn() as conn:
            async with pool.acquire_any_client(conn) as (client, harvester_id):
                if client is None or harvester_id is None:
                    lg.warning("Phone search blocked: no Telethon session available")
                    raise HTTPException(status_code=503, detail=PHONE_SEARCH_UNAVAILABLE)
                harvester = await get_harvester_by_id(conn, harvester_id)
                if harvester is None:
                    lg.error(
                        "Phone search blocked: harvester not found for session harvester_id=%s",
                        harvester_id,
                    )
                    raise HTTPException(status_code=503, detail=PHONE_SEARCH_UNAVAILABLE)
                try:
                    if note is not None:
                        note.units["telegram_calls"] = 1
                    entity = await resolve_phone(client, phone)
                except FloodWaitError as exc:
                    raise HTTPException(
                        status_code=429,
                        detail=rate_limited_retry(exc.seconds),
                    ) from exc

                if entity is None:
                    return PhoneSearchResponse(query=query, phone=phone, found=False, user=None)

                signed_peer_id = to_signed_peer_id(entity)
                if signed_peer_id is None:
                    raise HTTPException(status_code=502, detail=PHONE_SEARCH_FAILED)

                await materialize_user(
                    client,
                    conn,
                    entity,
                    harvester_id=harvester.id,
                    phone=phone,
                    s3_bucket=cfg.harvest_s3_bucket,
                    s3_region=cfg.aws_region,
                )
                user = await get_partial_telegram_user_by_external_id(
                    conn,
                    str(signed_peer_id),
                )
                if user is None:
                    raise HTTPException(status_code=502, detail=PHONE_SEARCH_FAILED)

                user = _user_with_phone_fallback(user, phone)
                user = await _presign_user_partial(user)

                return PhoneSearchResponse(
                    query=query,
                    phone=phone,
                    found=True,
                    user=user,
                )

    except HTTPException:
        raise
    except HarvesterSessionTerminatedError as exc:
        lg.warning("Phone search session terminated for %r: %s", phone, exc)
        raise HTTPException(status_code=503, detail=PHONE_SEARCH_UNAVAILABLE) from exc
    except ValueError as exc:
        lg.warning("Phone search rejected for %r: %s", phone, exc)
        raise HTTPException(status_code=503, detail=PHONE_SEARCH_UNAVAILABLE) from exc
    except Exception as exc:
        lg.exception("Phone search failed for %r", phone)
        raise HTTPException(status_code=502, detail=PHONE_SEARCH_FAILED) from exc


@router.post(
    "/image/copies",
    response_model=ImageMessageSearchResponse,
    summary="Image copies (beta)",
    description=(
        "Beta. Find near-duplicate copies of an image. Coverage is still partial while this "
        "index is expanded. A new search spends 2 credits. Send one image file in the image field, or message_id with "
        "date, or a 16-character phash. " + _IMAGE_PAGE_DETAIL
    ),
)
@metered(Meter.SEARCH_IMAGE_HASH)
async def search_images_by_hash(
    request: Request,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    search_id: str | None = Query(None, description=_IMAGE_PAGE_DETAIL),
    message_id: UUID | None = Query(None, description="Harvested message id. Send with date."),
    date: SearchDateExact | None = Query(None, description=DATE_EXACT_DESCRIPTION),
    phash: str | None = Query(None, description="64-bit pHash as 16 hex characters."),
) -> ImageMessageSearchResponse:
    if search_id and search_id.strip():
        return await _page_image_search(
            request,
            search_id.strip(),
            kind="hash",
            limit=limit,
            offset=offset,
        )
    message_specified = message_id is not None or date is not None
    phash_specified = bool(phash and phash.strip())
    file_specified = "multipart/form-data" in (request.headers.get("content-type") or "").lower()
    if sum((message_specified, phash_specified, file_specified)) != 1:
        raise HTTPException(
            status_code=400,
            detail="Provide one of an image file, message_id with date, or phash.",
        )
    if message_specified:
        if message_id is None or date is None:
            raise HTTPException(status_code=400, detail="message_id and date are required together.")
        return await _search_images_by_hash_for_message(
            request,
            message_id=message_id,
            date=date,
            limit=limit,
            offset=offset,
        )
    if phash_specified:
        return await _search_images_by_phash(request, phash=phash or "", limit=limit, offset=offset)
    lg.info("hash image search: start (limit=%s offset=%s)", limit, offset)
    images = await _collect_search_image_uploads(request)

    cfg = ConfigFactory.get_config()
    page_limit = limit or cfg.image_search_page_size
    if not cfg.harvest_s3_bucket:
        lg.error("Hash image search blocked: harvest_s3_bucket is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    region = cfg.aws_region.strip()
    if not region:
        lg.error("Hash image search blocked: aws_region is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    response = ImageMessageSearchResponse(items=[], total=0, limit=page_limit, offset=offset)
    uploaded: list[bytes] = []

    for upload in images:
        raw = await _read_search_image_upload(upload)
        uploaded.append(raw)
        lg.info("hash image search: read upload %s (%s bytes)", len(uploaded), len(raw))
        _note_image_uploads(request, uploaded, embedding_calls=0, milvus_searches=len(uploaded))
        lg.info("hash image search: waiting on Postgres Hamming + expand…")
        try:
            planned = await build_image_hash_search_plan(
                raw,
                top_n=cfg.image_phash_search_top_n,
                max_hamming=cfg.image_phash_max_hamming,
                max_messages_per_phash=cfg.image_max_messages_per_phash,
            )
        except Exception as exc:
            lg.exception("hash image search Postgres failed")
            raise HTTPException(status_code=502, detail=HASH_IMAGE_SEARCH_FAILED) from exc
        if not planned:
            continue
        response = await _respond_image_search_page(
            request=request,
            kind="hash",
            planned=planned,
            limit=page_limit,
            offset=offset,
            harvest_s3_bucket=cfg.harvest_s3_bucket,
            region=region,
        )
        break

    lg.info(
        "hash image search: finished (items=%s total=%s search_id=%s)",
        len(response.items),
        response.total,
        (response.search_id or "")[:8] or None,
    )
    return response


async def _search_images_by_hash_for_message(
    request: Request,
    *,
    message_id: UUID,
    date: datetime,
    limit: int,
    offset: int,
) -> ImageMessageSearchResponse:
    """Near-duplicate (pHash) search for an existing message.

    Uses stored ``harvest.phash`` when present; otherwise computes it from the
    harvested S3 object, persists it, then searches Postgres Hamming.
    """
    lg.info(
        "hash-by-message search: start (message_id=%s limit=%s offset=%s)",
        message_id,
        limit,
        offset,
    )
    cfg = ConfigFactory.get_config()
    page_limit = limit or cfg.image_search_page_size
    page_offset = max(0, offset)
    if not cfg.harvest_s3_bucket:
        lg.error("Hash-by-message search blocked: harvest_s3_bucket is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    region = cfg.aws_region.strip()
    if not region:
        lg.error("Hash-by-message search blocked: aws_region is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    lg.info("hash-by-message search: loading message + ensuring pHash…")
    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                  id,
                  date,
                  media,
                  source_external_id::text AS source_external_id,
                  peer_type,
                  message_id
                FROM telegram_messages
                WHERE id = %s AND date = %s
                LIMIT 1
                """,
                (str(message_id), date),
            )
            row = await cur.fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Message not found")

        phash_hex = await ensure_message_image_phash(
            conn,
            row=row,
            region=region,
        )

    empty = ImageMessageSearchResponse(items=[], total=0, limit=page_limit, offset=page_offset)
    if not phash_hex:
        lg.info("hash-by-message search: no pHash for message")
        return empty

    hash_note = current_usage(request)
    if hash_note is not None:
        hash_note.params["phash"] = phash_hex
        hash_note.units.update({"searches": 1, "milvus_searches": 1})

    lg.info("hash-by-message search: waiting on Postgres Hamming + expand (phash=%s…)…", phash_hex[:8])
    try:
        planned = await build_image_hash_search_plan_from_phash(
            phash_hex,
            top_n=cfg.image_phash_search_top_n,
            max_hamming=cfg.image_phash_max_hamming,
            max_messages_per_phash=cfg.image_max_messages_per_phash,
            sort="date_desc",
            exclude_message_ids={str(message_id)},
        )
    except Exception as exc:
        lg.exception("hash-by-message search Postgres failed")
        raise HTTPException(status_code=502, detail=HASH_IMAGE_SEARCH_FAILED) from exc
    response = await _respond_image_search_page(
        request=request,
        kind="hash",
        planned=planned,
        limit=page_limit,
        offset=page_offset,
        harvest_s3_bucket=cfg.harvest_s3_bucket,
        region=region,
    )
    lg.info(
        "hash-by-message search: finished (items=%s total=%s search_id=%s)",
        len(response.items),
        response.total,
        (response.search_id or "")[:8] or None,
    )
    return response


async def _search_images_by_phash(
    request: Request,
    *,
    phash: str,
    limit: int,
    offset: int,
) -> ImageMessageSearchResponse:
    normalized = normalize_phash_hex(phash)
    if normalized is None or not is_dedupable_phash(normalized):
        raise HTTPException(status_code=400, detail="phash must be 16 hex characters.")
    lg.info("hash-by-phash search: start (phash=%s… limit=%s offset=%s)", normalized[:8], limit, offset)
    cfg = ConfigFactory.get_config()
    page_limit = limit or cfg.image_search_page_size
    page_offset = max(0, offset)
    if not cfg.harvest_s3_bucket:
        lg.error("Hash-by-phash search blocked: harvest_s3_bucket is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)
    region = cfg.aws_region.strip()
    if not region:
        lg.error("Hash-by-phash search blocked: aws_region is not configured")
        raise HTTPException(status_code=500, detail=IMAGE_SEARCH_UNAVAILABLE)

    hash_note = current_usage(request)
    if hash_note is not None:
        hash_note.params["phash"] = normalized
        hash_note.units.update({"searches": 1, "milvus_searches": 1})

    try:
        planned = await build_image_hash_search_plan_from_phash(
            normalized,
            top_n=cfg.image_phash_search_top_n,
            max_hamming=cfg.image_phash_max_hamming,
            max_messages_per_phash=cfg.image_max_messages_per_phash,
            sort="date_desc",
        )
    except Exception as exc:
        lg.exception("hash-by-phash search Postgres failed")
        raise HTTPException(status_code=502, detail=HASH_IMAGE_SEARCH_FAILED) from exc
    response = await _respond_image_search_page(
        request=request,
        kind="hash",
        planned=planned,
        limit=page_limit,
        offset=page_offset,
        harvest_s3_bucket=cfg.harvest_s3_bucket,
        region=region,
    )
    lg.info(
        "hash-by-phash search: finished (items=%s total=%s search_id=%s)",
        len(response.items),
        response.total,
        (response.search_id or "")[:8] or None,
    )
    return response
