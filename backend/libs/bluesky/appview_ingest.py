"""Public App View actor search + upsert (shared by CLI scripts)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import httpx
from psycopg import AsyncConnection

from db.crud.bluesky.actor import upsert_actor
from db.crud.bluesky.search_query_run import (
    checkpoint_search_query_run,
    checkpoint_search_query_run_cursor,
    fetch_queries_to_skip,
    fetch_resumable_search_query_run,
    insert_search_query_run,
    mark_search_query_run_failed,
    mark_search_query_run_succeeded,
)
from db.schemas.bluesky.actor import BlueskyActorSchema, did_method_from_did

logger = logging.getLogger(__name__)

DEFAULT_APPVIEW = "https://public.api.bsky.app"
SEARCH_PATH = "/xrpc/app.bsky.actor.searchActors"
PROFILE_PATH = "/xrpc/app.bsky.actor.getProfile"


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def cid_from_cdn_url(url: str | None) -> str | None:
    if not url or not isinstance(url, str):
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    leaf = path.rsplit("/", 1)[-1]
    cid = leaf.split("@", 1)[0].strip()
    return cid or None


def blob_ref_from_cdn_url(url: str | None) -> dict[str, Any] | None:
    cid = cid_from_cdn_url(url)
    if not cid:
        return None
    return {"ref": {"$link": cid}, "source": "cdn_url"}


def profile_to_schema(profile: dict[str, Any]) -> BlueskyActorSchema:
    did = str(profile.get("did") or "").strip()
    if not did:
        raise ValueError("profile missing did")

    avatar_url = profile.get("avatar")
    banner_url = profile.get("banner")
    if isinstance(avatar_url, str):
        avatar_url = avatar_url.strip() or None
    else:
        avatar_url = None
    if isinstance(banner_url, str):
        banner_url = banner_url.strip() or None
    else:
        banner_url = None

    pinned = profile.get("pinnedPost")
    pinned_post = None
    if isinstance(pinned, dict) and pinned.get("uri"):
        pinned_post = {
            "uri": pinned.get("uri"),
            "cid": pinned.get("cid"),
        }

    labels = profile.get("labels")
    if labels is None:
        labels = []

    return BlueskyActorSchema(
        did=did,
        handle=profile.get("handle"),
        display_name=profile.get("displayName"),
        description=profile.get("description"),
        avatar=blob_ref_from_cdn_url(avatar_url),
        banner=blob_ref_from_cdn_url(banner_url),
        avatar_url=avatar_url,
        banner_url=banner_url,
        labels=labels if isinstance(labels, list) else [],
        pinned_post=pinned_post,
        profile_created_at=_parse_dt(profile.get("createdAt")),
        followers_count=profile.get("followersCount"),
        follows_count=profile.get("followsCount"),
        posts_count=profile.get("postsCount"),
        indexed_at=_parse_dt(profile.get("indexedAt")),
        did_method=did_method_from_did(did),
        source="appview",
        active=True,
        last_synced_at=datetime.now(UTC),
    )


async def search_actors(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    query: str,
    limit: int,
    cursor: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    params: dict[str, str | int] = {"q": query, "limit": limit}
    if cursor:
        params["cursor"] = cursor
    url = f"{base_url.rstrip('/')}{SEARCH_PATH}"
    logger.info("GET searchActors q=%r limit=%s cursor=%s", query, limit, cursor or "-")
    response = await client.get(url, params=params)
    response.raise_for_status()
    payload = response.json()
    actors = payload.get("actors") or []
    next_cursor = payload.get("cursor")
    if not isinstance(actors, list):
        actors = []
    logger.info(
        "searchActors ok status=%s actors=%s next_cursor=%s",
        response.status_code,
        len(actors),
        (next_cursor[:48] + "…") if isinstance(next_cursor, str) and len(next_cursor) > 48 else next_cursor,
    )
    return [a for a in actors if isinstance(a, dict)], next_cursor if isinstance(next_cursor, str) else None


async def get_profile(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    actor: str,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{PROFILE_PATH}"
    logger.info("GET getProfile actor=%s", actor)
    response = await client.get(url, params={"actor": actor})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"unexpected getProfile payload type={type(payload).__name__}")
    return payload


class SearchIngestResult:
    __slots__ = (
        "query",
        "skipped",
        "resumed",
        "interrupted",
        "run_id",
        "actors_seen",
        "actors_persisted",
        "actors_skipped",
        "inserted",
        "refreshed",
        "failed",
        "pages_done",
        "next_cursor",
        "complete",
        "hit_dids",
    )

    def __init__(
        self,
        *,
        query: str,
        skipped: bool = False,
        resumed: bool = False,
        interrupted: bool = False,
        run_id: UUID | None = None,
        actors_seen: int = 0,
        actors_persisted: int = 0,
        actors_skipped: int = 0,
        inserted: int = 0,
        refreshed: int = 0,
        failed: int = 0,
        pages_done: int = 0,
        next_cursor: str | None = None,
        complete: bool = False,
        hit_dids: list[str] | None = None,
    ) -> None:
        self.query = query
        self.skipped = skipped
        self.resumed = resumed
        self.interrupted = interrupted
        self.run_id = run_id
        self.actors_seen = actors_seen
        self.actors_persisted = actors_persisted
        self.actors_skipped = actors_skipped
        self.inserted = inserted
        self.refreshed = refreshed
        self.failed = failed
        self.pages_done = pages_done
        self.next_cursor = next_cursor
        self.complete = complete
        self.hit_dids = hit_dids or []


async def ingest_search_query(
    conn: AsyncConnection | None,
    client: httpx.AsyncClient,
    *,
    query: str,
    base_url: str = DEFAULT_APPVIEW,
    limit: int = 100,
    pages: int = 100,
    skip_hydrate: bool = False,
    record_run: bool = True,
    skip_if_known: bool = True,
    dry_run: bool = False,
    start_cursor: str | None = None,
) -> SearchIngestResult:
    """Search App View for ``query``, upsert actors, optionally record a run row.

    Walks at most ``pages`` App View pages (``limit`` hits each). One query
    maps to one run row: a later call resumes from ``next_cursor`` instead of
    inserting again. Status becomes ``succeeded`` only when App View is
    exhausted (no cursor). Hitting the page budget with a remaining cursor
    leaves the row ``running`` so the next call can advance.

    When ``skip_if_known`` is true and a succeeded bluesky search run exists,
    returns immediately with ``skipped=True``. Incomplete ``running`` rows with
    a saved ``next_cursor`` are resumed automatically. Pass ``start_cursor`` to
    force a resume point (e.g. from logs after an older interrupt).
    """
    cleaned = query.strip()
    if not cleaned:
        return SearchIngestResult(query=query, skipped=True)

    if (skip_if_known or record_run) and not dry_run and conn is None:
        raise ValueError("conn is required unless dry_run=True")

    run_id: UUID | None = None
    resumed = False
    cursor: str | None = start_cursor
    pages_done = 0
    inserted = 0
    refreshed = 0
    failed = 0
    actors_skipped = 0
    seen_dids: set[str] = set()
    hit_dids: list[str] = []
    base_seen = 0
    base_persisted = 0
    base_skipped = 0

    if not dry_run and conn is not None and record_run:
        existing = await fetch_resumable_search_query_run(conn, cleaned)
        if existing is not None and start_cursor is None:
            run_id = existing["id"]
            resumed = True
            cursor = existing.get("next_cursor")
            pages_done = int(existing.get("pages_done") or 0)
            base_seen = int(existing.get("actors_seen") or 0)
            base_persisted = int(existing.get("actors_persisted") or 0)
            base_skipped = int(existing.get("actors_skipped") or 0)
            logger.info(
                "resuming bluesky search query=%r run_id=%s pages_done=%s cursor=%s",
                cleaned,
                run_id,
                pages_done,
                (cursor[:48] + "…") if cursor and len(cursor) > 48 else cursor,
            )
        elif skip_if_known and start_cursor is None:
            known = await fetch_queries_to_skip(conn, [cleaned])
            if cleaned in known:
                logger.info("skip known bluesky search query=%r", cleaned)
                return SearchIngestResult(query=cleaned, skipped=True)

        if run_id is None:
            run_id = await insert_search_query_run(
                conn,
                query_text=cleaned,
                source="appview",
                next_cursor=cursor,
                pages_done=pages_done,
            )
            if start_cursor:
                logger.info(
                    "starting bluesky search query=%r from explicit cursor=%s…",
                    cleaned,
                    start_cursor[:48],
                )

    page_fetch_cursor: str | None = cursor

    def _counts() -> tuple[int, int, int]:
        seen = base_seen + len(seen_dids)
        persisted = base_persisted + inserted + refreshed
        skipped = base_skipped + actors_skipped + failed
        return seen, persisted, skipped

    async def _checkpoint(*, next_cursor: str | None, done_pages: int) -> None:
        if run_id is None or dry_run or conn is None:
            return
        seen, persisted, skipped = _counts()
        await checkpoint_search_query_run(
            conn,
            run_id,
            result_count=seen,
            actors_seen=seen,
            actors_persisted=persisted,
            actors_skipped=skipped,
            next_cursor=next_cursor,
            pages_done=done_pages,
        )

    async def _checkpoint_cursor(*, next_cursor: str | None, done_pages: int) -> None:
        if run_id is None or dry_run or conn is None:
            return
        await checkpoint_search_query_run_cursor(
            conn,
            run_id,
            next_cursor=next_cursor,
            pages_done=done_pages,
        )

    try:
        pages_remaining = max(1, pages)
        for page_i in range(1, pages_remaining + 1):
            page_fetch_cursor = cursor
            actors, cursor = await search_actors(
                client,
                base_url=base_url,
                query=cleaned,
                limit=max(1, min(100, limit)),
                cursor=cursor,
            )
            if not actors:
                break

            for hit in actors:
                actor_ref = str(hit.get("did") or hit.get("handle") or "").strip()
                if not actor_ref:
                    failed += 1
                    continue
                try:
                    profile = hit if skip_hydrate else await get_profile(client, base_url=base_url, actor=actor_ref)
                    schema = profile_to_schema(profile)
                    if schema.did in seen_dids:
                        actors_skipped += 1
                        continue
                    seen_dids.add(schema.did)
                    hit_dids.append(schema.did)

                    if dry_run:
                        continue

                    assert conn is not None
                    existed = await upsert_actor(conn, schema)
                    if existed:
                        refreshed += 1
                    else:
                        inserted += 1
                except Exception:
                    failed += 1
                    logger.exception("failed processing actor_ref=%s query=%r", actor_ref, cleaned)

            pages_done += 1
            # Checkpoint after a full page so resume continues at the *next* cursor.
            await _checkpoint(next_cursor=cursor, done_pages=pages_done)
            logger.info(
                "checkpoint query=%r pages_done=%s seen=%s next_cursor=%s",
                cleaned,
                pages_done,
                _counts()[0],
                (cursor[:48] + "…") if cursor and len(cursor) > 48 else cursor,
            )

            if not cursor:
                break

        actors_seen, actors_persisted, skipped = _counts()
        exhausted = not cursor
        if run_id is not None and not dry_run:
            assert conn is not None
            if exhausted:
                await mark_search_query_run_succeeded(
                    conn,
                    run_id,
                    result_count=actors_seen,
                    actors_seen=actors_seen,
                    actors_persisted=actors_persisted,
                    actors_skipped=skipped,
                    pages_done=pages_done,
                )
            else:
                # Page budget hit; keep the same row running for the next page.
                await _checkpoint(next_cursor=cursor, done_pages=pages_done)
        return SearchIngestResult(
            query=cleaned,
            resumed=resumed,
            run_id=run_id,
            actors_seen=actors_seen,
            actors_persisted=actors_persisted,
            actors_skipped=skipped,
            inserted=inserted,
            refreshed=refreshed,
            failed=failed,
            pages_done=pages_done,
            next_cursor=cursor,
            complete=exhausted,
            hit_dids=hit_dids,
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        # Leave status=running with the cursor for the unfinished page (re-fetch that page).
        # Counts stay at the last completed page so resume does not double-count.
        resume_cursor = page_fetch_cursor
        await _checkpoint_cursor(next_cursor=resume_cursor, done_pages=max(0, pages_done))
        logger.warning(
            "interrupted query=%r pages_done=%s resume_cursor=%s (re-runs incomplete page)",
            cleaned,
            pages_done,
            (resume_cursor[:48] + "…") if resume_cursor and len(resume_cursor) > 48 else resume_cursor,
        )
        seen, persisted, skipped = _counts()
        return SearchIngestResult(
            query=cleaned,
            resumed=resumed,
            interrupted=True,
            run_id=run_id,
            actors_seen=seen,
            actors_persisted=persisted,
            actors_skipped=skipped,
            inserted=inserted,
            refreshed=refreshed,
            failed=failed,
            pages_done=pages_done,
            next_cursor=resume_cursor,
            hit_dids=hit_dids,
        )
    except Exception as exc:
        if run_id is not None and not dry_run and conn is not None:
            # Stay resumable: save page-start cursor, keep last completed-page counts.
            await _checkpoint_cursor(next_cursor=page_fetch_cursor, done_pages=pages_done)
            await mark_search_query_run_failed(conn, run_id, str(exc))
            await _checkpoint_cursor(next_cursor=page_fetch_cursor, done_pages=pages_done)
        raise
