from dataclasses import dataclass, field, replace
import json
import logging
import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import InputPeerChannel, PeerChat

from db.crud.telegram.harvester import clear_harvester_peer_access_error, record_harvester_peer_access_error
from db.crud.telegram.harvester_status import record_harvester_flood_wait_from_exc
from db.schemas.general.primitives import TelegramPeerTypeSchema
from telegram.harvester_peer_access import (
    classify_harvester_access_error_code,
    deactivate_harvester_if_account_frozen,
    is_harvester_peer_inaccessible_error,
)
from db.crud.telegram.coverage import (
    build_fetch_interval_for_window,
    list_coverage,
    replace_coverage,
)
from db.crud.telegram.forward_sources import persist_forward_sources_from_message_batch
from db.crud.telegram.message import upsert_messages
from jobs.fetch_messages.db import (
    CursorState,
    PeerAssignmentRow,
    fetch_cursor,
    upsert_cursor,
)
from telegram.mappers import message_schema_from_telethon_message
from telegram.harvester_logging import get_active_harvester_logger
from telegram.utils import channel_id_from_signed_peer_id, json_safe_value, from_telethon
from utils.coverage_intervals import (
    CoverageInterval,
    FetchCoverageInterval,
    ensure_utc,
    merge_fetch_intervals,
    partition_envelope,
)
from jobs.progress import SharedProgress
from .reactions import (
    ReactionHarvestStats,
    harvest_reactors_for_messages,
    snapshot_reaction_state,
)

_base_lg = logging.getLogger(__name__)


def _lg() -> logging.Logger | logging.LoggerAdapter[logging.Logger]:
    return get_active_harvester_logger(_base_lg)


DEBUG_MESSAGE_LIMIT = 10


@dataclass
class DebugCapture:
    """Collect raw Telethon messages for ``--debug`` (first N across the run)."""

    limit: int = DEBUG_MESSAGE_LIMIT
    messages: list[dict[str, Any]] = field(default_factory=list)

    @property
    def is_full(self) -> bool:
        return len(self.messages) >= self.limit

    def add(self, msg: Any) -> None:
        if self.is_full:
            return
        raw = from_telethon(msg)
        if not isinstance(raw, dict):
            raw = {"value": raw}
        self.messages.append(json_safe_value(raw))


def write_debug_messages(path: Path, messages: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(messages, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _base_lg.info("Wrote %s debug Telethon message(s) to %s", len(messages), path)


@dataclass(frozen=True, slots=True)
class FetchStats:
    peers_seen: int = 0
    peers_skipped_complete: int = 0
    peers_skipped_unassigned: int = 0
    peers_resolved: int = 0
    peers_processed: int = 0
    messages_persisted: int = 0
    peers_failed: int = 0
    reaction_lists_fetched: int = 0
    reactors_persisted: int = 0
    reactor_users_upserted: int = 0
    reaction_list_errors: int = 0


def merge_fetch_stats(base: FetchStats, *parts: FetchStats) -> FetchStats:
    total = base
    for part in parts:
        total = FetchStats(
            peers_seen=total.peers_seen + part.peers_seen,
            peers_skipped_complete=total.peers_skipped_complete + part.peers_skipped_complete,
            peers_skipped_unassigned=total.peers_skipped_unassigned + part.peers_skipped_unassigned,
            peers_resolved=total.peers_resolved + part.peers_resolved,
            peers_processed=total.peers_processed + part.peers_processed,
            messages_persisted=total.messages_persisted + part.messages_persisted,
            peers_failed=total.peers_failed + part.peers_failed,
            reaction_lists_fetched=total.reaction_lists_fetched + part.reaction_lists_fetched,
            reactors_persisted=total.reactors_persisted + part.reactors_persisted,
            reactor_users_upserted=total.reactor_users_upserted + part.reactor_users_upserted,
            reaction_list_errors=total.reaction_list_errors + part.reaction_list_errors,
        )
    return total


def _merge_reaction_stats(stats: FetchStats, part: ReactionHarvestStats) -> FetchStats:
    return FetchStats(
        peers_seen=stats.peers_seen,
        peers_skipped_complete=stats.peers_skipped_complete,
        peers_skipped_unassigned=stats.peers_skipped_unassigned,
        peers_resolved=stats.peers_resolved,
        peers_processed=stats.peers_processed,
        messages_persisted=stats.messages_persisted,
        peers_failed=stats.peers_failed,
        reaction_lists_fetched=stats.reaction_lists_fetched + part.lists_fetched,
        reactors_persisted=stats.reactors_persisted + part.reactors_persisted,
        reactor_users_upserted=stats.reactor_users_upserted + part.users_upserted,
        reaction_list_errors=stats.reaction_list_errors + part.errors,
    )


def _entity_for_peer(peer: PeerAssignmentRow) -> InputPeerChannel | PeerChat:
    signed = int(peer.peer_external_id)
    if peer.peer_type in ("channel", "megagroup"):
        raw_channel_id = channel_id_from_signed_peer_id(signed)
        if raw_channel_id is None:
            raise ValueError(f"Peer external_id={peer.peer_external_id!r} is not a channel/megagroup signed id")
        if peer.access_hash is None:
            raise ValueError(
                f"Missing access_hash for peer {peer.peer_external_id} (need telegram_harvester_peer_roles.access_hash)"
            )
        _lg().info(f"raw_peer_id: {raw_channel_id}, access hash: {peer.access_hash}")
        return InputPeerChannel(raw_channel_id, peer.access_hash)

    if peer.peer_type == "chat":
        chat_id = -signed if signed < 0 else signed
        return PeerChat(chat_id)

    raise ValueError(f"Unsupported peer_type={peer.peer_type!r}")


async def fetch_messages_for_peers(
    client: TelegramClient,
    conn: Any,
    *,
    harvester_id: UUID,
    harvester_external_id: str,
    peers: list[PeerAssignmentRow],
    window_before: datetime | None,
    window_after: datetime | None,
    per_peer_limit: int = 0,
    db_batch_size: int = 250,
    delay_seconds: float = 0.0,
    debug_capture: DebugCapture | None = None,
    ignore_coverage: bool = False,
    stop_at_message_id_by_peer: dict[str, int] | None = None,
    progress: SharedProgress | None = None,
) -> FetchStats:
    before = ensure_utc(window_before)
    after = ensure_utc(window_after)
    # Config left TELEGRAM_FETCH_BEFORE unset → each run uses ``now`` as gap end.
    open_ended_before = window_before is None

    _lg().info(
        "Message fetch peers=%s | window_after=%s window_before=%s ignore_coverage=%s stop_at_peers=%s",
        len(peers),
        after,
        before,
        ignore_coverage,
        len(stop_at_message_id_by_peer or {}),
    )

    stats = FetchStats(peers_seen=len(peers))
    if progress is not None:
        await progress.bump(peers_total=len(peers), peers_seen=len(peers))

    for peer in peers:
        if debug_capture is not None and debug_capture.is_full:
            break
        if progress is not None:
            await progress.set(
                current_peer={"external_id": peer.peer_external_id},
                detail=f"fetch {peer.peer_external_id}",
            )
        stop_at = None
        if stop_at_message_id_by_peer is not None:
            stop_at = stop_at_message_id_by_peer.get(peer.peer_external_id)
        try:
            current_coverage: list[FetchCoverageInterval] = []
            if ignore_coverage:
                gap_before = before if before is not None else datetime.now(UTC)
                gaps = [CoverageInterval(covered_after=after, covered_before=gap_before)]
            else:
                current_coverage = await list_coverage(
                    conn,
                    harvester_id=harvester_id,
                    peer_external_id=peer.peer_external_id,
                    peer_type=peer.peer_type,
                )
                _, gaps = partition_envelope(
                    [i.as_coverage_interval() for i in current_coverage],
                    window_after=window_after,
                    window_before=window_before,
                )

                if not gaps:
                    stats = replace(stats, peers_skipped_complete=stats.peers_skipped_complete + 1)
                    if progress is not None:
                        await progress.bump(peers_done=1, peers_skipped=1)
                        await progress.finish_peer(
                            external_id=peer.peer_external_id,
                            messages=0,
                            status="skipped",
                        )
                    continue

            total_peer_persisted = 0
            for gap in gaps:
                gap_after = gap.covered_after
                gap_before = gap.covered_before

                if ignore_coverage:
                    cursor = CursorState(next_offset_id=0, is_complete=False)
                else:
                    cursor = await fetch_cursor(
                        conn,
                        harvester_id=harvester_id,
                        peer_external_id=peer.peer_external_id,
                        peer_type=peer.peer_type,
                        window_before=gap_before,
                        window_after=gap_after,
                        resume_open_ended=open_ended_before,
                    )
                    if cursor.is_complete:
                        continue

                _lg().info(
                    "Fetching %s %s | cursor_offset_id=%s | gap_after=%s gap_before=%s stop_at_message_id=%s",
                    peer.peer_type,
                    peer.peer_external_id,
                    cursor.next_offset_id,
                    gap_after,
                    gap_before,
                    stop_at,
                )

                persisted, completed, reaction_stats = await _fetch_peer(
                    client,
                    conn,
                    peer=peer,
                    cursor=cursor,
                    harvester_id=harvester_id,
                    harvester_external_id=harvester_external_id,
                    before=gap_before,
                    after=gap_after,
                    per_peer_limit=0 if per_peer_limit <= 0 else max(0, per_peer_limit - total_peer_persisted),
                    db_batch_size=db_batch_size,
                    debug_capture=debug_capture,
                    ignore_coverage=ignore_coverage,
                    stop_at_message_id=stop_at,
                    replace_open_ended_siblings=open_ended_before,
                )
                total_peer_persisted += persisted
                stats = _merge_reaction_stats(stats, reaction_stats)

                if debug_capture is not None and debug_capture.is_full:
                    break

                # If we completed the gap, merge it into coverage.
                if completed and not ignore_coverage:
                    completed_iv = await build_fetch_interval_for_window(
                        conn,
                        peer_external_id=peer.peer_external_id,
                        covered_after=completed.covered_after,
                        covered_before=completed.covered_before,
                    )
                    current_coverage = merge_fetch_intervals([*current_coverage, completed_iv])
                    await replace_coverage(
                        conn,
                        harvester_id=harvester_id,
                        peer_external_id=peer.peer_external_id,
                        peer_type=peer.peer_type,
                        intervals=current_coverage,
                    )

                if per_peer_limit > 0 and total_peer_persisted >= per_peer_limit:
                    break

                # Early-stop finished this peer's inventory target; don't keep filling −∞.
                if stop_at is not None and completed is not None:
                    break

            if debug_capture is not None and debug_capture.is_full:
                break

            persisted = total_peer_persisted
            await clear_harvester_peer_access_error(
                conn,
                harvester_id=harvester_id,
                external_id=peer.peer_external_id,
            )
            stats = replace(
                stats,
                peers_processed=stats.peers_processed + 1,
                messages_persisted=stats.messages_persisted + persisted,
            )
            if progress is not None:
                await progress.bump(
                    peers_done=1,
                    messages_persisted=persisted,
                    messages_fetched=persisted,
                )
                await progress.finish_peer(
                    external_id=peer.peer_external_id,
                    messages=persisted,
                    status="done",
                )

            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
        except FloodWaitError as exc:
            msg = f"FloodWait {exc.seconds}s while fetching {peer.peer_external_id} — aborting this worker run"
            _lg().warning(msg)
            try:
                await record_harvester_flood_wait_from_exc(
                    conn,
                    harvester_id=harvester_id,
                    exc=exc,
                    peer_external_id=peer.peer_external_id,
                    source="fetch_messages",
                )
                await conn.commit()
            except Exception:
                _lg().exception(
                    "failed to persist flood-wait harvester_id=%s peer=%s",
                    harvester_id,
                    peer.peer_external_id,
                )
            if progress is not None:
                await progress.event("flood_wait", msg)
                await progress.bump(peers_done=1, peers_failed=1)
                await progress.finish_peer(
                    external_id=peer.peer_external_id,
                    messages=0,
                    status="failed",
                )
            return replace(stats, peers_failed=stats.peers_failed + 1)
        except Exception as exc:
            if await deactivate_harvester_if_account_frozen(
                conn,
                harvester_id=harvester_id,
                exc=exc,
            ):
                msg = (
                    f"Harvester account frozen while fetching {peer.peer_external_id} "
                    f"— deactivated harvester and aborting this worker run"
                )
                _lg().error(msg)
                if progress is not None:
                    await progress.event("harvester_frozen", msg, warning=False)
                    await progress.bump(peers_done=1, peers_failed=1)
                    await progress.finish_peer(
                        external_id=peer.peer_external_id,
                        messages=0,
                        status="failed",
                    )
                return replace(stats, peers_failed=stats.peers_failed + 1)
            if is_harvester_peer_inaccessible_error(exc):
                error_code = classify_harvester_access_error_code(exc)
                await record_harvester_peer_access_error(
                    conn,
                    harvester_id=harvester_id,
                    external_id=peer.peer_external_id,
                    error_code=error_code,
                    peer_type=peer.peer_type,
                    access_hash=peer.access_hash,
                )
                msg = f"Peer inaccessible {peer.peer_external_id} ({peer.peer_type}) code={error_code} — skipping"
                _lg().warning(msg)
                if progress is not None:
                    await progress.event("peer_inaccessible", msg)
                    await progress.bump(peers_done=1, peers_failed=1)
                    await progress.finish_peer(
                        external_id=peer.peer_external_id,
                        messages=0,
                        status="failed",
                    )
            else:
                msg = f"Unexpected error fetching {peer.peer_external_id}: {exc}"
                _lg().error(msg)
                if progress is not None:
                    await progress.event("error", msg, warning=False)
                    await progress.bump(peers_done=1, peers_failed=1)
                    await progress.finish_peer(
                        external_id=peer.peer_external_id,
                        messages=0,
                        status="failed",
                    )
            stats = replace(stats, peers_failed=stats.peers_failed + 1)

    _lg().info(
        "Message fetch done | peers=%s processed=%s skipped_complete=%s "
        "skipped_unassigned=%s resolved=%s failed=%s | messages=%s | "
        "reaction_lists=%s reactors=%s reactor_users=%s reaction_errors=%s",
        stats.peers_seen,
        stats.peers_processed,
        stats.peers_skipped_complete,
        stats.peers_skipped_unassigned,
        stats.peers_resolved,
        stats.peers_failed,
        stats.messages_persisted,
        stats.reaction_lists_fetched,
        stats.reactors_persisted,
        stats.reactor_users_upserted,
        stats.reaction_list_errors,
    )
    return stats


async def _flush_message_batch(
    client: TelegramClient,
    conn: Any,
    buf: list[Any],
    *,
    peer_entity: Any,
    peer_external_id: str,
    peer_type: TelegramPeerTypeSchema | str,
    harvester_id: UUID,
) -> ReactionHarvestStats:
    if not buf:
        return ReactionHarvestStats()
    batch_size = len(buf)
    prior = await snapshot_reaction_state(conn, buf)
    await upsert_messages(conn, buf)
    edge_count = await persist_forward_sources_from_message_batch(
        conn,
        messages=buf,
        source_external_id=peer_external_id,
        from_peer_type=peer_type,  # type: ignore[arg-type]
        harvester_id=harvester_id,
    )
    if edge_count:
        _lg().info(
            "Forward edges incremented | %s %s | messages=%s targets=%s",
            peer_type,
            peer_external_id,
            batch_size,
            edge_count,
        )
    _lg().info(
        "Upserted message batch | %s %s | messages=%s",
        peer_type,
        peer_external_id,
        batch_size,
    )
    stats = await harvest_reactors_for_messages(
        client,
        conn,
        buf,
        peer_entity=peer_entity,
        harvester_id=harvester_id,
        prior_state=prior,
    )
    _lg().info(
        "Reaction harvest batch | %s %s | lists_fetched=%s lists_skipped=%s reactors=%s users=%s errors=%s",
        peer_type,
        peer_external_id,
        stats.lists_fetched,
        stats.lists_skipped,
        stats.reactors_persisted,
        stats.users_upserted,
        stats.errors,
    )
    return stats


async def _fetch_peer(
    client: TelegramClient,
    conn: Any,
    *,
    peer: PeerAssignmentRow,
    cursor: CursorState,
    harvester_id: UUID,
    harvester_external_id: str,
    before: datetime,
    after: datetime | None,
    per_peer_limit: int,
    db_batch_size: int,
    debug_capture: DebugCapture | None = None,
    ignore_coverage: bool = False,
    stop_at_message_id: int | None = None,
    replace_open_ended_siblings: bool = False,
) -> tuple[int, CoverageInterval | None, ReactionHarvestStats]:
    # Telethon iteration:
    # - newest -> oldest by default
    # - offset_date = before means "only messages strictly before this date"
    # - offset_id = cursor continues with ids < offset_id (older)
    peer_entity = _entity_for_peer(peer)
    offset_id = cursor.next_offset_id or 0
    iter_kwargs: dict[str, Any] = {"offset_date": before}
    if offset_id:
        iter_kwargs["offset_id"] = offset_id

    buf: list[Any] = []
    total_persisted = 0
    reaction_stats = ReactionHarvestStats()
    min_id_seen: int | None = None
    last_id_seen: int | None = None
    min_date_seen: datetime | None = None
    max_date_seen: datetime | None = None
    complete = False
    hit_limit = False
    stopped_at_target = False

    async for msg in client.iter_messages(peer_entity, **iter_kwargs):
        mid = getattr(msg, "id", None)
        if not isinstance(mid, int):
            continue

        mdate = getattr(msg, "date", None)
        if isinstance(mdate, datetime) and mdate.tzinfo is None:
            mdate = mdate.replace(tzinfo=UTC)

        if after is not None and isinstance(mdate, datetime) and mdate < after:
            complete = True
            break

        if debug_capture is not None and not debug_capture.is_full:
            debug_capture.add(msg)

        if isinstance(mdate, datetime):
            min_date_seen = mdate if min_date_seen is None else min(min_date_seen, mdate)
            max_date_seen = mdate if max_date_seen is None else max(max_date_seen, mdate)

        mapped = message_schema_from_telethon_message(
            msg,
            source_external_id=peer.peer_external_id,
            peer_type=peer.peer_type,
            harvester_external_id=harvester_external_id,
        )
        buf.append(mapped)

        min_id_seen = mid if min_id_seen is None else min(min_id_seen, mid)
        last_id_seen = mid

        if len(buf) >= max(1, db_batch_size):
            batch_stats = await _flush_message_batch(
                client,
                conn,
                buf,
                peer_entity=peer_entity,
                peer_external_id=peer.peer_external_id,
                peer_type=peer.peer_type,
                harvester_id=harvester_id,
            )
            reaction_stats = ReactionHarvestStats(
                lists_fetched=reaction_stats.lists_fetched + batch_stats.lists_fetched,
                lists_skipped=reaction_stats.lists_skipped + batch_stats.lists_skipped,
                reactors_persisted=reaction_stats.reactors_persisted + batch_stats.reactors_persisted,
                users_upserted=reaction_stats.users_upserted + batch_stats.users_upserted,
                errors=reaction_stats.errors + batch_stats.errors,
            )
            total_persisted += len(buf)
            buf.clear()

        # Inventory / layout-migrate prep: stop once we have paged through the
        # oldest missing message_id (ids decrease as we go older).
        if stop_at_message_id is not None and min_id_seen is not None and min_id_seen <= stop_at_message_id:
            complete = True
            stopped_at_target = True
            break

        if per_peer_limit > 0 and total_persisted >= per_peer_limit:
            hit_limit = True
            break

        if debug_capture is not None and debug_capture.is_full:
            hit_limit = True
            break

    # Iterator exhausted with no early stop → reached the start of history (or
    # the empty slice below). That completes the gap; coverage can be written.
    if not hit_limit and not complete and not stopped_at_target:
        complete = True

    if buf:
        batch_stats = await _flush_message_batch(
            client,
            conn,
            buf,
            peer_entity=peer_entity,
            peer_external_id=peer.peer_external_id,
            peer_type=peer.peer_type,
            harvester_id=harvester_id,
        )
        reaction_stats = ReactionHarvestStats(
            lists_fetched=reaction_stats.lists_fetched + batch_stats.lists_fetched,
            lists_skipped=reaction_stats.lists_skipped + batch_stats.lists_skipped,
            reactors_persisted=reaction_stats.reactors_persisted + batch_stats.reactors_persisted,
            users_upserted=reaction_stats.users_upserted + batch_stats.users_upserted,
            errors=reaction_stats.errors + batch_stats.errors,
        )
        total_persisted += len(buf)
        buf.clear()

    if min_id_seen is None:
        # No messages matched this slice; treat as complete (nothing more to page).
        complete = True
    # Hit per_peer_limit / debug cap: cursor stays incomplete so later runs can
    # continue, but we still record coverage for the dates we actually pulled
    # (same date window as the early-stop limit).
    partial_limit_window = False
    if hit_limit:
        complete = False
        stopped_at_target = False
        partial_limit_window = total_persisted > 0 and min_date_seen is not None

    if not ignore_coverage:
        await upsert_cursor(
            conn,
            harvester_id=harvester_id,
            peer_external_id=peer.peer_external_id,
            peer_type=peer.peer_type,
            window_before=before,
            window_after=after,
            next_offset_id=min_id_seen,
            is_complete=complete,
            last_message_id_seen=last_id_seen,
            last_message_date_seen=min_date_seen,
            replace_open_ended_siblings=replace_open_ended_siblings,
        )

    if not complete and not partial_limit_window:
        return total_persisted, None, reaction_stats

    gap_end = ensure_utc(before)
    if stopped_at_target or partial_limit_window:
        # Only claim dates we actually pulled — do not mark the full −∞ gap done.
        gap_start = ensure_utc(min_date_seen)
        if gap_start is None:
            return total_persisted, None, reaction_stats
        _lg().info(
            "%s for %s %s | coverage [%s, %s) persisted=%s",
            (
                f"Early-stop at message_id<={stop_at_message_id}"
                if stopped_at_target
                else f"Partial limit stop (limit={per_peer_limit})"
            ),
            peer.peer_type,
            peer.peer_external_id,
            gap_start,
            gap_end,
            total_persisted,
        )
    else:
        # Mark the full requested gap covered, not just [min_date, max_date).
        gap_start = ensure_utc(after)

    if gap_start is not None and gap_start >= gap_end:
        return total_persisted, None, reaction_stats

    return (
        total_persisted,
        CoverageInterval(covered_after=gap_start, covered_before=gap_end),
        reaction_stats,
    )
