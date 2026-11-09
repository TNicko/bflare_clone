"""On-demand Telethon resolve for forward-source peers missing from the DB."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from db.schemas.general.primitives import PeerTypeSchema
from services.api.src.crud.supabase.peers import get_peer_refresh_row
from services.api.src.message_peer_partials import (
    find_forward_sample_message_id,
    resolve_community_peer_by_external_id,
)
from services.api.src.models.community_insights import ForwardSourceInsight
from services.api.src.sync_peer_messages import fetch_and_upsert_messages_by_id

lg = logging.getLogger(__name__)


async def seed_community_from_stored_forward(
    client: TelegramClient,
    conn: Any,
    *,
    target_external_id: str,
    via_external_id: str,
    via_peer_type: PeerTypeSchema,
    harvester_id: UUID,
    s3_bucket: str = "",
    s3_region: str = "",
) -> bool:
    """Re-fetch a forward from a known ``via`` peer to seed Telethon's entity cache.

    Must be scoped to ``via_external_id`` so the query can use
    ``telegram_messages_source_date_idx`` (a global fwd_from scan times out).
    """
    via_row = await get_peer_refresh_row(conn, via_external_id, via_peer_type)
    if via_row is None:
        lg.info(
            "Forward via peer missing from DB target=%s via=%s",
            target_external_id,
            via_external_id,
        )
        return False
    if via_peer_type in ("channel", "megagroup") and via_row.get("access_hash") is None:
        lg.info(
            "Forward via peer has no access_hash target=%s via=%s",
            target_external_id,
            via_external_id,
        )
        return False

    sample_mid = await find_forward_sample_message_id(
        conn,
        source_external_id=via_external_id,
        target_external_id=target_external_id,
    )
    if sample_mid is None:
        lg.info(
            "No forward from target=%s in via=%s",
            target_external_id,
            via_external_id,
        )
        return False

    try:
        await fetch_and_upsert_messages_by_id(
            client,
            conn,
            external_id=via_external_id,
            peer_type=via_peer_type,
            access_hash=via_row.get("access_hash"),
            harvester_external_id=via_row.get("harvester_external_id"),
            message_ids=[sample_mid],
            harvester_id=harvester_id,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
        )
    except FloodWaitError:
        raise
    except ValueError as exc:
        lg.info(
            "Forward-seed via entity error target=%s via=%s: %s",
            target_external_id,
            via_external_id,
            exc,
        )
        return False
    except Exception:
        lg.exception(
            "Forward-seed message fetch failed target=%s via=%s mid=%s",
            target_external_id,
            via_external_id,
            sample_mid,
        )
        return False

    lg.info(
        "Seeded Telethon entity cache via forward target=%s via=%s mid=%s",
        target_external_id,
        via_external_id,
        sample_mid,
    )
    return True


async def enrich_forward_sources(
    client: TelegramClient,
    conn: Any,
    *,
    via_external_id: str,
    via_peer_type: PeerTypeSchema,
    sources: list[ForwardSourceInsight],
    harvester_id: UUID,
    via_access_hash: int | None,
    via_harvester_external_id: str | None,
    s3_bucket: str = "",
    s3_region: str = "",
) -> list[ForwardSourceInsight]:
    """Best-effort materialize missing forward-source peers before returning insights.

    For each source without a DB peer: re-fetch a sample message from ``via_*``
    that was forwarded from that source (seeds Telethon entity cache), then
    resolve + materialize. Private / inaccessible peers stay without ``peer``.
    Stops early on FloodWait (returns whatever was enriched so far).
    """
    if not sources:
        return sources

    out: list[ForwardSourceInsight] = []
    for item in sources:
        if item.peer is not None:
            out.append(item)
            continue
        try:
            peer = await _resolve_one_forward_source(
                client,
                conn,
                target_external_id=item.external_id,
                via_external_id=via_external_id,
                via_peer_type=via_peer_type,
                harvester_id=harvester_id,
                via_access_hash=via_access_hash,
                via_harvester_external_id=via_harvester_external_id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
        except FloodWaitError:
            lg.info(
                "Forward-source enrich stopped by FloodWait target=%s via=%s",
                item.external_id,
                via_external_id,
            )
            out.append(item)
            out.extend(sources[len(out) :])
            return out
        except Exception:
            lg.exception(
                "Forward-source enrich failed target=%s via=%s",
                item.external_id,
                via_external_id,
            )
            peer = None

        if peer is not None:
            out.append(item.model_copy(update={"peer": peer}))
        else:
            out.append(item)
    return out


async def _resolve_one_forward_source(
    client: TelegramClient,
    conn: Any,
    *,
    target_external_id: str,
    via_external_id: str,
    via_peer_type: PeerTypeSchema,
    harvester_id: UUID,
    via_access_hash: int | None,
    via_harvester_external_id: str | None,
    s3_bucket: str,
    s3_region: str,
):
    sample_mid = await find_forward_sample_message_id(
        conn,
        source_external_id=via_external_id,
        target_external_id=target_external_id,
    )
    if sample_mid is not None:
        try:
            await fetch_and_upsert_messages_by_id(
                client,
                conn,
                external_id=via_external_id,
                peer_type=via_peer_type,
                access_hash=via_access_hash,
                harvester_external_id=via_harvester_external_id,
                message_ids=[sample_mid],
                harvester_id=harvester_id,
                s3_bucket=s3_bucket,
                s3_region=s3_region,
            )
        except FloodWaitError:
            raise
        except ValueError as exc:
            lg.info(
                "Forward-via entity error target=%s via=%s: %s",
                target_external_id,
                via_external_id,
                exc,
            )
        except Exception:
            lg.exception(
                "Forward-via message fetch failed target=%s via=%s mid=%s",
                target_external_id,
                via_external_id,
                sample_mid,
            )

    peer, _attempt = await resolve_community_peer_by_external_id(
        client,
        conn,
        external_id=target_external_id,
        harvester_id=harvester_id,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )
    return peer
