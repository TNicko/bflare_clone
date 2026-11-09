"""Telethon client helpers with DB-backed multi-session leasing."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import AuthKeyDuplicatedError
from telethon.sessions import StringSession

from db.connection import rollback_quiet
from db.crud.telegram.harvester import list_eligible_harvesters
from db.crud.telegram.sessions import (
    load_session_auth_strings,
    mark_session_auth_reset_required,
)
from db.schemas.telegram.harvester import TelegramHarvesterSchema
from db.schemas.telegram.harvester_session import (
    TelegramHarvesterSessionSchema,
)
from telegram.db_backed_session import DbBackedSession
from telegram.lease import (
    claim_or_join_session,
    heartbeat_session_lease,
    release_session_lease,
)
from telegram.session_env import HEARTBEAT_INTERVAL_SECONDS, validate_telegram_session_env
from utils.graceful_stop import install_sigterm_cancels_asyncio_tasks

logger = logging.getLogger(__name__)

SessionTypeArg = Literal["api", "system"]


@dataclass(frozen=True)
class HarvesterSessionContext:
    harvester: TelegramHarvesterSchema
    session: TelegramHarvesterSessionSchema


class HarvesterSessionTerminatedError(RuntimeError):
    """Raised when a Telethon session can no longer be used."""

    def __init__(
        self,
        *,
        harvester_id: UUID,
        session_id: UUID,
        external_id: str,
        reason: str,
    ) -> None:
        self.harvester_id = harvester_id
        self.session_id = session_id
        self.external_id = external_id
        self.reason = reason
        super().__init__(
            f"Harvester session terminated (peer_id={external_id}, "
            f"harvester_id={harvester_id}, session_id={session_id}): {reason}"
        )


async def mark_session_reset_required(
    conn: Any,
    session_id: UUID,
    *,
    error_message: str | None = None,
) -> None:
    await mark_session_auth_reset_required(conn, session_id, error_message=error_message)
    await conn.commit()
    logger.error(
        "Session id=%s marked auth_state=reset_required error=%s",
        session_id,
        error_message,
    )


async def _rollback_quiet(conn: Any, *, session_id: UUID | None = None) -> None:
    """Best-effort rollback so a poisoned transaction does not block later commands."""
    await rollback_quiet(
        conn,
        context=f"session_id={session_id}" if session_id is not None else "harvester client",
    )


def _select_eligible_harvester(
    pool: list[TelegramHarvesterSchema],
    *,
    harvester_external_id: str,
    harvester_index: int,
) -> TelegramHarvesterSchema:
    if harvester_external_id and harvester_external_id.strip():
        want = harvester_external_id.strip()
        for h in pool:
            if h.external_id == want:
                return h
        raise ValueError(f"Harvester external_id={want!r} not found among eligible harvesters")

    if harvester_index < 0 or harvester_index >= len(pool):
        raise ValueError(f"harvester_index={harvester_index} out of range (eligible harvesters: {len(pool)})")
    return pool[harvester_index]


async def _load_session_schema(conn: Any, session_id: UUID) -> TelegramHarvesterSessionSchema:
    from db.crud.telegram.sessions import get_session_by_id

    row = await get_session_by_id(conn, session_id)
    if row is None:
        raise ValueError(f"Session id={session_id} not found")
    return row


async def _notify_job_session_lease(*, session_id: UUID, server_id: str, held: bool) -> None:
    """Best-effort job-progress bookkeeping so the agent can release after SIGKILL."""
    try:
        from jobs.progress import drop_session_lease, note_session_lease
    except ImportError:
        return
    try:
        if held:
            await note_session_lease(session_id=session_id, server_id=server_id)
        else:
            await drop_session_lease(session_id=session_id, server_id=server_id)
    except Exception:
        logger.exception(
            "Failed to update job session lease progress session_id=%s held=%s",
            session_id,
            held,
        )


def _dsn_from_connection(conn: Any) -> str:
    """Rebuild a connectable DSN from an open psycopg connection (includes password)."""
    from psycopg.conninfo import make_conninfo

    params = dict(conn.info.get_parameters())
    password = conn.info.password
    if password:
        params["password"] = password
    return make_conninfo(**params)


async def _open_heartbeat_connection(dsn: str) -> Any:
    """Open a sibling AsyncConnection for lease heartbeats.

    Heartbeats run on a background task and must not share the worker connection:
    an explicit ``commit()`` while the worker holds ``conn.transaction()`` raises
    ProgrammingError, and concurrent use of one psycopg connection is unsafe.
    """
    from psycopg import AsyncConnection

    return await AsyncConnection.connect(dsn, prepare_threshold=None)


@asynccontextmanager
async def authorized_harvester_client(
    conn: Any,
    *,
    telegram_api_id: int,
    telegram_api_hash: str,
    session_type: SessionTypeArg = "system",
    location_id: str,
    server_id: str,
    harvester_external_id: str = "",
    harvester_index: int = 0,
    allow_api_fallback: bool = False,
) -> AsyncIterator[tuple[TelegramClient, TelegramHarvesterSchema]]:
    """Connected Telethon client with session lease claim/join and heartbeat.

    On SIGTERM (fleet job cancel) asyncio tasks are cancelled so this context
    exits and ``finally`` releases the holder, marking the session ``inactive``
    when no holders remain.
    """
    validate_telegram_session_env()
    if telegram_api_id <= 0 or not telegram_api_hash.strip():
        raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")

    pool = await list_eligible_harvesters(conn, session_type=session_type)
    if not pool and session_type == "api" and allow_api_fallback:
        pool = await list_eligible_harvesters(conn, session_type="system")
        session_type = "system"  # type: ignore[assignment]

    if not pool:
        raise ValueError(
            f"No eligible harvesters with healthy {session_type!r} sessions "
            "(need is_active=true and auth_state=healthy)"
        )

    harvester = _select_eligible_harvester(
        pool,
        harvester_external_id=harvester_external_id,
        harvester_index=harvester_index,
    )

    session_id = await claim_or_join_session(
        conn,
        harvester_id=harvester.id,
        session_type=session_type,
        location_id=location_id,
        server_id=server_id,
    )
    # SIGTERM must cancel this task so this ``finally`` runs (lease → inactive).
    install_sigterm_cancels_asyncio_tasks()

    db_session: DbBackedSession | None = None
    heartbeat_task: asyncio.Task[None] | None = None
    stop_heartbeat = asyncio.Event()
    heartbeat_dsn = _dsn_from_connection(conn)

    async def _heartbeat_loop() -> None:
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
                break
            except TimeoutError:
                pass
            try:
                # Dedicated connection — same pattern as ApiSessionPool — so we never
                # commit/query on ``conn`` while the worker holds a transaction.
                async with await _open_heartbeat_connection(heartbeat_dsn) as hb_conn:
                    alive = await heartbeat_session_lease(
                        hb_conn,
                        session_id=session_id,
                        harvester_id=harvester.id,
                        location_id=location_id,
                        server_id=server_id,
                    )
                if not alive:
                    break
            except Exception:
                logger.exception("Session heartbeat failed session_id=%s", session_id)

    try:
        # Persist before Telethon connect so SIGKILL still has something to release.
        await _notify_job_session_lease(session_id=session_id, server_id=server_id, held=True)
        await _load_session_schema(conn, session_id)
        session_string = (await load_session_auth_strings(conn, session_id))[0]
        db_session = await DbBackedSession.load(
            conn,
            session_id=session_id,
            session_string=session_string,
        )
        # Workers are request/response only; live updates trigger GetDifference catch-up
        # that starves the same MTProto connection used for harvest RPCs.
        client = TelegramClient(
            db_session,
            telegram_api_id,
            telegram_api_hash,
            receive_updates=False,
        )
        # Use connect() — never async with / start(), which interactively asks for
        # a phone when a leased DB session is missing auth (wrong API id/hash, stale
        # string, etc.). Workers must fail closed, not prompt on stdin.
        await client.connect()
        try:
            if not await client.is_user_authorized():
                reason = f"Session not authorized (harvester={harvester.external_id}, session={session_id})"
                await _rollback_quiet(conn, session_id=session_id)
                await mark_session_reset_required(conn, session_id, error_message=reason)
                raise ValueError(reason)
            heartbeat_task = asyncio.create_task(_heartbeat_loop())
            yield client, harvester
        finally:
            if client.is_connected():
                await client.disconnect()
    except AuthKeyDuplicatedError as exc:
        await _rollback_quiet(conn, session_id=session_id)
        await mark_session_reset_required(
            conn,
            session_id,
            error_message=("Telegram auth key was used from multiple IPs; reset the session and connect again"),
        )
        raise HarvesterSessionTerminatedError(
            harvester_id=harvester.id,
            session_id=session_id,
            external_id=harvester.external_id,
            reason=("Telegram auth key was used from multiple IPs; reset the session and connect again"),
        ) from exc
    finally:
        stop_heartbeat.set()
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        # Clear aborted/open worker transactions so flush/release can run, and so
        # cleanup failures do not mask the original exception from the with-body.
        await _rollback_quiet(conn, session_id=session_id)
        try:
            if db_session is not None:
                await db_session.flush(conn)
            await release_session_lease(conn, session_id=session_id, server_id=server_id)
        except Exception:
            logger.exception(
                "Failed session cleanup (flush/release) session_id=%s",
                session_id,
            )
            await _rollback_quiet(conn, session_id=session_id)
        await _notify_job_session_lease(session_id=session_id, server_id=server_id, held=False)


async def connect_client_for_session(
    conn: Any,
    *,
    telegram_api_id: int,
    telegram_api_hash: str,
    session_id: UUID,
) -> tuple[TelegramClient, DbBackedSession]:
    """Connect without claiming (caller already holds lease). Used by API session pool."""
    session_string = (await load_session_auth_strings(conn, session_id))[0]
    db_session = await DbBackedSession.load(
        conn,
        session_id=session_id,
        session_string=session_string,
    )
    # API pool clients are request/response only (resolve/refresh/search); do not
    # subscribe to the live update stream on long-lived warm connections.
    client = TelegramClient(
        db_session,
        telegram_api_id,
        telegram_api_hash,
        receive_updates=False,
    )
    await client.connect()
    try:
        authorized = await client.is_user_authorized()
    except Exception:
        await _disconnect_client(client)
        raise
    if not authorized:
        await _disconnect_client(client)
        reason = f"Session id={session_id} not authorized"
        # Leave auth_state unhealthy so peer lookups skip this account and use
        # another harvester's own access hash.
        await mark_session_reset_required(conn, session_id, error_message=reason)
        raise ValueError(reason)
    return client, db_session


async def _disconnect_client(client: TelegramClient) -> None:
    try:
        if client.is_connected():
            disconnect_result = client.disconnect()
            if disconnect_result is not None:
                await disconnect_result
    except Exception:
        logger.exception("Failed to disconnect unauthorized Telegram client")


def session_string_from_telethon_client(client: TelegramClient) -> str:
    session = client.session
    if session is None:
        raise ValueError("Telegram client has no session")
    return StringSession.save(session)
