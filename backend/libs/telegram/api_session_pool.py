from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable, Collection
from dataclasses import dataclass, field
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from telethon import TelegramClient

from db.crud.telegram.forward_graph_coverage import list_harvester_ids_with_active_system_session
from db.crud.telegram.harvester import get_harvester_by_id, list_active_harvesters
from db.crud.telegram.sessions import get_session_by_id
from db.schemas.telegram.harvester_session import SessionTypeSchema
from telegram.client import connect_client_for_session
from telegram.db_backed_session import DbBackedSession
from telegram.harvester_logging import harvester_display_name
from utils.harvester_log import pop_harvester_log_label, push_harvester_log_label
from utils.request_spans import record_span
from telegram.harvester_metrics import observe_work_finished, observe_work_started
from telegram.lease import (
    NoAvailableSessionError,
    claim_or_join_session,
    heartbeat_session_lease,
    release_session_lease,
)
from telegram.session_env import (
    HEARTBEAT_INTERVAL_SECONDS,
    get_server_id,
    get_session_location_id,
    validate_telegram_session_env,
)

GetConn = Callable[[], AbstractAsyncContextManager[AsyncConnection[DictRow]]]

logger = logging.getLogger(__name__)

API_SESSION_TYPE: SessionTypeSchema = "api"


class PooledClientLease:
    """In-flight occupancy for one harvester. Rebind when the request switches sessions."""

    __slots__ = ("_pool", "client", "harvester_id", "access_hash", "_held_id")

    def __init__(self, pool: ApiSessionPool | None) -> None:
        self._pool = pool
        self.client: TelegramClient | None = None
        self.harvester_id: UUID | None = None
        self.access_hash: int | None = None
        self._held_id: UUID | None = None

    def bind(
        self,
        client: TelegramClient | None,
        harvester_id: UUID | None,
        access_hash: int | None = None,
    ) -> None:
        if self._pool is None or client is None or harvester_id is None:
            if self._held_id is not None and self._pool is not None:
                self._pool._end_work(self._held_id)
            self._held_id = None
            self.client = client
            self.harvester_id = harvester_id
            self.access_hash = access_hash
            return
        if self._held_id is not None and self._held_id != harvester_id:
            self._pool._end_work(self._held_id)
            self._held_id = None
        self.client = client
        self.harvester_id = harvester_id
        self.access_hash = access_hash
        if self._held_id is None:
            self._pool._begin_work(harvester_id)
            self._held_id = harvester_id

    def release(self) -> None:
        if self._held_id is not None and self._pool is not None:
            self._pool._end_work(self._held_id)
        self._held_id = None
        self.client = None
        self.harvester_id = None
        self.access_hash = None

    async def __aenter__(self) -> PooledClientLease:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self.release()


@dataclass
class PooledSession:
    harvester_id: UUID
    external_id: str
    session_id: UUID
    client: TelegramClient
    db_session: DbBackedSession
    display_name: str = ""
    heartbeat_task: asyncio.Task[None] | None = None
    stop_heartbeat: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class ApiSessionPool:
    location_id: str
    server_id: str
    telegram_api_id: int
    telegram_api_hash: str
    get_conn: GetConn
    sessions: dict[UUID, PooledSession] = field(default_factory=dict)
    failures: dict[UUID, float] = field(default_factory=dict)
    in_flight: dict[UUID, int] = field(default_factory=dict)
    display_names: dict[UUID, str] = field(default_factory=dict)
    negative_cache_seconds: float = 60.0

    def _display_name(self, harvester_id: UUID) -> str:
        pooled = self.sessions.get(harvester_id)
        if pooled is not None and pooled.display_name:
            return pooled.display_name
        return self.display_names.get(harvester_id, str(harvester_id))

    def _begin_work(self, harvester_id: UUID) -> None:
        count = self.in_flight.get(harvester_id, 0) + 1
        self.in_flight[harvester_id] = count
        name = self._display_name(harvester_id)
        observe_work_started(harvester_id, name)
        push_harvester_log_label(name)

    def _end_work(self, harvester_id: UUID) -> None:
        remaining = self.in_flight.get(harvester_id, 0) - 1
        if remaining <= 0:
            self.in_flight.pop(harvester_id, None)
            remaining = 0
        else:
            self.in_flight[harvester_id] = remaining
        name = self._display_name(harvester_id)
        observe_work_finished(harvester_id, name)
        pop_harvester_log_label()

    def _work_count(self, harvester_id: UUID) -> int:
        return self.in_flight.get(harvester_id, 0)

    def hold(self) -> PooledClientLease:
        return PooledClientLease(self)

    def _connected_candidates(self, excluded: set[str]) -> list[PooledSession]:
        return [pooled for pooled in self.sessions.values() if pooled.external_id not in excluded]

    def _idle_connected(self, excluded: set[str]) -> PooledSession | None:
        for pooled in self._connected_candidates(excluded):
            if self._work_count(pooled.harvester_id) == 0:
                return pooled
        return None

    def _least_busy_connected(self, excluded: set[str]) -> PooledSession | None:
        busy = self._connected_candidates(excluded)
        if not busy:
            return None
        return min(busy, key=lambda pooled: self._work_count(pooled.harvester_id))

    async def _heartbeat_loop(self, pooled: PooledSession) -> None:
        while not pooled.stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(
                    pooled.stop_heartbeat.wait(),
                    timeout=HEARTBEAT_INTERVAL_SECONDS,
                )
                break
            except TimeoutError:
                pass
            try:
                async with self.get_conn() as conn:
                    alive = await heartbeat_session_lease(
                        conn,
                        session_id=pooled.session_id,
                        harvester_id=pooled.harvester_id,
                        location_id=self.location_id,
                        server_id=self.server_id,
                    )
                if not alive:
                    await self._drop_pooled(pooled.harvester_id, disconnect=True)
                    break
            except Exception:
                logger.exception("API pool heartbeat failed harvester_id=%s", pooled.harvester_id)

    async def _drop_pooled(self, harvester_id: UUID, *, disconnect: bool) -> None:
        pooled = self.sessions.pop(harvester_id, None)
        if pooled is None:
            return
        pooled.stop_heartbeat.set()
        if pooled.heartbeat_task is not None:
            pooled.heartbeat_task.cancel()
            try:
                await pooled.heartbeat_task
            except asyncio.CancelledError:
                pass
        async with self.get_conn() as conn:
            if disconnect and pooled.client.is_connected():
                try:
                    await pooled.db_session.flush(conn)
                except Exception:
                    logger.exception(
                        "Failed to flush session update states harvester_id=%s session_id=%s",
                        harvester_id,
                        pooled.session_id,
                    )
                disconnect_result = pooled.client.disconnect()
                if disconnect_result is not None:
                    await disconnect_result
            await release_session_lease(
                conn,
                session_id=pooled.session_id,
                server_id=self.server_id,
            )

    async def drop_harvester(self, harvester_id: UUID) -> None:
        """Evict a harvester from the warm pool (e.g. after ``is_active=false``)."""
        self.failures[harvester_id] = time.monotonic()
        await self._drop_pooled(harvester_id, disconnect=True)
        logger.warning("Dropped harvester from API session pool harvester_id=%s", harvester_id)

    async def warm_up(self) -> None:
        validate_telegram_session_env()
        async with self.get_conn() as conn:
            harvesters = await list_active_harvesters(conn)
            for harvester in harvesters:
                try:
                    await self._connect_harvester(conn, harvester.id)
                except NoAvailableSessionError:
                    logger.error(
                        "No api session for active harvester id=%s external_id=%s",
                        harvester.id,
                        harvester.external_id,
                    )
                    self.failures[harvester.id] = time.monotonic()
                except Exception:
                    logger.exception("Failed to warm api session for harvester id=%s", harvester.id)
                    self.failures[harvester.id] = time.monotonic()

    async def _connect_harvester(
        self,
        conn: AsyncConnection[DictRow],
        harvester_id: UUID,
    ) -> None:
        if harvester_id in self.sessions:
            return
        session_id = await claim_or_join_session(
            conn,
            harvester_id=harvester_id,
            session_type=API_SESSION_TYPE,
            location_id=self.location_id,
            server_id=self.server_id,
        )

        try:
            harvester = await get_harvester_by_id(conn, harvester_id)
            if harvester is None:
                raise ValueError(f"Harvester id={harvester_id} not found")
            client, db_session = await connect_client_for_session(
                conn,
                telegram_api_id=self.telegram_api_id,
                telegram_api_hash=self.telegram_api_hash,
                session_id=session_id,
            )
        except Exception:
            try:
                await release_session_lease(
                    conn,
                    session_id=session_id,
                    server_id=self.server_id,
                )
            except Exception:
                logger.exception(
                    "Failed to release session after connect failure session_id=%s harvester_id=%s",
                    session_id,
                    harvester_id,
                )
            raise
        display_name = harvester_display_name(harvester)
        self.display_names[harvester_id] = display_name
        pooled = PooledSession(
            harvester_id=harvester_id,
            external_id=str(harvester.external_id),
            session_id=session_id,
            client=client,
            db_session=db_session,
            display_name=display_name,
        )
        pooled.heartbeat_task = asyncio.create_task(self._heartbeat_loop(pooled))
        self.sessions[harvester_id] = pooled
        self.failures.pop(harvester_id, None)
        await get_session_by_id(conn, session_id)

    async def get_client(self, conn: AsyncConnection[DictRow], harvester_id: UUID) -> TelegramClient | None:
        pooled = self.sessions.get(harvester_id)
        if pooled is not None:
            return pooled.client

        failed_at = self.failures.get(harvester_id)
        if failed_at is not None and (time.monotonic() - failed_at) < self.negative_cache_seconds:
            return None

        try:
            await self._connect_harvester(conn, harvester_id)
        except NoAvailableSessionError:
            logger.error("No api session available for harvester id=%s", harvester_id)
            self.failures[harvester_id] = time.monotonic()
            return None
        except Exception:
            logger.exception("Lazy api session connect failed harvester id=%s", harvester_id)
            self.failures[harvester_id] = time.monotonic()
            return None

        pooled = self.sessions.get(harvester_id)
        return pooled.client if pooled else None

    async def get_any_client(
        self,
        conn: AsyncConnection[DictRow],
        *,
        exclude_external_ids: Collection[str] | None = None,
    ) -> tuple[TelegramClient | None, UUID | None]:
        """Pick an API client, preferring harvesters that are not currently working.

        Does not mark occupancy; callers that will issue Telethon RPCs must use
        ``acquire_any_client`` (or bind a ``PooledClientLease``) so other
        requests can skip a busy session.

        Preference: idle connected → connect an unused harvester → least-busy
        connected. ``exclude_external_ids`` skips harvester accounts whose
        Telegram ``external_id`` is in the set (avoid ``is_self`` when the peer
        being resolved is itself a harvester).
        """
        excluded = {str(x) for x in (exclude_external_ids or ()) if x is not None and str(x)}

        try:
            system_busy = await list_harvester_ids_with_active_system_session(conn)
        except Exception:
            logger.exception("Failed to read active system session leases")
            system_busy = set()

        def _prefer_free_system(candidates: list[PooledSession]) -> list[PooledSession]:
            free = [c for c in candidates if c.harvester_id not in system_busy]
            return free or candidates

        idle_pool = _prefer_free_system(
            [
                pooled
                for pooled in self.sessions.values()
                if str(pooled.external_id) not in excluded and self._work_count(pooled.harvester_id) == 0
            ]
        )
        if idle_pool:
            pick = idle_pool[0]
            return pick.client, pick.harvester_id

        for harvester in await list_active_harvesters(conn):
            if str(harvester.external_id) in excluded:
                continue
            if harvester.id in self.sessions:
                continue
            if self._work_count(harvester.id) > 0:
                continue
            if harvester.id in system_busy:
                continue
            client = await self.get_client(conn, harvester.id)
            if client is not None:
                return client, harvester.id

        # Fall back: allow system-busy accounts if nothing else is free.
        for harvester in await list_active_harvesters(conn):
            if str(harvester.external_id) in excluded:
                continue
            if harvester.id in self.sessions:
                continue
            if self._work_count(harvester.id) > 0:
                continue
            client = await self.get_client(conn, harvester.id)
            if client is not None:
                return client, harvester.id

        idle = self._idle_connected(excluded)
        if idle is not None and idle.harvester_id not in system_busy:
            return idle.client, idle.harvester_id

        idle = self._idle_connected(excluded)
        if idle is not None:
            return idle.client, idle.harvester_id

        busy = self._least_busy_connected(excluded)
        if busy is not None:
            return busy.client, busy.harvester_id

        if excluded:
            logger.warning(
                "No API client available after excluding harvester external_ids=%s",
                sorted(excluded),
            )
        return None, None

    async def get_client_for_peer_role(
        self,
        conn: AsyncConnection[DictRow],
        *,
        harvester_id: UUID | None,
        access_hash: int | None,
        exclude_external_ids: Collection[str] | None = None,
    ) -> tuple[TelegramClient | None, UUID | None, int | None]:
        """Return a client that is safe to use with ``access_hash``.

        Access hashes are unique per harvester account. When a hash is present,
        only that harvester's session may use it — never substitute another
        harvester via ``get_any_client``. If the owning session is unavailable,
        returns ``(None, harvester_id, access_hash)`` so callers skip hash-based
        RPCs instead of pairing the hash with the wrong account.

        When ``access_hash`` is None (username / discovery paths), falls back to
        any available API client and returns ``access_hash=None``. Pass
        ``exclude_external_ids`` to avoid picking the peer's own harvester
        account (see ``get_any_client``).

        Does not mark occupancy; use ``acquire_client_for_peer_role`` (or bind a
        ``PooledClientLease``) around Telethon work.
        """
        if access_hash is not None:
            if harvester_id is None:
                logger.warning("Refusing Telethon client: access_hash set without harvester_id")
                return None, None, None
            client = await self.get_client(conn, harvester_id)
            if client is None:
                logger.warning(
                    "Owning harvester session unavailable for access_hash "
                    "harvester_id=%s; not falling back to another harvester",
                    harvester_id,
                )
            return client, harvester_id, access_hash

        client, any_id = await self.get_any_client(conn, exclude_external_ids=exclude_external_ids)
        return client, any_id, None

    @asynccontextmanager
    async def acquire_any_client(
        self,
        conn: AsyncConnection[DictRow],
        *,
        exclude_external_ids: Collection[str] | None = None,
    ) -> AsyncIterator[tuple[TelegramClient | None, UUID | None]]:
        """Pick a client (idle preferred) and mark that harvester in-flight until exit."""
        client, harvester_id = await self.get_any_client(
            conn,
            exclude_external_ids=exclude_external_ids,
        )
        if harvester_id is not None and client is not None:
            self._begin_work(harvester_id)
        started = time.perf_counter()
        try:
            yield client, harvester_id
        finally:
            record_span("telegram", "acquire_any_client", time.perf_counter() - started)
            if harvester_id is not None and client is not None:
                self._end_work(harvester_id)

    @asynccontextmanager
    async def acquire_client_for_peer_role(
        self,
        conn: AsyncConnection[DictRow],
        *,
        harvester_id: UUID | None,
        access_hash: int | None,
        exclude_external_ids: Collection[str] | None = None,
    ) -> AsyncIterator[tuple[TelegramClient | None, UUID | None, int | None]]:
        """Like ``get_client_for_peer_role``, marking the chosen harvester in-flight.

        Hash-bound work still uses the owning harvester even if it is busy; the
        occupancy count lets other (hash-independent) work prefer an idle one.
        """
        if access_hash is not None:
            if harvester_id is None:
                logger.warning("Refusing Telethon client: access_hash set without harvester_id")
                yield None, None, None
                return
            self._begin_work(harvester_id)
            try:
                client = await self.get_client(conn, harvester_id)
                if client is None:
                    logger.warning(
                        "Owning harvester session unavailable for access_hash "
                        "harvester_id=%s; not falling back to another harvester",
                        harvester_id,
                    )
                started = time.perf_counter()
                try:
                    yield client, harvester_id, access_hash
                finally:
                    record_span(
                        "telegram",
                        "acquire_client_for_peer_role",
                        time.perf_counter() - started,
                    )
            finally:
                self._end_work(harvester_id)
            return

        async with self.acquire_any_client(conn, exclude_external_ids=exclude_external_ids) as (
            client,
            any_id,
        ):
            yield client, any_id, None

    async def shutdown(self) -> None:
        for harvester_id in list(self.sessions.keys()):
            await self._drop_pooled(harvester_id, disconnect=True)


def create_api_session_pool(
    *,
    telegram_api_id: int,
    telegram_api_hash: str,
    get_conn: GetConn,
) -> ApiSessionPool:
    return ApiSessionPool(
        location_id=get_session_location_id(),
        server_id=get_server_id(service_name="api"),
        telegram_api_id=telegram_api_id,
        telegram_api_hash=telegram_api_hash,
        get_conn=get_conn,
    )
