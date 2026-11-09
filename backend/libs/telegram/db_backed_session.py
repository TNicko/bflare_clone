from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from telethon.crypto import AuthKey
from telethon.sessions import StringSession
from telethon.sessions.memory import MemorySession
from telethon.tl.types import updates

from db.crud.telegram.sessions import load_update_states, save_update_states


class DbBackedSession(MemorySession):
    def __init__(
        self,
        *,
        session_id: UUID,
        session_string: str,
        pending_update_states: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__()
        self._session_id = session_id
        self._dirty_update_states = False

        inner = StringSession(session_string)
        self.set_dc(inner.dc_id, inner.server_address, inner.port)
        self.auth_key = AuthKey(inner.auth_key)

        for row in pending_update_states or []:
            state_date = row["state_date"]
            if isinstance(state_date, datetime) and state_date.tzinfo is None:
                state_date = state_date.replace(tzinfo=UTC)
            self.set_update_state(
                int(row["entity_id"]),
                updates.State(
                    pts=int(row["pts"]),
                    qts=int(row["qts"]),
                    date=state_date,
                    seq=int(row["seq"]),
                    unread_count=0,
                ),
            )

    @classmethod
    async def load(
        cls,
        conn: Any,
        *,
        session_id: UUID,
        session_string: str,
    ) -> DbBackedSession:
        states = await load_update_states(conn, session_id)
        return cls(
            session_id=session_id,
            session_string=session_string,
            pending_update_states=states,
        )

    def set_update_state(self, entity_id, state) -> None:
        super().set_update_state(entity_id, state)
        self._dirty_update_states = True

    async def flush(self, conn: Any) -> None:
        if not self._dirty_update_states:
            return
        payload: list[tuple[int, int, int, int, datetime]] = []
        for entity_id, state in self.get_update_states():
            state_date = state.date
            if state_date.tzinfo is None:
                state_date = state_date.replace(tzinfo=UTC)
            else:
                state_date = state_date.astimezone(UTC)
            payload.append((int(entity_id), int(state.pts), int(state.qts), int(state.seq), state_date))
        await save_update_states(conn, self._session_id, payload)
        self._dirty_update_states = False

    def close(self) -> None:
        pass
