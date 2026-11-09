from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class PlatformUser:
    id: UUID
    email: str | None = None
