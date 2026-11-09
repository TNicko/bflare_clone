"""Shared Quickwit index naming for keyword search over telegram messages."""

from __future__ import annotations

# Physical index id. Bump it and reindex to migrate.
MESSAGES_INDEX = "messages"

# Config format version — keep aligned with the Quickwit server image (infra/quickwit default 0.9.0).
INDEX_CONFIG_VERSION = "0.9"
