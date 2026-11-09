"""Register Table definitions on shared metadata (import all model modules here)."""

import db.models.auth.users  # noqa: F401 — FK target for profiles, projects, etc.

import db.models.peers.categories  # noqa: F401

import db.models.telegram.channel  # noqa: F401
import db.models.telegram.megagroup  # noqa: F401
import db.models.telegram.chat  # noqa: F401
import db.models.telegram.user  # noqa: F401
import db.models.telegram.peer_counts  # noqa: F401
import db.models.telegram.message  # noqa: F401
import db.models.telegram.image_blobs  # noqa: F401
import db.models.telegram.field_history  # noqa: F401
import db.models.telegram.harvesters  # noqa: F401
import db.models.telegram.discovery  # noqa: F401
import db.models.telegram.pipelines  # noqa: F401

import db.models.bluesky.actor  # noqa: F401
import db.models.bluesky.field_history  # noqa: F401
import db.models.bluesky.search_query_run  # noqa: F401

import db.models.system.outbox  # noqa: F401
import db.models.system.embedding_store  # noqa: F401
import db.models.system.jobs  # noqa: F401

import db.models.user.profiles  # noqa: F401
import db.models.user.messenger  # noqa: F401
import db.models.user.usage  # noqa: F401
import db.models.user.billing  # noqa: F401
import db.models.user.api_access  # noqa: F401

import db.models.projects.projects  # noqa: F401
import db.models.projects.features  # noqa: F401
