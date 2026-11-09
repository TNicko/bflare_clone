"""Alembic migration environment."""

from __future__ import annotations

import os
import re
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from db.metadata import metadata

# Import models so Table objects are registered on metadata.
import db.models  # noqa: F401

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = metadata

# Monthly RANGE partitions of telegram_messages (e.g. telegram_messages_p2024_12).
# Managed by SQL/cron, not Alembic models — reflection otherwise invents remove_fk noise.
_MSG_PARTITION_RE = re.compile(r"^telegram_messages_p")

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url() -> str:
    # docker run --env-file keeps shell quotes; Compose env_file often strips them.
    url = os.environ.get("POSTGRES_DSN", "").strip().strip("'").strip('"')
    if not url:
        raise RuntimeError("POSTGRES_DSN must be set for Alembic")
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    return url


def _fk_targets_message_partition(fk) -> bool:
    """True when a reflected FK points at a telegram_messages_* partition child."""
    for elem in fk.elements:
        colspec = getattr(elem, "_colspec", None)
        if isinstance(colspec, str):
            table_name = colspec.split(".", 1)[0]
            if _MSG_PARTITION_RE.match(table_name):
                return True
        try:
            tname = elem.column.table.name
        except Exception:
            continue
        if tname and _MSG_PARTITION_RE.match(tname):
            return True
    return False


def include_name(name, type_, parent_names):
    """Skip reflecting message partition children before they enter the compare."""
    if type_ == "table" and name and _MSG_PARTITION_RE.match(name):
        return False
    return True


def include_object(object, name, type_, reflected, compare_to):
    """Filter what autogenerate compares between the live DB and libs/db/models.

    Used only by ``alembic revision --autogenerate`` and ``alembic check``;
    upgrade/downgrade migrations are not affected.

    Return False to skip an object entirely (Alembic will not propose changes for it).

    1. Supabase ``auth`` schema — managed by Supabase, not this repo. We only
       reference ``auth.users`` via FKs in platform models; never migrate auth tables.

    2. Public tables that exist in prod/dev but are not yet in ``libs/db/models`` —
       without this, autogenerate would emit ``drop_table`` for every unmodeled table
       (e.g. telegram_*). Remove this branch once all prod tables are defined in models.

    3. ``telegram_messages_pYYYY_MM`` partition children + FKs that target them —
       Postgres/SQLAlchemy reflection expands the parent FK into per-partition FKs;
       Alembic would otherwise propose hundreds of ``remove_fk`` ops.
    """
    if type_ == "table" and getattr(object, "schema", None) == "auth":
        return False
    if type_ == "table" and name and _MSG_PARTITION_RE.match(name):
        return False
    if type_ == "table" and reflected and compare_to is None:
        return False
    if type_ == "foreign_key_constraint" and _fk_targets_message_partition(object):
        return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_name=include_name,
        include_object=include_object,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args={"prepare_threshold": None},
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
            include_object=include_object,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
