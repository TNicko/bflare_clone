from __future__ import annotations

import json
from typing import Any, Sequence
from uuid import UUID
from psycopg import sql
from pydantic import BaseModel


def build_upsert_sql(
    *,
    table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
    exclude_from_update: Sequence[str] | None = None,
) -> sql.Composed:
    immutable = set(conflict_columns)
    if exclude_from_update:
        immutable.update(exclude_from_update)
    update_cols = [c for c in columns if c not in immutable]

    updates = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col)) for col in update_cols
    )

    return sql.SQL(
        """
        INSERT INTO {table} ({fields})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_fields})
        DO UPDATE SET {updates}
        """
    ).format(
        table=sql.Identifier(table),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        conflict_fields=sql.SQL(", ").join(map(sql.Identifier, conflict_columns)),
        updates=updates,
    )


def adapt_param(value: Any) -> Any:
    """Adapt Python values for psycopg parameters (notably JSON)."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def uuid_param(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def adapt_jsonb_param(value: Any) -> Any:
    """Adapt a value for a PostgreSQL ``jsonb`` column (objects, arrays, or JSON scalars)."""
    if value is None:
        return value
    if isinstance(value, (dict, list, bool, int, float, str)):
        try:
            from psycopg.types.json import Json
        except Exception:  # pragma: no cover
            return json.dumps(value)
        return Json(value)
    return value


def schema_upsert_values(
    row: BaseModel,
    *,
    explicit_null_fields: frozenset[str],
) -> dict[str, Any]:
    """Build upsert columns from a peer schema row.

    Most ``None`` fields are omitted so partial upserts leave existing DB values
    unchanged. Unset fields (``exclude_unset``) are omitted entirely so unknown
    booleans are not written as false. Fields in ``explicit_null_fields`` are
    included as SQL NULL when they were set on the schema (e.g. Telegram returned
    no username, bio, or profile photo).
    """
    fields_set = row.model_fields_set
    dump = row.model_dump(mode="json", exclude={"id"}, exclude_unset=True)
    values: dict[str, Any] = {}
    for key, value in dump.items():
        if value is not None:
            values[key] = value
        elif key in explicit_null_fields and key in fields_set:
            values[key] = None
    return values
