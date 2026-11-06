"""update_record action target: a real Unity Catalog Delta table written through the SQL warehouse.

The most Databricks-native action in the app. Sidekick proposes an account update during a call; on approval the
ActionService runs a **parameterized MERGE** against `${CRM_CATALOG}.${CRM_SCHEMA}.${CRM_TABLE}` through the SQL
Statement Execution API, authenticated as the app's service principal (the same executor the Insights page uses).

Safety and honesty:
- Identifiers (catalog, schema, table, column names) are validated against a strict pattern and never come from a
  request. Every value is a bound parameter (`:name`), so a note or account id can never inject SQL.
- No warehouse configured -> outbox mode: the exact MERGE and its parameters are rendered and stored, nothing runs.
  Mirrors the email/invite outbox behaviour, so a local or un-provisioned demo still shows what *would* run.
- Undo restores the previous row (captured by a SELECT before the MERGE), or deletes the row when the update
  created it. Only an update that actually executed against the warehouse is undoable.
"""

from __future__ import annotations

import re
from typing import Any, Awaitable, Callable

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")

# The account record Sidekick maintains. account_id is the merge key; the rest are updatable fields, plus a
# server-set updated_at_ms. Keep this in step with the DDL in src/notebooks/init_history.py (crm_accounts).
KEY_COLUMN = "account_id"
FIELD_COLUMNS: tuple[str, ...] = ("name", "stage", "arr", "owner", "next_step", "notes")
# SQL types for the Statement Execution API bound parameters.
COLUMN_TYPES: dict[str, str] = {
    "account_id": "STRING", "name": "STRING", "stage": "STRING", "arr": "DOUBLE",
    "owner": "STRING", "next_step": "STRING", "notes": "STRING", "updated_at_ms": "BIGINT",
}

# Executor: (statement, parameters {name: value}, types {name: sql type}) -> rows as dicts. Same shape as
# admin/insights.py, so the app reuses one make_sdk_executor over the SQL warehouse.
Executor = Callable[[str, dict[str, Any], dict[str, str]], Awaitable[list[dict[str, Any]]]]


class RecordError(ValueError):
    """The record target is misconfigured (bad identifier). Safe to show; refused before any SQL runs."""


class RecordTarget:
    """A validated `catalog.schema.table` the update_record action may write to."""

    def __init__(self, catalog: str, schema: str, table: str) -> None:
        for label, value in (("CRM_CATALOG", catalog), ("CRM_SCHEMA", schema), ("CRM_TABLE", table)):
            if not IDENT.match(value or ""):
                raise RecordError(f"{label} must be a plain identifier, got {value!r}")
        self.catalog, self.schema, self.table = catalog, schema, table

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def build_merge(target: RecordTarget, account_id: str, fields: dict[str, Any],
                updated_at_ms: int) -> tuple[str, dict[str, Any], dict[str, str]]:
    """A parameterized MERGE that updates the given fields of one account (insert if new). Only known field
    columns are written; the caller has already validated them. Returns (sql, params, types)."""
    set_cols = [c for c in FIELD_COLUMNS if c in fields]
    params: dict[str, Any] = {"account_id": account_id, "updated_at_ms": updated_at_ms}
    for c in set_cols:
        params[c] = fields[c]
    types = {k: COLUMN_TYPES[k] for k in params}
    set_clause = ", ".join(f"{c} = :{c}" for c in set_cols) + ", updated_at_ms = :updated_at_ms"
    insert_cols = ["account_id", *set_cols, "updated_at_ms"]
    insert_vals = ", ".join(f":{c}" for c in insert_cols)
    sql = (
        f"MERGE INTO {target.full_name} AS t "
        f"USING (SELECT :account_id AS account_id) AS s ON t.{KEY_COLUMN} = s.{KEY_COLUMN} "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(insert_cols)}) VALUES ({insert_vals})"
    )
    return sql, params, types


def build_select(target: RecordTarget, account_id: str) -> tuple[str, dict[str, Any], dict[str, str]]:
    cols = ", ".join((KEY_COLUMN, *FIELD_COLUMNS, "updated_at_ms"))
    return (f"SELECT {cols} FROM {target.full_name} WHERE {KEY_COLUMN} = :account_id",
            {"account_id": account_id}, {"account_id": "STRING"})


def build_delete(target: RecordTarget, account_id: str) -> tuple[str, dict[str, Any], dict[str, str]]:
    return (f"DELETE FROM {target.full_name} WHERE {KEY_COLUMN} = :account_id",
            {"account_id": account_id}, {"account_id": "STRING"})


def render_sql(sql: str, params: dict[str, Any]) -> str:
    """A readable, non-executable rendering of a parameterized statement for the card preview / outbox. Values are
    quoted for display only; the real call always uses bound parameters, never this text."""
    out = sql
    for name, value in sorted(params.items(), key=lambda kv: -len(kv[0])):
        literal = "NULL" if value is None else (repr(value) if isinstance(value, str) else str(value))
        out = out.replace(f":{name}", literal)
    return out
