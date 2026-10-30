"""Pure helpers for reconcile_history.py: the Lakebase -> Delta table map, the SQL the job sends, the watermark.

No Spark or dbutils imports, so tests/notebooks can check every statement the job runs (parity with
src/app/history/schema.py and the Lakebase DDL, MERGE keys, dedupe, watermark).

Why: Zerobus (src/app/history) is the low-latency path into the Delta history tables. It is at-least-once and
best effort, so it can duplicate rows and can drop rows. Lakebase (Postgres, the live store) holds every record
under the same natural keys. This job reads Lakebase through the registered Unity Catalog catalog
(`<lakebase_catalog>.<lakebase_schema>.<table>`, serverless compute) and MERGEs into the Delta tables by key.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

MS_PER_HOUR = 3_600_000
IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")


@dataclass(frozen=True)
class TableSpec:
    """One history table. `columns` is the Delta column list in history/schema.py order."""

    name: str
    columns: tuple[tuple[str, str], ...]
    keys: tuple[str, ...]                     # natural key (one row per key after reconcile)
    order: str                                # ORDER BY that picks the surviving row when a key has duplicates
    watermark: str                            # Lakebase time expression in epoch ms, compared with the cutoff
    literals: tuple[tuple[str, str], ...] = ()  # Delta column -> SQL literal, for columns Lakebase does not have
    preserve: tuple[str, ...] = ()            # columns Zerobus owns: inserted, never updated from Lakebase
    linked: tuple[str, str, str] | None = None  # (child table, parent key column, child time): a child change counts


TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        name="calls",
        columns=(("call_id", "STRING"), ("playbook", "STRING"), ("started_at", "BIGINT"), ("ended_at", "BIGINT"),
                 ("customer_id", "STRING"), ("status", "STRING"), ("mode", "STRING"), ("script", "STRING"),
                 ("owner", "STRING")),
        keys=("call_id",),
        # Zerobus writes a start row and an end row per call: the end row (ended_at set) wins.
        order="ended_at DESC NULLS LAST",
        watermark="GREATEST(started_at, ended_at)",
    ),
    TableSpec(
        name="utterances",
        columns=(("call_id", "STRING"), ("seq", "INT"), ("speaker", "STRING"), ("text", "STRING"),
                 ("t_start_ms", "BIGINT"), ("t_end_ms", "BIGINT"), ("emitted_at_ms", "BIGINT"),
                 ("playbook", "STRING")),
        keys=("call_id", "seq"),
        order="emitted_at_ms",
        watermark="emitted_at_ms",
    ),
    TableSpec(
        name="enrichments",
        columns=(("call_id", "STRING"), ("seq", "INT"), ("speaker", "STRING"), ("text", "STRING"),
                 ("intent", "STRING"), ("sentiment", "DOUBLE"), ("risk_flags", "STRING"), ("objection", "STRING"),
                 ("entities", "STRING"), ("methodology_hits", "STRING"), ("emitted_at_ms", "BIGINT"),
                 ("enriched_at_ms", "BIGINT"), ("latency_ms", "BIGINT"), ("enrichment_source", "STRING")),
        keys=("call_id", "seq"),
        order="enriched_at_ms",
        watermark="enriched_at_ms",
        # Lakebase has no enrichment_source column. The direct backbone enriches in process, so its source is 'direct'.
        literals=(("enrichment_source", "'direct'"),),
        preserve=("enrichment_source",),
    ),
    TableSpec(
        name="cards",
        columns=(("card_id", "STRING"), ("call_id", "STRING"), ("seq", "INT"), ("kind", "STRING"), ("title", "STRING"),
                 ("body", "STRING"), ("confidence", "DOUBLE"), ("source_doc", "STRING"),
                 ("source_clause", "STRING"), ("trigger", "STRING"), ("status", "STRING"),
                 ("created_at_ms", "BIGINT"), ("trace", "STRING")),
        keys=("card_id",),
        # Prefer a feedback-updated status over the 'shown' copy when Zerobus wrote the card twice.
        order="CASE WHEN status = 'shown' THEN 1 ELSE 0 END",
        watermark="created_at_ms",
        # feedback updates cards.status in Lakebase, so a card with recent feedback is in the window too.
        linked=("feedback", "card_id", "at_ms"),
    ),
    TableSpec(
        name="feedback",
        columns=(("card_id", "STRING"), ("call_id", "STRING"), ("action", "STRING"), ("edited_text", "STRING"),
                 ("at_ms", "BIGINT")),
        keys=("card_id", "at_ms"),           # no primary key in Lakebase; (card_id, at_ms) is the natural key
        order="at_ms",
        watermark="at_ms",
    ),
    TableSpec(
        name="voice_turns",
        columns=(("session_id", "STRING"), ("turn", "INT"), ("role", "STRING"), ("user_text", "STRING"),
                 ("agent_text", "STRING"), ("interrupted", "BOOLEAN"), ("timing", "STRING"),
                 ("created_at_ms", "BIGINT")),
        keys=("session_id", "turn"),
        order="created_at_ms",
        watermark="created_at_ms",
    ),
    TableSpec(
        name="voice_items",
        columns=(("item_id", "STRING"), ("session_id", "STRING"), ("turn", "INT"), ("kind", "STRING"),
                 ("text", "STRING"), ("owner", "STRING"), ("due", "STRING"), ("source_doc", "STRING"),
                 ("created_at_ms", "BIGINT")),
        keys=("item_id",),
        order="created_at_ms",
        watermark="created_at_ms",
    ),
)

TABLE_BY_NAME = {spec.name: spec for spec in TABLES}


def table_names() -> list[str]:
    return [spec.name for spec in TABLES]


def quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def validate_identifier(label: str, value: str) -> str:
    value = (value or "").strip()
    if not IDENT_RE.fullmatch(value):
        raise ValueError(f"{label} must be a simple identifier, got {value!r}")
    return value


def parse_bool(label: str, value: str) -> bool:
    text = (value or "").strip().lower()
    if text in ("true", "1", "yes"):
        return True
    if text in ("false", "0", "no", ""):
        return False
    raise ValueError(f"{label} must be true or false, got {value!r}")


def cutoff_ms(now_ms: int, watermark_hours: float, full_sync: bool) -> int | None:
    """Lakebase rows with a time >= the cutoff are reconciled. None means no watermark (full sync)."""
    if full_sync:
        return None
    if watermark_hours <= 0:
        raise ValueError("watermark_hours must be positive (use full_sync=true for a full read)")
    return int(now_ms - watermark_hours * MS_PER_HOUR)


def table_ref(catalog: str, schema: str, table: str) -> str:
    return f"{quote(catalog)}.{quote(schema)}.{quote(table)}"


def source_ref(lb_catalog: str, lb_schema: str, table: str) -> str:
    """The Lakebase table as the registered UC catalog exposes it: <catalog>.<postgres schema>.<table>."""
    return table_ref(lb_catalog, lb_schema, table)


def key_cols(keys: tuple[str, ...]) -> str:
    return ", ".join(quote(k) for k in keys)


def key_tuple(keys: tuple[str, ...]) -> str:
    """Left-hand side of `<tuple> IN (subquery)`. Only the left side is parenthesised: a parenthesised SELECT list
    would be one struct column, not the key columns."""
    return f"({key_cols(keys)})" if len(keys) > 1 else key_cols(keys)


def window_predicate(spec: TableSpec, cutoff: int | None, lb_catalog: str, lb_schema: str) -> str | None:
    if cutoff is None:
        return None
    parts = [f"{spec.watermark} >= {cutoff}"]
    if spec.linked:
        child, parent_col, child_time = spec.linked
        child_ref = source_ref(lb_catalog, lb_schema, child)
        parts.append(f"{quote(parent_col)} IN (SELECT {quote(parent_col)} FROM {child_ref} "
                     f"WHERE {child_time} >= {cutoff})")
    return "(" + " OR ".join(parts) + ")"


def source_sql(spec: TableSpec, lb_catalog: str, lb_schema: str, cutoff: int | None) -> str:
    """One row per natural key, typed to the Delta table. QUALIFY collapses duplicate keys in the source."""
    literals = dict(spec.literals)
    items = []
    for col, typ in spec.columns:
        expr = literals.get(col, quote(col))
        items.append(f"CAST({expr} AS {typ}) AS {quote(col)}")
    where = window_predicate(spec, cutoff, lb_catalog, lb_schema)
    where_sql = f" WHERE {where}" if where else ""
    partition = ", ".join(quote(k) for k in spec.keys)
    return (f"SELECT {', '.join(items)} FROM {source_ref(lb_catalog, lb_schema, spec.name)}{where_sql} "
            f"QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY {spec.order}) = 1")


def column_list(spec: TableSpec) -> str:
    return ", ".join(quote(c) for c, _ in spec.columns)


def dup_keys_sql(spec: TableSpec, target: str, stage: str | None = None) -> str:
    """Keys that have more than one row in the target. With `stage`, only keys present in the stage (the window)."""
    if stage is None:
        hits = f"SELECT * FROM {target}"
    else:
        on = " AND ".join(f"t.{quote(k)} = s.{quote(k)}" for k in spec.keys)
        hits = f"SELECT t.* FROM {target} t LEFT SEMI JOIN {stage} s ON {on}"
    keys = ", ".join(quote(k) for k in spec.keys)
    return f"SELECT {keys} FROM ({hits}) h GROUP BY {keys} HAVING count(*) > 1"


def delete_dups_sql(spec: TableSpec, target: str, dups: str) -> str:
    return (f"DELETE FROM {target} WHERE {key_tuple(spec.keys)} IN "
            f"(SELECT {key_cols(spec.keys)} FROM {dups})")


def keep_sql(spec: TableSpec, target: str, dups: str, keep: str) -> str:
    """Materialise one surviving row per duplicated key (dedupe_existing)."""
    partition = key_cols(spec.keys)
    return (f"CREATE OR REPLACE TABLE {keep} AS SELECT {column_list(spec)} FROM {target} "
            f"WHERE {key_tuple(spec.keys)} IN (SELECT {key_cols(spec.keys)} FROM {dups}) "
            f"QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY {spec.order}) = 1")


def reinsert_keep_sql(spec: TableSpec, target: str, keep: str) -> str:
    return f"INSERT INTO {target} SELECT {column_list(spec)} FROM {keep}"


def merge_sql(spec: TableSpec, target: str, stage: str) -> str:
    """MERGE the typed source into the target by natural key. Updates only rows that differ (null-safe), and
    never touches the preserved columns. The stage has one row per key (source_sql), so MERGE cannot fail on
    duplicate source rows; target duplicates are removed first (delete_dups_sql)."""
    on = " AND ".join(f"t.{quote(k)} = s.{quote(k)}" for k in spec.keys)
    updatable = [c for c, _ in spec.columns if c not in spec.keys and c not in spec.preserve]
    changed = " OR ".join(f"NOT (t.{quote(c)} <=> s.{quote(c)})" for c in updatable)
    assignments = ", ".join(f"t.{quote(c)} = s.{quote(c)}" for c in updatable)
    values = ", ".join(f"s.{quote(c)}" for c, _ in spec.columns)
    return (f"MERGE INTO {target} AS t USING {stage} AS s ON {on} "
            f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({column_list(spec)}) VALUES ({values})")
