"""Unit tests for reconcile_lib.py (the SQL reconcile_history.py sends). No Spark, no Lakebase.

Parity: the Delta table map must match src/app/history/schema.py TABLES (names, order, types) and the Lakebase DDL in
src/app/store/lakebase.py (columns, types, primary keys). Both sources are read with ast, so the app is not imported.
"""

import ast
import re
from pathlib import Path

import pytest
import reconcile_lib as rl

ROOT = Path(__file__).resolve().parents[2]
LB, SCHEMA = "call_sidekick_lakebase", "public"


def _module_assign(path, name):
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return node.value
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == name:
            return node.value
    raise AssertionError(f"{name} not found in {path}")


SCHEMA_TABLES = ast.literal_eval(_module_assign(ROOT / "src/app/history/schema.py", "TABLES"))
LAKEBASE_DDL = ast.literal_eval(_module_assign(ROOT / "src/app/store/lakebase.py", "DDL"))


def _lakebase_tables(ddl):
    """table -> (columns {name: TYPE}, primary key tuple). Same parsing idea as tests/app/test_schema_parity.py."""
    ddl = re.sub(r"--[^\n]*", "", ddl)
    out = {}
    for table, body in re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\)\s*(?:;|$)", ddl, re.S):
        cols, pk = {}, ()
        m = re.search(r"PRIMARY KEY \(([^)]*)\)", body)
        if m:
            pk = tuple(c.strip() for c in m.group(1).split(","))
        for part in re.split(r",(?![^()]*\))", body):
            tokens = part.replace('"', "").split()
            if not tokens or tokens[0].upper() in ("PRIMARY", "CHECK", "CONSTRAINT"):
                continue
            typ = tokens[1].upper()
            cols[tokens[0]] = {"INT": "INTEGER"}.get(typ, typ)
            if "PRIMARY" in [t.upper() for t in tokens]:
                pk = (tokens[0],)
        out[table] = (cols, pk)
    return out


LAKEBASE = _lakebase_tables(LAKEBASE_DDL)
LB_TO_DELTA = {"TEXT": "STRING", "INTEGER": "INT", "INT": "INT", "BIGINT": "BIGINT", "DOUBLE": "DOUBLE",
               "BOOLEAN": "BOOLEAN"}


# ---------------------------------------------------------------- parity

def test_table_map_matches_history_schema_names_order_and_types():
    assert [s.name for s in rl.TABLES] == [name for name, _ in SCHEMA_TABLES.items()]
    for spec in rl.TABLES:
        assert list(spec.columns) == [tuple(c) for c in SCHEMA_TABLES[spec.name]], spec.name


def test_every_history_table_has_a_spec_and_only_those():
    assert set(rl.TABLE_BY_NAME) == set(SCHEMA_TABLES) == {
        "calls", "utterances", "enrichments", "cards", "feedback", "voice_turns", "voice_items",
        "actions", "contacts", "enrollments"}


@pytest.mark.parametrize("spec", rl.TABLES, ids=lambda s: s.name)
def test_delta_columns_match_lakebase_columns_and_types(spec):
    cols, _pk = LAKEBASE[spec.name]
    literal_cols = {c for c, _ in spec.literals}
    delta = dict(spec.columns)
    lakebase_for_delta = {c for c in delta if c not in literal_cols}
    assert lakebase_for_delta == set(cols), spec.name
    for col, typ in delta.items():
        if col in literal_cols:
            continue
        assert LB_TO_DELTA[cols[col]] == typ, (spec.name, col)


@pytest.mark.parametrize("spec", rl.TABLES, ids=lambda s: s.name)
def test_keys_are_the_lakebase_primary_key_where_there_is_one(spec):
    _cols, pk = LAKEBASE[spec.name]
    if spec.name == "feedback":
        assert pk == ()                                  # no primary key in Lakebase: the documented natural key
        assert spec.keys == ("card_id", "at_ms")
    else:
        assert tuple(spec.keys) == tuple(pk), spec.name


def test_enrichment_source_is_the_only_column_lakebase_lacks():
    enr = rl.TABLE_BY_NAME["enrichments"]
    assert dict(enr.literals) == {"enrichment_source": "'direct'"}
    assert enr.preserve == ("enrichment_source",)


def test_calls_owner_is_reconciled():
    calls = rl.TABLE_BY_NAME["calls"]
    assert "owner" in dict(calls.columns)
    assert "owner" in LAKEBASE["calls"][0]


# ---------------------------------------------------------------- identifiers, flags, watermark

@pytest.mark.parametrize("bad", ["", "a-b", "x; DROP", "1abc", "a`b"])
def test_validate_identifier_rejects_non_identifiers(bad):
    with pytest.raises(ValueError):
        rl.validate_identifier("uc_catalog", bad)


def test_validate_identifier_accepts_simple_names():
    assert rl.validate_identifier("uc_catalog", " call_sidekick_lakebase ") == "call_sidekick_lakebase"


def test_parse_bool():
    assert rl.parse_bool("full_sync", "TRUE") is True
    assert rl.parse_bool("full_sync", "false") is False
    with pytest.raises(ValueError):
        rl.parse_bool("full_sync", "maybe")


def test_cutoff_is_now_minus_watermark_hours_and_none_for_full_sync():
    now = 1_700_000_000_000
    assert rl.cutoff_ms(now, 48, False) == now - 48 * 3_600_000
    assert rl.cutoff_ms(now, 48, True) is None
    with pytest.raises(ValueError):
        rl.cutoff_ms(now, 0, False)


def test_full_sync_has_no_where_clause():
    for spec in rl.TABLES:
        assert " WHERE " not in rl.source_sql(spec, LB, SCHEMA, None), spec.name


def test_watermark_window_per_table():
    cutoff = 123
    expected = {
        "calls": "(GREATEST(started_at, ended_at) >= 123)",
        "utterances": "(emitted_at_ms >= 123)",
        "enrichments": "(enriched_at_ms >= 123)",
        "feedback": "(at_ms >= 123)",
        "voice_turns": "(created_at_ms >= 123)",
        "voice_items": "(created_at_ms >= 123)",
        "actions": "(GREATEST(created_at_ms, COALESCE(updated_at_ms, 0), COALESCE(decided_at_ms, 0)) >= 123)",
        "contacts": "(updated_at_ms >= 123)",
        "enrollments": "(created_at_ms >= 123)",
    }
    for spec in rl.TABLES:
        pred = rl.window_predicate(spec, cutoff, LB, SCHEMA)
        if spec.name == "cards":
            continue
        assert pred == expected[spec.name], spec.name


def test_cards_window_includes_cards_with_recent_feedback():
    cards = rl.TABLE_BY_NAME["cards"]
    pred = rl.window_predicate(cards, 123, LB, SCHEMA)
    assert pred.startswith("(created_at_ms >= 123 OR `card_id` IN (SELECT `card_id` FROM "
                           "`call_sidekick_lakebase`.`public`.`feedback` WHERE at_ms >= 123))")


# ---------------------------------------------------------------- source SQL (dedupe in the source)

@pytest.mark.parametrize("spec", rl.TABLES, ids=lambda s: s.name)
def test_source_sql_reads_lakebase_through_the_uc_catalog_and_dedupes_by_key(spec):
    sql = rl.source_sql(spec, LB, SCHEMA, 5)
    assert f"FROM `{LB}`.`{SCHEMA}`.`{spec.name}`" in sql
    key_list = ", ".join(f"`{k}`" for k in spec.keys)
    assert f"QUALIFY row_number() OVER (PARTITION BY {key_list} ORDER BY {spec.order}) = 1" in sql


@pytest.mark.parametrize("spec", rl.TABLES, ids=lambda s: s.name)
def test_source_sql_casts_every_column_to_the_delta_type(spec):
    sql = rl.source_sql(spec, LB, SCHEMA, None)
    for col, typ in spec.columns:
        assert f"AS {typ}) AS `{col}`" in sql, (spec.name, col)


def test_enrichment_source_is_a_literal_in_the_source():
    sql = rl.source_sql(rl.TABLE_BY_NAME["enrichments"], LB, SCHEMA, None)
    assert "CAST('direct' AS STRING) AS `enrichment_source`" in sql


# ---------------------------------------------------------------- MERGE

@pytest.mark.parametrize("spec", rl.TABLES, ids=lambda s: s.name)
def test_merge_joins_on_every_natural_key_and_nothing_else(spec):
    sql = rl.merge_sql(spec, f"`m`.`s`.`{spec.name}`", "`m`.`s`.`_stage`")
    on_clause = sql.split(" ON ", 1)[1].split(" WHEN ", 1)[0]
    assert on_clause == " AND ".join(f"t.`{k}` = s.`{k}`" for k in spec.keys)


@pytest.mark.parametrize("spec", rl.TABLES, ids=lambda s: s.name)
def test_merge_updates_only_changed_non_key_columns_null_safely(spec):
    sql = rl.merge_sql(spec, "T", "S")
    updatable = [c for c, _ in spec.columns if c not in spec.keys and c not in spec.preserve]
    matched = sql.split("WHEN MATCHED AND (", 1)[1].split(") THEN UPDATE SET", 1)[0]
    assert matched == " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in updatable)
    assigned = sql.split("UPDATE SET ", 1)[1].split(" WHEN NOT MATCHED", 1)[0]
    assert assigned == ", ".join(f"t.`{c}` = s.`{c}`" for c in updatable)
    for key in spec.keys:
        assert f"t.`{key}` = s.`{key}`" not in assigned


def test_merge_never_updates_enrichment_source_but_inserts_it():
    sql = rl.merge_sql(rl.TABLE_BY_NAME["enrichments"], "T", "S")
    assert "t.`enrichment_source` = s.`enrichment_source`" not in sql
    assert sql.endswith("VALUES (" + ", ".join(f"s.`{c}`" for c, _ in rl.TABLE_BY_NAME["enrichments"].columns) + ")")
    assert "`enrichment_source`)" in sql.split("WHEN NOT MATCHED", 1)[1].split(" VALUES", 1)[0]


def test_cards_status_change_is_an_update_column():
    sql = rl.merge_sql(rl.TABLE_BY_NAME["cards"], "T", "S")
    assert "NOT (t.`status` <=> s.`status`)" in sql


def test_calls_ended_at_change_is_an_update_column():
    sql = rl.merge_sql(rl.TABLE_BY_NAME["calls"], "T", "S")
    assert "NOT (t.`ended_at` <=> s.`ended_at`)" in sql and "NOT (t.`owner` <=> s.`owner`)" in sql


# ---------------------------------------------------------------- duplicates in the target

def test_dup_keys_window_is_a_semi_join_against_the_stage():
    spec = rl.TABLE_BY_NAME["utterances"]
    sql = rl.dup_keys_sql(spec, "T", "S")
    assert "LEFT SEMI JOIN S s ON t.`call_id` = s.`call_id` AND t.`seq` = s.`seq`" in sql
    assert sql.endswith("GROUP BY `call_id`, `seq` HAVING count(*) > 1")


def test_dup_keys_full_table_has_no_join():
    sql = rl.dup_keys_sql(rl.TABLE_BY_NAME["cards"], "T")
    assert "SEMI JOIN" not in sql and "HAVING count(*) > 1" in sql


def test_delete_dups_uses_key_tuple_for_composite_keys():
    spec = rl.TABLE_BY_NAME["feedback"]
    assert rl.delete_dups_sql(spec, "T", "D") == \
        "DELETE FROM T WHERE (`card_id`, `at_ms`) IN (SELECT `card_id`, `at_ms` FROM D)"


def test_delete_dups_single_key():
    assert rl.delete_dups_sql(rl.TABLE_BY_NAME["cards"], "T", "D") == \
        "DELETE FROM T WHERE `card_id` IN (SELECT `card_id` FROM D)"


def test_composite_key_subquery_selects_columns_not_a_struct():
    sql = rl.keep_sql(rl.TABLE_BY_NAME["utterances"], "T", "D", "K")
    assert "WHERE (`call_id`, `seq`) IN (SELECT `call_id`, `seq` FROM D)" in sql
    assert "(SELECT (" not in sql


def test_keep_sql_keeps_one_row_per_key_by_the_table_order():
    calls = rl.TABLE_BY_NAME["calls"]
    sql = rl.keep_sql(calls, "T", "D", "K")
    assert sql.startswith("CREATE OR REPLACE TABLE K AS SELECT `call_id`, `playbook`")
    assert "QUALIFY row_number() OVER (PARTITION BY `call_id` ORDER BY ended_at DESC NULLS LAST) = 1" in sql


def test_reinsert_writes_back_the_kept_rows_in_column_order():
    spec = rl.TABLE_BY_NAME["voice_turns"]
    assert rl.reinsert_keep_sql(spec, "T", "K").startswith("INSERT INTO T SELECT `session_id`, `turn`, `role`")


def test_zerobus_calls_start_and_end_rows_collapse_to_the_ended_row():
    # Zerobus writes a start row and an end row per call: ORDER BY ended_at DESC NULLS LAST keeps the end row.
    assert rl.TABLE_BY_NAME["calls"].order == "ended_at DESC NULLS LAST"


def test_cards_dedupe_prefers_a_feedback_status_over_shown():
    assert "status = 'shown'" in rl.TABLE_BY_NAME["cards"].order


def test_table_ref_quotes_backticks_in_names():
    assert rl.table_ref("a`b", "s", "t") == "`a``b`.`s`.`t`"


def test_pg8000_mode_reads_temp_views_with_typed_schemas():
    import reconcile_lib as rl

    assert rl.source_ref(rl.PG_SOURCE, "public", "calls") == "`lakebase_src_calls`"
    spec = next(s for s in rl.TABLES if s.name == "utterances")
    sql = rl.source_sql(spec, rl.PG_SOURCE, "public", cutoff=123)
    assert "FROM `lakebase_src_utterances`" in sql and "lakebase_src_calls" not in sql.split("FROM `lakebase_src_utterances`")[0]
    assert rl.spark_schema_ddl([("call_id", "text"), ("seq", "integer"), ("t_start_ms", "bigint"),
                                ("sentiment", "double precision"), ("weird", "jsonb")]) == \
        "`call_id` STRING, `seq` INT, `t_start_ms` BIGINT, `sentiment` DOUBLE, `weird` STRING"
