# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — reconcile Lakebase into the Zerobus history tables (every 15 min)
# MAGIC
# MAGIC Zerobus is the low-latency path into the seven history tables, and it is best effort: it can duplicate rows
# MAGIC (at-least-once retries) and drop them (queue overflow, restart mid-batch, failed retries). Lakebase holds every
# MAGIC record under the same natural keys, so Lakebase is the source of truth for completeness.
# MAGIC
# MAGIC Per table, this job:
# MAGIC 1. reads the Lakebase rows in the watermark window through the registered UC catalog
# MAGIC    (`<lakebase_catalog>.<lakebase_schema>.<table>`, serverless), one row per natural key (QUALIFY);
# MAGIC 2. removes target duplicates for the keys in that window (Zerobus retries), then
# MAGIC 3. MERGEs by natural key: inserts missing rows and updates rows that changed (cards.status after feedback,
# MAGIC    calls.ended_at, ...). Unchanged rows are not rewritten.
# MAGIC
# MAGIC `dedupe_existing=true` also collapses duplicates across the whole table (one-time backfill). `full_sync=true`
# MAGIC reads all of Lakebase instead of the watermark window. SQL builders live in `reconcile_lib.py` and are unit
# MAGIC tested in `tests/notebooks/test_reconcile_history.py`.
# MAGIC
# MAGIC Recovery point: a record is in Delta within one schedule interval (15 min) plus the job runtime after it reaches
# MAGIC Lakebase, as long as its time is inside the watermark (default 48 h). Older changes need `full_sync=true`.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("lakebase_catalog", "call_sidekick_lakebase")   # UC catalog registered on the Lakebase database
dbutils.widgets.text("lakebase_schema", "public")
dbutils.widgets.text("watermark_hours", "48")
dbutils.widgets.text("full_sync", "false")
dbutils.widgets.text("dedupe_existing", "false")
dbutils.widgets.text("grant_to", "")                                   # principal given the job's grants (bundle: deployer)

# COMMAND ----------

# MAGIC %run ./reconcile_lib

# COMMAND ----------

import time

catalog = validate_identifier("uc_catalog", dbutils.widgets.get("uc_catalog"))
schema = validate_identifier("uc_schema", dbutils.widgets.get("uc_schema"))
lb_catalog = validate_identifier("lakebase_catalog", dbutils.widgets.get("lakebase_catalog"))
lb_schema = validate_identifier("lakebase_schema", dbutils.widgets.get("lakebase_schema"))
watermark_hours = float(dbutils.widgets.get("watermark_hours"))
full_sync = parse_bool("full_sync", dbutils.widgets.get("full_sync"))
dedupe_existing = parse_bool("dedupe_existing", dbutils.widgets.get("dedupe_existing"))
grant_to = dbutils.widgets.get("grant_to").strip()

cutoff = cutoff_ms(int(time.time() * 1000), watermark_hours, full_sync)
print(f"history {catalog}.{schema} <- lakebase {lb_catalog}.{lb_schema}; "
      f"{'full sync' if cutoff is None else f'watermark {watermark_hours:g} h (cutoff {cutoff} ms)'}; "
      f"dedupe_existing={dedupe_existing}")

# COMMAND ----------

# MAGIC %md ## Access: the Lakebase catalog must be registered, and the job identity needs grants (idempotent)

# COMMAND ----------

lb_schemas = [row[0] for row in spark.sql(f"SHOW SCHEMAS IN {quote(lb_catalog)}").collect()]
if lb_schema not in lb_schemas:
    raise RuntimeError(f"{lb_catalog}.{lb_schema} not found; schemas: {lb_schemas}. Check the postgres_catalogs "
                       "resource (lakebase.yml) and that the Lakebase database is registered in Unity Catalog.")

if grant_to:
    principal = quote(grant_to)
    spark.sql(f"GRANT USE CATALOG ON CATALOG {quote(catalog)} TO {principal}")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {quote(catalog)}.{quote(schema)} TO {principal}")
    for name in table_names():
        spark.sql(f"GRANT MODIFY, SELECT ON TABLE {table_ref(catalog, schema, name)} TO {principal}")
    spark.sql(f"GRANT USE CATALOG ON CATALOG {quote(lb_catalog)} TO {principal}")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {quote(lb_catalog)}.{quote(lb_schema)} TO {principal}")
    spark.sql(f"GRANT SELECT ON SCHEMA {quote(lb_catalog)}.{quote(lb_schema)} TO {principal}")
    print(f"grants ensured for {grant_to}")

# COMMAND ----------

# MAGIC %md ## Reconcile each table

# COMMAND ----------

def dedupe_whole_table(spec, target):
    """One-time (dedupe_existing): keep one row per duplicated key across the whole Delta table.
    Lakebase has the authoritative row for these keys; the MERGE that follows refreshes the kept row if it differs."""
    dups = table_ref(catalog, schema, f"_reconcile_dups_{spec.name}")
    keep = table_ref(catalog, schema, f"_reconcile_keep_{spec.name}")
    spark.sql(f"CREATE OR REPLACE TABLE {dups} AS {dup_keys_sql(spec, target)}")
    n = spark.table(dups).count()
    if n:
        spark.sql(keep_sql(spec, target, dups, keep))
        spark.sql(delete_dups_sql(spec, target, dups))
        spark.sql(reinsert_keep_sql(spec, target, keep))
        left = spark.sql(dup_keys_sql(spec, target)).count()
        if left:
            raise RuntimeError(f"{spec.name}: {left} duplicated keys remain after dedupe_existing (rows were written "
                               "during the dedupe). Re-run the job with dedupe_existing=true.")
    spark.sql(f"DROP TABLE IF EXISTS {dups}")
    spark.sql(f"DROP TABLE IF EXISTS {keep}")
    return n


def reconcile_table(spec):
    target = table_ref(catalog, schema, spec.name)
    stage = table_ref(catalog, schema, f"_reconcile_stage_{spec.name}")
    dups = table_ref(catalog, schema, f"_reconcile_dups_{spec.name}")
    # 1. Snapshot the Lakebase window once, so the steps below see the same rows.
    spark.sql(f"CREATE OR REPLACE TABLE {stage} AS {source_sql(spec, lb_catalog, lb_schema, cutoff)}")
    source_rows = spark.table(stage).count()
    # 2. Target duplicates for keys in the window (Zerobus retries). MERGE needs one target row per key.
    spark.sql(f"CREATE OR REPLACE TABLE {dups} AS {dup_keys_sql(spec, target, stage)}")
    collapsed = spark.table(dups).count()
    if collapsed:
        spark.sql(delete_dups_sql(spec, target, dups))   # the MERGE below re-inserts each of these keys
    # 3. MERGE by natural key: insert missing, update changed.
    merged = spark.sql(merge_sql(spec, target, stage)).first().asDict()
    spark.sql(f"DROP TABLE IF EXISTS {stage}")
    spark.sql(f"DROP TABLE IF EXISTS {dups}")
    return {"source_rows": source_rows, "duplicate_keys_collapsed": collapsed,
            "inserted": merged.get("num_inserted_rows"), "updated": merged.get("num_updated_rows")}

# COMMAND ----------

results, errors = {}, {}
for spec in TABLES:
    try:
        target = table_ref(catalog, schema, spec.name)
        if dedupe_existing:
            results.setdefault(spec.name, {})["dedupe_existing_removed_keys"] = dedupe_whole_table(spec, target)
        results.setdefault(spec.name, {}).update(reconcile_table(spec))
        print(spec.name, results[spec.name])
    except Exception as exc:  # noqa: BLE001 - one table must not stop the others; the job fails at the end
        errors[spec.name] = f"{type(exc).__name__}: {str(exc)[:400]}"
        print("FAILED", spec.name, errors[spec.name])

# COMMAND ----------

if errors:
    raise RuntimeError(f"reconcile_history failed for {sorted(errors)}: {errors}")
print("reconcile_history complete:", {name: r.get("inserted") for name, r in results.items()})
