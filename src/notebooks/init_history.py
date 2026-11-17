# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — history views over Lakebase Change Data Feed (backbone=direct)
# MAGIC
# MAGIC History reaches UC Delta through **Lakebase Change Data Feed** (CDF), a managed Postgres->Delta
# MAGIC mirror. Every INSERT/UPDATE/DELETE on the app's operational Lakebase tables (created by
# MAGIC `src/notebooks/init.py`, which also sets `REPLICA IDENTITY FULL` on them) is captured from the
# MAGIC write-ahead log and written to a Delta table named `lb_<table>_history` in the destination
# MAGIC catalog/schema, batched and flushed ~15 s. There is **no** app-side history write any more
# MAGIC (the old Zerobus Ingest sink is retired; `src/app/history/cdf.py` is a no-op sink).
# MAGIC
# MAGIC Docs: https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf
# MAGIC
# MAGIC This notebook does the Delta-side setup that is NOT part of starting the feed:
# MAGIC   1. Creates the destination schema (idempotent).
# MAGIC   2. Creates **silver current-state views** over each `lb_<table>_history` change feed:
# MAGIC      the latest row per primary key by `_pg_lsn`, with deleted rows removed. Downstream
# MAGIC      consumers (post_call_scoring.py, talk_track_insights.py) read these views, so they see
# MAGIC      the same current-state shape the old Zerobus history tables had, not a raw change log.
# MAGIC   3. Creates + seeds `crm_accounts` — NOT a CDF table; the app writes it with a parameterized
# MAGIC      MERGE through the SQL warehouse, so it is created here.
# MAGIC
# MAGIC **Starting the feed itself is a one-time platform step** (not done here): a workspace admin
# MAGIC enables the CDF preview, then in Lakebase Postgres -> Branch overview -> Lakebase CDF tab ->
# MAGIC Start, source schema `public` of the `call-sidekick` project -> destination = this catalog/schema.
# MAGIC Equivalent Postgres REST API: "Change Data Feed" in the Lakebase API guide. The views below
# MAGIC are created `IF NOT EXISTS`-style with existence guards so this notebook is safe to run both
# MAGIC before and after the feed has produced its `lb_*_history` tables.
# MAGIC
# MAGIC Keep the table/PK map in step with `src/app/history/schema.py` TABLES and the operational DDL
# MAGIC in `src/notebooks/init.py`. `tests/app/test_history.py` checks it.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("cdf_prefix", "lb_")       # Lakebase CDF destination table prefix: lb_<table>_history
dbutils.widgets.text("cdf_suffix", "_history")

catalog = dbutils.widgets.get("uc_catalog").strip()
schema = dbutils.widgets.get("uc_schema").strip()
cdf_prefix = dbutils.widgets.get("cdf_prefix").strip()
cdf_suffix = dbutils.widgets.get("cdf_suffix").strip()
for label, value in (("uc_catalog", catalog), ("uc_schema", schema)):
    if not value.replace("_", "").isalnum():
        raise ValueError(f"{label} must be a simple identifier, got {value!r}")

# COMMAND ----------

# The history-contract tables and their primary keys (same set/order as src/app/history/schema.py TABLES).
# A None PK means the source table is append-only with no natural single key (feedback): its current-state
# view is simply every inserted row. For the rest, the current-state view keeps the latest row per PK.
HISTORY_PKS = {
    "calls": ["call_id"],
    "utterances": ["call_id", "seq"],
    "enrichments": ["call_id", "seq"],
    "cards": ["card_id"],
    "feedback": None,
    "voice_turns": ["session_id", "turn"],
    "voice_items": ["item_id"],
    "actions": ["action_id"],
    "contacts": ["contact_id"],
    "enrollments": ["enrollment_id"],
}

# CDF system columns on every lb_<table>_history row (docs "Destination table schema"):
#   _pg_change_type TEXT  insert | delete | update_preimage | update_postimage
#   _pg_lsn BIGINT        Postgres Log Sequence Number (monotonic; the ordering key)
#   _pg_xid INTEGER       transaction id
#   _timestamp TIMESTAMP  change time
CDF_SYS_COLS = {"_pg_change_type", "_pg_lsn", "_pg_xid", "_timestamp"}

# COMMAND ----------

# MAGIC %md ## 1. Destination schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
print(f"schema ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## 2. Silver current-state views over the CDF change feeds
# MAGIC
# MAGIC For a keyed table: keep the row with the greatest `_pg_lsn` per PK, then drop it if that latest
# MAGIC change was a `delete`. `update_preimage` rows (the "before" image of an update) are excluded;
# MAGIC `insert` and `update_postimage` are the live values. For the append-only `feedback` table we
# MAGIC keep every non-delete change row (there are no updates in practice).
# MAGIC
# MAGIC The view name is the bare table name (`calls`, `utterances`, …) so downstream SQL is unchanged
# MAGIC from the Zerobus era — only the source (now a CDF feed) differs.

# COMMAND ----------

def cdf_table(table):
    return f"`{catalog}`.`{schema}`.`{cdf_prefix}{table}{cdf_suffix}`"


def view_name(table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def source_columns(fqtn):
    """Non-system source columns of a CDF history table, as they exist right now."""
    cols = [f.name for f in spark.table(fqtn).schema.fields]
    return [c for c in cols if c not in CDF_SYS_COLS]


created, skipped = [], []
for table, pk in HISTORY_PKS.items():
    src = cdf_table(table)
    # The feed may not have produced this table yet (needs >= 1 row in the source). Guard so the
    # notebook is safe to run before Start / before any rows exist.
    if not spark.catalog.tableExists(f"{catalog}.{schema}.{cdf_prefix}{table}{cdf_suffix}"):
        skipped.append(table)
        continue
    cols = source_columns(src)
    col_list = ", ".join(f"`{c}`" for c in cols)
    if pk is None:
        # Append-only: every non-delete change row is a live row.
        body = f"""
            SELECT {col_list}
            FROM {src}
            WHERE _pg_change_type IN ('insert', 'update_postimage')
        """
    else:
        part = ", ".join(f"`{c}`" for c in pk)
        body = f"""
            WITH ranked AS (
              SELECT {col_list}, _pg_change_type,
                     ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY _pg_lsn DESC) AS _rn
              FROM {src}
              WHERE _pg_change_type IN ('insert', 'update_postimage', 'delete')
            )
            SELECT {col_list}
            FROM ranked
            WHERE _rn = 1 AND _pg_change_type <> 'delete'
        """
    spark.sql(f"CREATE OR REPLACE VIEW {view_name(table)} AS {body}")
    created.append(table)

print(f"current-state views created: {created}")
if skipped:
    print(f"skipped (no CDF feed table yet — run again after the feed is Started and has rows): {skipped}")

# COMMAND ----------

# MAGIC %md ## 3. crm_accounts (NOT a CDF table)
# MAGIC The update_record action (SPEC 12) writes this via a parameterized MERGE through the SQL
# MAGIC warehouse (Statement Execution API). It lives in Delta directly, so it is created and seeded
# MAGIC here. Keep the columns in step with src/app/actions/records.py.

# COMMAND ----------

def qualified(table):
    return f"{catalog}.{schema}.{table}"


spark.sql(f"""CREATE TABLE IF NOT EXISTS {qualified('crm_accounts')} (
  account_id STRING,
  name STRING,
  stage STRING,
  arr DOUBLE,
  owner STRING,
  next_step STRING,
  notes STRING,
  updated_at_ms BIGINT
) USING DELTA""")
print("ready: crm_accounts")

DEMO_ACCOUNTS = [
    ("acct-northwind", "Northwind Traders", "Discovery", 180000.0, "patrick.bayne@databricks.com",
     "Send fee breakdown", "Prospect; evaluating managed portfolios."),
    ("acct-contoso", "Contoso Capital", "Proposal", 420000.0, "patrick.bayne@databricks.com",
     "Schedule portfolio review", "Weighing fees vs. a competing advisor."),
    ("acct-fabrikam", "Fabrikam Financial", "Negotiation", 650000.0, "patrick.bayne@databricks.com",
     "Confirm onboarding timeline", "Verbally agreed; wants quarterly check-ins."),
]
if spark.table(qualified("crm_accounts")).count() == 0:
    from pyspark.sql import Row
    rows = [Row(account_id=a, name=n, stage=s, arr=arr, owner=o, next_step=ns, notes=nt, updated_at_ms=1767225600000)
            for a, n, s, arr, o, ns, nt in DEMO_ACCOUNTS]
    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(qualified("crm_accounts"))
    print(f"seeded {len(DEMO_ACCOUNTS)} demo accounts into crm_accounts")

# COMMAND ----------

# MAGIC %md ## 4. Verify
# MAGIC Shows which current-state views resolved and their row counts. Views over feeds that have not
# MAGIC produced data yet are listed under "pending" above and simply re-run later.

# COMMAND ----------

for table in created:
    try:
        n = spark.table(f"{catalog}.{schema}.{table}").count()
        print(f"{catalog}.{schema}.{table}: {n} current rows")
    except Exception as exc:  # noqa: BLE001 - verification only
        print(f"{catalog}.{schema}.{table}: view exists, count failed ({exc})")
