# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — history tables for Zerobus Ingest (backbone=direct)
# MAGIC
# MAGIC Creates the seven managed Delta tables the app writes through **Zerobus Ingest**
# MAGIC (`src/app/history/zerobus.py`, SPEC section 3.7) and grants the app's service principal the
# MAGIC privileges Zerobus needs. Idempotent. Serverless compute is fine: this notebook uses SQL only.
# MAGIC
# MAGIC Zerobus requirements (Zerobus Ingest docs, https://docs.databricks.com/aws/en/ingestion/zerobus-ingest):
# MAGIC - Target tables must already exist as UC managed Delta tables. Zerobus does not create them.
# MAGIC - The ingest principal needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY, SELECT` on each table
# MAGIC   (ALL PRIVILEGES on the table also satisfies the zerobuswrite token request), plus the
# MAGIC   Workspace access entitlement on the service principal.
# MAGIC - Column types are restricted to STRING, INT, BIGINT, DOUBLE, BOOLEAN (the types the docs show).
# MAGIC   JSON-valued columns are STRING. Epoch times are BIGINT milliseconds.
# MAGIC
# MAGIC The column list must match `src/app/history/schema.py` (`TABLES`). `tests/app/test_history.py` checks it.
# MAGIC
# MAGIC Tables are append-only. `calls` gets one row at start and one at end (latest `ended_at` wins).

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("zerobus_principal", "")   # service principal application id (UUID) = ZEROBUS_CLIENT_ID

catalog = dbutils.widgets.get("uc_catalog").strip()
schema = dbutils.widgets.get("uc_schema").strip()
principal = dbutils.widgets.get("zerobus_principal").strip()
for label, value in (("uc_catalog", catalog), ("uc_schema", schema)):
    if not value.replace("_", "").isalnum():
        raise ValueError(f"{label} must be a simple identifier, got {value!r}")
if not principal:
    raise ValueError("zerobus_principal is required: the service principal application id the app uses for Zerobus")

# COMMAND ----------

# Same table order and columns as src/app/history/schema.py TABLES.
TABLE_DDL = [
    """CREATE TABLE IF NOT EXISTS calls (
      call_id STRING,
      playbook STRING,
      started_at BIGINT,
      ended_at BIGINT,
      customer_id STRING,
      status STRING,
      mode STRING,
      script STRING,
      owner STRING
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS utterances (
      call_id STRING,
      seq INT,
      speaker STRING,
      text STRING,
      t_start_ms BIGINT,
      t_end_ms BIGINT,
      emitted_at_ms BIGINT,
      playbook STRING
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS enrichments (
      call_id STRING,
      seq INT,
      speaker STRING,
      text STRING,
      intent STRING,
      sentiment DOUBLE,
      risk_flags STRING,
      objection STRING,
      entities STRING,
      methodology_hits STRING,
      emitted_at_ms BIGINT,
      enriched_at_ms BIGINT,
      latency_ms BIGINT,
      enrichment_source STRING
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS cards (
      card_id STRING,
      call_id STRING,
      seq INT,
      kind STRING,
      title STRING,
      body STRING,
      confidence DOUBLE,
      source_doc STRING,
      source_clause STRING,
      trigger STRING,
      status STRING,
      created_at_ms BIGINT,
      trace STRING
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS feedback (
      card_id STRING,
      call_id STRING,
      action STRING,
      edited_text STRING,
      at_ms BIGINT
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS voice_turns (
      session_id STRING,
      turn INT,
      role STRING,
      user_text STRING,
      agent_text STRING,
      interrupted BOOLEAN,
      timing STRING,
      created_at_ms BIGINT
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS voice_items (
      item_id STRING,
      session_id STRING,
      turn INT,
      kind STRING,
      text STRING,
      owner STRING,
      due STRING,
      source_doc STRING,
      created_at_ms BIGINT
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS actions (
      action_id STRING,
      session_id STRING,
      call_id STRING,
      playbook STRING,
      owner STRING,
      kind STRING,
      summary STRING,
      params STRING,
      status STRING,
      proposed_by STRING,
      created_at_ms BIGINT,
      updated_at_ms BIGINT,
      decided_at_ms BIGINT,
      decided_by STRING,
      result STRING,
      error STRING
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS contacts (
      contact_id STRING,
      owner STRING,
      first_name STRING,
      last_name STRING,
      email STRING,
      phone STRING,
      company STRING,
      updated_at_ms BIGINT
    ) USING DELTA""",
    """CREATE TABLE IF NOT EXISTS enrollments (
      enrollment_id STRING,
      contact_id STRING,
      program STRING,
      owner STRING,
      note STRING,
      created_at_ms BIGINT
    ) USING DELTA""",
]

# COMMAND ----------

def qualified(table):
    return f"{catalog}.{schema}.{table}"


def qualify_ddl(ddl):
    # ddl names bare tables; the schema is set here.
    import re
    return re.sub(r"CREATE TABLE IF NOT EXISTS (\w+)", lambda m: f"CREATE TABLE IF NOT EXISTS {qualified(m.group(1))}", ddl, count=1)


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
for ddl in TABLE_DDL:
    spark.sql(qualify_ddl(ddl))
    print("ready:", ddl.split()[5])

# Existing deployments: calls.owner was added later (SPEC 10). Delta has no ADD COLUMN IF NOT EXISTS, so check first.
if "owner" not in [field.name for field in spark.table(qualified("calls")).schema.fields]:
    spark.sql(f"ALTER TABLE {qualified('calls')} ADD COLUMNS (owner STRING)")
    print("added column calls.owner")

# COMMAND ----------

# MAGIC %md ## Grants for the Zerobus service principal
# MAGIC The principal is a UUID, so it is backquoted. Re-running is harmless.

# COMMAND ----------

quoted = f"`{principal}`"
spark.sql(f"GRANT USE CATALOG ON CATALOG `{catalog}` TO {quoted}")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO {quoted}")
for table in [name for name in ("calls", "utterances", "enrichments", "cards", "feedback", "voice_turns", "voice_items",
                                "actions", "contacts", "enrollments")]:
    spark.sql(f"GRANT MODIFY, SELECT ON TABLE `{catalog}`.`{schema}`.`{table}` TO {quoted}")
    print("granted MODIFY, SELECT on", table)

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

for table in ("calls", "utterances", "enrichments", "cards", "feedback", "voice_turns", "voice_items",
               "actions", "contacts", "enrollments"):
    n = len(spark.sql(f"DESCRIBE TABLE {qualified(table)}").collect())
    print(f"{qualified(table)}: {n} rows in DESCRIBE")
