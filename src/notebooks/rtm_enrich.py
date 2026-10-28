# Databricks notebook source
# MAGIC %md
# MAGIC # Call copilot — RTM enrichment
# MAGIC
# MAGIC Kafka `utterances` (final utterances only) → parse JSON → deterministic
# MAGIC enrichment (objection / risk / methodology rules, entity regex, sentiment
# MAGIC lexicon, question intent) → native `format("postgresql")` sink into the
# MAGIC Lakebase `enrichments` table, upserting on `(call_id, seq)`.
# MAGIC
# MAGIC Real-Time Mode constraints this job obeys (verified against the docs, see README):
# MAGIC - Kafka is a supported RTM source; Delta is not. Sink is `format("postgresql")`.
# MAGIC - `update` output mode only. No `foreachBatch`. No joins, no aggregations.
# MAGIC - Stateless ops only here: `from_json`, `filter`, `select`, built-in regex/array functions.
# MAGIC - No Python UDFs (see `rtm_lib.py`).
# MAGIC - Classic compute, Photon off, autoscaling off, DBR 18.3+ for the Lakebase sink.
# MAGIC   Spark conf `spark.databricks.streaming.realTimeMode.enabled=true` is set on the cluster.
# MAGIC - Trigger is `realTime="5 minutes"`. That is the checkpoint interval, not a delay.
# MAGIC   Records flow through as they arrive.

# COMMAND ----------

# MAGIC %run ./rtm_lib

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402  (also available via %run rtm_lib)

dbutils.widgets.text("secret_scope", "call-copilot")
dbutils.widgets.text("kafka_security", SASL_PLAIN)          # SASL_SSL_PLAIN | SASL_SSL_SCRAM_SHA_256 | SASL_SSL_SCRAM_SHA_512 | SASL_SSL_MSK_IAM
dbutils.widgets.text("utterances_topic", "utterances")
dbutils.widgets.text("starting_offsets", "latest")          # used only for a brand-new checkpoint
dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("rules_path", "")                      # .../src/notebooks/enrich_rules.json
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("sink_batchinterval", "50 milliseconds")
dbutils.widgets.text("sink_batchsize", "500")

scope             = dbutils.widgets.get("secret_scope")
kafka_security    = dbutils.widgets.get("kafka_security")
topic             = dbutils.widgets.get("utterances_topic")
starting_offsets  = dbutils.widgets.get("starting_offsets")
checkpoint_path   = dbutils.widgets.get("checkpoint_path")
rules_path        = dbutils.widgets.get("rules_path")
lb_database       = dbutils.widgets.get("lakebase_database")
batchinterval     = dbutils.widgets.get("sink_batchinterval")
batchsize         = dbutils.widgets.get("sink_batchsize")

assert checkpoint_path, "checkpoint_path is required (a UC volume path unique to this query)"
assert starting_offsets in ("latest", "earliest"), starting_offsets
assert topic in topic_names(topic), topic

# COMMAND ----------

# MAGIC %md ## 2. Secrets and rules

# COMMAND ----------

# kafka_bootstrap: broker list (SASL_SSL endpoint for Confluent, IAM bootstrap string for MSK).
bootstrap = dbutils.secrets.get(scope, "kafka_bootstrap")
api_key = dbutils.secrets.get(scope, "kafka_api_key") if kafka_needs_credentials(kafka_security) else None
api_secret = dbutils.secrets.get(scope, "kafka_api_secret") if kafka_needs_credentials(kafka_security) else None
lakebase_endpoint = dbutils.secrets.get(scope, "lakebase_endpoint")  # project_id.branch_id[.endpoint_id]

rules = load_rules(rules_path)
print(f"rules: " + ", ".join(f"{g}={len(rules[g])}" for g in ("objections", "risk", "methodology", "entities")))

# COMMAND ----------

# MAGIC %md ## 3. Source → parse → enrich
# MAGIC
# MAGIC `from_json` with an explicit schema (no inference) keeps the plan
# MAGIC stateless. Malformed JSON becomes NULL and is dropped by the filter.

# COMMAND ----------

raw = (
    spark.readStream.format("kafka")
    .options(**kafka_reader_options(bootstrap, kafka_security, api_key, api_secret))
    .option("subscribe", topic)
    .option("startingOffsets", starting_offsets)
    .load()
)

parsed = (
    raw.select(F.from_json(F.col("value").cast("string"), UTTERANCE_SCHEMA).alias("u"))
    .select("u.*")
    .where(F.col("call_id").isNotNull() & F.col("seq").isNotNull() & (F.col("is_final") == True))  # noqa: E712
)

enriched = build_enrichment(parsed, rules)

# COMMAND ----------

# MAGIC %md ## 4. Sink: native Lakebase `postgresql` sink (upsert on PK)
# MAGIC
# MAGIC - `endpoint`: `project_id.branch_id` is accepted (endpoint_id may be omitted when the branch has one RW endpoint).
# MAGIC - `upsertkey`: must equal the table's primary key `call_id,seq`.
# MAGIC - `batchinterval` / `batchsize`: flush on whichever is hit first.
# MAGIC - Auth is workspace identity of the job run (no password here).

# COMMAND ----------

query = (
    enriched.writeStream
    .format("postgresql")
    .outputMode("update")
    .option("endpoint", lakebase_endpoint)
    .option("database", lb_database)
    .option("dbtable", "public.enrichments")
    .option("upsertkey", "call_id,seq")
    .option("batchinterval", batchinterval)
    .option("batchsize", batchsize)
    .option("checkpointLocation", checkpoint_path)
    .trigger(realTime="5 minutes")
    .queryName("call_copilot_rtm_enrich")
    .start()
)

print(f"started query {query.id} (run {query.runId})")

# COMMAND ----------

# MAGIC %md ## 5. Run until stopped
# MAGIC
# MAGIC RTM is continuous. The job has no timeout; stop it with
# MAGIC `databricks jobs cancel-run` (or the UI). A failed query surfaces here
# MAGIC so the job run goes red instead of silently idling.

# COMMAND ----------

query.awaitTermination()
