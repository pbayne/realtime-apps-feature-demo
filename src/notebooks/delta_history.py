# Databricks notebook source
# MAGIC %md
# MAGIC # Call copilot — Delta history (bronze)
# MAGIC
# MAGIC Separate micro-batch Structured Streaming job, **not** Real-Time Mode. It reads
# MAGIC the Kafka `utterances` and `cards` topics and appends them to Unity Catalog
# MAGIC bronze Delta tables for history, analytics, and post-call scoring.
# MAGIC
# MAGIC - Runs on the batch cluster (`cluster.yml` `batch_cluster`), not the RTM cluster.
# MAGIC - Mode `processing_time` runs continuously with a 5 s trigger. Mode
# MAGIC   `available_now` drains the topic and stops (useful for backfills and tests).
# MAGIC - No `foreachBatch`: `toTable()` on a streaming DataFrame is enough, and it
# MAGIC   keeps the job on the simplest exactly-once path.
# MAGIC - Each query has its own checkpoint, so the two topics progress independently.

# COMMAND ----------

# MAGIC %run ./rtm_lib

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

dbutils.widgets.text("secret_scope", "call-copilot")
dbutils.widgets.text("kafka_security", "SASL_SSL_PLAIN")
dbutils.widgets.text("utterances_topic", "utterances")
dbutils.widgets.text("cards_topic", "cards")
dbutils.widgets.text("bronze_utterances_table", "")   # catalog.schema.bronze_utterances
dbutils.widgets.text("bronze_cards_table", "")        # catalog.schema.bronze_cards
dbutils.widgets.text("checkpoint_root", "")           # /Volumes/<cat>/<schema>/checkpoints/delta_history
dbutils.widgets.text("trigger_mode", "processing_time")  # processing_time | available_now

scope = dbutils.widgets.get("secret_scope")
security = dbutils.widgets.get("kafka_security")
utt_topic = dbutils.widgets.get("utterances_topic")
cards_topic = dbutils.widgets.get("cards_topic")
utt_table = dbutils.widgets.get("bronze_utterances_table")
cards_table = dbutils.widgets.get("bronze_cards_table")
ckpt_root = dbutils.widgets.get("checkpoint_root").rstrip("/")
trigger_mode = dbutils.widgets.get("trigger_mode")

assert utt_table and cards_table and ckpt_root, "table and checkpoint parameters are required"
assert trigger_mode in ("processing_time", "available_now"), trigger_mode

# COMMAND ----------

# MAGIC %md ## Kafka source helper

# COMMAND ----------

bootstrap = dbutils.secrets.get(scope, "kafka_bootstrap")
api_key = dbutils.secrets.get(scope, "kafka_api_key") if kafka_needs_credentials(security) else None
api_secret = dbutils.secrets.get(scope, "kafka_api_secret") if kafka_needs_credentials(security) else None
kafka_opts = kafka_reader_options(bootstrap, security, api_key, api_secret)


def kafka_stream(topic, schema):
    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_opts)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # the checkpoint pins the position after first run
        .option("failOnDataLoss", "false")      # topic retention may roll past a stopped job
        .load()
    )
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), schema).alias("v"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .where(F.col("v").isNotNull())   # malformed JSON is dropped here, not in the sink
        .select("v.*", "kafka_key", "kafka_partition", "kafka_offset", "kafka_ts")
        .withColumn("ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## Start both queries

# COMMAND ----------

def _trigger(writer):
    if trigger_mode == "available_now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime="5 seconds")


def start_bronze(topic, schema, table, name):
    writer = (
        kafka_stream(topic, schema).writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{ckpt_root}/{name}")
        .option("mergeSchema", "true")
        .queryName(f"bronze_{name}")
    )
    return _trigger(writer).toTable(table)


q_utt = start_bronze(utt_topic, UTTERANCE_SCHEMA, utt_table, "utterances")
q_cards = start_bronze(cards_topic, CARD_SCHEMA, cards_table, "cards")
print(f"started {q_utt.name} ({q_utt.id}) and {q_cards.name} ({q_cards.id}) in {trigger_mode} mode")

# COMMAND ----------

# MAGIC %md ## Run
# MAGIC
# MAGIC `processing_time` blocks until stopped. `available_now` finishes after the backlog drains.

# COMMAND ----------

q_utt.awaitTermination()
q_cards.awaitTermination()
