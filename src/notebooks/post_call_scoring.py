# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — post-call scoring (Lakeflow job)
# MAGIC
# MAGIC Scores completed calls with `ai_query()` and writes `gold_call_scores`:
# MAGIC summary, methodology coverage, objections handled, next steps, overall score.
# MAGIC
# MAGIC **What counts as "completed":** no utterance for `idle_minutes` (default 15),
# MAGIC and not already present in `gold_call_scores`. Lakebase is the live store and
# MAGIC this job reads the Delta bronze history instead, so it doesn't load OLTP.
# MAGIC
# MAGIC Runs as a scheduled batch job on serverless compute. Not RTM, no streaming.
# MAGIC Source tables: the Zerobus history tables `utterances` / `cards` (backbone=direct, the default job),
# MAGIC or the Kafka bronze tables `bronze_utterances` / `bronze_cards` (backbone=kafka). Pass them as widgets.
# MAGIC The output schema is a fixed JSON contract; the model is asked for JSON only,
# MAGIC and unparseable outputs are kept with `parse_ok = false` so nothing is silently dropped.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("scoring_endpoint", "databricks-claude-sonnet-5-5")
dbutils.widgets.text("idle_minutes", "15")
dbutils.widgets.text("max_calls_per_run", "200")
dbutils.widgets.text("playbook_filter", "")   # optional: scope to one playbook name
dbutils.widgets.text("utterances_table", "utterances")  # direct: Zerobus history; kafka: bronze_utterances
dbutils.widgets.text("cards_table", "cards")            # direct: Zerobus history; kafka: bronze_cards

catalog = dbutils.widgets.get("uc_catalog")
schema = dbutils.widgets.get("uc_schema")
endpoint = dbutils.widgets.get("scoring_endpoint")
idle_minutes = int(dbutils.widgets.get("idle_minutes"))
max_calls = int(dbutils.widgets.get("max_calls_per_run"))
playbook_filter = dbutils.widgets.get("playbook_filter").strip()

import re

assert re.fullmatch(r"[A-Za-z0-9._-]+", endpoint), "scoring_endpoint must be a serving endpoint name"
UTT = f"{catalog}.{schema}.{dbutils.widgets.get('utterances_table')}"
CARDS = f"{catalog}.{schema}.{dbutils.widgets.get('cards_table')}"
GOLD = f"{catalog}.{schema}.gold_call_scores"

# COMMAND ----------

# MAGIC %md ## 1. Candidate calls

# COMMAND ----------

from pyspark.sql import functions as F

utt = spark.table(UTT)
if playbook_filter:
    utt = utt.where(F.col("playbook") == playbook_filter)

done = (
    utt.groupBy("call_id")
    .agg(F.max("emitted_at_ms").alias("last_emitted_ms"), F.first("playbook").alias("playbook"),
         F.count("*").alias("n_utterances"))
    .where(F.col("last_emitted_ms") < (F.unix_millis(F.current_timestamp()) - idle_minutes * 60_000))
)

already = spark.table(GOLD).select("call_id") if spark.catalog.tableExists(GOLD) else spark.createDataFrame([], "call_id string")
candidates = done.join(already, "call_id", "left_anti").orderBy("last_emitted_ms").limit(max_calls)
print(f"{candidates.count()} calls to score")

# COMMAND ----------

# MAGIC %md ## 2. Transcript and prompt

# COMMAND ----------

PROMPT_HEAD = """You are a call quality analyst. Score the call transcript below.
Return ONLY a JSON object with exactly these keys:
{"summary": string (<= 60 words),
 "methodology_coverage": [{"key": string, "covered": boolean, "evidence": string|null}],
 "objections_handled": [{"objection": string, "handled": boolean, "how": string|null}],
 "next_steps": [string],
 "overall_score": number between 0 and 1}
Use only what the transcript says. Do not invent facts. If unsure, set covered/handled to false.

TRANSCRIPT:
"""

transcripts = (
    utt.join(candidates.select("call_id"), "call_id", "inner")
    .groupBy("call_id")
    .agg(F.array_sort(F.collect_list(F.struct("seq", F.concat_ws(": ", "speaker", "text").alias("line")))).alias("turns"))
    .select(
        "call_id",
        F.concat(
            F.lit(PROMPT_HEAD),
            F.array_join(F.transform("turns", lambda t: t["line"]), "\n"),
        ).alias("prompt"),
    )
)

# COMMAND ----------

# MAGIC %md ## 3. ai_query scoring

# COMMAND ----------

scored = transcripts.select(
    "call_id",
    F.expr(f"ai_query('{endpoint}', prompt)").alias("raw"),
)

SCORE_SCHEMA = (
    "summary STRING, "
    "methodology_coverage ARRAY<STRUCT<key: STRING, covered: BOOLEAN, evidence: STRING>>, "
    "objections_handled ARRAY<STRUCT<objection: STRING, handled: BOOLEAN, how: STRING>>, "
    "next_steps ARRAY<STRING>, "
    "overall_score DOUBLE"
)

# Strip code fences the model sometimes adds, then parse against the fixed schema.
cleaned = scored.withColumn(
    "json_text",
    F.regexp_replace(F.regexp_replace(F.col("raw"), r"^\s*```(json)?", ""), r"```\s*$", ""),
)
parsed = cleaned.withColumn("s", F.from_json("json_text", SCORE_SCHEMA))

gold = parsed.select(
    "call_id",
    F.col("s.summary").alias("summary"),
    F.col("s.methodology_coverage").alias("methodology_coverage"),
    F.col("s.objections_handled").alias("objections_handled"),
    F.col("s.next_steps").alias("next_steps"),
    F.col("s.overall_score").alias("overall_score"),
    (F.col("s").isNotNull()).alias("parse_ok"),
    F.col("raw").alias("raw_response"),
    F.lit(endpoint).alias("scoring_endpoint"),
    F.current_timestamp().alias("scored_at"),
).join(candidates.select("call_id", "playbook", "n_utterances"), "call_id", "left")

# COMMAND ----------

# MAGIC %md ## 4. Write gold (append only: candidates are anti-joined on prior results)

# COMMAND ----------

if not spark.catalog.tableExists(GOLD):
    gold.limit(0).write.format("delta").saveAsTable(GOLD)

(gold.write.format("delta").mode("append").saveAsTable(GOLD))
print(f"appended scores to {GOLD}")
