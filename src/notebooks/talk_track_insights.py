# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick: talk-track insights (Lakeflow job, serverless, daily)
# MAGIC
# MAGIC Reads the last `days` of call transcripts and the cards the sidekick showed (Zerobus history tables), asks
# MAGIC a model (`ai_query`) for recurring objections, responses that worked, knowledge-base gaps and coverage gaps,
# MAGIC and appends the run to `talk_track_insights`. The admin insights page shows the latest run (MAX(run_id)).
# MAGIC
# MAGIC Pure helpers (prompt, parsing, run id) live in `talk_track_lib.py` and are unit tested. Nothing is written
# MAGIC when the model response does not parse: the run records zero items and the dropped count, so a bad
# MAGIC response never looks like "no gaps".

# COMMAND ----------

# MAGIC %run ./talk_track_lib

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("scoring_endpoint", "databricks-claude-sonnet-5-5")
dbutils.widgets.text("days", "7")
dbutils.widgets.text("max_calls", "150")
dbutils.widgets.text("utterances_table", "utterances")
dbutils.widgets.text("cards_table", "cards")

import re
from datetime import datetime, timezone

catalog = dbutils.widgets.get("uc_catalog")
schema = dbutils.widgets.get("uc_schema")
endpoint = dbutils.widgets.get("scoring_endpoint")
days = int(dbutils.widgets.get("days"))
max_calls = int(dbutils.widgets.get("max_calls"))

assert re.fullmatch(r"[A-Za-z0-9._-]+", endpoint), "scoring_endpoint must be a serving endpoint name"
assert 1 <= days <= 90 and 1 <= max_calls <= 500
UTT = f"{catalog}.{schema}.{dbutils.widgets.get('utterances_table')}"
CARDS = f"{catalog}.{schema}.{dbutils.widgets.get('cards_table')}"
GOLD = f"{catalog}.{schema}.talk_track_insights"

# COMMAND ----------

# MAGIC %md ## 1. Calls in the window, rendered as transcript plus cards

# COMMAND ----------

from pyspark.sql import functions as F

cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - days * 86_400_000
utt = spark.table(UTT).where(F.col("emitted_at_ms") >= cutoff_ms)
cards = spark.table(CARDS).where(F.col("created_at_ms") >= cutoff_ms)

lines = (
    utt.select("call_id", "seq", F.concat_ws(": ", "speaker", "text").alias("line"), "playbook")
    .groupBy("call_id")
    .agg(F.array_sort(F.collect_list(F.struct("seq", "line"))).alias("turns"), F.first("playbook").alias("playbook"))
)
card_lines = (
    cards.select("call_id", F.concat_ws(" | ", F.lit("CARD"), "kind", "title", "status").alias("card"))
    .groupBy("call_id")
    .agg(F.collect_list("card").alias("cards"))
)
calls = (
    lines.join(card_lines, "call_id", "left")
    .orderBy(F.desc(F.array_max(F.transform("turns", lambda t: t["seq"]))))
    .limit(max_calls)
    .collect()
)
print(f"{len(calls)} calls in the last {days} days")

# COMMAND ----------

# MAGIC %md ## 2. Ask the model (one ai_query call over the batch)

# COMMAND ----------

rendered = []
for row in calls:
    turns = [t["line"] for t in sorted(row["turns"], key=lambda t: t["seq"])]
    cards_txt = list(row["cards"] or [])
    rendered.append((row["call_id"], "\n".join(turns + cards_txt)))

prompt, included = build_prompt(rendered)
print(f"prompt covers {included} calls")

raw = None
if included:
    raw = (
        spark.createDataFrame([(prompt,)], "prompt string")
        .select(F.expr(f"ai_query('{endpoint}', prompt)").alias("raw"))
        .collect()[0]["raw"]
    )

items, dropped = parse_items(raw)
print(f"{len(items)} items, {dropped} dropped")

# COMMAND ----------

# MAGIC %md ## 3. Append the run to talk_track_insights

# COMMAND ----------

generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
rid = run_id(generated_at)
out_schema = (
    "run_id STRING, generated_at STRING, kind STRING, playbook STRING, label STRING, detail STRING, "
    "occurrences INT, evidence_calls INT, scoring_endpoint STRING, window_days INT, calls_in_prompt INT, "
    "items_dropped INT"
)
rows = [
    (
        rid, generated_at, it["kind"], None, it["label"], it["detail"], it["occurrences"],
        it["evidence_calls"], endpoint, days, included, dropped,
    )
    for it in items
]
if not rows:
    # Keep one marker row so the page can say the run happened and why it is empty.
    rows = [(rid, generated_at, "none", None, "no items found", "", 0, 0, endpoint, days, included, dropped)]

out = spark.createDataFrame(rows, out_schema).withColumn("generated_at", F.to_timestamp("generated_at"))
out.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(GOLD)
print(f"run {rid} appended to {GOLD} ({len(rows)} rows)")
