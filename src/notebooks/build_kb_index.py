# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — playbook KB → Delta → Vector Search
# MAGIC
# MAGIC Reads `src/app/playbooks/kb/<playbook>/*.md` from the bundle, splits each document on `##` headings
# MAGIC into chunks, writes a Delta table with Change Data Feed enabled, and creates
# MAGIC (or syncs) a **delta-sync** Vector Search index that embeds `content` with
# MAGIC `databricks-gte-large-en` (SPEC section 6 `EMBED_MODEL`).
# MAGIC
# MAGIC Retrieval is the app's job (`sidekick/retrieval.py`, top-3 per utterance). This
# MAGIC notebook only builds the index.
# MAGIC
# MAGIC Chunk ids are `<doc stem>#<n>`. Each chunk keeps `source_doc` and `clause` (the
# MAGIC heading), which the card's `source_doc` / `source_clause` fields need.

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-vectorsearch>=0.50" "databricks-sdk>=0.40"

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("kb_dir", "")                       # ${workspace.file_path}/src/app/playbooks/kb
dbutils.widgets.text("vs_endpoint", "call-sidekick-vs")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")
dbutils.widgets.text("index_name", "kb_chunks_index")

catalog = dbutils.widgets.get("uc_catalog")
schema = dbutils.widgets.get("uc_schema")
kb_dir = dbutils.widgets.get("kb_dir").rstrip("/")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
index_short = dbutils.widgets.get("index_name")

assert kb_dir, "kb_dir is required"
TABLE = f"{catalog}.{schema}.kb_chunks"
INDEX = f"{catalog}.{schema}.{index_short}"

# COMMAND ----------

# MAGIC %md ## 1. Read and chunk the markdown

# COMMAND ----------

import glob
import os
import re
from pathlib import Path

HEADING = re.compile(r"^(#{1,3})\s+(.+?)\s*$", re.M)
MAX_CHARS = 2000  # ~500 tokens; gte-large truncates at 512 tokens


def chunk_markdown(doc_id: str, text: str):
    """Split on #/##/### headings. Long sections are split on paragraph boundaries."""
    matches = list(HEADING.finditer(text))
    sections = []
    if not matches:
        sections.append(("", text))
    else:
        if matches[0].start() > 0 and text[: matches[0].start()].strip():
            sections.append(("", text[: matches[0].start()]))
        for i, m in enumerate(matches):
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            sections.append((m.group(2).strip(), text[m.start():end]))

    chunks = []
    for clause, body in sections:
        body = body.strip()
        if not body:
            continue
        pieces, buf = [], ""
        for para in re.split(r"\n\s*\n", body):
            if len(buf) + len(para) > MAX_CHARS and buf:
                pieces.append(buf)
                buf = ""
            buf = f"{buf}\n\n{para}".strip()
        if buf:
            pieces.append(buf)
        for piece in pieces:
            chunks.append({"doc_id": doc_id, "clause": clause or doc_id, "content": piece})
    for n, c in enumerate(chunks):
        c["chunk_id"] = f"{doc_id}#{n}"
    return chunks


# KB docs live per playbook: kb/<playbook>/<doc>.md (the app's retriever filters on `playbook`).
paths = sorted(glob.glob(os.path.join(kb_dir, "**", "*.md"), recursive=True))
if not paths:
    raise FileNotFoundError(f"no *.md under {kb_dir} (expected kb/<playbook>/<doc>.md)")

rows = []
for path in paths:
    rel = Path(path).relative_to(kb_dir)
    playbook = rel.parts[0] if len(rel.parts) > 1 else "shared"
    doc_id = f"kb/{rel.as_posix()}"           # same doc ids the app cites (kb/advisor_coach/fee_objections.md)
    for c in chunk_markdown(doc_id, Path(path).read_text(encoding="utf-8")):
        c["playbook"] = playbook
        c["title"] = Path(path).stem.replace("_", " ")
        rows.append(c)
print(f"{len(paths)} documents -> {len(rows)} chunks across {len({r['playbook'] for r in rows})} playbooks")

# COMMAND ----------

# MAGIC %md ## 2. Write the Delta source table (CDF on, required for delta-sync)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Column names match the app's Vector Search client (sidekick/retrieval.py): doc_id, title, section, text.
schema_ddl = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("playbook", StringType(), False),
    StructField("doc_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("section", StringType(), True),
    StructField("text", StringType(), False),
])
records = [{"chunk_id": r["chunk_id"], "playbook": r["playbook"], "doc_id": r["doc_id"], "title": r["title"],
            "section": r["clause"], "text": r["content"]} for r in rows]
df = spark.createDataFrame(records, schema=schema_ddl).withColumn("updated_at", F.current_timestamp())

(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))
spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"wrote {df.count()} rows to {TABLE}")

# COMMAND ----------

# MAGIC %md ## 3. Vector Search endpoint and delta-sync index

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

endpoint_names = {e["name"] for e in vsc.list_endpoints().get("endpoints", [])}
if vs_endpoint not in endpoint_names:
    vsc.create_endpoint_and_wait(name=vs_endpoint, endpoint_type="STANDARD")
    print(f"created Vector Search endpoint {vs_endpoint}")

try:
    index = vsc.get_index(endpoint_name=vs_endpoint, index_name=INDEX)
    index.sync()
    print(f"synced existing index {INDEX}")
except Exception:
    vsc.create_delta_sync_index_and_wait(
        endpoint_name=vs_endpoint,
        index_name=INDEX,
        source_table_name=TABLE,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",
        embedding_source_column="text",
        embedding_model_endpoint_name=embedding_endpoint,
    )
    print(f"created index {INDEX}")

# COMMAND ----------

# MAGIC %md ## 4. Smoke query

# COMMAND ----------

res = vsc.get_index(endpoint_name=vs_endpoint, index_name=INDEX).similarity_search(
    query_text="client objects to the advisory fee",
    columns=["chunk_id", "doc_id", "clause"],
    num_results=3,
)
print(res.get("result", {}).get("data_array", []))
