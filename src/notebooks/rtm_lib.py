# Databricks notebook source
# MAGIC %md
# MAGIC # Shared library for the call-copilot Databricks side
# MAGIC
# MAGIC Pure functions only (no widgets, no dbutils, no network). Imported by
# MAGIC `rtm_enrich.py`, `init.py`, `delta_history.py` via `%run ./rtm_lib`, and by
# MAGIC `tests/notebooks/` via plain `import rtm_lib`.
# MAGIC
# MAGIC Design rules:
# MAGIC - Enrichment uses **Spark SQL built-ins only** (`rlike`, `regexp_extract_all`,
# MAGIC   `filter` / `size` higher-order functions, `from_json`, `to_json`). No Python
# MAGIC   UDFs, because Real-Time Mode has a restricted UDF surface and UDFs add
# MAGIC   per-row Python round-trips that break the sub-second budget.
# MAGIC - Rules come from `enrich_rules.json`, the same file Workstream A loads.

# COMMAND ----------

import json
import re

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)

# SPEC section 3: utterances topic value (key = call_id).
UTTERANCE_SCHEMA = StructType([
    StructField("call_id", StringType()),
    StructField("seq", IntegerType()),
    StructField("speaker", StringType()),
    StructField("text", StringType()),
    StructField("is_final", BooleanType()),
    StructField("t_start_ms", LongType()),
    StructField("t_end_ms", LongType()),
    StructField("emitted_at_ms", LongType()),
    StructField("playbook", StringType()),
])

# SPEC section 3: Card object (cards topic, written by the app for history).
CARD_SCHEMA = StructType([
    StructField("card_id", StringType()),
    StructField("call_id", StringType()),
    StructField("seq", IntegerType()),
    StructField("kind", StringType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("confidence", DoubleType()),
    StructField("source_doc", StringType()),
    StructField("source_clause", StringType()),
    StructField("trigger", StringType()),
    StructField("status", StringType()),
    StructField("created_at_ms", LongType()),
    StructField("trace", StringType()),  # JSON string when present
])

# Question detection for the deterministic `intent` field. The LLM perception
# path (app side) overrides this; RTM only guarantees a cheap baseline.
QUESTION_RE = r"(?i)(\?\s*$)|(^\s*(what|how|when|why|who|where|can|could|do|does|is|are|will|would|should)\b)"

SASL_PLAIN = "SASL_SSL_PLAIN"
SASL_SCRAM_256 = "SASL_SSL_SCRAM_SHA_256"   # Redpanda Cloud (Serverless / Dedicated)
SASL_SCRAM_512 = "SASL_SSL_SCRAM_SHA_512"
MSK_IAM = "SASL_SSL_MSK_IAM"
KAFKA_SECURITY_MODES = (SASL_PLAIN, SASL_SCRAM_256, SASL_SCRAM_512, MSK_IAM)

# SASL mechanism and Spark-shaded JAAS login module for the username/password modes.
_USERPASS_MODES = {
    SASL_PLAIN: ("PLAIN", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"),
    SASL_SCRAM_256: ("SCRAM-SHA-256", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"),
    SASL_SCRAM_512: ("SCRAM-SHA-512", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"),
}


def kafka_needs_credentials(security: str) -> bool:
    """True when the mode authenticates with kafka_api_key / kafka_api_secret from the scope."""
    return security in _USERPASS_MODES


def kafka_sasl_mechanism(security: str) -> str:
    return _USERPASS_MODES[security][0] if security in _USERPASS_MODES else "OAUTHBEARER"

REQUIRED_RULE_GROUPS = ("objections", "risk", "methodology")


# --------------------------------------------------------------------------- #
# Rules
# --------------------------------------------------------------------------- #

def load_rules(source):
    """Load and validate enrich_rules.json.

    `source` is a path or an already-parsed dict. Raises ValueError on any
    schema problem so a bad rules file fails the job before the stream starts.
    """
    if isinstance(source, dict):
        rules = source
    else:
        with open(source, encoding="utf-8") as fh:
            rules = json.load(fh)

    for group in REQUIRED_RULE_GROUPS:
        items = rules.get(group)
        if not isinstance(items, list) or not items:
            raise ValueError(f"enrich_rules: '{group}' must be a non-empty list")
        _validate_items(group, items)

    sentiment = rules.get("sentiment")
    if not isinstance(sentiment, dict):
        raise ValueError("enrich_rules: 'sentiment' must be an object")
    for polarity in ("positive", "negative"):
        words = sentiment.get(polarity)
        if not isinstance(words, list) or not all(isinstance(w, str) for w in words):
            raise ValueError(f"enrich_rules: sentiment.{polarity} must be a list of strings")

    entities = rules.get("entities", [])  # optional extension, see README
    _validate_items("entities", entities)
    rules["entities"] = entities
    return rules


def _validate_items(group, items):
    seen = set()
    for item in items:
        key = item.get("key")
        patterns = item.get("patterns")
        if not isinstance(key, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", key):
            raise ValueError(f"enrich_rules: {group} key {key!r} must be snake_case")
        if key in seen:
            raise ValueError(f"enrich_rules: duplicate {group} key {key!r}")
        seen.add(key)
        if not isinstance(patterns, list) or not patterns:
            raise ValueError(f"enrich_rules: {group}.{key} needs at least one pattern")
        for pattern in patterns:
            try:
                re.compile(pattern)
            except re.error as exc:  # Python re is a proxy for Java regex here
                raise ValueError(f"enrich_rules: {group}.{key} bad pattern {pattern!r}: {exc}")
        excludes = item.get("exclude_patterns", [])
        if not isinstance(excludes, list) or not all(isinstance(e, str) for e in excludes):
            raise ValueError(f"enrich_rules: {group}.{key} exclude_patterns must be a list of strings")
        for pattern in excludes:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"enrich_rules: {group}.{key} bad exclude pattern {pattern!r}: {exc}")


def _group_pattern(patterns):
    """One case-insensitive alternation for a rule item."""
    return "(?i)(?:" + "|".join(f"(?:{p})" for p in patterns) + ")"


EXCLUDE_REPLACEMENT = " "  # must equal rules.EXCLUDE_REPLACEMENT in src/app/enrich/rules.py


def _cleaned(text: Column, item) -> Column:
    """Text with every exclude_patterns match replaced by a space (identity when none).
    Spark built-in regexp_replace; same semantics as RuleItem.cleaned in rules.py."""
    excludes = item.get("exclude_patterns") or []
    if not excludes:
        return text
    return F.regexp_replace(text, F.lit(_group_pattern(excludes)), F.lit(EXCLUDE_REPLACEMENT))


# --------------------------------------------------------------------------- #
# Enrichment expressions (Spark built-ins only)
# --------------------------------------------------------------------------- #

def _hit_keys(text: Column, items) -> Column:
    """Array<string> of every item key whose patterns match `text`."""
    if not items:
        return F.array().cast(ArrayType(StringType()))
    hits = [
        F.when(_cleaned(text, it).rlike(_group_pattern(it["patterns"])), F.lit(it["key"]))
        for it in items
    ]
    return F.filter(F.array(*hits), lambda x: x.isNotNull())


def _first_hit_key(text: Column, items) -> Column:
    """First matching objection key, or NULL."""
    return F.coalesce(*[
        F.when(_cleaned(text, it).rlike(_group_pattern(it["patterns"])), F.lit(it["key"]))
        for it in items
    ])


def _sentiment(text: Column, positive, negative) -> Column:
    """Lexicon score in (-1, 1): (pos - neg) / (pos + neg + 1). Token match."""
    tokens = F.split(F.lower(text), r"[^a-z']+")
    pos_n = F.size(F.filter(tokens, lambda t: t.isin(positive))) if positive else F.lit(0)
    neg_n = F.size(F.filter(tokens, lambda t: t.isin(negative))) if negative else F.lit(0)
    return (pos_n - neg_n).cast("double") / (pos_n + neg_n + F.lit(1.0))


def _entities_json(text: Column, entities) -> Column:
    """JSON object {type: [matches...]} via regexp_extract_all (group 0)."""
    if not entities:
        return F.lit("{}")
    fields = [
        F.regexp_extract_all(_cleaned(text, e), F.lit(_group_pattern(e["patterns"])), 0).alias(e["key"])
        for e in entities
    ]
    return F.to_json(F.struct(*fields))


def build_enrichment(utterances, rules, now_ms: Column = None):
    """Map a parsed utterances DataFrame to the Lakebase `enrichments` shape.

    Input columns: call_id, seq, speaker, text, emitted_at_ms (SPEC section 3).
    `now_ms` defaults to wall-clock ms at evaluation time; tests inject a literal.
    Output columns match the `enrichments` table exactly (plus latency_ms).
    Objections are scored on customer turns only; risk and methodology on all turns.
    """
    if now_ms is None:
        now_ms = F.unix_millis(F.current_timestamp())

    text = F.coalesce(F.col("text"), F.lit(""))
    speaker = F.col("speaker")
    sentiment_rules = rules["sentiment"]

    out = utterances.select(
        F.col("call_id").cast("string").alias("call_id"),
        F.col("seq").cast("int").alias("seq"),
        speaker.alias("speaker"),
        text.alias("text"),
        F.when(text.rlike(QUESTION_RE), F.lit("question"))
         .otherwise(F.lit("statement")).alias("intent"),
        _sentiment(text, sentiment_rules["positive"], sentiment_rules["negative"]).alias("sentiment"),
        F.to_json(_hit_keys(text, rules["risk"])).alias("risk_flags"),
        F.when(speaker == "customer", _first_hit_key(text, rules["objections"]))
         .alias("objection"),
        _entities_json(text, rules["entities"]).alias("entities"),
        F.to_json(_hit_keys(text, rules["methodology"])).alias("methodology_hits"),
        F.col("emitted_at_ms").cast("long").alias("emitted_at_ms"),
        now_ms.cast("long").alias("enriched_at_ms"),
    )
    return out.withColumn("latency_ms", F.col("enriched_at_ms") - F.col("emitted_at_ms"))


# --------------------------------------------------------------------------- #
# Kafka options
# --------------------------------------------------------------------------- #

def _safe_secret_fragment(value: str, name: str) -> str:
    # Values are embedded inside a JAAS string; reject characters that would
    # break quoting rather than trying to escape them.
    if value is None or any(c in value for c in '";\n\r'):
        raise ValueError(f"{name} contains characters not allowed in a JAAS string")
    return value


def kafka_reader_options(bootstrap: str, security: str, api_key: str = None, api_secret: str = None) -> dict:
    """Options for `spark.readStream.format("kafka")` (Kafka client keys keep `kafka.`)."""
    if security not in KAFKA_SECURITY_MODES:
        raise ValueError(f"kafka_security must be one of {KAFKA_SECURITY_MODES}, got {security!r}")
    opts = {"kafka.bootstrap.servers": bootstrap}
    if security in _USERPASS_MODES:
        mechanism, login_module = _USERPASS_MODES[security]
        user = _safe_secret_fragment(api_key, "kafka api key")
        pwd = _safe_secret_fragment(api_secret, "kafka api secret")
        opts.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": mechanism,
            "kafka.sasl.jaas.config": f'{login_module} required username="{user}" password="{pwd}";',
        })
    else:  # MSK_IAM: credentials come from the cluster instance profile
        opts.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;",
            "kafka.sasl.client.callback.handler.class":
                "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        })
    return opts


def topic_names(csv: str) -> list:
    names = [t.strip() for t in csv.split(",") if t.strip()]
    for n in names:
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", n):
            raise ValueError(f"invalid Kafka topic name {n!r}")
    return names
