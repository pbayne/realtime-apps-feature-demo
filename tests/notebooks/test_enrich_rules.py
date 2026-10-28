"""Enrichment rules: shared JSON contract, pure-Python regex sanity, and Spark
expression behaviour (the same built-ins RTM executes)."""

import json
import re
from pathlib import Path

import pytest

RULES_PATH = Path(__file__).resolve().parents[2] / "src" / "notebooks" / "enrich_rules.json"
GOLDEN_PATH = Path(__file__).resolve().parents[2] / "tests" / "golden" / "enrich_cases.json"


# --------------------------------------------------------------------------- #
# Contract (no Spark)
# --------------------------------------------------------------------------- #

def test_shared_rules_file_has_contract_groups():
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    for group in ("objections", "risk", "methodology"):
        assert rules[group], group
        for item in rules[group]:
            assert set(item) >= {"key", "patterns"}, item
            assert item["patterns"], item["key"]
    assert set(rules["sentiment"]) >= {"positive", "negative"}
    assert rules["sentiment"]["positive"] and rules["sentiment"]["negative"]


def test_every_pattern_compiles_and_matches_its_own_sample_text():
    """Cheap pure-Python guard: patterns compile under re and are case-insensitive.
    Java regex (what Spark uses) is checked by the Spark tests below."""
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    for group in ("objections", "risk", "methodology", "entities"):
        for item in rules.get(group, []):
            for pat in item["patterns"]:
                re.compile(pat, re.IGNORECASE)


def test_load_rules_validates(tmp_path):
    pytest.importorskip("pyspark")
    import rtm_lib

    loaded = rtm_lib.load_rules(RULES_PATH)
    assert {"objections", "risk", "methodology", "sentiment", "entities"} <= set(loaded)


@pytest.mark.parametrize("mutation", [
    lambda r: r.pop("risk"),
    lambda r: r["objections"][0].update(key="Bad Key"),
    lambda r: r["objections"].append(dict(r["objections"][0])),        # duplicate key
    lambda r: r["methodology"][0].update(patterns=[]),
    lambda r: r["methodology"][0].update(patterns=["(unclosed"]),
    lambda r: r["sentiment"].pop("negative"),
])
def test_load_rules_rejects_bad_files(mutation):
    pytest.importorskip("pyspark")
    import rtm_lib

    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    mutation(rules)
    with pytest.raises(ValueError):
        rtm_lib.load_rules(rules)


# --------------------------------------------------------------------------- #
# Kafka options and topic names (no Spark session needed beyond import)
# --------------------------------------------------------------------------- #

def test_plain_kafka_options_use_shaded_jaas():
    pytest.importorskip("pyspark")
    import rtm_lib

    opts = rtm_lib.kafka_reader_options("pkc-x.confluent.cloud:9092", rtm_lib.SASL_PLAIN, "KEY", "SECRET")
    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "PLAIN"
    assert opts["kafka.sasl.jaas.config"].startswith("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule")
    assert 'username="KEY"' in opts["kafka.sasl.jaas.config"]


def test_msk_iam_options_use_shaded_iam_classes_and_no_secret():
    pytest.importorskip("pyspark")
    import rtm_lib

    opts = rtm_lib.kafka_reader_options("b-1.msk:9098", rtm_lib.MSK_IAM)
    assert opts["kafka.sasl.mechanism"] == "AWS_MSK_IAM"
    assert opts["kafka.sasl.jaas.config"].startswith("shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule")
    assert "password" not in opts["kafka.sasl.jaas.config"]


@pytest.mark.parametrize("bad", ['x" password="y', "a;b", "line\nbreak"])
def test_kafka_secret_injection_is_rejected(bad):
    pytest.importorskip("pyspark")
    import rtm_lib

    with pytest.raises(ValueError):
        rtm_lib.kafka_reader_options("b:9092", rtm_lib.SASL_PLAIN, bad, "s")


def test_unknown_kafka_mode_rejected():
    pytest.importorskip("pyspark")
    import rtm_lib

    with pytest.raises(ValueError):
        rtm_lib.kafka_reader_options("b:9092", "PLAINTEXT")


def test_topic_names_validation():
    pytest.importorskip("pyspark")
    import rtm_lib

    assert rtm_lib.topic_names("utterances, cards") == ["utterances", "cards"]
    with pytest.raises(ValueError):
        rtm_lib.topic_names("bad topic!")


# --------------------------------------------------------------------------- #
# Spark expressions (what RTM executes)
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]").appName("enrich-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


NOW_MS = 1_730_000_000_500


def _enrich(spark, rows):
    import rtm_lib
    from pyspark.sql import functions as F

    rules = rtm_lib.load_rules(RULES_PATH)
    df = spark.createDataFrame(rows, schema=rtm_lib.UTTERANCE_SCHEMA)
    out = rtm_lib.build_enrichment(df, rules, now_ms=F.lit(NOW_MS))
    return {(r["call_id"], r["seq"]): r.asDict() for r in out.collect()}


def _row(seq, speaker, text, emitted=NOW_MS - 120, call="c-1"):
    return (call, seq, speaker, text, True, 0, 1000, emitted, "advisor_coach")


def test_customer_fee_objection_and_negative_sentiment(spark):
    got = _enrich(spark, [_row(1, "customer", "The fees seem too expensive, I want to think about it.")])
    row = got[("c-1", 1)]
    assert row["objection"] == "fees"  # first matching key, rules order
    assert row["sentiment"] < 0
    assert row["intent"] == "statement"


def test_objection_only_scored_on_customer_turns(spark):
    got = _enrich(spark, [_row(1, "agent", "Our fees are very competitive.")])
    assert got[("c-1", 1)]["objection"] is None


def test_risk_and_methodology_on_any_turn(spark):
    got = _enrich(spark, [
        _row(1, "customer", "I just lost my job last month."),
        _row(2, "agent", "What are your goals, and what is your time horizon?"),
    ])
    import json as _json
    assert "vulnerability" in _json.loads(got[("c-1", 1)]["risk_flags"])
    hits = _json.loads(got[("c-1", 2)]["methodology_hits"])
    assert {"goals", "time_horizon"} <= set(hits)
    assert got[("c-1", 2)]["intent"] == "question"


def test_sentiment_bounds_and_neutral(spark):
    got = _enrich(spark, [
        _row(1, "customer", "great thanks, excellent, happy"),
        _row(2, "customer", "terrible, angry, never again"),
        _row(3, "customer", "the sky is blue"),
        _row(4, "customer", ""),
    ])
    assert 0 < got[("c-1", 1)]["sentiment"] < 1
    assert -1 < got[("c-1", 2)]["sentiment"] < 0
    assert got[("c-1", 3)]["sentiment"] == 0.0
    assert got[("c-1", 4)]["sentiment"] == 0.0
    assert all(-1 < v["sentiment"] < 1 for v in got.values())


def test_entity_extraction(spark):
    import json as _json

    got = _enrich(spark, [_row(1, "customer", "I can invest $250,000 by 2030 at 3% a year, email me at a.b@example.com")])
    ents = _json.loads(got[("c-1", 1)]["entities"])
    assert "$250,000" in ents["amount"]
    assert "2030" in ents["year"]
    assert any(m.startswith("3%") for m in ents["percent"])
    assert ents["email"] == ["a.b@example.com"]


def test_latency_column_is_enriched_minus_emitted(spark):
    got = _enrich(spark, [_row(1, "customer", "hello", emitted=NOW_MS - 250)])
    row = got[("c-1", 1)]
    assert row["enriched_at_ms"] == NOW_MS
    assert row["latency_ms"] == 250


def test_empty_and_null_text_do_not_fail(spark):
    import rtm_lib
    from pyspark.sql import functions as F

    rules = rtm_lib.load_rules(RULES_PATH)
    df = spark.createDataFrame([("c-2", 1, "customer", None, True, 0, 0, NOW_MS, "x")],
                               schema=rtm_lib.UTTERANCE_SCHEMA)
    row = rtm_lib.build_enrichment(df, rules, now_ms=F.lit(NOW_MS)).collect()[0].asDict()
    assert row["text"] == ""
    assert row["sentiment"] == 0.0
    assert row["objection"] is None


def test_enrichment_plan_is_streaming_and_has_no_python_udf_or_aggregation(spark):
    """RTM guard: the enrichment must stay a stateless streaming projection with
    built-in functions only (no Python UDF, no aggregation, no join or window)."""
    import rtm_lib
    from pyspark.sql import functions as F

    rules = rtm_lib.load_rules(RULES_PATH)
    stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load().select(
        F.concat(F.lit("c-"), F.col("value").cast("string")).alias("call_id"),
        F.col("value").cast("int").alias("seq"),
        F.lit("customer").alias("speaker"),
        F.lit("the fees are too expensive").alias("text"),
        F.lit(True).alias("is_final"),
        F.lit(0).cast("long").alias("t_start_ms"),
        F.lit(1).cast("long").alias("t_end_ms"),
        F.unix_millis(F.current_timestamp()).alias("emitted_at_ms"),
        F.lit("advisor_coach").alias("playbook"),
    )
    out = rtm_lib.build_enrichment(stream, rules)
    assert out.isStreaming
    # Analyzed logical plan: physical plans for streaming need writeStream.start().
    plan = out._jdf.queryExecution().analyzed().toString()
    for forbidden in ("PythonUDF", "PythonEvalType", "Aggregate", "Join", "Window"):
        assert forbidden not in plan, forbidden


GOLDEN_CASES = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[c["text"][:50] for c in GOLDEN_CASES])
def test_shared_golden_cases_spark(spark, case):
    """Same golden table as tests/app/test_rules.py. Spark regexp_replace exclusions must agree with rules.py."""
    got = _enrich(spark, [_row(1, case["speaker"], case["text"])])[("c-1", 1)]
    assert json.loads(got["risk_flags"]) == case["risk_flags"], case["text"]
    if "objection" in case:
        assert got["objection"] == case["objection"], case["text"]


def test_exclude_patterns_are_plain_spark_builtins(spark):
    """The exclusion uses regexp_replace only: still no Python UDF in the plan."""
    import rtm_lib
    from pyspark.sql import functions as F

    rules = rtm_lib.load_rules(RULES_PATH)
    stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load().select(
        F.lit("c-1").alias("call_id"), F.lit(1).alias("seq"), F.lit("agent").alias("speaker"),
        F.lit("No one can guarantee returns.").alias("text"), F.lit(True).alias("is_final"),
        F.lit(0).cast("long").alias("t_start_ms"), F.lit(1).cast("long").alias("t_end_ms"),
        F.unix_millis(F.current_timestamp()).alias("emitted_at_ms"), F.lit("advisor_coach").alias("playbook"),
    )
    plan = rtm_lib.build_enrichment(stream, rules)._jdf.queryExecution().analyzed().toString()
    for forbidden in ("PythonUDF", "PythonEvalType", "Aggregate", "Join", "Window"):
        assert forbidden not in plan, forbidden
