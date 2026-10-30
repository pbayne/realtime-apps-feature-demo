"""Admin insights (SPEC 14): statement builder, parameterisation, fail-soft, admin gate and cache.

No warehouse is contacted: the executor is a recording fake. The one place that builds SDK parameters is covered
by checking the parameter list it would send.
"""

import asyncio
import re

import pytest
from admin import insights as I
from test_hardening import build_client

ADMIN = {"x-forwarded-email": "patrick.bayne@databricks.com"}
USER = {"x-forwarded-email": "alice@example.com"}


class Recorder:
    def __init__(self, fail_markers=()):
        self.calls: list[tuple[str, dict, dict]] = []
        self.fail_markers = tuple(fail_markers)

    async def __call__(self, statement, params, types):
        self.calls.append((statement, params, types))
        if any(m in statement for m in self.fail_markers):
            raise I.InsightsError("table not found")
        return []


def make(recorder=None, warehouse="wh-1", cache_s=60.0):
    return I.Insights("realtime_call_sidekick_catalog", "call_sidekick", warehouse, cache_s, recorder)


# ----------------------------------------------------------------- statement builder

def test_every_statement_uses_only_bound_parameters_for_values():
    ins = make()
    for name in I._queries():
        sql = ins.statement(name)
        assert not re.search(r"\{t_\w+\}", sql), name
        # A value is a :name parameter. The only quoted text allowed is a fixed constant in the query itself.
        literals = re.findall(r"'[^']*'", sql)
        assert set(literals) <= {"'customer'", "'yyyy-MM-dd'", "''", "'array<string>'", "'array<string>'"}, (name, literals)


def test_table_identifiers_come_from_validated_catalog_and_schema():
    ins = make()
    sql = ins.statement("calls_daily")
    assert "realtime_call_sidekick_catalog.call_sidekick.calls" in sql
    assert ":cutoff_ms" in sql


def test_catalog_and_schema_must_be_plain_identifiers():
    for bad in ("main; DROP TABLE x", "a.b", "", "x-y", "`quoted`"):
        with pytest.raises(ValueError):
            I.Insights(bad, "call_sidekick", "wh")
        with pytest.raises(ValueError):
            I.Insights("main", bad, "wh")


def test_overview_sends_the_cutoff_as_a_bigint_parameter():
    rec = Recorder()
    ins = make(rec)
    asyncio.run(ins.overview(7))
    statement, params, types = rec.calls[0]
    assert set(params) == {"cutoff_ms"} and types == {"cutoff_ms": "BIGINT"}
    assert params["cutoff_ms"] > 0


def test_call_detail_binds_the_call_id_and_never_interpolates_it():
    rec = Recorder()
    ins = make(rec)
    asyncio.run(ins.call_detail("c1' OR '1'='1"))
    for statement, params, types in rec.calls:
        assert "c1" not in statement
        assert params == {"call_id": "c1' OR '1'='1"} and types == {"call_id": "STRING"}


# ----------------------------------------------------------------- fail-soft

def test_unconfigured_warehouse_is_an_empty_state_without_any_call():
    rec = Recorder()
    got = asyncio.run(make(rec, warehouse=None).overview(7))
    assert got["configured"] is False and "DATABRICKS_WAREHOUSE_ID" in got["reason"]
    assert rec.calls == []


def test_one_failing_section_does_not_blank_the_page():
    rec = Recorder(fail_markers=["objection IS NOT NULL"])   # the top_objections statement only
    got = asyncio.run(make(rec).overview(7))
    assert got["configured"] is True
    assert "top_objections" in got["errors"]
    assert got["sections"]["top_objections"] == []
    assert "calls_daily" in got["sections"]


def test_the_result_is_cached_for_the_ttl():
    rec = Recorder()
    ins = make(rec, cache_s=60.0)
    asyncio.run(ins.overview(7))
    n = len(rec.calls)
    asyncio.run(ins.overview(7))
    assert len(rec.calls) == n, "second call inside the TTL must not hit the warehouse"
    asyncio.run(ins.overview(30))
    assert len(rec.calls) > n, "a different window is a different cache key"


# ----------------------------------------------------------------- HTTP

@pytest.fixture()
def client(monkeypatch):
    with build_client(monkeypatch, mode="databricks", store_backend="memory", history_backend="memory",
                      admin_emails="patrick.bayne@databricks.com") as c:
        yield c


def test_insights_are_admin_only(client):
    assert client.get("/api/admin/insights", headers=USER).status_code == 403
    assert client.get("/api/admin/insights").status_code == 403
    assert client.get("/api/admin/calls/abc", headers=USER).status_code == 403


def test_days_must_be_between_one_and_ninety(client):
    assert client.get("/api/admin/insights?days=0", headers=ADMIN).status_code == 422
    assert client.get("/api/admin/insights?days=91", headers=ADMIN).status_code == 422
    assert client.get("/api/admin/insights?days=abc", headers=ADMIN).status_code == 422


def test_admin_gets_the_empty_state_without_a_warehouse(client):
    r = client.get("/api/admin/insights?days=7", headers=ADMIN)
    assert r.status_code == 200
    assert r.json()["configured"] is False


def test_call_id_shape_is_checked_before_any_query(client):
    assert client.get("/api/admin/calls/bad%20id%3B", headers=ADMIN).status_code == 404
