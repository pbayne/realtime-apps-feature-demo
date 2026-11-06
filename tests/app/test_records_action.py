"""update_record action (SPEC 12): the Databricks-native action that MERGEs a CRM account into a Unity Catalog
Delta table through the SQL warehouse. Schema validation, outbox mode (no warehouse), warehouse execution with a
fake executor (asserting parameterized SQL + bound params), and undo. No network.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from actions import records
from actions.schemas import ActionValidationError, summarize, validate_params
from actions.service import ActionService
from config import Settings
from enrich.rules import load_rules
from sidekick.playbooks import PlaybookRegistry
from store.memory import MemoryStore

APP = Path(__file__).resolve().parents[2] / "src" / "app"
OWNER = "dana.advisor@databricks.com"
RECORD = {"account_id": "acct-northwind", "fields": {"stage": "Proposal", "notes": "Hot; wants fee breakdown"}}


class FakeWarehouse:
    """Records every statement the action runs, and answers SELECTs from an in-memory table keyed by account_id."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.rows: dict[str, dict] = {}

    async def __call__(self, statement: str, params: dict, types: dict) -> list[dict]:
        self.calls.append((statement, dict(params)))
        if statement.strip().upper().startswith("SELECT"):
            row = self.rows.get(params["account_id"])
            return [dict(row)] if row else []
        if statement.strip().upper().startswith("MERGE"):
            cur = self.rows.get(params["account_id"], {"account_id": params["account_id"]})
            self.rows[params["account_id"]] = {**cur, **{k: v for k, v in params.items()}}
            return []
        if statement.strip().upper().startswith("DELETE"):
            self.rows.pop(params["account_id"], None)
            return []
        return []


def _registry() -> PlaybookRegistry:
    return PlaybookRegistry.load(APP / "playbooks", load_rules(None), APP / "data" / "replay")


def _service(records_exec=None, **settings):
    base = dict(mode="local", llm_enabled=False, action_safety_lock=False, smtp_host=None, smtp_from=None)
    base.update(settings)
    store = MemoryStore()
    svc = ActionService(settings=Settings(**base), store=store, registry=_registry(), records_exec=records_exec)
    return svc


# ----------------------------------------------------------------- schema
def test_update_record_validates_and_summarizes():
    clean = validate_params("update_record", RECORD)
    assert clean["account_id"] == "acct-northwind"
    assert clean["fields"]["stage"] == "Proposal" and clean["fields"]["name"] is None
    assert summarize("update_record", clean).startswith("Update account acct-northwind:")


def test_update_record_rejects_empty_fields_and_unknown_columns():
    with pytest.raises(ActionValidationError):
        validate_params("update_record", {"account_id": "a", "fields": {}})
    with pytest.raises(ActionValidationError):
        validate_params("update_record", {"account_id": "a", "fields": {"salary": 1}})
    with pytest.raises(ActionValidationError):
        validate_params("update_record", {"account_id": "bad id!", "fields": {"stage": "x"}})


# ----------------------------------------------------------------- merge builder (parameterized, no injection)
def test_build_merge_is_parameterized_and_table_qualified():
    target = records.RecordTarget("main", "call_sidekick", "crm_accounts")
    sql, params, types = records.build_merge(target, "acct-x", {"stage": "Won", "notes": "'; DROP TABLE"}, 123)
    assert "main.call_sidekick.crm_accounts" in sql
    assert ":account_id" in sql and ":stage" in sql and ":notes" in sql
    # The malicious value is a bound parameter, never inlined into the SQL text.
    assert "DROP TABLE" not in sql
    assert params["notes"] == "'; DROP TABLE" and params["account_id"] == "acct-x"
    assert types["arr"] == "DOUBLE" if "arr" in types else True


# ----------------------------------------------------------------- outbox (no warehouse)
def test_outbox_renders_the_merge_without_running_anything():
    svc = _service(records_exec=None)

    async def run():
        rec = (await svc.propose(owner=OWNER, playbook="advisor_coach",
                                 proposals=[("update_record", validate_params("update_record", RECORD))],
                                 call_id="c-1"))[0]
        done = await svc.decide(OWNER, rec.action_id, "approve")
        return done

    done = asyncio.run(run())
    assert done.status == "outboxed"
    assert done.result["mode"] == "outbox"
    assert "MERGE INTO main.call_sidekick.crm_accounts" in done.result["merge_sql"]


# ----------------------------------------------------------------- warehouse execution + undo
def test_warehouse_merge_runs_and_is_undoable():
    wh = FakeWarehouse()
    wh.rows["acct-northwind"] = {"account_id": "acct-northwind", "stage": "Discovery", "notes": "old",
                                 "name": "Northwind", "arr": 1.0, "owner": "o", "next_step": "x",
                                 "updated_at_ms": 1}
    svc = _service(records_exec=wh)

    async def run():
        rec = (await svc.propose(owner=OWNER, playbook="advisor_coach",
                                 proposals=[("update_record", validate_params("update_record", RECORD))],
                                 call_id="c-1"))[0]
        done = await svc.decide(OWNER, rec.action_id, "approve")
        # Snapshot the done state before undo (the service returns one mutable Action per id).
        done_snap = {"status": done.status, "result": dict(done.result or {})}
        undone = await svc.undo(OWNER, rec.action_id)
        return done_snap, undone

    done, undone = asyncio.run(run())
    assert done["status"] == "done" and done["result"]["mode"] == "warehouse" and done["result"]["existed"] is True
    # A SELECT (capture previous) then a MERGE ran.
    kinds = [c[0].strip().split()[0].upper() for c in wh.calls]
    assert "SELECT" in kinds and "MERGE" in kinds
    # Undo restored the previous stage via another MERGE.
    assert undone.status == "undone"
    assert wh.rows["acct-northwind"]["stage"] == "Discovery"


def test_undo_deletes_a_record_the_update_created():
    wh = FakeWarehouse()  # empty: the account does not exist yet
    svc = _service(records_exec=wh)

    async def run():
        rec = (await svc.propose(owner=OWNER, playbook="advisor_coach",
                                 proposals=[("update_record", validate_params("update_record",
                                             {"account_id": "acct-new", "fields": {"name": "New Co"}}))],
                                 call_id="c-1"))[0]
        await svc.decide(OWNER, rec.action_id, "approve")
        assert "acct-new" in wh.rows
        await svc.undo(OWNER, rec.action_id)

    asyncio.run(run())
    assert "acct-new" not in wh.rows, "undo deletes a row the update created"
