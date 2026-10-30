"""The app's fallback DDL (store/lakebase.py) and the setup job's DDL (notebooks/init.py) must agree.

The app creates tables if missing; the setup job creates them (plus the NOTIFY trigger) on deploy.
Whichever runs first wins, so any column/type drift breaks inserts at runtime.
"""

import ast
import re
from pathlib import Path

from store.lakebase import DDL

ROOT = Path(__file__).resolve().parents[2]


def _init_ddl() -> str:
    for node in ast.parse((ROOT / "src/notebooks/init.py").read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TABLE_DDL" for t in node.targets):
            return ";\n".join(ast.literal_eval(node.value))
    raise AssertionError("TABLE_DDL not found")


def _columns(ddl: str) -> dict[str, dict[str, str]]:
    ddl = re.sub(r"--[^\n]*", "", ddl)
    out: dict[str, dict[str, str]] = {}
    for table, body in re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\)\s*(?:;|$)", ddl, re.S):
        cols = {}
        for part in re.split(r",(?![^()]*\))", body):
            tokens = part.replace('"', "").split()
            if not tokens or tokens[0].upper() in ("PRIMARY", "CHECK", "CONSTRAINT"):
                continue
            typ = tokens[1].upper()
            cols[tokens[0]] = {"INT": "INTEGER"}.get(typ, typ)
        out[table] = cols
    return out


def test_app_and_setup_job_define_the_same_tables_and_columns():
    app, job = _columns(DDL), _columns(_init_ddl())
    assert set(app) == set(job) == {"calls", "utterances", "enrichments", "cards", "feedback",
                                    "voice_turns", "voice_items"}
    for table in app:
        assert app[table] == job[table], table
