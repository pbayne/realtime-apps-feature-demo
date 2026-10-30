"""Every `%run ./<lib>` target must be a Databricks notebook (first line `# Databricks notebook source`).

Bundle sync uploads a .py file without that header as a plain FILE, and `%run` cannot include files: the
reconcile job failed every run with "Notebook not found: .../reconcile_lib".
"""

import re
from pathlib import Path

NB = Path(__file__).resolve().parents[2] / "src" / "notebooks"


def test_every_run_target_is_a_notebook():
    targets = set()
    for f in NB.glob("*.py"):
        targets |= set(re.findall(r"%run \./([A-Za-z0-9_]+)", f.read_text()))
    assert targets, "expected %run includes"
    for lib in sorted(targets):
        first = (NB / f"{lib}.py").read_text().splitlines()[0]
        assert first == "# Databricks notebook source", f"{lib}.py would be uploaded as a FILE, not a notebook"
