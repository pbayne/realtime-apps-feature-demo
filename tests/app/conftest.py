"""Shared test setup. Adds src/app to sys.path; no network or Databricks access in tests."""

import os
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[2] / "src" / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

os.environ.setdefault("MODE", "local")
os.environ.setdefault("LLM_ENABLED", "false")
os.environ.pop("DATABRICKS_CONFIG_PROFILE", None)
