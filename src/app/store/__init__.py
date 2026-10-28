"""Store: memory (local) and Lakebase/Postgres (databricks). See base.Store."""

from __future__ import annotations

from config import Settings

from .base import Store
from .memory import MemoryStore


def build_store(settings: Settings) -> Store:
    if settings.effective_store == "lakebase":
        from .lakebase import LakebaseStore

        return LakebaseStore(settings)
    return MemoryStore()


__all__ = ["Store", "MemoryStore", "build_store"]
