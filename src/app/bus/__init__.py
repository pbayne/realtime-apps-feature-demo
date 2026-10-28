"""Event bus: memory (local) and Kafka (databricks). See base.EventBus."""

from __future__ import annotations

from config import Settings

from .base import EventBus, Handler
from .memory import MemoryBus


def build_bus(settings: Settings) -> EventBus:
    if settings.effective_bus == "kafka":
        from .kafka import KafkaBus

        return KafkaBus(settings)
    return MemoryBus()


__all__ = ["EventBus", "Handler", "MemoryBus", "build_bus"]
