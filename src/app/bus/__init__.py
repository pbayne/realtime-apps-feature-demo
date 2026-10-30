"""Event bus: none (direct backbone), memory (kafka backbone, local) and Kafka (kafka backbone, databricks).
See base.EventBus."""

from __future__ import annotations

from config import Settings

from .base import EventBus, Handler
from .memory import MemoryBus
from .null import NullBus


def build_bus(settings: Settings) -> EventBus:
    if settings.effective_bus == "none":
        return NullBus()
    if settings.effective_bus == "kafka":
        from .kafka import KafkaBus

        return KafkaBus(settings)
    return MemoryBus()


__all__ = ["EventBus", "Handler", "MemoryBus", "NullBus", "build_bus"]
