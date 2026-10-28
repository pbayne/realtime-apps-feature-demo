"""aiokafka producer for the `utterances` (and `cards`) topics.

Consumers are not needed by the app: the RTM job reads utterances, and enrichments
come back through Lakebase. `subscribe` is implemented for parity with MemoryBus.
Auth: SASL/SSL via env. KAFKA_SECURITY (SASL_SSL_PLAIN | SASL_SSL_SCRAM_SHA_256 |
SASL_SSL_SCRAM_SHA_512, same names as the notebooks) or the raw KAFKA_SECURITY_PROTOCOL /
KAFKA_SASL_MECHANISM pair, plus KAFKA_USERNAME, KAFKA_PASSWORD. Publish failures are
logged, never raised.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from config import Settings

from .base import Handler

log = logging.getLogger("copilot.bus.kafka")

# KAFKA_SECURITY mode (shared with the notebooks) -> SASL mechanism.
_MODE_MECHANISM = {
    "SASL_SSL_PLAIN": "PLAIN",
    "SASL_SSL_SCRAM_SHA_256": "SCRAM-SHA-256",
    "SASL_SSL_SCRAM_SHA_512": "SCRAM-SHA-512",
}


class KafkaBus:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._producer: Any = None
        self._handlers: dict[str, list[Handler]] = {}
        self._consumer_tasks: list[Any] = []

    def _sasl_kwargs(self) -> dict[str, Any]:
        protocol, mechanism = self._s.kafka_security_protocol, self._s.kafka_sasl_mechanism
        if self._s.kafka_security:
            protocol = "SASL_SSL"
            mechanism = _MODE_MECHANISM[self._s.kafka_security]
        kw: dict[str, Any] = {"security_protocol": protocol}
        if protocol in ("SSL", "SASL_SSL"):
            # aiokafka refuses SSL without an explicit context.
            from aiokafka.helpers import create_ssl_context
            kw["ssl_context"] = create_ssl_context()
        if protocol.startswith("SASL"):
            kw["sasl_mechanism"] = mechanism
            kw["sasl_plain_username"] = self._s.kafka_username
            kw["sasl_plain_password"] = self._s.kafka_password
        return kw

    async def start(self) -> None:
        if not self._s.kafka_bootstrap_servers:
            log.warning("KAFKA_BOOTSTRAP_SERVERS unset; publishes will be dropped")
            return
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._s.kafka_bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            linger_ms=0,
            **self._sasl_kwargs(),
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if self._producer is None:
            return
        try:
            await self._producer.send_and_wait(topic, value, key=key)
        except Exception:  # noqa: BLE001 - fail-soft: the call keeps running on local rules
            log.exception("kafka publish to %s failed", topic)

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._handlers.setdefault(topic, []).append(handler)
