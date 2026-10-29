"""Kafka round-trip smoke test against a real broker (e.g. Redpanda Serverless).

Publishes one utterance through the app's KafkaBus as the producer user, reads it back
as the consumer user, and checks the producer user is denied reads (least privilege).
Credentials come from an env file and are never printed.

  .venv/bin/python scripts/kafka_smoke.py ~/.config/sidekick/redpanda.env
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "app"))

from aiokafka import AIOKafkaConsumer  # noqa: E402
from aiokafka.errors import KafkaError  # noqa: E402
from bus.kafka import KafkaBus  # noqa: E402
from config import Settings  # noqa: E402


def load_env(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def settings(env: dict[str, str], user: str, password: str) -> Settings:
    return Settings(mode="databricks", kafka_bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
                    kafka_security=env["KAFKA_SECURITY"], kafka_username=user, kafka_password=password)


async def consume_one(s: Settings, topic: str, call_id: str, timeout_s: float) -> dict | None:
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=s.kafka_bootstrap_servers, group_id=f"spark-kafka-source-smoke-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest", enable_auto_commit=False, **KafkaBus(s)._sasl_kwargs())
    try:
        await consumer.start()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            batch = await consumer.getmany(timeout_ms=1000)
            for records in batch.values():
                for r in records:
                    if r.key and r.key.decode() == call_id:
                        return {"partition": r.partition, "offset": r.offset, "ts": r.timestamp}
        return None
    finally:
        await consumer.stop()


async def main() -> None:
    env = load_env(Path(sys.argv[1]).expanduser())
    app = settings(env, env["SIDEKICK_APP_USER"], env["SIDEKICK_APP_PASSWORD"])
    rtm = settings(env, env["SIDEKICK_RTM_USER"], env["SIDEKICK_RTM_PASSWORD"])
    call_id = f"smoke-{uuid.uuid4().hex[:8]}"

    bus = KafkaBus(app)
    await bus.start()
    acks = []
    for seq in range(11):  # first send includes connection + metadata; the rest are steady state
        t0 = time.monotonic()
        await bus.publish("utterances", call_id, {"call_id": call_id, "seq": seq, "speaker": "customer",
                                                  "text": "smoke test", "is_final": True,
                                                  "emitted_at_ms": int(time.time() * 1000)})
        acks.append((time.monotonic() - t0) * 1000)
    await bus.stop()
    steady = sorted(acks[1:])
    print(f"produce (as {app.kafka_username}): first {acks[0]:.0f} ms, steady p50 {steady[len(steady) // 2]:.0f} ms "
          f"max {steady[-1]:.0f} ms (acks=all)")

    got = await consume_one(rtm, "utterances", call_id, 30)
    print(f"consume (as {rtm.kafka_username}):", f"ok {got}" if got else "NOT FOUND")

    try:
        denied = await consume_one(app, "utterances", call_id, 8) is None
    except KafkaError as exc:
        denied = True
        print(f"  app-user read raised {type(exc).__name__}")
    print(f"least privilege (app user cannot read): {'ok' if denied else 'FAILED - app user could read'}")
    sys.exit(0 if got and denied else 1)


if __name__ == "__main__":
    asyncio.run(main())
