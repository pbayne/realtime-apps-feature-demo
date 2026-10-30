"""Live-mic calls against a cold (scale-to-zero) Parakeet endpoint, and a Lakebase role that does not own the tables.

Found on the deployed app: a mic call against a cold endpoint tripped the LLM breaker for sidekick-parakeet,
so every later window was rejected even once the GPU was up; and the app's store never started because the
app role could not run ALTER TABLE on tables owned by the setup role (it then ran on the in-memory fallback).
"""

import asyncio

import pytest
from llm import LlmUnavailable
from stt.base import Window
from stt.parakeet import WARMING_AFTER, ParakeetDecoder


class ColdThenWarmLlm:
    """multipart() fails `cold` times (like a scale-to-zero endpoint starting), then answers."""

    def __init__(self, cold):
        self.cold, self.calls = cold, 0
        self.enabled = True

    async def multipart(self, endpoint, route, *, files, data, timeout):
        from llm import WARMUP

        assert WARMUP.get() is True, "window decodes must not count towards the breaker"
        self.calls += 1
        if self.calls <= self.cold:
            raise LlmUnavailable("endpoint starting")
        return {"text": "hello there", "words": [{"word": "hello", "start": 0.0, "end": 0.3},
                                                 {"word": "there", "start": 0.3, "end": 0.6}]}


def test_cold_endpoint_reports_warming_then_ready_and_recovers():
    events = []
    llm = ColdThenWarmLlm(cold=WARMING_AFTER + 2)
    dec = ParakeetDecoder(llm, "sidekick-parakeet", on_status=events.append)
    win = Window(0, 1000, b"\x01\x00" * 16000, final=False)

    async def go():
        out = []
        for _ in range(WARMING_AFTER + 3):
            out.append(await dec.decode(win))
        return out

    results = asyncio.run(go())
    assert all(r == [] for r in results[:-1]) and [w.w for w in results[-1]] == ["hello", "there"]
    assert [e["state"] for e in events] == ["warming", "ready"]
    assert events[0]["stage"] == "stt" and "Warming" in events[0]["message"]


def test_real_llm_client_breaker_stays_closed_for_failed_windows(monkeypatch):
    from config import Settings
    from llm import LlmClient

    llm = LlmClient(Settings(mode="local", llm_enabled=True, llm_failure_threshold=2))

    async def boom(*a, **k):
        raise RuntimeError("cold")

    monkeypatch.setattr(llm, "_post_multipart", boom, raising=False)
    dec = ParakeetDecoder(llm, "sidekick-parakeet")

    async def go():
        for _ in range(6):
            try:
                await dec.decode(Window(0, 1000, b"\x01\x00" * 1600, final=False))
            except Exception:
                pass

    asyncio.run(go())
    assert not llm.breaker_open("sidekick-parakeet")


@pytest.mark.parametrize("bad_stmt", ["ALTER TABLE calls ADD COLUMN IF NOT EXISTS owner TEXT"])
def test_store_start_skips_schema_statements_the_app_role_may_not_run(bad_stmt):
    import asyncpg
    from config import Settings
    from store.lakebase import LakebaseStore

    ran = []

    class Conn:
        async def execute(self, stmt):
            if stmt.startswith("ALTER TABLE"):
                raise asyncpg.exceptions.InsufficientPrivilegeError("must be owner of table calls")
            ran.append(stmt.split("(")[0].strip())

    store = LakebaseStore(Settings(mode="databricks", lakebase_host="h", lakebase_user="u", lakebase_password="p"))
    asyncio.run(store._apply_ddl(Conn()))
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS calls") for s in ran)
    assert not any(s.startswith("ALTER") for s in ran)
