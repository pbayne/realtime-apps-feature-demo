"""Playbook and replay-script validation (SPEC sections 5 and src/app/data/replay)."""

from pathlib import Path

import pytest
from copilot.playbooks import PlaybookRegistry
from enrich.rules import load_rules
from stt.replay import ReplayFrameSource, load_script

APP_ROOT = Path(__file__).resolve().parents[2] / "src" / "app"
PLAYBOOKS = APP_ROOT / "playbooks"
REPLAY = APP_ROOT / "data" / "replay"


@pytest.fixture(scope="module")
def registry():
    return PlaybookRegistry.load(PLAYBOOKS, load_rules(), REPLAY)


def test_two_playbooks_with_enough_kb(registry):
    assert set(registry.names()) == {"advisor_coach", "contact_center"}
    for pb in registry.all():
        assert 6 <= len(pb.kb_docs) <= 10, pb.name
        assert len(pb.methodology) >= 4, pb.name
        assert pb.objections and pb.compliance, pb.name


def test_objection_kb_refs_resolve(registry):
    for pb in registry.all():
        for obj in pb.objections:
            assert pb.doc(obj.kb_ref) is not None, (pb.name, obj.kb_ref)


def test_compliance_keys_exist_in_shared_rules(registry):
    shared = {r["key"] for r in load_rules().raw["risk"]}
    for pb in registry.all():
        for c in pb.compliance:
            assert c.key in shared and c.message, (pb.name, c.key)


def test_each_playbook_has_two_replay_scripts(registry):
    for pb in registry.all():
        assert len(pb.replay_scripts) >= 2, pb.name
    assert "fee_objection" in registry.get("advisor_coach").replay_scripts


@pytest.mark.parametrize("path", sorted(REPLAY.glob("*__*.jsonl")), ids=lambda p: p.name)
def test_replay_script_is_well_formed(path, registry):
    script = load_script(path)
    pb = registry.get(script.playbook)
    assert 120_000 <= script.duration_ms <= 240_000, "scripts should run 2-4 minutes"
    speakers = {u.speaker for u in script.utterances}
    assert speakers == {"agent", "customer"}
    starts = [u.t_start_ms for u in script.utterances]
    assert starts == sorted(starts)
    for u in script.utterances:
        assert u.words and u.words[0].s >= u.t_start_ms and u.words[-1].e == u.t_end_ms
        assert all(a.e <= b.s for a, b in zip(u.words, u.words[1:]))
    assert script.expect.get("cards"), "expected card kinds are declared"
    assert {"objection_pickup", "compliance"} & set(script.expect["cards"])
    assert pb.name == script.playbook


def test_replay_frames_mark_speech_per_channel():
    script = load_script(REPLAY / "advisor_coach__fee_objection.jsonl")
    src = ReplayFrameSource(script, "agent", speed=1000)
    first_agent = script.utterances[0]
    assert first_agent.speaker == "agent"
    assert src._speaking(first_agent.t_start_ms + 10) is True
    assert src._speaking(first_agent.t_start_ms - 500) is False
