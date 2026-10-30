from stt.agreement import LocalAgreement2, norm
from stt.base import Word


def w(text, s, e):
    return Word(w=text, s=s, e=e)


def hyp(*words):
    return list(words)


def test_identical_hypotheses_commit_all_but_hold_word():
    la = LocalAgreement2(hold_words=1)
    h = hyp(w("I", 0, 100), w("want", 100, 300), w("retirement", 300, 700))
    assert [x.w for x in la.update(h)] == []  # first hypothesis: nothing agreed yet
    newly = la.update(h)
    assert [x.w for x in newly] == ["I", "want"]  # last word held back
    assert la.text() == "I want retirement"     # tail still shown in partials


def test_growing_hypothesis_commits_only_agreed_prefix():
    la = LocalAgreement2(hold_words=1)
    la.update(hyp(w("the", 0, 100), w("fee", 100, 300)))
    # 'the' and 'fee' agree across the two hypotheses; only the current last word ('is') is held back.
    newly = la.update(hyp(w("the", 0, 100), w("fee", 100, 300), w("is", 300, 400)))
    assert [x.w for x in newly] == ["the", "fee"]
    assert la.text() == "the fee is"


def test_disagreeing_word_is_not_committed():
    la = LocalAgreement2(hold_words=0)
    la.update(hyp(w("five", 0, 200), w("percent", 200, 500)))
    newly = la.update(hyp(w("nine", 0, 200), w("percent", 200, 500)))
    assert [x.w for x in newly] == []  # 'five' vs 'nine' disagree: nothing committed at position 0


def test_normalisation_ignores_case_and_punctuation():
    la = LocalAgreement2(hold_words=0)
    la.update(hyp(w("Fees,", 0, 200)))
    newly = la.update(hyp(w("fees", 0, 200), w("apply", 200, 400)))
    assert [x.w for x in newly] == ["fees"]


def test_overlapping_window_does_not_recommit_old_words():
    la = LocalAgreement2(hold_words=0)
    la.update(hyp(w("a", 0, 200), w("b", 200, 400)))
    la.update(hyp(w("a", 0, 200), w("b", 200, 400)))
    assert [x.w for x in la.committed] == ["a", "b"]
    # Next window starts before 'b' again; the time filter drops words that start before the committed end.
    newly = la.update(hyp(w("a", 0, 200), w("b", 200, 400), w("c", 400, 600)))
    assert [x.w for x in newly] == []
    newly = la.update(hyp(w("b", 200, 400), w("c", 400, 600)))
    assert [x.w for x in newly] == ["c"]
    assert [x.w for x in la.committed] == ["a", "b", "c"]


def test_flush_commits_everything_in_final_hypothesis():
    la = LocalAgreement2(hold_words=1)
    la.update(hyp(w("thanks", 0, 300), w("bye", 300, 500)))
    la.update(hyp(w("thanks", 0, 300), w("bye", 300, 500)))
    final = la.flush(hyp(w("thanks", 0, 300), w("bye", 300, 500)))
    assert [x.w for x in final] == ["bye"]
    assert la.text() == "thanks bye"


def test_boundary_guard_drops_repeated_committed_run_after_timestamp_drift():
    la = LocalAgreement2(hold_words=0)
    la.update(hyp(w("what", 0, 200), w("are", 200, 400), w("the", 400, 600)))
    la.update(hyp(w("what", 0, 200), w("are", 200, 400), w("the", 400, 600)))
    assert la.text() == "what are the"
    # Same words re-decoded with drifted timestamps just after the committed boundary.
    flushed = la.flush(hyp(w("what", 650, 800), w("are", 800, 950), w("the", 950, 1100), w("fees", 1100, 1300)))
    assert [x.w for x in flushed] == ["fees"]
    assert la.text() == "what are the fees"


def test_genuine_repeated_word_survives_boundary_guard():
    la = LocalAgreement2(hold_words=0)
    la.update(hyp(w("in", 0, 200), w("four", 250, 500)))
    la.update(hyp(w("in", 0, 200), w("four", 250, 500)))
    flushed = la.flush(hyp(w("in", 0, 200), w("four", 250, 500), w("four", 550, 800), w("two", 850, 1000)))
    assert [x.w for x in flushed] == ["four", "two"]
    assert la.text() == "in four four two"


def test_real_parakeet_rolling_windows_commit_each_word_once():
    """Recorded from the deployed Parakeet endpoint: 12 s windows every 400 ms, where the same word's
    timestamps drift 160-400 ms between decodes. Time-based dedupe produced "I a first meeting with a a
    couple who are are worried"; text alignment must reproduce the utterance exactly once."""
    import json
    from pathlib import Path

    hyps = json.loads((Path(__file__).parent / "fixtures" / "parakeet_hyps_fee_question.json").read_text())
    la = LocalAgreement2(hold_words=1)
    for h in hyps[:-1]:
        la.update([Word(w, s, e) for w, s, e in h["words"]])
    la.flush([Word(w, s, e) for w, s, e in hyps[-1]["words"]])
    got = [norm(w.w) for w in la.committed]
    want = [norm(w) for w in ("I have a first meeting with a couple who are worried about fees What should I "
                              "ask them and what should I follow up on afterwards").split()]
    assert got == want
