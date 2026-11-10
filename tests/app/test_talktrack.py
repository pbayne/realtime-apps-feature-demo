"""Talk-track matching (request C): the lexical matcher that decides whether the agent's spoken line covered a
suggestion card well enough to auto-accept it."""

from __future__ import annotations

from sidekick.talktrack import coverage_score, matches_talk_track


def test_matches_when_agent_says_the_suggestion():
    # Card advises asking about retirement timeline; the agent asks almost exactly that.
    assert matches_talk_track(
        "So, when are you thinking about retiring — what's your target timeline?",
        title="Ask about retirement timeline", body="Find out the client's target retirement date.")


def test_no_match_when_agent_talks_about_something_else():
    assert not matches_talk_track(
        "The weather has been terrible this week, hasn't it?",
        title="Ask about retirement timeline", body="Find out the client's target retirement date.")


def test_requires_min_overlap_so_one_generic_word_does_not_trip():
    # Shares only "fees" — a single content word must not auto-accept.
    assert not matches_talk_track(
        "fees",
        title="Explain the advisory fee schedule and breakpoints", body="Walk through tiered fees.")


def test_threshold_is_configurable():
    said = "Let me confirm your risk tolerance before we proceed."
    # High bar against a longer title that is only partly covered -> no match.
    assert not matches_talk_track(said, title="Confirm risk tolerance and investment time horizon",
                                  body="", threshold=0.95)
    # The fully-covered short title matches at the default bar.
    assert matches_talk_track(said, title="Confirm risk tolerance", body="", threshold=0.6)


def test_coverage_score_is_fraction_of_suggestion_content_words():
    # "retirement timeline" both covered -> 1.0
    assert coverage_score("what is your retirement timeline", "retirement timeline") == 1.0
    # none covered -> 0.0
    assert coverage_score("hello there", "retirement timeline") == 0.0
    # half covered
    assert 0.0 < coverage_score("tell me the timeline", "retirement timeline") < 1.0


def test_empty_inputs_never_match():
    assert not matches_talk_track("", title="Ask about fees", body="")
    assert not matches_talk_track("anything", title="", body="")
    assert coverage_score("", "x") == 0.0
    assert coverage_score("x", "") == 0.0
