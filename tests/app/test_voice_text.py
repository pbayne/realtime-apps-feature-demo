"""Pure voice helpers: sentence chunking, speakable text, the <<ITEMS>> splitter and item parsing (SPEC 9.4)."""

from voice.items import DELIM, ReplySplitter, parse_items
from voice.sentences import SentenceChunker, speakable, split_sentences


def test_sentences_split_on_end_punctuation_followed_by_space():
    assert split_sentences("Hello there. How are you? Fine!") == ["Hello there.", "How are you?", "Fine!"]


def test_decimals_and_currency_do_not_split():
    assert split_sentences("The fee is 1.25 percent of assets.") == ["The fee is 1.25 percent of assets."]


def test_chunker_emits_only_complete_sentences_while_streaming():
    c = SentenceChunker()
    out = []
    for token in ["Sure", ". We", " can", " talk", " fees.", " Next"]:
        out += c.feed(token)
    assert out == ["Sure.", "We can talk fees."]   # complete once the space after it arrives
    assert c.flush() == ["Next"]


def test_first_piece_may_end_at_a_clause_once_long_enough():
    c = SentenceChunker()
    assert c.feed("Thanks for calling today, let me look at that") == ["Thanks for calling today,"]
    # later pieces only split at sentence ends
    assert c.feed(" and the fee is one percent, which is common") == []


def test_short_first_piece_waits_for_a_sentence_end():
    c = SentenceChunker()
    assert c.feed("Okay, sure") == []
    assert c.feed(". Next.") == ["Okay, sure.", ]


def test_speakable_removes_markdown_and_emoji():
    assert speakable("**Great** news :) \U0001F389 - `fee`") == "Great news :) - fee"
    assert speakable("  - a bullet\n- two ") == "a bullet two"


def test_splitter_releases_text_before_delimiter_and_collects_items():
    s = ReplySplitter()
    spoken = s.feed("Sure thing. ") + s.feed("Done.\n") + s.feed(DELIM + "\n") + s.feed('[{"kind": "note",')
    spoken += s.feed(' "text": "x"}]')
    assert spoken.strip() == "Sure thing. Done."
    assert s.in_items and s.items_raw.strip().startswith('[{"kind": "note"')
    assert s.finish() == ""


def test_delimiter_split_across_tokens_is_held_back():
    s = ReplySplitter()
    out = []
    for token in ["Hi.", " <", "<", "ITEM", "S>>", "[]"]:
        out.append(s.feed(token))
    assert "".join(out).strip() == "Hi."
    assert s.in_items and s.items_raw == "[]"


def test_false_alarm_prefix_is_released_at_the_end():
    s = ReplySplitter()
    spoken = s.feed("Price is <") + s.feed(" ok") + s.finish()
    assert spoken == "Price is < ok"


def test_items_are_parsed_tolerantly():
    raw = ('[{"kind": "action_item", "text": "Send schedule", "owner": "advisor"},'
           ' {"kind": "weird", "text": "fallback kind"}, {"kind": "risk"}, "junk", {"text": "  "}]')
    items = parse_items(raw, turn=4, session_id="s", now_ms=1)
    assert [(i["kind"], i["text"]) for i in items] == [("action_item", "Send schedule"), ("note", "fallback kind")]
    assert items[0]["owner"] == "advisor" and items[1]["owner"] is None
    assert items[0]["turn"] == 4 and items[0]["session_id"] == "s" and items[0]["item_id"]


def test_malformed_and_truncated_items_give_nothing_and_never_raise():
    assert parse_items("", turn=1, session_id="s", now_ms=1) == []
    assert parse_items("not json at all", turn=1, session_id="s", now_ms=1) == []
    assert parse_items('[{"kind": "note", "text": "cut off', turn=1, session_id="s", now_ms=1) == []
    assert parse_items("[1, 2, 3]", turn=1, session_id="s", now_ms=1) == []
    assert parse_items('{"items": [{"kind": "summary", "text": "ok"}]}', turn=1, session_id="s", now_ms=1)[0]["kind"] == "summary"


def test_item_list_is_capped():
    raw = "[" + ",".join('{"kind": "note", "text": "n%d"}' % i for i in range(30)) + "]"
    assert len(parse_items(raw, turn=1, session_id="s", now_ms=1)) == 10
