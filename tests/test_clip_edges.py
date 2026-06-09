"""Tests for the word-stream clip selection / edge refinement core."""
from yt_scheduler.services.clip_edges import (
    MAX_RAMP_SECONDS,
    MIN_RAMP_SECONDS,
    build_units,
    collapse_repeat_loops,
    compute_edges,
    numbered_units_block,
    resolve_unit_range,
)
from yt_scheduler.services.transcription import TranscriptWord


def W(word: str, start: float, end: float) -> TranscriptWord:
    return TranscriptWord(start=start, end=end, word=word, probability=1.0)


def words_from(spec: list[tuple[str, float, float]]) -> list[TranscriptWord]:
    return [W(*s) for s in spec]


# --- collapse_repeat_loops ---

def test_collapse_removes_runaway_single_word():
    words = [W("uh", i * 0.06, i * 0.06 + 0.06) for i in range(50)]
    out = collapse_repeat_loops(words)
    assert len(out) == 2  # kept first two copies only


def test_collapse_removes_runaway_phrase():
    phrase = ["he", "would", "have", "been"]
    words, t = [], 0.0
    for _ in range(20):
        for token in phrase:
            words.append(W(token, t, t + 0.06))
            t += 0.06
    out = collapse_repeat_loops(words)
    assert len(out) == 8  # two copies of the 4-gram


def test_collapse_leaves_clean_speech_untouched():
    words = words_from([("the", 0, 0.2), ("the", 0.2, 0.4), ("cat", 0.4, 0.8)])
    # a single "the the" stutter is not a >=3x loop
    assert len(collapse_repeat_loops(words)) == 3


# --- build_units ---

def test_unit_breaks_on_sentence_punctuation():
    words = words_from([("Hello.", 0, 0.5), ("World", 0.6, 1.0), ("again.", 1.0, 1.4)])
    units = build_units(words)
    assert [u.text for u in units] == ["Hello.", "World again."]
    assert units[0].index == 1 and units[1].index == 2


def test_unit_breaks_on_pause_gap():
    # a 0.5s gap (> _PAUSE_GAP) splits even without punctuation
    words = words_from([("one", 0, 0.3), ("two", 0.3, 0.6), ("three", 1.2, 1.5)])
    units = build_units(words)
    assert [u.text for u in units] == ["one two", "three"]


def test_unit_hard_cap_splits_continuous_speech():
    # 40 distinct contiguous words, no punctuation, no pauses -> must still split
    words = [W(f"w{i}", i * 0.1, i * 0.1 + 0.1) for i in range(40)]
    units = build_units(words)
    assert len(units) >= 2
    assert all(len(u.words) <= 32 for u in units)


def test_unit_times_track_words():
    words = words_from([("a.", 0.1, 0.4), ("b", 0.5, 0.9)])
    units = build_units(words)
    assert units[0].start == 0.1 and units[0].end == 0.4


# --- numbered_units_block ---

def test_numbered_block_format():
    words = words_from([("Hi.", 0.0, 2.0), ("Bye.", 2.5, 3.0)])
    units = build_units(words)
    block = numbered_units_block(units)
    lines = block.splitlines()
    assert lines[0] == "1\t(2s)\tHi."
    assert lines[1].startswith("2\t(")


# --- resolve_unit_range ---

def _three_units() -> list:
    words = words_from([
        ("One.", 0.0, 0.5),
        ("Two.", 1.0, 1.5),
        ("Three.", 2.0, 2.5),
    ])
    return build_units(words)


def test_resolve_single_and_multi():
    units = _three_units()
    r = resolve_unit_range(units, 1, 1)
    assert r.text == "One." and r.start == 0.0 and r.end == 0.5
    r2 = resolve_unit_range(units, 1, 3)
    assert r2.text == "One. Two. Three." and r2.start == 0.0 and r2.end == 2.5


def test_resolve_rejects_bad_ranges():
    units = _three_units()
    assert resolve_unit_range(units, 0, 2) is None
    assert resolve_unit_range(units, 2, 1) is None
    assert resolve_unit_range(units, 1, 99) is None


# --- compute_edges (gap-ramp) ---

def test_edges_ramp_across_inter_word_gaps():
    units = _three_units()  # gaps of 0.5s between units
    e = compute_edges(units, 2, 2)              # middle unit "Two."
    # head gap = 1.0 - 0.5 = 0.5; tail gap = 2.0 - 1.5 = 0.5
    assert e.fade_in == 0.5 and e.fade_out == 0.5
    assert e.final_start == 0.5                 # prior word end
    assert e.final_end == 2.0                   # next word onset


def test_edges_capped_at_max_ramp():
    words = words_from([("A.", 0.0, 0.5), ("B.", 5.0, 5.5), ("C.", 10.0, 10.5)])
    units = build_units(words)
    e = compute_edges(units, 2, 2)              # huge gaps both sides
    assert e.fade_in == MAX_RAMP_SECONDS and e.fade_out == MAX_RAMP_SECONDS
    assert e.final_start == round(5.0 - MAX_RAMP_SECONDS, 3)
    assert e.final_end == round(5.5 + MAX_RAMP_SECONDS, 3)


def test_edges_first_and_last_unit_use_fallback():
    units = _three_units()
    first = compute_edges(units, 1, 1)          # no prior word
    assert first.fade_in == MIN_RAMP_SECONDS
    assert first.final_start == round(max(0.0, 0.0 - MIN_RAMP_SECONDS), 3)
    last = compute_edges(units, 3, 3)           # no next word
    assert last.fade_out == MIN_RAMP_SECONDS


def test_edges_never_start_negative():
    units = _three_units()
    e = compute_edges(units, 1, 1)
    assert e.final_start >= 0.0
