"""Tests for the word-stream clip selection / edge refinement core."""
from yt_scheduler.services.clip_edges import (
    ClipUnit,
    EDGE_FADE_SECONDS,
    HEAD_PAD_SECONDS,
    QUANTUM_CEIL_SECONDS,
    QUANTUM_FLOOR_SECONDS,
    TAIL_MAX_SECONDS,
    TAIL_MIN_SECONDS,
    build_units,
    collapse_repeat_loops,
    compute_edges,
    detect_quantum,
    numbered_units_block,
    resolve_unit_range,
    snap_clip_end_to_pause,
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


# --- detect_quantum ---

def test_detect_quantum_apple_grid():
    # Smallest word duration is one 60ms Apple cell -> quantum 0.06.
    words = words_from([("Good", 0.06, 0.12), ("morning.", 0.12, 0.30)])
    units = build_units(words)
    assert detect_quantum(units) == 0.06


def test_detect_quantum_clamps_and_falls_back():
    # Sparse transcript (long words) is clamped at the ceiling, not over-detected.
    words = words_from([("Hello.", 0.0, 0.5), ("World.", 0.6, 1.2)])
    assert detect_quantum(build_units(words)) == QUANTUM_CEIL_SECONDS
    # No word timings (cue-only units) -> the floor.
    from yt_scheduler.services.clip_edges import ClipUnit
    bare = [ClipUnit(index=1, text="x", start=0.0, end=1.0, words=[])]
    assert detect_quantum(bare) == QUANTUM_FLOOR_SECONDS


# --- compute_edges: HEAD pad/quantum, TAIL min/max pause clamp, fixed fade ---

def test_head_capped_at_pad():
    units = build_units(words_from([("A.", 0.0, 0.5), ("B.", 5.0, 5.5), ("C.", 10.0, 10.5)]))
    e = compute_edges(units, 2, 2)              # huge head gap (4.5s)
    assert e.final_start == round(5.0 - HEAD_PAD_SECONDS, 3)   # capped at the head pad


def test_tail_takes_full_pause_between_min_and_max():
    # sentence-split with a 0.40s gap (between TAIL_MIN 0.30 and TAIL_MAX 0.50)
    # -> the full pause is used, neither floored nor capped.
    units = build_units(words_from([("One.", 0.0, 0.5), ("Two.", 0.9, 1.4)]))
    e = compute_edges(units, 1, 1)
    assert e.final_end == round(0.5 + 0.40, 3)


def test_tail_capped_at_max_on_big_pause():
    units = build_units(words_from([("A.", 0.0, 0.5), ("B.", 1.5, 2.0)]))  # 1.0s gap
    e = compute_edges(units, 1, 1)
    assert e.final_end == round(0.5 + TAIL_MAX_SECONDS, 3)


def test_tail_floored_at_min_when_contiguous():
    # contiguous tail (gap 0) -> floored at TAIL_MIN, bleeding into the next word
    units = build_units(words_from([("Hello.", 0.06, 0.12), ("World.", 0.12, 0.30)]))
    e = compute_edges(units, 1, 1)
    assert e.final_end == round(0.12 + TAIL_MIN_SECONDS, 3)


def test_tail_at_transcript_end_takes_max():
    units = _three_units()
    last = compute_edges(units, 3, 3)           # no next word -> unlimited pause
    assert last.final_end == round(2.5 + TAIL_MAX_SECONDS, 3)


def test_cut_never_starts_negative():
    e = compute_edges(_three_units(), 1, 1)
    assert e.final_start >= 0.0


def test_fade_is_a_fixed_cosine_length_decoupled_from_the_room():
    # The fade is always EDGE_FADE_SECONDS regardless of how big the room is.
    big = compute_edges(_three_units(), 2, 2)
    tight = compute_edges(
        build_units(words_from([("Hello.", 0.06, 0.12), ("World.", 0.12, 0.30)])), 1, 1)
    assert big.fade_in == big.fade_out == EDGE_FADE_SECONDS
    assert tight.fade_in == tight.fade_out == EDGE_FADE_SECONDS


# --- snap_clip_end_to_pause (a) ---

def _u(i: int, start: float, end: float) -> ClipUnit:
    return ClipUnit(index=i, text=f"u{i}", start=start, end=end, words=[])


def test_snap_end_moves_to_next_real_pause():
    # units 1,2 end contiguous (0.06 gaps); unit 3 has a real 0.5s pause after it.
    units = [_u(1, 0.0, 1.0), _u(2, 1.06, 2.0), _u(3, 2.06, 3.0), _u(4, 3.5, 4.0)]
    assert snap_clip_end_to_pause(units, 1) == 3


def test_snap_end_already_on_pause_stays():
    units = [_u(1, 0.0, 1.0), _u(2, 1.5, 2.0)]   # gap after 1 = 0.5 (a pause)
    assert snap_clip_end_to_pause(units, 1) == 1


def test_snap_end_no_pause_within_window_stays():
    # every gap is 0.06 (contiguous) -> nothing to snap to within the window
    units = [_u(i, i * 1.0, i * 1.0 + 0.94) for i in range(1, 20)]
    assert snap_clip_end_to_pause(units, 1) == 1


# --- timing_grid_warning ---

def test_timing_grid_warning_none_on_apple_grid():
    from yt_scheduler.services.clip_edges import timing_grid_warning
    assert timing_grid_warning(0.06) is None
    assert timing_grid_warning(0.05999999999994543) is None   # float residual still OK


def test_timing_grid_warning_flags_unexpected_grid():
    from yt_scheduler.services.clip_edges import timing_grid_warning
    w = timing_grid_warning(0.04)
    assert w is not None and w["code"] == "unexpected_timing_grid"
    assert "40ms" in w["message"] and "60ms" in w["message"]
