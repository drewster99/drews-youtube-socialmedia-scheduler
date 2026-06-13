"""Tests for the word-stream (index) proposal validator in clipper."""
from yt_scheduler.services import clipper
from yt_scheduler.services.clip_edges import ClipUnit

# Hook duration math in these tests never approaches the segment cap, so any
# parent duration comfortably above the last unit's end works.
PARENT_DURATION = 1000.0


def make_units(n: int, dur: float = 10.0, gap: float = 1.0) -> list[ClipUnit]:
    """n units of ``dur`` seconds each, separated by ``gap`` seconds of silence."""
    units, t = [], 0.0
    for i in range(1, n + 1):
        units.append(ClipUnit(index=i, text=f"unit {i}.", start=t, end=t + dur, words=[]))
        t += dur + gap
    return units


def test_accepts_valid_hook_with_edges_and_rating():
    units = make_units(5, dur=10.0, gap=1.0)  # each unit 10s -> in the 5-30 hook window
    raw = [{"first_index": 2, "last_index": 2, "title": "Hi", "reason": "r", "rating": 4}]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert len(out) == 1
    p = out[0]
    assert p.kind == "hook" and p.title == "Hi" and p.rating == 4
    # gap-ramp edges: cut at prior word end / next word onset, fade across the 1s gaps
    assert p.audio_fade_in > 0 and p.audio_fade_out > 0
    assert p.start_seconds < units[1].start and p.end_seconds > units[1].end


def test_drops_out_of_range_indices():
    units = make_units(3)
    raw = [
        {"first_index": 0, "last_index": 1, "title": "a", "reason": "r", "rating": 1},
        {"first_index": 2, "last_index": 99, "title": "b", "reason": "r", "rating": 1},
        {"first_index": 3, "last_index": 2, "title": "c", "reason": "r", "rating": 1},
    ]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert out == []


def test_drops_clip_outside_duration_window():
    units = make_units(6, dur=10.0)
    # hook window is 5-30s; 1..4 spans 4 units (~43s content) -> too long
    raw = [{"first_index": 1, "last_index": 4, "title": "long", "reason": "r", "rating": 2}]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert out == []


def test_drops_overlap_with_existing_and_within_batch():
    units = make_units(5, dur=10.0, gap=1.0)
    # unit 2 resolves to ~[10,20]; an existing clip covering it should drop it
    raw = [{"first_index": 2, "last_index": 2, "title": "x", "reason": "r", "rating": 3}]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[(9.0, 21.0)], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert out == []


def test_max_proposals_cap():
    units = make_units(5, dur=10.0)
    raw = [{"first_index": i, "last_index": i, "title": f"t{i}", "reason": "r", "rating": 3}
           for i in range(1, 5)]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=2,
        parent_duration_seconds=PARENT_DURATION)
    assert len(out) == 2


def test_symmetric_overlap_drops_long_proposal_containing_short_existing():
    """Regression: the overlap test used to measure only against the
    PROPOSAL's length, so a ~22s proposal fully containing a 6s existing
    clip slipped through (6s is < 50% of 22s). Measured against the shorter
    clip, full containment is 100% overlap and must drop."""
    units = make_units(5, dur=10.0, gap=1.0)
    # units 2..3 span ~[11, 32] (~21s of content) — inside the 5-30s hook
    # window once gap-ramp edges land; existing (12, 18) sits fully inside.
    raw = [{"first_index": 2, "last_index": 3, "title": "x", "reason": "r", "rating": 3}]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[(12.0, 18.0)], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert out == []


def test_title_duplicate_of_existing_clip_dropped_without_any_range():
    """Regression: imported clips have NULL cut ranges, so they're invisible
    to the range-overlap check — the title is the only duplicate signal.
    This mirrors the real incident: Generate re-proposed 'Claude Nuked My
    Production Database' when an imported 'Claude Nuked My Database'
    already existed."""
    units = make_units(5, dur=10.0, gap=1.0)
    raw = [{"first_index": 2, "last_index": 2,
            "title": "Claude Nuked My Production Database", "reason": "r", "rating": 3}]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION,
        existing_titles=["Claude Nuked My Database"])
    assert out == []


def test_dissimilar_title_survives_title_guard():
    units = make_units(5, dur=10.0, gap=1.0)
    raw = [{"first_index": 2, "last_index": 2,
            "title": "Goodhart's Law Kills Metrics", "reason": "r", "rating": 3}]
    out = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION,
        existing_titles=["Claude Nuked My Database"])
    assert len(out) == 1


def test_titles_similar_matrix():
    # Insertion in the middle — the real incident pair.
    assert clipper._titles_similar(
        "Claude Nuked My Production Database", "Claude Nuked My Database")
    # Case / punctuation noise only.
    assert clipper._titles_similar(
        "claude nuked my database!", "Claude Nuked My Database")
    # Genuinely different clips sharing a couple of words must NOT match.
    assert not clipper._titles_similar(
        "One Line Nuked All Your Code", "Goodhart's Law Kills Metrics")
    # Empty titles never match anything.
    assert not clipper._titles_similar("", "Claude Nuked My Database")
