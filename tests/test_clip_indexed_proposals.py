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
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert len(accepted) == 1 and rejected == []
    p = accepted[0]
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
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert accepted == []
    assert [r.reason for r in rejected] == [
        clipper.RejectionReason.INDEX_OUT_OF_BOUNDS,
    ] * 3


def test_drops_clip_outside_duration_window():
    units = make_units(9, dur=10.0)
    # hook window is 5-60s; 1..7 spans 7 units (~76s content) -> too long
    raw = [{"first_index": 1, "last_index": 7, "title": "long", "reason": "r", "rating": 2}]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert accepted == []
    assert len(rejected) == 1
    assert rejected[0].reason is clipper.RejectionReason.DURATION_OUT_OF_BAND
    # The reason must be actionable on screen, not just a code.
    assert "over the hook window" in rejected[0].detail
    assert rejected[0].duration_seconds is not None


def test_drops_overlap_with_existing_and_within_batch():
    units = make_units(5, dur=10.0, gap=1.0)
    # unit 2 resolves to ~[10,20]; an existing clip covering it should drop it
    raw = [{"first_index": 2, "last_index": 2, "title": "x", "reason": "r", "rating": 3}]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[(9.0, 21.0)], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert accepted == []
    assert len(rejected) == 1
    assert rejected[0].reason is clipper.RejectionReason.OVERLAPS_EXISTING


def test_max_proposals_cap():
    units = make_units(5, dur=10.0)
    raw = [{"first_index": i, "last_index": i, "title": f"t{i}", "reason": "r", "rating": 3}
           for i in range(1, 5)]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=2,
        parent_duration_seconds=PARENT_DURATION)
    assert len(accepted) == 2
    # The ones that didn't fit are reported, not vanished.
    assert len(rejected) == 2
    assert all(
        r.reason is clipper.RejectionReason.OVER_OUTPUT_CAP for r in rejected
    )


def test_max_proposals_cap_keeps_the_best_rated():
    """Regression: the cap used to truncate in Claude's emission order, so a
    4-star clip listed last lost to a 1-star clip listed first."""
    units = make_units(5, dur=10.0)
    raw = [
        {"first_index": 1, "last_index": 1, "title": "meh", "reason": "r", "rating": 1},
        {"first_index": 2, "last_index": 2, "title": "ok", "reason": "r", "rating": 2},
        {"first_index": 3, "last_index": 3, "title": "best", "reason": "r", "rating": 4},
    ]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=1,
        parent_duration_seconds=PARENT_DURATION)
    assert [p.title for p in accepted] == ["best"]
    assert {r.title for r in rejected} == {"meh", "ok"}


def test_overlapping_pair_resolved_by_rating_not_position():
    """Two overlapping candidates: the higher-rated one wins even when the
    model listed it second."""
    units = make_units(6, dur=10.0, gap=1.0)
    raw = [
        {"first_index": 2, "last_index": 3, "title": "listed first",
         "reason": "r", "rating": 1},
        {"first_index": 2, "last_index": 3, "title": "listed second",
         "reason": "r", "rating": 4},
    ]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert [p.title for p in accepted] == ["listed second"]
    assert rejected[0].reason is clipper.RejectionReason.OVERLAPS_EXISTING


def test_symmetric_overlap_drops_long_proposal_containing_short_existing():
    """Regression: the overlap test used to measure only against the
    PROPOSAL's length, so a ~22s proposal fully containing a 6s existing
    clip slipped through (6s is < 50% of 22s). Measured against the shorter
    clip, full containment is 100% overlap and must drop."""
    units = make_units(5, dur=10.0, gap=1.0)
    # units 2..3 span ~[11, 32] (~21s of content) — inside the 5-60s hook
    # window once gap-ramp edges land; existing (12, 18) sits fully inside.
    raw = [{"first_index": 2, "last_index": 3, "title": "x", "reason": "r", "rating": 3}]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[(12.0, 18.0)], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert accepted == []
    assert rejected[0].reason is clipper.RejectionReason.OVERLAPS_EXISTING


def test_title_duplicate_of_existing_clip_dropped_without_any_range():
    """Regression: imported clips have NULL cut ranges, so they're invisible
    to the range-overlap check — the title is the only duplicate signal.
    This mirrors the real incident: Generate re-proposed 'Claude Nuked My
    Production Database' when an imported 'Claude Nuked My Database'
    already existed."""
    units = make_units(5, dur=10.0, gap=1.0)
    raw = [{"first_index": 2, "last_index": 2,
            "title": "Claude Nuked My Production Database", "reason": "r", "rating": 3}]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION,
        existing_titles=["Claude Nuked My Database"])
    assert accepted == []
    assert rejected[0].reason is clipper.RejectionReason.DUPLICATE_TITLE
    assert "Claude Nuked My Database" in rejected[0].detail


def test_within_batch_title_duplicate_dropped():
    """Two proposals in ONE batch with near-identical titles but
    non-overlapping ranges: the range guard can't link them, so the title
    guard must — it now compares against titles accepted earlier in the same
    batch, not only clips already on the parent."""
    units = make_units(5, dur=10.0, gap=1.0)
    raw = [
        {"first_index": 2, "last_index": 2,
         "title": "Claude Nuked My Database", "reason": "r", "rating": 4},
        {"first_index": 4, "last_index": 4,
         "title": "Claude Nuked My Production Database", "reason": "r", "rating": 4},
    ]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert len(accepted) == 1
    assert accepted[0].title == "Claude Nuked My Database"
    assert rejected[0].reason is clipper.RejectionReason.DUPLICATE_TITLE


def test_prompt_lists_already_covered_titles():
    """Existing same-kind titles are injected into the proposer prompt so the
    model avoids re-covering the same moment in different words — the only
    guard against a semantic repeat the lexical post-filter can't catch."""
    units = make_units(5, dur=10.0, gap=1.0)
    text = clipper._build_index_user_text(
        "hook", units, parent_title="P", max_proposals=8,
        existing_titles=["One Line Nuked All Your Code", "   "])
    assert "Already covered" in text
    assert "One Line Nuked All Your Code" in text
    # Blank / whitespace-only titles are filtered out of the bullet list.
    assert "- One Line Nuked All Your Code" in text
    assert "-    \n" not in text


def test_prompt_omits_already_covered_when_none():
    units = make_units(5, dur=10.0, gap=1.0)
    text = clipper._build_index_user_text(
        "hook", units, parent_title="P", max_proposals=8)
    assert "Already covered" not in text


def test_dissimilar_title_survives_title_guard():
    units = make_units(5, dur=10.0, gap=1.0)
    raw = [{"first_index": 2, "last_index": 2,
            "title": "Goodhart's Law Kills Metrics", "reason": "r", "rating": 3}]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION,
        existing_titles=["Claude Nuked My Database"])
    assert len(accepted) == 1 and rejected == []


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


def test_raw_count_must_account_for_every_proposal():
    """The UI subtracts accepted from raw_count to say "N proposed, M kept".
    A proposal that vanished without a reason would make that lie."""
    import pytest

    with pytest.raises(ValueError, match="discarded without being recorded"):
        clipper.KindProposals(kind="hook", accepted=[], rejected=[], raw_count=3)


def test_error_and_results_are_mutually_exclusive():
    import pytest

    with pytest.raises(ValueError, match="a failed pass produced neither"):
        clipper.KindProposals(
            kind="hook", accepted=[], raw_count=0,
            rejected=[clipper.RejectedProposal(
                kind="hook", reason=clipper.RejectionReason.INVALID_INDICES, detail="x",
            )],
            error="boom",
        )


def test_non_object_entries_are_rejected_not_filtered_away():
    """Claude returning a bare string where an object belongs must show up as
    a rejection, not silently shrink the batch."""
    units = make_units(4, dur=10.0)
    raw = [
        "not an object",
        {"first_index": 1, "last_index": 1, "title": "fine", "reason": "r", "rating": 3},
    ]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=PARENT_DURATION)
    assert [p.title for p in accepted] == ["fine"]
    assert len(rejected) == 1
    assert rejected[0].reason is clipper.RejectionReason.INVALID_INDICES
    assert "not an object" in rejected[0].detail


def test_cap_rejection_never_masks_the_real_reason():
    """Regression: the cap was tested first, so a clip that could never be
    accepted was told it merely ranked too low — and raising the cap would
    not have surfaced it."""
    units = make_units(6, dur=10.0, gap=1.0)
    raw = [
        {"first_index": 2, "last_index": 2, "title": "keeper", "reason": "r", "rating": 4},
        {"first_index": 2, "last_index": 2, "title": "same range", "reason": "r", "rating": 1},
    ]
    accepted, rejected = clipper._validate_indexed_proposals(
        raw, kind="hook", units=units, existing_ranges=[], max_proposals=1,
        parent_duration_seconds=PARENT_DURATION)
    assert [p.title for p in accepted] == ["keeper"]
    assert rejected[0].reason is clipper.RejectionReason.OVERLAPS_EXISTING, (
        "an overlapping clip must be told it overlapped, not that it hit the cap"
    )
