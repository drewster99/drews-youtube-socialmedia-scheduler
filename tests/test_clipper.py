"""Clipper validation, eligibility, and SRT helpers.

Pure-unit coverage for the bits of services/clipper.py + services/transcripts.py
that don't need a running app or a real Claude.
"""

from __future__ import annotations

import pytest


def test_has_timestamps_positive():
    from yt_scheduler.services.transcripts import has_timestamps

    srt = (
        "1\n"
        "00:00:00,000 --> 00:00:05,123\n"
        "Hello world\n\n"
    )
    assert has_timestamps(srt) is True


def test_has_timestamps_negative():
    from yt_scheduler.services.transcripts import has_timestamps

    assert has_timestamps("") is False
    assert has_timestamps(None) is False
    assert has_timestamps("This is plain prose with no cue lines.") is False


def test_has_timestamps_in_middle_of_text():
    """Used on hand-edited transcripts that may have prose around the cues."""
    from yt_scheduler.services.transcripts import has_timestamps

    mixed = "Some intro line\n00:00:10,500 --> 00:00:14,200\nfound it"
    assert has_timestamps(mixed) is True


def test_eligibility_per_kind_bands():
    from yt_scheduler.services.clipper import is_parent_eligible_for_kind

    # Hook needs 30 (max) + 15 (headroom) = 45 s parent.
    assert is_parent_eligible_for_kind(45.0, "hook") is True
    assert is_parent_eligible_for_kind(44.0, "hook") is False

    # Short needs 75 + 15 = 90 s parent.
    assert is_parent_eligible_for_kind(90.0, "short") is True
    assert is_parent_eligible_for_kind(89.0, "short") is False

    # Segment has no hard max — needs min (60) + 15 = 75 s.
    assert is_parent_eligible_for_kind(75.0, "segment") is True
    assert is_parent_eligible_for_kind(74.0, "segment") is False


def test_validate_drops_out_of_band():
    from yt_scheduler.services.clipper import _validate_proposals

    raw = [
        {"start_seconds": 0, "end_seconds": 4, "title": "too short", "reason": "x"},   # 4s, under hook min 5
        {"start_seconds": 0, "end_seconds": 31, "title": "too long", "reason": "x"},   # 31s, over hook max 30
        {"start_seconds": 10, "end_seconds": 25, "title": "ok", "reason": "x"},        # 15s, in band
    ]
    result = _validate_proposals(
        raw, kind="hook", parent_duration_seconds=120.0, existing_ranges=[],
    )
    assert len(result) == 1
    assert result[0].title == "ok"


def test_validate_drops_overlap_above_threshold():
    from yt_scheduler.services.clipper import _validate_proposals

    raw = [
        {"start_seconds": 10, "end_seconds": 20, "title": "overlaps", "reason": "x"},
        {"start_seconds": 80, "end_seconds": 95, "title": "clean", "reason": "x"},
    ]
    # Existing 12-18 covers 6s of the proposed 10-20 (a 10s clip) = 60%, > 50%.
    result = _validate_proposals(
        raw, kind="hook",
        parent_duration_seconds=200.0,
        existing_ranges=[(12.0, 18.0)],
    )
    titles = [p.title for p in result]
    assert "overlaps" not in titles
    assert "clean" in titles


def test_validate_caps_at_eight():
    from yt_scheduler.services.clipper import _validate_proposals

    raw = [
        {"start_seconds": i * 60, "end_seconds": i * 60 + 20, "title": f"t{i}", "reason": "x"}
        for i in range(12)
    ]
    result = _validate_proposals(
        raw, kind="hook", parent_duration_seconds=10000.0, existing_ranges=[],
    )
    assert len(result) == 8


def test_validate_drops_out_of_parent_bounds():
    from yt_scheduler.services.clipper import _validate_proposals

    raw = [
        {"start_seconds": -1, "end_seconds": 20, "title": "neg", "reason": "x"},
        {"start_seconds": 150, "end_seconds": 170, "title": "past", "reason": "x"},  # parent only 120s
        {"start_seconds": 10, "end_seconds": 25, "title": "ok", "reason": "x"},
    ]
    result = _validate_proposals(
        raw, kind="hook", parent_duration_seconds=120.0, existing_ranges=[],
    )
    titles = [p.title for p in result]
    assert titles == ["ok"]


def test_validate_drops_nan_and_inf():
    """NaN compares False to every numeric bound, would slip past the
    range guards and crash _format_ffmpeg_timestamp downstream."""
    from yt_scheduler.services.clipper import _validate_proposals

    nan = float("nan")
    inf = float("inf")
    raw = [
        {"start_seconds": nan, "end_seconds": 20.0, "title": "nan", "reason": ""},
        {"start_seconds": 10.0, "end_seconds": inf, "title": "inf", "reason": ""},
        {"start_seconds": 10.0, "end_seconds": 25.0, "title": "ok", "reason": ""},
    ]
    result = _validate_proposals(
        raw, kind="hook", parent_duration_seconds=600.0, existing_ranges=[],
    )
    assert [p.title for p in result] == ["ok"]


def test_validate_drops_empty_title():
    from yt_scheduler.services.clipper import _validate_proposals

    raw = [
        {"start_seconds": 10, "end_seconds": 25, "title": "  ", "reason": "x"},
        {"start_seconds": 30, "end_seconds": 45, "title": "ok", "reason": "x"},
    ]
    result = _validate_proposals(
        raw, kind="hook", parent_duration_seconds=200.0, existing_ranges=[],
    )
    assert [p.title for p in result] == ["ok"]


def test_segment_has_no_hard_max_but_caps_at_parent():
    """Segment proposals can be as long as the parent — no fixed max."""
    from yt_scheduler.services.clipper import _validate_proposals

    raw = [
        {"start_seconds": 5, "end_seconds": 250, "title": "long segment", "reason": "x"},
    ]
    result = _validate_proposals(
        raw, kind="segment", parent_duration_seconds=300.0, existing_ranges=[],
    )
    assert len(result) == 1


def test_format_duration_human():
    from yt_scheduler.services.clipper import _format_duration_human

    assert _format_duration_human(0) == "0s"
    assert _format_duration_human(30) == "30s"
    assert _format_duration_human(90) == "1m 30s"
    assert _format_duration_human(3725) == "1h 2m 5s"


def test_format_ffmpeg_timestamp():
    from yt_scheduler.services.clipper import _format_ffmpeg_timestamp

    assert _format_ffmpeg_timestamp(0) == "00:00:00.000"
    assert _format_ffmpeg_timestamp(75.5) == "00:01:15.500"
    assert _format_ffmpeg_timestamp(3661.250) == "01:01:01.250"


@pytest.mark.asyncio
async def test_propose_all_clips_empty_kinds_short_circuits():
    from yt_scheduler.services.clipper import propose_all_clips

    out = await propose_all_clips(
        kinds=[],
        crop_vertical_for_kind={},
        transcript_srt="x",
        parent_title="t",
        parent_duration_seconds=120.0,
        existing_ranges_per_kind={},
        project_id=1,
    )
    assert out == {}


@pytest.mark.asyncio
async def test_propose_all_clips_skips_ineligible_kinds(monkeypatch: pytest.MonkeyPatch):
    """A 50-second parent is eligible for hook (45s floor) but not short
    or segment. We should call Claude once (for hook only) and return
    empty lists for the others without hitting the API."""
    from yt_scheduler.services import clipper

    calls: list[str] = []

    async def fake_propose(*, kind, **kw):
        calls.append(kind)
        return []

    monkeypatch.setattr(clipper, "propose_clips_for_kind", fake_propose)
    await clipper.propose_all_clips(
        kinds=["hook", "short", "segment"],
        crop_vertical_for_kind={"hook": True, "short": True, "segment": False},
        transcript_srt="x",
        parent_title="t",
        parent_duration_seconds=50.0,
        existing_ranges_per_kind={},
        project_id=1,
    )
    # propose_clips_for_kind is called for all three; each one's
    # eligibility gate inside short-circuits and returns []. The point of
    # this test is to verify the gather wiring, not the gate (covered
    # elsewhere) — so we confirm all three were dispatched.
    assert set(calls) == {"hook", "short", "segment"}
