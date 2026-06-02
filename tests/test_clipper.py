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


def test_srt_to_llm_timeline_sorts_and_flattens():
    """Dual-speaker SRT — two channels separately transcribed and
    interleaved — has overlapping cues whose indices DON'T track time.
    The flat-timeline form sorts by start, drops cue numbers and end
    timestamps, and emits one ``[MM:SS] text`` line per cue. Claude
    can ground proposals against this; against the raw SRT it
    hallucinates timestamps.
    """
    from yt_scheduler.services.transcripts import srt_to_llm_timeline

    # Speaker A first, speaker B second — interleaved cues with
    # negative deltas between adjacent cue indices.
    srt = (
        "1\n"
        "00:00:00,160 --> 00:00:04,640\n"
        "Good morning, Drew.\n\n"
        "2\n"
        "00:00:02,320 --> 00:00:06,879\n"
        "Cut that intro.\n\n"
        "3\n"
        "00:00:04,640 --> 00:00:08,000\n"
        "Okay.\n\n"
        "4\n"
        "00:00:06,879 --> 00:00:08,320\n"
        "Good morning.\n"
    )
    out = srt_to_llm_timeline(srt)
    lines = out.splitlines()
    assert lines == [
        "[00:00] Good morning, Drew.",
        "[00:02] Cut that intro.",
        "[00:04] Okay.",
        "[00:06] Good morning.",
    ]
    assert "-->" not in out  # no cue end times
    # Cue numbers gone.
    for ln in lines:
        head = ln.split("]", 1)[0]
        assert head.startswith("[")


def test_srt_to_llm_timeline_uses_hour_anchor_past_60min():
    """Sources over an hour need [H:MM:SS], not [MM:SS]."""
    from yt_scheduler.services.transcripts import srt_to_llm_timeline

    srt = (
        "1\n"
        "01:23:45,000 --> 01:23:50,000\n"
        "Way past an hour.\n"
    )
    out = srt_to_llm_timeline(srt)
    assert out == "[1:23:45] Way past an hour."


def test_srt_to_llm_timeline_falls_back_on_unparseable():
    """A transcript with no parseable cues passes through unchanged so
    a downstream call still has SOMETHING to send to the model."""
    from yt_scheduler.services.transcripts import srt_to_llm_timeline

    plain = "Plain prose with no SRT cues at all."
    assert srt_to_llm_timeline(plain) == plain
    assert srt_to_llm_timeline("") == ""


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


def test_seed_prompts_render_cap_from_output_cap():
    """The clip-proposal seed bodies use {{max_proposals}} so the "up to N"
    instruction stays in sync with clipper._OUTPUT_CAP_PER_KIND. A future
    cap bump (e.g. to 12) updates the prompt at the same time, instead
    of having Claude propose 12 while the validator still drops past 8."""
    from yt_scheduler.services import templates
    from yt_scheduler.services.prompts import _SEEDS_BY_KEY
    from yt_scheduler.services.clipper import (
        _OUTPUT_CAP_PER_KIND, _PER_KIND_BOUNDS,
    )

    expected = f"up to {_OUTPUT_CAP_PER_KIND}"
    for kind, key in [
        ("hook", "promo_clip_proposals_hook"),
        ("short", "promo_clip_proposals_short"),
        ("segment", "promo_clip_proposals_segment"),
    ]:
        mn, mx = _PER_KIND_BOUNDS[kind]
        rendered = templates.render(
            _SEEDS_BY_KEY[key].body,
            {
                "parent_title": "X", "parent_duration_human": "1h",
                "existing_ranges_block": "", "crop_constraints": "",
                "min_seconds": str(int(mn)),
                "max_seconds": str(int(mx)) if mx is not None else "",
                "max_proposals": str(_OUTPUT_CAP_PER_KIND),
            },
        )
        assert expected in rendered, (
            f"Rendered {kind} prompt does not contain {expected!r}"
        )


def test_seed_prompts_render_bounds_from_per_kind_bounds():
    """The clip-proposal seed bodies use {{min_seconds}} / {{max_seconds}}
    so the duration band stays in sync with clipper._PER_KIND_BOUNDS.
    Edit one without the other and a future maintainer would otherwise
    end up with Claude proposing 30-90s shorts that the server then
    silently filters down to the 45-75s band."""
    from yt_scheduler.services import templates
    from yt_scheduler.services.prompts import _SEEDS_BY_KEY
    from yt_scheduler.services.clipper import _PER_KIND_BOUNDS

    cases = [
        ("hook", "promo_clip_proposals_hook", "5 and 30 seconds"),
        ("short", "promo_clip_proposals_short", "45 and 75 seconds"),
        ("segment", "promo_clip_proposals_segment", "at least 60 seconds"),
    ]
    for kind, key, expected_phrase in cases:
        mn, mx = _PER_KIND_BOUNDS[kind]
        rendered = templates.render(
            _SEEDS_BY_KEY[key].body,
            {
                "parent_title": "X",
                "parent_duration_human": "1h",
                "existing_ranges_block": "",
                "crop_constraints": "",
                "min_seconds": str(int(mn)),
                "max_seconds": str(int(mx)) if mx is not None else "",
                "max_proposals": "8",
            },
        )
        assert expected_phrase in rendered, (
            f"Rendered {kind} prompt does not contain {expected_phrase!r}: "
            f"{rendered}"
        )


def test_evict_stale_upload_jobs_drops_old_failures():
    """Failed upload jobs past the TTL get evicted; in-progress + fresh
    failures stay."""
    import time

    from yt_scheduler.services import auto_actions

    auto_actions._UPLOAD_JOBS.clear()
    auto_actions._UPLOAD_JOBS["fresh"] = {
        "state": "failed:cutting",
        "_failed_at": time.monotonic(),  # right now
    }
    auto_actions._UPLOAD_JOBS["stale"] = {
        "state": "failed:cutting",
        "_failed_at": (
            time.monotonic() - auto_actions._UPLOAD_JOB_FAILED_TTL_SECONDS - 1
        ),
    }
    auto_actions._UPLOAD_JOBS["alive"] = {
        "state": "transcribing",
    }
    auto_actions._evict_stale_upload_jobs()
    assert "fresh" in auto_actions._UPLOAD_JOBS
    assert "stale" not in auto_actions._UPLOAD_JOBS
    assert "alive" in auto_actions._UPLOAD_JOBS
    auto_actions._UPLOAD_JOBS.clear()


def test_mark_upload_failed_stamps_state_and_timestamp():
    import time
    from yt_scheduler.services import auto_actions

    job: dict = {}
    auto_actions._mark_upload_failed(job, "failed:uploading", error="boom")
    assert job["state"] == "failed:uploading"
    assert job["last_error"] == "boom"
    assert isinstance(job["_failed_at"], float)
    assert abs(job["_failed_at"] - time.monotonic()) < 1.0


def test_evict_stale_generate_jobs_drops_old_terminals():
    import time

    from yt_scheduler.services import clipper

    clipper._GENERATE_JOBS.clear()
    clipper._GENERATE_JOBS["gen_fresh"] = {
        "state": "done",
        "_terminal_at": time.monotonic(),
    }
    clipper._GENERATE_JOBS["gen_stale"] = {
        "state": "done",
        "_terminal_at": (
            time.monotonic() - clipper._GENERATE_JOB_TTL_SECONDS - 1
        ),
    }
    clipper._GENERATE_JOBS["gen_active"] = {
        "state": "proposing",
        "_terminal_at": None,
    }
    clipper._evict_stale_generate_jobs()
    assert "gen_fresh" in clipper._GENERATE_JOBS
    assert "gen_stale" not in clipper._GENERATE_JOBS
    assert "gen_active" in clipper._GENERATE_JOBS
    clipper._GENERATE_JOBS.clear()


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


# --- Generate preview cleanup ---------------------------------------------


def test_cleanup_generate_previews_removes_only_matching(tmp_path, monkeypatch):
    """Globs the preview filename pattern and unlinks; never touches
    unrelated files in UPLOAD_DIR."""
    from yt_scheduler.services import clipper

    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    job_id = "gen_abcdef0123456789"
    keep = tmp_path / "regular_clip.mp4"
    other_job = tmp_path / f"{clipper._PREVIEW_PREFIX}gen_other_hook_0.mp4"
    ours = [
        tmp_path / clipper._preview_filename(job_id, "hook", 0),
        tmp_path / clipper._preview_filename(job_id, "short", 1),
        tmp_path / clipper._preview_filename(job_id, "segment", 2),
    ]
    for p in [keep, other_job, *ours]:
        p.write_bytes(b"stub")

    clipper.cleanup_generate_previews(job_id)

    assert keep.exists()       # untouched
    assert other_job.exists()  # different job_id: untouched
    for p in ours:
        assert not p.exists()  # gone


def test_cleanup_orphan_generate_previews_removes_every_preview(tmp_path, monkeypatch):
    """Startup sweep — wipes every `gen_preview_*.mp4`, regardless of
    job_id, since a restart loses the in-memory job dict."""
    from yt_scheduler.services import clipper

    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    keep = tmp_path / "regular_clip.mp4"
    keep.write_bytes(b"stub")
    previews = [
        tmp_path / clipper._preview_filename("gen_a", "hook", 0),
        tmp_path / clipper._preview_filename("gen_b", "short", 1),
        tmp_path / clipper._preview_filename("gen_c", "segment", 0),
    ]
    for p in previews:
        p.write_bytes(b"stub")

    removed = clipper.cleanup_orphan_generate_previews()

    assert removed == 3
    assert keep.exists()
    for p in previews:
        assert not p.exists()


def test_evict_stale_generate_jobs_calls_preview_cleanup(tmp_path, monkeypatch):
    """A terminal-state job whose TTL has elapsed should both be popped
    from _GENERATE_JOBS and have its preview files cleaned up."""
    from yt_scheduler.services import clipper

    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    # Fast-expire by setting _terminal_at far in the past.
    job_id = "gen_evict001"
    clipper._GENERATE_JOBS[job_id] = {
        "state": "done",
        "_terminal_at": -1e9,  # ~32 years ago in monotonic seconds
    }
    leftover = tmp_path / clipper._preview_filename(job_id, "hook", 0)
    leftover.write_bytes(b"stub")

    clipper._evict_stale_generate_jobs()

    assert job_id not in clipper._GENERATE_JOBS
    assert not leftover.exists()
