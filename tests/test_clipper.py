"""Clipper validation, eligibility, and SRT helpers.

Pure-unit coverage for the bits of services/clipper.py + services/transcripts.py
that don't need a running app or a real Claude.
"""

from __future__ import annotations

import pytest


def test_has_timestamps_positive():
    from yt_scheduler.services.transcripts import has_timestamps

    srt = "1\n00:00:00,000 --> 00:00:05,123\nHello world\n\n"
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

    srt = "1\n01:23:45,000 --> 01:23:50,000\nWay past an hour.\n"
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


async def test_start_generate_job_stores_normalised_max_per_kind(monkeypatch):
    """start_generate_job must clamp + default the per-kind cap before
    stashing it on the job dict — the background task reads it back
    without re-validating."""
    from yt_scheduler.services import clipper

    async def _noop(_job_id):
        return None

    monkeypatch.setattr(clipper, "_run_generate_job", _noop)
    monkeypatch.setattr(clipper, "spawn_background", lambda coro, name=None: None)

    job_id = await clipper.start_generate_job(
        parent_id="vid_test",
        project_id=1,
        parent_video_path="/tmp/x.mp4",
        parent_title="Parent",
        parent_duration_seconds=600.0,
        kinds=["hook", "short", "segment"],
        crop_vertical_for_kind={"hook": True, "short": True, "segment": False},
        existing_ranges_per_kind={"hook": [], "short": [], "segment": []},
        max_per_kind={"hook": 3, "short": 9999, "segment": -5},
    )
    job = clipper._GENERATE_JOBS[job_id]
    try:
        assert job["max_per_kind"]["hook"] == 3
        assert job["max_per_kind"]["short"] == clipper.MAX_PROPOSALS_PER_KIND_CAP
        # Negative → per-kind default fallback (segment = 6).
        assert job["max_per_kind"]["segment"] == clipper._DEFAULT_MAX_PER_KIND["segment"]
    finally:
        clipper._GENERATE_JOBS.pop(job_id, None)


def test_seed_prompts_render_cap_from_output_cap():
    """The clip-proposal seed bodies use {{max_proposals}} so the "up to N"
    instruction stays in sync with clipper._OUTPUT_CAP_PER_KIND. A future
    cap bump (e.g. to 12) updates the prompt at the same time, instead
    of having Claude propose 12 while the validator still drops past 8."""
    from yt_scheduler.services import templates
    from yt_scheduler.services.prompts import _SEEDS_BY_KEY
    from yt_scheduler.services.clipper import (
        _OUTPUT_CAP_PER_KIND,
        _PER_KIND_BOUNDS,
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
                "parent_title": "X",
                "parent_duration_human": "1h",
                "parent_transcript": "0.0s hello world",
                "existing_ranges_block": "",
                "crop_constraints": "",
                "min_seconds": str(int(mn)),
                "max_seconds": str(int(mx)) if mx is not None else "",
                "max_proposals": str(_OUTPUT_CAP_PER_KIND),
            },
        )
        assert expected in rendered, f"Rendered {kind} prompt does not contain {expected!r}"


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
                "parent_transcript": "0.0s hello world",
                "existing_ranges_block": "",
                "crop_constraints": "",
                "min_seconds": str(int(mn)),
                "max_seconds": str(int(mx)) if mx is not None else "",
                "max_proposals": "8",
            },
        )
        assert expected_phrase in rendered, (
            f"Rendered {kind} prompt does not contain {expected_phrase!r}: {rendered}"
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
        "_failed_at": (time.monotonic() - auto_actions._UPLOAD_JOB_FAILED_TTL_SECONDS - 1),
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
        "_terminal_at": (time.monotonic() - clipper._GENERATE_JOB_TTL_SECONDS - 1),
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
        units=[],
        parent_title="t",
        parent_duration_seconds=120.0,
        existing_ranges_per_kind={},
        project_id=1,
    )
    assert out == {}


@pytest.mark.asyncio
async def test_propose_all_clips_dispatches_each_kind(monkeypatch: pytest.MonkeyPatch):
    """propose_all_clips fans out one index call per requested kind via the
    gather wiring — there is no anchor-text path."""
    from yt_scheduler.services import clipper

    calls: list[str] = []

    async def fake_propose(*, kind, **kw):
        calls.append(kind)
        return []

    monkeypatch.setattr(clipper, "propose_clips_for_kind_indexed", fake_propose)
    await clipper.propose_all_clips(
        kinds=["hook", "short", "segment"],
        units=[],
        parent_title="t",
        parent_duration_seconds=600.0,
        existing_ranges_per_kind={},
        project_id=1,
    )
    assert set(calls) == {"hook", "short", "segment"}


@pytest.mark.asyncio
async def test_index_over_requests_when_existing_then_caps_output(
    monkeypatch: pytest.MonkeyPatch,
):
    """When the kind already has cut clips, Claude is asked for base+3
    candidates (no timestamps to 'avoid'); the output is still capped at the
    base max after post-LLM dedup/overlap removal."""
    from yt_scheduler.services import ai, clip_edges, clipper

    units = [
        clip_edges.ClipUnit(index=i + 1, text=f"unit {i}", start=i * 10.0, end=i * 10 + 8.0, words=[])
        for i in range(30)
    ]
    captured: dict = {}
    props = [
        {"first_index": i + 1, "last_index": i + 1, "start_echo": "", "end_echo": "",
         "title": f"t{i}", "reason": "r", "rating": 4}
        for i in range(9)
    ]

    class _Block:
        type = "tool_use"
        name = "propose_clips"
        input = {"proposals": props}

    class _Msg:
        content = [_Block()]

    def fake_create(**kw):
        captured["user_text"] = kw["messages"][0]["content"]
        return _Msg()

    class _Client:
        class messages:
            create = staticmethod(fake_create)

    monkeypatch.setattr(ai, "get_client", lambda: _Client())

    async def _model():
        return "claude-x"

    monkeypatch.setattr(ai, "_resolve_model", _model)

    out = await clipper.propose_clips_for_kind_indexed(
        kind="hook", units=units, parent_title="P",
        parent_duration_seconds=600.0,
        existing_ranges=[(1000.0, 1010.0)],  # non-empty → over-request
        max_proposals=6,
    )
    assert "UP TO 9" in captured["user_text"]  # 6 + 3 bonus
    assert len(out) == 6  # capped at the base max


@pytest.mark.asyncio
async def test_run_generate_job_fails_loudly_without_transcriber(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """No fallback: if the on-device transcriber raises, the job fails with a
    surfaced error rather than dropping to another backend."""
    import asyncio

    from yt_scheduler.services import clipper, transcription

    parent = tmp_path / "p.mp4"
    parent.write_bytes(b"\x00")

    def boom(**kw):
        raise RuntimeError("no speech backend")

    monkeypatch.setattr(transcription, "transcribe", boom)

    job_id = await clipper.start_generate_job(
        parent_id="PFAIL00001", project_id=1, parent_video_path=str(parent),
        parent_title="P", parent_duration_seconds=600.0, kinds=["hook"],
        crop_vertical_for_kind={"hook": False}, existing_ranges_per_kind={},
    )
    for _ in range(200):
        job = clipper._GENERATE_JOBS.get(job_id)
        if job and job.get("state") in ("done", "failed"):
            break
        await asyncio.sleep(0.01)
    job = clipper._GENERATE_JOBS.get(job_id)
    assert job is not None
    assert job["state"] == "failed"
    assert "transcription failed" in (job.get("last_error") or "").lower()
    clipper._GENERATE_JOBS.pop(job_id, None)


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

    assert keep.exists()  # untouched
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
