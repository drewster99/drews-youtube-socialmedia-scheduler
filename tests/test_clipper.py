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

    # Hook needs 60 (max) + 15 (headroom) = 75 s parent.
    assert is_parent_eligible_for_kind(75.0, "hook") is True
    assert is_parent_eligible_for_kind(74.0, "hook") is False

    # Short needs 180 + 15 = 195 s parent.
    assert is_parent_eligible_for_kind(195.0, "short") is True
    assert is_parent_eligible_for_kind(194.0, "short") is False

    # Segment's ceiling is 75% of the parent, so the binding constraint is
    # the FRACTION clearing the 180 s floor (parent >= 240), not the floor
    # plus headroom (195). A 200 s parent clears the headroom rule and would
    # still produce nothing: its longest legal segment is 150 s.
    assert is_parent_eligible_for_kind(240.0, "segment") is True
    assert is_parent_eligible_for_kind(239.0, "segment") is False
    assert is_parent_eligible_for_kind(200.0, "segment") is False


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


def test_editorial_block_is_spliced_between_the_code_sections():
    """The proposal system prompt is code + editable editorial + code.

    The editable middle must land BETWEEN the input-format section and the
    tool contract. Those two are exactly what a prompt edit must never be
    able to break, so their presence on either side of the block is the
    invariant worth pinning.
    """
    from yt_scheduler.services.clipper import _build_index_system_text

    system = _build_index_system_text(
        "hook", "## What makes a good hook\n- Be brief and surprising.",
    )
    assert "## Input format" in system
    assert "## Output format" in system
    assert "- Be brief and surprising." in system
    assert (
        system.index("## Input format")
        < system.index("- Be brief and surprising.")
        < system.index("## Output format")
    )


def test_system_prompt_holds_no_run_specific_data():
    """Byte-identical across calls for a kind — that is what keeps the run's
    material (parent title, transcript, counts) in the user turn and makes
    the system block cacheable."""
    from yt_scheduler.services.clipper import _build_index_system_text

    assert _build_index_system_text("short", "E") == _build_index_system_text("short", "E")


def test_every_clip_kind_maps_to_an_existing_editorial_seed():
    """Each kind's editorial prose is a real, non-empty prompt key.

    A typo here would only surface as a KeyError partway through a generate
    run, after transcription has already been paid for.
    """
    from yt_scheduler.services.clipper import (
        CLIP_EDITORIAL_PROMPT_KEYS,
        _PER_KIND_BOUNDS,
    )
    from yt_scheduler.services.prompts import _SEEDS_BY_KEY

    assert set(CLIP_EDITORIAL_PROMPT_KEYS) == set(_PER_KIND_BOUNDS)
    for kind, key in CLIP_EDITORIAL_PROMPT_KEYS.items():
        assert key in _SEEDS_BY_KEY, f"{kind} points at missing prompt key {key!r}"
        assert _SEEDS_BY_KEY[key].body.strip(), f"{key} seed body is blank"


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
    """When the kind already has cut clips, Claude is asked for
    base + _EXISTING_OVERREQUEST_BONUS candidates (no timestamps to
    'avoid'); the output is still capped at the
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

    # The editorial block is a DB read; this test is about the over-request
    # arithmetic, so stub it rather than stand up a prompt_templates row.
    async def _editorial(kind, *, project_id):
        return "## What makes a good hook\n- stub"

    monkeypatch.setattr(clipper, "editorial_block_for_kind", _editorial)

    out = await clipper.propose_clips_for_kind_indexed(
        kind="hook", units=units, parent_title="P",
        parent_duration_seconds=600.0,
        existing_ranges=[(1000.0, 1010.0)],  # non-empty → over-request
        project_id=1,
        max_proposals=6,
    )
    # Derived, not hardcoded: the bonus is a tuning knob and this test is
    # about the over-request happening at all, not about its size.
    assert f"UP TO {6 + clipper._EXISTING_OVERREQUEST_BONUS}" in captured["user_text"]
    assert len(out.accepted) == 6  # capped at the base max
    # The 3 that didn't fit are reported, not silently dropped.
    assert len(out.rejected) == 3
    assert out.raw_count == 9


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


@pytest.mark.asyncio
async def test_truncated_tool_response_fails_loudly(monkeypatch: pytest.MonkeyPatch):
    """A tool_use call cut off at max_tokens arrives as an ordinary 200 with no
    usable block. Reporting that as "no proposals" would be a statement about
    the model that is actually about a truncated response — the exact lie the
    KindProposals return type exists to prevent."""
    from yt_scheduler.services import ai, clip_edges, clipper

    units = [clip_edges.ClipUnit(index=i + 1, text=f"u{i}", start=i * 10.0,
                                 end=i * 10 + 8.0, words=[]) for i in range(30)]

    class _Msg:
        stop_reason = "max_tokens"
        content = []          # truncated before any tool_use block landed

    monkeypatch.setattr(ai, "get_client", lambda: type(
        "C", (), {"messages": type("M", (), {"create": staticmethod(lambda **kw: _Msg())})}
    )())

    async def _model():
        return "claude-x"

    monkeypatch.setattr(ai, "_resolve_model", _model)

    async def _editorial(kind, *, project_id):
        return "## editorial"

    monkeypatch.setattr(clipper, "editorial_block_for_kind", _editorial)

    out = await clipper.propose_clips_for_kind_indexed(
        kind="hook", units=units, parent_title="P", parent_duration_seconds=600.0,
        existing_ranges=[], project_id=1, max_proposals=5,
    )
    assert out.accepted == [] and out.rejected == []
    assert out.error is not None
    assert "max_tokens" in out.error and "propose_clips" in out.error


@pytest.mark.asyncio
async def test_one_kind_failing_does_not_take_down_the_others(
    monkeypatch: pytest.MonkeyPatch,
):
    """A raised exception in one kind's pass is captured for that kind only.
    Without return_exceptions the siblings would also be lost — and worse,
    they'd keep running uncancelled, billing tokens nobody reads."""
    from yt_scheduler.services import clipper

    async def flaky(*, kind, **kw):
        if kind == "hook":
            raise RuntimeError("anthropic key missing")
        return clipper.KindProposals(kind=kind, accepted=[], rejected=[], raw_count=0)

    monkeypatch.setattr(clipper, "propose_clips_for_kind_indexed", flaky)

    out = await clipper.propose_all_clips(
        kinds=["hook", "short", "segment"], units=[], parent_title="P",
        parent_duration_seconds=600.0, existing_ranges_per_kind={}, project_id=1,
    )
    assert set(out) == {"hook", "short", "segment"}
    assert out["hook"].error is not None
    assert "anthropic key missing" in out["hook"].error
    assert out["short"].error is None and out["segment"].error is None


def test_generate_job_payload_omits_private_fields():
    """Deny-by-default: the job dict holds an absolute filesystem path the
    browser must never see, and a new field is dropped unless whitelisted."""
    from yt_scheduler.services import clipper

    job = {
        "job_id": "gen_x", "parent_id": "p", "project_id": 1, "state": "done",
        "last_error": None, "kinds": ["hook"], "crop_vertical": {}, "proposals": {},
        "progress_message": "", "rejected": {}, "raw_counts": {}, "kind_errors": {},
        "parent_video_path": "/Users/someone/secret/master.mov",
        "existing_titles_per_kind": {}, "cuts_total": 3,
    }
    public = clipper._public_job(job) if hasattr(clipper, "_public_job") else None
    if public is None:
        clipper._GENERATE_JOBS["gen_x"] = job
        try:
            public = clipper.get_generate_job("gen_x")
        finally:
            clipper._GENERATE_JOBS.pop("gen_x", None)
    assert "parent_video_path" not in public
    assert "cuts_total" not in public
    for key in ("rejected", "raw_counts", "kind_errors"):
        assert key in public, f"{key} must reach the browser"
