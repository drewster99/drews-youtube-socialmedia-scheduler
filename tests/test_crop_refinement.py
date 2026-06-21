"""Generate-from-source crop routing (after the Claude-vision retirement).

The Claude-vision crop pass and the single-column ``_vertical_crop_filter`` were
retired — hooks/shorts are recropped to native-resolution 9:16 by the all-Swift
``clipcrop`` engine (YOLO head-tracking stacked/single) at cut time. These cover
the per-kind crop gate (crop-on vs crop-off reaching the cut) and the still-used
keyframe extractor.
"""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_run_generate_job_transcribes_on_device_and_routes_crop_per_kind(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """_run_generate_job transcribes the parent on-device with Apple Speech
    (never large-v3) and routes the per-kind 9:16 toggle to the cut: crop-on
    (hook) gets ``vertical_crop=True``, crop-off (segment) ``False``. Guards the
    per-kind gate against silently regressing to all-False (landscape hooks).
    """
    from yt_scheduler.services import clip_edges, clipper, transcription

    parent_path = tmp_path / "parent.mp4"
    parent_path.write_bytes(b"\x00" * 16)

    transcribe_kwargs: dict = {}

    class _FakeResult:
        has_word_timestamps = True
        all_words = ["w"]
        backend = "macos-speech"

    def fake_transcribe(*, video_path, backend=None, language=None, model=None,
                        progress_callback=None):
        transcribe_kwargs.update(backend=backend, model=model)
        return _FakeResult()

    monkeypatch.setattr(transcription, "transcribe", fake_transcribe)
    monkeypatch.setattr(clip_edges, "build_units", lambda words: ["unit"])

    async def fake_propose(**kw):
        return {
            "hook": [clipper.ProposedClip(
                kind="hook", start_seconds=5, end_seconds=20,
                title="h1", reason="x",
            )],
            "segment": [clipper.ProposedClip(
                kind="segment", start_seconds=120, end_seconds=300,
                title="seg1", reason="x",
            )],
        }

    monkeypatch.setattr(clipper, "propose_all_clips", fake_propose)

    # Stand in for the per-proposal preview cut and record the crop flag each cut
    # was actually asked for. Returns (path, uncertain) like the real one.
    cut_crops: dict[str, bool] = {}

    async def fake_cut(*, job_id, parent_video_path, proposal, idx, **kw):
        cut_crops[proposal.kind] = kw.get("vertical_crop")
        out = tmp_path / f"prev_{proposal.kind}_{idx}.mp4"
        out.write_bytes(b"\x00")
        return out, False

    monkeypatch.setattr(clipper, "cut_preview_for_proposal", fake_cut)

    job_id = await clipper.start_generate_job(
        parent_id="PARENT00001",
        project_id=1,
        parent_video_path=str(parent_path),
        parent_title="Parent",
        parent_duration_seconds=600.0,
        kinds=["hook", "segment"],
        crop_vertical_for_kind={"hook": True, "segment": False},
        existing_ranges_per_kind={},
    )

    import asyncio
    for _ in range(200):
        job = clipper._GENERATE_JOBS.get(job_id)
        if job and job.get("state") in ("done", "failed"):
            break
        await asyncio.sleep(0.01)

    job = clipper._GENERATE_JOBS.get(job_id)
    assert job is not None
    assert job["state"] == "done", job.get("last_error")
    assert transcribe_kwargs.get("backend") == "macos-speech"
    assert transcribe_kwargs.get("model") is None  # never large-v3
    # The per-kind crop toggle reaches the cut: hook 9:16, segment full-frame.
    assert cut_crops == {"hook": True, "segment": False}

    clipper._GENERATE_JOBS.pop(job_id, None)


# --- extract_keyframes_in_range -----------------------------------------

def test_extract_keyframes_in_range_empty_when_inverted_window(tmp_path):
    from yt_scheduler.services.media import extract_keyframes_in_range

    f = tmp_path / "a.mp4"
    f.write_bytes(b"\x00")
    assert extract_keyframes_in_range(f, start_seconds=20, end_seconds=10) == []


def test_extract_keyframes_in_range_empty_when_missing(tmp_path):
    from yt_scheduler.services.media import extract_keyframes_in_range

    assert extract_keyframes_in_range(
        tmp_path / "nope.mp4", start_seconds=0, end_seconds=10,
    ) == []
