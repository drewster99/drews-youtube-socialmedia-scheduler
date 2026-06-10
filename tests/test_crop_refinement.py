"""3d — Claude-vision crop refinement.

Covers the pure-logic helpers (cautious threshold, classification →
shift mapping, public-dict shape) and an integration of the refinement
step inside ``_run_generate_job`` with a stubbed Claude vision call.
"""

from __future__ import annotations

import pytest


# --- _apply_assessment_shift --------------------------------------------

def test_apply_shift_zero_for_non_offcenter_classifications():
    """``drift`` / ``multi_face`` / ``no_face`` / ``centered`` all fall
    back to center crop regardless of what shift value the model picked."""
    from yt_scheduler.services.clipper import _apply_assessment_shift, CropAssessment

    for cls in ("centered", "drift", "multi_face", "no_face"):
        a = CropAssessment(classification=cls, x_shift_normalized=0.7, confidence=0.9)
        assert _apply_assessment_shift(a) == 0.0


def test_apply_shift_cautious_floor():
    """Below the cautious threshold (~0.15), even ``off_center`` becomes
    a plain center crop. The model's noise floor sometimes returns small
    non-zero shifts that aren't worth applying."""
    from yt_scheduler.services.clipper import (
        _apply_assessment_shift, _MIN_SHIFT_TO_APPLY, CropAssessment,
    )

    just_under = _MIN_SHIFT_TO_APPLY - 0.01
    a = CropAssessment(
        classification="off_center", x_shift_normalized=just_under,
        confidence=0.9,
    )
    assert _apply_assessment_shift(a) == 0.0

    a_neg = CropAssessment(
        classification="off_center", x_shift_normalized=-just_under,
        confidence=0.9,
    )
    assert _apply_assessment_shift(a_neg) == 0.0


def test_apply_shift_passes_through_above_threshold():
    from yt_scheduler.services.clipper import _apply_assessment_shift, CropAssessment

    a = CropAssessment(
        classification="off_center", x_shift_normalized=0.5, confidence=0.9,
    )
    assert _apply_assessment_shift(a) == 0.5


def test_apply_shift_clamps_extreme_values():
    """Vision shouldn't return |shift| > 1 but the tool schema doesn't
    enforce it; defend in depth at the apply step."""
    from yt_scheduler.services.clipper import _apply_assessment_shift, CropAssessment

    a = CropAssessment(
        classification="off_center", x_shift_normalized=2.0, confidence=1.0,
    )
    assert _apply_assessment_shift(a) == 1.0


# --- CropAssessment.uncertain ------------------------------------------

def test_uncertain_classifications():
    from yt_scheduler.services.clipper import CropAssessment

    assert CropAssessment("drift", 0.0, 0.5).uncertain is True
    assert CropAssessment("multi_face", 0.0, 0.5).uncertain is True
    assert CropAssessment("centered", 0.0, 0.5).uncertain is False
    assert CropAssessment("off_center", 0.3, 0.5).uncertain is False
    assert CropAssessment("no_face", 0.0, 0.5).uncertain is False


# --- proposal_to_public_dict --------------------------------------------

def test_public_dict_without_assessment_keeps_zero_shift():
    from yt_scheduler.services.clipper import ProposedClip, proposal_to_public_dict

    p = ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    d = proposal_to_public_dict(p, crop_vertical=False, assessment=None)
    assert d["x_shift_normalized"] == 0.0
    assert d["vertical_crop"] is False
    assert "crop_classification" not in d
    assert "crop_uncertain" not in d


def test_public_dict_with_uncertain_assessment_keeps_zero_shift_but_flags():
    from yt_scheduler.services.clipper import (
        CropAssessment, ProposedClip, proposal_to_public_dict,
    )

    p = ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    a = CropAssessment(classification="drift", x_shift_normalized=0.4, confidence=0.7)
    d = proposal_to_public_dict(p, crop_vertical=True, assessment=a)
    # Even though the model put a number in shift, drift → center.
    assert d["x_shift_normalized"] == 0.0
    assert d["crop_uncertain"] is True
    assert d["crop_classification"] == "drift"


def test_public_dict_marks_vision_crash_as_uncertain():
    """A crashed vision call must surface as 'uncertain' so the user sees a
    badge — silently treating it as a clean center crop would hide failures."""
    from yt_scheduler.services.clipper import ProposedClip, proposal_to_public_dict

    p = ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    d = proposal_to_public_dict(
        p, crop_vertical=True, assessment=None, vision_crashed=True,
    )
    assert d["crop_uncertain"] is True
    assert d["crop_classification"] == "vision_error"
    assert d["x_shift_normalized"] == 0.0


def test_public_dict_with_off_center_above_threshold():
    from yt_scheduler.services.clipper import (
        CropAssessment, ProposedClip, proposal_to_public_dict,
    )

    p = ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    a = CropAssessment(classification="off_center", x_shift_normalized=0.6, confidence=0.8)
    d = proposal_to_public_dict(p, crop_vertical=True, assessment=a)
    assert d["x_shift_normalized"] == 0.6
    assert d["crop_uncertain"] is False


# --- assess_crop_for_proposal: failure modes return neutral ------------

@pytest.mark.asyncio
async def test_run_generate_job_transcribes_on_device_and_skips_vision(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """_run_generate_job transcribes the parent on-device with Apple Speech
    (never large-v3), and — because vertical crop is deferred to a separate
    later step — never fires a vision call. Vision spend is real money.
    """
    from yt_scheduler.services import clip_edges, clipper, transcription

    parent_path = tmp_path / "parent.mp4"
    parent_path.write_bytes(b"\x00" * 16)

    # Capture the transcription request to assert the backend/model.
    transcribe_kwargs: dict = {}

    class _FakeResult:
        has_word_timestamps = True
        all_words = ["w"]
        backend = "macos-speech"

    def fake_transcribe(*, video_path, backend=None, language=None, model=None):
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

    vision_calls: list[str] = []

    async def fake_assess(*, proposal, **kw):
        vision_calls.append(proposal.kind)
        return clipper._NEUTRAL_ASSESSMENT

    monkeypatch.setattr(clipper, "assess_crop_for_proposal", fake_assess)

    # Avoid real ffmpeg: stand in for the per-proposal preview cut.
    async def fake_cut(*, job_id, parent_video_path, proposal, idx, **kw):
        out = tmp_path / f"prev_{proposal.kind}_{idx}.mp4"
        out.write_bytes(b"\x00")
        return out

    monkeypatch.setattr(clipper, "cut_preview_for_proposal", fake_cut)

    # Crop requested on hook, but it must be deferred (no vision fires).
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
    assert vision_calls == []  # crop deferred → no vision spend

    clipper._GENERATE_JOBS.pop(job_id, None)


@pytest.mark.asyncio
async def test_assess_crop_returns_neutral_when_no_frames(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """No keyframes (e.g. ffmpeg failed) → centered/no-shift, not raised.
    Vision is an enhancement; lack of it must never block the pipeline."""
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(
        media, "extract_keyframes_in_range",
        lambda *a, **k: [],
    )

    p = clipper.ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    result = await clipper.assess_crop_for_proposal(
        proposal=p, parent_video_path=tmp_path / "src.mp4", project_id=1,
    )
    assert result.classification == "no_face"
    assert result.x_shift_normalized == 0.0


@pytest.mark.asyncio
async def test_assess_crop_returns_neutral_when_claude_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import ai, clipper, media

    monkeypatch.setattr(
        media, "extract_keyframes_in_range",
        lambda *a, **k: [b"fake_jpeg_bytes"],
    )

    class _ExplodingClient:
        def __init__(self):
            self.messages = self

        def create(self, **kwargs):
            raise RuntimeError("simulated API error")

    monkeypatch.setattr(ai, "get_client", lambda: _ExplodingClient())

    p = clipper.ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    result = await clipper.assess_crop_for_proposal(
        proposal=p, parent_video_path=tmp_path / "src.mp4", project_id=1,
    )
    assert result.classification == "no_face"


@pytest.mark.asyncio
async def test_assess_crop_parses_tool_use_response(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """Happy path: Claude returns a structured tool_use block. We pull
    out the classification, shift, and confidence."""
    from yt_scheduler.services import ai, clipper, media

    monkeypatch.setattr(
        media, "extract_keyframes_in_range",
        lambda *a, **k: [b"fake_jpeg_bytes_1", b"fake_jpeg_bytes_2"],
    )

    class _ToolBlock:
        type = "tool_use"
        name = "assess_crop"
        input = {
            "classification": "off_center",
            "x_shift_normalized": 0.45,
            "confidence": 0.85,
        }

    class _Message:
        content = [_ToolBlock()]

    class _Client:
        def __init__(self):
            self.messages = self

        def create(self, **kwargs):
            return _Message()

    monkeypatch.setattr(ai, "get_client", lambda: _Client())

    p = clipper.ProposedClip(
        kind="hook", start_seconds=10, end_seconds=25,
        title="t", reason="r",
    )
    result = await clipper.assess_crop_for_proposal(
        proposal=p, parent_video_path=tmp_path / "src.mp4", project_id=1,
    )
    assert result.classification == "off_center"
    assert result.x_shift_normalized == 0.45
    assert result.confidence == 0.85


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
