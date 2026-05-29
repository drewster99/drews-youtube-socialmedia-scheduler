"""3c — vertical crop + hardware encoder routing.

Pure-unit coverage of the filter graph, encoder detection plumbing, and
extract_clip's command construction. The actual ffmpeg invocation is
mocked out — we only verify that the right command lines get built.
"""

from __future__ import annotations

import pytest


# --- _vertical_crop_filter ----------------------------------------------

def test_vertical_crop_filter_center():
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(0.0)
    assert out.startswith("crop=min(iw,ih*9/16):ih:(iw-min(iw,ih*9/16))/2:0")
    assert out.endswith("scale=1080:1920")


def test_vertical_crop_filter_shift_right():
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(0.5)
    # Center expression with +0.5*(iw-cw)/2 added on.
    assert "(iw-min(iw,ih*9/16))/2+(0.5000)*(iw-min(iw,ih*9/16))/2" in out


def test_vertical_crop_filter_shift_left():
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(-0.4)
    assert "(-0.4000)" in out


def test_vertical_crop_filter_clamps_out_of_range():
    """Values outside [-1, 1] are clamped so the crop never leaves the frame."""
    from yt_scheduler.services.media import _vertical_crop_filter

    out_pos = _vertical_crop_filter(5.0)
    assert "(1.0000)" in out_pos
    out_neg = _vertical_crop_filter(-5.0)
    assert "(-1.0000)" in out_neg


# --- hardware encoder detection -----------------------------------------

def test_hardware_encoder_detection_pulls_videotoolbox_names(
    monkeypatch: pytest.MonkeyPatch,
):
    from yt_scheduler.services import media

    fake_stdout = (
        " V....D h264_videotoolbox     VideoToolbox H.264 Encoder\n"
        " V....D hevc_videotoolbox     VideoToolbox HEVC Encoder\n"
        " V....D libx264               libx264 H.264 / AVC / MPEG-4 AVC\n"
        " A....D aac                   AAC (Advanced Audio Coding)\n"
    )

    class _R:
        returncode = 0
        stdout = fake_stdout

    monkeypatch.setattr(media.subprocess, "run", lambda *a, **k: _R())
    detected = media._detect_hardware_encoders()
    assert detected == frozenset({"h264_videotoolbox", "hevc_videotoolbox"})


def test_hardware_encoder_detection_returns_empty_when_ffmpeg_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    from yt_scheduler.services import media

    def fake_run(*args, **kwargs):
        raise FileNotFoundError("ffmpeg")

    monkeypatch.setattr(media.subprocess, "run", fake_run)
    assert media._detect_hardware_encoders() == frozenset()


def test_hardware_encoder_available_helper(monkeypatch: pytest.MonkeyPatch):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    assert media.hardware_encoder_available("h264") is True
    assert media.hardware_encoder_available("hevc") is False


# --- extract_clip command construction ---------------------------------

def _capture_cmd(monkeypatch, media_module):
    """Replace subprocess.run with a recorder; return the list it writes to."""
    captured: list[list[str]] = []

    class _R:
        returncode = 0

    def fake_run(cmd, **kw):
        captured.append(cmd)
        return _R()

    monkeypatch.setattr(media_module.subprocess, "run", fake_run)
    return captured


def test_extract_clip_software_no_crop(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(src, "0:00", "0:10", output_name="out.mp4", encoder="software")

    cmd = captured[0]
    assert "libx264" in cmd
    assert "h264_videotoolbox" not in cmd
    assert "-vf" not in cmd


def test_extract_clip_hardware_for_vertical_crop_when_auto(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """``auto`` picks hardware only when ``vertical_crop=True`` — that's the
    case where output is always scaled to 1080×1920 and the fixed-bitrate
    hardware path is appropriate. For non-vertical, libx264 + CRF keeps
    quality consistent across source resolutions."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(
        src, "0:00", "0:10", output_name="out.mp4",
        encoder="auto", vertical_crop=True,
    )

    cmd = captured[0]
    assert "h264_videotoolbox" in cmd
    # Hardware needs an explicit bitrate, not CRF.
    assert "-b:v" in cmd
    assert "libx264" not in cmd


def test_extract_clip_auto_stays_software_without_vertical_crop(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """Even with hardware available, ``auto`` defers to libx264 when no
    crop is requested, because the hardware path's fixed bitrate would
    butcher a 4K-source horizontal cut."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(
        src, "0:00", "0:10", output_name="out.mp4",
        encoder="auto", vertical_crop=False,
    )

    cmd = captured[0]
    assert "libx264" in cmd
    assert "h264_videotoolbox" not in cmd


def test_extract_clip_software_when_auto_and_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(
        src, "0:00", "0:10", output_name="out.mp4",
        encoder="auto", vertical_crop=True,
    )

    cmd = captured[0]
    assert "libx264" in cmd
    assert "h264_videotoolbox" not in cmd


def test_extract_clip_vertical_crop_filter_included(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(
        src, "0:00", "0:10",
        output_name="out.mp4",
        vertical_crop=True,
        x_shift_normalized=0.0,
        encoder="software",
    )
    cmd = captured[0]
    assert "-vf" in cmd
    vf_idx = cmd.index("-vf")
    vf_value = cmd[vf_idx + 1]
    assert vf_value.startswith("crop=min(iw,ih*9/16):ih:")
    assert vf_value.endswith("scale=1080:1920")


def test_extract_clip_hardware_force_when_unavailable_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    with pytest.raises(RuntimeError, match="not available"):
        media.extract_clip(
            src, "0:00", "0:10",
            output_name="out.mp4",
            encoder="hardware",
        )


# --- clipper.cut_clip_from_parent uses the right semaphore -------------

class _TracingSemaphore:
    """Stand-in for ``asyncio.Semaphore`` that records each acquire."""

    def __init__(self, log: list[str], name: str) -> None:
        self._log = log
        self._name = name

    async def __aenter__(self):
        self._log.append(self._name)
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_cut_clip_picks_lane_matching_extract_clip_choice(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """The semaphore lane held during the cut must match the encoder
    extract_clip will actually use, otherwise hardware sessions and
    software cores skew under load.

    vertical_crop + hardware available → hardware lane.
    Non-vertical OR no hardware → software lane.
    """
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))

    lanes: list[str] = []
    monkeypatch.setattr(clipper, "_HARDWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "hw"))
    monkeypatch.setattr(clipper, "_SOFTWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "sw"))
    monkeypatch.setattr(media, "extract_clip", lambda *a, **k: tmp_path / "fake.mp4")

    proposal = clipper.ProposedClip(
        kind="hook", start_seconds=0, end_seconds=10, title="x", reason="x",
    )

    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4",
        proposal=proposal, vertical_crop=True,
    )
    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4",
        proposal=proposal, vertical_crop=False,
    )
    assert lanes == ["hw", "sw"]


@pytest.mark.asyncio
async def test_cut_clip_uses_software_lane_when_hardware_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """Even with vertical_crop=True, if videotoolbox isn't available the
    cut acquires the software lane and extract_clip falls back to libx264.
    """
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())

    lanes: list[str] = []
    monkeypatch.setattr(clipper, "_HARDWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "hw"))
    monkeypatch.setattr(clipper, "_SOFTWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "sw"))
    monkeypatch.setattr(media, "extract_clip", lambda *a, **k: tmp_path / "fake.mp4")

    proposal = clipper.ProposedClip(
        kind="hook", start_seconds=0, end_seconds=10, title="x", reason="x",
    )
    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4",
        proposal=proposal, vertical_crop=True,
    )
    assert lanes == ["sw"]
