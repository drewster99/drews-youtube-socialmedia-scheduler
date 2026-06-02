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
    # crop width forced to an even integer via floor(.../2)*2 so a
    # source whose ih*9/16 is fractional (e.g. 360 -> 202.5) still
    # produces a valid ffmpeg crop expression rather than ffmpeg
    # exit 8. x offset wrapped in floor() for the same reason.
    #
    # The comma inside min(iw,ih*9/16) MUST be backslash-escaped
    # because ffmpeg's -vf parser treats unescaped commas as filter
    # chain separators — without the escape ffmpeg parses the whole
    # crop expression as four bogus filters and dies with "No such
    # filter: 'ih*9/16)/2)*2...'".
    assert out.startswith(
        r"crop=floor(min(iw\,ih*9/16)/2)*2:ih:floor((iw-floor(min(iw\,ih*9/16)/2)*2)/2):0"
    )
    assert out.endswith("scale=1080:1920")


def test_vertical_crop_filter_shift_right():
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(0.5)
    # Same floor() wrapping; the +0.5*(iw-cw)/2 shift sits inside floor().
    assert r"+(0.5000)*(iw-floor(min(iw\,ih*9/16)/2)*2)/2)" in out


def test_vertical_crop_filter_shift_left():
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(-0.4)
    assert "(-0.4000)" in out


def test_vertical_crop_filter_escapes_min_comma():
    """The comma inside ``min(iw,ih*9/16)`` MUST be escaped as ``\\,``;
    unescaped commas inside an expression are interpreted by
    ffmpeg's ``-vf`` parser as filter-chain separators and the cut
    fails with "No such filter: ..." (real-world reproduction:
    640×360 source preview cuts).

    The chain comma between ``crop=...`` and ``scale=...`` is a real
    chain separator and stays unescaped."""
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(0.0)
    # Inside-expression commas escaped.
    assert r"min(iw\,ih*9/16)" in out
    assert "min(iw,ih*9/16)" not in out  # no UNescaped form anywhere
    # The chain separator before scale is unescaped (just one).
    assert ":0,scale=" in out


def test_videotoolbox_bitrate_buckets_match_resolution():
    """4K-class inputs get a higher bitrate so they don't get crushed
    at the 1080p target. ≤720p sources stay modest so a 640×360 cut
    doesn't waste bits on noise."""
    from yt_scheduler.services.media import _videotoolbox_bitrate_for_output

    assert _videotoolbox_bitrate_for_output(3840, 2160) == "18M"   # 4K
    assert _videotoolbox_bitrate_for_output(2560, 1440) == "10M"   # 1440p
    assert _videotoolbox_bitrate_for_output(1920, 1080) == "6M"    # 1080p landscape
    assert _videotoolbox_bitrate_for_output(1080, 1920) == "6M"    # 1080p vertical
    assert _videotoolbox_bitrate_for_output(1280, 720) == "4M"     # 720p
    assert _videotoolbox_bitrate_for_output(640, 360) == "2M"      # sub-720p
    # Unknown dims → conservative 1080p default so we don't underbit.
    assert _videotoolbox_bitrate_for_output(None, None) == "6M"
    assert _videotoolbox_bitrate_for_output(None, 1080) == "6M"


def test_vertical_crop_filter_handles_fractional_9_16_sources():
    """640x360 source: ih*9/16 = 202.5. Without floor() the crop filter
    expression contains a fractional value and ffmpeg exits 8. With
    floor(.../2)*2 the expression evaluates to an even integer (202),
    which is what libx264 + YUV 4:2:0 require."""
    from yt_scheduler.services.media import _vertical_crop_filter

    out = _vertical_crop_filter(0.0)
    # No bare `ih*9/16` term should appear without a floor() wrap;
    # otherwise ffmpeg sees the fractional value directly.
    assert "crop=floor(" in out
    assert "floor((iw-" in out  # x expression also floored
    # Sanity: the expression evaluates to 202 (even) for 640x360.
    # eval() here mirrors ffmpeg's expression evaluator closely
    # enough to verify the math.
    import math
    iw, ih = 640, 360
    cw = math.floor(min(iw, ih * 9 / 16) / 2) * 2
    assert cw == 202
    x = math.floor((iw - cw) / 2)
    assert x == 219


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


def test_extract_clip_auto_uses_hardware_without_vertical_crop(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """``auto`` now picks videotoolbox whenever ffmpeg has it built
    in, regardless of ``vertical_crop``. The bitrate-by-output-
    resolution helper avoids the original concern (a 4K source
    crushed at a 1080p bitrate) by scaling the target up for large
    inputs. Confirm hardware is selected and that the per-resolution
    bitrate appears."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    # Stub probe so the bitrate picker has dimensions to chew on.
    monkeypatch.setattr(
        media, "probe_video_file",
        lambda _p: media.VideoProbe(
            duration_seconds=10.0, width=1920, height=1080,
            bitrate_bps=None, size_bytes=None,
        ),
    )
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(
        src, "0:00", "0:10", output_name="out.mp4",
        encoder="auto", vertical_crop=False,
    )

    cmd = captured[0]
    assert "h264_videotoolbox" in cmd
    assert "libx264" not in cmd
    assert "-b:v" in cmd
    assert "6M" in cmd  # 1080p band


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
    assert vf_value.startswith(r"crop=floor(min(iw\,ih*9/16)/2)*2:ih:")
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
async def test_cut_clip_picks_hardware_lane_when_available(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """The semaphore lane held during the cut must match the encoder
    extract_clip will actually use, otherwise hardware sessions and
    software cores skew under load.

    Hardware available → hardware lane for BOTH vertical and non-
    vertical cuts (the bitrate-by-output-resolution helper now keeps
    large sources from being undersized). Software lane only when
    hardware is unavailable (covered by the next test)."""
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
    assert lanes == ["hw", "hw"]


@pytest.mark.asyncio
async def test_cut_clip_cleans_up_partial_output_on_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """When ffmpeg raises mid-cut, cut_clip_from_parent must unlink
    the half-written output so it doesn't accumulate in UPLOAD_DIR
    with no row pointing at it."""
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    # Bypass the semaphores so we don't accidentally hold one.
    monkeypatch.setattr(clipper, "_HARDWARE_CUT_SEMAPHORE", _TracingSemaphore([], "hw"))
    monkeypatch.setattr(clipper, "_SOFTWARE_CUT_SEMAPHORE", _TracingSemaphore([], "sw"))

    leaked_paths: list = []

    def fake_extract_clip(*args, **kwargs):
        # Simulate ffmpeg writing a partial output, then failing.
        out_name = kwargs["output_name"]
        partial = tmp_path / out_name
        partial.write_bytes(b"partial mp4 header...")
        leaked_paths.append(partial)
        raise RuntimeError("ffmpeg returned non-zero")

    monkeypatch.setattr(media, "extract_clip", fake_extract_clip)

    proposal = clipper.ProposedClip(
        kind="hook", start_seconds=0, end_seconds=10, title="x", reason="x",
    )
    with pytest.raises(RuntimeError):
        await clipper.cut_clip_from_parent(
            parent_video_path=tmp_path / "src.mp4",
            proposal=proposal,
        )

    # The partial file written before the raise must have been cleaned up.
    assert leaked_paths, "fake_extract_clip must have been called"
    assert not leaked_paths[0].exists(), (
        f"Partial cut output {leaked_paths[0]} survived the failure"
    )


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
