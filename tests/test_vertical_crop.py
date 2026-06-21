"""Clip cutting + encoder routing.

Pure-unit coverage of encoder detection, ``extract_clip``'s (landscape, no-crop)
command construction, the all-Swift ``extract_clip_stacked`` (clipcrop) wrapper,
and the cut-lane selection in ``clipper``. The actual ffmpeg / clipcrop
invocations are mocked — we only verify the command lines + error handling.
(The single-column ``_vertical_crop_filter`` was retired; 9:16 recrop is owned by
clipcrop, exercised via ``extract_clip_stacked``.)
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


# --- videotoolbox bitrate buckets ---------------------------------------

def test_videotoolbox_bitrate_buckets_match_resolution():
    """4K-class inputs get a higher bitrate so they don't get crushed at the
    1080p target. ≤720p sources stay modest."""
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


# --- extract_clip command construction (landscape, no crop) -------------

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
    # -vf is always present: setpts rebases the first frame to t=0 (fixes the
    # black-first-frame edit-list offset). No crop filter — extract_clip is
    # landscape-only now.
    assert "-vf" in cmd
    assert cmd[cmd.index("-vf") + 1] == "setpts=PTS-STARTPTS"


def test_extract_clip_auto_uses_hardware(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """``auto`` picks videotoolbox whenever ffmpeg has it built in. The
    bitrate-by-output-resolution helper keeps a 4K source from being crushed at
    a 1080p bitrate."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
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
    media.extract_clip(src, "0:00", "0:10", output_name="out.mp4", encoder="auto")

    cmd = captured[0]
    assert "h264_videotoolbox" in cmd
    assert "libx264" not in cmd
    assert "-b:v" in cmd
    assert "6M" in cmd  # 1080p band


def test_extract_clip_uses_hardware_decode_and_fast_seek(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """``-hwaccel auto`` + input-side ``-ss`` (before ``-i``) turn minute-long 4K
    cuts into second-long ones. ``precise=True`` keeps ``-accurate_seek`` (the
    default) for a sample-accurate cut; ``precise=False`` adds ``-noaccurate_seek``."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")

    media.extract_clip(
        src, "00:10:00", "00:10:30", output_name="precise.mp4",
        encoder="software", precise=True,
    )
    cmd = captured[-1]
    assert cmd[cmd.index("-hwaccel") + 1] == "auto"
    assert cmd.index("-ss") < cmd.index("-i"), f"-ss must precede -i; got {cmd!r}"
    assert "-noaccurate_seek" not in cmd

    media.extract_clip(
        src, "00:10:00", "00:10:30", output_name="fast.mp4",
        encoder="software", precise=False,
    )
    cmd = captured[-1]
    assert "-noaccurate_seek" in cmd
    assert cmd.index("-ss") < cmd.index("-i")


def test_extract_clip_software_when_auto_and_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())
    captured = _capture_cmd(monkeypatch, media)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    media.extract_clip(src, "0:00", "0:10", output_name="out.mp4", encoder="auto")

    cmd = captured[0]
    assert "libx264" in cmd
    assert "h264_videotoolbox" not in cmd


def test_extract_clip_hardware_force_when_unavailable_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    with pytest.raises(RuntimeError, match="not available"):
        media.extract_clip(src, "0:00", "0:10", output_name="out.mp4", encoder="hardware")


# --- extract_clip_stacked (the clipcrop wrapper) ------------------------

def _fake_clipcrop(monkeypatch, tmp_path):
    """Point _resolve_clipcrop at a fake binary + model that exist, and return a
    dict that records the clipcrop argv (the recorder writes the --out file so
    the atomic rename succeeds)."""
    from yt_scheduler.services import media

    binary = tmp_path / "clipcrop"
    binary.write_bytes(b"\x00")
    model = tmp_path / "yolo.mlmodelc"
    model.mkdir()
    monkeypatch.setenv("DYS_CLIPCROP_BIN", str(binary))
    monkeypatch.setenv("DYS_CLIPCROP_MODEL", str(model))
    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    return binary, model


def test_extract_clip_stacked_builds_clipcrop_argv(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    binary, model = _fake_clipcrop(monkeypatch, tmp_path)
    captured: dict[str, list[str]] = {}

    class _R:
        returncode = 0
        stderr = b""

    def fake_run(cmd, **kw):
        captured["cmd"] = cmd
        Path(cmd[cmd.index("--clipcrop") + 1]).write_bytes(b"\x00")  # the .cutpart temp
        return _R()

    monkeypatch.setattr(media.subprocess, "run", fake_run)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    out, uncertain = media.extract_clip_stacked(
        src, 10.0, 25.0, output_name="o.mp4", fade_in=0.12, fade_out=0.3,
    )

    cmd = captured["cmd"]
    assert cmd[0] == str(binary)
    assert cmd[cmd.index("--start") + 1] == "10.0000"
    assert cmd[cmd.index("--end") + 1] == "25.0000"
    assert cmd[cmd.index("--fade-in") + 1] == "0.1200"
    assert cmd[cmd.index("--fade-out") + 1] == "0.3000"
    assert cmd[cmd.index("--model") + 1] == str(model)
    assert "--min-height" in cmd
    assert uncertain is False
    assert (tmp_path / "o.mp4").exists()


def test_extract_clip_stacked_flags_uncertain_from_stderr(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    _fake_clipcrop(monkeypatch, tmp_path)

    class _R:
        returncode = 0
        stderr = b"CROPPABILITY=low present=0.10\n"

    def fake_run(cmd, **kw):
        Path(cmd[cmd.index("--clipcrop") + 1]).write_bytes(b"\x00")
        return _R()

    monkeypatch.setattr(media.subprocess, "run", fake_run)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    _out, uncertain = media.extract_clip_stacked(src, 10.0, 25.0, output_name="o.mp4")
    assert uncertain is True


def test_extract_clip_stacked_reraises_clipcrop_error_with_stderr_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    _fake_clipcrop(monkeypatch, tmp_path)

    def fake_run(cmd, **kw):
        raise subprocess.CalledProcessError(
            1, cmd, output=b"", stderr=b"some spam\nERROR: bad range [25,10]\n",
        )

    monkeypatch.setattr(media.subprocess, "run", fake_run)

    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    with pytest.raises(RuntimeError, match=r"clipcrop exit 1:.*bad range"):
        media.extract_clip_stacked(src, 10.0, 25.0, output_name="o.mp4")
    # No partial/temp left behind.
    assert not list(tmp_path.glob(".cutpart_*"))


def test_extract_clip_stacked_raises_when_binary_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setenv("DYS_CLIPCROP_BIN", str(tmp_path / "nope"))
    monkeypatch.setenv("DYS_CLIPCROP_MODEL", str(tmp_path / "nope.mlmodelc"))
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    with pytest.raises(RuntimeError, match="clipcrop binary not found"):
        media.extract_clip_stacked(src, 10.0, 25.0, output_name="o.mp4")


# --- cut-lane selection in clipper --------------------------------------

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
async def test_cut_lanes_crop_on_hardware_crop_off_follows_encoder(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """Crop-on (vertical) cuts go through clipcrop, which owns its own hardware
    encode, so they ALWAYS take the hardware lane. Crop-off (landscape) cuts
    follow ffmpeg's encoder: hardware when videotoolbox is available."""
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))

    lanes: list[str] = []
    monkeypatch.setattr(clipper, "_HARDWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "hw"))
    monkeypatch.setattr(clipper, "_SOFTWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "sw"))
    monkeypatch.setattr(media, "extract_clip", lambda *a, **k: tmp_path / "fake.mp4")
    monkeypatch.setattr(
        media, "extract_clip_stacked", lambda *a, **k: (tmp_path / "fake.mp4", False),
    )

    proposal = clipper.ProposedClip(
        kind="hook", start_seconds=0, end_seconds=10, title="x", reason="x",
    )
    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4", proposal=proposal, vertical_crop=True,
    )
    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4", proposal=proposal, vertical_crop=False,
    )
    assert lanes == ["hw", "hw"]


@pytest.mark.asyncio
async def test_cut_lanes_crop_off_software_when_hardware_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """No videotoolbox: crop-on still takes the hardware lane (clipcrop), crop-off
    falls to the software lane (libx264)."""
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())

    lanes: list[str] = []
    monkeypatch.setattr(clipper, "_HARDWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "hw"))
    monkeypatch.setattr(clipper, "_SOFTWARE_CUT_SEMAPHORE", _TracingSemaphore(lanes, "sw"))
    monkeypatch.setattr(media, "extract_clip", lambda *a, **k: tmp_path / "fake.mp4")
    monkeypatch.setattr(
        media, "extract_clip_stacked", lambda *a, **k: (tmp_path / "fake.mp4", False),
    )

    proposal = clipper.ProposedClip(
        kind="hook", start_seconds=0, end_seconds=10, title="x", reason="x",
    )
    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4", proposal=proposal, vertical_crop=True,
    )
    await clipper.cut_clip_from_parent(
        parent_video_path=tmp_path / "src.mp4", proposal=proposal, vertical_crop=False,
    )
    assert lanes == ["hw", "sw"]


@pytest.mark.asyncio
async def test_cut_clip_cleans_up_partial_output_on_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """When the cut raises mid-way, cut_clip_from_parent must unlink the
    half-written output so it doesn't accumulate in UPLOAD_DIR."""
    from yt_scheduler.services import clipper, media

    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(clipper, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())
    monkeypatch.setattr(clipper, "_HARDWARE_CUT_SEMAPHORE", _TracingSemaphore([], "hw"))
    monkeypatch.setattr(clipper, "_SOFTWARE_CUT_SEMAPHORE", _TracingSemaphore([], "sw"))

    leaked_paths: list = []

    def fake_extract_clip(*args, **kwargs):
        out_name = kwargs["output_name"]
        partial = tmp_path / out_name
        partial.write_bytes(b"partial mp4 header...")
        leaked_paths.append(partial)
        raise RuntimeError("ffmpeg returned non-zero")

    monkeypatch.setattr(media, "extract_clip", fake_extract_clip)

    proposal = clipper.ProposedClip(
        kind="segment", start_seconds=0, end_seconds=10, title="x", reason="x",
    )
    with pytest.raises(RuntimeError):
        await clipper.cut_clip_from_parent(
            parent_video_path=tmp_path / "src.mp4", proposal=proposal,
        )

    assert leaked_paths, "fake_extract_clip must have been called"
    assert not leaked_paths[0].exists(), (
        f"Partial cut output {leaked_paths[0]} survived the failure"
    )
