"""Encoder-lane selection under a byte-size budget.

videotoolbox controls output size only through an explicit ``-b:v`` bitrate
target and overshoots it, so against a marginal byte cap it can produce a file
that fails the post-encode size verification. Because encoder selection is
deterministic, re-running the same hardware path fails identically every time.
``transcode_for_platform`` therefore retries the overshoot once on libx264
(software), whose rate control meets a byte budget reliably — and, when both
lanes overshoot, fails loudly rather than shipping an oversized file.

These tests mock the ffmpeg invocation: the fake ``subprocess.run`` writes a
controllable number of real bytes to the output path so the *real*
``_verify_output_within_limits`` size check runs against a genuine file size.
"""

from __future__ import annotations

from pathlib import Path

import pytest


# --- pure decision helper -----------------------------------------------

def test_should_retry_on_software_decision_table():
    from yt_scheduler.services import media

    # Hardware lane overshot a real cap -> switch to libx264.
    assert media._should_retry_on_software(
        used_hardware=True, max_bytes=1_000, measured_size_bytes=1_001,
    ) is True
    # Exactly at the cap is not over it.
    assert media._should_retry_on_software(
        used_hardware=True, max_bytes=1_000, measured_size_bytes=1_000,
    ) is False
    # Hardware met the budget -> nothing to repair.
    assert media._should_retry_on_software(
        used_hardware=True, max_bytes=1_000, measured_size_bytes=500,
    ) is False
    # Already on libx264: re-running the same path would loop; fail loudly.
    assert media._should_retry_on_software(
        used_hardware=False, max_bytes=1_000, measured_size_bytes=5_000,
    ) is False
    # No byte cap at all: a size overshoot is not the failure mode.
    assert media._should_retry_on_software(
        used_hardware=True, max_bytes=None, measured_size_bytes=5_000,
    ) is False


# --- transcode_for_platform lane routing --------------------------------

def _source_probe(media):
    """640x360 h264, 1 s, 500 kbps source — small enough that no byte ceiling
    binds for the caps used here, so the encode starts on hardware."""
    return media.VideoProbe(
        duration_seconds=1.0, width=640, height=360,
        bitrate_bps=500_000, size_bytes=1_000_000,
        codec_name="h264", frame_rate=30.0,
    )


def _install_fake_encode(monkeypatch, media, *, sizes: dict[str, int]):
    """Replace subprocess.run with a recorder that writes ``sizes[encoder]``
    bytes to the ffmpeg output path. Returns the list of captured argv lists."""
    calls: list[list[str]] = []

    class _R:
        returncode = 0
        stdout = b""
        stderr = b""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        encoder = "h264_videotoolbox" if "h264_videotoolbox" in cmd else "libx264"
        Path(cmd[-1]).write_bytes(b"\x00" * sizes[encoder])
        return _R()

    monkeypatch.setattr(media.subprocess, "run", fake_run)
    # The output probe: valid dims/codec/duration so the ONLY thing that can
    # fail verification is the measured byte size.
    monkeypatch.setattr(
        media, "probe_video_file",
        lambda _p: media.VideoProbe(
            duration_seconds=1.0, width=640, height=360,
            bitrate_bps=500_000, size_bytes=0,
            codec_name="h264", frame_rate=30.0,
        ),
    )
    return calls


def test_hardware_overshoot_retries_on_software(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """A videotoolbox encode that overshoots the byte cap is re-encoded on
    libx264, which meets it — and the result is returned."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    calls = _install_fake_encode(
        monkeypatch, media, sizes={"h264_videotoolbox": 150_000, "libx264": 90_000},
    )

    limits = media.PlatformMediaLimits(max_bytes=100_000)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    out = tmp_path / "out.mp4"

    result = media.transcode_for_platform(
        src, out, limits, probe=_source_probe(media),
    )

    assert result == out
    assert out.exists()
    assert out.stat().st_size == 90_000
    assert len(calls) == 2, "expected a hardware attempt then a software retry"
    assert "h264_videotoolbox" in calls[0] and "libx264" not in calls[0]
    assert "libx264" in calls[1] and "h264_videotoolbox" not in calls[1]


def test_both_lanes_overshoot_raises_and_ships_no_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """When libx264 also can't fit the cap, the function raises the real
    verification error and leaves no oversized file behind (rule C)."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    calls = _install_fake_encode(
        monkeypatch, media, sizes={"h264_videotoolbox": 150_000, "libx264": 140_000},
    )

    limits = media.PlatformMediaLimits(max_bytes=100_000)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    out = tmp_path / "out.mp4"

    with pytest.raises(media.TranscodeVerificationError):
        media.transcode_for_platform(src, out, limits, probe=_source_probe(media))

    assert not out.exists(), "an oversized output must never survive verification"
    assert len(calls) == 2, "one hardware attempt, one software retry, then fail"
    assert "h264_videotoolbox" in calls[0]
    assert "libx264" in calls[1]


def test_software_only_overshoot_fails_loudly_without_retry(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """With no hardware encoder the first (software) encode is the only one:
    an overshoot fails loudly rather than re-running the same libx264 path."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset())
    calls = _install_fake_encode(
        monkeypatch, media, sizes={"h264_videotoolbox": 150_000, "libx264": 150_000},
    )

    limits = media.PlatformMediaLimits(max_bytes=100_000)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    out = tmp_path / "out.mp4"

    with pytest.raises(media.TranscodeVerificationError):
        media.transcode_for_platform(src, out, limits, probe=_source_probe(media))

    assert not out.exists()
    assert len(calls) == 1, "software lane must not loop back onto itself"
    assert "libx264" in calls[0]


def test_hardware_meets_budget_first_try_no_retry(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """When videotoolbox lands under the cap on the first try, nothing changes:
    one hardware encode, no software retry."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    calls = _install_fake_encode(
        monkeypatch, media, sizes={"h264_videotoolbox": 90_000, "libx264": 90_000},
    )

    limits = media.PlatformMediaLimits(max_bytes=100_000)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    out = tmp_path / "out.mp4"

    result = media.transcode_for_platform(
        src, out, limits, probe=_source_probe(media),
    )

    assert result == out and out.exists()
    assert len(calls) == 1
    assert "h264_videotoolbox" in calls[0]


def test_no_byte_budget_stays_on_hardware(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """With no byte cap the existing behavior is preserved: hardware encode,
    no size-driven retry even if the output is large."""
    from yt_scheduler.services import media

    monkeypatch.setattr(media, "_HARDWARE_ENCODERS", frozenset({"h264_videotoolbox"}))
    calls = _install_fake_encode(
        monkeypatch, media, sizes={"h264_videotoolbox": 500_000, "libx264": 500_000},
    )

    limits = media.PlatformMediaLimits(max_bytes=None)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"\x00")
    out = tmp_path / "out.mp4"

    result = media.transcode_for_platform(
        src, out, limits, probe=_source_probe(media),
    )

    assert result == out and out.exists()
    assert len(calls) == 1
    assert "h264_videotoolbox" in calls[0]
