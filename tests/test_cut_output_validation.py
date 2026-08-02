"""A cut is validated before it is published — ffmpeg exit 0 is not proof.

An encoder exiting 0 only means it didn't error: an empty videotoolbox encode,
a seek landing at EOF (zero frames, "success"), or a filter graph silently
dropping the audio track all exit 0. Before this, such a file was atomically
renamed to its final name and sailed on — a blank preview with no error
anywhere (rule C), or worse, an upload to YouTube. ``_validate_encoded_clip``
probes the temp BEFORE the rename, so a final filename only ever holds a
verified clip.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _probe(media, *, duration=10.0, width=3840, height=2160, has_audio=True):
    return media.VideoProbe(
        duration_seconds=duration, width=width, height=height,
        bitrate_bps=18_000_000, size_bytes=1_000_000,
        codec_name="h264", container="mp4", has_audio=has_audio,
    )


@pytest.fixture
def media(isolated_data_dir):
    return importlib.import_module("yt_scheduler.services.media")


def _tmp_clip(tmp_path: Path) -> Path:
    f = tmp_path / ".cutpart_test.mp4"
    f.write_bytes(b"\x00" * 32)
    return f


def test_good_output_passes_and_keeps_the_temp(media, monkeypatch, tmp_path):
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(media, "probe_video_file", lambda p: _probe(media))

    media._validate_encoded_clip(
        tmp, expected_duration_seconds=10.0, source_has_audio=True,
        describe="test cut",
    )
    assert tmp.exists()


def test_duration_mismatch_raises_and_deletes_the_temp(media, monkeypatch, tmp_path):
    """The EOF-seek / truncated-write case: file much shorter than asked."""
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: _probe(media, duration=2.0),
    )

    with pytest.raises(RuntimeError, match=r"duration 2\.00s vs expected 10\.00s"):
        media._validate_encoded_clip(
            tmp, expected_duration_seconds=10.0, source_has_audio=None,
            describe="test cut",
        )
    assert not tmp.exists(), "a failed temp must not survive to look usable"


def test_no_video_stream_raises(media, monkeypatch, tmp_path):
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(
        media, "probe_video_file",
        lambda p: _probe(media, duration=None, width=None, height=None),
    )

    with pytest.raises(RuntimeError, match="no readable video stream"):
        media._validate_encoded_clip(
            tmp, expected_duration_seconds=10.0, source_has_audio=None,
            describe="test cut",
        )
    assert not tmp.exists()


def test_dropped_audio_raises_when_source_had_audio(media, monkeypatch, tmp_path):
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: _probe(media, has_audio=False),
    )

    with pytest.raises(RuntimeError, match="output has no audio stream"):
        media._validate_encoded_clip(
            tmp, expected_duration_seconds=10.0, source_has_audio=True,
            describe="test cut",
        )
    assert not tmp.exists()


def test_unknown_source_audio_skips_the_audio_check(media, monkeypatch, tmp_path):
    """Unknown is not confirmed-absent: no source probe -> no audio verdict."""
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: _probe(media, has_audio=False),
    )

    media._validate_encoded_clip(
        tmp, expected_duration_seconds=10.0, source_has_audio=None,
        describe="test cut",
    )
    assert tmp.exists()


def test_small_drift_within_tolerance_passes(media, monkeypatch, tmp_path):
    """Precise cuts land within a frame or two — ±0.5s must not false-alarm."""
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: _probe(media, duration=10.4),
    )

    media._validate_encoded_clip(
        tmp, expected_duration_seconds=10.0, source_has_audio=True,
        describe="test cut",
    )
    assert tmp.exists()


def test_ffprobe_unavailable_skips_validation_loudly(
    media, monkeypatch, tmp_path, caplog,
):
    tmp = _tmp_clip(tmp_path)
    monkeypatch.setattr(media, "probe_video_file", lambda p: None)

    with caplog.at_level("WARNING"):
        media._validate_encoded_clip(
            tmp, expected_duration_seconds=10.0, source_has_audio=True,
            describe="test cut",
        )
    assert tmp.exists()
    assert any("Skipping output validation" in r.message for r in caplog.records)


def test_extract_clip_refuses_to_publish_a_bad_encode(
    media, monkeypatch, tmp_path,
):
    """End to end through extract_clip: exit-0 ffmpeg + garbage output must
    raise and leave NO file under the final name."""
    source = tmp_path / "uploads" / "src.mp4"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"\x00" * 64)

    def fake_run(cmd, **kwargs):
        # "ffmpeg" writes its temp output and exits 0.
        Path(cmd[-1]).write_bytes(b"\x00" * 32)
        class R:
            returncode = 0
            stderr = b""
        return R()

    monkeypatch.setattr(media.subprocess, "run", fake_run)
    monkeypatch.setattr(media, "hardware_encoder_available", lambda codec: False)
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: _probe(media, duration=0.0),
    )

    with pytest.raises(RuntimeError, match="cut validation failed"):
        media.extract_clip(
            source, "0:10", "0:20", output_name="validated_out.mp4",
            precise=True, encoder="software",
        )
    assert not (media.UPLOAD_DIR / "validated_out.mp4").exists()
    assert not list(media.UPLOAD_DIR.glob(".cutpart_*")), "temp must be cleaned"
