"""extract_clip must publish its output atomically.

Regression for the generate-from-source faststart failure: a preview-cleanup
sweep could delete a gen_preview_*.mp4 while ffmpeg was still doing its
``+faststart`` moov-shift pass on a large 4K cut, producing "Unable to re-open
output file for shifting data / No such file or directory". The fix encodes to a
``.cutpart_*.mp4`` temp (which no cleanup glob matches) and renames to the final
name only after ffmpeg fully succeeds.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from yt_scheduler.services import media


def _fake_success(cmd, **kwargs):
    out = Path(cmd[-1])  # ffmpeg output path is the last arg
    # The in-progress file must be a hidden temp, never the final name, so a
    # cleanup globbing gen_preview_*.mp4 can't delete it mid-encode.
    assert out.name.startswith(".cutpart_"), out.name
    assert out.suffix == ".mp4"  # ffmpeg needs the mp4 extension for the muxer
    out.write_bytes(b"encoded")
    return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")


def test_extract_clip_publishes_atomically(tmp_path, monkeypatch):
    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "hardware_encoder_available", lambda codec: False)
    monkeypatch.setattr(media.subprocess, "run", _fake_success)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"fake")

    result = media.extract_clip(src, "0:00", "0:05", output_name="gen_preview_x.mp4")

    assert result == tmp_path / "gen_preview_x.mp4"
    assert result.read_bytes() == b"encoded"
    assert not list(tmp_path.glob(".cutpart_*"))  # temp renamed away, none left


def test_extract_clip_failure_leaves_no_final_or_temp(tmp_path, monkeypatch):
    monkeypatch.setattr(media, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(media, "hardware_encoder_available", lambda codec: False)
    src = tmp_path / "src.mp4"
    src.write_bytes(b"fake")

    def fake_fail(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"partial")  # ffmpeg wrote a partial temp
        raise subprocess.CalledProcessError(1, cmd, output=b"", stderr=b"boom")

    monkeypatch.setattr(media.subprocess, "run", fake_fail)
    with pytest.raises(RuntimeError):
        media.extract_clip(src, "0:00", "0:05", output_name="gen_preview_x.mp4")

    assert not (tmp_path / "gen_preview_x.mp4").exists()  # no misleading final file
    assert not list(tmp_path.glob(".cutpart_*"))  # temp cleaned up
