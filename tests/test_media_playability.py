"""Browser-playability allowlist + source-quality warnings.

Pure unit coverage for the helpers in ``services/media.py`` — no server,
no DB. Each test calls the helper directly with constructed inputs.
"""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    "codec,container,expected",
    [
        # H.264 / AVC in both common containers — bread and butter.
        ("h264", "mp4", True),
        ("h264", "mov", True),
        ("H264", "MP4", True),  # case-insensitive
        # HEVC works in modern Safari on Apple Silicon.
        ("hevc", "mp4", True),
        ("hevc", "mov", True),
        # VP9 / AV1 in their canonical containers.
        ("vp9", "webm", True),
        ("av1", "mp4", True),
        ("av1", "webm", True),
        # Pro / archival codecs — won't play in browsers, must fall back.
        ("prores", "mov", False),
        ("dnxhd", "mov", False),
        # Wrong-container case: VP9 in MP4 is technically playable on some
        # browsers, but not reliably on Safari — keep the allowlist tight.
        ("vp9", "mp4", False),
    ],
)
def test_is_browser_playable_matrix(codec, container, expected):
    from yt_scheduler.services.media import is_browser_playable

    assert is_browser_playable(codec, container) is expected


def test_is_browser_playable_returns_none_on_missing_pieces():
    """Caller treats None as "unknown, fall back to YouTube embed"."""
    from yt_scheduler.services.media import is_browser_playable

    assert is_browser_playable(None, "mp4") is None
    assert is_browser_playable("h264", None) is None
    assert is_browser_playable("", "mp4") is None
    assert is_browser_playable(None, None) is None


def test_quality_warnings_empty_on_clean_1080p_upload():
    from yt_scheduler.services.media import source_quality_warnings

    assert source_quality_warnings(
        width=1920, height=1080, source_origin="uploaded",
    ) == []


def test_quality_warnings_low_resolution_landscape():
    from yt_scheduler.services.media import source_quality_warnings

    warnings = source_quality_warnings(
        width=1280, height=720, source_origin="uploaded",
    )
    assert len(warnings) == 1
    assert warnings[0]["code"] == "low_resolution"
    assert warnings[0]["min_dimension"] == 720
    assert "1280×720" in warnings[0]["message"]


def test_quality_warnings_low_resolution_vertical():
    """A vertical 720p source has width=720 — `min(w,h) < 1080` catches it
    regardless of which dimension is short."""
    from yt_scheduler.services.media import source_quality_warnings

    warnings = source_quality_warnings(
        width=720, height=1280, source_origin="uploaded",
    )
    assert len(warnings) == 1
    assert warnings[0]["code"] == "low_resolution"
    assert warnings[0]["min_dimension"] == 720


def test_quality_warnings_youtube_download_fires_at_any_resolution():
    """A 4K YouTube re-download is still lossy — the origin warning fires
    independently of pixel count."""
    from yt_scheduler.services.media import source_quality_warnings

    warnings = source_quality_warnings(
        width=3840, height=2160, source_origin="youtube_download",
    )
    assert len(warnings) == 1
    assert warnings[0]["code"] == "youtube_download_lossy"


def test_quality_warnings_both_fire_when_both_apply():
    """A 720p YouTube re-download fires both warnings, in order."""
    from yt_scheduler.services.media import source_quality_warnings

    warnings = source_quality_warnings(
        width=1280, height=720, source_origin="youtube_download",
    )
    codes = [w["code"] for w in warnings]
    assert codes == ["low_resolution", "youtube_download_lossy"]


def test_quality_warnings_skips_resolution_when_unknown():
    """Missing dimensions → no resolution warning. Origin warning still
    fires independently."""
    from yt_scheduler.services.media import source_quality_warnings

    assert source_quality_warnings(
        width=None, height=None, source_origin="uploaded",
    ) == []
    warnings = source_quality_warnings(
        width=None, height=None, source_origin="youtube_download",
    )
    assert len(warnings) == 1
    assert warnings[0]["code"] == "youtube_download_lossy"


def test_probe_captures_codec_and_container(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
):
    """Mock ffprobe output to verify the JSON parsing pulls codec + container."""
    from yt_scheduler.services import media

    f = tmp_path / "fake.mp4"
    f.write_bytes(b"\x00")

    class _R:
        returncode = 0
        stdout = (
            '{"streams":[{"width":1920,"height":1080,"codec_name":"h264"}],'
            '"format":{"duration":"60.0","format_name":"mov,mp4,m4a,3gp,3g2,mj2"}}'
        )

    monkeypatch.setattr(media.subprocess, "run", lambda *a, **k: _R())
    probe = media.probe_video_file(f)
    assert probe is not None
    assert probe.codec_name == "h264"
    # format_name is a comma list; first token is the canonical one.
    assert probe.container == "mov"
    assert probe.width == 1920
    assert probe.height == 1080
