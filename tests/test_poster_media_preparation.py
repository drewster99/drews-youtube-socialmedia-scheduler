"""Every send path routes attachments through the destination's limits.

Media preparation lives on the base poster as a template method rather than
in each of the five implementations, so the smart queue, the publish fan-out,
and manual Send cannot disagree about whether a clip was checked.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from yt_scheduler.services import media, social


class RecordingPoster(social.SocialPoster):
    """Captures what the platform-specific leg was handed."""

    platform = "bluesky"

    def __init__(self):
        super().__init__(bundle={})
        self.received: list[str] | None = None

    async def _post_prepared(
        self, text, media_path=None, *, media_paths=None, alt_texts=None
    ) -> dict:
        self.received = list(media_paths or [])
        # Captured while the context manager is still open, so a temp file
        # is still on disk here.
        self.existed = [Path(p).exists() for p in self.received]
        return {"url": "https://example.test/post", "id": "1"}


@pytest.fixture
def upload_dir(tmp_path, monkeypatch):
    """Point UPLOAD_DIR at a temp dir and exercise the REAL containment check.

    Stubbing ``is_managed_media_path`` here would hide the bug this fixture
    exists to catch: a derived file written outside UPLOAD_DIR is rejected by
    every poster's own ``_require_paths_managed`` re-check, so a transcoded
    post fails on the containment guard rather than uploading.
    """
    from yt_scheduler import config

    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(config, "UPLOAD_DIR", uploads)
    return uploads


@pytest.fixture
def managed_video(upload_dir):
    video = upload_dir / "clip.mp4"
    video.write_bytes(b"not really a video")
    return video


async def test_compliant_video_is_passed_through_untouched(
    managed_video, monkeypatch
):
    """No re-encode when the source already fits — lossless and instant."""
    poster = RecordingPoster()
    monkeypatch.setattr(
        poster, "media_limits",
        lambda: _async(media.PlatformMediaLimits(max_bytes=300_000_000)),
    )
    monkeypatch.setattr(
        media, "probe_video_file",
        lambda p: media.VideoProbe(
            duration_seconds=10.0, width=1080, height=1920,
            bitrate_bps=4_000_000, size_bytes=5_000_000, codec_name="h264",
        ),
    )
    called = []
    monkeypatch.setattr(
        media, "transcode_for_platform",
        lambda *a, **k: called.append(a),
    )

    await poster.post("hi", media_paths=[str(managed_video)])

    assert poster.received == [str(managed_video)]
    assert called == [], "a compliant file must not be re-encoded"


async def test_violating_video_is_transcoded_then_cleaned_up(
    managed_video, upload_dir, monkeypatch
):
    poster = RecordingPoster()
    monkeypatch.setattr(
        poster, "media_limits",
        lambda: _async(media.PlatformMediaLimits(max_pixels=2_304_000)),
    )
    monkeypatch.setattr(
        media, "probe_video_file",
        lambda p: media.VideoProbe(
            duration_seconds=10.0, width=2160, height=3840,
            bitrate_bps=10_000_000, size_bytes=90_000_000, codec_name="h264",
        ),
    )

    def fake_transcode(source, output, limits, *, probe):
        Path(output).write_bytes(b"transcoded")
        return Path(output)

    monkeypatch.setattr(media, "transcode_for_platform", fake_transcode)

    await poster.post("hi", media_paths=[str(managed_video)])

    assert poster.received != [str(managed_video)], "should upload the derived file"
    assert poster.existed == [True], "temp file must exist during the send"
    # The derived file MUST live inside UPLOAD_DIR. Every poster re-checks its
    # attachments with _require_paths_managed, which rejects anything outside
    # it — a system temp dir would fail every transcoded post.
    assert Path(poster.received[0]).is_relative_to(upload_dir)
    # Late-bound means short-lived: nothing is left behind to need a janitor.
    assert not Path(poster.received[0]).exists()
    assert managed_video.exists(), "the source must never be touched"


async def test_temp_file_is_removed_even_when_the_send_fails(
    managed_video, monkeypatch
):
    class FailingPoster(RecordingPoster):
        async def _post_prepared(self, text, media_path=None, **kwargs):
            self.received = list(kwargs.get("media_paths") or [])
            raise RuntimeError("platform exploded")

    poster = FailingPoster()
    monkeypatch.setattr(
        poster, "media_limits",
        lambda: _async(media.PlatformMediaLimits(max_pixels=1000)),
    )
    monkeypatch.setattr(
        media, "probe_video_file",
        lambda p: media.VideoProbe(
            duration_seconds=10.0, width=2160, height=3840,
            bitrate_bps=1, size_bytes=1, codec_name="h264",
        ),
    )
    monkeypatch.setattr(
        media, "transcode_for_platform",
        lambda source, output, limits, *, probe: Path(output).write_bytes(b"x"),
    )

    with pytest.raises(RuntimeError):
        await poster.post("hi", media_paths=[str(managed_video)])

    assert not Path(poster.received[0]).exists()


async def test_unprobeable_video_refuses_rather_than_uploading_blind(
    managed_video, monkeypatch
):
    """If we can't tell whether it fits, we don't hand it to the platform."""
    poster = RecordingPoster()
    monkeypatch.setattr(
        poster, "media_limits",
        lambda: _async(media.PlatformMediaLimits(max_bytes=1_000)),
    )
    monkeypatch.setattr(media, "probe_video_file", lambda p: None)

    with pytest.raises(social.MediaUploadError):
        await poster.post("hi", media_paths=[str(managed_video)])
    assert poster.received is None


async def test_non_video_attachments_skip_preparation(upload_dir, monkeypatch):
    image = upload_dir / "thumb.jpg"
    image.write_bytes(b"jpeg")

    poster = RecordingPoster()
    monkeypatch.setattr(
        poster, "media_limits",
        lambda: _async(media.PlatformMediaLimits(max_bytes=1)),
    )
    probed = []
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: probed.append(p)
    )

    await poster.post("hi", media_paths=[str(image)])

    assert poster.received == [str(image)]
    assert probed == [], "images are not video and must not be probed"


async def test_threads_skips_preparation_entirely(managed_video, monkeypatch):
    """Threads fetches media from a public URL we don't have, so its poster
    rejects attachments. Preparing first would burn a transcode on a post
    that cannot succeed."""
    probed = []
    monkeypatch.setattr(
        media, "probe_video_file", lambda p: probed.append(p)
    )
    poster = social.ThreadsPoster(bundle={})

    assert poster.accepts_media is False
    with pytest.raises(social.MediaUploadError):
        await poster.post("hi", media_paths=[str(managed_video)])
    assert probed == []


def _async(value):
    """Wrap a plain value in an awaitable, for patching async methods."""
    async def _coro():
        return value
    return _coro()
