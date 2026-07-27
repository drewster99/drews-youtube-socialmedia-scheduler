"""Media normalisation for social posting.

Two bugs these cover, both found by transcoding a real 4K clip rather than by
reading the code:

* A byte ceiling read as a *target* rather than a cap. X allows 512 MB, so
  budget-filling maths re-encoded a 92 MB source into a 457 MB file.
* A frame-rate cap read as a target, upsampling 24fps content to 120fps.

Both are the same mistake — treating a limit as something to spend — so the
invariant worth protecting is: a transcode never inflates.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from yt_scheduler.services import media


def _probe(**overrides) -> media.VideoProbe:
    """A 4K vertical clip like the ones the promo cutter produces."""
    fields = {
        "duration_seconds": 68.0,
        "width": 2160,
        "height": 3840,
        "bitrate_bps": 10_900_000,
        "size_bytes": 92_000_000,
        "codec_name": "h264",
        "container": "mov",
        "has_audio": True,
        "frame_rate": 24.0,
    }
    fields.update(overrides)
    return media.VideoProbe(**fields)


class TestFitDimensions:
    @pytest.mark.parametrize(
        "source,expected",
        [
            ((2160, 3840), (1080, 1920)),   # 4K vertical -> 1080 vertical
            ((3840, 2160), (1920, 1080)),   # 4K landscape -> 1080 landscape
            ((2160, 2160), (1080, 1080)),   # square stays square
            ((1080, 1920), (1080, 1920)),   # already compliant, untouched
            ((640, 480), (640, 480)),       # never upscaled
        ],
    )
    def test_preserves_orientation_and_never_upscales(self, source, expected):
        assert media.fit_dimensions(*source) == expected

    def test_dimensions_are_even(self):
        # h264 yuv420p cannot encode an odd dimension.
        width, height = media.fit_dimensions(1215, 2159)
        assert width % 2 == 0 and height % 2 == 0

    def test_rounds_down_so_a_cap_is_never_re_breached(self):
        width, height = media.fit_dimensions(
            2160, 3840, max_pixels=2_304_000
        )
        assert width * height <= 2_304_000

    def test_rejects_nonsense_dimensions(self):
        with pytest.raises(ValueError):
            media.fit_dimensions(0, 1080)


class TestViolatesLimits:
    def test_compliant_file_reports_nothing(self):
        # Bluesky: 300 MB / 3 min, no resolution cap — a 92 MB 68s clip fits.
        limits = media.PlatformMediaLimits(
            max_bytes=300_000_000, max_duration_seconds=180
        )
        assert media.violates_limits(_probe(), limits) == []

    def test_reports_matrix_breach(self):
        limits = media.PlatformMediaLimits(max_pixels=1920 * 1200)
        assert len(media.violates_limits(_probe(), limits)) == 1

    def test_unknown_fields_are_not_violations(self):
        """We never re-encode on a guess."""
        limits = media.PlatformMediaLimits(max_bytes=1, max_pixels=1)
        blind = _probe(width=None, height=None, size_bytes=None)
        assert media.violates_limits(blind, limits) == []


class TestTranscodePlanning:
    """The planning maths, without invoking ffmpeg."""

    def test_target_bitrate_scales_down_with_resolution(self):
        assert media._target_video_bitrate_bps(1080, 1920) < \
            media._target_video_bitrate_bps(2160, 3840)

    def test_duration_over_cap_is_unfixable(self, tmp_path):
        """No encode shortens a clip, so the caller must skip the platform."""
        limits = media.PlatformMediaLimits(max_duration_seconds=140)
        with pytest.raises(media.MediaTooLongError):
            media.transcode_for_platform(
                tmp_path / "in.mp4", tmp_path / "out.mp4", limits,
                probe=_probe(duration_seconds=168.0),
            )

    def test_impossible_byte_budget_fails_loudly(self, tmp_path):
        """Better a clear error than a smeared, unwatchable clip."""
        limits = media.PlatformMediaLimits(max_bytes=1_000_000)
        with pytest.raises(media.TranscodeVerificationError):
            media.transcode_for_platform(
                tmp_path / "in.mp4", tmp_path / "out.mp4", limits,
                probe=_probe(duration_seconds=600.0),
            )

    def test_unknown_dimensions_refuse_rather_than_guess(self, tmp_path):
        with pytest.raises(ValueError):
            media.transcode_for_platform(
                tmp_path / "in.mp4", tmp_path / "out.mp4",
                media.PlatformMediaLimits(),
                probe=_probe(width=None, height=None),
            )


class TestPlatformRegistry:
    def test_every_platform_has_limits(self):
        from yt_scheduler.services.social import ALL_PLATFORMS, PLATFORM_MEDIA_LIMITS

        assert set(PLATFORM_MEDIA_LIMITS) == set(ALL_PLATFORMS)

    def test_x_duration_cap_is_the_standard_account_one(self):
        from yt_scheduler.services.social import PLATFORM_MEDIA_LIMITS

        # Premium raises this; assuming the higher tier would produce uploads
        # that fail for anyone without it.
        assert PLATFORM_MEDIA_LIMITS["twitter"].max_duration_seconds == 140

    def test_mastodon_static_entry_is_the_strict_fallback(self):
        """The static entry is only used when the instance can't be reached,
        so it must be the restrictive case — never more permissive than a
        real instance would be."""
        from yt_scheduler.services.social import PLATFORM_MEDIA_LIMITS

        entry = PLATFORM_MEDIA_LIMITS["mastodon"]
        assert entry.max_bytes == 40 * 1024 * 1024
        assert entry.max_pixels == 2_304_000


class TestRotatedSources:
    """A phone clip shot vertically is stored as a landscape frame plus a
    90-degree display matrix. Every player, and ffmpeg's own decoder, shows it
    portrait — so measuring the coded frame files it as landscape, and an
    orientation filter set to portrait misses the very videos it exists for.
    """

    @staticmethod
    def _rotated(degrees: int) -> media.VideoProbe:
        """Coded landscape 1920x1080, displayed per ``degrees``."""
        return media.VideoProbe(
            duration_seconds=30.0, width=1920, height=1080,
            bitrate_bps=8_000_000, size_bytes=30_000_000,
            codec_name="h264", rotation_degrees=degrees,
        )

    @pytest.mark.parametrize("degrees", [90, -90, 270, -270])
    def test_a_quarter_turn_swaps_the_display_shape(self, degrees):
        probe = self._rotated(degrees)
        assert (probe.display_width, probe.display_height) == (1080, 1920)

    @pytest.mark.parametrize("degrees", [0, 180, -180, 360])
    def test_a_half_turn_leaves_the_shape_alone(self, degrees):
        probe = self._rotated(degrees)
        assert (probe.display_width, probe.display_height) == (1920, 1080)

    def test_orientation_follows_what_a_viewer_sees(self):
        from yt_scheduler.services.video_dimensions import orientation_of

        rotated = self._rotated(90)
        assert orientation_of(rotated.width, rotated.height) == "landscape"
        assert orientation_of(
            rotated.display_width, rotated.display_height
        ) == "portrait", "selecting portrait must find a phone-shot vertical clip"

    def test_limits_are_measured_against_the_displayed_frame(self):
        """A platform's pixel and edge caps apply to the frame it renders."""
        # 1920 long edge either way, so the edge cap must fire for both.
        limits = media.PlatformMediaLimits(max_long_edge=1280)
        assert media.violates_limits(self._rotated(90), limits)
        assert media.violates_limits(self._rotated(0), limits)

    def test_unknown_dimensions_stay_unknown_through_rotation(self):
        blind = media.VideoProbe(
            duration_seconds=1.0, width=None, height=None,
            bitrate_bps=None, size_bytes=None, rotation_degrees=90,
        )
        assert blind.display_width is None
        assert blind.display_height is None

    def test_transcode_targets_the_displayed_shape(self, monkeypatch, tmp_path):
        """ffmpeg autorotates, so the frame reaching the scale filter is the
        rotated one. Scaling to the coded shape would squash a vertical clip
        into a landscape box."""
        import subprocess

        captured: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            captured.append(cmd)
            Path(cmd[-1]).write_bytes(b"\0" * 1000)
            return subprocess.CompletedProcess(cmd, 0, b"", b"")

        monkeypatch.setattr(media.subprocess, "run", fake_run)
        monkeypatch.setattr(
            media, "hardware_encoder_available", lambda codec="h264": False
        )
        monkeypatch.setattr(
            media, "probe_video_file",
            lambda path: media.VideoProbe(
                duration_seconds=30.0, width=1080, height=1920,
                bitrate_bps=1, size_bytes=1000, codec_name="h264",
            ),
        )
        media.transcode_for_platform(
            tmp_path / "in.mp4", tmp_path / "out.mp4",
            media.PlatformMediaLimits(max_pixels=2_304_000),
            probe=self._rotated(90),
        )
        scale = captured[0][captured[0].index("-vf") + 1]
        assert scale == "scale=1080:1920", (
            f"scaled to {scale}; a rotated vertical source must not be "
            "squashed into the shape it happens to be stored as"
        )

    def test_a_probe_round_trips_its_rotation(self):
        """The Replace-source pending snapshot stores a probe's fields and
        rebuilds it later. Dropping rotation there would rebuild a probe that
        claims coded == display, quietly undoing the fix for that path."""
        original = self._rotated(90)
        snapshot = {
            "probe_width": original.width,
            "probe_height": original.height,
            "probe_rotation_degrees": original.rotation_degrees,
        }
        rebuilt = media.VideoProbe(
            duration_seconds=None, width=snapshot["probe_width"],
            height=snapshot["probe_height"], bitrate_bps=None, size_bytes=None,
            rotation_degrees=snapshot["probe_rotation_degrees"],
        )
        assert (rebuilt.display_width, rebuilt.display_height) == (1080, 1920)

    def test_a_pre_rotation_snapshot_still_rebuilds(self):
        """Entries written before rotation was captured have no such key; 0
        reproduces exactly what they meant at the time."""
        rebuilt = media.VideoProbe(
            duration_seconds=None, width=1920, height=1080,
            bitrate_bps=None, size_bytes=None,
            rotation_degrees={}.get("probe_rotation_degrees", 0),
        )
        assert (rebuilt.display_width, rebuilt.display_height) == (1920, 1080)
