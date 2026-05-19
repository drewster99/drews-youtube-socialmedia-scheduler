"""Deterministic title-from-filename fallback used by the Promo Videos
auto-action chain when the Claude call fails / no API key is configured.

The AI path is exercised in higher-level integration tests; these only
cover the always-available no-API branch.
"""

from __future__ import annotations

from yt_scheduler.services.ai import fallback_title_from_filename


def test_strips_known_recording_prefixes() -> None:
    out = fallback_title_from_filename(
        "riverside_apple's_accessibility & nutrition labels reminder_drew_and dan in the.mp4"
    )
    assert out.lower().startswith("apple's accessibility")
    assert "riverside" not in out.lower()
    assert ".mp4" not in out


def test_replaces_underscores_and_hyphens_with_spaces() -> None:
    assert fallback_title_from_filename("my_great-clip.mov") == "My Great Clip"


def test_strips_extension() -> None:
    assert fallback_title_from_filename("clip.MP4") == "Clip"


def test_drops_path() -> None:
    assert (
        fallback_title_from_filename("/tmp/uploads/some_clip.mp4")
        == "Some Clip"
    )


def test_collapses_whitespace() -> None:
    assert (
        fallback_title_from_filename("hello   world   .mp4")
        == "Hello World"
    )


def test_empty_filename_returns_untitled() -> None:
    assert fallback_title_from_filename(".mp4") == "Untitled video"
    assert fallback_title_from_filename("") == "Untitled video"


def test_caps_at_one_hundred_chars() -> None:
    long_name = ("word_" * 50) + ".mp4"
    out = fallback_title_from_filename(long_name)
    assert len(out) <= 100


def test_multiple_known_prefixes() -> None:
    for prefix in [
        "riverside_",
        "recording_",
        "screen_recording_",
        "screenrecording_",
        "untitled_",
        "new_recording_",
    ]:
        out = fallback_title_from_filename(prefix + "hello world.mp4")
        assert out.lower().startswith("hello"), f"{prefix} not stripped"
