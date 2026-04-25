"""Tier classification + duration parsing tests."""

from __future__ import annotations

import pytest

from yt_scheduler.services.tiers import parse_iso8601_duration, tier_for_duration


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (0, "hook"),
        (49, "hook"),
        (49.999, "hook"),
        (50, "short"),
        (51, "short"),
        (179, "short"),
        (180, "segment"),
        (181, "segment"),
        (719, "segment"),
        (720, "video"),
        (3600, "video"),
        (None, None),
        (-1, None),
    ],
)
def test_tier_boundaries(seconds, expected) -> None:
    assert tier_for_duration(seconds) == expected


@pytest.mark.parametrize(
    "iso, expected",
    [
        ("PT27S", 27.0),
        ("PT3M31S", 211.0),
        ("PT1H2M3S", 3723.0),
        ("PT1H", 3600.0),
        ("P1DT2H", 93600.0),
        ("PT0S", 0.0),
        ("", None),
        (None, None),
        ("garbage", None),
    ],
)
def test_parse_iso8601_duration(iso, expected) -> None:
    assert parse_iso8601_duration(iso) == expected
