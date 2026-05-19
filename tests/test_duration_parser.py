"""Tests for the single-unit duration string parser used by the
Promo schedule-all default delays.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from yt_scheduler.services.duration_parser import (
    InvalidDuration,
    MAX_DURATION,
    parse_duration,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("24h", timedelta(hours=24)),
        ("1.5h", timedelta(hours=1.5)),
        ("30s", timedelta(seconds=30)),
        ("90m", timedelta(minutes=90)),
        ("1w", timedelta(weeks=1)),
        ("1 hour", timedelta(hours=1)),
        ("90 minutes", timedelta(minutes=90)),
        ("3 days", timedelta(days=3)),
        ("3D", timedelta(days=3)),
        ("0.5h", timedelta(minutes=30)),
    ],
)
def test_parses_valid_durations(text: str, expected: timedelta) -> None:
    assert parse_duration(text) == expected


@pytest.mark.parametrize(
    "text",
    ["0s", "0", "-1h", "-30s"],
)
def test_rejects_zero_or_negative(text: str) -> None:
    with pytest.raises(InvalidDuration):
        parse_duration(text)


@pytest.mark.parametrize(
    "text",
    ["1d 2h", "24", "h24", "abc", "", "  ", "10.5.5h"],
)
def test_rejects_garbage_or_compound(text: str) -> None:
    with pytest.raises(InvalidDuration):
        parse_duration(text)


def test_rejects_overlong_duration() -> None:
    with pytest.raises(InvalidDuration):
        parse_duration("91d")


def test_accepts_exact_max_duration() -> None:
    assert parse_duration("90d") == MAX_DURATION


def test_unknown_unit_rejected() -> None:
    with pytest.raises(InvalidDuration):
        parse_duration("10y")
