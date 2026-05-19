"""Single-unit duration string parser used by the Promo schedule-all
default-delay configuration.

Accepts forms like ``24h``, ``1.5h``, ``30s``, ``90m``, ``90 minutes``,
``1w``, ``"3 days"``. Single-unit only — no compounds like ``1d 2h``.

Rejects:

* zero or negative values
* anything longer than 90 days (matches the spec's outer bound)
* unparseable strings (raises :class:`InvalidDuration`)
"""

from __future__ import annotations

import re
from datetime import timedelta

MAX_DURATION = timedelta(days=90)

_UNIT_TO_SECONDS = {
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
    "w": 604800.0,
    "week": 604800.0,
    "weeks": 604800.0,
}

_DURATION_RE = re.compile(
    r"""
    ^
    \s*
    (?P<value>\d+(?:\.\d+)?)
    \s*
    (?P<unit>[a-zA-Z]+)
    \s*
    $
    """,
    re.VERBOSE,
)


class InvalidDuration(ValueError):
    """Raised when a duration string can't be parsed or is out of range."""


def parse_duration(text: str) -> timedelta:
    """Parse a single-unit duration string and return a :class:`timedelta`.

    Raises :class:`InvalidDuration` for empty strings, compound forms,
    unknown units, zero / negative values, or anything exceeding
    :data:`MAX_DURATION`.
    """
    if text is None:
        raise InvalidDuration("Duration must be a string, got None")
    candidate = text.strip()
    if not candidate:
        raise InvalidDuration("Duration string is empty")
    match = _DURATION_RE.match(candidate)
    if not match:
        raise InvalidDuration(
            f"Cannot parse duration {text!r}; expected forms like "
            "'24h', '1.5h', '90m', '3 days'."
        )
    raw_value = match.group("value")
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise InvalidDuration(
            f"Cannot parse numeric value {raw_value!r}"
        ) from exc
    unit = match.group("unit").lower()
    seconds_per_unit = _UNIT_TO_SECONDS.get(unit)
    if seconds_per_unit is None:
        raise InvalidDuration(
            f"Unknown duration unit {match.group('unit')!r}. "
            f"Accepted: s/sec/seconds, m/min/minutes, h/hr/hours, "
            f"d/day/days, w/week/weeks."
        )
    if value <= 0:
        raise InvalidDuration("Duration must be strictly positive")
    total_seconds = value * seconds_per_unit
    delta = timedelta(seconds=total_seconds)
    if delta > MAX_DURATION:
        raise InvalidDuration(
            f"Duration exceeds the 90-day maximum (got {delta})."
        )
    return delta
