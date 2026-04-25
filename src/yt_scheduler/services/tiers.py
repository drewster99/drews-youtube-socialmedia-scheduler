"""Tier classification — single source of truth for the four-tier system.

Cutoffs (per spec):

* ``hook``    — under 50 seconds
* ``short``   — 50 to under 180 seconds
* ``segment`` — 180 to under 720 seconds (3 to 12 minutes)
* ``video``   — 720 seconds or more (12 minutes and up)
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Literal

Tier = Literal["hook", "short", "segment", "video"]
ALL_TIERS: tuple[Tier, ...] = ("hook", "short", "segment", "video")


def tier_for_duration(seconds: float | int | None) -> Tier | None:
    """Classify a duration into a tier; returns None when duration is unknown."""
    if seconds is None:
        return None
    if seconds < 0:
        return None
    if seconds < 50:
        return "hook"
    if seconds < 180:
        return "short"
    if seconds < 720:
        return "segment"
    return "video"


_ISO8601_DURATION = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$"
)


def parse_iso8601_duration(value: str | None) -> float | None:
    """Parse an ISO 8601 duration like 'PT3M31S' (YouTube's format) into seconds."""
    if not value:
        return None
    match = _ISO8601_DURATION.match(value)
    if not match:
        return None
    parts = match.groupdict()
    days = int(parts.get("days") or 0)
    hours = int(parts.get("hours") or 0)
    minutes = int(parts.get("minutes") or 0)
    seconds = float(parts.get("seconds") or 0.0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def probe_local_duration(video_path: str | Path) -> float | None:
    """Return the duration of a local video file via ffprobe.

    Returns None when ffprobe isn't available or the file can't be read; the
    caller should treat that as "unknown duration" rather than an error.
    """
    path = Path(video_path)
    if not path.exists():
        return None
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        return float(result.stdout.strip())
    except ValueError:
        return None
