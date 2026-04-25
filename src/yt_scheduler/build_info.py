"""Build identity surfaced via ``GET /api/build`` and the X-DYS-Build-Id header.

The actual values come from a ``_build_info.py`` module that ``macos/build.sh``
generates *into the bundle* — it's never checked into source. When running
outside the bundle (terminal ``yt-scheduler``, pytest), the import fails and
we generate a fresh UUID per process start, so every server restart trips
the mismatch banner in any open browser tab — matching the production
behaviour of "different build → reload required".
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

try:
    from yt_scheduler._build_info import (  # type: ignore[import-not-found]
        BUILD_DATE,
        BUILD_ID,
        BUILD_KIND,
        BUILD_NUMBER,
        VERSION,
    )
except ImportError:
    # Running from source. Each process start gets a fresh UUID, so any tab
    # loaded against the old server sees a mismatch on its next response and
    # surfaces the "reload required" banner.
    BUILD_KIND = "debug"
    VERSION = "0.0.0-dev"
    BUILD_NUMBER = "0"
    BUILD_DATE = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
    BUILD_ID = f"dev-{uuid.uuid4().hex[:12]}"


# Ad-hoc override (e.g. ``DYS_BUILD_ID=$(uuidgen) yt-scheduler``) for tests
# or stress-testing the mismatch path with a known id.
BUILD_ID = os.getenv("DYS_BUILD_ID") or BUILD_ID


def as_dict() -> dict[str, str]:
    return {
        "kind": BUILD_KIND,
        "version": VERSION,
        "build_number": BUILD_NUMBER,
        "build_date": BUILD_DATE,
        "build_id": BUILD_ID,
    }
