"""Outbound HTTP timeouts live in config.py, not at call sites.

The Threads publish path once ran on httpx's implicit 5-second default —
every surrounding call had an explicit budget, so the one bare client was
invisible until a working token met a real media post and died on
ReadTimeout mid-publish. Centralizing the budgets makes a missing one
greppable; these tests keep it that way.
"""

from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).parent.parent / "src" / "yt_scheduler"

# Every file whose outbound HTTP calls take their budgets from config.py.
CENTRALIZED_FILES = [
    SRC / "services" / "social.py",
    SRC / "services" / "media_hosting.py",
    SRC / "services" / "social_identity.py",
    SRC / "routers" / "oauth_routes.py",
]


def test_no_hardcoded_timeouts_at_call_sites() -> None:
    offenders = []
    for path in CENTRALIZED_FILES:
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if re.search(r"\btimeout\s*=\s*\d", line):
                offenders.append(f"{path.name}:{lineno}: {line.strip()}")
    assert offenders == [], (
        "Outbound HTTP timeouts belong in config.py's 'Outbound HTTP call "
        f"budgets' section, not at call sites: {offenders}"
    )


def test_api_call_timeouts_share_the_default() -> None:
    """The per-call names exist so a call site reads clearly and a single
    value can later diverge deliberately — but until one does, they must all
    equal the default the user set."""
    from yt_scheduler import config

    assert config.DEFAULT_API_CALL_TIMEOUT_SECONDS == 120
    for name in (
        "TWITTER_BEARER_REFRESH_TIMEOUT_SECONDS",
        "TWITTER_SIMPLE_UPLOAD_TIMEOUT_SECONDS",
        "BLUESKY_POST_TIMEOUT_SECONDS",
        "BLUESKY_BLOB_UPLOAD_TIMEOUT_SECONDS",
        "MASTODON_INSTANCE_PROBE_TIMEOUT_SECONDS",
        "LINKEDIN_MEDIA_UPLOAD_TIMEOUT_SECONDS",
        "LINKEDIN_POST_TIMEOUT_SECONDS",
        "THREADS_POST_TIMEOUT_SECONDS",
        "THREADS_TOKEN_REFRESH_TIMEOUT_SECONDS",
        "THREADS_VERIFY_TIMEOUT_SECONDS",
        "THREADS_USERINFO_TIMEOUT_SECONDS",
        "MEDIA_HOSTING_CONNECTION_TEST_TIMEOUT_SECONDS",
        "OAUTH_EXCHANGE_TIMEOUT_SECONDS",
        "USERNAME_RESOLVE_TIMEOUT_SECONDS",
    ):
        assert getattr(config, name) == config.DEFAULT_API_CALL_TIMEOUT_SECONDS, name


def test_bulk_transfer_budgets_keep_their_own_values() -> None:
    """Deliberately not tied to the default: a 512 MB video on a slow link is
    not "one API call"."""
    from yt_scheduler import config

    assert config.TWITTER_CHUNKED_UPLOAD_TIMEOUT_SECONDS == 120
    assert config.TWITTER_VIDEO_UPLOAD_CHUNK_BYTES == 4 * 1024 * 1024
    assert config.MEDIA_HOSTING_UPLOAD_TIMEOUT_SECONDS == 30 * 60
    assert config.MEDIA_HOSTING_UPLOAD_CHUNK_BYTES == 64 * 1024 * 1024
