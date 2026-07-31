"""The token-refresh sweep hands each poster its own lookahead window.

One global 45-minute window meant a 60-day Threads token — unrefreshable once
expired — was only renewed in the final 45 minutes of day 60; a sleeping
laptop during that window killed the credential permanently. The window is
platform behavior now: a poster class attribute backed by config constants.
"""

from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def sweep_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    await database.get_db()
    yield monkeypatch
    await database.close_db()


async def test_sweep_passes_each_poster_its_own_window(sweep_env) -> None:
    monkeypatch = sweep_env
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    social = importlib.import_module("yt_scheduler.services.social")
    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    config = importlib.import_module("yt_scheduler.config")

    received: dict[str, int] = {}

    class _StubPoster:
        def __init__(self, platform: str, window: int) -> None:
            self._platform = platform
            self.token_refresh_window_secs = window

        async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
            received[self._platform] = window_secs
            return False

    stubs = {
        "twitter": _StubPoster(
            "twitter", social.TwitterPoster.token_refresh_window_secs
        ),
        "threads": _StubPoster(
            "threads", social.ThreadsPoster.token_refresh_window_secs
        ),
    }

    async def fake_list_credentials(**_kw):
        return [
            {"uuid": "u-tw", "platform": "twitter", "label": "tw"},
            {"uuid": "u-th", "platform": "threads", "label": "th"},
        ]

    async def fake_get_poster(platform, _uuid):
        return stubs[platform]

    monkeypatch.setattr(creds_mod, "list_credentials", fake_list_credentials)
    monkeypatch.setattr(social, "get_poster_for_uuid", fake_get_poster)

    await scheduler.refresh_social_tokens_job()

    assert received["twitter"] == config.SOCIAL_TOKEN_REFRESH_WINDOW_SECONDS
    assert received["threads"] == config.THREADS_TOKEN_REFRESH_WINDOW_SECONDS


def test_window_constants_keep_their_relationships() -> None:
    """The Threads window must leave Meta's 24-hour minimum token age
    satisfied at refresh time, and every window must exceed the sweep
    interval or the due-moment can fall entirely between sweeps."""
    from yt_scheduler import config
    from yt_scheduler.services import scheduler, social

    sixty_days = social.ThreadsPoster._TOKEN_TTL_FALLBACK_SECONDS
    # 24h is Meta's documented minimum age before a token may be refreshed.
    assert config.THREADS_TOKEN_REFRESH_WINDOW_SECONDS + 24 * 3600 < sixty_days
    interval_secs = scheduler._TOKEN_REFRESH_INTERVAL_MINUTES * 60
    assert config.SOCIAL_TOKEN_REFRESH_WINDOW_SECONDS > interval_secs
    assert config.THREADS_TOKEN_REFRESH_WINDOW_SECONDS > interval_secs


def test_posters_carry_the_config_windows() -> None:
    from yt_scheduler import config
    from yt_scheduler.services import social

    assert (social.SocialPoster.token_refresh_window_secs
            == config.SOCIAL_TOKEN_REFRESH_WINDOW_SECONDS)
    assert (social.ThreadsPoster.token_refresh_window_secs
            == config.THREADS_TOKEN_REFRESH_WINDOW_SECONDS)
    assert (social.ThreadsPoster.token_refresh_window_secs
            > social.SocialPoster.token_refresh_window_secs)


def test_a_threads_token_is_due_inside_a_week_not_outside() -> None:
    from yt_scheduler import config
    from yt_scheduler.services import social

    poster = social.ThreadsPoster(bundle={})
    window = config.THREADS_TOKEN_REFRESH_WINDOW_SECONDS
    six_days = {"expires_at": int(time.time()) + 6 * 24 * 3600}
    eight_days = {"expires_at": int(time.time()) + 8 * 24 * 3600}
    assert poster._token_is_due(six_days, window) is True
    assert poster._token_is_due(eight_days, window) is False


async def test_sweep_skips_credentials_awaiting_reconnect(sweep_env) -> None:
    """A credential already flagged needs_reauth cannot be fixed by refresh —
    only a re-OAuth clears it — so the sweep must not retry it every 20 minutes
    forever."""
    monkeypatch = sweep_env
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    social = importlib.import_module("yt_scheduler.services.social")
    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")

    attempted: list[str] = []

    class _StubPoster:
        token_refresh_window_secs = 0

        async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
            attempted.append("called")
            return False

    async def fake_list_credentials(**_kw):
        return [
            {"uuid": "u-live", "platform": "twitter", "label": "live",
             "needs_reauth": False},
            {"uuid": "u-dead", "platform": "twitter", "label": "dead",
             "needs_reauth": True},
        ]

    got_uuids: list[str] = []

    async def fake_get_poster(platform, uuid):
        got_uuids.append(uuid)
        return _StubPoster()

    monkeypatch.setattr(creds_mod, "list_credentials", fake_list_credentials)
    monkeypatch.setattr(social, "get_poster_for_uuid", fake_get_poster)

    await scheduler.refresh_social_tokens_job()

    # Only the live credential was even resolved to a poster; the dead one was
    # skipped before any work.
    assert got_uuids == ["u-live"]
    assert attempted == ["called"]
