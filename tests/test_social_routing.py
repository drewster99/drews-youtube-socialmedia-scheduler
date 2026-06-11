"""Regression tests for social-post account routing.

A post must never be silently routed to the default project's credentials when
its own project can't be resolved (that would post from the wrong account), and
a bundle-less poster must resolve a credential deterministically rather than
merging keys across accounts.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    social_routes = importlib.import_module("yt_scheduler.routers.social_routes")
    social = importlib.import_module("yt_scheduler.services.social")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield social_routes, social, db
    await database.close_db()


async def test_project_id_raises_when_video_link_missing(env):
    social_routes, _social, db = env
    # A post whose JOIN to videos returns nothing must raise, not default to
    # project 1 (which would route the send to the wrong account).
    with pytest.raises(ValueError):
        await social_routes._project_id_for_post(db, {"id": 999999})


async def test_bundleless_poster_no_credential_returns_empty(env):
    _social_routes, social, _db = env
    # No credential configured: _get_creds returns {} (so is_configured() is
    # False) rather than merging keys from unrelated accounts.
    poster = social.TwitterPoster()  # bundle=None
    creds = await poster._get_creds()
    assert creds == {}
    assert await poster.is_configured() is False
