"""The app-wide failed-sends surface: GET /api/social/failed-posts.

A failed send used to exist only as an 8-second toast and a badge on the one
page that owns the post; scheduled sends could fail with no page open at all.
The banner in base.html polls this endpoint from every page, so the endpoint's
contract — only failed posts, newest first, carrying the real error — is what
keeps failures visible.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)

    app_module = importlib.import_module("yt_scheduler.app")

    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as test_client:
        yield test_client


async def _seed_posts() -> None:
    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidA', 1, 'My Video', 'uploaded')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status, error) "
        "VALUES (1, 'vidA', 'threads', 'older failure', 'failed', 'boom one')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status) "
        "VALUES (2, 'vidA', 'bluesky', 'went out fine', 'posted')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status) "
        "VALUES (3, 'vidA', 'mastodon', 'not sent yet', 'draft')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status, error) "
        "VALUES (4, 'vidA', 'threads', 'newer failure', 'failed', 'boom two')"
    )
    await db.commit()


async def test_only_failed_posts_are_listed_newest_first(client) -> None:
    await _seed_posts()

    resp = client.get("/api/social/failed-posts")
    assert resp.status_code == 200
    posts = resp.json()

    assert [p["id"] for p in posts] == [4, 1]
    newest = posts[0]
    assert newest["platform"] == "threads"
    assert newest["error"] == "boom two"
    assert newest["video_id"] == "vidA"
    assert newest["video_title"] == "My Video"
    # The server vends the ready page link, slug included — the detail route
    # 404s unless the slug actually owns the video, so the client never guesses.
    assert newest["page_url"].startswith("/projects/")
    assert newest["page_url"].endswith("/videos/vidA")


async def test_empty_when_nothing_failed(client) -> None:
    resp = client.get("/api/social/failed-posts")
    assert resp.status_code == 200
    assert resp.json() == []
