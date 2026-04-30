"""Regression test for the partial-platform regenerate bug.

Repro of the original bug:
1. User generated + approved a Twitter post.
2. User came back, regenerated only the OTHER 3 platforms (linkedin,
   mastodon, bluesky).
3. The Twitter row was hard-deleted by the unscoped DELETE in
   ``generate-posts/{video_id}``.

The fix scopes the DELETE (and the scheduled-overwrite guard) to the
platforms actually being regenerated. This test exercises that path
directly via the FastAPI route.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-fake")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")

    from fastapi.testclient import TestClient
    with TestClient(app_module.app) as c:
        yield c

    from yt_scheduler.database import close_db
    await close_db()


async def _seed_video_and_existing_posts(c) -> str:
    """Insert a video + a single approved Twitter post (the historical
    state from step 1 of the user's repro). Returns the video_id."""
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, item_type) "
        "VALUES ('vidPR', 1, 'Partial Regen Test', 'uploaded', 'episode')"
    )
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('vidPR', 'twitter', 'pre-approved tweet body', 'approved')"
    )
    await db.commit()
    return "vidPR"


async def test_partial_regenerate_preserves_other_platforms(client) -> None:
    """Generating only ['bluesky'] must leave the previously-approved
    Twitter row intact."""
    video_id = await _seed_video_and_existing_posts(client)

    # Default project has the announce_video template seeded with all 5
    # platform slots (twitter, bluesky, mastodon, linkedin, threads).
    resp = client.post(
        f"/api/social/generate-posts/{video_id}",
        json={"template_name": "announce_video", "platforms": ["bluesky"]},
    )
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cursor = await db.execute(
        "SELECT platform, status, content FROM social_posts "
        "WHERE video_id = ? ORDER BY platform",
        (video_id,),
    )
    rows = [dict(r) for r in await cursor.fetchall()]

    by_platform = {r["platform"]: r for r in rows}

    # The Twitter row from step 1 must survive untouched.
    assert "twitter" in by_platform, (
        "regenerating only bluesky must NOT delete the existing twitter row"
    )
    assert by_platform["twitter"]["status"] == "approved"
    assert by_platform["twitter"]["content"] == "pre-approved tweet body"

    # Bluesky must have been freshly generated (status='draft' from the
    # INSERT in the route).
    assert "bluesky" in by_platform
    assert by_platform["bluesky"]["status"] == "draft"

    # No other platforms should have been touched.
    assert set(by_platform.keys()) == {"twitter", "bluesky"}


async def test_full_regenerate_still_replaces_all_unsent(client) -> None:
    """Sanity: when no platform filter is supplied (i.e. the user wants
    to regenerate everything), the route still replaces all unsent rows
    on every platform the template covers — i.e. the original behaviour
    is preserved when the user actually wants it."""
    video_id = await _seed_video_and_existing_posts(client)

    resp = client.post(
        f"/api/social/generate-posts/{video_id}",
        json={"template_name": "announce_video"},
    )
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cursor = await db.execute(
        "SELECT platform, status FROM social_posts WHERE video_id = ?",
        (video_id,),
    )
    rows = [dict(r) for r in await cursor.fetchall()]

    # All 5 announce_video slots replaced; nothing carried over from the
    # previous approved state because the user asked for a full regen.
    assert {r["platform"] for r in rows} == {
        "twitter", "bluesky", "mastodon", "linkedin", "threads",
    }
    for r in rows:
        assert r["status"] == "draft", (
            f"expected fresh draft on {r['platform']}, got {r['status']}"
        )
