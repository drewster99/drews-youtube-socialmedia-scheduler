"""PUT /api/videos/{id} must keep the lifecycle ``status`` column in
sync with ``privacy_status`` transitions when the caller doesn't pass
``status`` explicitly.

Regression: editing the Privacy dropdown to "Public" used to write
``privacy_status='public'`` to the DB without bumping ``status`` to
'published'. Downstream consumers that gate on both columns (project
counts, the per-post send precheck) then saw contradictory state and
the user got "YouTube video is public, not public" errors when
APScheduler fired a queued social post.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


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
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


def _mock_youtube(monkeypatch, *, privacy: str) -> None:
    """Make update_video_metadata a no-op and have the readback report
    ``privacy`` so the route's "trust YouTube" branch fires."""
    from yt_scheduler.routers import video_routes
    monkeypatch.setattr(
        video_routes.youtube, "update_video_metadata",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        video_routes.youtube, "get_video",
        lambda vid: {
            "snippet": {"title": "T", "description": "D", "tags": []},
            "status": {"privacyStatus": privacy, "publishAt": None},
        },
    )


async def _seed_video(*, video_id: str, status: str, privacy: str) -> None:
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, privacy_status) "
        "VALUES (?, 1, 'T', ?, ?)",
        (video_id, status, privacy),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_privacy_flip_to_public_bumps_status_to_published(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_video(video_id="SYNCSTATUS1", status="ready", privacy="unlisted")
    _mock_youtube(monkeypatch, privacy="public")

    resp = client.put("/api/videos/SYNCSTATUS1", json={"privacy_status": "public"})
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cursor = await db.execute(
        "SELECT privacy_status, status FROM videos WHERE id = ?",
        ("SYNCSTATUS1",),
    )
    row = await cursor.fetchone()
    assert row["privacy_status"] == "public"
    assert row["status"] == "published", (
        "Lifecycle status must follow privacy when the caller didn't "
        "explicitly pass status — otherwise the per-post send gate sees "
        "drift and rejects with 'video is public, not public'."
    )


@pytest.mark.asyncio
async def test_privacy_flip_from_public_to_unlisted_drops_status_to_ready(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_video(video_id="SYNCSTATUS2", status="published", privacy="public")
    _mock_youtube(monkeypatch, privacy="unlisted")

    resp = client.put("/api/videos/SYNCSTATUS2", json={"privacy_status": "unlisted"})
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cursor = await db.execute(
        "SELECT privacy_status, status FROM videos WHERE id = ?",
        ("SYNCSTATUS2",),
    )
    row = await cursor.fetchone()
    assert row["privacy_status"] == "unlisted"
    assert row["status"] == "ready", (
        "Pulling a published video back to unlisted leaves it in a "
        "non-published state; status='published' would lie."
    )


@pytest.mark.asyncio
async def test_explicit_status_in_payload_wins(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the caller explicitly sends ``status``, the sync logic must
    not override it. Otherwise the publish_video_job atomic write
    (``status='published'`` + ``privacy_status='public'``) couldn't
    distinguish itself from a passive metadata edit."""
    await _seed_video(video_id="SYNCSTATUS3", status="ready", privacy="unlisted")
    _mock_youtube(monkeypatch, privacy="public")

    resp = client.put(
        "/api/videos/SYNCSTATUS3",
        json={"privacy_status": "public", "status": "scheduled"},
    )
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cursor = await db.execute(
        "SELECT status FROM videos WHERE id = ?", ("SYNCSTATUS3",),
    )
    row = await cursor.fetchone()
    assert row["status"] == "scheduled"


@pytest.mark.asyncio
async def test_no_privacy_change_does_not_touch_status(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Editing just the title on a published video must not pull status
    backward — privacy_status didn't change, so neither does lifecycle."""
    await _seed_video(video_id="SYNCSTATUS4", status="published", privacy="public")
    _mock_youtube(monkeypatch, privacy="public")

    resp = client.put("/api/videos/SYNCSTATUS4", json={"title": "New title"})
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cursor = await db.execute(
        "SELECT status FROM videos WHERE id = ?", ("SYNCSTATUS4",),
    )
    row = await cursor.fetchone()
    assert row["status"] == "published"
