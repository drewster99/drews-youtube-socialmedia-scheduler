"""GET /api/videos/{id} auto-syncs the local row from YouTube every
fetch. When title / description / tags / privacy_status drift on
YouTube (the user edited them in Studio, another tool changed them,
etc.) we overwrite the local row so the page always shows current
truth.

Mocked entirely — no real YouTube calls. Drives ``youtube.get_video``
via monkeypatch.
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
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


@pytest.fixture
def make_yt_payload():
    """Build a YouTube videos.list response item with the four fields
    we care about (title, description, tags, privacyStatus)."""
    def factory(*, title: str, description: str, tags: list[str], privacy: str):
        return {
            "snippet": {"title": title, "description": description, "tags": tags},
            "status": {"privacyStatus": privacy},
            "contentDetails": {"duration": "PT5M"},
            "statistics": {"viewCount": "0"},
        }
    return factory


async def _seed_video(monkeypatch, *, video_id: str, title: str, description: str,
                       tags: list[str], privacy: str) -> None:
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, description, tags, "
        " privacy_status, status, imported_from_youtube) "
        "VALUES (?, 1, ?, ?, ?, ?, 'uploaded', 1)",
        (video_id, title, description, json.dumps(tags), privacy),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_drift_pulls_youtube_values_into_local(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, make_yt_payload,
) -> None:
    """Title diverges on YouTube → GET overwrites local title."""
    await _seed_video(
        monkeypatch,
        video_id="SYNC0000001",
        title="Old title",
        description="Old description",
        tags=["a", "b"],
        privacy="unlisted",
    )

    from yt_scheduler.routers import video_routes
    monkeypatch.setattr(
        video_routes.youtube, "get_video",
        lambda vid: make_yt_payload(
            title="New title", description="Old description",
            tags=["a", "b"], privacy="unlisted",
        ),
    )

    resp = client.get("/api/videos/SYNC0000001")
    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == "New title"
    # Untouched fields stay put.
    assert body["description"] == "Old description"
    assert json.loads(body["tags"]) == ["a", "b"]
    assert body["privacy_status"] == "unlisted"


@pytest.mark.asyncio
async def test_no_diff_noops(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, make_yt_payload,
) -> None:
    """When everything matches, no UPDATE runs and no fields change."""
    await _seed_video(
        monkeypatch,
        video_id="SYNC0000002",
        title="Same title",
        description="Same description",
        tags=["x"],
        privacy="public",
    )
    from yt_scheduler.routers import video_routes
    monkeypatch.setattr(
        video_routes.youtube, "get_video",
        lambda vid: make_yt_payload(
            title="Same title", description="Same description",
            tags=["x"], privacy="public",
        ),
    )

    resp = client.get("/api/videos/SYNC0000002")
    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == "Same title"
    assert body["privacy_status"] == "public"


@pytest.mark.asyncio
async def test_tags_drift_syncs(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, make_yt_payload,
) -> None:
    """Tag list edited on YouTube must overwrite the local JSON column."""
    await _seed_video(
        monkeypatch,
        video_id="SYNC0000003",
        title="t", description="d", tags=["old1", "old2"], privacy="unlisted",
    )
    from yt_scheduler.routers import video_routes
    monkeypatch.setattr(
        video_routes.youtube, "get_video",
        lambda vid: make_yt_payload(
            title="t", description="d", tags=["new1"], privacy="unlisted",
        ),
    )

    body = client.get("/api/videos/SYNC0000003").json()
    assert json.loads(body["tags"]) == ["new1"]


@pytest.mark.asyncio
async def test_youtube_error_leaves_local_untouched(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A YouTube fetch failure surfaces youtube_data_error but never
    overwrites local fields with empty/garbage values."""
    await _seed_video(
        monkeypatch,
        video_id="SYNC0000004",
        title="Local title",
        description="Local desc",
        tags=["keep"],
        privacy="unlisted",
    )

    from yt_scheduler.routers import video_routes

    def boom(vid):
        raise RuntimeError("YouTube is down")

    monkeypatch.setattr(video_routes.youtube, "get_video", boom)

    body = client.get("/api/videos/SYNC0000004").json()
    assert body["title"] == "Local title"
    assert body["description"] == "Local desc"
    assert json.loads(body["tags"]) == ["keep"]
    assert body["privacy_status"] == "unlisted"
    assert body.get("youtube_data_error") == "YouTube is down"


@pytest.mark.asyncio
async def test_diff_helper_returns_empty_when_aligned(
    monkeypatch: pytest.MonkeyPatch, make_yt_payload,
) -> None:
    """Direct unit test of _diff_youtube_metadata — sanity-checks the
    comparison logic without going through HTTP."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    from yt_scheduler.routers.video_routes import _diff_youtube_metadata

    yt = make_yt_payload(title="a", description="b", tags=["x"], privacy="public")
    local = {
        "title": "a", "description": "b",
        "tags": json.dumps(["x"]), "privacy_status": "public",
    }
    assert _diff_youtube_metadata(local, yt) == {}

    # Drift on every field
    yt2 = make_yt_payload(title="A2", description="B2", tags=["y"], privacy="private")
    out = _diff_youtube_metadata(local, yt2)
    assert out["title"] == "A2"
    assert out["description"] == "B2"
    assert json.loads(out["tags"]) == ["y"]
    assert out["privacy_status"] == "private"
