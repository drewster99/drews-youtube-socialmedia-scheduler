"""End-to-end smoke test: app boots, migrations apply, key pages render."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Boot the FastAPI app against an isolated data dir."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    # StaticFiles validates its directory at module-import time, so the upload
    # dir must exist before we import the app.
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    # Clear cached modules so config picks up the env var.
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


def test_home_renders(client: TestClient) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Home" in resp.text
    assert "Projects" in resp.text


def test_default_project_dashboard_renders(client: TestClient) -> None:
    resp = client.get("/projects/default")
    assert resp.status_code == 200
    assert "Default" in resp.text
    assert "Dashboard" in resp.text


def test_project_settings_page_renders(client: TestClient) -> None:
    """Catches template-syntax errors in project_settings.html (Jinja2
    parses the file at request time; a stray ``{{`` in inlined JS
    would 500 the page without this smoke test catching it)."""
    resp = client.get("/projects/default/settings")
    assert resp.status_code == 200, resp.text
    assert "LLM prompt templates" in resp.text


def test_project_templates_page_renders(client: TestClient) -> None:
    resp = client.get("/projects/default/templates")
    assert resp.status_code == 200, resp.text


def test_project_moderation_page_renders(client: TestClient) -> None:
    resp = client.get("/projects/default/moderation")
    assert resp.status_code == 200, resp.text


def test_unknown_project_404(client: TestClient) -> None:
    resp = client.get("/projects/does-not-exist")
    assert resp.status_code == 404


def test_legacy_video_url_redirects(client: TestClient) -> None:
    resp = client.get("/videos/abc123", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/projects/default/videos/abc123"


def test_api_lists_default_project(client: TestClient) -> None:
    resp = client.get("/api/projects")
    assert resp.status_code == 200
    data = resp.json()
    assert any(p["slug"] == "default" for p in data)


def test_api_creates_and_renames_project(client: TestClient) -> None:
    resp = client.post("/api/projects", json={"name": "Demo Channel"})
    assert resp.status_code == 200
    project = resp.json()
    assert project["slug"] == "demo-channel"

    resp = client.patch("/api/projects/demo-channel", json={"name": "Demo Renamed"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Demo Renamed"
    assert resp.json()["slug"] == "demo-channel"  # immutable


def test_api_rejects_default_delete(client: TestClient) -> None:
    resp = client.delete("/api/projects/default")
    assert resp.status_code == 400


def test_video_events_endpoint_returns_log(client: TestClient) -> None:
    """Insert a video + a couple of events and verify they're served back."""
    import asyncio
    from yt_scheduler.database import get_db
    from yt_scheduler.services import events as events_service

    async def seed():
        db = await get_db()
        await db.execute(
            "INSERT INTO videos (id, project_id, title) VALUES ('vidX', 1, 'Hello')"
        )
        await db.commit()
        await events_service.record_event("vidX", "uploaded", {"platform": "youtube"})
        await events_service.record_event(
            "vidX",
            "metadata_updated",
            {"title": {"old": "Hello", "new": "Hi"}},
        )

    asyncio.new_event_loop().run_until_complete(seed())

    resp = client.get("/api/videos/vidX/events")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 2
    assert rows[0]["type"] == "metadata_updated"
    assert rows[0]["payload"] == {"title": {"old": "Hello", "new": "Hi"}}


def test_video_detail_page_renders(client: TestClient) -> None:
    resp = client.get("/projects/default/videos/vidX")
    assert resp.status_code == 200
    assert "Update to YouTube" in resp.text
    assert "Revert changes" in resp.text
    assert "YouTube metadata" in resp.text
    # Removed: top "Generate Description" button + "Generated Description" tab
    assert "Generate Description</button>" not in resp.text
    assert "Generated Description" not in resp.text
