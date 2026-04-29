"""End-to-end smoke for the typed-items / custom-variables / non-YT-items
surface added in migration 010 (Phases A–D backend).

Boots the FastAPI app against an isolated data dir, like
`test_app_smoke.py`, then exercises the new endpoints to make sure
schema, routing, validation, and inheritance behave as documented.
"""

from __future__ import annotations

import importlib
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


def test_global_variable_crud_round_trip(client: TestClient) -> None:
    assert client.get("/api/global-variables").json() == []

    resp = client.put(
        "/api/global-variables/signoff", json={"value": "-- Drew"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["key"] == "signoff"
    assert body["value"] == "-- Drew"

    listed = client.get("/api/global-variables").json()
    assert any(v["key"] == "signoff" for v in listed)

    # update existing
    resp = client.put(
        "/api/global-variables/signoff", json={"value": "-- Updated"}
    )
    assert resp.status_code == 200
    assert resp.json()["value"] == "-- Updated"

    # invalid key
    resp = client.put(
        "/api/global-variables/Bad-Key", json={"value": "x"}
    )
    assert resp.status_code == 400

    # delete
    resp = client.delete("/api/global-variables/signoff")
    assert resp.status_code == 200
    assert client.get("/api/global-variables").json() == []


def test_expand_text_merges_globals_and_caller_overrides(client: TestClient) -> None:
    """The generic /api/expand_text endpoint should pull in install-wide
    globals automatically; caller-passed variables override on collision."""
    client.put("/api/global-variables/signoff", json={"value": "-- global"})

    # global picked up
    resp = client.post(
        "/api/expand_text",
        json={"template": "Bye! {{signoff}}", "variables": {}},
    )
    assert resp.status_code == 200
    assert resp.json() == {"rendered": "Bye! -- global"}

    # caller wins
    resp = client.post(
        "/api/expand_text",
        json={
            "template": "Bye! {{signoff}}",
            "variables": {"signoff": "-- override"},
        },
    )
    assert resp.json() == {"rendered": "Bye! -- override"}


def test_create_project_with_kind_and_project_url(client: TestClient) -> None:
    resp = client.post(
        "/api/projects",
        json={
            "name": "AI Chess Machine",
            "slug": "ai-chess",
            "kind": "github",
            "project_url": "https://github.com/me/ai-chess",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["slug"] == "ai-chess"
    assert body["project_url"] == "https://github.com/me/ai-chess"
    assert body["youtube_channel_id"] is None  # social-only project

    # PATCH project_url
    resp = client.patch(
        "/api/projects/ai-chess",
        json={"project_url": "https://example.com/blog"},
    )
    assert resp.status_code == 200
    assert resp.json()["project_url"] == "https://example.com/blog"

    # clear via PATCH
    resp = client.patch("/api/projects/ai-chess", json={"project_url": ""})
    assert resp.status_code == 200
    assert resp.json()["project_url"] is None


def test_items_endpoint_creates_standalone(client: TestClient) -> None:
    resp = client.post(
        "/api/videos/items",
        data={
            "title": "Chess preview",
            "description": "Working on chess",
            "item_type": "standalone",
            "url": "https://github.com/me/ai-chess",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["item_type"] == "standalone"
    assert body["url"] == "https://github.com/me/ai-chess"
    video_id = body["video_id"]
    assert len(video_id) > 0
    # The id should NOT look like a YouTube id (which would be 11 chars).
    assert len(video_id) > 11

    # The item shows up via list
    resp = client.get(f"/api/videos/{video_id}")
    assert resp.status_code == 200


def test_items_rejects_episode_type(client: TestClient) -> None:
    resp = client.post(
        "/api/videos/items",
        data={"title": "x", "item_type": "episode"},
    )
    assert resp.status_code == 400
    assert "hook" in resp.json()["detail"].lower()
    assert "standalone" in resp.json()["detail"].lower()


def test_items_rejects_standalone_with_parent(client: TestClient) -> None:
    resp = client.post(
        "/api/videos/items",
        data={"title": "x", "item_type": "standalone", "parent_item_id": "fake"},
    )
    assert resp.status_code == 400


def test_upload_rejects_when_project_has_no_channel(client: TestClient) -> None:
    """Create a fresh project that we *know* has no YT channel, then try to
    upload. Don't use 'default' — `backfill_channel_ids` runs at app start
    and pulls real Keychain creds (which are install-wide and not isolated
    by `DYS_DATA_DIR`), so the default project on a developer's machine
    can come up with a real channel id bound. A fresh project we just
    created via the API has no creds in Keychain, so it stays clean."""
    proj = client.post(
        "/api/projects",
        json={
            "name": "GitHub Repo Project",
            "slug": "gh-test",
            "kind": "github",
            "project_url": "https://github.com/me/x",
        },
    ).json()
    assert proj["youtube_channel_id"] is None

    resp = client.post(
        "/api/videos/upload",
        data={
            "title": "x",
            "item_type": "episode",
            "project_slug": "gh-test",
        },
        files={"video_file": ("clip.mp4", b"\x00" * 16, "video/mp4")},
    )
    assert resp.status_code == 400, resp.text
    assert "youtube channel" in resp.json()["detail"].lower()


def test_item_variable_crud_round_trip(client: TestClient) -> None:
    # Need a real item to attach variables to.
    item = client.post(
        "/api/videos/items",
        data={"title": "test", "item_type": "standalone"},
    ).json()
    video_id = item["video_id"]

    assert client.get(f"/api/videos/{video_id}/variables").json() == []

    resp = client.put(
        f"/api/videos/{video_id}/variables/footer",
        json={"value": "Subscribe!"},
    )
    assert resp.status_code == 200
    assert resp.json()["key"] == "footer"

    listed = client.get(f"/api/videos/{video_id}/variables").json()
    assert any(v["key"] == "footer" and v["value"] == "Subscribe!" for v in listed)

    # 404 on unknown video
    resp = client.put(
        "/api/videos/no-such-video/variables/footer",
        json={"value": "x"},
    )
    assert resp.status_code == 404


def test_project_variable_crud_round_trip(client: TestClient) -> None:
    # Default project exists.
    resp = client.put(
        "/api/projects/default/variables/tag",
        json={"value": "podcast"},
    )
    assert resp.status_code == 200

    listed = client.get("/api/projects/default/variables").json()
    assert any(v["key"] == "tag" and v["value"] == "podcast" for v in listed)

    # 404 on unknown project
    resp = client.put(
        "/api/projects/no-such-project/variables/x", json={"value": "x"}
    )
    assert resp.status_code == 404
