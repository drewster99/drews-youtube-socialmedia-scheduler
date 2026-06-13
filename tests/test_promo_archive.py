"""Archive / unarchive promo clips (migration 031) — non-destructive cleanup.

Archiving hides a clip from the Promo Videos page without deleting the row or
the YouTube video, and is fully reversible. Used to clear duplicates (e.g. an
imported clip that duplicates a generated one).
"""

from __future__ import annotations

import importlib
import sqlite3
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


def _insert(video_id: str, **cols) -> None:
    from yt_scheduler.config import DB_PATH, UPLOAD_DIR

    path = UPLOAD_DIR / f"{video_id}.mp4"
    path.write_bytes(b"\x00" * 16)
    row = {
        "project_id": 1,
        "title": video_id,
        "status": "ready",
        "video_file_path": str(path),
    }
    row.update(cols)
    keys = ", ".join(["id", *row.keys()])
    placeholders = ", ".join("?" for _ in range(1 + len(row)))
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            f"INSERT INTO videos ({keys}) VALUES ({placeholders})",
            (video_id, *row.values()),
        )
        conn.commit()


def _hook_ids(data: dict) -> set[str]:
    return {c["id"] for c in data["children"]["hook"]}


def test_archive_hides_clip_and_unarchive_restores(client: TestClient) -> None:
    _insert("parent01", item_type="episode", duration_seconds=600.0)
    _insert("childA", parent_item_id="parent01", item_type="hook",
            cut_start_seconds=10.0, cut_end_seconds=25.0)
    _insert("childB", parent_item_id="parent01", item_type="hook",
            cut_start_seconds=40.0, cut_end_seconds=55.0)
    base = "/api/projects/default/videos/parent01/promos"

    data = client.get(base).json()
    assert _hook_ids(data) == {"childA", "childB"}
    assert data["archived_count"] == 0

    r = client.post(f"{base}/childA/archive")
    assert r.status_code == 200 and r.json()["archived"] is True

    data = client.get(base).json()
    assert _hook_ids(data) == {"childB"}
    assert data["archived_count"] == 1
    # Active-only summary/readiness must not count the archived clip.
    assert data["summary"]["hook"] == 1
    assert data["readiness"]["hook"]["count"] == 1

    data = client.get(f"{base}?include_archived=1").json()
    archived = [c for c in data["children"]["hook"] if c["id"] == "childA"]
    assert archived and archived[0]["archived"] == 1

    r = client.post(f"{base}/childA/unarchive")
    assert r.status_code == 200 and r.json()["archived"] is False
    data = client.get(base).json()
    assert _hook_ids(data) == {"childA", "childB"}
    assert data["archived_count"] == 0


def test_archive_unknown_clip_is_404(client: TestClient) -> None:
    _insert("parent02", item_type="episode", duration_seconds=600.0)
    r = client.post("/api/projects/default/videos/parent02/promos/nope/archive")
    assert r.status_code == 404
