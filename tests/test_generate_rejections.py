"""Rejection persistence + Restore round-trip for the Generate review page.

Migration 028 backs a generate_rejections table that the review page
populates when the user un-checks a proposal and clicks "Cut & insert
selected". The next visit reads from this table to render the
"Previously dismissed" section. Restore deletes the row.
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


def _insert_parent(video_id: str, duration: float = 600.0) -> None:
    from yt_scheduler.config import DB_PATH, UPLOAD_DIR

    path = UPLOAD_DIR / f"{video_id}.mp4"
    path.write_bytes(b"\x00" * 32)
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            "INSERT INTO videos (id, project_id, title, status, "
            "duration_seconds, item_type, video_file_path) "
            "VALUES (?, 1, ?, 'ready', ?, 'episode', ?)",
            (video_id, "Parent", duration, str(path)),
        )
        conn.commit()


def test_migration_028_applies(client: TestClient):
    """Migration runner picks up the new SQL and creates the table.

    The ``client`` fixture provisions the isolated DB and triggers the
    app's lifespan which runs migrations — without it, this test would
    read the user's real database via the default config.
    """
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        cur = conn.execute("PRAGMA table_info(generate_rejections)")
        cols = {r[1] for r in cur.fetchall()}
        assert "parent_id" in cols
        assert "kind" in cols
        assert "x_shift_normalized" in cols


def test_list_rejections_empty(client: TestClient):
    _insert_parent("PARENTNO0001")
    resp = client.get(
        "/api/projects/default/videos/PARENTNO0001/promos/generate/rejections",
    )
    assert resp.status_code == 200
    assert resp.json() == {"rejections": []}


def test_confirm_persists_rejected_entries(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """When the user clicks Cut & insert with both accepted and rejected
    entries, the rejected ones land in generate_rejections and the
    list endpoint returns them."""
    from yt_scheduler.services import auto_actions

    async def fake_start(**kwargs):
        return "job_fake_1"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTRJ0001")

    resp = client.post(
        "/api/projects/default/videos/PARENTRJ0001/promos/generate/confirm",
        json={
            "accepted": [
                {"kind": "hook", "start_seconds": 5, "end_seconds": 20,
                 "title": "kept"},
            ],
            "rejected": [
                {"kind": "hook", "start_seconds": 100, "end_seconds": 115,
                 "title": "dropped", "reason": "boring",
                 "x_shift_normalized": 0.3,
                 "crop_classification": "off_center",
                 "crop_confidence": 0.7},
                {"kind": "short", "start_seconds": 200, "end_seconds": 250,
                 "title": "also dropped", "reason": "covered later"},
            ],
        },
    )
    assert resp.status_code == 200, resp.text

    # List the rejections back.
    resp2 = client.get(
        "/api/projects/default/videos/PARENTRJ0001/promos/generate/rejections",
    )
    assert resp2.status_code == 200
    rejections = resp2.json()["rejections"]
    assert len(rejections) == 2
    titles = sorted(r["title"] for r in rejections)
    assert titles == ["also dropped", "dropped"]
    # Optional assessment fields round-trip.
    hook = next(r for r in rejections if r["title"] == "dropped")
    assert hook["x_shift_normalized"] == 0.3
    assert hook["crop_classification"] == "off_center"
    assert hook["duration_seconds"] == pytest.approx(15.0)


def test_confirm_rejected_dedupes_on_repeat(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """Re-rejecting the same range UPDATE-replaces instead of producing
    duplicate rows (unique constraint on parent/project/kind/start/end)."""
    from yt_scheduler.services import auto_actions

    async def fake_start(**kwargs):
        return "job_fake_1"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTRJ0002")

    body = {
        "accepted": [
            {"kind": "hook", "start_seconds": 5, "end_seconds": 20,
             "title": "kept"},
        ],
        "rejected": [
            {"kind": "hook", "start_seconds": 100, "end_seconds": 115,
             "title": "v1"},
        ],
    }
    client.post(
        "/api/projects/default/videos/PARENTRJ0002/promos/generate/confirm",
        json=body,
    )
    # Second confirm rejects the same range with a different title;
    # row count stays at 1, title is now v2.
    body["rejected"][0]["title"] = "v2"
    client.post(
        "/api/projects/default/videos/PARENTRJ0002/promos/generate/confirm",
        json=body,
    )
    resp = client.get(
        "/api/projects/default/videos/PARENTRJ0002/promos/generate/rejections",
    )
    rejections = resp.json()["rejections"]
    assert len(rejections) == 1
    assert rejections[0]["title"] == "v2"


def test_restore_round_trip(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """DELETE /generate/rejections/{id} removes the row (Restore)."""
    from yt_scheduler.services import auto_actions

    async def fake_start(**kwargs):
        return "job_fake_1"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTRJ0003")

    client.post(
        "/api/projects/default/videos/PARENTRJ0003/promos/generate/confirm",
        json={
            "accepted": [
                {"kind": "hook", "start_seconds": 5, "end_seconds": 20,
                 "title": "kept"},
            ],
            "rejected": [
                {"kind": "hook", "start_seconds": 100, "end_seconds": 115,
                 "title": "dropped"},
            ],
        },
    )
    rid = client.get(
        "/api/projects/default/videos/PARENTRJ0003/promos/generate/rejections",
    ).json()["rejections"][0]["id"]

    # Restore it.
    resp = client.delete(
        f"/api/projects/default/videos/PARENTRJ0003/promos/generate/rejections/{rid}",
    )
    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}

    # List is now empty.
    rejections = client.get(
        "/api/projects/default/videos/PARENTRJ0003/promos/generate/rejections",
    ).json()["rejections"]
    assert rejections == []


def test_restore_rejection_belonging_to_other_parent_blocked(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """A rejection_id from one parent's bucket can't be deleted via
    another parent's URL — defense against slug-confused / cross-tab
    requests."""
    from yt_scheduler.services import auto_actions

    async def fake_start(**kwargs):
        return "job_fake_1"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTRJ0004")
    _insert_parent("PARENTRJ0005")

    client.post(
        "/api/projects/default/videos/PARENTRJ0004/promos/generate/confirm",
        json={
            "accepted": [{"kind": "hook", "start_seconds": 5,
                          "end_seconds": 20, "title": "kept"}],
            "rejected": [{"kind": "hook", "start_seconds": 100,
                          "end_seconds": 115, "title": "dropped"}],
        },
    )
    rid = client.get(
        "/api/projects/default/videos/PARENTRJ0004/promos/generate/rejections",
    ).json()["rejections"][0]["id"]

    # Try to delete via the OTHER parent's URL.
    resp = client.delete(
        f"/api/projects/default/videos/PARENTRJ0005/promos/generate/rejections/{rid}",
    )
    assert resp.status_code == 200
    assert resp.json() == {"deleted": False}

    # Original rejection is still there.
    rejections = client.get(
        "/api/projects/default/videos/PARENTRJ0004/promos/generate/rejections",
    ).json()["rejections"]
    assert len(rejections) == 1


def test_review_page_renders(client: TestClient):
    """The full-screen review page loads with the picker visible."""
    _insert_parent("PARENTRJ0006")
    resp = client.get(
        "/projects/default/videos/PARENTRJ0006/promos/generate",
    )
    assert resp.status_code == 200
    body = resp.text
    # Picker is the initial visible state.
    assert "Pick what to propose" in body
    assert "gen-kind-hook" in body
    assert "gen-confirm" in body  # confirm button id rendered
    # Rejection section is in the markup (hidden until populated).
    assert "gen-rejected-wrap" in body
