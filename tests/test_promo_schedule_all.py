"""Tests for the Promo Videos schedule-all flow.

Covers the preview / commit endpoints, readiness gating, per-tier
chain math, and quota warning.

The actual APScheduler write path is exercised via the existing
schedule_publish tests; here we just verify the new helpers produce
the right shape and call schedule_publish for each row.
"""

from __future__ import annotations

import importlib
import json
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


def _insert_ready_video(
    video_id: str,
    *,
    title: str = "Test",
    parent_item_id: str | None = None,
    item_type: str = "episode",
    publish_at: str | None = None,
    transcript_text: str = "Some transcript text",
    description: str = "A real description.",
    tags: list[str] | None = None,
    thumbnail_path: str | None = "/tmp/thumb.jpg",
    thumbnail_source: str | None = None,
    status: str = "uploaded",
) -> None:
    """Insert a videos row that passes the readiness check by default.
    Caller passes empty strings / None to make specific fields fail."""
    from yt_scheduler.config import DB_PATH

    tag_json = json.dumps(tags if tags is not None else ["one", "two", "three"])
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            """INSERT INTO videos (id, project_id, title, description, tags,
               privacy_status, status, item_type, parent_item_id,
               publish_at, transcript, thumbnail_path, thumbnail_source,
               url, tier)
            VALUES (?, 1, ?, ?, ?, 'unlisted', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                video_id, title, description, tag_json, status, item_type,
                parent_item_id, publish_at, transcript_text, thumbnail_path,
                thumbnail_source, f"https://youtu.be/{video_id}",
                item_type if item_type != "episode" else None,
            ),
        )
        conn.commit()


def test_readiness_check_helper_passes_full_row() -> None:
    """Pure helper; doesn't need the TestClient fixture."""
    from yt_scheduler.services.scheduler import is_ready_for_schedule

    ready, missing = is_ready_for_schedule({
        "transcript": "hello world",
        "description": "A real description",
        "tags": json.dumps(["a", "b", "c"]),
        "thumbnail_path": "/tmp/x.jpg",
    })
    assert ready
    assert missing == []


def test_readiness_check_flags_each_missing_field() -> None:
    from yt_scheduler.services.scheduler import is_ready_for_schedule

    ready, missing = is_ready_for_schedule({
        "transcript": "  ",
        "description": "",
        "tags": json.dumps(["one", "two"]),
        "thumbnail_path": None,
        "thumbnail_source": None,
    })
    assert not ready
    assert "transcript" in missing
    assert "description" in missing
    assert any("tags" in m for m in missing)
    assert "thumbnail" in missing


def test_readiness_accepts_youtube_thumbnail_source() -> None:
    from yt_scheduler.services.scheduler import is_ready_for_schedule

    ready, _ = is_ready_for_schedule({
        "transcript": "x",
        "description": "y",
        "tags": json.dumps(["a", "b", "c"]),
        "thumbnail_path": None,
        "thumbnail_source": "youtube",
    })
    assert ready


def test_preview_requires_parent_publish_or_existing(client: TestClient) -> None:
    _insert_ready_video("parentid001", publish_at=None, status="uploaded")
    _insert_ready_video(
        "child000001", parent_item_id="parentid001", item_type="short",
    )
    resp = client.get(
        "/api/projects/default/videos/parentid001/promos/schedule-all/preview"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["anchor_publish_at"] is None
    assert any("publish time" in w.lower() for w in data["warnings"])


def test_preview_computes_per_tier_chains(client: TestClient) -> None:
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video(
        "parentid002", publish_at=parent_iso, status="scheduled",
    )
    _insert_ready_video(
        "hookchild01", parent_item_id="parentid002", item_type="hook",
    )
    _insert_ready_video(
        "shrtchild01", parent_item_id="parentid002", item_type="short",
    )
    _insert_ready_video(
        "segchild001", parent_item_id="parentid002", item_type="segment",
    )

    resp = client.get(
        "/api/projects/default/videos/parentid002/promos/schedule-all/preview"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["anchor_publish_at"] is not None
    targets = {r["video_id"]: r["target_time"] for r in data["rows"]}
    # Per the spec: hook +4h, short +18h, segment +3d.
    from datetime import datetime, timedelta
    parent_dt = datetime.fromisoformat(parent_iso)
    assert datetime.fromisoformat(targets["hookchild01"]) == parent_dt + timedelta(hours=4)
    assert datetime.fromisoformat(targets["shrtchild01"]) == parent_dt + timedelta(hours=18)
    assert datetime.fromisoformat(targets["segchild001"]) == parent_dt + timedelta(days=3)


def test_preview_blocks_when_child_missing_fields(client: TestClient) -> None:
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video(
        "parentid003", publish_at=parent_iso, status="scheduled",
    )
    _insert_ready_video(
        "missingtag1", parent_item_id="parentid003", item_type="hook",
        tags=[],
    )
    resp = client.get(
        "/api/projects/default/videos/parentid003/promos/schedule-all/preview"
    )
    data = resp.json()
    row = next(r for r in data["rows"] if r["video_id"] == "missingtag1")
    assert row["ready"] is False
    assert any("tag" in m for m in row["missing"])


def test_quota_warning_surfaced_for_large_batches(client: TestClient) -> None:
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video(
        "parentid004", publish_at=parent_iso, status="scheduled",
    )
    # 17 children × 150 quota = 2,550 (~25.5% of daily 10k) → warns.
    for i in range(17):
        _insert_ready_video(
            f"hk{i:08d}", parent_item_id="parentid004", item_type="hook",
        )
    resp = client.get(
        "/api/projects/default/videos/parentid004/promos/schedule-all/preview"
    )
    data = resp.json()
    assert any("quota" in w.lower() for w in data["warnings"])


def test_commit_rejects_not_ready_children(client: TestClient) -> None:
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video(
        "parentid005", publish_at=parent_iso, status="scheduled",
    )
    _insert_ready_video(
        "notready01", parent_item_id="parentid005", item_type="hook",
        description="",
    )
    resp = client.post(
        "/api/projects/default/videos/parentid005/promos/schedule-all",
        json={},
    )
    assert resp.status_code == 400
    assert "ready" in resp.json()["detail"].lower()


def test_promo_quota_helper() -> None:
    from yt_scheduler.services.scheduler import promo_quota_for

    assert promo_quota_for(0) == 0
    assert promo_quota_for(1) == 150
    assert promo_quota_for(10) == 1500


def test_preview_preserves_already_scheduled_children(client: TestClient) -> None:
    """Schedule-all must be non-destructive: already-scheduled children
    keep their existing publish_at, even when no unscheduled children
    are added in this batch."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    custom_short_time = "2026-04-15T09:00:00+00:00"
    _insert_ready_video(
        "parentid006", publish_at=parent_iso, status="scheduled",
    )
    _insert_ready_video(
        "shortmanl1", parent_item_id="parentid006", item_type="short",
        publish_at=custom_short_time, status="scheduled",
    )
    resp = client.get(
        "/api/projects/default/videos/parentid006/promos/schedule-all/preview"
    )
    data = resp.json()
    row = next(r for r in data["rows"] if r["video_id"] == "shortmanl1")
    assert row["target_time"] == custom_short_time


def test_readiness_accepts_user_edited_description_starting_with_placeholder(
) -> None:
    """A user-edited description that happens to begin with the same
    phrase as the placeholder must still count as ready (it's a real
    description now, just unfortunate phrasing)."""
    from yt_scheduler.services.scheduler import is_ready_for_schedule

    ready, _ = is_ready_for_schedule({
        "transcript": "real transcript",
        "description": "Description pending generation but here's more text",
        "tags": '["one", "two", "three"]',
        "thumbnail_path": "/tmp/x.jpg",
    })
    assert ready
