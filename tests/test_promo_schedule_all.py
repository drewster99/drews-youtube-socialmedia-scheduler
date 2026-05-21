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
    privacy_status: str = "unlisted",
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
            VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                video_id, title, description, tag_json, privacy_status,
                status, item_type, parent_item_id, publish_at,
                transcript_text, thumbnail_path, thumbnail_source,
                f"https://youtu.be/{video_id}",
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


def test_tier_readiness_summary_line() -> None:
    """Pure helper behind the Promo videos card's per-tier one-liner."""
    from yt_scheduler.routers.promo_routes import _tier_readiness

    assert _tier_readiness([])["count"] == 0
    assert _tier_readiness([])["state"] == "empty"

    ready_child = {
        "auto_action_state": "ready", "status": "ready",
        "transcript": "t", "description": "d",
        "tags": json.dumps(["a", "b", "c"]), "thumbnail_path": "/t.jpg",
    }
    r = _tier_readiness([dict(ready_child), dict(ready_child)])
    assert r["count"] == 2
    assert r["line"] == "all ready"
    assert r["state"] == "ready"

    published = {"auto_action_state": "ready", "status": "published"}
    no_thumb = {
        "auto_action_state": "ready", "status": "ready",
        "transcript": "t", "description": "d",
        "tags": json.dumps(["a", "b", "c"]),
        "thumbnail_path": None, "thumbnail_source": None,
    }
    r = _tier_readiness([published, no_thumb])
    assert r["count"] == 2
    assert "1 published" in r["line"]
    assert "thumbnail" in r["line"]
    assert r["state"] == "attention"

    r = _tier_readiness([{"auto_action_state": "transcribing", "status": "uploaded"}])
    assert r["state"] == "working"
    assert "1 processing" in r["line"]


def test_promos_endpoint_includes_readiness(client: TestClient) -> None:
    _insert_ready_video("rdytparent1", status="captioned", privacy_status="public")
    _insert_ready_video(
        "rdytchild01", parent_item_id="rdytparent1", item_type="segment",
    )
    resp = client.get("/api/projects/default/videos/rdytparent1/promos")
    assert resp.status_code == 200
    data = resp.json()
    assert set(data["readiness"]) == {"segment", "short", "hook"}
    seg = data["readiness"]["segment"]
    assert seg["count"] == 1
    assert seg["state"] == "ready"
    assert data["readiness"]["hook"]["count"] == 0


def test_preview_requires_parent_publish_or_existing(client: TestClient) -> None:
    _insert_ready_video("parentid001", publish_at=None, status="uploaded")
    _insert_ready_video(
        "child000001", parent_item_id="parentid001", item_type="short",
    )
    resp = client.post(
        "/api/projects/default/videos/parentid001/promos/schedule-all/preview",
        json={},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["anchor_publish_at"] is None
    assert any("publish time" in w.lower() for w in data["warnings"])


def test_preview_treats_public_parent_as_published(client: TestClient) -> None:
    """An imported parent that's already public on YouTube — app status
    not 'published', no publish_at — must be recognized as published:
    flagged already_published, no publish-time prompt, no readiness
    warning (even with no tags), and children anchored from now."""
    _insert_ready_video(
        "pubparent01", publish_at=None, status="captioned",
        privacy_status="public", tags=[],
    )
    _insert_ready_video(
        "segchildp01", parent_item_id="pubparent01", item_type="segment",
    )
    resp = client.post(
        "/api/projects/default/videos/pubparent01/promos/schedule-all/preview",
        json={},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["parent"]["already_published"] is True
    assert not any("publish time" in w.lower() for w in data["warnings"])
    assert not any("isn't ready" in w.lower() for w in data["warnings"])
    # Anchored from now, so a concrete child target time is computed.
    assert data["anchor_publish_at"] is not None
    assert data["rows"][0]["target_time"] is not None


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

    resp = client.post(
        "/api/projects/default/videos/parentid002/promos/schedule-all/preview",
        json={},
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


def test_preview_honours_project_promo_delays(client: TestClient) -> None:
    """A custom per-tier delay saved in project settings changes the
    schedule-all preview's computed target times."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video("pdparent001", publish_at=parent_iso, status="scheduled")
    _insert_ready_video(
        "pdsegchild1", parent_item_id="pdparent001", item_type="segment",
    )
    # Default segment 'initial' is 3 days; override to 1 day.
    delays = client.get("/api/projects/default/promo-delays").json()
    delays["segment"]["initial"] = {"value": 1, "unit": "days"}
    assert client.put(
        "/api/projects/default/promo-delays", json=delays,
    ).status_code == 200

    resp = client.post(
        "/api/projects/default/videos/pdparent001/promos/schedule-all/preview",
        json={},
    )
    assert resp.status_code == 200
    targets = {r["video_id"]: r["target_time"] for r in resp.json()["rows"]}
    from datetime import datetime, timedelta
    parent_dt = datetime.fromisoformat(parent_iso)
    assert datetime.fromisoformat(targets["pdsegchild1"]) == parent_dt + timedelta(days=1)


def test_preview_blocks_when_child_missing_fields(client: TestClient) -> None:
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video(
        "parentid003", publish_at=parent_iso, status="scheduled",
    )
    _insert_ready_video(
        "missingtag1", parent_item_id="parentid003", item_type="hook",
        tags=[],
    )
    resp = client.post(
        "/api/projects/default/videos/parentid003/promos/schedule-all/preview",
        json={},
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
    # 51 children × 50 quota = 2,550 (~25.5% of daily 10k) → warns.
    for i in range(51):
        _insert_ready_video(
            f"hk{i:08d}", parent_item_id="parentid004", item_type="hook",
        )
    resp = client.post(
        "/api/projects/default/videos/parentid004/promos/schedule-all/preview",
        json={},
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


def test_preview_honours_explicit_order(client: TestClient) -> None:
    """A drag-reordered video-id sequence resequences the tier chain."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video("ordparent01", publish_at=parent_iso, status="scheduled")
    _insert_ready_video("ordsegmentA", parent_item_id="ordparent01", item_type="segment")
    _insert_ready_video("ordsegmentB", parent_item_id="ordparent01", item_type="segment")

    resp = client.post(
        "/api/projects/default/videos/ordparent01/promos/schedule-all/preview",
        json={"order": ["ordsegmentB", "ordsegmentA"]},
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert [r["video_id"] for r in rows] == ["ordsegmentB", "ordsegmentA"]


def test_preview_accepts_delay_override(client: TestClient) -> None:
    """Delays in the POST body override the project's saved delays."""
    from datetime import datetime, timedelta

    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video("ovparent001", publish_at=parent_iso, status="scheduled")
    _insert_ready_video("ovsegchild1", parent_item_id="ovparent001", item_type="segment")
    delays = {
        t: {"initial": {"value": 2, "unit": "hours"},
            "subsequent": {"value": 1, "unit": "days"}}
        for t in ("hook", "short", "segment")
    }
    resp = client.post(
        "/api/projects/default/videos/ovparent001/promos/schedule-all/preview",
        json={"delays": delays},
    )
    assert resp.status_code == 200
    targets = {r["video_id"]: r["target_time"] for r in resp.json()["rows"]}
    parent_dt = datetime.fromisoformat(parent_iso)
    assert datetime.fromisoformat(targets["ovsegchild1"]) == parent_dt + timedelta(hours=2)


def test_schedule_all_persists_delays(client: TestClient) -> None:
    """A delays payload sent with the commit is saved as the project
    default so the next batch keeps the same pace."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video("perparent01", publish_at=parent_iso, status="scheduled")
    _insert_ready_video("persegchild", parent_item_id="perparent01", item_type="segment")
    delays = {
        t: {"initial": {"value": 5, "unit": "hours"},
            "subsequent": {"value": 8, "unit": "hours"}}
        for t in ("hook", "short", "segment")
    }
    resp = client.post(
        "/api/projects/default/videos/perparent01/promos/schedule-all",
        json={"delays": delays},
    )
    assert resp.status_code == 200, resp.text
    saved = client.get("/api/projects/default/promo-delays").json()
    assert saved["segment"]["initial"] == {"value": 5, "unit": "hours"}
    assert saved["segment"]["subsequent"] == {"value": 8, "unit": "hours"}


def test_preview_resumes_from_last_published_promo(client: TestClient) -> None:
    """When a tier already has a published promo, a fresh chain resumes
    from that promo's publish time + the First delay — not the parent."""
    from datetime import datetime, timedelta

    _insert_ready_video("rsparent001", status="published", privacy_status="public")
    published_iso = "2026-05-01T12:00:00+00:00"
    _insert_ready_video(
        "rspublished1", parent_item_id="rsparent001", item_type="segment",
        status="published", publish_at=published_iso,
    )
    _insert_ready_video(
        "rsnewchild1", parent_item_id="rsparent001", item_type="segment",
    )
    delays = {
        t: {"initial": {"value": 2, "unit": "days"},
            "subsequent": {"value": 5, "unit": "days"}}
        for t in ("hook", "short", "segment")
    }
    resp = client.post(
        "/api/projects/default/videos/rsparent001/promos/schedule-all/preview",
        json={"delays": delays},
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    # The published promo is excluded; the new one anchors at
    # last-published + First delay (2 days).
    assert [r["video_id"] for r in rows] == ["rsnewchild1"]
    expected = datetime.fromisoformat(published_iso) + timedelta(days=2)
    assert datetime.fromisoformat(rows[0]["target_time"]) == expected


def test_promo_quota_helper() -> None:
    from yt_scheduler.services.scheduler import promo_quota_for

    # Scheduling cost is the publish-time privacy flip (~50 units/promo),
    # not the upload chain's ~150 — that was spent before scheduling.
    assert promo_quota_for(0) == 0
    assert promo_quota_for(1) == 50
    assert promo_quota_for(10) == 500


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
    resp = client.post(
        "/api/projects/default/videos/parentid006/promos/schedule-all/preview",
        json={},
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


def test_commit_schedules_parent_when_publish_at_supplied(
    client: TestClient,
) -> None:
    """Committing with parent_publish_at for an unscheduled parent
    schedules the parent row itself, not just the children."""
    _insert_ready_video("scparent001", publish_at=None, status="uploaded")
    _insert_ready_video(
        "scchild0001", parent_item_id="scparent001", item_type="segment",
    )
    parent_iso = "2026-12-01T15:00:00+00:00"
    resp = client.post(
        "/api/projects/default/videos/scparent001/promos/schedule-all",
        json={"parent_publish_at": parent_iso},
    )
    assert resp.status_code == 200, resp.text
    scheduled_ids = {r["video_id"] for r in resp.json()["scheduled"]}
    assert "scparent001" in scheduled_ids

    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        row = conn.execute(
            "SELECT publish_at, status FROM videos WHERE id = ?",
            ("scparent001",),
        ).fetchone()
    assert row[0] is not None
    assert row[1] == "scheduled"


def test_schedule_all_preserves_manual_pin(client: TestClient) -> None:
    """A child the user manually pinned (publish_at_manual = 1) must keep
    both its publish time AND the pin through a schedule-all batch —
    schedule-all is non-destructive to manually-set schedules."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    pinned_time = "2026-04-20T09:00:00+00:00"
    _insert_ready_video("mpparent001", publish_at=parent_iso, status="scheduled")
    _insert_ready_video(
        "mppinned001", parent_item_id="mpparent001", item_type="segment",
        publish_at=pinned_time, status="scheduled",
    )
    _insert_ready_video(
        "mpfresh0001", parent_item_id="mpparent001", item_type="hook",
    )

    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            "UPDATE videos SET publish_at_manual = 1 WHERE id = ?",
            ("mppinned001",),
        )
        conn.commit()

    resp = client.post(
        "/api/projects/default/videos/mpparent001/promos/schedule-all",
        json={},
    )
    assert resp.status_code == 200, resp.text

    with sqlite3.connect(str(DB_PATH)) as conn:
        row = conn.execute(
            "SELECT publish_at, publish_at_manual FROM videos WHERE id = ?",
            ("mppinned001",),
        ).fetchone()
    assert row[0] == pinned_time
    assert row[1] == 1

    scheduled_ids = {r["video_id"] for r in resp.json()["scheduled"]}
    assert "mpfresh0001" in scheduled_ids
    assert "mppinned001" not in scheduled_ids


def test_schedule_all_endpoints_reject_malformed_delays(
    client: TestClient,
) -> None:
    """A malformed delays payload is a 400 on both the preview and the
    commit endpoint — validated up front, never silently defaulted."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video("mdparent001", publish_at=parent_iso, status="scheduled")
    _insert_ready_video(
        "mdchild0001", parent_item_id="mdparent001", item_type="segment",
    )
    bad = {"delays": {"hook": {"initial": {"value": -1, "unit": "weeks"}}}}
    for suffix in ("schedule-all/preview", "schedule-all"):
        resp = client.post(
            f"/api/projects/default/videos/mdparent001/promos/{suffix}",
            json=bad,
        )
        assert resp.status_code == 400, (suffix, resp.text)


def test_schedule_all_reports_partial_failures(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """schedule_publish commits per child, so the batch isn't atomic. A
    per-child write failure is reported in `errors` while the rest still
    go through — re-running resumes the chain idempotently."""
    parent_iso = "2026-04-01T17:30:00+00:00"
    _insert_ready_video("pfparent001", publish_at=parent_iso, status="scheduled")
    _insert_ready_video(
        "pfchildgood", parent_item_id="pfparent001", item_type="hook",
    )
    _insert_ready_video(
        "pfchildbad0", parent_item_id="pfparent001", item_type="segment",
    )

    from yt_scheduler.services import scheduler as sched

    real_schedule_publish = sched.schedule_publish

    async def flaky(video_id, publish_at):
        if video_id == "pfchildbad0":
            raise RuntimeError("simulated APScheduler failure")
        return await real_schedule_publish(video_id, publish_at)

    monkeypatch.setattr(sched, "schedule_publish", flaky)

    resp = client.post(
        "/api/projects/default/videos/pfparent001/promos/schedule-all",
        json={},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    scheduled_ids = {r["video_id"] for r in data["scheduled"]}
    error_ids = {e["video_id"] for e in data["errors"]}
    assert "pfchildgood" in scheduled_ids
    assert error_ids == {"pfchildbad0"}
