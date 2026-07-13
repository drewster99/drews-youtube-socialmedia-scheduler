"""Publish content gate: scheduling refuses (409, forceable) when the
video's content is in an error state, and the fire-time job refuses to
push the pending-generation placeholder public or send render-error posts.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    yield


async def _insert_video(db, video_id: str, **overrides) -> None:
    fields = {
        "id": video_id,
        "project_id": 1,
        "title": "T",
        "description": "A real description.",
        "tags": json.dumps(["a", "b", "c"]),
        "privacy_status": "unlisted",
        "status": "uploaded",
        "item_type": "episode",
        "url": f"https://youtu.be/{video_id}",
    }
    fields.update(overrides)
    columns = ", ".join(fields)
    marks = ", ".join("?" * len(fields))
    await db.execute(
        f"INSERT INTO videos ({columns}) VALUES ({marks})",
        tuple(fields.values()),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_blockers_clean_video(env) -> None:
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    await _insert_video(db, "cleanvid0001")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    assert await scheduler.publish_content_blockers("cleanvid0001") == []
    await db_module.close_db()


@pytest.mark.asyncio
async def test_blockers_placeholder_and_empty_description(env) -> None:
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    await _insert_video(
        db, "pendingvid01",
        description=scheduler.DESCRIPTION_PENDING_PLACEHOLDER,
    )
    blockers = await scheduler.publish_content_blockers("pendingvid01")
    assert any("placeholder" in b for b in blockers)

    await _insert_video(db, "emptyvid0001", description="")
    blockers = await scheduler.publish_content_blockers("emptyvid0001")
    assert any("empty" in b for b in blockers)

    # Standalone items aren't listed on YouTube — no description blocker.
    await _insert_video(
        db, "standalone01", description="", item_type="standalone", url="",
    )
    assert await scheduler.publish_content_blockers("standalone01") == []
    await db_module.close_db()


@pytest.mark.asyncio
async def test_blockers_failed_chain_and_error_posts(env) -> None:
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    await _insert_video(
        db, "failedvid001",
        auto_action_state="failed:description",
        auto_action_last_error="Claude API call failed",
    )
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES (?, 'twitter', ?, 'approved')",
        ("failedvid001", "[Error generating: Undefined template variable(s): {{repo}}]"),
    )
    await db.commit()

    blockers = await scheduler.publish_content_blockers("failedvid001")
    assert any("failed at step 'description'" in b for b in blockers)
    assert any("render error" in b for b in blockers)
    await db_module.close_db()


@pytest.mark.asyncio
async def test_schedule_endpoint_blocks_and_force_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    # The reschedule cascade needs a running APScheduler; stub it.
    async def _fake_reschedule(video_id, publish_at):
        return {"cascaded_children": 0, "cascaded_siblings": 0}
    monkeypatch.setattr(scheduler, "apply_user_reschedule", _fake_reschedule)

    from fastapi.testclient import TestClient
    with TestClient(app_module.app) as c:
        from yt_scheduler.database import get_db
        db = await get_db()
        await _insert_video(
            db, "gatevid00001",
            description=scheduler.DESCRIPTION_PENDING_PLACEHOLDER,
        )

        when = "2099-01-01T09:00:00+00:00"
        resp = c.post(
            "/api/videos/gatevid00001/schedule", json={"publish_at": when}
        )
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert any("placeholder" in b for b in detail["publish_blockers"])

        forced = c.post(
            "/api/videos/gatevid00001/schedule",
            json={"publish_at": when, "force": True},
        )
        assert forced.status_code == 200, forced.text

    from yt_scheduler.database import close_db
    await close_db()


@pytest.mark.asyncio
async def test_per_post_job_blocks_render_error_posts(env, monkeypatch) -> None:
    """The independent per-post job can win the atomic claim — it needs the
    same render-error guard as the video publish job."""
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    # privacy_status='public' so the video-not-public pre-flight passes and
    # the flow reaches the atomic claim + render-error guard under test.
    await _insert_video(db, "perpost00001", privacy_status="public")
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status, "
        "scheduled_at, scheduler_job_id) "
        "VALUES (?, 'twitter', ?, 'approved', '2099-01-01T00:00:00', 'job1')",
        ("perpost00001", "[Error generating: boom]"),
    )
    await db.commit()
    rows = await db.execute_fetchall(
        "SELECT id FROM social_posts WHERE video_id = 'perpost00001'"
    )
    post_id = int(dict(rows[0])["id"])

    sent = []
    social = importlib.import_module("yt_scheduler.services.social")
    monkeypatch.setattr(
        social, "get_poster", lambda *a, **k: sent.append(a) or None
    )

    await scheduler._send_scheduled_post(post_id)

    assert sent == []  # never reached a poster
    rows = await db.execute_fetchall(
        "SELECT status, error, scheduled_at, scheduler_job_id "
        "FROM social_posts WHERE id = ?", (post_id,),
    )
    row = dict(rows[0])
    assert row["status"] == "failed"
    assert "render-error" in (row["error"] or "")
    # Scheduling columns cleared so a restart's restore pass can't
    # resurrect this post back to 'approved' and send it.
    assert row["scheduled_at"] is None
    assert row["scheduler_job_id"] is None
    await db_module.close_db()


@pytest.mark.asyncio
async def test_promo_readiness_blocks_failed_chain(env) -> None:
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    ready, missing = scheduler.is_ready_for_schedule({
        "transcript": "words", "description": "real", "tags": '["a","b","c"]',
        "thumbnail_path": "/t.jpg", "auto_action_state": "failed:description",
    })
    assert not ready
    assert any("failed at 'description'" in m for m in missing)


@pytest.mark.asyncio
async def test_fire_time_refuses_placeholder_description(env, monkeypatch) -> None:
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    # A scheduled video has publish_at set and status='scheduled' — the
    # cancel path only proceeds when there's a schedule to clear.
    await _insert_video(
        db, "firegate0001",
        description=scheduler.DESCRIPTION_PENDING_PLACEHOLDER,
        status="scheduled",
        publish_at="2000-01-01T00:00:00+00:00",
    )

    called = []
    youtube = importlib.import_module("yt_scheduler.services.youtube")
    monkeypatch.setattr(
        youtube, "update_video_metadata",
        lambda *a, **k: called.append(a),
    )

    results = await scheduler.publish_video_job("firegate0001")
    assert results.get("publish_blocked")
    assert results["published"] is False
    assert called == []  # YouTube never touched

    rows = await db.execute_fetchall(
        "SELECT privacy_status, status, publish_at FROM videos "
        "WHERE id = 'firegate0001'"
    )
    row = dict(rows[0])
    assert row["privacy_status"] == "unlisted"
    # The spent schedule is cleared — a restart's missed-job restore must
    # not re-attempt this publish at an arbitrary later time.
    assert row["status"] == "ready"
    assert row["publish_at"] is None
    await db_module.close_db()


@pytest.mark.asyncio
async def test_fire_time_blocks_render_error_posts(env, monkeypatch) -> None:
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    await _insert_video(db, "postgate0001", item_type="standalone", url="")
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES (?, 'mastodon', ?, 'approved')",
        ("postgate0001", "[Error generating: SectionTagError...]"),
    )
    await db.commit()

    results = await scheduler.publish_video_job("postgate0001")
    outcomes = results["social_results"].get("mastodon") or []
    assert outcomes and outcomes[0]["status"] == "failed"
    assert "render-error" in outcomes[0]["reason"]

    rows = await db.execute_fetchall(
        "SELECT status, error FROM social_posts WHERE video_id = 'postgate0001'"
    )
    row = dict(rows[0])
    assert row["status"] == "failed"
    assert "render-error" in (row["error"] or "")
    await db_module.close_db()
