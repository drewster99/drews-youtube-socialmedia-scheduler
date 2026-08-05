"""Reading video privacy back from YouTube.

``videos.privacy_status`` was write-only with respect to YouTube: every writer
was a change this app made. A video published in YouTube Studio stayed
'unlisted' here, so auto-add never saw it and the send gate refused its posts as
"not public" — about a public video.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def privacy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """The service, a live DB, and a project with a channel bound."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for module in list(sys.modules):
        if module.startswith("yt_scheduler"):
            sys.modules.pop(module, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    # Resolved lazily, not at module scope: the purge above orphans any module
    # object captured at import time, and the test would then patch a dead one.
    service = importlib.import_module("yt_scheduler.services.video_privacy_sync")

    db = await database.get_db()
    await projects.ensure_default_project()
    await db.execute(
        "UPDATE projects SET youtube_channel_id = 'UCchannel' WHERE id = 1"
    )
    await db.commit()
    project = {"id": 1, "slug": "default", "youtube_channel_id": "UCchannel"}
    yield service, db, project
    await database.close_db()


async def _add_video(db, video_id: str, *, privacy: str, youtube_id: str | None,
                     archived: int = 0, title: str = "A video") -> None:
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, "
        "item_type, duration_seconds, privacy_status, status, archived) "
        "VALUES (?, ?, 1, ?, 'short', 60, ?, 'published', ?)",
        (video_id, youtube_id, title, privacy, archived),
    )
    await db.commit()


def _fake_youtube(monkeypatch, service, answers: dict[str, str], *,
                  seen: list | None = None):
    def fake(video_ids):
        if seen is not None:
            seen.append(list(video_ids))
        return {v: answers[v] for v in video_ids if v in answers}

    monkeypatch.setattr(
        service.youtube_service, "get_videos_privacy_status", fake
    )


async def test_a_video_published_in_studio_is_detected(privacy, monkeypatch):
    """The reported bug: publish on YouTube, and this app never learns."""
    service, db, project = privacy
    await _add_video(db, "v1", privacy="unlisted", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "public"})

    summary = await service.sync_project_video_privacy(project)

    assert summary["checked"] == 1
    assert summary["changed"] == [
        {"video_id": "v1", "title": "A video", "from": "unlisted", "to": "public"}
    ]
    rows = await db.execute_fetchall(
        "SELECT privacy_status, privacy_synced_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["privacy_status"] == "public"
    assert rows[0]["privacy_synced_at"] is not None


async def test_a_video_pulled_back_to_unlisted_is_also_detected(
    privacy, monkeypatch
):
    """The reverse direction, and the more dangerous one.

    A stale 'public' is read by the send gate — the only thing stopping the
    scheduler announcing a link nobody can open. Checking only the non-public
    videos would leave that failure permanently undetectable.
    """
    service, db, project = privacy
    await _add_video(db, "v1", privacy="public", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "unlisted"})

    summary = await service.sync_project_video_privacy(project)

    assert summary["changed"][0]["to"] == "unlisted"
    rows = await db.execute_fetchall(
        "SELECT privacy_status FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["privacy_status"] == "unlisted"


async def test_a_video_youtube_does_not_return_is_left_alone(
    privacy, monkeypatch
):
    """Absence is not evidence of privacy.

    A deleted video, one we lost read access to, and a truncated response are
    indistinguishable here. Reading any of them as 'private' would unpublish a
    live video in our own records and stop its posts going out.
    """
    service, db, project = privacy
    await _add_video(db, "v1", privacy="public", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {})  # YouTube returns nothing

    summary = await service.sync_project_video_privacy(project)

    assert summary["missing"] == ["ytv1"]
    assert summary["changed"] == []
    rows = await db.execute_fetchall(
        "SELECT privacy_status, privacy_synced_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["privacy_status"] == "public", "unchanged"
    assert rows[0]["privacy_synced_at"] is None, (
        "a video we could not read was not verified, and must not be stamped "
        "as though it had been"
    )


async def test_going_public_runs_the_auto_add_funnel(privacy, monkeypatch):
    """The whole point: a video that went live must reach the smart queues."""
    service, db, project = privacy
    await _add_video(db, "v1", privacy="unlisted", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "public"})

    called: list[str] = []

    async def fake_live(video_id):
        called.append(video_id)
        return {"considered": True, "added_to": [7], "reasons": {}}

    live = importlib.import_module("yt_scheduler.services.smart_queue_live")
    monkeypatch.setattr(live, "on_video_became_live", fake_live)

    summary = await service.sync_project_video_privacy(project)

    assert called == ["v1"]
    assert summary["became_live"] == ["v1"]


async def test_going_non_public_does_not_run_auto_add(privacy, monkeypatch):
    service, db, project = privacy
    await _add_video(db, "v1", privacy="public", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "private"})

    called: list[str] = []

    async def fake_live(video_id):
        called.append(video_id)
        return {"considered": True, "added_to": [], "reasons": {}}

    live = importlib.import_module("yt_scheduler.services.smart_queue_live")
    monkeypatch.setattr(live, "on_video_became_live", fake_live)

    await service.sync_project_video_privacy(project)

    assert called == []


async def test_auto_add_failure_does_not_lose_the_privacy_correction(
    privacy, monkeypatch
):
    """The correction is the more important half.

    Letting an auto-add error propagate would roll the sweep back to reporting
    a public video as unlisted — and the send gate would go on refusing its
    posts, which is the bug this module exists to fix.
    """
    service, db, project = privacy
    await _add_video(db, "v1", privacy="unlisted", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "public"})

    async def boom(video_id):
        raise RuntimeError("queue exploded")

    live = importlib.import_module("yt_scheduler.services.smart_queue_live")
    monkeypatch.setattr(live, "on_video_became_live", boom)

    summary = await service.sync_project_video_privacy(project)

    assert summary["became_live"] == []
    rows = await db.execute_fetchall(
        "SELECT privacy_status FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["privacy_status"] == "public"


async def test_unchanged_privacy_still_stamps_the_check(privacy, monkeypatch):
    """"We looked" and "it moved" are different facts, and the stamp is the
    first one — it is what makes a failing sweep visible as staleness."""
    service, db, project = privacy
    await _add_video(db, "v1", privacy="public", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "public"})

    summary = await service.sync_project_video_privacy(project)

    assert summary["changed"] == []
    rows = await db.execute_fetchall(
        "SELECT privacy_synced_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["privacy_synced_at"] is not None


async def test_non_youtube_and_archived_videos_are_not_swept(
    privacy, monkeypatch
):
    """A row with no YouTube video has nothing to ask about, and an archived one
    is not in play. Both would otherwise burn quota every sweep."""
    service, db, project = privacy
    await _add_video(db, "local", privacy="unlisted", youtube_id=None)
    await _add_video(db, "old", privacy="public", youtube_id="ytold", archived=1)
    await _add_video(db, "live", privacy="unlisted", youtube_id="ytlive")
    seen: list = []
    _fake_youtube(monkeypatch, service, {"ytlive": "public"}, seen=seen)

    await service.sync_project_video_privacy(project)

    assert seen == [["ytlive"]]


async def test_least_recently_verified_is_read_first(privacy, monkeypatch):
    """A capped sweep must rotate through the library, not re-read the same head
    of the list forever while the tail is never checked at all."""
    service, db, project = privacy
    await _add_video(db, "v1", privacy="public", youtube_id="yt1")
    await _add_video(db, "v2", privacy="public", youtube_id="yt2")
    await _add_video(db, "v3", privacy="public", youtube_id="yt3")
    # v1 checked recently, v2 long ago, v3 never.
    await db.execute(
        "UPDATE videos SET privacy_synced_at = '2026-08-04 00:00:00' "
        "WHERE id = 'v1'"
    )
    await db.execute(
        "UPDATE videos SET privacy_synced_at = '2020-01-01 00:00:00' "
        "WHERE id = 'v2'"
    )
    await db.commit()

    seen: list = []
    _fake_youtube(monkeypatch, service, {}, seen=seen)

    await service.sync_project_video_privacy(project, limit=2)

    assert seen == [["yt3", "yt2"]], "never-checked first, then oldest"


async def test_one_projects_failure_does_not_stop_the_others(
    privacy, monkeypatch
):
    """Each project is a separate channel with its own OAuth grant. A revoked
    grant on one must not silence the sweep for every other."""
    service, db, _ = privacy
    await db.execute(
        "INSERT INTO projects (id, name, slug, youtube_channel_id) "
        "VALUES (2, 'Second', 'second', 'UCsecond')"
    )
    await db.commit()

    async def fake_one(project, *, limit=200):
        if project["slug"] == "default":
            raise RuntimeError("token revoked")
        return {"project_slug": project["slug"], "checked": 3, "missing": [],
                "changed": [], "became_live": [], "quota_units": 1}

    monkeypatch.setattr(service, "sync_project_video_privacy", fake_one)

    summaries = await service.sync_all_projects_video_privacy()

    assert len(summaries) == 2
    assert summaries[0]["error"] == "RuntimeError: token revoked"
    assert summaries[1]["checked"] == 3


async def test_a_youtube_failure_stamps_nothing(privacy, monkeypatch):
    """A sweep that cannot reach YouTube must raise and leave every stamp
    untouched. Writing a confirmation it did not earn would present an
    unverified library as freshly checked."""
    service, db, project = privacy
    await _add_video(db, "v1", privacy="unlisted", youtube_id="ytv1")

    def boom(video_ids):
        raise RuntimeError("HTTP 403 insufficientPermissions")

    monkeypatch.setattr(
        service.youtube_service, "get_videos_privacy_status", boom
    )

    with pytest.raises(RuntimeError, match="insufficientPermissions"):
        await service.sync_project_video_privacy(project)

    rows = await db.execute_fetchall(
        "SELECT privacy_status, privacy_synced_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["privacy_status"] == "unlisted"
    assert rows[0]["privacy_synced_at"] is None


async def test_a_change_is_recorded_as_an_event(privacy, monkeypatch):
    """A privacy change nobody made in this app is news, and the Recent feed is
    where the user finds out. Distinct from `metadata_updated`, which records a
    change WE made."""
    service, db, project = privacy
    await _add_video(db, "v1", privacy="unlisted", youtube_id="ytv1")
    _fake_youtube(monkeypatch, service, {"ytv1": "public"})

    await service.sync_project_video_privacy(project)

    rows = await db.execute_fetchall(
        "SELECT type, payload_json FROM video_events WHERE video_id = 'v1'"
    )
    types = [r["type"] for r in rows]
    assert "privacy_changed_on_youtube" in types
    payload = next(
        r["payload_json"] for r in rows
        if r["type"] == "privacy_changed_on_youtube"
    )
    assert '"old": "unlisted"' in payload and '"new": "public"' in payload
