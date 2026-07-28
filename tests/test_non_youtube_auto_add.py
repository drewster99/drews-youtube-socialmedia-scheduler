"""Publishing a non-YouTube item runs the auto-add funnel.

It used to be skipped outright. That branch never sets `privacy_status` — the
item has no YouTube presence — so it sat at 'unlisted' forever, eligibility read
that as "not live", and running the funnel would have recorded a permanent "no"
for an item that was as live as it would ever get. Liveness now asks `status`
for items with nothing behind them on YouTube, so the question has an answer.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain

# 22 characters: what the app mints for an item created outside YouTube. The
# length is what distinguishes it — a YouTube-backed row is keyed by its
# 11-character YouTube video id.
STANDALONE_ID = "ccgubMJsE8q7uHzf0TK2qw"


@pytest.fixture
async def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    queue_service = importlib.import_module("yt_scheduler.services.smart_queue")

    db = await database.get_db()
    await projects.ensure_default_project()
    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'standalone posts', '[\"standalone\"]')"
    )
    template_id = int(cursor.lastrowid)
    await db.execute(
        "INSERT INTO videos (id, project_id, title, item_type, status, "
        "privacy_status, duration_seconds, width, height) "
        "VALUES (?, 1, 'A standalone post', 'standalone', 'ready', "
        "'unlisted', 60, 1080, 1920)",
        (STANDALONE_ID,),
    )
    await db.commit()
    queue_id = await queue_service.create_queue(
        project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
        slots=[{"weekday": 0, "time_of_day": "09:00"}],
    )
    async with database.write_transaction() as write_db:
        await write_db.execute(
            "UPDATE smart_queues SET auto_add_on_live = 1 WHERE id = ?",
            (queue_id,),
        )
    yield db, queue_id, database
    await database.close_db()


async def test_publishing_a_standalone_item_adds_it_to_the_queue(env, monkeypatch):
    db, queue_id, database = env
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    # The item never touches YouTube; stub the social fan-out so the test is
    # about the funnel and not about posting.
    async def no_posts(*args, **kwargs):
        return []

    monkeypatch.setattr(
        scheduler, "_send_social_posts_for_video", no_posts, raising=False
    )

    result = await scheduler.publish_video_job(STANDALONE_ID)
    assert result.get("youtube_skipped") is True

    rows = await db.execute_fetchall(
        "SELECT video_id, state FROM smart_queue_items WHERE queue_id = ?",
        (queue_id,),
    )
    assert [(r["video_id"], r["state"]) for r in rows] == [
        (STANDALONE_ID, "queued")
    ], "a published standalone item must reach the queue it is eligible for"

    considered = await db.execute_fetchall(
        "SELECT auto_add_considered_at FROM videos WHERE id = ?", (STANDALONE_ID,)
    )
    assert considered[0]["auto_add_considered_at"] is not None
