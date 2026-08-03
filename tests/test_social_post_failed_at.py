"""``social_posts.failed_at``: when the last send attempt failed.

A failure carried no time at all before migration 044. ``created_at`` is when
the post row was written — for a smart-queue post that is days or weeks before
the send — and marking a post failed *clears* ``scheduled_at``, so the failure
surfaces rendered "unscheduled" with no time. The banner could say what broke
but never whether it broke minutes or a week ago.

The stamp is written in exactly one place, :func:`models.social_post.mark_failed`.
These tests hold that line: the column is stamped, the API vends it, and no
second writer of the ``'failed'`` state can reappear.
"""

from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "yt_scheduler"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)

    app_module = importlib.import_module("yt_scheduler.app")

    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as test_client:
        yield test_client


async def _seed_post(*, post_id: int, status: str, error: str | None = None) -> None:
    database = importlib.import_module("yt_scheduler.database")
    db = await database.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO videos (id, project_id, title, status) "
        "VALUES ('vidA', 1, 'My Video', 'uploaded')"
    )
    await db.execute(
        "INSERT INTO social_posts "
        "(id, video_id, platform, content, status, error, scheduled_at, scheduler_job_id) "
        "VALUES (?, 'vidA', 'threads', 'body', ?, ?, '2026-01-01T00:00:00+00:00', 'job-x')",
        (post_id, status, error),
    )
    await db.commit()


async def _row(post_id: int) -> dict:
    database = importlib.import_module("yt_scheduler.database")
    db = await database.get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE id = ?", (post_id,)
    )
    return dict(rows[0])


async def test_mark_failed_stamps_the_time_and_clears_scheduling(client) -> None:
    await _seed_post(post_id=1, status="approved")
    social_post = importlib.import_module("yt_scheduler.models.social_post")

    await social_post.mark_failed(1, error="Threads container create failed")

    row = await _row(1)
    assert row["status"] == "failed"
    assert row["error"] == "Threads container create failed"
    # Naive UTC, the same shape posted_at has always used.
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", row["failed_at"])
    # A row that keeps these is resurrected and re-sent by the restore pass on
    # the next restart — the one thing a terminal failure must never do.
    assert row["scheduled_at"] is None
    assert row["scheduler_job_id"] is None


async def test_a_later_failure_replaces_the_earlier_stamp(client) -> None:
    """failed_at is the *latest* attempt, not the first — it is what the UI
    calls the failure's age, and a retry that fails again is newly urgent."""
    await _seed_post(post_id=1, status="approved")
    social_post = importlib.import_module("yt_scheduler.models.social_post")
    database = importlib.import_module("yt_scheduler.database")

    await social_post.mark_failed(1, error="first")
    db = await database.get_db()
    await db.execute(
        "UPDATE social_posts SET failed_at = '2020-01-01 00:00:00' WHERE id = 1"
    )
    await db.commit()

    await social_post.mark_failed(1, error="second")

    row = await _row(1)
    assert row["error"] == "second"
    assert not row["failed_at"].startswith("2020")


async def test_mark_posted_after_a_failure_leaves_no_live_failure(client) -> None:
    """A recovered post must not read as a current failure. status is the
    single source of truth for that, so the banner never sees it again — the
    old failed_at simply stops being consulted."""
    await _seed_post(post_id=1, status="approved")
    social_post = importlib.import_module("yt_scheduler.models.social_post")

    await social_post.mark_failed(1, error="transient 502")
    await social_post.mark_posted(1, post_url="https://example.test/p/1")

    row = await _row(1)
    assert row["status"] == "posted"
    assert row["error"] is None

    resp = client.get("/api/social/failed-posts")
    assert [p["id"] for p in resp.json()] == []


async def test_failed_posts_endpoint_vends_the_time(client) -> None:
    await _seed_post(post_id=1, status="approved")
    social_post = importlib.import_module("yt_scheduler.models.social_post")
    await social_post.mark_failed(1, error="boom")

    resp = client.get("/api/social/failed-posts")
    assert resp.status_code == 200
    posts = resp.json()
    assert len(posts) == 1
    assert posts[0]["failed_at"]


async def test_pre_migration_rows_report_no_time_rather_than_a_wrong_one(client) -> None:
    """NULL failed_at is the honest answer for a row that failed before the
    column existed. It must not be papered over with created_at, which is when
    the post was written and can predate the attempt by weeks."""
    await _seed_post(post_id=1, status="failed", error="ancient failure")

    resp = client.get("/api/social/failed-posts")
    posts = resp.json()
    assert len(posts) == 1
    assert posts[0]["failed_at"] is None


async def test_missed_list_carries_the_failure_time(client) -> None:
    """The smart-schedule "Didn't go out" list reads failed_at for the same
    reason the banner does: marking a post failed clears scheduled_at, so that
    column is empty on exactly the rows a time matters most for."""
    database = importlib.import_module("yt_scheduler.database")
    social_post = importlib.import_module("yt_scheduler.models.social_post")
    disposition = importlib.import_module(
        "yt_scheduler.services.smart_queue_disposition"
    )

    db = await database.get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidA', 1, 'My Video', 'uploaded')"
    )
    # smart_queues.template_id is ON DELETE RESTRICT, so it has to name a live
    # row — reuse whichever template startup seeded.
    templates = await db.execute_fetchall("SELECT id FROM templates LIMIT 1")
    assert templates, "startup should have seeded at least one template"
    cursor = await db.execute(
        "INSERT INTO smart_queues (project_id, name, template_id, timezone) "
        "VALUES (1, 'Daily', ?, 'America/Los_Angeles')",
        (int(templates[0]["id"]),),
    )
    queue_id = int(cursor.lastrowid)
    cursor = await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, 'vidA', 0, 'scheduled')",
        (queue_id,),
    )
    item_id = int(cursor.lastrowid)
    await db.execute(
        "INSERT INTO social_posts "
        "(id, video_id, platform, content, status, smart_queue_item_id) "
        "VALUES (1, 'vidA', 'threads', 'body', 'approved', ?)",
        (item_id,),
    )
    await db.commit()

    await social_post.mark_failed(1, error="Threads container create failed")

    items = await disposition.missed_items(queue_id)
    assert len(items) == 1
    assert items[0]["scheduled_at"] is None
    assert items[0]["failed_at"]


def test_only_one_writer_of_the_failed_state() -> None:
    """The statement this replaced existed as ten byte-identical copies. Adding
    failed_at meant finding every one, and the eleventh send path would have
    shipped without it. Route new failures through mark_failed instead."""
    mutator = SRC_ROOT / "models" / "social_post.py"
    offenders: list[str] = []
    pattern = re.compile(r"UPDATE\s+social_posts\s+SET\s+status\s*=\s*'failed'")
    for path in SRC_ROOT.rglob("*.py"):
        if path == mutator:
            continue  # The one writer this test exists to protect.
        if pattern.search(path.read_text()):
            offenders.append(str(path.relative_to(SRC_ROOT)))
    assert offenders == [], (
        "These files write the 'failed' state directly and so skip the "
        f"failed_at stamp — call models.social_post.mark_failed instead: {offenders}"
    )


def test_no_template_redefines_the_shared_date_helpers() -> None:
    """``_ensureUtc`` existed as six byte-identical copies across the page
    templates, none of them reachable from ``static/js/``. That is why the
    failed-sends banner could not format a timestamp at all. One copy lives in
    ``static/js/datetime.js``, loaded in <head> by ``base.html``; pages call
    ``window.dysDateTime``.
    """
    templates = SRC_ROOT / "templates_html"
    offenders = [
        str(path.relative_to(SRC_ROOT))
        for path in sorted(templates.glob("*.html"))
        if re.search(r"function\s+_?ensureUtc\s*\(", path.read_text())
    ]
    assert offenders == [], (
        "These templates define their own ensureUtc instead of calling "
        f"window.dysDateTime: {offenders}"
    )


def test_every_template_that_uses_dysdatetime_inherits_base() -> None:
    """``dysDateTime`` is vended by a <script> in base.html's <head>. A page
    that calls it without extending base has no such script, and the call
    throws at render time rather than degrading."""
    templates = SRC_ROOT / "templates_html"
    missing = []
    for path in sorted(templates.glob("*.html")):
        if path.name == "base.html":
            continue
        text = path.read_text()
        if "dysDateTime" in text and '{% extends "base.html" %}' not in text:
            missing.append(path.name)
    assert missing == [], (
        f"These templates use dysDateTime but do not extend base.html: {missing}"
    )
