"""The app-wide failed-sends surface: GET /api/social/failed-posts.

A failed send used to exist only as an 8-second toast and a badge on the one
page that owns the post; scheduled sends could fail with no page open at all.
The banner in base.html polls this endpoint from every page, so the endpoint's
contract — only failed posts, newest first, carrying the real error — is what
keeps failures visible.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


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


async def _seed_posts() -> None:
    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidA', 1, 'My Video', 'uploaded')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status, error) "
        "VALUES (1, 'vidA', 'threads', 'older failure', 'failed', 'boom one')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status) "
        "VALUES (2, 'vidA', 'bluesky', 'went out fine', 'posted')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status) "
        "VALUES (3, 'vidA', 'mastodon', 'not sent yet', 'draft')"
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status, error) "
        "VALUES (4, 'vidA', 'threads', 'newer failure', 'failed', 'boom two')"
    )
    await db.commit()


async def test_only_failed_posts_are_listed_newest_first(client) -> None:
    await _seed_posts()

    resp = client.get("/api/social/failed-posts")
    assert resp.status_code == 200
    posts = resp.json()

    assert [p["id"] for p in posts] == [4, 1]
    newest = posts[0]
    assert newest["platform"] == "threads"
    assert newest["error"] == "boom two"
    assert newest["video_id"] == "vidA"
    assert newest["video_title"] == "My Video"
    # The server vends the ready page link, slug included — the detail route
    # 404s unless the slug actually owns the video, so the client never guesses.
    assert newest["page_url"].startswith("/projects/")
    assert newest["page_url"].endswith("/videos/vidA")


async def test_ordering_is_by_when_it_failed_not_by_row_id(client) -> None:
    """`id` is creation order, so a post written weeks ago that failed minutes
    ago sorted below one written yesterday that failed last week. That is how a
    five-day-old failure came to head the banner over four from the same
    afternoon — the exact confusion `failed_at` was added to end. A pre-migration
    row (NULL) is old by definition and goes last."""
    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidA', 1, 'My Video', 'uploaded')"
    )
    await db.execute(
        "INSERT INTO social_posts "
        "(id, video_id, platform, content, status, error, failed_at) VALUES "
        # Lowest id, but it failed most recently.
        "(1, 'vidA', 'twitter', 'x', 'failed', 'recent', '2026-08-04 13:00:00'),"
        # Highest id, but it failed days earlier.
        "(9, 'vidA', 'bluesky', 'x', 'failed', 'older', '2026-07-30 02:00:00'),"
        # Highest id of all and no date at all — a pre-migration row.
        "(12, 'vidA', 'mastodon', 'x', 'failed', 'undated', NULL)"
    )
    await db.commit()

    posts = client.get("/api/social/failed-posts").json()

    assert [p["id"] for p in posts] == [1, 9, 12]


async def test_skipping_takes_a_failure_out_of_the_banner(client) -> None:
    """Not by hiding it. The post becomes 'skipped' — the state this codebase
    already uses for a post nobody is going to send — so it leaves the list
    because it is genuinely no longer failing."""
    from yt_scheduler.database import get_db

    await _seed_posts()
    assert [p["id"] for p in client.get("/api/social/failed-posts").json()] == [4, 1]

    assert client.post("/api/social/posts/4/skip").status_code == 200

    assert [p["id"] for p in client.get("/api/social/failed-posts").json()] == [1]
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT status, error, retryable, next_retry_at FROM social_posts WHERE id = 4"
    )
    assert rows[0]["status"] == "skipped"
    # The record of what went wrong is kept; this is a decision, not a rewrite.
    assert rows[0]["error"] == "boom two"
    # And it must leave the retry path, or the job would send it a minute later.
    assert not rows[0]["retryable"]
    assert rows[0]["next_retry_at"] is None


async def test_only_a_failed_post_can_be_skipped(client) -> None:
    """On any other status this would silently change something the user is not
    looking at."""
    await _seed_posts()

    # id 2 is 'posted', id 3 is 'draft'.
    assert client.post("/api/social/posts/2/skip").status_code == 409
    assert client.post("/api/social/posts/9999/skip").status_code == 404


async def test_empty_when_nothing_failed(client) -> None:
    resp = client.get("/api/social/failed-posts")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_skipping_the_last_posting_retires_its_queue_item(client) -> None:
    """A queue item left 'scheduled' with nothing that could ever send counts
    toward the scheduled total forever AND permanently blocks its video from
    being selected again — the zombie the reconcile handlers exist to prevent.
    Before the banner had Skip there was no way to reach a queue-owned post from
    here, so this could not arise."""
    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidQ', 1, 'Queued', 'published')"
    )
    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'skip-test-template', '[]')"
    )
    template_id = cursor.lastrowid
    await db.execute(
        "INSERT INTO smart_queues (id, project_id, name, template_id, timezone) "
        "VALUES (1, 1, 'Q', ?, 'UTC')",
        (template_id,),
    )
    await db.execute(
        "INSERT INTO smart_queue_items (id, queue_id, video_id, position, state) "
        "VALUES (7, 1, 'vidQ', 0, 'scheduled')"
    )
    await db.execute(
        "INSERT INTO social_posts "
        "(id, video_id, platform, content, status, error, smart_queue_item_id) "
        "VALUES (1, 'vidQ', 'bluesky', 'x', 'failed', 'boom', 7)"
    )
    await db.commit()

    assert client.post("/api/social/posts/1/skip").status_code == 200

    cursor = await db.execute("SELECT state FROM smart_queue_items WHERE id = 7")
    assert (await cursor.fetchone())["state"] == "removed"
