"""Tests for repost protection.

The route-level integration test runs through ``TestClient`` so the
end-to-end ``send → 409 → confirm → 200`` round-trip is exercised.
The unit-level tests pin the matching rules: same platform + same
account + same trimmed content + within the lookback window =
duplicate; anything else is fine.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    social = importlib.import_module("yt_scheduler.services.social")

    db = await database.get_db()
    await projects.ensure_default_project()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidD', 1, 'Dup test', 'uploaded')"
    )
    # Pre-create two synthetic social_accounts rows for the test fixtures
    # to point at — the FK on social_posts.social_account_id is enforced
    # after migration 008 turns PRAGMA foreign_keys ON.
    for i in (1, 2):
        await db.execute(
            "INSERT INTO social_accounts (id, uuid, platform, "
            "provider_account_id, username, credentials_ref) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (i, f"u{i}", "mastodon" if i == 1 else "twitter",
             f"acct:{i}", f"user{i}", f"cred.u{i}"),
        )
    await db.commit()
    yield social, db
    await database.close_db()


async def _seed_posted(db, *, platform, account_id, content, posted_at=None):
    """Insert a row in social_posts with status='posted'."""
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status, "
        "social_account_id, posted_at, post_url) "
        "VALUES ('vidD', ?, ?, 'posted', ?, ?, 'https://x.test/1')",
        (platform, content, account_id, posted_at),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def test_no_duplicate_when_no_prior_post(app_db) -> None:
    social, _db = app_db
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=1, content="hello",
    )
    assert result is None


async def test_finds_exact_duplicate(app_db) -> None:
    social, db = app_db
    pid = await _seed_posted(
        db, platform="mastodon", account_id=1, content="hello",
        posted_at="2026-04-26 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=1, content="hello",
    )
    assert result is not None
    assert result["id"] == pid


async def test_excludes_self(app_db) -> None:
    """A post about to be sent should not match itself in the dedup check."""
    social, db = app_db
    pid = await _seed_posted(
        db, platform="mastodon", account_id=1, content="hello",
        posted_at="2026-04-26 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=1, content="hello",
        exclude_post_id=pid,
    )
    assert result is None


async def test_different_account_is_not_duplicate(app_db) -> None:
    """Two accounts on the same platform legitimately post the same text."""
    social, db = app_db
    await _seed_posted(
        db, platform="mastodon", account_id=1, content="hello",
        posted_at="2026-04-26 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=2, content="hello",
    )
    assert result is None


async def test_different_platform_is_not_duplicate(app_db) -> None:
    social, db = app_db
    await _seed_posted(
        db, platform="mastodon", account_id=1, content="hello",
        posted_at="2026-04-26 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="twitter", social_account_id=1, content="hello",
    )
    assert result is None


async def test_outside_lookback_window_is_not_duplicate(app_db) -> None:
    """Anything older than 30 days isn't a dup. We only protect against
    accidental near-term reposts; legitimate evergreen content rotation
    is fine."""
    social, db = app_db
    await _seed_posted(
        db, platform="mastodon", account_id=1, content="hello",
        posted_at="2026-01-01 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=1, content="hello",
        lookback_days=30,
    )
    assert result is None


async def test_whitespace_is_normalised(app_db) -> None:
    social, db = app_db
    pid = await _seed_posted(
        db, platform="mastodon", account_id=1, content="hello",
        posted_at="2026-04-26 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=1, content="  hello\n",
    )
    assert result is not None
    assert result["id"] == pid


async def test_empty_content_never_matches(app_db) -> None:
    social, db = app_db
    await _seed_posted(
        db, platform="mastodon", account_id=1, content="",
        posted_at="2026-04-26 12:00:00",
    )
    result = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=1, content="",
    )
    assert result is None


async def test_send_post_returns_409_on_duplicate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Route-level: POST /api/social/posts/{id}/send returns 409 with the
    dup payload when a recent identical post exists, and 200/POST again
    when ``?confirm_dup=true`` is set."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)

    app_module = importlib.import_module("yt_scheduler.app")
    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    social_mod = importlib.import_module("yt_scheduler.services.social")

    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as c:
        # Set up a Twitter credential the test poster will resolve to.
        cred = await creds_mod.upsert_credential(
            "twitter", "tw:1", "alice", {"bearer_token": "tok"}
        )

        from yt_scheduler.database import get_db
        db = await get_db()
        await db.execute(
            "INSERT INTO videos (id, project_id, title, status) "
            "VALUES ('vidR', 1, 'R', 'uploaded')"
        )
        # Seed an already-posted twin so the fresh post will dup against it.
        await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, "
            "social_account_id, posted_at, post_url) "
            "VALUES ('vidR', 'twitter', 'hi there', 'posted', ?, "
            "datetime('now'), 'https://x.com/i/status/old')",
            (cred["id"],),
        )
        # Insert the post we're about to send.
        cursor = await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, "
            "social_account_id) VALUES ('vidR', 'twitter', 'hi there', "
            "'approved', ?)",
            (cred["id"],),
        )
        post_id = int(cursor.lastrowid)
        await db.commit()

        # Without confirm_dup → 409 with the dup payload
        resp = c.post(f"/api/social/posts/{post_id}/send")
        assert resp.status_code == 409, resp.text
        body = resp.json()
        # FastAPI wraps HTTPException(detail=dict) under "detail"
        detail = body["detail"]
        assert detail["duplicate"] is True
        assert detail["platform"] == "twitter"
        assert "previous" in detail
        assert detail["previous"]["post_url"] == "https://x.com/i/status/old"

        # With confirm_dup=true the dup gate is skipped — but the actual
        # send call still tries to hit the platform with a fake bearer
        # token. We just need to verify the route DIDN'T 409 a second
        # time (i.e. confirm_dup actually flipped the gate).
        resp2 = c.post(f"/api/social/posts/{post_id}/send?confirm_dup=true")
        assert resp2.status_code != 409, resp2.text
        # Either 500 (twitter rejected the fake bearer) or 200 — both
        # are valid for this test; the point is the dup gate let us
        # past on the second call.

    from yt_scheduler.database import close_db
    await close_db()
