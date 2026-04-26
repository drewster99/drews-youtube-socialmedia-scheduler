"""Tests for the slot CRUD methods on ``services/templates.py``.

These exercise the post-migration-008 slot model: built-in slots are
created by ``ensure_default_template`` / ``save_template``; non-builtin
slots are added/updated/deleted via the new service helpers.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Reload everything against a tmp data dir + force file-backed keychain."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in (
        "yt_scheduler.services.templates",
        "yt_scheduler.services.social_credentials",
        "yt_scheduler.services.social",
        "yt_scheduler.services.keychain",
        "yt_scheduler.services.projects",
        "yt_scheduler.database",
        "yt_scheduler.config",
    ):
        sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    templates = importlib.import_module("yt_scheduler.services.templates")
    creds = importlib.import_module("yt_scheduler.services.social_credentials")

    db = await database.get_db()
    await projects.ensure_default_project()
    await templates.ensure_default_template()
    yield templates, creds, db
    await database.close_db()


async def test_ensure_default_template_creates_builtin_slots(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    assert announce is not None
    assert announce["is_builtin"] is True
    assert len(announce["slots"]) == 5
    platforms = {slot["platform"] for slot in announce["slots"]}
    assert platforms == {"twitter", "bluesky", "mastodon", "linkedin", "threads"}
    for slot in announce["slots"]:
        assert slot["is_builtin"] is True
        assert slot["is_disabled"] is False


async def test_add_slot_creates_non_builtin(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    new_slot = await templates.add_slot(
        announce["id"], "twitter", body="Second X variant", max_chars=200,
    )
    assert new_slot["is_builtin"] is False
    assert new_slot["platform"] == "twitter"
    assert new_slot["body"] == "Second X variant"
    assert new_slot["max_chars"] == 200

    refreshed = await templates.get_template("announce_video")
    twitter_slots = [s for s in refreshed["slots"] if s["platform"] == "twitter"]
    assert len(twitter_slots) == 2


async def test_add_slot_rejects_unknown_platform(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    with pytest.raises(ValueError, match="Unknown platform"):
        await templates.add_slot(announce["id"], "myspace")


async def test_add_slot_rejects_missing_template(app_db) -> None:
    templates, _creds, _db = app_db
    with pytest.raises(ValueError, match="not found"):
        await templates.add_slot(9999, "twitter")


async def test_update_slot_changes_fields(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    twitter_builtin = next(s for s in announce["slots"] if s["platform"] == "twitter")

    updated = await templates.update_slot(
        twitter_builtin["id"], body="hello", max_chars=180,
    )
    assert updated["body"] == "hello"
    assert updated["max_chars"] == 180
    # Untouched fields remain
    assert updated["is_builtin"] is True
    assert updated["platform"] == "twitter"


async def test_update_slot_rejects_invalid_max_chars(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    twitter = next(s for s in announce["slots"] if s["platform"] == "twitter")
    with pytest.raises(ValueError, match="max_chars must be positive"):
        await templates.update_slot(twitter["id"], max_chars=0)


async def test_update_slot_can_disable_builtin(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    threads = next(s for s in announce["slots"] if s["platform"] == "threads")
    updated = await templates.update_slot(threads["id"], is_disabled=True)
    assert updated["is_disabled"] is True


async def test_update_slot_can_set_and_clear_account(app_db) -> None:
    templates, creds, _db = app_db
    cred = await creds.upsert_credential(
        "twitter", "tw:abc", "tester", {"bearer_token": "tok"}
    )
    announce = await templates.get_template("announce_video")
    twitter = next(s for s in announce["slots"] if s["platform"] == "twitter")

    bound = await templates.update_slot(twitter["id"], social_account_id=cred["id"])
    assert bound["social_account_id"] == cred["id"]
    assert bound["resolved_account"]["uuid"] == cred["uuid"]

    cleared = await templates.update_slot(twitter["id"], social_account_id=None)
    assert cleared["social_account_id"] is None
    assert cleared["resolved_account"] is None


async def test_delete_slot_refuses_builtin(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    twitter = next(s for s in announce["slots"] if s["platform"] == "twitter")
    with pytest.raises(ValueError, match="built-in"):
        await templates.delete_slot(twitter["id"])


async def test_delete_slot_works_for_non_builtin(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    new_slot = await templates.add_slot(announce["id"], "linkedin", body="extra")
    await templates.delete_slot(new_slot["id"])
    assert await templates.get_slot(new_slot["id"]) is None


async def test_add_slot_default_order_index_appends(app_db) -> None:
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    initial_max = max(s["order_index"] for s in announce["slots"])
    new_slot = await templates.add_slot(announce["id"], "twitter")
    assert new_slot["order_index"] == initial_max + 1


async def test_get_template_returns_resolved_account_for_slot(app_db) -> None:
    templates, creds, _db = app_db
    cred = await creds.upsert_credential(
        "linkedin", "li:1", "Pro User", {"access_token": "tok"}
    )
    announce = await templates.get_template("announce_video")
    li = next(s for s in announce["slots"] if s["platform"] == "linkedin")
    await templates.update_slot(li["id"], social_account_id=cred["id"])

    refreshed = await templates.get_template("announce_video")
    li_after = next(s for s in refreshed["slots"] if s["platform"] == "linkedin")
    assert li_after["resolved_account"]["uuid"] == cred["uuid"]
    assert li_after["resolved_account"]["deleted"] is False

    await creds.soft_delete_credential(cred["uuid"])
    refreshed2 = await templates.get_template("announce_video")
    li_after2 = next(s for s in refreshed2["slots"] if s["platform"] == "linkedin")
    assert li_after2["resolved_account"]["deleted"] is True


async def test_save_template_only_touches_builtin_slots(app_db) -> None:
    """Re-saving a template via save_template must not clobber non-builtin slots."""
    templates, _creds, _db = app_db
    announce = await templates.get_template("announce_video")
    extra = await templates.add_slot(announce["id"], "twitter", body="extra variant")

    await templates.save_template(
        "announce_video",
        description="updated description",
        platforms={"twitter": {"template": "new built-in", "media": "thumbnail", "max_chars": 280}},
    )

    refreshed = await templates.get_template("announce_video")
    assert refreshed["description"] == "updated description"
    builtin_twitter = next(
        s for s in refreshed["slots"]
        if s["platform"] == "twitter" and s["is_builtin"]
    )
    assert builtin_twitter["body"] == "new built-in"
    # The non-builtin slot we added must still be there
    extra_after = next(
        (s for s in refreshed["slots"] if s["id"] == extra["id"]), None
    )
    assert extra_after is not None
    assert extra_after["body"] == "extra variant"
