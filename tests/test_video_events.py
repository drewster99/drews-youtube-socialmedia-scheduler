"""Tests for the per-video event log."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def events_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    events = importlib.import_module("yt_scheduler.services.events")
    db = await database.get_db()
    await projects.ensure_default_project()
    await db.execute(
        "INSERT INTO videos (id, project_id, title) VALUES ('vid1', 1, 'Hello')"
    )
    await db.commit()
    yield events, db
    await database.close_db()


async def test_record_event_persists(events_env) -> None:
    events, _db = events_env
    eid = await events.record_event("vid1", "uploaded", {"platform": "youtube"})
    assert eid > 0
    rows = await events.list_events_for_video("vid1")
    assert len(rows) == 1
    assert rows[0]["type"] == "uploaded"
    assert rows[0]["payload"] == {"platform": "youtube"}


async def test_recent_events_join_project(events_env) -> None:
    events, _db = events_env
    await events.record_event("vid1", "created", {"tier": None})
    await events.record_event("vid1", "uploaded", {"platform": "youtube"})
    recent = await events.list_recent_events(limit=5)
    assert len(recent) == 2
    assert all(r["project_slug"] == "default" for r in recent)
    assert recent[0]["type"] == "uploaded"  # newest first


async def test_diff_payload_only_changed(events_env) -> None:
    events, _db = events_env
    payload = events.diff_payload(
        {"title": "A", "description": "x", "tags": ["a", "b"]},
        {"title": "B", "description": "x", "tags": ["a", "b"]},
        ["title", "description", "tags"],
    )
    assert payload == {"title": {"old": "A", "new": "B"}}


async def test_diff_payload_tags_treats_lists_normally(events_env) -> None:
    events, _db = events_env
    payload = events.diff_payload(
        {"tags": ["a", "b"]}, {"tags": ["a", "c"]}, ["tags"]
    )
    assert payload == {"tags": {"old": ["a", "b"], "new": ["a", "c"]}}


async def test_diff_payload_dict_key_order_irrelevant(events_env) -> None:
    events, _db = events_env
    payload = events.diff_payload(
        {"meta": {"a": 1, "b": 2}}, {"meta": {"b": 2, "a": 1}}, ["meta"]
    )
    assert payload == {}


async def test_video_delete_cascades_events(events_env) -> None:
    events, db = events_env
    await events.record_event("vid1", "uploaded")
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("DELETE FROM videos WHERE id = 'vid1'")
    await db.commit()
    rows = await events.list_events_for_video("vid1")
    assert rows == []
