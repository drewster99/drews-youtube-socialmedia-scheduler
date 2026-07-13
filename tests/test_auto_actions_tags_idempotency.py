"""Regression tests for auto_actions._maybe_generate_tags idempotency.

Once a video row has ``tags_generated_at`` set, calling ``_maybe_generate_tags``
must be a no-op: the AI generator must not be called and the existing tags
must not be overwritten. This prevents a server restart from clobbering
user-edited tags.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Isolated DB fixture that mirrors the pattern used in other auto_actions tests."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    db_module = importlib.import_module("yt_scheduler.database")
    db_conn = await db_module.get_db()
    # Seed a default project row so FK references resolve.
    await db_conn.execute(
        "INSERT INTO projects (id, name, slug) VALUES (1, 'default', 'default')"
        " ON CONFLICT DO NOTHING"
    )
    await db_conn.commit()
    yield db_conn
    await db_module.close_db()


async def _insert_video(
    db_conn,
    video_id: str,
    *,
    tags: list[str],
    tags_generated_at: str | None,
) -> None:
    """Insert a minimal video row with the given tags and timestamp."""
    await db_conn.execute(
        "INSERT INTO videos (id, project_id, title, status, tags, tags_generated_at) "
        "VALUES (?, 1, 'Test Video', 'uploaded', ?, ?)",
        (video_id, json.dumps(tags), tags_generated_at),
    )
    await db_conn.commit()


async def _read_tags(db_conn, video_id: str) -> list[str]:
    cur = await db_conn.execute(
        "SELECT tags FROM videos WHERE id = ?", (video_id,)
    )
    row = await cur.fetchone()
    assert row is not None
    return json.loads(row[0] or "[]")


async def test_tags_generated_at_set_skips_ai_call(db, monkeypatch) -> None:
    """When tags_generated_at is already stamped the AI generator must not be called."""
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    ai_call_count = 0

    async def _fake_generate_tags(*, title, description, transcript, project_id, prompt_variables=None, is_promo=False):
        nonlocal ai_call_count
        ai_call_count += 1
        return ["new_tag"]

    monkeypatch.setattr(auto_actions.ai, "generate_tags_from_metadata", _fake_generate_tags)

    existing_tags = ["original", "tag"]
    await _insert_video(
        db, "TAGTEST001",
        tags=existing_tags,
        tags_generated_at="2025-01-01T00:00:00",
    )

    video = {
        "id": "TAGTEST001",
        "project_id": 1,
        "title": "Test Video",
        "description": "Some description",
        "transcript": "Some transcript",
        "tags": json.dumps(existing_tags),
        "tags_generated_at": "2025-01-01T00:00:00",
    }
    column = {
        "auto_tags_include_title": True,
        "auto_tags_include_description": True,
        "auto_tags_include_transcript": True,
    }

    await auto_actions._maybe_generate_tags(video, project_id=1, column=column)

    assert ai_call_count == 0, (
        "_maybe_generate_tags called the AI generator even though tags_generated_at was set"
    )


async def test_tags_generated_at_set_preserves_existing_tags(db, monkeypatch) -> None:
    """When tags_generated_at is set, the tags column must not be modified."""
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    async def _fake_generate_tags(*, title, description, transcript, project_id, prompt_variables=None, is_promo=False):
        return ["replacement_tag"]

    monkeypatch.setattr(auto_actions.ai, "generate_tags_from_metadata", _fake_generate_tags)

    original_tags = ["keep", "these"]
    await _insert_video(
        db, "TAGTEST002",
        tags=original_tags,
        tags_generated_at="2025-01-01T00:00:00",
    )

    video = {
        "id": "TAGTEST002",
        "project_id": 1,
        "title": "Test Video",
        "description": "",
        "transcript": "",
        "tags": json.dumps(original_tags),
        "tags_generated_at": "2025-01-01T00:00:00",
    }
    column: dict = {}

    await auto_actions._maybe_generate_tags(video, project_id=1, column=column)

    after_tags = await _read_tags(db, "TAGTEST002")
    assert after_tags == original_tags, (
        "_maybe_generate_tags overwrote tags despite tags_generated_at being set"
    )


async def test_no_tags_generated_at_does_call_ai(db, monkeypatch) -> None:
    """Control case: when tags_generated_at is NOT set, the AI generator IS called.

    This verifies that the idempotency guard only fires when the column is set,
    not unconditionally.
    """
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    ai_call_count = 0

    async def _fake_generate_tags(*, title, description, transcript, project_id, prompt_variables=None, is_promo=False):
        nonlocal ai_call_count
        ai_call_count += 1
        return ["generated_tag"]

    monkeypatch.setattr(auto_actions.ai, "generate_tags_from_metadata", _fake_generate_tags)

    await _insert_video(
        db, "TAGTEST003",
        tags=[],
        tags_generated_at=None,
    )

    video = {
        "id": "TAGTEST003",
        "project_id": 1,
        "title": "Test Video",
        "description": "Some description",
        "transcript": "Some transcript",
        "tags": "[]",
        "tags_generated_at": None,
    }
    column = {
        "auto_tags_include_title": True,
        "auto_tags_include_description": True,
        "auto_tags_include_transcript": True,
    }

    await auto_actions._maybe_generate_tags(video, project_id=1, column=column)

    assert ai_call_count == 1, (
        "_maybe_generate_tags did NOT call the AI generator when tags_generated_at was None"
    )
    after_tags = await _read_tags(db, "TAGTEST003")
    assert after_tags == ["generated_tag"]
