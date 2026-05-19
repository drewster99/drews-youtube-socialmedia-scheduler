"""Tests for the parent_* render-context built-ins introduced in
migration 023.

A child video that points at a parent should see ``parent_url``,
``parent_title``, ``parent_description``, ``parent_tags`` resolve to
the parent row's columns; a primary (no parent) should see empty
strings. ``parent_context_block`` collapses the four fields into a
ready-to-paste paragraph (empty when the video has no parent).
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
def reset_modules() -> None:
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)


@pytest.mark.asyncio
async def test_parent_vars_resolve_for_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)

    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()

    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, url)
           VALUES (?, ?, ?, ?, ?, 'unlisted', 'uploaded', 'episode', ?)""",
        (
            "parentid001",
            1,
            "Parent Title Goes Here",
            "Parent description text that is moderately long.",
            json.dumps(["foo", "bar", "baz"]),
            "https://youtu.be/parentid001",
        ),
    )
    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, parent_item_id, url)
           VALUES (?, ?, ?, ?, ?, 'unlisted', 'uploaded', 'short', ?, ?)""",
        (
            "childid0001",
            1,
            "Child Promo Title",
            "Child description",
            json.dumps(["bar"]),
            "parentid001",
            "https://youtu.be/childid0001",
        ),
    )
    await db.commit()

    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", ("childid0001",)
    )
    child = dict(rows[0])

    social_routes = importlib.import_module(
        "yt_scheduler.routers.social_routes"
    )
    ctx = await social_routes._build_render_context(db, child)

    variables = ctx["variables"]
    assert variables["parent_url"] == "https://youtu.be/parentid001"
    assert variables["parent_title"] == "Parent Title Goes Here"
    assert variables["parent_description"].startswith("Parent description")
    assert variables["parent_tags"] == "foo, bar, baz"

    block = variables["parent_context_block"]
    assert "Parent title: Parent Title Goes Here" in block
    assert "Parent URL: https://youtu.be/parentid001" in block
    assert "Parent tags: foo, bar, baz" in block


@pytest.mark.asyncio
async def test_parent_vars_empty_for_primary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)

    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()

    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, url)
           VALUES (?, ?, ?, ?, ?, 'unlisted', 'uploaded', 'episode', ?)""",
        (
            "primaryid01",
            1,
            "Primary Video",
            "",
            "[]",
            "https://youtu.be/primaryid01",
        ),
    )
    await db.commit()

    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", ("primaryid01",)
    )
    primary = dict(rows[0])

    social_routes = importlib.import_module(
        "yt_scheduler.routers.social_routes"
    )
    ctx = await social_routes._build_render_context(db, primary)

    variables = ctx["variables"]
    assert variables["parent_url"] == ""
    assert variables["parent_title"] == ""
    assert variables["parent_description"] == ""
    assert variables["parent_tags"] == ""
    assert variables["parent_context_block"] == ""


@pytest.mark.asyncio
async def test_parent_context_block_handles_missing_tags_field(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None
) -> None:
    """A child whose parent has malformed JSON in ``tags`` should not raise
    — the renderer must degrade to an empty parent_tags."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)

    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()

    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, url)
           VALUES (?, ?, ?, ?, ?, 'unlisted', 'uploaded', 'episode', ?)""",
        (
            "badtagsprn1",
            1,
            "Parent",
            "Parent desc",
            "this is not json",
            "https://youtu.be/badtagsprn1",
        ),
    )
    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, parent_item_id, url)
           VALUES (?, ?, ?, ?, ?, 'unlisted', 'uploaded', 'hook', ?, ?)""",
        (
            "badtagschd1",
            1,
            "Child",
            "",
            "[]",
            "badtagsprn1",
            "https://youtu.be/badtagschd1",
        ),
    )
    await db.commit()

    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", ("badtagschd1",)
    )
    child = dict(rows[0])

    social_routes = importlib.import_module(
        "yt_scheduler.routers.social_routes"
    )
    ctx = await social_routes._build_render_context(db, child)
    assert ctx["variables"]["parent_tags"] == ""
    assert "Parent title: Parent" in ctx["variables"]["parent_context_block"]
