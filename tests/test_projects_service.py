"""Project CRUD service tests."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Isolate config + database to a tmp dir so tests don't share global state."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in (
        "yt_scheduler.services.projects",
        "yt_scheduler.database",
        "yt_scheduler.config",
    ):
        sys.modules.pop(mod, None)
    config = importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    assert config.DATA_DIR == tmp_path
    db = await database.get_db()
    yield projects, db
    await database.close_db()


async def test_ensure_default_project_idempotent(app_db) -> None:
    projects, _db = app_db
    pid_a = await projects.ensure_default_project()
    pid_b = await projects.ensure_default_project()
    assert pid_a == pid_b


async def test_create_and_list_projects(app_db) -> None:
    projects, _db = app_db
    await projects.ensure_default_project()
    new = await projects.create_project(name="My Show", slug=None)
    assert new["slug"] == "my-show"
    items = await projects.list_projects()
    slugs = [item["slug"] for item in items]
    assert "default" in slugs
    assert "my-show" in slugs


async def test_create_rejects_duplicate_slug(app_db) -> None:
    projects, _db = app_db
    await projects.ensure_default_project()
    with pytest.raises(ValueError, match="already exists"):
        await projects.create_project(name="Other", slug="default")


async def test_create_rejects_bad_slug(app_db) -> None:
    projects, _db = app_db
    with pytest.raises(ValueError, match="Slug"):
        await projects.create_project(name="Bad", slug="UPPER CASE")


async def test_rename_keeps_slug(app_db) -> None:
    projects, _db = app_db
    await projects.ensure_default_project()
    project = await projects.create_project(name="Show A", slug=None)
    renamed = await projects.rename_project(project["id"], name="Show A Renamed")
    assert renamed["name"] == "Show A Renamed"
    assert renamed["slug"] == project["slug"]


async def test_delete_default_refused(app_db) -> None:
    projects, _db = app_db
    pid = await projects.ensure_default_project()
    with pytest.raises(ValueError, match="Default project"):
        await projects.delete_project(pid)


async def test_delete_other_project_succeeds(app_db) -> None:
    projects, _db = app_db
    project = await projects.create_project(name="Disposable", slug=None)
    await projects.delete_project(project["id"])
    again = await projects.get_project_by_id(project["id"])
    assert again is None


async def test_list_includes_video_counts(app_db) -> None:
    projects, db = app_db
    await projects.ensure_default_project()
    await db.execute(
        "INSERT INTO videos (id, project_id, title) VALUES "
        "('v1', 1, 'one'), ('v2', 1, 'two')"
    )
    await db.commit()
    items = await projects.list_projects()
    default = next(item for item in items if item["slug"] == "default")
    assert default["video_count"] == 2
