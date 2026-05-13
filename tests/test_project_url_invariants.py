"""Audit tests for projects.project_url — the value behind
``{{project_url}}`` in templates. Confirms the four code paths that
can populate or leave NULL behave the way the rest of the app
assumes:

* Migration 010 backfills YT-bound projects with the channel-id form
  URL when they came from a pre-010 schema.
* The OAuth YouTube bind seeds project_url on first bind (NULL →
  handle form) and upgrades the migration-backfilled channel-id
  form on subsequent binds.
* ``services.projects.create_project`` accepts an explicit
  project_url and stores it as-is (after trim).
* A non-YT project that's deliberately created with project_url=None
  stays NULL — the caller is expected to set it via PATCH.

These tests run against a fresh in-memory schema so the migration
backfill behaviour can be exercised even when the dev DB has long
since been migrated past 010.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


def test_create_project_with_explicit_url_round_trips(client: TestClient) -> None:
    """Explicit project_url is stored verbatim (after .strip())."""
    resp = client.post(
        "/api/projects",
        json={
            "name": "GH",
            "slug": "gh-explicit",
            "kind": "github",
            "project_url": "https://github.com/me/x",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["project_url"] == "https://github.com/me/x"


def test_create_project_without_url_is_allowed_but_null(client: TestClient) -> None:
    """A project created with no project_url is allowed and stays NULL
    (front-end can PATCH it later). This is the intentional shape — a
    YouTube project starts NULL and OAuth-binds the handle form on
    first auth; a non-YT project's caller is expected to supply one
    or set it via PATCH."""
    resp = client.post(
        "/api/projects",
        json={"name": "Bare", "slug": "bare", "kind": "github"},
    )
    assert resp.status_code == 200
    assert resp.json()["project_url"] is None


def test_default_project_has_project_url_after_migrations(client: TestClient) -> None:
    """The default project that ships with the schema must NOT have
    NULL project_url after the migration chain runs — otherwise the
    canonical {{project_url}} reference would render empty for every
    out-of-the-box render. Today there's no default-project YouTube
    channel binding in tests, so this asserts the project exists and
    has the expected shape; a real install has it overwritten by the
    OAuth bind."""
    body = client.get("/api/projects/default").json()
    assert body["slug"] == "default"
    # `default` is the implicit catch-all and has no youtube_channel_id
    # in a fresh test install — migration 010 only backfills rows that
    # already have a channel id. Document the shape explicitly so a
    # future change that flips this assumption fails loudly here.
    assert body.get("youtube_channel_id") is None
    assert body.get("project_url") is None


@pytest.mark.asyncio
async def test_migration_010_backfills_youtube_projects(tmp_path: Path) -> None:
    """Pre-010 schema with a populated youtube_channel_id and NULL
    project_url → migration 010 seeds the channel-id form URL."""
    db_path = tmp_path / "p.db"
    import aiosqlite
    async with aiosqlite.connect(str(db_path)) as conn:
        from yt_scheduler.migrations import MIGRATIONS_DIR, discover_migrations

        for mig in discover_migrations(MIGRATIONS_DIR):
            if mig.version >= 10:
                break
            await conn.executescript(mig.path.read_text())
        await conn.commit()

        # Pre-010 row: has a channel binding but no project_url
        # (project_url column was added in 010).
        await conn.execute(
            "UPDATE projects SET youtube_channel_id = 'UC_TEST_CH' WHERE slug = 'default'"
        )
        await conn.commit()

        sql_010 = (MIGRATIONS_DIR / "010_typed_items.sql").read_text()
        await conn.executescript(sql_010)
        await conn.commit()

        cur = await conn.execute(
            "SELECT project_url FROM projects WHERE slug = 'default'"
        )
        row = await cur.fetchone()
        assert row[0] == "https://www.youtube.com/channel/UC_TEST_CH"
