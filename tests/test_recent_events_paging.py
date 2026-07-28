"""Paging the Home page activity feed.

The feed fetched one page and stopped, with nothing on screen to say so. A run
that scheduled 48 posts filled it completely and buried everything older, with
no way to reach any of it.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for module in list(sys.modules):
        if module.startswith("yt_scheduler"):
            sys.modules.pop(module, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    app_module = importlib.import_module("yt_scheduler.app")

    db = await database.get_db()
    await projects.ensure_default_project()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, item_type, status) "
        "VALUES ('vid00000001', 1, 'A clip', 'hook', 'published')"
    )
    # Every event shares a timestamp, which is exactly what a batch schedule
    # produces — and what a created_at-only sort cannot page through.
    for n in range(25):
        await db.execute(
            "INSERT INTO video_events (video_id, type, payload_json, created_at) "
            "VALUES ('vid00000001', 'social_post_scheduled', ?, "
            "'2026-07-27 18:11:00')",
            (f'{{"n": {n}}}',),
        )
    await db.commit()

    transport = ASGITransport(app=app_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as http:
        yield http
    await database.close_db()


async def test_offset_walks_back_without_repeating_or_dropping(client):
    """Ties on created_at are broken by id, so the order is total: paging
    cannot show the same event twice or skip one between pages."""
    page_one = (await client.get("/api/projects/recent-events?limit=10")).json()
    page_two = (
        await client.get("/api/projects/recent-events?limit=10&offset=10")
    ).json()
    page_three = (
        await client.get("/api/projects/recent-events?limit=10&offset=20")
    ).json()

    assert [len(p) for p in (page_one, page_two, page_three)] == [10, 10, 5]
    ids = [e["id"] for e in page_one + page_two + page_three]
    assert len(set(ids)) == 25, "every event appears exactly once across pages"


async def test_a_short_page_is_how_the_end_is_known(client):
    """The button hides on a short page, so the count has to be exact."""
    assert len((await client.get(
        "/api/projects/recent-events?limit=30")).json()) == 25


@pytest.mark.parametrize("query", ["limit=0", "limit=201", "offset=-1"])
async def test_bad_paging_is_refused_not_clamped(client, query):
    """A silently corrected limit returns a page nobody asked for, which reads
    as the feed having ended."""
    response = await client.get(f"/api/projects/recent-events?{query}")
    assert response.status_code == 400, response.text
