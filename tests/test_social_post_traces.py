"""F2 — social_post_traces persistence + pruning.

* Migration 020 creates the table and FK.
* The GET /api/social/posts/{post_id}/trace endpoint returns the row's
  parsed trace (or 404 when absent / pruned).
* prune_social_post_traces_job deletes rows older than 24h and leaves
  fresh rows alone.
"""

from __future__ import annotations

import importlib
import json
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


async def _seed_post_with_trace(post_id_out: list[int]) -> None:
    """Insert a video + a post + a trace row using direct SQL — keeps
    the test focused on the trace surface, not on the full
    generate-posts pipeline."""
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, imported_from_youtube) "
        "VALUES ('TRC0000001', 1, 't', 'uploaded', 1)"
    )
    cur = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('TRC0000001', 'bluesky', 'hi', 'draft')"
    )
    post_id = int(cur.lastrowid)
    trace = [
        {"kind": "template_body", "text": "hi {{name}}"},
        {"kind": "variables", "values": {"name": "Drew"}},
        {"kind": "substituted", "text": "hi Drew"},
        {"kind": "ai_call", "prompt": "p", "system": "s", "model": "m",
         "response": "r", "elapsed_ms": 12},
    ]
    await db.execute(
        "INSERT INTO social_post_traces (post_id, trace_json) VALUES (?, ?)",
        (post_id, json.dumps(trace)),
    )
    await db.commit()
    post_id_out.append(post_id)


@pytest.mark.asyncio
async def test_get_trace_returns_parsed_list(client: TestClient) -> None:
    held: list[int] = []
    await _seed_post_with_trace(held)
    post_id = held[0]
    resp = client.get(f"/api/social/posts/{post_id}/trace")
    assert resp.status_code == 200
    body = resp.json()
    assert body["post_id"] == post_id
    assert isinstance(body["trace"], list)
    assert body["trace"][0]["kind"] == "template_body"
    assert body["trace"][-1]["response"] == "r"


@pytest.mark.asyncio
async def test_get_trace_404_when_absent(client: TestClient) -> None:
    resp = client.get("/api/social/posts/99999/trace")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_cascade_delete_with_post(client: TestClient) -> None:
    """Deleting a social_posts row removes its trace via FK cascade."""
    held: list[int] = []
    await _seed_post_with_trace(held)
    post_id = held[0]

    from yt_scheduler.database import get_db
    db = await get_db()
    # The schema declares ON DELETE CASCADE; SQLite honors it only when
    # ``PRAGMA foreign_keys = ON`` was set on this connection, which
    # ``database.get_db()`` does on first acquire.
    await db.execute("DELETE FROM social_posts WHERE id = ?", (post_id,))
    await db.commit()

    resp = client.get(f"/api/social/posts/{post_id}/trace")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_prune_job_evicts_old_rows(client: TestClient) -> None:
    """A row whose created_at is older than 24h gets deleted by the
    pruning job; a fresh row stays."""
    held: list[int] = []
    await _seed_post_with_trace(held)
    old_post_id = held[0]

    from yt_scheduler.database import get_db
    db = await get_db()
    # Insert a second post + trace with created_at "now" (default).
    cur = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('TRC0000001', 'mastodon', 'fresh', 'draft')"
    )
    fresh_post_id = int(cur.lastrowid)
    await db.execute(
        "INSERT INTO social_post_traces (post_id, trace_json) VALUES (?, ?)",
        (fresh_post_id, "[]"),
    )
    # Backdate the original trace to >24h ago.
    await db.execute(
        "UPDATE social_post_traces SET created_at = datetime('now', '-2 days') "
        "WHERE post_id = ?",
        (old_post_id,),
    )
    await db.commit()

    from yt_scheduler.services.scheduler import prune_social_post_traces_job
    await prune_social_post_traces_job()

    # Old row gone, fresh row stays.
    rows = await db.execute_fetchall(
        "SELECT post_id FROM social_post_traces ORDER BY post_id"
    )
    ids = [int(r[0]) for r in rows]
    assert old_post_id not in ids
    assert fresh_post_id in ids
