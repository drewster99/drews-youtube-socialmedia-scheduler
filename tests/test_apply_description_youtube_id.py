"""The YouTube API must be addressed by ``videos.youtube_video_id``, never the
row primary key.

``videos.id`` and ``videos.youtube_video_id`` are equal for today's
YouTube-backed rows, so a call that passes the PK where the YouTube id belongs
works purely by coincidence. Migration 037 split identity from the YouTube id
precisely so nothing may conflate them again: the instant a YouTube-backed
row's PK differs from its ``youtube_video_id``, passing the PK updates the wrong
video (or 404s). Each test below seeds exactly that divergence — a 22-char PK
next to an 11-char YouTube id — so a PK-vs-id mixup fails loudly instead of
passing by accident.

Covers both endpoints that share the bug:
  * ``POST /api/videos/{id}/apply-description`` (the flagged one)
  * ``PUT  /api/videos/{id}`` (the identical sibling — push + read-back)
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

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
    with TestClient(app_module.app) as c:
        yield c


async def _seed_video(**columns: object) -> None:
    from yt_scheduler.database import get_db

    db = await get_db()
    names = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    await db.execute(
        f"INSERT INTO videos ({names}) VALUES ({placeholders})",
        tuple(columns.values()),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_apply_description_targets_youtube_id_not_row_pk(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    row_pk = "ROWPK_0001_22charlong"
    youtube_id = "YTREAL00001"
    await _seed_video(
        id=row_pk,
        project_id=1,
        title="T",
        status="captioned",
        youtube_video_id=youtube_id,
        generated_description="A generated description.",
    )

    pushed_to: list[object] = []

    def record_update(video_id=None, *args, **kwargs):
        pushed_to.append(video_id)
        return {}

    from yt_scheduler.routers import video_routes

    monkeypatch.setattr(video_routes.youtube, "update_video_metadata", record_update)

    resp = client.post(f"/api/videos/{row_pk}/apply-description")
    assert resp.status_code == 200, resp.text

    assert pushed_to == [youtube_id], (
        "apply-description must push to the youtube_video_id, not the row PK; "
        f"got {pushed_to!r}"
    )


@pytest.mark.asyncio
async def test_update_video_metadata_targets_youtube_id_not_row_pk(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    row_pk = "ROWPK_0002_22charlong"
    youtube_id = "YTREAL00002"
    await _seed_video(
        id=row_pk,
        project_id=1,
        title="Old title",
        status="ready",
        privacy_status="unlisted",
        youtube_video_id=youtube_id,
    )

    pushed_to: list[object] = []
    read_back_from: list[object] = []

    def record_update(video_id=None, *args, **kwargs):
        pushed_to.append(video_id)
        return {}

    def record_readback(video_id, *args, **kwargs):
        read_back_from.append(video_id)
        return {
            "snippet": {"title": "New title", "description": "", "tags": []},
            "status": {"privacyStatus": "unlisted", "publishAt": None},
        }

    from yt_scheduler.routers import video_routes

    monkeypatch.setattr(video_routes.youtube, "update_video_metadata", record_update)
    monkeypatch.setattr(video_routes.youtube, "get_video", record_readback)

    resp = client.put(f"/api/videos/{row_pk}", json={"title": "New title"})
    assert resp.status_code == 200, resp.text

    assert pushed_to == [youtube_id], (
        "PUT /api/videos/{id} must push metadata to the youtube_video_id, not "
        f"the row PK; got {pushed_to!r}"
    )
    assert read_back_from == [youtube_id], (
        "The post-update read-back must also query by youtube_video_id, not the "
        f"row PK; got {read_back_from!r}"
    )
