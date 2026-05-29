"""``videos.video_file_download_state`` is the column the detail-page
polling loop watches while the auto-action chain pulls a YouTube mp4
locally. Verify the success / private-video / generic-failure
transitions of ``auto_actions._maybe_download_video_file`` without
actually contacting YouTube.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    db_module = importlib.import_module("yt_scheduler.database")
    db_conn = await db_module.get_db()
    # Insert a placeholder imported-from-YT row so the UPDATEs have
    # something to target.
    await db_conn.execute(
        "INSERT INTO videos (id, project_id, title, status, imported_from_youtube) "
        "VALUES (?, 1, ?, 'uploaded', 1)",
        ("DLTEST00001", "import target"),
    )
    await db_conn.commit()
    yield db_conn


async def _state(db_conn, video_id: str) -> tuple[str | None, str | None]:
    """Read (download_state, video_file_path) for assertions."""
    cur = await db_conn.execute(
        "SELECT video_file_download_state, video_file_path FROM videos WHERE id = ?",
        (video_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    return row[0], row[1]


@pytest.mark.asyncio
async def test_success_clears_state_and_sets_path(
    db, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """On a happy-path download, the column resets to NULL and
    video_file_path is set to the fake target."""
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    target = tmp_path / "uploads" / "DLTEST00001.mp4"
    target.write_bytes(b"\x00" * 16)

    def fake_download(video_id, target_dir):
        # Mirrors the real signature; just hand back our fake path.
        return target

    monkeypatch.setattr(auto_actions.youtube, "download_video_file", fake_download)

    await auto_actions._maybe_download_video_file("DLTEST00001")
    state, path = await _state(db, "DLTEST00001")
    assert state is None
    assert path == str(target)


@pytest.mark.asyncio
async def test_private_video_marks_unavailable(
    db, monkeypatch: pytest.MonkeyPatch,
) -> None:
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    def fake_download(video_id, target_dir):
        raise auto_actions.youtube.PrivateVideoError("yes private")

    monkeypatch.setattr(auto_actions.youtube, "download_video_file", fake_download)

    await auto_actions._maybe_download_video_file("DLTEST00001")
    state, path = await _state(db, "DLTEST00001")
    assert state == "unavailable"
    assert path is None


@pytest.mark.asyncio
async def test_generic_failure_marks_failed(
    db, monkeypatch: pytest.MonkeyPatch,
) -> None:
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    def fake_download(video_id, target_dir):
        raise RuntimeError("network blip")

    monkeypatch.setattr(auto_actions.youtube, "download_video_file", fake_download)

    await auto_actions._maybe_download_video_file("DLTEST00001")
    state, path = await _state(db, "DLTEST00001")
    assert state == "failed"
    assert path is None


@pytest.mark.asyncio
async def test_does_not_overwrite_user_attached_master(
    db, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A user-attached master must not be clobbered by a re-download.
    Regression test for the code-review fix that protects
    source_file_origin='user_attached' from _maybe_download_video_file.
    """
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    # Set up the row as if Replace source had already run.
    master = tmp_path / "uploads" / "MASTER.mov"
    master.write_bytes(b"\xff" * 32)
    await db.execute(
        "UPDATE videos SET source_file_origin = 'user_attached', "
        "video_file_path = ? WHERE id = ?",
        (str(master), "DLTEST00001"),
    )
    await db.commit()

    download_calls: list[str] = []

    def fake_download(video_id, target_dir):
        download_calls.append(video_id)
        return target_dir / "lossy.mp4"

    monkeypatch.setattr(auto_actions.youtube, "download_video_file", fake_download)
    await auto_actions._maybe_download_video_file("DLTEST00001")

    # No download attempted; the row still points at the master.
    assert download_calls == []
    state, path = await _state(db, "DLTEST00001")
    assert path == str(master)
    # State column untouched (no in_progress / failed transition).
    assert state is None
