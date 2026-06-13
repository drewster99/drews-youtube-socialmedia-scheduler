"""Restart survival for pre-INSERT promo-chain jobs (migration 030).

A promo clip's pre-INSERT state (cut params + title) lives only in memory, so a
crash before the YouTube upload + row INSERT used to strand it. ``pending_promo_jobs``
persists that state and ``resume_pending_promo_jobs`` re-spawns it on startup.

The safety-critical branch: a job whose upload already finalized
(``youtube_video_id`` set) must NEVER be re-uploaded on resume — that would
duplicate the YouTube video, the exact failure we were fixing.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

import yt_scheduler.database as database
from yt_scheduler.migrations import apply_migrations


async def _make_conn(tmp_path: Path) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(tmp_path / "test.db"))
    conn.row_factory = aiosqlite.Row
    await apply_migrations(conn)
    return conn


async def _insert_pending(conn: aiosqlite.Connection, **overrides) -> None:
    row = {
        "job_id": "job_test",
        "project_id": 1,
        "parent_id": "PARENT0001",
        "forced_item_type": "hook",
        "original_filename": "clip.mp4",
        "title": "A Clip",
        "parent_video_path": "/tmp/parent.mp4",
        "local_path": None,
        "cut_start_seconds": 10.0,
        "cut_end_seconds": 25.0,
        "vertical_crop": 0,
        "x_shift_normalized": 0.0,
        "audio_fade_in": 0.0,
        "audio_fade_out": 0.0,
        "youtube_video_id": None,
        "status": "pending",
    }
    row.update(overrides)
    cols = ", ".join(row.keys())
    placeholders = ", ".join("?" for _ in row)
    await conn.execute(
        f"INSERT INTO pending_promo_jobs ({cols}) VALUES ({placeholders})",
        tuple(row.values()),
    )
    await conn.commit()


@pytest.fixture
def wired(monkeypatch, tmp_path):
    """auto_actions wired to a fresh migrated DB and a spawn-recorder that does
    NOT run the chain (we only assert the resumer's branching)."""
    from yt_scheduler.services import auto_actions as aa

    aa._UPLOAD_JOBS.clear()
    spawned: list[str] = []

    def _stub_spawn(coro, *, name):
        coro.close()  # never run the chain in the test
        spawned.append(name)

    monkeypatch.setattr(aa, "spawn_background", _stub_spawn)
    return aa, spawned


async def _status(conn: aiosqlite.Connection, job_id: str) -> str:
    rows = await conn.execute_fetchall(
        "SELECT status FROM pending_promo_jobs WHERE job_id = ?", (job_id,)
    )
    return rows[0]["status"]


async def test_uploaded_but_uninserted_is_not_re_uploaded(wired, tmp_path, monkeypatch):
    aa, spawned = wired
    conn = await _make_conn(tmp_path)

    # Make the migrated temp connection the module singleton so BOTH the
    # reads (auto_actions.get_db) AND the writes (write_transaction, which
    # calls database.get_db internally) operate on the same DB.
    monkeypatch.setattr(database, "_db", conn)
    await _insert_pending(conn, youtube_video_id="YTVIDEO123")

    resumed = await aa.resume_pending_promo_jobs(window_hours=24)

    assert resumed == 0
    assert spawned == [], "must not re-spawn (would re-upload a duplicate)"
    assert "job_test" not in aa._UPLOAD_JOBS
    assert await _status(conn, "job_test") == "failed"
    await conn.close()


async def test_clean_pending_job_is_respawned(wired, tmp_path, monkeypatch):
    aa, spawned = wired
    conn = await _make_conn(tmp_path)

    # Make the migrated temp connection the module singleton so BOTH the
    # reads (auto_actions.get_db) AND the writes (write_transaction, which
    # calls database.get_db internally) operate on the same DB.
    monkeypatch.setattr(database, "_db", conn)
    cut_file = tmp_path / "clip.mp4"
    cut_file.write_bytes(b"\x00" * 16)
    await _insert_pending(conn, local_path=str(cut_file))

    resumed = await aa.resume_pending_promo_jobs(window_hours=24)

    assert resumed == 1
    assert len(spawned) == 1
    job = aa._UPLOAD_JOBS.get("job_test")
    assert job is not None
    assert job["local_path"] == str(cut_file)
    assert job["pre_supplied_title"] == "A Clip"
    assert job["forced_item_type"] == "hook"
    await conn.close()


async def test_missing_cut_file_and_no_recut_params_is_failed(wired, tmp_path, monkeypatch):
    aa, spawned = wired
    conn = await _make_conn(tmp_path)

    # Make the migrated temp connection the module singleton so BOTH the
    # reads (auto_actions.get_db) AND the writes (write_transaction, which
    # calls database.get_db internally) operate on the same DB.
    monkeypatch.setattr(database, "_db", conn)
    # No parent_video_path and a local_path that doesn't exist -> can't recut.
    await _insert_pending(
        conn, parent_video_path=None, cut_start_seconds=None,
        local_path=str(tmp_path / "gone.mp4"),
    )

    resumed = await aa.resume_pending_promo_jobs(window_hours=24)

    assert resumed == 0
    assert spawned == []
    assert await _status(conn, "job_test") == "failed"
    await conn.close()
