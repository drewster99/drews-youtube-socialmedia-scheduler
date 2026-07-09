"""Comment moderation must be exactly-once per (project, comment).

_process_one used to SELECT for an existing log row, `await` the YouTube
moderate call, then INSERT. The periodic sweep and a manual "run now" share one
event loop, so both could pass the SELECT before either INSERTed — moderating the
comment twice (wasted quota) and writing two log rows.

Migration 032 adds a unique index on (project_id, comment_id) and the claim is an
INSERT OR IGNORE, so the database decides the winner.
"""

from __future__ import annotations

import asyncio
import importlib

import pytest


def _comment(comment_id: str, text: str = "buy cheap watches") -> dict:
    return {
        "id": comment_id,
        "snippet": {"textDisplay": text, "authorDisplayName": "spammer"},
    }


BLOCKLIST = [{"keyword": "cheap watches", "is_regex": False}]


@pytest.fixture
async def moderation(isolated_db):
    module = importlib.import_module("yt_scheduler.services.moderation")
    await isolated_db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vid00000001', 1, 'v', 'published')"
    )
    await isolated_db.commit()
    return module


async def _log_rows(db, comment_id: str) -> list:
    return await db.execute_fetchall(
        "SELECT action FROM moderation_log WHERE comment_id = ?", (comment_id,)
    )


async def test_unique_index_exists(isolated_db) -> None:
    rows = await isolated_db.execute_fetchall(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        ("idx_moderation_log_comment_unique",),
    )
    assert rows, "migration 032 did not create the unique index"


async def test_concurrent_sweeps_moderate_and_log_exactly_once(
    moderation, isolated_db, monkeypatch
) -> None:
    calls: list[str] = []

    def fake_moderate(comment_id: str, action: str) -> None:
        calls.append(comment_id)

    monkeypatch.setattr(moderation.youtube, "moderate_comment", fake_moderate)

    await asyncio.gather(*[
        moderation._process_one(
            isolated_db, 1, "vid00000001", _comment("c1"), BLOCKLIST
        )
        for _ in range(2)
    ])

    assert calls == ["c1"], f"comment moderated {len(calls)} times"
    rows = await _log_rows(isolated_db, "c1")
    assert len(rows) == 1
    assert rows[0]["action"] == "deleted"


async def test_already_completed_comment_is_not_reprocessed(
    moderation, isolated_db, monkeypatch
) -> None:
    """YouTube keeps returning already-rejected comments on every sweep."""
    calls: list[str] = []
    monkeypatch.setattr(
        moderation.youtube, "moderate_comment", lambda cid, action: calls.append(cid)
    )

    await moderation._process_one(
        isolated_db, 1, "vid00000001", _comment("c2"), BLOCKLIST
    )
    await moderation._process_one(
        isolated_db, 1, "vid00000001", _comment("c2"), BLOCKLIST
    )

    assert calls == ["c2"]
    assert len(await _log_rows(isolated_db, "c2")) == 1


async def test_failed_moderation_is_recorded_not_swallowed(
    moderation, isolated_db, monkeypatch
) -> None:
    def boom(comment_id: str, action: str) -> None:
        raise RuntimeError("quota exceeded")

    monkeypatch.setattr(moderation.youtube, "moderate_comment", boom)

    result = await moderation._process_one(
        isolated_db, 1, "vid00000001", _comment("c3"), BLOCKLIST
    )

    rows = await _log_rows(isolated_db, "c3")
    assert len(rows) == 1
    assert rows[0]["action"] == "error"
    assert "quota exceeded" in result[0]["error"]


async def test_stale_pending_claim_is_retried(
    moderation, isolated_db, monkeypatch
) -> None:
    """A run that died between claiming and recording must not strand the comment."""
    await isolated_db.execute(
        "INSERT INTO moderation_log "
        "(project_id, video_id, comment_id, action, created_at) "
        "VALUES (1, 'vid00000001', 'c4', 'pending', datetime('now', '-60 minutes'))"
    )
    await isolated_db.commit()

    calls: list[str] = []
    monkeypatch.setattr(
        moderation.youtube, "moderate_comment", lambda cid, action: calls.append(cid)
    )

    await moderation._process_one(
        isolated_db, 1, "vid00000001", _comment("c4"), BLOCKLIST
    )

    assert calls == ["c4"], "a stale pending claim must be retried"
    rows = await _log_rows(isolated_db, "c4")
    assert len(rows) == 1
    assert rows[0]["action"] == "deleted"


async def test_fresh_pending_claim_is_left_alone(
    moderation, isolated_db, monkeypatch
) -> None:
    """A pending row from a concurrent live run must not be stolen."""
    await isolated_db.execute(
        "INSERT INTO moderation_log (project_id, video_id, comment_id, action) "
        "VALUES (1, 'vid00000001', 'c5', 'pending')"
    )
    await isolated_db.commit()

    calls: list[str] = []
    monkeypatch.setattr(
        moderation.youtube, "moderate_comment", lambda cid, action: calls.append(cid)
    )

    result = await moderation._process_one(
        isolated_db, 1, "vid00000001", _comment("c5"), BLOCKLIST
    )

    assert calls == []
    assert result == []


async def test_pending_rows_are_hidden_from_the_log_ui(moderation, isolated_db) -> None:
    await isolated_db.execute(
        "INSERT INTO moderation_log (project_id, video_id, comment_id, action) "
        "VALUES (1, 'vid00000001', 'c6', 'pending')"
    )
    await isolated_db.commit()

    entries = await moderation.get_moderation_log(project_id=1)

    assert all(e["action"] != "pending" for e in entries)
