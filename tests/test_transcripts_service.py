"""Transcript service + migration tests."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import aiosqlite
import pytest


@pytest.fixture
async def transcripts_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    transcripts = importlib.import_module("yt_scheduler.services.transcripts")
    db = await database.get_db()
    await projects.ensure_default_project()
    await db.execute(
        "INSERT INTO videos (id, project_id, title) VALUES ('vid1', 1, 'Test')"
    )
    await db.commit()
    yield transcripts, db
    await database.close_db()


async def test_add_and_list(transcripts_env) -> None:
    transcripts, _db = transcripts_env
    a = await transcripts.add_transcript("vid1", "youtube", "youtube text")
    b = await transcripts.add_transcript(
        "vid1", "mlx_whisper", "whisper text", source_detail="large-v3"
    )
    items = await transcripts.list_transcripts("vid1")
    assert {t["id"] for t in items} == {a, b}
    sources = {t["source"]: t for t in items}
    assert sources["mlx_whisper"]["source_detail"] == "large-v3"


async def test_invalid_source_rejected(transcripts_env) -> None:
    transcripts, _db = transcripts_env
    with pytest.raises(ValueError):
        await transcripts.add_transcript("vid1", "what", "text")


async def test_upsert_dedup_per_source_detail(transcripts_env) -> None:
    transcripts, _db = transcripts_env
    a = await transcripts.upsert_transcript_for_source(
        "vid1", "mlx_whisper", "v1 text", source_detail="large-v3"
    )
    b = await transcripts.upsert_transcript_for_source(
        "vid1", "mlx_whisper", "v2 text", source_detail="large-v3"
    )
    assert a == b  # same row updated
    c = await transcripts.upsert_transcript_for_source(
        "vid1", "mlx_whisper", "tiny text", source_detail="tiny"
    )
    assert c != a  # different model -> different row


async def test_set_active_mirrors_to_videos(transcripts_env) -> None:
    transcripts, db = transcripts_env
    tid = await transcripts.add_transcript("vid1", "youtube", "yt text")
    result = await transcripts.set_active_transcript(
        "vid1", tid, text="yt text", is_edited=False
    )
    assert result["transcript_id"] == tid
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = 'vid1'")
    row = dict(rows[0])
    assert row["transcript"] == "yt text"
    assert row["transcript_source"] == "youtube"
    assert row["transcript_is_edited"] == 0


async def test_set_active_marks_edited_when_text_differs(transcripts_env) -> None:
    transcripts, db = transcripts_env
    tid = await transcripts.add_transcript("vid1", "youtube", "yt text")
    await transcripts.set_active_transcript(
        "vid1", tid, text="my edits", is_edited=True
    )
    rows = await db.execute_fetchall(
        "SELECT transcript, transcript_is_edited FROM videos WHERE id = 'vid1'"
    )
    row = dict(rows[0])
    assert row["transcript"] == "my edits"
    assert row["transcript_is_edited"] == 1


async def test_set_active_rejects_cross_video(transcripts_env) -> None:
    transcripts, db = transcripts_env
    await db.execute(
        "INSERT INTO videos (id, project_id, title) VALUES ('vid2', 1, 'Two')"
    )
    await db.commit()
    tid = await transcripts.add_transcript("vid2", "youtube", "two text")
    with pytest.raises(ValueError, match="does not belong"):
        await transcripts.set_active_transcript(
            "vid1", tid, text="two text", is_edited=False
        )


async def test_legacy_transcript_migrated_to_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Pre-existing single-column transcripts should produce a user_edited row."""
    db_path = tmp_path / "p.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        from yt_scheduler.migrations import MIGRATIONS_DIR

        # Create the baseline schema directly + insert a row with a transcript.
        baseline = (MIGRATIONS_DIR / "001_baseline.sql").read_text()
        await conn.executescript(baseline)
        await conn.execute(
            "INSERT INTO videos (id, title, transcript, created_at) "
            "VALUES ('vid_legacy', 'Old', 'legacy text', '2024-01-01 00:00:00')"
        )
        await conn.commit()

        from yt_scheduler.migrations import apply_migrations

        await apply_migrations(conn)
        cursor = await conn.execute(
            "SELECT video_id, source, text, created_at FROM transcripts"
        )
        rows = await cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "vid_legacy"
        assert rows[0][1] == "user_edited"
        assert rows[0][2] == "legacy text"
        assert rows[0][3] == "2024-01-01 00:00:00"

        cursor = await conn.execute(
            "SELECT transcript_id, transcript_source, transcript_created_at "
            "FROM videos WHERE id = 'vid_legacy'"
        )
        v = await cursor.fetchone()
        assert v[0] is not None
        assert v[1] == "user_edited"
        assert v[2] == "2024-01-01 00:00:00"


async def test_video_delete_cascades_transcripts(transcripts_env) -> None:
    transcripts, db = transcripts_env
    await transcripts.add_transcript("vid1", "youtube", "text")
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("DELETE FROM videos WHERE id = 'vid1'")
    await db.commit()
    items = await transcripts.list_transcripts("vid1")
    assert items == []
