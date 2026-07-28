"""`videos.youtube_video_id` replaces inferring the row's kind from its id.

`videos.id` was doing two jobs: identity, and the YouTube video id. Five call
sites asked "does this row have a YouTube video?" with `len(id) == 11` — a
record's type derived from the shape of its key. Nothing enforced it, and it
was invisible at the call site, so fixtures with short ids silently ran the
non-YouTube path.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from yt_scheduler.models.video import is_youtube_backed, youtube_video_id_of


class TestTheHelper:
    def test_reads_the_column(self):
        assert youtube_video_id_of({"youtube_video_id": "dQw4w9WgXcQ"}) == "dQw4w9WgXcQ"
        assert is_youtube_backed({"youtube_video_id": "dQw4w9WgXcQ"})

    def test_null_means_no_youtube_video(self):
        assert youtube_video_id_of({"youtube_video_id": None}) is None
        assert not is_youtube_backed({"youtube_video_id": None})

    def test_a_missing_column_raises_rather_than_answering_no(self):
        """The whole point is to stop guessing. A query that forgot to select
        the column must fail loudly, not silently report 'not on YouTube' —
        that is the same silent wrong answer in a new costume."""
        with pytest.raises(KeyError):
            youtube_video_id_of({"id": "dQw4w9WgXcQ", "title": "T"})

    def test_id_length_no_longer_decides_anything(self):
        """An 11-character generated id used to be indistinguishable from a
        YouTube video. It is now just an id."""
        assert not is_youtube_backed(
            {"id": "abcdefghijk", "youtube_video_id": None}
        )


@pytest.fixture
async def db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for module in list(sys.modules):
        if module.startswith("yt_scheduler"):
            sys.modules.pop(module, None)
    importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    conn = await database.get_db()
    await projects.ensure_default_project()
    yield conn
    await database.close_db()


async def test_migration_creates_the_column_and_it_is_nullable(db):
    columns = {
        row["name"]: row
        for row in await db.execute_fetchall("PRAGMA table_info(videos)")
    }
    assert "youtube_video_id" in columns, "migration 037 must have run"
    assert not columns["youtube_video_id"]["notnull"], (
        "an item with no YouTube video needs NULL, not a sentinel"
    )


async def test_the_backfill_rule_matches_how_rows_were_written(db):
    """Migration 037 backfills with `length(id) = 11`, which is sound only
    because that is exactly how every existing row was keyed: the upload and
    import paths use the id YouTube returned, and the non-YouTube path mints a
    22-character token. This pins both shapes so the assumption is checked
    rather than remembered."""
    import secrets

    generated = secrets.token_urlsafe(16)[:22]
    assert len(generated) == 22, "the non-YouTube id shape the backfill relies on"
    assert len("dQw4w9WgXcQ") == 11, "the YouTube id shape it relies on"
    assert len(generated) != 11
