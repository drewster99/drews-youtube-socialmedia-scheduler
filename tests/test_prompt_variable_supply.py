"""Phase-2 variable supply: the {{transcript*}} family and
templates.build_prompt_variables (the merged dict AI prompt bodies render
with, so description/tags prompts can reference the parent video and
inherited item variables like {{apple}})."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

from yt_scheduler.services.transcripts import (
    TRANSCRIPT_PROMPT_CHAR_BUDGET,
    TRANSCRIPT_SRT_PROMPT_CHAR_BUDGET,
    transcript_prompt_variables,
    truncate_srt_at_cue_boundary,
)

SAMPLE_SRT = (
    "1\n00:00:00,000 --> 00:00:02,000\nhello there\n\n"
    "2\n00:00:02,000 --> 00:00:04,000\nsecond cue\n\n"
    "3\n00:00:04,000 --> 00:00:06,000\nthird cue\n"
)


def test_transcript_family_shapes():
    fam = transcript_prompt_variables(SAMPLE_SRT)
    assert set(fam) == {
        "transcript", "transcript_truncated",
        "transcript_srt", "transcript_srt_truncated",
    }
    assert fam["transcript"] == "hello there\nsecond cue\nthird cue"
    assert fam["transcript_srt"] == SAMPLE_SRT
    # Short input: truncated forms equal the full forms.
    assert fam["transcript_truncated"] == fam["transcript"]
    assert fam["transcript_srt_truncated"] == fam["transcript_srt"]


def test_transcript_family_empty_input():
    for empty in ("", None):
        fam = transcript_prompt_variables(empty)
        assert all(v == "" for v in fam.values())


def test_transcript_truncated_uses_shared_budget():
    long_plainish_srt = (
        "1\n00:00:00,000 --> 00:00:02,000\n" + "word " * 5000
    )
    fam = transcript_prompt_variables(long_plainish_srt)
    assert len(fam["transcript_truncated"]) == TRANSCRIPT_PROMPT_CHAR_BUDGET
    assert fam["transcript"].startswith(fam["transcript_truncated"])


def test_srt_truncation_cuts_at_cue_boundary():
    cue = "1\n00:00:00,000 --> 00:00:02,000\nhello hello hello\n"
    many_cues = "\n\n".join(
        cue.replace("1\n", f"{i}\n") for i in range(1, 2000)
    )
    out = truncate_srt_at_cue_boundary(many_cues, TRANSCRIPT_SRT_PROMPT_CHAR_BUDGET)
    assert len(out) <= TRANSCRIPT_SRT_PROMPT_CHAR_BUDGET
    # Must end with a complete cue, not a chopped timestamp line.
    assert out.endswith("hello hello hello")


def test_srt_truncation_falls_back_to_hard_cut_for_unstructured_text():
    blob = "x" * 500
    assert truncate_srt_at_cue_boundary(blob, 100) == "x" * 100


def test_srt_truncation_noop_when_under_budget():
    assert truncate_srt_at_cue_boundary(SAMPLE_SRT, 10_000) == SAMPLE_SRT


# --- build_prompt_variables ------------------------------------------------


@pytest.fixture
def reset_modules() -> None:
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)


@pytest.mark.asyncio
async def test_build_prompt_variables_for_promo_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)

    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()

    await db.execute(
        "UPDATE projects SET project_url = ? WHERE id = 1",
        ("https://youtube.com/@chan",),
    )
    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, url, transcript)
           VALUES (?, 1, ?, ?, ?, 'unlisted', 'uploaded', 'episode', ?, ?)""",
        (
            "parentid001", "Parent Title", "Parent description.",
            json.dumps(["swift", "ai"]), "https://youtu.be/parentid001",
            SAMPLE_SRT,
        ),
    )
    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, parent_item_id, url, transcript)
           VALUES (?, 1, ?, ?, ?, 'unlisted', 'uploaded', 'short', ?, ?, ?)""",
        (
            "childid0001", "Child Promo", "", json.dumps([]),
            "parentid001", "https://youtu.be/childid0001",
            "1\n00:00:00,000 --> 00:00:01,000\nclip line\n",
        ),
    )
    # {{apple}} lives on the PARENT; the child must inherit it.
    await db.execute(
        "INSERT INTO item_variables (video_id, key, value) VALUES (?, ?, ?)",
        ("parentid001", "apple", "https://podcasts.apple.com/ep1"),
    )
    await db.commit()

    templates = importlib.import_module("yt_scheduler.services.templates")
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", ("childid0001",)
    )
    variables = await templates.build_prompt_variables(dict(rows[0]))

    assert variables["apple"] == "https://podcasts.apple.com/ep1"
    assert variables["parent_url"] == "https://youtu.be/parentid001"
    assert variables["episode_url"] == "https://youtu.be/parentid001"
    assert variables["url"] == "https://youtu.be/childid0001"
    assert variables["project_url"] == "https://youtube.com/@chan"
    assert variables["parent_title"] == "Parent Title"
    assert variables["parent_tags"] == "swift, ai"
    assert "Parent URL: https://youtu.be/parentid001" in variables["parent_context_block"]
    # Transcript family comes from the CHILD's transcript.
    assert variables["transcript"] == "clip line"
    assert variables["transcript_srt"].startswith("1\n00:00:00")
    # Builtins never inherit from the parent.
    assert variables["title"] == "Child Promo"

    await db_module.close_db()


@pytest.mark.asyncio
async def test_build_prompt_variables_no_parent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)

    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    await db.execute(
        """INSERT INTO videos
           (id, project_id, title, description, tags, privacy_status,
            status, item_type, url)
           VALUES (?, 1, ?, ?, ?, 'unlisted', 'uploaded', 'episode', ?)""",
        ("soloid00001", "Solo", "", json.dumps([]), "https://youtu.be/x"),
    )
    await db.commit()

    templates = importlib.import_module("yt_scheduler.services.templates")
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", ("soloid00001",)
    )
    variables = await templates.build_prompt_variables(dict(rows[0]))

    # Parent family defined-but-blank: {{parent_url??}} and {{#parent_url}}
    # both behave, and strict bare references don't raise.
    assert variables["parent_url"] == ""
    assert variables["episode_url"] == ""
    assert variables["parent_context_block"] == ""
    assert variables["transcript"] == ""

    await db_module.close_db()


@pytest.mark.asyncio
async def test_build_prompt_variables_rejects_missing_project_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    importlib.import_module("yt_scheduler.database")
    templates = importlib.import_module("yt_scheduler.services.templates")
    with pytest.raises(ValueError):
        await templates.build_prompt_variables({"id": "x", "project_id": None})
