"""Prompt template service tests."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def prompts_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    prompts = importlib.import_module("yt_scheduler.services.prompts")
    db = await database.get_db()
    await projects.ensure_default_project()
    yield prompts, db
    await database.close_db()


async def test_seeded_description_from_transcript_present(prompts_env) -> None:
    prompts, _db = prompts_env
    record = await prompts.get_prompt_template("description_from_transcript")
    assert record is not None
    assert "{{transcript_truncated}}" in record["body"]


async def test_seeded_tags_from_metadata_present(prompts_env) -> None:
    prompts, _db = prompts_env
    record = await prompts.get_prompt_template("tags_from_metadata")
    assert record is not None
    assert "comma-separated" in record["body"]


async def test_fallback_when_row_missing(prompts_env, monkeypatch) -> None:
    prompts, db = prompts_env
    # Wipe the row to simulate a fresh install pre-migration.
    await db.execute("DELETE FROM prompt_templates WHERE key = 'description_from_transcript'")
    await db.commit()
    body = await prompts.get_prompt_body_with_fallback("description_from_transcript")
    assert "Generate an SEO-friendly YouTube video description" in body


async def test_unknown_key_raises(prompts_env) -> None:
    prompts, _db = prompts_env
    with pytest.raises(KeyError):
        await prompts.get_prompt_body_with_fallback("not_a_real_key")


async def test_upsert_round_trip(prompts_env) -> None:
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="custom_thing",
        name="Custom thing",
        body="Hello {{title}}",
        applies_to=["hook", "short"],
    )
    record = await prompts.get_prompt_template("custom_thing")
    assert record is not None
    assert record["body"] == "Hello {{title}}"
    assert record["applies_to"] == ["hook", "short"]


async def test_upsert_replaces_existing(prompts_env) -> None:
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="description_from_transcript",
        name="My override",
        body="Custom body {{title}}",
    )
    record = await prompts.get_prompt_template("description_from_transcript")
    assert record["name"] == "My override"
    assert record["body"] == "Custom body {{title}}"
