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
    record = await prompts.get_prompt_template(
        "description_from_transcript_prompt", project_id=1
    )
    assert record is not None
    assert "{{transcript_truncated}}" in record["body"]


async def test_seeded_tags_from_metadata_present(prompts_env) -> None:
    prompts, _db = prompts_env
    record = await prompts.get_prompt_template(
        "tags_from_metadata_prompt", project_id=1
    )
    assert record is not None
    assert "comma-separated" in record["body"]


async def test_fallback_when_row_missing(prompts_env, monkeypatch) -> None:
    prompts, db = prompts_env
    # Wipe the row to simulate a fresh install pre-migration.
    await db.execute(
        "DELETE FROM prompt_templates WHERE key = 'description_from_transcript_prompt'"
    )
    await db.commit()
    body = await prompts.get_prompt_body_with_fallback(
        "description_from_transcript_prompt", project_id=1
    )
    assert "Generate an SEO-friendly YouTube video description" in body


async def test_unknown_key_raises(prompts_env) -> None:
    prompts, _db = prompts_env
    with pytest.raises(KeyError):
        await prompts.get_prompt_body_with_fallback("not_a_real_key", project_id=1)


async def test_upsert_round_trip(prompts_env) -> None:
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="custom_thing",
        name="Custom thing",
        body="Hello {{title}}",
        applies_to=["hook", "short"],
        project_id=1,
    )
    record = await prompts.get_prompt_template("custom_thing", project_id=1)
    assert record is not None
    assert record["body"] == "Hello {{title}}"
    assert record["applies_to"] == ["hook", "short"]


async def test_upsert_replaces_existing(prompts_env) -> None:
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="description_from_transcript_prompt",
        name="My override",
        body="Custom body {{title}}",
        project_id=1,
    )
    record = await prompts.get_prompt_template(
        "description_from_transcript_prompt", project_id=1
    )
    assert record["name"] == "My override"
    assert record["body"] == "Custom body {{title}}"


async def test_get_prompt_with_fallback_uses_seed_system(prompts_env) -> None:
    """Seed system prompt surfaces even when no row is saved."""
    prompts, db = prompts_env
    # No row for shorten_post_prompt in a fresh DB; the system seed should
    # still come through the fallback.
    await db.execute(
        "DELETE FROM prompt_templates WHERE key = 'shorten_post_prompt'"
    )
    await db.commit()
    record = await prompts.get_prompt_with_fallback(
        "shorten_post_prompt", project_id=1
    )
    assert "Shorten this social post" in record["body"]
    assert record["system"] is not None
    assert "rewrite social media posts to be shorter" in record["system"]


async def test_get_prompt_with_fallback_body_and_system_fall_back_independently(
    prompts_env,
) -> None:
    """User edits only the body; the system prompt keeps the seed default."""
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="tags_from_metadata_prompt",
        name="My tags",
        body="My custom body",
        project_id=1,
        system=None,  # explicit: don't override the system
    )
    record = await prompts.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=1
    )
    assert record["body"] == "My custom body"
    # Seed system surfaces because the user didn't override it.
    assert record["system"] == prompts.SEED_TAGS_FROM_METADATA_PROMPT.system


async def test_get_prompt_with_fallback_user_system_override(prompts_env) -> None:
    """A user-saved system_body wins over the seed."""
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="tags_from_metadata_prompt",
        name="My tags",
        body="My custom body",
        system="my custom system",
        project_id=1,
    )
    record = await prompts.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=1
    )
    assert record["system"] == "my custom system"


async def test_seeded_ai_block_default_system_present(prompts_env) -> None:
    """The system-only seed has an empty body but a real system string."""
    prompts, _db = prompts_env
    record = await prompts.get_prompt_with_fallback(
        "ai_block_default_system_prompt", project_id=1
    )
    assert record["body"] == ""
    assert record["system"] is not None
    assert "social media copywriter" in record["system"]


# ---------------------------------------------------------------------------
# Regression tests: a saved-but-empty/whitespace body must FAIL LOUDLY rather
# than silently fall back to the seed. A blank saved body means the user cleared
# the editor; surfacing the error stops a blank prompt from quietly driving
# generation. (A *missing* row still uses the seed — that's the intended default,
# covered by test_fallback_when_row_missing.)
# ---------------------------------------------------------------------------

async def test_empty_body_raises(prompts_env) -> None:
    """Upserting body='' for a seeded key must raise, not return the seed."""
    import pytest

    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="tags_from_metadata_prompt",
        name="Tags from metadata",
        body="",
        project_id=1,
    )
    with pytest.raises(prompts.EmptyPromptBodyError):
        await prompts.get_prompt_with_fallback(
            "tags_from_metadata_prompt", project_id=1
        )


async def test_whitespace_only_body_raises(prompts_env) -> None:
    """Upserting body='   ' (whitespace only) must also raise."""
    import pytest

    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="tags_from_metadata_prompt",
        name="Tags from metadata",
        body="   ",
        project_id=1,
    )
    with pytest.raises(prompts.EmptyPromptBodyError):
        await prompts.get_prompt_with_fallback(
            "tags_from_metadata_prompt", project_id=1
        )


async def test_non_empty_custom_body_is_preserved(prompts_env) -> None:
    """A genuinely non-empty saved body must win over the seed — not replaced."""
    prompts, _db = prompts_env
    await prompts.upsert_prompt_template(
        key="tags_from_metadata_prompt",
        name="Tags from metadata",
        body="My custom tags prompt",
        project_id=1,
    )
    record = await prompts.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=1
    )
    assert record["body"] == "My custom tags prompt"
