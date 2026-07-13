"""Promo prompt-variant resolution (Phase 3).

For a promo child, the ``<key>_promo`` variant is a distinct prompt that
wins outright when it exists in any form:

    saved promo row → promo seed → saved base row → base seed

The promo seed deliberately beats a saved base row because migration 006
seeds base ROWS on every install — row-existence can't distinguish a user
customisation from install defaults.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
def reset_modules() -> None:
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)


@pytest.fixture
async def prompts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_modules: None):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    db_module = importlib.import_module("yt_scheduler.database")
    await db_module.get_db()
    yield importlib.import_module("yt_scheduler.services.prompts")
    await db_module.close_db()


DESC_KEY = "description_from_transcript_prompt"


@pytest.mark.asyncio
async def test_promo_seed_used_when_no_promo_row(prompts) -> None:
    """Even though migration 006 seeds a base ROW, the promo seed wins for
    promos — that's the whole point of the variant."""
    resolved = await prompts.get_prompt_with_fallback(
        DESC_KEY, project_id=1, prefer_promo_variant=True
    )
    assert resolved["body"] == prompts._SEEDS_BY_KEY[f"{DESC_KEY}_promo"].body


@pytest.mark.asyncio
async def test_non_promo_resolution_ignores_promo_variant(prompts) -> None:
    resolved = await prompts.get_prompt_with_fallback(DESC_KEY, project_id=1)
    # Migration 006 seeds a base row; non-promo resolution returns it (or
    # the code seed on installs without the row) — never the promo seed.
    assert "promo clip" not in resolved["body"]


@pytest.mark.asyncio
async def test_promo_seed_beats_customised_base_row(prompts) -> None:
    """Promos use the promo prompt even when the base row was edited —
    base and promo are separately editable prompts, and a migration-006
    row is indistinguishable from a customisation anyway."""
    await prompts.upsert_prompt_template(
        key=DESC_KEY, name="Custom", body="MY CUSTOM BASE {{title}}",
        project_id=1,
    )
    resolved = await prompts.get_prompt_with_fallback(
        DESC_KEY, project_id=1, prefer_promo_variant=True
    )
    assert resolved["body"] == prompts._SEEDS_BY_KEY[f"{DESC_KEY}_promo"].body
    # Non-promo resolution still honours the customisation.
    base = await prompts.get_prompt_with_fallback(DESC_KEY, project_id=1)
    assert base["body"] == "MY CUSTOM BASE {{title}}"


@pytest.mark.asyncio
async def test_saved_promo_row_beats_everything(prompts) -> None:
    await prompts.upsert_prompt_template(
        key=DESC_KEY, name="Custom base", body="BASE", project_id=1,
    )
    await prompts.upsert_prompt_template(
        key=f"{DESC_KEY}_promo", name="Custom promo", body="PROMO {{title}}",
        project_id=1,
    )
    resolved = await prompts.get_prompt_with_fallback(
        DESC_KEY, project_id=1, prefer_promo_variant=True
    )
    assert resolved["body"] == "PROMO {{title}}"
    # Non-promo resolution still gets the base row.
    base = await prompts.get_prompt_with_fallback(DESC_KEY, project_id=1)
    assert base["body"] == "BASE"


@pytest.mark.asyncio
async def test_promo_variant_falls_back_to_base_when_no_promo_seed(prompts) -> None:
    """Keys without a shipped promo seed (e.g. tags) resolve to the base
    flow when prefer_promo_variant is set — the flag is safe to pass
    unconditionally from promo callers."""
    promo = await prompts.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=1, prefer_promo_variant=True
    )
    base = await prompts.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=1
    )
    assert promo == base


def test_promo_seed_footer_renders_with_and_without_parent(reset_modules) -> None:
    """The promo seed's {{#episode_url}} footer block: present for a promo
    with a parent URL, absent (including the instruction line) without."""
    from yt_scheduler.services import templates
    from yt_scheduler.services.prompts import _SEEDS_BY_KEY

    body = _SEEDS_BY_KEY[f"{DESC_KEY}_promo"].body
    base_vars = {
        "title": "Clip", "channel_name_block": "",
        "transcript": "words", "transcript_truncated": "words",
        "transcript_srt": "", "transcript_srt_truncated": "",
        "extra_instructions": "", "parent_context_block": "",
    }

    with_parent = templates.render(
        body, {**base_vars, "episode_url": "https://youtu.be/parent1"}
    )
    assert "Full episode: https://youtu.be/parent1" in with_parent
    assert "End the description with this line" in with_parent

    without_parent = templates.render(body, {**base_vars, "episode_url": ""})
    assert "Full episode" not in without_parent
    assert "End the description with this line" not in without_parent


def test_base_description_seed_uses_srt_transcript(reset_modules) -> None:
    """The chapters instruction is only honest if Claude actually sees
    timestamps — the base seed must feed the SRT form."""
    from yt_scheduler.services.prompts import _SEEDS_BY_KEY

    body = _SEEDS_BY_KEY[DESC_KEY].body
    assert "{{transcript_srt_truncated}}" in body
    assert "HH:MM:SS" in body  # conversion guidance present
