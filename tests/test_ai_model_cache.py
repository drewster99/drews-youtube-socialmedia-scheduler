"""Anthropic model cache: sync and async resolvers honour Settings → Model
and the cache busts on save."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def ai_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    ai = importlib.import_module("yt_scheduler.services.ai")
    db = await database.get_db()
    await projects.ensure_default_project()
    yield ai, db
    await database.close_db()


def test_sync_resolver_returns_env_default_when_no_db_row(ai_env, monkeypatch) -> None:
    ai, _db = ai_env
    monkeypatch.setattr(ai, "ANTHROPIC_MODEL", "test-default-model")
    ai.invalidate_model_cache()
    assert ai._resolve_model_sync() == "test-default-model"


async def test_sync_resolver_picks_up_db_value(ai_env) -> None:
    ai, db = ai_env
    await db.execute(
        "INSERT INTO settings (key, value) VALUES ('anthropic_model', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        ("claude-haiku-4-5",),
    )
    await db.commit()
    ai.invalidate_model_cache()
    assert ai._resolve_model_sync() == "claude-haiku-4-5"


async def test_cache_invalidation_picks_up_changes(ai_env) -> None:
    ai, db = ai_env
    await db.execute(
        "INSERT INTO settings (key, value) VALUES ('anthropic_model', 'first-model')"
    )
    await db.commit()
    ai.invalidate_model_cache()
    assert ai._resolve_model_sync() == "first-model"

    await db.execute(
        "UPDATE settings SET value = 'second-model' WHERE key = 'anthropic_model'"
    )
    await db.commit()
    # Without invalidation, the cached value sticks.
    assert ai._resolve_model_sync() == "first-model"

    ai.invalidate_model_cache()
    assert ai._resolve_model_sync() == "second-model"


async def test_async_and_sync_resolvers_agree(ai_env) -> None:
    ai, db = ai_env
    await db.execute(
        "INSERT INTO settings (key, value) VALUES ('anthropic_model', 'shared-model')"
    )
    await db.commit()
    ai.invalidate_model_cache()
    assert await ai._resolve_model() == "shared-model"
    assert ai._resolve_model_sync() == "shared-model"
