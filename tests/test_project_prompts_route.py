"""Coverage for the per-project prompts API: GET merges seed + saved row,
PUT supports body and the three-state ``system`` parameter."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def app_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    keychain.store_secret("anthropic", "api_key", "sk-ant-test-fake")

    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    app_module = importlib.import_module("yt_scheduler.app")

    await database.get_db()
    await projects.ensure_default_project()

    transport = ASGITransport(app=app_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    await database.close_db()


async def test_get_prompts_includes_system_and_body_required(app_client) -> None:
    resp = await app_client.get("/api/projects/default/prompts")
    assert resp.status_code == 200
    by_key = {row["key"]: row for row in resp.json()}

    # The system-only seed should hide its body in the UI.
    sys_only = by_key["ai_block_default_system_prompt"]
    assert sys_only["body_required"] is False
    assert sys_only["body"] == ""
    assert "social media copywriter" in sys_only["system"]

    # The tags-from-metadata seed exposes both body and system.
    tags = by_key["tags_from_metadata_prompt"]
    assert tags["body_required"] is True
    assert "comma-separated" in tags["body"]
    assert "comma-separated list of tags" in tags["system"]

    # The description-from-transcript seed has a body but no system.
    desc = by_key["description_from_transcript_prompt"]
    assert desc["body_required"] is True
    assert desc["system"] is None
    assert desc["default_system"] is None


async def test_put_saves_body_and_system(app_client) -> None:
    resp = await app_client.put(
        "/api/projects/default/prompts/tags_from_metadata_prompt",
        json={"body": "Custom {{title}} body", "system": "Custom system"},
    )
    assert resp.status_code == 200

    listed = await app_client.get("/api/projects/default/prompts")
    row = next(r for r in listed.json() if r["key"] == "tags_from_metadata_prompt")
    assert row["body"] == "Custom {{title}} body"
    assert row["system"] == "Custom system"
    assert row["is_default"] is False


async def test_put_null_system_falls_back_to_seed(app_client) -> None:
    """system=null in payload → seed's system surfaces at read time."""
    # First save a custom system, then null it out.
    await app_client.put(
        "/api/projects/default/prompts/tags_from_metadata_prompt",
        json={"body": "x", "system": "ephemeral override"},
    )
    resp = await app_client.put(
        "/api/projects/default/prompts/tags_from_metadata_prompt",
        json={"body": "x", "system": None},
    )
    assert resp.status_code == 200

    listed = await app_client.get("/api/projects/default/prompts")
    row = next(r for r in listed.json() if r["key"] == "tags_from_metadata_prompt")
    # The seed's system text resurfaces because the saved row's
    # system_body is now NULL.
    assert "comma-separated list of tags" in row["system"]


async def test_put_omitted_system_preserves_saved(app_client) -> None:
    """Partial update with no ``system`` key must not clobber the saved value."""
    await app_client.put(
        "/api/projects/default/prompts/tags_from_metadata_prompt",
        json={"body": "v1", "system": "keep me"},
    )
    # Second save sends only body — system must be preserved.
    resp = await app_client.put(
        "/api/projects/default/prompts/tags_from_metadata_prompt",
        json={"body": "v2"},
    )
    assert resp.status_code == 200

    listed = await app_client.get("/api/projects/default/prompts")
    row = next(r for r in listed.json() if r["key"] == "tags_from_metadata_prompt")
    assert row["body"] == "v2"
    assert row["system"] == "keep me"


async def test_put_unknown_key_404(app_client) -> None:
    resp = await app_client.put(
        "/api/projects/default/prompts/not_a_real_prompt",
        json={"body": "x"},
    )
    assert resp.status_code == 404


async def test_put_empty_body_rejected_for_body_required_seed(app_client) -> None:
    resp = await app_client.put(
        "/api/projects/default/prompts/tags_from_metadata_prompt",
        json={"body": "   "},
    )
    assert resp.status_code == 400


async def test_put_empty_body_allowed_for_system_only_seed(app_client) -> None:
    """ai_block_default_system_prompt has body_required=False."""
    resp = await app_client.put(
        "/api/projects/default/prompts/ai_block_default_system_prompt",
        json={"body": "", "system": "My custom default."},
    )
    assert resp.status_code == 200
    listed = await app_client.get("/api/projects/default/prompts")
    row = next(
        r for r in listed.json() if r["key"] == "ai_block_default_system_prompt"
    )
    assert row["system"] == "My custom default."
