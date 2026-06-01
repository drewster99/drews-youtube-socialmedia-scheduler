"""End-to-end coverage for the ``shorten_post`` flow.

The route reads its body and system prompt from ``prompt_templates`` (with
seed fallbacks), renders them through ``services.templates.render`` with
``{{target_chars}}`` and ``{{post_text}}``, and forwards the result to
``ai.call_ai_block``. These tests mock ``call_ai_block`` so no real Claude
call happens and assert the contract.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def app_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    # Keychain stub so config.get_anthropic_api_key doesn't reach the real
    # macOS keychain in CI. ``shorten_post`` doesn't need a real key — the
    # call is mocked — but the import of services.ai walks through config.
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    keychain.store_secret("anthropic", "api_key", "sk-ant-test-fake")

    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    app_module = importlib.import_module("yt_scheduler.app")

    db = await database.get_db()
    await projects.ensure_default_project()

    # Seed a video + social_post pair for the route to find.
    await db.execute(
        "INSERT INTO videos (id, title, project_id, status) VALUES (?, ?, ?, ?)",
        ("vid_test", "Test Video", 1, "draft"),
    )
    await db.execute(
        "INSERT INTO social_posts (id, video_id, platform, content, status, max_chars) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            101,
            "vid_test",
            "twitter",
            "A long social post that goes well over the limit and needs to be shortened. " * 4,
            "draft",
            120,
        ),
    )
    await db.commit()

    transport = ASGITransport(app=app_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client, db

    await database.close_db()


async def test_shorten_post_uses_seed_prompts(app_client) -> None:
    """No user override saved → renderer pulls the seed body + system."""
    client, _db = app_client
    from yt_scheduler.services import ai
    from yt_scheduler.services import prompts

    captured: dict = {}

    def _fake_call(prompt, *, system, model=None, max_tokens=512):
        captured["prompt"] = prompt
        captured["system"] = system
        return "Shorter version."

    with patch.object(ai, "call_ai_block", side_effect=_fake_call):
        resp = await client.post(
            "/api/social/posts/101/shorten",
            json={"target_chars": 80},
        )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["content"] == "Shorter version."

    # Body rendered with the variables from the seed template.
    assert "80 characters" in captured["prompt"]
    assert "A long social post" in captured["prompt"]
    # System from the seed (verbatim — no row was saved).
    assert captured["system"] == prompts.SEED_SHORTEN_POST_PROMPT.system


async def test_shorten_post_honors_saved_overrides(app_client) -> None:
    """User-saved body + system surface in the Claude call."""
    client, _db = app_client
    from yt_scheduler.services import ai
    from yt_scheduler.services import prompts

    await prompts.upsert_prompt_template(
        key="shorten_post_prompt",
        name="Custom shorten",
        body="Trim to {{target_chars}} chars (post: {{post_text}}). Be ruthless.",
        system="You are a brutal editor.",
        project_id=1,
    )

    captured: dict = {}

    def _fake_call(prompt, *, system, model=None, max_tokens=512):
        captured["prompt"] = prompt
        captured["system"] = system
        return "Brutally short."

    with patch.object(ai, "call_ai_block", side_effect=_fake_call):
        resp = await client.post(
            "/api/social/posts/101/shorten",
            json={"target_chars": 40},
        )
    assert resp.status_code == 200, resp.text
    assert "Trim to 40 chars" in captured["prompt"]
    assert "Be ruthless." in captured["prompt"]
    assert captured["system"] == "You are a brutal editor."


async def test_shorten_post_explicit_empty_system_suppresses(app_client) -> None:
    """Saving ``system=""`` explicitly suppresses the system prompt."""
    client, _db = app_client
    from yt_scheduler.services import ai
    from yt_scheduler.services import prompts

    await prompts.upsert_prompt_template(
        key="shorten_post_prompt",
        name="Custom shorten",
        body="Shorten: {{post_text}} → {{target_chars}}.",
        system="",  # explicit empty → no system message
        project_id=1,
    )

    captured: dict = {}

    def _fake_call(prompt, *, system, model=None, max_tokens=512):
        captured["system"] = system
        return "ok"

    with patch.object(ai, "call_ai_block", side_effect=_fake_call):
        resp = await client.post(
            "/api/social/posts/101/shorten",
            json={"target_chars": 60},
        )
    assert resp.status_code == 200
    # Empty string passes through render() as empty; call site treats
    # falsy as "don't send system".
    assert captured["system"] in (None, "")
