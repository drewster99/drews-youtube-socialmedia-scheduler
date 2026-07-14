"""Save-time section validation on template-slot bodies (Phase 4).

A body whose {{#…}}/{{^…}}/{{/…}} section tags don't pair up is rejected
with a 400 at save time — when the author is looking at the editor —
instead of failing at generation time.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")
    from fastapi.testclient import TestClient
    with TestClient(app_module.app) as c:
        yield c
    # Close the shared aiosqlite connection so its non-daemon thread can't
    # wedge interpreter shutdown (see conftest's leaked-connection note).
    database = importlib.import_module("yt_scheduler.database")
    await database.close_db()


def _create_template(client, name: str) -> None:
    resp = client.post(
        "/api/templates?project_slug=default",
        json={"name": name, "description": "", "platforms": {}},
    )
    assert resp.status_code == 200, resp.text


def test_add_slot_rejects_unclosed_section(client) -> None:
    _create_template(client, "sectiontest")
    resp = client.post(
        "/api/templates/sectiontest/slots?project_slug=default",
        json={"platform": "mastodon", "body": "{{#url}}Watch: {{url}}"},
    )
    assert resp.status_code == 400, resp.text
    assert "section_error" in resp.text


def test_add_slot_accepts_balanced_sections(client) -> None:
    _create_template(client, "sectionok")
    resp = client.post(
        "/api/templates/sectionok/slots?project_slug=default",
        json={"platform": "mastodon", "body": "{{#url}}Watch: {{url}}{{/url}}"},
    )
    assert resp.status_code == 200, resp.text


def test_update_slot_rejects_mismatched_section(client) -> None:
    _create_template(client, "sectionpatch")
    created = client.post(
        "/api/templates/sectionpatch/slots?project_slug=default",
        json={"platform": "bluesky", "body": "hello"},
    )
    assert created.status_code == 200, created.text
    slot_id = created.json()["id"]
    resp = client.patch(
        f"/api/templates/sectionpatch/slots/{slot_id}?project_slug=default",
        json={"body": "{{#a}}{{#b}}x{{/a}}{{/b}}"},
    )
    assert resp.status_code == 400, resp.text
    assert "section_error" in resp.text
