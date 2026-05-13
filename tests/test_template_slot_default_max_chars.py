"""When a new template slot is added without an explicit max_chars, the
default must match the platform — LinkedIn gets 3000, X/Twitter 280,
etc. The bug: every slot defaulted to 280 (front-end) or 500 (API),
which silently truncated LinkedIn posts to a tweet's worth of text.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


def _create_template(client: TestClient, name: str) -> None:
    resp = client.post(
        "/api/templates",
        json={"name": name, "description": "test"},
    )
    assert resp.status_code in (200, 201), resp.text


@pytest.mark.parametrize(
    "platform,expected_default",
    [
        ("twitter",  280),
        ("bluesky",  300),
        ("mastodon", 500),
        ("linkedin", 3000),
        ("threads",  500),
    ],
)
def test_add_slot_uses_platform_default_when_max_chars_omitted(
    client: TestClient, platform: str, expected_default: int,
) -> None:
    _create_template(client, "perplat")
    resp = client.post(
        "/api/templates/perplat/slots?project_slug=default",
        json={"platform": platform},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["platform"] == platform
    assert body["max_chars"] == expected_default, (
        f"Expected {platform}={expected_default}, got {body['max_chars']}"
    )


def test_add_slot_respects_explicit_max_chars(client: TestClient) -> None:
    _create_template(client, "explicit")
    resp = client.post(
        "/api/templates/explicit/slots?project_slug=default",
        json={"platform": "linkedin", "max_chars": 1234},
    )
    assert resp.status_code == 200
    assert resp.json()["max_chars"] == 1234


def test_add_slot_rejects_non_integer_max_chars(client: TestClient) -> None:
    _create_template(client, "bad")
    resp = client.post(
        "/api/templates/bad/slots?project_slug=default",
        json={"platform": "twitter", "max_chars": "lots"},
    )
    assert resp.status_code == 400


def test_default_max_chars_helper() -> None:
    """Direct unit test of the helper, independent of HTTP."""
    from yt_scheduler.services import templates as tmpl
    assert tmpl.default_max_chars("twitter") == 280
    assert tmpl.default_max_chars("Twitter") == 280  # case-insensitive
    assert tmpl.default_max_chars("linkedin") == 3000
    assert tmpl.default_max_chars("unknown-platform") == tmpl.GENERIC_MAX_CHARS_FALLBACK
    assert tmpl.default_max_chars(None) == tmpl.GENERIC_MAX_CHARS_FALLBACK
