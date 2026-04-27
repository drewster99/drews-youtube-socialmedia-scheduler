"""Tests for the YouTube OAuth start endpoint's validation branches.

These cover the input checks the wizard depends on. The actual OAuth
exchange isn't tested here — that happens against Google's servers and
needs an interactive browser. Instead we mock ``has_client_secret`` /
``get_client_secret_dict`` and verify the start endpoint's branching.
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
    # Force the file-backed keychain so this test reads from tmp_path's
    # secrets.json, not the real macOS Keychain (which on the dev box
    # actually has a client_secret stored from manual testing — that
    # would make ``test_no_client_secret_returns_400`` falsely 200).
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c, monkeypatch


def _stub_client_secret(monkeypatch) -> None:
    """Make has_client_secret return True without actually storing a
    secret, plus return a usable client config for Flow.from_client_config.
    Real OAuth flow can't run in a unit test, but the start endpoint only
    needs to build the authorization URL — that succeeds without a network
    call given a syntactically-valid client config."""
    auth = importlib.import_module("yt_scheduler.services.auth")
    monkeypatch.setattr(auth, "has_client_secret", lambda: True)
    monkeypatch.setattr(
        auth,
        "get_client_secret_dict",
        lambda: {
            "installed": {
                "client_id": "fake-client-id.apps.googleusercontent.com",
                "client_secret": "fake-secret",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://127.0.0.1:8008/api/oauth/youtube/callback"],
            }
        },
    )

    routes = importlib.import_module("yt_scheduler.routers.oauth_routes")
    monkeypatch.setattr(routes, "has_client_secret", lambda: True)
    monkeypatch.setattr(routes, "get_client_secret_dict", auth.get_client_secret_dict)


def test_missing_origin_returns_400(client) -> None:
    c, _monkeypatch = client
    resp = c.post("/api/oauth/youtube/start", json={"pre_create": {"name": "x"}})
    assert resp.status_code == 400
    assert "origin" in resp.json()["detail"].lower()


def test_no_client_secret_returns_400(client) -> None:
    c, _monkeypatch = client
    resp = c.post(
        "/api/oauth/youtube/start",
        json={"origin": "http://127.0.0.1:8008", "pre_create": {"name": "x"}},
    )
    assert resp.status_code == 400
    assert "client" in resp.json()["detail"].lower()


def test_neither_mode_returns_400(client) -> None:
    c, monkeypatch = client
    _stub_client_secret(monkeypatch)
    resp = c.post(
        "/api/oauth/youtube/start",
        json={"origin": "http://127.0.0.1:8008"},
    )
    assert resp.status_code == 400
    assert "project_slug or pre_create" in resp.json()["detail"]


def test_both_modes_returns_400(client) -> None:
    c, monkeypatch = client
    _stub_client_secret(monkeypatch)
    resp = c.post(
        "/api/oauth/youtube/start",
        json={
            "origin": "http://127.0.0.1:8008",
            "project_slug": "default",
            "pre_create": {"name": "x"},
        },
    )
    assert resp.status_code == 400
    assert "not both" in resp.json()["detail"]


def test_pre_create_empty_name_is_accepted_for_channel_first_mode(client) -> None:
    """Channel-first wizard: caller posts ``pre_create: {}`` (or with an
    empty name) before the channel is known, and the callback derives
    name + slug from the resolved YouTube channel title."""
    c, monkeypatch = client
    _stub_client_secret(monkeypatch)
    resp = c.post(
        "/api/oauth/youtube/start",
        json={"origin": "http://127.0.0.1:8008", "pre_create": {"name": "  "}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "auth_url" in body

    # ``pre_create: {}`` (no name key at all) is also accepted — same path.
    resp2 = c.post(
        "/api/oauth/youtube/start",
        json={"origin": "http://127.0.0.1:8008", "pre_create": {}},
    )
    assert resp2.status_code == 200


def test_pre_create_slug_collision_returns_400(client) -> None:
    c, monkeypatch = client
    _stub_client_secret(monkeypatch)
    resp = c.post(
        "/api/oauth/youtube/start",
        json={
            "origin": "http://127.0.0.1:8008",
            "pre_create": {"name": "Default"},  # slugifies to "default" which exists
        },
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "default" in detail
    assert "already exists" in detail


def test_re_auth_unknown_project_returns_404(client) -> None:
    c, monkeypatch = client
    _stub_client_secret(monkeypatch)
    resp = c.post(
        "/api/oauth/youtube/start",
        json={"origin": "http://127.0.0.1:8008", "project_slug": "nope"},
    )
    assert resp.status_code == 404


def test_pre_create_returns_auth_url_on_success(client) -> None:
    c, monkeypatch = client
    _stub_client_secret(monkeypatch)
    resp = c.post(
        "/api/oauth/youtube/start",
        json={
            "origin": "http://127.0.0.1:8008",
            "pre_create": {"name": "Brand New Show"},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "auth_url" in body
    assert "accounts.google.com" in body["auth_url"]
    # The pending state was registered — the URL carries our state token
    assert "state=" in body["auth_url"]
