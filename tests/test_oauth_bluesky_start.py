"""Validation-branch tests for ``POST /api/oauth/bluesky/start``.

The actual Bluesky OAuth handshake (PAR, redirect, callback, token
exchange) requires reachable Bluesky servers and is not exercised here.
Instead we monkeypatch the resolution + discovery + PAR helpers in
``services.bluesky_oauth`` so the start endpoint can be poked through
its happy and sad paths in isolation.
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
        yield c, monkeypatch


def _stub_resolution_and_par(monkeypatch) -> None:
    """Make resolve_identity / discover_auth_server_for_pds /
    push_authorization_request return canned values without hitting any
    network endpoint."""
    bo = importlib.import_module("yt_scheduler.services.bluesky_oauth")
    routes = importlib.import_module("yt_scheduler.routers.oauth_routes")

    async def _fake_resolve(handle, _client):
        return bo.ResolvedIdentity(
            handle=handle, did="did:plc:fake", pds="https://pds.test"
        )

    async def _fake_discover(_pds, _client):
        return bo.AuthServerMetadata(
            issuer="https://bsky.social",
            authorization_endpoint="https://bsky.social/oauth/authorize",
            token_endpoint="https://bsky.social/oauth/token",
            pushed_authorization_request_endpoint="https://bsky.social/oauth/par",
        )

    async def _fake_par(_pending, _client):
        return "urn:ietf:params:oauth:request_uri:fake-12345"

    monkeypatch.setattr(bo, "resolve_identity", _fake_resolve)
    monkeypatch.setattr(bo, "discover_auth_server_for_pds", _fake_discover)
    monkeypatch.setattr(bo, "push_authorization_request", _fake_par)
    monkeypatch.setattr(routes.bluesky_oauth, "resolve_identity", _fake_resolve)
    monkeypatch.setattr(routes.bluesky_oauth, "discover_auth_server_for_pds", _fake_discover)
    monkeypatch.setattr(routes.bluesky_oauth, "push_authorization_request", _fake_par)


def test_missing_handle_returns_400(client) -> None:
    c, _monkeypatch = client
    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008"},
    )
    assert resp.status_code == 400
    assert "handle" in resp.json()["detail"].lower()


def test_invalid_handle_returns_400(client) -> None:
    c, _monkeypatch = client
    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008", "handle": "not a handle"},
    )
    assert resp.status_code == 400


def test_missing_origin_returns_400(client) -> None:
    c, _monkeypatch = client
    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"handle": "alice.bsky.social"},
    )
    assert resp.status_code == 400
    assert "origin" in resp.json()["detail"].lower()


def test_resolution_failure_returns_400(client) -> None:
    c, monkeypatch = client
    bo = importlib.import_module("yt_scheduler.services.bluesky_oauth")
    routes = importlib.import_module("yt_scheduler.routers.oauth_routes")

    async def _fail_resolve(handle, _client):
        raise ValueError(f"unknown handle {handle}")

    monkeypatch.setattr(bo, "resolve_identity", _fail_resolve)
    monkeypatch.setattr(routes.bluesky_oauth, "resolve_identity", _fail_resolve)

    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008", "handle": "ghost.bsky.social"},
    )
    assert resp.status_code == 400
    assert "resolve" in resp.json()["detail"].lower()


def test_par_failure_returns_400(client) -> None:
    c, monkeypatch = client
    _stub_resolution_and_par(monkeypatch)
    bo = importlib.import_module("yt_scheduler.services.bluesky_oauth")
    routes = importlib.import_module("yt_scheduler.routers.oauth_routes")

    async def _fail_par(_pending, _client):
        raise RuntimeError("PAR boom")

    monkeypatch.setattr(bo, "push_authorization_request", _fail_par)
    monkeypatch.setattr(routes.bluesky_oauth, "push_authorization_request", _fail_par)

    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008", "handle": "alice.bsky.social"},
    )
    assert resp.status_code == 400
    assert "par" in resp.json()["detail"].lower()


def test_happy_path_returns_auth_url_and_persists_pending(client) -> None:
    c, monkeypatch = client
    _stub_resolution_and_par(monkeypatch)
    routes = importlib.import_module("yt_scheduler.routers.oauth_routes")

    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008", "handle": "alice.bsky.social"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "auth_url" in body
    assert body["auth_url"].startswith("https://bsky.social/oauth/authorize")
    # request_uri appears as a URL parameter
    assert "request_uri=urn%3Aietf%3Aparams%3Aoauth%3Arequest_uri" in body["auth_url"]
    # client_id query param uses the localhost shortcut
    assert "client_id=http%3A%2F%2Flocalhost" in body["auth_url"]
    # Pending state was registered for the eventual callback
    assert len(routes._bluesky_pending) == 1


def test_handle_with_at_prefix_normalises(client) -> None:
    c, monkeypatch = client
    _stub_resolution_and_par(monkeypatch)
    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008", "handle": "@Alice.BSKY.Social"},
    )
    assert resp.status_code == 200


def test_settings_put_bluesky_rejected(client) -> None:
    """Phase F removed the paste form; the legacy PUT route must refuse
    Bluesky outright so an arbitrary payload can't merge into an OAuth
    bundle (e.g. re-introduce ``app_password``)."""
    c, _monkeypatch = client
    resp = c.put(
        "/api/settings/social/bluesky",
        json={"handle": "x.bsky.social", "app_password": "abcd"},
    )
    assert resp.status_code == 400
    assert "OAuth-only" in resp.json()["detail"]


def test_callback_unknown_state_returns_html_error(client) -> None:
    c, _monkeypatch = client
    resp = c.get("/api/oauth/bluesky/callback", params={"code": "x", "state": "nope"})
    # Callback always returns 200 + HTML (so the popup script can run);
    # the failure is communicated via the page's postMessage payload.
    assert resp.status_code == 200
    assert "Unknown or expired OAuth state" in resp.text


def test_callback_iss_mismatch_returns_error_page(client) -> None:
    """A callback whose ``iss`` differs from the pending AS issuer must
    be rejected — defense against mix-up attacks."""
    c, monkeypatch = client
    _stub_resolution_and_par(monkeypatch)
    resp = c.post(
        "/api/oauth/bluesky/start",
        json={"origin": "http://127.0.0.1:8008", "handle": "alice.bsky.social"},
    )
    assert resp.status_code == 200
    routes = importlib.import_module("yt_scheduler.routers.oauth_routes")
    state = next(iter(routes._bluesky_pending))

    bad_resp = c.get(
        "/api/oauth/bluesky/callback",
        params={"code": "abc", "state": state, "iss": "https://evil.test"},
    )
    assert bad_resp.status_code == 200
    assert "Issuer mismatch" in bad_resp.text


def test_callback_oauth_error_returns_html_error(client) -> None:
    c, _monkeypatch = client
    resp = c.get(
        "/api/oauth/bluesky/callback",
        params={"error": "access_denied", "error_description": "user said no"},
    )
    assert resp.status_code == 200
    assert "denied authorization" in resp.text
