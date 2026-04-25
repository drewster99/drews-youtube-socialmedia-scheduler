"""Tests for username-resolution + social_accounts population.

The platform HTTP calls are stubbed via httpx.MockTransport so the test never
hits the real network, and the keychain backend is forced to file-mode so we
don't poke the real macOS Keychain.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import httpx
import pytest


@pytest.fixture
async def identity_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    identity = importlib.import_module("yt_scheduler.services.social_identity")
    db = await database.get_db()
    await projects.ensure_default_project()
    yield identity, keychain, db, monkeypatch
    await database.close_db()


def _mock_transport(response_map: dict[str, dict]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        body = response_map.get(str(request.url))
        if body is None:
            return httpx.Response(404)
        return httpx.Response(200, json=body)
    return httpx.MockTransport(handler)


async def test_resolve_twitter_uses_users_me(identity_env) -> None:
    identity, keychain, _db, monkeypatch = identity_env
    keychain.store_secret("twitter", "bearer_token", "tok")

    transport = _mock_transport({
        "https://api.twitter.com/2/users/me": {"data": {"username": "drewster99"}},
    })

    real_async_client = httpx.AsyncClient
    def patched(*args, **kwargs):
        kwargs["transport"] = transport
        return real_async_client(*args, **kwargs)
    monkeypatch.setattr(httpx, "AsyncClient", patched)

    assert await identity.resolve_twitter() == "drewster99"


async def test_resolve_mastodon_appends_instance(identity_env) -> None:
    identity, keychain, _db, monkeypatch = identity_env
    keychain.store_secret("mastodon", "access_token", "tok")
    keychain.store_secret("mastodon", "instance_url", "https://mastodon.social")

    transport = _mock_transport({
        "https://mastodon.social/api/v1/accounts/verify_credentials": {
            "username": "drewster99",
        },
    })

    real_async_client = httpx.AsyncClient
    def patched(*args, **kwargs):
        kwargs["transport"] = transport
        return real_async_client(*args, **kwargs)
    monkeypatch.setattr(httpx, "AsyncClient", patched)

    assert await identity.resolve_mastodon() == "drewster99@mastodon.social"


async def test_bluesky_falls_back_to_stored_handle(identity_env) -> None:
    identity, keychain, _db, _ = identity_env
    keychain.store_secret("bluesky", "handle", "me.bsky.social")
    assert await identity.resolve_bluesky() == "me.bsky.social"


async def test_unknown_platform_returns_none(identity_env) -> None:
    identity, _kc, _db, _ = identity_env
    assert await identity.resolve_username("nonexistent") is None
