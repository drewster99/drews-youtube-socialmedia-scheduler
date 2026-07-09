"""auth_routes handlers must keep blocking Keychain/network work off the event loop.

get_auth_status can fire a blocking Google token refresh plus several ~100ms-1s
`security` subprocess reads; the rest hit the Keychain. Running any of them on
the loop freezes every other request and every scheduled job.

The handlers are called directly (no TestClient) so this module never opens the
database.
"""

from __future__ import annotations

import asyncio
import importlib
import threading
import time
from pathlib import Path

import pytest


@pytest.fixture
def auth_routes(isolated_data_dir: Path):
    return importlib.import_module("yt_scheduler.routers.auth_routes")


async def test_auth_status_offloads_to_thread(auth_routes, monkeypatch):
    """The whole get_auth_status composition rides one worker thread."""
    seen: dict[str, object] = {}

    def fake_get_auth_status(slug: str) -> dict:
        seen["thread"] = threading.current_thread()
        seen["slug"] = slug
        return {"authenticated": True}

    monkeypatch.setattr(auth_routes, "get_auth_status", fake_get_auth_status)

    result = await auth_routes.auth_status(project_slug="default")

    assert result == {"authenticated": True}
    assert seen["slug"] == "default"
    assert seen["thread"] is not threading.main_thread(), (
        "get_auth_status ran on the event loop thread"
    )


async def test_client_secret_status_offloads(auth_routes, monkeypatch):
    seen: dict[str, object] = {}

    def fake_has_client_secret() -> bool:
        seen["thread"] = threading.current_thread()
        return True

    monkeypatch.setattr(auth_routes, "has_client_secret", fake_has_client_secret)

    assert await auth_routes.client_secret_status() == {"uploaded": True}
    assert seen["thread"] is not threading.main_thread()


async def test_logout_offloads(auth_routes, monkeypatch):
    seen: dict[str, object] = {}

    def fake_clear_credentials(slug: str) -> None:
        seen["thread"] = threading.current_thread()

    monkeypatch.setattr(auth_routes, "clear_credentials", fake_clear_credentials)

    result = await auth_routes.logout(project_slug="default")

    assert result["status"] == "ok"
    assert seen["thread"] is not threading.main_thread()


async def test_delete_client_secret_offloads(auth_routes, monkeypatch):
    seen: dict[str, object] = {}

    def fake_clear_client_secret() -> None:
        seen["thread"] = threading.current_thread()

    monkeypatch.setattr(auth_routes, "clear_client_secret", fake_clear_client_secret)

    assert await auth_routes.delete_client_secret() == {"status": "ok"}
    assert seen["thread"] is not threading.main_thread()


async def test_concurrent_auth_status_does_not_serialize(auth_routes, monkeypatch):
    """Two in-flight status calls overlap instead of queueing behind the loop."""
    monkeypatch.setattr(
        auth_routes,
        "get_auth_status",
        lambda slug: (time.sleep(0.3), {"authenticated": True})[1],
    )

    started = time.monotonic()
    await asyncio.gather(
        auth_routes.auth_status(project_slug="a"),
        auth_routes.auth_status(project_slug="b"),
    )
    elapsed = time.monotonic() - started

    # Serialized on the loop this is >=0.6s; offloaded it is ~0.3s.
    assert elapsed < 0.55, f"calls serialized on the event loop ({elapsed:.2f}s)"


async def test_logout_still_catches_exceptions_through_to_thread(auth_routes, monkeypatch):
    """to_thread re-raises the original exception, so existing excepts still fire."""

    def boom(slug: str) -> None:
        raise RuntimeError("keychain wedged")

    monkeypatch.setattr(auth_routes, "clear_credentials", boom)

    result = await auth_routes.logout(project_slug="default")

    assert result["status"] == "error"
    assert "keychain wedged" in result["message"]


async def test_upload_client_secret_maps_bad_json_to_400(auth_routes, monkeypatch):
    """JSONDecodeError (a ValueError) must still surface as a clean 400."""
    import json as _json

    from fastapi import HTTPException

    def boom(text: str) -> None:
        raise _json.JSONDecodeError("bad", text, 0)

    monkeypatch.setattr(auth_routes, "store_client_secret_from_text", boom)

    class _FakeUpload:
        async def read(self, n: int = -1) -> bytes:
            return b"not json"

    with pytest.raises(HTTPException) as excinfo:
        await auth_routes.upload_client_secret(file=_FakeUpload())

    assert excinfo.value.status_code == 400
    assert "Invalid client_secret JSON" in str(excinfo.value.detail)
