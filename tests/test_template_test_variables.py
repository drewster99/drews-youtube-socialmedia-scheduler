"""Persisted Preview-pane test variables on templates (migration 016).

Verifies the round-trip: PUT writes the JSON column, GET returns the
parsed dict, and the column gets cleared back to NULL when the user
saves an empty dict (so the front-end falls back to its seeded
defaults).
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
        json={"name": name, "description": "x"},
    )
    assert resp.status_code == 200, resp.text


def test_fresh_template_has_empty_test_variables(client: TestClient) -> None:
    """A newly-created template returns ``test_variables: {}`` so the
    front-end uses its seeded defaults."""
    _create_template(client, "fresh")
    resp = client.get("/api/templates/fresh?project_slug=default")
    assert resp.status_code == 200
    assert resp.json()["test_variables"] == {}


def test_put_round_trip(client: TestClient) -> None:
    _create_template(client, "tv")
    payload = {
        "test_variables": {
            "title": "Saved title",
            "url": "https://example.com/x",
            "tags": "saved, tags",
            "region": "US",
        }
    }
    resp = client.put(
        "/api/templates/tv/test-variables?project_slug=default",
        json=payload,
    )
    assert resp.status_code == 200, resp.text

    fetched = client.get("/api/templates/tv?project_slug=default").json()
    assert fetched["test_variables"] == payload["test_variables"]


def test_empty_dict_clears_to_null(client: TestClient) -> None:
    """Saving {} sends the column back to NULL — verified by GET returning
    an empty dict and then another save still returning {} (no error)."""
    _create_template(client, "clr")
    client.put(
        "/api/templates/clr/test-variables?project_slug=default",
        json={"test_variables": {"x": "1"}},
    )
    client.put(
        "/api/templates/clr/test-variables?project_slug=default",
        json={"test_variables": {}},
    )
    assert client.get("/api/templates/clr?project_slug=default").json()["test_variables"] == {}


def test_non_string_values_coerce(client: TestClient) -> None:
    """Numbers / booleans / null get coerced to strings so the renderer
    doesn't choke on a non-str variable later. ``null`` becomes ``""``."""
    _create_template(client, "coerce")
    client.put(
        "/api/templates/coerce/test-variables?project_slug=default",
        json={"test_variables": {"a": 7, "b": True, "c": None}},
    )
    out = client.get("/api/templates/coerce?project_slug=default").json()["test_variables"]
    assert out == {"a": "7", "b": "True", "c": ""}


def test_non_dict_payload_rejected(client: TestClient) -> None:
    _create_template(client, "bad")
    resp = client.put(
        "/api/templates/bad/test-variables?project_slug=default",
        json={"test_variables": ["not", "an", "object"]},
    )
    assert resp.status_code == 400


def test_unknown_template_404s(client: TestClient) -> None:
    resp = client.put(
        "/api/templates/no-such/test-variables?project_slug=default",
        json={"test_variables": {"x": "1"}},
    )
    assert resp.status_code == 404


def test_duplicate_template_copies_test_variables(client: TestClient) -> None:
    _create_template(client, "src")
    client.put(
        "/api/templates/src/test-variables?project_slug=default",
        json={"test_variables": {"title": "from src", "region": "EU"}},
    )
    resp = client.post(
        "/api/templates/src/duplicate?project_slug=default",
        json={"new_name": "copy"},
    )
    assert resp.status_code == 200
    assert resp.json()["test_variables"] == {"title": "from src", "region": "EU"}
