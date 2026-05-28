"""Per-tier promo delay settings: storage, validation, unit conversion,
and the GET/PUT endpoints."""

from __future__ import annotations

import importlib
import json
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


def test_promo_delays_to_timedeltas_units() -> None:
    from datetime import timedelta

    from yt_scheduler.services.scheduler import _promo_delays_to_timedeltas

    raw = {
        "hook": {"initial": {"value": 90, "unit": "minutes"},
                 "subsequent": {"value": 2, "unit": "hours"}},
        "short": {"initial": {"value": 1, "unit": "days"},
                  "subsequent": {"value": 3, "unit": "days"}},
        "segment": {"initial": {"value": 5, "unit": "hours"},
                    "subsequent": {"value": 7, "unit": "days"}},
    }
    td = _promo_delays_to_timedeltas(raw)
    assert td["hook"]["initial"] == timedelta(minutes=90)
    assert td["hook"]["subsequent"] == timedelta(hours=2)
    assert td["short"]["initial"] == timedelta(days=1)
    assert td["segment"]["subsequent"] == timedelta(days=7)


def test_promo_delays_to_timedeltas_falls_back_on_missing() -> None:
    from yt_scheduler.services.scheduler import (
        DEFAULT_PROMO_DELAYS,
        _promo_delays_to_timedeltas,
    )

    # Falls back to the defaults by value — but as a fresh copy, never
    # the shared module-level dict (so a mutating caller can't corrupt it).
    fallback = _promo_delays_to_timedeltas(None)
    assert fallback == DEFAULT_PROMO_DELAYS
    assert fallback is not DEFAULT_PROMO_DELAYS
    # A tier with a malformed field falls back to that tier's default.
    td = _promo_delays_to_timedeltas({"hook": {"initial": "bogus"}})
    assert td["hook"]["initial"] == DEFAULT_PROMO_DELAYS["hook"]["initial"]


def test_validate_promo_delays_rejects_bad_input() -> None:
    from yt_scheduler.services.project_settings import validate_promo_delays

    good = {
        t: {"initial": {"value": 1, "unit": "hours"},
            "subsequent": {"value": 2, "unit": "days"}}
        for t in ("hook", "short", "segment")
    }
    out = validate_promo_delays(good)
    assert out["hook"]["initial"]["value"] == 1

    with pytest.raises(ValueError):
        validate_promo_delays({})  # missing tiers
    with pytest.raises(ValueError):
        bad = json.loads(json.dumps(good))
        bad["hook"]["initial"]["unit"] = "weeks"
        validate_promo_delays(bad)
    with pytest.raises(ValueError):
        bad = json.loads(json.dumps(good))
        bad["short"]["subsequent"]["value"] = -1
        validate_promo_delays(bad)
    with pytest.raises(ValueError):
        # Absurd value would overflow timedelta() downstream.
        bad = json.loads(json.dumps(good))
        bad["segment"]["initial"] = {"value": 1_000_000_000, "unit": "days"}
        validate_promo_delays(bad)


def test_promo_delays_endpoints_round_trip(client: TestClient) -> None:
    resp = client.get("/api/projects/default/promo-delays")
    assert resp.status_code == 200
    data = resp.json()
    assert set(data) == {"hook", "short", "segment"}
    # Default segment subsequent gap is 9 days.
    assert data["segment"]["subsequent"] == {"value": 9, "unit": "days"}

    payload = json.loads(json.dumps(data))
    payload["hook"]["initial"] = {"value": 30, "unit": "minutes"}
    put = client.put("/api/projects/default/promo-delays", json=payload)
    assert put.status_code == 200
    assert put.json()["hook"]["initial"] == {"value": 30, "unit": "minutes"}

    # Persisted across reads.
    assert client.get(
        "/api/projects/default/promo-delays"
    ).json()["hook"]["initial"] == {"value": 30, "unit": "minutes"}

    # Malformed payload is rejected, not silently defaulted.
    assert client.put(
        "/api/projects/default/promo-delays", json={"hook": {}},
    ).status_code == 400
