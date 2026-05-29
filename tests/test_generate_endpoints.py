"""Generate-from-source preview / jobs / confirm endpoints.

Isolated tmp_path data dir per the existing pattern. Claude calls are
monkeypatched out via ``clipper.propose_all_clips`` so the tests don't
hit the API and don't need a key configured.
"""

from __future__ import annotations

import importlib
import sqlite3
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


def _insert_parent(
    video_id: str,
    *,
    duration: float | None = 600.0,
    transcript: str | None = None,
    transcript_source: str | None = None,
    has_local: bool = True,
) -> Path | None:
    from yt_scheduler.config import DB_PATH, UPLOAD_DIR

    path: Path | None = None
    if has_local:
        path = UPLOAD_DIR / f"{video_id}.mp4"
        path.write_bytes(b"\x00" * 32)

    cols = {
        "id": video_id,
        "title": "Parent Video",
        "status": "ready",
        "project_id": 1,
        "duration_seconds": duration,
        "item_type": "episode",
        "video_file_path": str(path) if path else None,
        "transcript": transcript,
        "transcript_source": transcript_source,
    }
    with sqlite3.connect(str(DB_PATH)) as conn:
        keys = list(cols.keys())
        conn.execute(
            f"INSERT INTO videos ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
            list(cols.values()),
        )
        conn.commit()
    return path


def test_preview_404_on_unknown_parent(client: TestClient):
    resp = client.post(
        "/api/projects/default/videos/UNKNOWN0001/promos/generate/preview",
        json={"kinds": ["hook"]},
    )
    assert resp.status_code == 404


def test_preview_400_no_kinds(client: TestClient):
    _insert_parent("PARENTAAAA1", duration=600.0, transcript=None)
    resp = client.post(
        "/api/projects/default/videos/PARENTAAAA1/promos/generate/preview",
        json={"kinds": []},
    )
    assert resp.status_code == 400


def test_preview_400_when_parent_too_long(client: TestClient):
    _insert_parent("PARENTAAAA2", duration=5 * 3600.0, transcript=None)
    resp = client.post(
        "/api/projects/default/videos/PARENTAAAA2/promos/generate/preview",
        json={"kinds": ["hook"]},
    )
    assert resp.status_code == 400
    assert "longer than" in resp.json()["detail"]


def test_preview_400_when_no_local_file(client: TestClient):
    _insert_parent("PARENTAAAA3", duration=600.0, has_local=False)
    resp = client.post(
        "/api/projects/default/videos/PARENTAAAA3/promos/generate/preview",
        json={"kinds": ["hook"]},
    )
    assert resp.status_code == 400
    assert "local video file" in resp.json()["detail"]


def test_preview_400_when_parent_too_short_for_any_kind(client: TestClient):
    """40 s parent: hook needs 45, short needs 90, segment needs 75 → none."""
    _insert_parent("PARENTAAAA4", duration=40.0)
    resp = client.post(
        "/api/projects/default/videos/PARENTAAAA4/promos/generate/preview",
        json={"kinds": ["hook", "short", "segment"]},
    )
    assert resp.status_code == 400


def test_preview_400_user_edited_transcript_without_timestamps(client: TestClient):
    _insert_parent(
        "PARENTAAAA5", duration=600.0,
        transcript="This is plain prose without timestamps.",
        transcript_source="user_edited",
    )
    resp = client.post(
        "/api/projects/default/videos/PARENTAAAA5/promos/generate/preview",
        json={"kinds": ["hook"]},
    )
    assert resp.status_code == 400
    assert "hand-edited" in resp.json()["detail"]


def test_preview_returns_job_id_and_eligibility(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """Happy path: 600 s parent → all kinds eligible, job created.

    We stub propose_all_clips so the background task completes
    instantly without touching Claude.
    """
    from yt_scheduler.services import clipper

    async def fake_propose(**kwargs):
        return {
            "hook": [clipper.ProposedClip(
                kind="hook", start_seconds=10.0, end_seconds=25.0,
                title="A great hook", reason="because",
            )],
            "short": [],
            "segment": [],
        }
    monkeypatch.setattr(clipper, "propose_all_clips", fake_propose)

    _insert_parent(
        "PARENTBBBB1", duration=600.0,
        transcript=(
            "1\n00:00:10,000 --> 00:00:14,000\nhello there\n\n"
            "2\n00:00:15,000 --> 00:00:20,000\nmore content\n\n"
        ),
        transcript_source="mlx_whisper",
    )
    resp = client.post(
        "/api/projects/default/videos/PARENTBBBB1/promos/generate/preview",
        json={"kinds": ["hook", "short", "segment"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["job_id"].startswith("gen_")
    assert set(body["eligible_kinds"]) == {"hook", "short", "segment"}
    assert body["ineligible_kinds"] == []


def test_jobs_endpoint_404_unknown(client: TestClient):
    _insert_parent("PARENTCCCC1", duration=600.0)
    resp = client.get(
        "/api/projects/default/videos/PARENTCCCC1/promos/generate/jobs/nope",
    )
    assert resp.status_code == 404


def test_confirm_400_empty_accepted(client: TestClient):
    _insert_parent("PARENTDDDD1", duration=600.0)
    resp = client.post(
        "/api/projects/default/videos/PARENTDDDD1/promos/generate/confirm",
        json={"accepted": []},
    )
    assert resp.status_code == 400


def test_confirm_400_no_local_file(client: TestClient):
    _insert_parent("PARENTDDDD2", duration=600.0, has_local=False)
    resp = client.post(
        "/api/projects/default/videos/PARENTDDDD2/promos/generate/confirm",
        json={"accepted": [{
            "kind": "hook", "start_seconds": 10.0, "end_seconds": 25.0, "title": "x",
        }]},
    )
    assert resp.status_code == 400


def test_confirm_overrides_crop_against_preview_snapshot(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """When the client supplies a job_id, the server cross-checks
    vertical_crop against the preview snapshot. A tampered request
    that flips a non-crop kind's vertical_crop to True gets forced
    back to False before reaching start_promo_from_cut."""
    from yt_scheduler.services import auto_actions, clipper

    captured: list[dict] = []

    async def fake_start(**kwargs):
        captured.append(kwargs)
        return f"job_fake_{len(captured)}"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTOOOOX", duration=600.0)

    # Stage a generate job in clipper._GENERATE_JOBS where 'segment'
    # had crop OFF in the preview.
    clipper._GENERATE_JOBS["gen_test"] = {
        "job_id": "gen_test",
        "state": "done",
        "crop_vertical": {"hook": True, "short": True, "segment": False},
    }
    try:
        payload = {
            "job_id": "gen_test",
            "accepted": [
                # Legit: hook with crop=true matches snapshot → stays true.
                {"kind": "hook", "start_seconds": 5, "end_seconds": 20,
                 "title": "h1", "vertical_crop": True,
                 "x_shift_normalized": 0.4},
                # Tampered: segment with crop=true but preview said off.
                {"kind": "segment", "start_seconds": 100, "end_seconds": 200,
                 "title": "seg1", "vertical_crop": True,
                 "x_shift_normalized": 0.6},
            ],
        }
        resp = client.post(
            "/api/projects/default/videos/PARENTOOOOX/promos/generate/confirm",
            json=payload,
        )
        assert resp.status_code == 200, resp.text
        # Both jobs created.
        assert len(captured) == 2
        # Hook: crop kept on with its shift.
        hook_call = next(c for c in captured if c["item_type"] == "hook")
        assert hook_call["vertical_crop"] is True
        assert hook_call["x_shift_normalized"] == 0.4
        # Segment: crop forced off + shift zeroed despite the request.
        seg_call = next(c for c in captured if c["item_type"] == "segment")
        assert seg_call["vertical_crop"] is False
        assert seg_call["x_shift_normalized"] == 0.0
    finally:
        clipper._GENERATE_JOBS.pop("gen_test", None)


def test_confirm_400_when_job_id_unknown(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """A supplied job_id that doesn't exist (e.g. server restart,
    TTL expired) must reject — silently dropping back to "no
    cross-check" would defeat the security improvement."""
    from yt_scheduler.services import auto_actions

    async def fake_start(**kwargs):
        return "job_fake_1"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTOOOOY", duration=600.0)

    resp = client.post(
        "/api/projects/default/videos/PARENTOOOOY/promos/generate/confirm",
        json={
            "job_id": "gen_does_not_exist",
            "accepted": [
                {"kind": "hook", "start_seconds": 5, "end_seconds": 20,
                 "title": "h1"},
            ],
        },
    )
    assert resp.status_code == 400
    assert "expired" in resp.json()["detail"]


def test_confirm_filters_invalid_entries(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
):
    """Wrong kind, negative start, end-before-start, end past parent — all dropped.
    A valid entry survives and creates a job."""
    from yt_scheduler.services import auto_actions

    created: list[dict] = []

    async def fake_start(**kwargs):
        created.append(kwargs)
        return f"job_fake_{len(created)}"

    monkeypatch.setattr(auto_actions, "start_promo_from_cut", fake_start)
    _insert_parent("PARENTDDDD3", duration=600.0)

    payload = {"accepted": [
        # Valid
        {"kind": "hook", "start_seconds": 10.0, "end_seconds": 25.0, "title": "ok"},
        # Wrong kind
        {"kind": "video", "start_seconds": 10.0, "end_seconds": 25.0, "title": "bad"},
        # Negative
        {"kind": "hook", "start_seconds": -1.0, "end_seconds": 10.0, "title": "neg"},
        # Past parent (parent is 600 s)
        {"kind": "hook", "start_seconds": 700.0, "end_seconds": 720.0, "title": "past"},
        # End before start
        {"kind": "hook", "start_seconds": 25.0, "end_seconds": 10.0, "title": "rev"},
        # Empty title
        {"kind": "short", "start_seconds": 100.0, "end_seconds": 160.0, "title": ""},
    ]}
    resp = client.post(
        "/api/projects/default/videos/PARENTDDDD3/promos/generate/confirm",
        json=payload,
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["jobs"]) == 1
    assert len(created) == 1
    assert created[0]["item_type"] == "hook"
    assert created[0]["title"] == "ok"
