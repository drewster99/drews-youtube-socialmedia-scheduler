"""Bulk description re-generation for a parent's promo clips.

"Update all descriptions" on the Promo screen re-runs description generation
against the CURRENT prompt template and pushes each result to YouTube. The
things that must not regress:

* eligibility is explicit — every skipped clip carries a reason;
* the run claims each row atomically, so two tabs can't double-generate;
* generation is FORCED (the chain's "already has a description" short-circuit
  is exactly what we're overriding);
* only the description is pushed — a title/tag list edited on YouTube survives;
* a quota wall stops the batch instead of burning through N identical failures.
"""

from __future__ import annotations

import asyncio
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


# A transcript comfortably over TRANSCRIPT_MIN_USABLE_CHARS.
TRANSCRIPT = "1\n00:00:01,000 --> 00:00:04,000\nWe talk about the thing here.\n"

PARENT = "parent00001"
CHILD_A = "childAAAAAA"
CHILD_B = "childBBBBBB"


def _insert(video_id: str, **cols) -> None:
    from yt_scheduler.config import DB_PATH, UPLOAD_DIR

    path = UPLOAD_DIR / f"{video_id}.mp4"
    path.write_bytes(b"\x00" * 16)
    row = {
        "project_id": 1,
        "title": video_id,
        "status": "ready",
        "video_file_path": str(path),
        "description": "Old description with the link at the bottom.",
    }
    row.update(cols)
    keys = ", ".join(["id", *row.keys()])
    placeholders = ", ".join("?" for _ in range(1 + len(row)))
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            f"INSERT INTO videos ({keys}) VALUES ({placeholders})",
            (video_id, *row.values()),
        )
        conn.commit()


def _column(video_id: str, column: str):
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        row = conn.execute(
            f"SELECT {column} FROM videos WHERE id = ?", (video_id,)
        ).fetchone()
    return row[0] if row else None


def _seed_parent_with_two_hooks() -> None:
    _insert(PARENT, item_type="episode", duration_seconds=600.0,
            url="https://youtu.be/parent00001")
    _insert(CHILD_A, parent_item_id=PARENT, item_type="hook",
            transcript=TRANSCRIPT)
    _insert(CHILD_B, parent_item_id=PARENT, item_type="short",
            transcript=TRANSCRIPT)


def _base() -> str:
    return f"/api/projects/default/videos/{PARENT}/promos"


@pytest.fixture
def fake_generation(monkeypatch: pytest.MonkeyPatch):
    """Stub the two external calls: Claude generation and the YouTube push.

    Returns the list of (video_id, kwargs) pushed to YouTube so a test can
    assert exactly which fields went over the wire.
    """
    ai = importlib.import_module("yt_scheduler.services.ai")
    youtube = importlib.import_module("yt_scheduler.services.youtube")
    pushed: list[tuple[str, dict]] = []

    async def fake_description(**kwargs):
        return f"NEW description for {kwargs.get('title')}"

    def fake_update(video_id: str, **kwargs):
        pushed.append((video_id, kwargs))
        return {}

    monkeypatch.setattr(ai, "generate_seo_description", fake_description)
    monkeypatch.setattr(youtube, "update_video_metadata", fake_update)
    return pushed


def _wait_for_states(video_ids: list[str], *, timeout: float = 10.0) -> None:
    """Block until every id has left the non-terminal update state.

    The endpoint returns 202 with the work detached, so the test has to wait
    for the background task the same way the browser polls for it.
    """
    import time

    deadline = time.monotonic() + timeout
    states: list = []
    while True:
        states = [_column(v, "auto_action_state") for v in video_ids]
        if all(s != "updating_desc" for s in states):
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out waiting for {video_ids}: {states}")
        time.sleep(0.05)


def test_preview_reports_eligible_clips_and_quota(client: TestClient) -> None:
    _seed_parent_with_two_hooks()
    data = client.get(f"{_base()}/update-descriptions/preview").json()
    assert {c["id"] for c in data["eligible"]} == {CHILD_A, CHILD_B}
    assert data["counts"] == {"segment": 0, "short": 1, "hook": 1}
    # 2 clips × (videos.list + videos.update)
    assert data["quota_units_estimate"] == 102
    assert data["ineligible"] == []


def test_preview_names_why_a_clip_is_skipped(client: TestClient) -> None:
    _insert(PARENT, item_type="episode", duration_seconds=600.0)
    _insert(CHILD_A, parent_item_id=PARENT, item_type="hook", transcript="")
    _insert(CHILD_B, parent_item_id=PARENT, item_type="hook",
            transcript=TRANSCRIPT, youtube_deleted=1)
    _insert("localonly", parent_item_id=PARENT, item_type="hook",
            transcript=TRANSCRIPT)

    data = client.get(f"{_base()}/update-descriptions/preview").json()
    assert data["eligible"] == []
    reasons = {c["id"]: c["reason"] for c in data["ineligible"]}
    assert "transcript" in reasons[CHILD_A]
    assert "deleted" in reasons[CHILD_B]
    assert "Not on YouTube" in reasons["localonly"]


def test_tier_filter_narrows_the_run(client: TestClient) -> None:
    _seed_parent_with_two_hooks()
    data = client.get(f"{_base()}/update-descriptions/preview?tiers=hook").json()
    assert {c["id"] for c in data["eligible"]} == {CHILD_A}

    r = client.get(f"{_base()}/update-descriptions/preview?tiers=hook,nope")
    assert r.status_code == 400
    assert "nope" in r.json()["detail"]


def test_update_regenerates_and_pushes_only_the_description(
    client: TestClient, fake_generation: list,
) -> None:
    _seed_parent_with_two_hooks()

    r = client.post(f"{_base()}/update-descriptions", json={})
    assert r.status_code == 202
    body = r.json()
    assert {c["id"] for c in body["started"]} == {CHILD_A, CHILD_B}
    assert body["quota_units_estimate"] == 102

    _wait_for_states([CHILD_A, CHILD_B])

    for child in (CHILD_A, CHILD_B):
        # The chain's "already has a description" short-circuit must NOT apply.
        assert _column(child, "description") == f"NEW description for {child}"
        assert _column(child, "auto_action_state") == "ready"
        assert _column(child, "auto_action_last_error") is None

    pushed = dict(fake_generation)
    assert set(pushed) == {CHILD_A, CHILD_B}
    for kwargs in pushed.values():
        # Title and tags stay untouched so a YouTube-side edit survives.
        assert set(kwargs) == {"description"}
        assert kwargs["description"].startswith("NEW description")


def test_failure_lands_on_its_own_state_not_a_chain_step(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, fake_generation: list,
) -> None:
    """A failed update must not present the chain's Retry, which would skip
    generation and push the OLD text while reporting success."""
    _seed_parent_with_two_hooks()
    youtube = importlib.import_module("yt_scheduler.services.youtube")

    def boom(video_id: str, **kwargs):
        raise RuntimeError("YouTube said no")

    monkeypatch.setattr(youtube, "update_video_metadata", boom)

    r = client.post(f"{_base()}/update-descriptions", json={"video_ids": [CHILD_A]})
    assert r.status_code == 202
    _wait_for_states([CHILD_A])

    assert _column(CHILD_A, "auto_action_state") == "failed:updating_desc"
    assert "YouTube said no" in _column(CHILD_A, "auto_action_last_error")
    # Untouched sibling.
    assert _column(CHILD_B, "auto_action_state") is None


def test_failed_push_leaves_the_local_description_alone(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, fake_generation: list,
) -> None:
    """Push before persist. If the row were written first, a failed push would
    leave the app showing a description YouTube never received — and since these
    clips are already published, nothing would ever reconcile it."""
    _seed_parent_with_two_hooks()
    youtube = importlib.import_module("yt_scheduler.services.youtube")
    monkeypatch.setattr(
        youtube, "update_video_metadata",
        lambda video_id, **kwargs: (_ for _ in ()).throw(RuntimeError("push failed")),
    )

    r = client.post(f"{_base()}/update-descriptions", json={"video_ids": [CHILD_A]})
    assert r.status_code == 202
    _wait_for_states([CHILD_A])

    assert _column(CHILD_A, "auto_action_state") == "failed:updating_desc"
    assert _column(CHILD_A, "description").startswith("Old description")
    assert _column(CHILD_A, "description_generated_at") is None


def test_interrupted_update_is_failed_at_boot_not_left_spinning(
    client: TestClient,
) -> None:
    """A row claimed but never processed (server quit mid-batch) must not stay
    in a running state: both claim helpers refuse non-terminal states, so it
    would be permanently unclaimable with no repair path in the UI."""
    _seed_parent_with_two_hooks()
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            "UPDATE videos SET auto_action_state = 'updating_desc', "
            # Older than the chain's 24h resume window — this sweep must not
            # inherit that cap, or such a row stays wedged forever.
            "updated_at = datetime('now', '-40 hours') WHERE id = ?",
            (CHILD_A,),
        )
        conn.commit()

    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    swept = asyncio.run(_sweep(auto_actions))
    assert swept == 1
    assert _column(CHILD_A, "auto_action_state") == "failed:updating_desc"
    assert "restart" in _column(CHILD_A, "auto_action_last_error")
    # The previous description survives — nothing was half-applied.
    assert _column(CHILD_A, "description").startswith("Old description")


async def _sweep(auto_actions) -> int:
    """Run the boot-time sweep on its own connection, then release it.

    conftest closes leaked aiosqlite connections, but an open one here would
    also keep a non-daemon thread alive for the rest of the test session.
    """
    database = importlib.import_module("yt_scheduler.database")
    try:
        return await auto_actions.fail_interrupted_description_updates()
    finally:
        await database.close_db()


def test_quota_exhaustion_stops_the_batch(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, fake_generation: list,
) -> None:
    """Batch bigger than the concurrency gate: the clips still queued when the
    quota wall is hit are failed with that explanation instead of each being
    walked into its own identical 403."""
    _insert(PARENT, item_type="episode", duration_seconds=600.0)
    children = [f"quota{i:06d}" for i in range(8)]
    for child in children:
        _insert(child, parent_item_id=PARENT, item_type="hook",
                transcript=TRANSCRIPT)
    youtube = importlib.import_module("yt_scheduler.services.youtube")

    attempts: list[str] = []

    def quota_wall(video_id: str, **kwargs):
        attempts.append(video_id)
        raise RuntimeError("HttpError 403: quotaExceeded")

    monkeypatch.setattr(youtube, "update_video_metadata", quota_wall)

    r = client.post(f"{_base()}/update-descriptions", json={})
    assert r.status_code == 202
    _wait_for_states(children)

    assert all(_column(c, "auto_action_state") == "failed:updating_desc"
               for c in children)
    errors = [_column(c, "auto_action_last_error") for c in children]
    assert any("quotaExceeded" in e for e in errors)
    assert any("daily API quota is exhausted" in e for e in errors)
    # The whole point: we stopped calling YouTube rather than burning a
    # doomed request per clip.
    assert len(attempts) < len(children)


def test_busy_clip_is_not_claimed(client: TestClient, fake_generation: list) -> None:
    _seed_parent_with_two_hooks()
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            "UPDATE videos SET auto_action_state = 'uploading' WHERE id = ?",
            (CHILD_A,),
        )
        conn.commit()

    r = client.post(f"{_base()}/update-descriptions", json={})
    assert r.status_code == 202
    body = r.json()
    assert [c["id"] for c in body["started"]] == [CHILD_B]
    skipped = {c["id"]: c["reason"] for c in body["skipped"]}
    assert "Busy" in skipped[CHILD_A]

    _wait_for_states([CHILD_B])
    # The busy row was never touched.
    assert _column(CHILD_A, "auto_action_state") == "uploading"
    assert _column(CHILD_A, "description").startswith("Old description")


def test_explicit_video_ids_must_be_eligible(client: TestClient) -> None:
    _seed_parent_with_two_hooks()
    r = client.post(
        f"{_base()}/update-descriptions", json={"video_ids": ["notachild1"]},
    )
    assert r.status_code == 400
    assert "notachild1" in r.json()["detail"]


def test_claim_is_atomic(client: TestClient) -> None:
    """Two concurrent callers, one winner — the guard against a second tab
    starting a duplicate generation on the same row."""
    _seed_parent_with_two_hooks()
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    async def race() -> list[bool]:
        return list(await asyncio.gather(
            auto_actions.claim_description_update(CHILD_A),
            auto_actions.claim_description_update(CHILD_A),
        ))

    results = asyncio.run(race())
    assert sorted(results) == [False, True]
