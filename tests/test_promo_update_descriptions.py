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
import json
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
    # Force the encrypted-file keychain. Without this the suite reads the real
    # login Keychain, which is exactly what the project forbids touching from
    # automated runs.
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


def _quota_http_error(
    *, reason: str = "quotaExceeded", extra_error_fields: dict | None = None,
):
    """A real googleapiclient HttpError shaped like YouTube's actual refusal.

    Not a RuntimeError carrying a lookalike string: the whole point of the
    classifier is that it reads the response body, so a fake that has no body
    would let the test pass while detection was broken against the live API.
    """
    import httplib2
    from googleapiclient.errors import HttpError

    message = (
        "The request cannot be completed because you have exceeded your quota."
    )
    error = {
        "code": 403,
        "message": message,
        "errors": [
            {"message": message, "domain": "youtube.quota", "reason": reason}
        ],
    }
    error.update(extra_error_fields or {})
    return HttpError(
        httplib2.Response({"status": 403, "reason": "Forbidden"}),
        json.dumps({"error": error}).encode(),
        uri="https://youtube.googleapis.com/youtube/v3/videos"
            "?part=snippet%2Cstatus&alt=json",
    )


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
        raise _quota_http_error()

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


def test_quota_reason_is_read_from_the_body_not_the_rendered_string(
    client: TestClient,
) -> None:
    """HttpError renders only the FIRST of detail/details/errors/message, so a
    payload that also carries `details` stringifies with no reason code in it at
    all — the shape that silently defeats substring matching."""
    from yt_scheduler.services.youtube import daily_quota_exhausted_reason

    assert daily_quota_exhausted_reason(_quota_http_error()) == "quotaExceeded"

    with_details = _quota_http_error(extra_error_fields={
        "status": "RESOURCE_EXHAUSTED",
        "details": [{
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "RATE_LIMIT_EXCEEDED",
        }],
    })
    assert "quotaExceeded" not in str(with_details)
    assert daily_quota_exhausted_reason(with_details) == "quotaExceeded"

    # Transient throttling is not an exhausted day; it must not stop a batch.
    assert daily_quota_exhausted_reason(
        _quota_http_error(reason="rateLimitExceeded")) is None
    # Text that merely mentions the word is not the machine-readable signal —
    # a transcript, a variable name or an HTML error page must never be able to
    # claim the user's quota is gone.
    assert daily_quota_exhausted_reason(
        RuntimeError("the transcript discusses the quotaExceeded error")) is None


def test_transient_rate_limit_does_not_stop_the_batch(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, fake_generation: list,
) -> None:
    """rateLimitExceeded means "slow down", not "your day is over". Aborting on
    it would abandon clips that were about to succeed and tell the user
    something untrue about their quota."""
    _insert(PARENT, item_type="episode", duration_seconds=600.0)
    children = [f"rate{i:07d}" for i in range(6)]
    for child in children:
        _insert(child, parent_item_id=PARENT, item_type="hook",
                transcript=TRANSCRIPT)
    youtube = importlib.import_module("yt_scheduler.services.youtube")

    attempts: list[str] = []

    def throttled(video_id: str, **kwargs):
        attempts.append(video_id)
        raise _quota_http_error(reason="rateLimitExceeded")

    monkeypatch.setattr(youtube, "update_video_metadata", throttled)

    r = client.post(f"{_base()}/update-descriptions", json={})
    assert r.status_code == 202
    _wait_for_states(children)

    # Every clip was tried, and none was told the daily quota was exhausted.
    assert len(attempts) == len(children)
    for child in children:
        assert "daily API quota is exhausted" not in (
            _column(child, "auto_action_last_error") or ""
        )


def test_partial_claim_failure_wedges_no_clip(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DB fault part-way through the claim loop must not commit the claims
    that already succeeded. A claimed row with no runner behind it is
    unclaimable, renders as a permanent spinner with no Retry, and is only
    settled by a server restart."""
    _insert(PARENT, item_type="episode", duration_seconds=600.0)
    children = [f"wedge{i:06d}" for i in range(5)]
    for child in children:
        _insert(child, parent_item_id=PARENT, item_type="hook",
                transcript=TRANSCRIPT)

    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    promo_routes = importlib.import_module("yt_scheduler.routers.promo_routes")
    real_claim = auto_actions.claim_description_update
    calls: list[str] = []

    async def failing_claim(video_id: str) -> bool:
        calls.append(video_id)
        if len(calls) == 3:
            raise sqlite3.OperationalError("disk I/O error")
        return await real_claim(video_id)

    monkeypatch.setattr(
        promo_routes.auto_actions, "claim_description_update", failing_claim
    )

    with pytest.raises(sqlite3.OperationalError):
        client.post(f"{_base()}/update-descriptions", json={})

    assert len(calls) == 3
    # The two claims that had already succeeded were rolled back with the rest.
    assert [_column(c, "auto_action_state") for c in children] == [None] * 5


def test_batch_failure_does_not_clobber_clips_that_already_finished() -> None:
    """The batch-level rescue is scoped by state, not just id.

    A blanket "fail every id" would overwrite a clip that had already been
    updated — claiming its update failed when YouTube has the new description,
    and inviting the user to re-spend quota redoing work that succeeded.
    """
    async def scenario() -> tuple[str, str, int]:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            import os
            os.environ["DYS_DATA_DIR"] = tmp
            for mod in list(sys.modules.keys()):
                if mod.startswith("yt_scheduler"):
                    sys.modules.pop(mod, None)
            database = importlib.import_module("yt_scheduler.database")
            auto_actions = importlib.import_module(
                "yt_scheduler.services.auto_actions"
            )
            db = await database.get_db()
            try:
                await db.execute(
                    "INSERT INTO videos (id, project_id, title, auto_action_state) "
                    "VALUES ('doneAAAAAAA', 1, 'finished', 'ready')"
                )
                await db.execute(
                    "INSERT INTO videos (id, project_id, title, auto_action_state) "
                    "VALUES ('stuckBBBBBB', 1, 'mid-flight', 'updating_desc')"
                )
                await db.commit()
                stranded = await auto_actions._fail_unfinished_description_updates(
                    ["doneAAAAAAA", "stuckBBBBBB"], "boom",
                )
                rows = await db.execute_fetchall(
                    "SELECT id, auto_action_state FROM videos ORDER BY id"
                )
                states = {r["id"]: r["auto_action_state"] for r in rows}
                return states["doneAAAAAAA"], states["stuckBBBBBB"], stranded
            finally:
                await database.close_db()

    done_state, stuck_state, stranded = asyncio.run(scenario())
    assert done_state == "ready"                      # untouched
    assert stuck_state == "failed:updating_desc"      # rescued
    assert stranded == 1


def test_empty_tier_selection_is_refused_not_treated_as_everything(
    client: TestClient,
) -> None:
    """`[]` means "nothing selected". Reading it as "every tier" would spend a
    Claude call and ~51 quota units per clip that nobody asked for."""
    _seed_parent_with_two_hooks()

    r = client.post(f"{_base()}/update-descriptions", json={"tiers": []})
    assert r.status_code == 400
    assert "tiers" in r.json()["detail"]
    assert [_column(c, "auto_action_state") for c in (CHILD_A, CHILD_B)] == [None] * 2

    assert client.get(f"{_base()}/update-descriptions/preview?tiers=").status_code == 400


def test_tiers_is_a_real_list_not_a_comma_string(client: TestClient) -> None:
    """The POST takes a JSON list, so ["hook,segment"] is one unknown tier —
    not two tiers smuggled through a string round-trip."""
    _seed_parent_with_two_hooks()
    r = client.post(
        f"{_base()}/update-descriptions", json={"tiers": ["hook,segment"]},
    )
    assert r.status_code == 400
    assert "hook,segment" in r.json()["detail"]


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
