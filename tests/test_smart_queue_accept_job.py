"""Accept runs on the reconcile worker, not inside its HTTP request.

It renders every post it schedules — one Anthropic call per post — so a 27-video
batch held the connection for two and a half minutes behind a static
"Scheduling…" label, with no progress anywhere and no way to tell a slow run
from a wedged one. Its outcome existed only in the response body, so navigating
away discarded the record of which slots were skipped and which videos failed.

It is now the fifth job kind, alongside the four a template edit queues: same
worker, same lock, same banner, same persisted progress and detail.

These tests pin the parts that are easy to break silently:

* the route enqueues instead of scheduling, and still refuses a locked queue
* the handler calls the LOCKED body — going through the public wrapper would
  deadlock the worker against the lock ``run_job`` already holds
* progress counts videos and advances even when one fails
* a batch with per-video errors fails the job rather than finishing quietly
* a clean batch's report survives the request that started it
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from tests.conftest import install_in_memory_keychain

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """App + DB on a throwaway data dir, with one queue and one template slot."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for name in list(sys.modules):
        if name.startswith("yt_scheduler"):
            sys.modules.pop(name, None)

    install_in_memory_keychain(
        monkeypatch, importlib.import_module("yt_scheduler.services.keychain")
    )
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    app_module = importlib.import_module("yt_scheduler.app")

    db = await database.get_db()
    await projects.ensure_default_project()

    # "testserver" is the one non-loopback host TrustedHostMiddleware allows;
    # anything else is rejected with "Invalid host header" before routing.
    transport = ASGITransport(app=app_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as http:
        yield http, db
    await database.close_db()


async def _make_queue(http, db) -> int:
    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'clips', '[\"hook\"]')"
    )
    template_id = int(cursor.lastrowid)
    await db.execute(
        "INSERT INTO template_slots (template_id, platform, body, media, max_chars) "
        "VALUES (?, 'bluesky', 'Watch {{title}}', 'none', 300)",
        (template_id,),
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues",
        json={
            "name": "Q", "template_id": template_id, "timezone": "UTC",
            "slots": [{"weekday": d, "time_of_day": "09:00"} for d in range(7)],
            "min_duration_seconds": 0, "max_duration_seconds": 600,
            "orientations": ["portrait"], "exclude_already_posted": True,
            "auto_add_on_live": False, "missed_policy": "post_late",
            "missed_grace_hours": 24,
        },
    )
    assert created.status_code == 200, created.text
    return int(created.json()["id"])


async def _add_waiting_video(db, queue_id: int, video_id: str) -> None:
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES (?, ?, 1, 'Clip', 'hook', 60, 'public', 1080, 1920)",
        (video_id, video_id),
    )
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, ?, (SELECT COUNT(*) FROM smart_queue_items WHERE queue_id = ?), "
        "'queued')",
        (queue_id, video_id, queue_id),
    )
    await db.commit()


async def test_accept_enqueues_a_job_instead_of_scheduling_inline(client) -> None:
    """The route must return at once. Holding the connection for the whole
    render is what left the user staring at a static label for minutes."""
    http, db = client
    queue_id = await _make_queue(http, db)
    await _add_waiting_video(db, queue_id, "vid00000001")

    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    response = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/accept", json={}
    )
    assert response.status_code == 200, response.text
    assert response.json()["queued"] is True

    rows = await db.execute_fetchall(
        "SELECT kind, status, payload FROM smart_queue_reconcile_jobs"
    )
    assert len(rows) == 1
    assert rows[0]["kind"] == reconcile.KIND_ACCEPT
    assert rows[0]["status"] == reconcile.STATUS_PENDING

    # Nothing is scheduled until the worker runs it — the whole point.
    items = await db.execute_fetchall("SELECT state FROM smart_queue_items")
    assert [row["state"] for row in items] == ["queued"]


async def test_a_queued_accept_locks_the_queue_against_a_second_one(client) -> None:
    """Two Accepts on one queue would each read the same "next free time" and
    "next position" maxima and stamp the same instants. The 409 that already
    guarded the other job kinds now guards this one for free."""
    http, db = client
    queue_id = await _make_queue(http, db)
    await _add_waiting_video(db, queue_id, "vid00000001")

    first = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/accept", json={}
    )
    assert first.status_code == 200
    second = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/accept", json={}
    )
    assert second.status_code == 409, second.text


async def test_accept_refuses_a_queue_with_no_posting_times(client) -> None:
    """Dead on arrival and immediately fixable, so it is a 400 at the button
    rather than a red banner the user has to go and read."""
    http, db = client
    queue_id = await _make_queue(http, db)
    await db.execute("DELETE FROM smart_queue_slots WHERE queue_id = ?", (queue_id,))
    await db.commit()

    response = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/accept", json={}
    )
    assert response.status_code == 400, response.text
    assert "no posting times" in response.json()["detail"]
    rows = await db.execute_fetchall("SELECT 1 FROM smart_queue_reconcile_jobs")
    assert rows == [], "a request refused up front must not leave a job behind"


async def test_the_handler_does_not_deadlock_against_the_worker_s_lock(client) -> None:
    """``run_job`` takes the queue's Accept lock for every kind. A handler that
    called the public ``accept_selection`` wrapper would wait forever on a lock
    its own caller holds — and the symptom is a job stuck at 'running' that
    409s every mutation on that queue until the app restarts."""
    http, db = client
    queue_id = await _make_queue(http, db)
    await _add_waiting_video(db, queue_id, "vid00000001")
    await http.post(f"/api/projects/default/smart-queues/{queue_id}/accept", json={})

    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    job = await reconcile._claim_next_job()
    assert job is not None

    # Generous, but it is a deadlock we are ruling out — the real thing never
    # returns, so any finite bound distinguishes it.
    await asyncio.wait_for(reconcile.run_job(job), timeout=30)

    items = await db.execute_fetchall("SELECT state FROM smart_queue_items")
    assert [row["state"] for row in items] == ["scheduled"]


async def test_progress_counts_videos_and_survives_a_failure(client) -> None:
    """Progress is per VIDEO, because that is the number the user selected.

    It advances in a `finally`, so a video that fails still moves the count —
    a batch frozen at 1 of 3 while it works through video 2 reads exactly like
    a wedged job, which is the confusion this whole change exists to end.
    """
    http, db = client
    queue_id = await _make_queue(http, db)
    for video_id in ("vid00000001", "vid00000002", "vid00000003"):
        await _add_waiting_video(db, queue_id, video_id)

    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")
    real_plan = accept._plan_video

    async def plan_with_one_failure(db_, video, slots, **kwargs):
        if video["id"] == "vid00000002":
            return accept.VideoPlan(
                posts=[], skipped_slots=[], transient_error="Anthropic overloaded"
            )
        return await real_plan(db_, video, slots, **kwargs)

    accept._plan_video = plan_with_one_failure
    seen: list[tuple[int, int]] = []

    async def progress(done: int, total: int) -> None:
        seen.append((done, total))

    try:
        result = await accept.accept_selection(queue_id, [], progress=progress)
    finally:
        accept._plan_video = real_plan

    assert seen[0] == (0, 3), "the total is published before the first render"
    assert seen[-1] == (3, 3), "a failed video still advances the count"
    assert [done for done, _ in seen] == [0, 1, 2, 3]
    assert len(result["errors"]) == 1


async def test_a_batch_with_failures_fails_the_job_and_says_what_landed(
    client,
) -> None:
    """A job that finishes 'done' leaves the banner immediately. A batch where
    two of three videos were left untouched is not something to let scroll
    away — it stays until retried or dismissed, and it reports what DID land so
    the user is not left guessing at the damage."""
    http, db = client
    queue_id = await _make_queue(http, db)
    for video_id in ("vid00000001", "vid00000002"):
        await _add_waiting_video(db, queue_id, video_id)

    handlers = importlib.import_module(
        "yt_scheduler.services.smart_queue_reconcile_handlers"
    )
    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")
    real_plan = accept._plan_video

    async def plan_with_one_failure(db_, video, slots, **kwargs):
        if video["id"] == "vid00000002":
            return accept.VideoPlan(
                posts=[], skipped_slots=[], transient_error="Anthropic overloaded"
            )
        return await real_plan(db_, video, slots, **kwargs)

    accept._plan_video = plan_with_one_failure
    try:
        with pytest.raises(reconcile.ReportedJobFailure) as caught:
            await handlers.accept_videos(queue_id, [], _noop_progress)
    finally:
        accept._plan_video = real_plan

    message = str(caught.value)
    assert "Scheduled 1 video." in message, message
    assert "vid00000002" in message and "Anthropic overloaded" in message, message
    assert "press Accept again to retry" in message, message
    # The message is the user's, so it must not be prefixed with a class name
    # the way an unexpected exception is.
    assert not message.startswith("ReportedJobFailure"), message


async def test_a_clean_run_reports_through_the_worker(client) -> None:
    """The outcome has to outlive the request that started it. Skipped slots
    are the case that proves it: a clean Accept still has something to say, and
    it used to say it only in a response body."""
    http, db = client
    queue_id = await _make_queue(http, db)
    await _add_waiting_video(db, queue_id, "vid00000001")
    await http.post(f"/api/projects/default/smart-queues/{queue_id}/accept", json={})

    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    await reconcile.start_worker()
    try:
        for _ in range(300):
            rows = await db.execute_fetchall(
                "SELECT status, detail FROM smart_queue_reconcile_jobs"
            )
            if rows and rows[0]["status"] == reconcile.STATUS_DONE:
                break
            await asyncio.sleep(0.01)
    finally:
        await reconcile.stop_worker()

    rows = await db.execute_fetchall(
        "SELECT status, detail, progress_done, progress_total "
        "FROM smart_queue_reconcile_jobs"
    )
    assert rows[0]["status"] == reconcile.STATUS_DONE
    assert rows[0]["detail"] == "Scheduled 1 video."
    assert (rows[0]["progress_done"], rows[0]["progress_total"]) == (1, 1)

    summary = await reconcile.status_summary()
    assert [job["id"] for job in summary["completed"]] == [1], (
        "a just-finished job must be reportable — it is the only record of "
        "what a clean Accept did"
    )
    assert summary["completed"][0]["detail"] == "Scheduled 1 video."
    # Finished work must not keep the queue locked, or every mutation would 409
    # for the whole reporting window.
    assert summary["locked_queue_ids"] == []


async def _noop_progress(done: int, total: int) -> None:
    return None


async def test_an_interrupted_accept_is_safe_to_re_run(client) -> None:
    """Moving Accept onto the worker made restart-recovery load-bearing.

    ``start_worker`` resets every ``running`` job back to ``pending``, so an
    Accept the app died in the middle of runs AGAIN from the top with the same
    payload. That is a clear improvement on the old inline behaviour, where the
    remainder was simply lost and nothing retried it — but only if a re-run
    cannot double-book what the first pass already scheduled.

    Two separate guards make it safe, and this exercises both: a waiting item
    promoted to 'scheduled' is no longer in state 'queued' so the waiting query
    skips it, and an explicit video_id is caught by already_scheduled_video_ids.
    """
    http, db = client
    queue_id = await _make_queue(http, db)
    await _add_waiting_video(db, queue_id, "vid00000001")
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES ('vid00000002', 'vid00000002', 1, 'Clip', 'hook', 60, "
        "'public', 1080, 1920)"
    )
    await db.commit()

    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")
    first = await accept.accept_selection(queue_id, ["vid00000002"])
    assert first["scheduled"] == 2

    scheduled_at = [
        row["scheduled_at"]
        for row in await db.execute_fetchall(
            "SELECT scheduled_at FROM smart_queue_items ORDER BY position"
        )
    ]

    # Exactly what the worker replays after a restart: same payload, again.
    second = await accept.accept_selection(queue_id, ["vid00000002"])

    assert second["scheduled"] == 0, "a replay must schedule nothing twice"
    assert {entry["reason"] for entry in second["skipped"]} == {
        "already scheduled by this queue"
    }
    items = await db.execute_fetchall(
        "SELECT scheduled_at FROM smart_queue_items ORDER BY position"
    )
    assert [row["scheduled_at"] for row in items] == scheduled_at, (
        "a replay must not append items or move existing posting times"
    )
    posts = await db.execute_fetchall("SELECT COUNT(*) AS n FROM social_posts")
    assert posts[0]["n"] == 2, "one post per video, not two"
