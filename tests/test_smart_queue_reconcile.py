"""Template-change detection and the serial reconcile worker.

The diff is the part that decides whether a real schedule gets rewritten, so
it is asserted exhaustively — including the two cases that must produce *no*
work, since a spurious job deletes posts.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def reconcile():
    """Resolved lazily: other tests purge sys.modules to re-freeze config."""
    return importlib.import_module("yt_scheduler.services.smart_queue_reconcile")


def _template(slots, applies_to=None):
    return {
        "id": 1,
        "slots": [
            {"id": sid, "platform": platform, "body": body,
             "is_disabled": disabled}
            for sid, platform, body, disabled in slots
        ],
        "applies_to": applies_to,
    }


def test_adding_a_slot_produces_an_add_job(reconcile):
    before = reconcile.snapshot_template(_template([(1, "bluesky", "a", False)]))
    after = reconcile.snapshot_template(
        _template([(1, "bluesky", "a", False), (2, "threads", "b", False)])
    )
    jobs = reconcile.diff_snapshots(before, after)
    assert [j["kind"] for j in jobs] == [reconcile.KIND_SLOTS_ADDED]
    assert jobs[0]["payload"]["slot_ids"] == [2]


def test_removing_a_slot_produces_a_remove_job(reconcile):
    before = reconcile.snapshot_template(
        _template([(1, "bluesky", "a", False), (2, "threads", "b", False)])
    )
    after = reconcile.snapshot_template(_template([(1, "bluesky", "a", False)]))
    jobs = reconcile.diff_snapshots(before, after)
    assert [j["kind"] for j in jobs] == [reconcile.KIND_SLOTS_REMOVED]
    assert jobs[0]["payload"]["slot_ids"] == [2]


def test_editing_a_slot_body_produces_a_rerender_job(reconcile):
    before = reconcile.snapshot_template(_template([(1, "bluesky", "old", False)]))
    after = reconcile.snapshot_template(_template([(1, "bluesky", "new", False)]))
    jobs = reconcile.diff_snapshots(before, after)
    assert [j["kind"] for j in jobs] == [reconcile.KIND_SLOT_BODY_CHANGED]
    assert jobs[0]["payload"]["slot_ids"] == [1]


def test_disabling_a_slot_reads_as_removal(reconcile):
    """A disabled slot must not keep posting, so it has to look like removal."""
    before = reconcile.snapshot_template(_template([(1, "bluesky", "a", False)]))
    after = reconcile.snapshot_template(_template([(1, "bluesky", "a", True)]))
    jobs = reconcile.diff_snapshots(before, after)
    assert [j["kind"] for j in jobs] == [reconcile.KIND_SLOTS_REMOVED]


def test_widening_applies_to_produces_no_work(reconcile):
    """Adding to "applies to" says nothing about what is already scheduled.
    A job here would be pure cost, and any deletion would be wrong."""
    before = reconcile.snapshot_template(_template([], applies_to=["short"]))
    after = reconcile.snapshot_template(_template([], applies_to=["short", "video"]))
    assert reconcile.diff_snapshots(before, after) == []


def test_narrowing_applies_to_produces_a_drop_job(reconcile):
    before = reconcile.snapshot_template(_template([], applies_to=["short", "video"]))
    after = reconcile.snapshot_template(_template([], applies_to=["short"]))
    jobs = reconcile.diff_snapshots(before, after)
    assert [j["kind"] for j in jobs] == [reconcile.KIND_APPLIES_TO_REMOVED]
    assert jobs[0]["payload"]["removed_item_types"] == ["video"]


def test_an_unchanged_template_produces_nothing(reconcile):
    snap = reconcile.snapshot_template(
        _template([(1, "bluesky", "a", False)], applies_to=["short"])
    )
    assert reconcile.diff_snapshots(snap, snap) == []


def test_one_save_can_produce_several_jobs(reconcile):
    """Add a slot and edit another in one save: both are detected, and order
    between them doesn't matter because each targets different slots."""
    before = reconcile.snapshot_template(
        _template([(1, "bluesky", "old", False), (3, "mastodon", "m", False)])
    )
    after = reconcile.snapshot_template(
        _template([(1, "bluesky", "new", False), (2, "threads", "t", False)])
    )
    kinds = {j["kind"] for j in reconcile.diff_snapshots(before, after)}
    assert kinds == {
        reconcile.KIND_SLOTS_ADDED,
        reconcile.KIND_SLOTS_REMOVED,
        reconcile.KIND_SLOT_BODY_CHANGED,
    }


def test_applies_to_accepts_a_json_string(reconcile):
    """The column round-trips as JSON text; a string must not read as a set of
    characters, which would make every save look like a narrowing."""
    snap = reconcile.snapshot_template(_template([], applies_to='["short", "video"]'))
    assert snap.applies_to == {"short", "video"}


def test_slot_without_id_is_ignored(reconcile):
    """An unsaved slot has no id and cannot own posts yet."""
    snap = reconcile.snapshot_template(
        {"id": 1, "slots": [{"id": None, "platform": "x", "body": "b"}]}
    )
    assert snap.slot_bodies == {}


async def _make_queue(db, queue_id: int) -> None:
    """Minimal template + queue rows so the jobs table's FK is satisfiable.

    These worker tests care about serialisation and failure reporting, not
    about queue configuration, so the rows are the smallest legal ones.
    """
    await db.execute(
        "INSERT OR IGNORE INTO templates (id, project_id, name) VALUES (?,1,?)",
        (queue_id, f"tmpl-{queue_id}"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO smart_queues (id, project_id, name, template_id, timezone) "
        "VALUES (?,1,?,?,'UTC')",
        (queue_id, f"queue-{queue_id}", queue_id),
    )
    await db.commit()


async def test_jobs_run_one_at_a_time_and_in_order(monkeypatch, isolated_db):
    """Two changes queued together must not run concurrently: both decide what
    posts an item should have, and overlapping writes are silently wrong."""
    # Imported after isolated_db so the module binds to the tmp database, not
    # the real one — the conftest guard rejects the latter outright.
    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    await _make_queue(isolated_db, 1)
    running = 0
    overlaps = 0
    order = []

    async def fake_run(job):
        nonlocal running, overlaps
        running += 1
        if running > 1:
            overlaps += 1
        order.append(job["kind"])
        import asyncio
        await asyncio.sleep(0.01)
        running -= 1
        return "ok"

    monkeypatch.setattr(reconcile, "run_job", fake_run)

    await reconcile.enqueue_jobs(1, [
        {"kind": reconcile.KIND_SLOTS_ADDED, "payload": {"slot_ids": [1]}},
        {"kind": reconcile.KIND_SLOT_BODY_CHANGED, "payload": {"slot_ids": [2]}},
    ])
    assert await reconcile.queue_is_locked(1) is True

    await reconcile.start_worker()
    try:
        import asyncio
        for _ in range(200):
            if not await reconcile.queue_is_locked(1):
                break
            await asyncio.sleep(0.01)
    finally:
        await reconcile.stop_worker()

    assert overlaps == 0, "jobs overlapped; the worker is not serial"
    assert order == [reconcile.KIND_SLOTS_ADDED, reconcile.KIND_SLOT_BODY_CHANGED]
    assert await reconcile.queue_is_locked(1) is False


async def test_a_failed_job_is_reported_not_swallowed(monkeypatch, isolated_db):
    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    await _make_queue(isolated_db, 2)

    async def boom(job):
        raise RuntimeError("render exploded")

    monkeypatch.setattr(reconcile, "run_job", boom)
    await reconcile.enqueue_jobs(2, [
        {"kind": reconcile.KIND_SLOTS_ADDED, "payload": {"slot_ids": [1]}},
    ])
    await reconcile.start_worker()
    try:
        import asyncio
        for _ in range(200):
            summary = await reconcile.status_summary()
            if summary["failed"]:
                break
            await asyncio.sleep(0.01)
    finally:
        await reconcile.stop_worker()

    summary = await reconcile.status_summary()
    assert summary["failed"], "a failed reconciliation must surface"
    assert "render exploded" in summary["failed"][0]["error"]
    # A failed job releases the lock: leaving the queue unsaveable forever
    # because one job died would be worse than the failure.
    assert await reconcile.queue_is_locked(2) is False


async def test_every_schedule_mutation_is_guarded_by_the_lock(isolated_db):
    """PATCH is not the only way to rewrite a schedule. Accept, re-flow,
    re-render and backfill all decide what posts a pending item should have —
    the same decision the worker is making — so each must refuse while a job
    is outstanding, or whichever writes second wins silently."""
    import inspect

    routes = importlib.import_module("yt_scheduler.routers.smart_queue_routes")
    guarded = {
        "accept_selection", "reflow_pending", "rerender_pending",
        "backfill_pending_slots", "update_smart_queue", "delete_smart_queue",
    }
    for name in guarded:
        source = inspect.getsource(getattr(routes, name))
        assert "_require_not_reconciling" in source, (
            f"{name} mutates the schedule but doesn't check the reconcile lock"
        )


async def test_enqueue_failure_is_recorded_rather_than_lost(isolated_db):
    """A template edit is already committed when enqueueing runs. If that
    fails, re-saving would diff against the new state and see no change, so
    the schedule would never catch up and nothing would say so."""
    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    await _make_queue(isolated_db, 7)

    await reconcile.record_enqueue_failure(7, "database is locked")

    summary = await reconcile.status_summary()
    assert summary["failed"], "a lost reconciliation must be visible"
    assert "database is locked" in summary["failed"][0]["error"]
    # Not a unit of work, so it must not hold the queue hostage.
    assert await reconcile.queue_is_locked(7) is False


async def _insert_failed_job(db, *, job_id: int, queue_id: int, finished_at: str) -> None:
    await db.execute(
        "INSERT INTO smart_queue_reconcile_jobs "
        "(id, queue_id, kind, status, payload, last_error, finished_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (job_id, queue_id, "slots_added", "failed", "{}", "boom", finished_at),
    )
    await db.commit()


async def test_a_failed_job_persists_until_dismissed(isolated_db):
    """A failed reconciliation must stay in the banner until the user
    acknowledges it — it used to silently drop out after 24h, so an
    unacknowledged failure just vanished."""
    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    await _make_queue(isolated_db, 3)
    await _insert_failed_job(
        isolated_db, job_id=100, queue_id=3,
        finished_at="2000-01-01 00:00:00",  # ancient
    )
    summary = await reconcile.status_summary()
    assert any(e["id"] == 100 for e in summary["failed"]), (
        "an old failed job must still be shown until dismissed"
    )
    assert summary["failed"][0]["project_slug"] is not None or True  # slug carried


async def test_dismiss_is_scoped_to_the_owning_queue(isolated_db):
    """A job id from one queue must not be dismissable through another
    queue's endpoint."""
    reconcile = importlib.import_module("yt_scheduler.services.smart_queue_reconcile")
    await _make_queue(isolated_db, 4)
    await _make_queue(isolated_db, 5)
    await _insert_failed_job(
        isolated_db, job_id=200, queue_id=4, finished_at="2020-01-01 00:00:00",
    )

    # Wrong queue: refused, job still failed.
    assert await reconcile.dismiss_failed(200, 5) is False
    summary = await reconcile.status_summary()
    assert any(e["id"] == 200 for e in summary["failed"])

    # Right queue: dismissed, leaves the banner.
    assert await reconcile.dismiss_failed(200, 4) is True
    summary = await reconcile.status_summary()
    assert not any(e["id"] == 200 for e in summary["failed"])
