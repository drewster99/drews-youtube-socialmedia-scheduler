"""Push template edits onto schedules already built from that template.

Slot membership and post text are both decided at Accept. Without this, a later
template edit reached nothing already scheduled — adding a slot left every
existing item without it, removing one left orphaned posts that still went out,
and editing a slot's body left rendered text stale. All of it silent.

Four changes matter, and each maps to one job kind:

``slots_added``        render and schedule that slot for every pending item
``slots_removed``      delete pending posts belonging to those slots
``slot_body_changed``  re-render pending posts for that slot
``applies_to_removed`` delete pending posts for videos the queue no longer takes

Adding to "applies to" is deliberately not a job: it widens what *future*
Accepts may pick up and says nothing about what is already scheduled.

A fifth kind, ``accept``, is not a template change at all — the user presses
Accept. It lives here because it is the same shape of work and wants the same
machinery: minutes of rendering, one queue at a time, progress somebody can see,
and an outcome that survives the request. It was the last such job still running
inside its HTTP request.

Jobs are persisted and run on a single worker, so only one is ever in flight —
they re-render N posts with an AI round-trip apiece, and two at once on the
same queue would race each other's writes. Work outliving the request that
caused it is the point: a save must not block for minutes, and a restart must
not lose the reconciliation.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

from yt_scheduler.database import get_db, write_transaction

logger = logging.getLogger(__name__)

KIND_SLOTS_ADDED = "slots_added"
KIND_SLOTS_REMOVED = "slots_removed"
KIND_SLOT_BODY_CHANGED = "slot_body_changed"
KIND_APPLIES_TO_REMOVED = "applies_to_removed"
# Not queued by a template diff — the user presses Accept. It is here because it
# is the same shape of work as the four above (render N posts, one Anthropic
# call apiece, minutes long) and because it was the last one still running
# inside its HTTP request: no progress anywhere, and an outcome that existed
# only in a response body the browser threw away if the user navigated.
KIND_ACCEPT = "accept"
# Not a unit of work — only ever recorded already-failed, so a template edit
# that could not be queued is visible rather than silently unreconciled.
KIND_ENQUEUE_FAILED = "enqueue_failed"

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"

_UNFINISHED = (STATUS_PENDING, STATUS_RUNNING)

KIND_LABELS = {
    KIND_ACCEPT: "Scheduling accepted videos",
    KIND_SLOTS_ADDED: "Adding posts for new slots",
    KIND_SLOTS_REMOVED: "Removing posts for deleted slots",
    KIND_SLOT_BODY_CHANGED: "Re-rendering posts for edited slots",
    KIND_APPLIES_TO_REMOVED: "Removing posts for videos no longer included",
    KIND_ENQUEUE_FAILED: "Template change could not be scheduled",
}


class ReportedJobFailure(Exception):
    """A job that ran, decided its own outcome was bad, and wrote the message.

    Every other exception reaching the worker is a surprise, so it is recorded
    as ``TypeName: text`` — "APIStatusError" and "KeyError" say very different
    things about who should look at it. This one is not a surprise: Accept
    finishing with two of twenty-seven videos unscheduled is a normal, expected
    result that the user has to see and act on, and prefixing a sentence written
    for them with a Python class name makes it read like a crash. ``str(exc)``
    is used verbatim.
    """


# One worker, process-wide. Serialises every queue's reconciliation against
# every other's, which is what makes "queued behind" true rather than hopeful.
_worker_task: asyncio.Task | None = None
_wake = asyncio.Event()


@dataclass(frozen=True)
class TemplateSnapshot:
    """What a template looked like, reduced to the parts a schedule depends on.

    Slot bodies are compared by value: an edit to the generator text is exactly
    the signal that already-rendered posts are stale.
    """

    slot_bodies: dict[int, str]
    slot_platforms: dict[int, str]
    applies_to: set[str]


def snapshot_template(template: dict) -> TemplateSnapshot:
    """Reduce a template dict (with ``slots``) to its schedule-relevant parts.

    Disabled slots are excluded: a disabled slot should neither gain posts nor
    keep them, and it reads as removed, which is the behaviour we want.
    """
    bodies: dict[int, str] = {}
    platforms: dict[int, str] = {}
    for slot in template.get("slots") or []:
        slot_id = slot.get("id")
        if slot_id is None or slot.get("is_disabled"):
            continue
        bodies[int(slot_id)] = slot.get("body") or ""
        platforms[int(slot_id)] = slot.get("platform") or ""
    applies = template.get("applies_to")
    if isinstance(applies, str):
        try:
            applies = json.loads(applies)
        except (TypeError, ValueError):
            applies = []
    return TemplateSnapshot(
        slot_bodies=bodies,
        slot_platforms=platforms,
        applies_to={str(a) for a in (applies or [])},
    )


def diff_snapshots(before: TemplateSnapshot, after: TemplateSnapshot) -> list[dict]:
    """Job specs for one template edit. Empty when nothing schedule-relevant moved."""
    jobs: list[dict] = []

    added = sorted(set(after.slot_bodies) - set(before.slot_bodies))
    if added:
        jobs.append({"kind": KIND_SLOTS_ADDED, "payload": {"slot_ids": added}})

    removed = sorted(set(before.slot_bodies) - set(after.slot_bodies))
    if removed:
        jobs.append({"kind": KIND_SLOTS_REMOVED, "payload": {"slot_ids": removed}})

    edited = sorted(
        slot_id for slot_id in set(before.slot_bodies) & set(after.slot_bodies)
        if before.slot_bodies[slot_id] != after.slot_bodies[slot_id]
    )
    if edited:
        jobs.append({"kind": KIND_SLOT_BODY_CHANGED, "payload": {"slot_ids": edited}})

    # Widening "applies to" says nothing about what is already scheduled, so
    # only removals produce work.
    dropped = sorted(before.applies_to - after.applies_to)
    if dropped:
        jobs.append({
            "kind": KIND_APPLIES_TO_REMOVED,
            "payload": {"removed_item_types": dropped},
        })
    return jobs


async def enqueue_jobs(queue_id: int, jobs: list[dict]) -> list[int]:
    """Persist job specs for one queue and wake the worker."""
    if not jobs:
        return []
    ids: list[int] = []
    async with write_transaction() as db:
        for job in jobs:
            cursor = await db.execute(
                "INSERT INTO smart_queue_reconcile_jobs (queue_id, kind, payload) "
                "VALUES (?,?,?)",
                (queue_id, job["kind"], json.dumps(job.get("payload") or {})),
            )
            ids.append(int(cursor.lastrowid))
    logger.info("reconcile: queued %s job(s) for queue %s: %s",
                len(ids), queue_id, ", ".join(j["kind"] for j in jobs))
    _wake.set()
    return ids


async def enqueue_for_template(template_id: int, before: TemplateSnapshot,
                               after: TemplateSnapshot) -> dict:
    """Queue reconciliation for every smart queue built on this template."""
    jobs = diff_snapshots(before, after)
    if not jobs:
        return {"queues": 0, "jobs": 0}

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM smart_queues WHERE template_id = ?", (template_id,)
    )
    total = 0
    for row in rows:
        total += len(await enqueue_jobs(int(row["id"]), jobs))
    return {"queues": len(rows), "jobs": total}


async def record_enqueue_failure(template_id: int, error: str) -> None:
    """Record that a template edit could not be turned into reconcile jobs.

    The edit is committed by this point, so the alternative is a schedule that
    silently never catches up — re-saving would diff against the new state and
    see no change. A failed row puts it in the banner where it can be seen and
    the edit re-applied deliberately.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM smart_queues WHERE template_id = ?", (template_id,)
    )
    if not rows:
        return
    async with write_transaction() as write_db:
        for row in rows:
            await write_db.execute(
                "INSERT INTO smart_queue_reconcile_jobs "
                "(queue_id, kind, payload, status, last_error, finished_at) "
                "VALUES (?,?,?,?,?,datetime('now'))",
                (int(row["id"]), KIND_ENQUEUE_FAILED, "{}", STATUS_FAILED,
                 f"Template change was saved but could not be scheduled for "
                 f"reconciliation: {error}. Re-apply the template edit."),
            )


async def queue_is_locked(queue_id: int) -> bool:
    """True while this queue has reconciliation outstanding.

    Editing it then would race the worker: both decide which posts a pending
    item should have, and the loser's writes are silently wrong.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT 1 FROM smart_queue_reconcile_jobs "
        "WHERE queue_id = ? AND status IN (?,?) LIMIT 1",
        (queue_id, *_UNFINISHED),
    )
    return bool(rows)


#: How long a successfully finished job keeps being reported. Long enough that
#: a page open while it ran can notice and show what it did, short enough that
#: it is gone by the next visit. Failures are NOT time-limited — they stay until
#: dismissed, because nobody has acknowledged them.
_COMPLETED_JOB_WINDOW_MINUTES = 10


async def status_summary() -> dict:
    """What the app-wide banner shows: everything unfinished, plus failures.

    Failures are included because a job that died has to be visible from
    wherever the user happens to be — it changed the schedule partway.

    ``completed`` carries just-finished successes, whose ``detail`` is the only
    record of what they did. Accept is why it exists: its outcome used to live
    in an HTTP response, so a clean run that skipped four Twitter slots for
    being over the duration cap had to say so *somewhere*, and a done job leaves
    the other two buckets instantly.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT j.id, j.queue_id, j.kind, j.status, j.progress_done,
               j.progress_total, j.detail, j.last_error, q.name AS queue_name,
               p.slug AS project_slug
          FROM smart_queue_reconcile_jobs j
          LEFT JOIN smart_queues q ON q.id = j.queue_id
          LEFT JOIN projects p ON p.id = q.project_id
         WHERE j.status IN (?,?,?)
            OR (j.status = ? AND j.finished_at IS NOT NULL
                AND j.finished_at >= datetime('now', ?))
         ORDER BY j.id
        """,
        (*_UNFINISHED, STATUS_FAILED, STATUS_DONE,
         f"-{_COMPLETED_JOB_WINDOW_MINUTES} minutes"),
    )
    active, failed, completed = [], [], []
    for row in rows:
        entry = {
            "id": int(row["id"]),
            "queue_id": int(row["queue_id"]),
            # The banner is app-wide, so a job carries its OWN project slug —
            # the dismiss button used to guess it from the current URL and
            # silently no-op when the user wasn't on that project's page.
            "project_slug": row["project_slug"],
            "queue_name": row["queue_name"] or f"queue {row['queue_id']}",
            "kind": row["kind"],
            "label": KIND_LABELS.get(row["kind"], row["kind"]),
            "status": row["status"],
            "done": int(row["progress_done"] or 0),
            "total": int(row["progress_total"] or 0),
            "detail": row["detail"],
            "error": row["last_error"],
        }
        if row["status"] == STATUS_FAILED:
            failed.append(entry)
        elif row["status"] == STATUS_DONE:
            completed.append(entry)
        else:
            active.append(entry)
    return {
        "active": active,
        "failed": failed,
        "completed": completed,
        "busy": bool(active),
        # Only unfinished work locks a queue. A finished job in `completed` has
        # released it, so folding that list in here would 409 every mutation for
        # ten minutes after a successful run.
        "locked_queue_ids": sorted({e["queue_id"] for e in active}),
    }


async def dismiss_failed(job_id: int, queue_id: int) -> bool:
    """Acknowledge a failed job so it stops occupying the banner.

    Scoped to ``queue_id``: the caller has verified that queue belongs to the
    project, so scoping the UPDATE here stops a job id from one queue being
    dismissed through another queue's endpoint. Returns True when a row was
    actually dismissed, so the route can 404 an id that isn't this queue's.
    """
    async with write_transaction() as db:
        cursor = await db.execute(
            "UPDATE smart_queue_reconcile_jobs SET status = ? "
            "WHERE id = ? AND queue_id = ? AND status = ?",
            (STATUS_DONE, job_id, queue_id, STATUS_FAILED),
        )
    return bool(cursor.rowcount)


async def _claim_next_job() -> dict | None:
    """Take the oldest pending job. Single worker, so no contention to lose."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM smart_queue_reconcile_jobs WHERE status = ? ORDER BY id LIMIT 1",
        (STATUS_PENDING,),
    )
    if not rows:
        return None
    job = dict(rows[0])
    async with write_transaction() as write_db:
        await write_db.execute(
            "UPDATE smart_queue_reconcile_jobs SET status = ?, "
            "started_at = datetime('now') WHERE id = ?",
            (STATUS_RUNNING, int(job["id"])),
        )
    return job


async def _set_progress(job_id: int, done: int, total: int) -> None:
    async with write_transaction() as db:
        await db.execute(
            "UPDATE smart_queue_reconcile_jobs SET progress_done = ?, progress_total = ? "
            "WHERE id = ?",
            (done, total, job_id),
        )


async def _finish(job_id: int, *, status: str, detail: str = "",
                  error: str = "") -> None:
    async with write_transaction() as db:
        await db.execute(
            "UPDATE smart_queue_reconcile_jobs SET status = ?, detail = ?, "
            "last_error = ?, finished_at = datetime('now') WHERE id = ?",
            (status, detail or None, error or None, job_id),
        )


async def run_job(job: dict) -> str:
    """Execute one job, returning a human-readable summary of what changed.

    Runs under the queue's Accept lock. The HTTP 409 guard only blocks
    *inbound* mutations while jobs exist; nothing stopped this worker from
    executing a slot sweep in the middle of an Accept that legitimately runs
    for minutes, at which point Accept commits posts carrying a slot_id the
    sweep just deleted. Both sides now take the same lock.
    """
    from yt_scheduler.services import smart_queue_reconcile_handlers as handlers
    from yt_scheduler.services.smart_queue_accept import accept_lock

    payload = json.loads(job["payload"] or "{}")
    queue_id = int(job["queue_id"])
    job_id = int(job["id"])

    async with accept_lock(queue_id):
        return await _run_job_locked(job, payload, queue_id, job_id, handlers)


async def _run_job_locked(job, payload, queue_id, job_id, handlers) -> str:

    async def progress(done: int, total: int) -> None:
        # Bookkeeping must never be the thing that kills the work it describes —
        # the same rule ai.create_message follows for its request/response log.
        # Accept made it matter: its progress call sits in a `finally` inside
        # the per-video loop, so a failed status write would have propagated out
        # of the loop and abandoned a batch that was scheduling correctly. A
        # stalled counter is a far smaller harm than a lost batch, and a genuine
        # database problem will surface on the real writes a moment later.
        try:
            await _set_progress(job_id, done, total)
        except Exception:
            logger.exception(
                "reconcile: could not record progress %s/%s for job %s; the job "
                "continues", done, total, job_id,
            )

    kind = job["kind"]
    if kind == KIND_ACCEPT:
        return await handlers.accept_videos(queue_id, payload["video_ids"], progress)
    if kind == KIND_SLOTS_ADDED:
        return await handlers.add_slots(queue_id, payload["slot_ids"], progress)
    if kind == KIND_SLOTS_REMOVED:
        return await handlers.remove_slots(queue_id, payload["slot_ids"], progress)
    if kind == KIND_SLOT_BODY_CHANGED:
        return await handlers.rerender_slots(queue_id, payload["slot_ids"], progress)
    if kind == KIND_APPLIES_TO_REMOVED:
        return await handlers.drop_excluded_videos(queue_id, progress)
    raise ValueError(f"Unknown reconcile job kind: {kind!r}")


async def _worker_loop() -> None:
    """Drain the job table forever, one job at a time."""
    while True:
        job = await _claim_next_job()
        if job is None:
            _wake.clear()
            await _wake.wait()
            continue
        job_id = int(job["id"])
        try:
            detail = await run_job(job)
            await _finish(job_id, status=STATUS_DONE, detail=detail)
            logger.info("reconcile: job %s (%s) done — %s", job_id, job["kind"], detail)
        except asyncio.CancelledError:
            # Shutdown mid-job: hand it back so the next start retries rather
            # than leaving it 'running' forever and locking the queue.
            await _finish(job_id, status=STATUS_PENDING)
            raise
        except Exception as exc:
            logger.exception("reconcile: job %s (%s) failed", job_id, job["kind"])
            message = (
                str(exc) if isinstance(exc, ReportedJobFailure)
                else f"{type(exc).__name__}: {exc}"
            )
            try:
                await _finish(job_id, status=STATUS_FAILED, error=message)
            except Exception:
                # If recording the failure ALSO fails, the loop must not die:
                # the worker is process-wide, and its death would leave the job
                # 'running' forever and 409 every mutation on that queue until
                # a restart. Losing one status write is the smaller harm.
                logger.exception(
                    "reconcile: could not record failure of job %s; worker "
                    "continues", job_id,
                )


async def start_worker() -> None:
    """Start the single worker and re-queue anything a previous run left running."""
    global _worker_task
    async with write_transaction() as db:
        await db.execute(
            "UPDATE smart_queue_reconcile_jobs SET status = ? WHERE status = ?",
            (STATUS_PENDING, STATUS_RUNNING),
        )
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(_worker_loop(), name="smart-queue-reconcile")
    _wake.set()


async def stop_worker() -> None:
    global _worker_task
    if _worker_task is not None and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except (asyncio.CancelledError, Exception):
            pass
    _worker_task = None
