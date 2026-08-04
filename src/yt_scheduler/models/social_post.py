"""Social post model — terminal-state transitions for ``social_posts`` rows."""

from __future__ import annotations

from yt_scheduler.database import write_transaction


async def mark_posted(post_id: int, *, post_url: str) -> None:
    """Record that this post reached the platform.

    Clearing ``error`` is not cosmetic. A row left carrying the previous
    attempt's failure text renders that text next to a delivered post, and the
    message this app writes on an ambiguous send ends with "use Send to retry" —
    which is exactly what a user acts on. A Threads post that had timed out at
    the publish step (published anyway, response lost) was re-sent that way and
    went out twice.

    ``scheduled_at`` / ``scheduler_job_id`` clear because a manual Send
    supersedes any pending APScheduler job: a future job must not fire against a
    post that is already delivered, and the UI must not show "still scheduled"
    beside a ``posted_at``.
    """
    async with write_transaction() as db:
        await db.execute(
            """UPDATE social_posts
            SET status = 'posted', posted_at = datetime('now'), post_url = ?,
                error = NULL, scheduler_job_id = NULL, scheduled_at = NULL
            WHERE id = ?""",
            (post_url, post_id),
        )


async def mark_failed(
    post_id: int,
    *,
    error: str,
    retryable: bool = False,
    next_retry_at: str | None = None,
    retry_until: str | None = None,
) -> None:
    """Record that this post's send attempt failed, and why.

    The mirror of :func:`mark_posted`, and the ONLY writer of the ``'failed'``
    state. This statement previously existed as ten byte-identical copies
    across ``services/scheduler`` and ``routers/social_routes``; when
    ``failed_at`` was added, every one of them would have had to be found and
    edited, and the eleventh send path would have shipped without it. A
    regression test greps for the raw UPDATE so a new copy can't reappear.

    ``scheduled_at`` / ``scheduler_job_id`` clear for the same reason they do
    on success, plus one specific to failure: a row left holding its scheduling
    columns is resurrected and re-sent by the restore pass on the next restart,
    which is exactly what a terminal failure must not do.

    ``retryable`` / ``next_retry_at`` / ``retry_until`` describe whether the
    automatic retry path may pick this row up, and are passed in rather than
    decided here: only the caller holds the exception, and the decision is made
    from its TYPE. Inferring it later from ``error`` text would be guessing at a
    string we wrote for a human.
    """
    async with write_transaction() as db:
        await db.execute(
            """UPDATE social_posts
            SET status = 'failed', error = ?, failed_at = datetime('now'),
                scheduler_job_id = NULL, scheduled_at = NULL,
                retryable = ?, next_retry_at = ?,
                -- Computed once, on the first failure of a run: recomputing it
                -- on every attempt would walk the deadline forward forever and
                -- the retry would never stop.
                retry_until = COALESCE(retry_until, ?),
                retry_count = retry_count + 1
            WHERE id = ?""",
            (error, 1 if retryable else 0, next_retry_at, retry_until, post_id),
        )
