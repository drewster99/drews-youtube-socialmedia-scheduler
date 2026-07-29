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
