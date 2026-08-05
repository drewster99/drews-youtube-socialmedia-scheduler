"""Read each YouTube-backed video's privacy back from YouTube.

``videos.privacy_status`` was write-only with respect to YouTube. Every path
that set it was one this app initiated — upload, the publish timer, the metadata
dropdown, import — and nothing ever read it back. Publishing a video directly in
YouTube Studio was therefore invisible here, and two separate guards then acted
on a belief that was months out of date:

* :func:`smart_queue.is_eligible` decides a YouTube-backed video is live from
  this column, so auto-add never considered such a video and it never entered a
  queue.
* the send gate refuses to post a link to a non-public video from this same
  column, so its social posts were marked failed with "YouTube video is still
  'unlisted'" — about a video that was public.

The sweep checks **every** YouTube-backed video, not only the non-public ones,
because the reverse error is worse. A video pulled back to unlisted on YouTube
leaves a stale ``'public'`` here, and the send gate — the one thing standing
between the scheduler and announcing a link nobody can open — reads exactly this
column and waves the post through.

Per project, never across them: credentials are per project, and
``get_youtube_service`` deliberately refuses to guess which. Reading one
project's videos under another's OAuth grant is the wrong-channel mistake that
function's error message exists to prevent.

Cost is 1 quota unit per 50 videos. A ~350-video install is 7 units a sweep,
under 350 a day at the default interval, against a 10,000-unit budget.
"""

from __future__ import annotations

import asyncio
import json
import logging

from datetime import datetime, timezone

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.models.video import is_youtube_backed
from yt_scheduler.services import events, youtube as youtube_service
from yt_scheduler.services.auth import scoped_active_project

logger = logging.getLogger(__name__)

#: Videos read per project per sweep. A cap so one run cannot spend an unbounded
#: slice of the quota, paired with least-recently-verified-first ordering so a
#: library larger than the cap rotates through instead of starving its tail.
MAX_VIDEOS_PER_SWEEP = 200

#: YouTube's own per-request id limit, and so the unit of quota accounting.
IDS_PER_REQUEST = 50


#: Consecutive failed sweeps before the UI says anything. One failure is
#: routine — a sleeping laptop, a token mid-refresh, a flaky minute — and a
#: banner for each of those is a banner nobody reads. Two in a row across a
#: 90-minute interval is three hours of not knowing whether a video is public,
#: which is long enough to matter and short enough to still be actionable.
MIN_FAILURES_BEFORE_SURFACING = 2


def _utc_now() -> str:
    """SQLite's own shape, because these stamps are only ever compared with
    each other and with ``datetime('now')`` — see the timestamp-shape rule."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


async def _record_sweep_run(
    project_id: int, *, started_at: str, error: str | None, detail: dict | None,
) -> None:
    """Persist the outcome of one sweep, overwriting this project's last.

    The sweep's only caller is a background job, so its failures happen with no
    page open. A log line is not a surface: nothing would say the check had
    stopped, while the two guards that read ``privacy_status`` went on trusting
    a value nothing had verified for days.

    Never raises. This records what happened and must not become a new way for
    the sweep to fail — least of all on the path already reporting one.
    """
    detail_json = json.dumps(detail) if detail is not None else None
    try:
        async with write_transaction() as db:
            await db.execute(
                """
                INSERT INTO video_privacy_sweep_runs
                    (project_id, started_at, finished_at, ok, error,
                     consecutive_failures, last_success_at, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    ok = excluded.ok,
                    error = excluded.error,
                    -- Reset by success, incremented by failure. Never cleared
                    -- BY a failure, so a check that flaps still accumulates
                    -- instead of resetting itself to invisibility each time it
                    -- briefly recovers.
                    consecutive_failures = CASE
                        WHEN excluded.ok = 1 THEN 0
                        ELSE video_privacy_sweep_runs.consecutive_failures + 1
                    END,
                    -- Held through a failure on purpose: paired with
                    -- finished_at it is what lets the UI say how LONG this has
                    -- been broken, rather than only that it is.
                    last_success_at = CASE
                        WHEN excluded.ok = 1 THEN excluded.finished_at
                        ELSE video_privacy_sweep_runs.last_success_at
                    END,
                    detail = excluded.detail
                """,
                (project_id, started_at, _utc_now(), 0 if error else 1, error,
                 0 if error is None else 1,
                 _utc_now() if error is None else None, detail_json),
            )
    except Exception:
        logger.exception(
            "Could not record the privacy sweep outcome for project %s",
            project_id,
        )


async def last_sweep_runs() -> list[dict]:
    """Every project whose privacy sweep has been failing long enough to say so.

    Returns only what the UI should act on. A single failure is deliberately
    invisible; see MIN_FAILURES_BEFORE_SURFACING.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT r.project_id, p.slug AS project_slug, p.name AS project_name,
               r.started_at, r.finished_at, r.error,
               r.consecutive_failures, r.last_success_at
          FROM video_privacy_sweep_runs r
          JOIN projects p ON p.id = r.project_id
         WHERE r.ok = 0 AND r.consecutive_failures >= ?
         ORDER BY r.consecutive_failures DESC, p.name
        """,
        (MIN_FAILURES_BEFORE_SURFACING,),
    )
    return [dict(row) for row in rows]


async def _videos_due_for_check(project_id: int, limit: int) -> list[dict]:
    """YouTube-backed, non-archived videos, least-recently-verified first.

    NULL sorts first, so anything never checked is read before anything
    re-checked — a video we have never verified is the one most likely to be
    wrong. Ties break on id purely so the order is total and a capped sweep
    cannot revisit one subset while never reaching another.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT id, youtube_video_id, project_id, title, privacy_status,
               privacy_synced_at
          FROM videos
         WHERE project_id = ?
           AND youtube_video_id IS NOT NULL
           AND COALESCE(archived, 0) = 0
         ORDER BY privacy_synced_at IS NOT NULL, privacy_synced_at, id
         LIMIT ?
        """,
        (project_id, limit),
    )
    return [dict(row) for row in rows]


async def _record_observations(pairs: list[tuple[dict, str]]) -> None:
    """Write what YouTube said, for every video read, in ONE transaction.

    The stamp goes on whether or not the value moved: it records that the read
    happened, which is a different fact from the value it returned.

    One transaction rather than one per video, for both reasons that usually
    argue for it — a sweep is up to ``MAX_VIDEOS_PER_SWEEP`` rows and a commit
    apiece is a needless fsync apiece, and a failure partway through leaves
    NOTHING stamped, which is the honest state. A half-stamped sweep would
    report some of the library as freshly verified on the strength of a read
    that did not finish.
    """
    if not pairs:
        return
    async with write_transaction() as db:
        await db.executemany(
            "UPDATE videos SET privacy_status = ?, "
            "privacy_synced_at = datetime('now') WHERE id = ?",
            [(privacy, video["id"]) for video, privacy in pairs],
        )


async def _consider_for_auto_add(video: dict) -> bool:
    """Run the live-transition funnel for a video YouTube now reports public.

    Returns whether any queue took it. Failure here is logged and swallowed on
    purpose: the privacy correction is already committed and is the more
    important half — losing it would leave the send gate still refusing posts
    for a video that is public.
    """
    from yt_scheduler.services.smart_queue_live import on_video_became_live

    try:
        result = await on_video_became_live(video["id"])
    except Exception:
        logger.exception(
            "Auto-add failed for video %s after it went public on YouTube; "
            "the privacy correction stands",
            video["id"],
        )
        return False
    return bool(result.get("added_to"))


async def sync_project_video_privacy(
    project: dict, *, limit: int = MAX_VIDEOS_PER_SWEEP
) -> dict:
    """Read privacy for one project's videos and reconcile what changed.

    Raises whatever the YouTube client raises, having recorded the failure
    first. A sweep that cannot reach YouTube must fail loudly and leave every
    ``privacy_synced_at`` untouched — writing a confirmation it did not earn is
    the misleading-fine state this module exists to remove.
    """
    started_at = _utc_now()
    try:
        summary = await _sync_project_video_privacy(project, limit=limit)
    except Exception as exc:
        # Recorded on the way past, not instead of raising: the caller still
        # needs to know, and so does anyone looking at a page tomorrow.
        await _record_sweep_run(
            int(project["id"]), started_at=started_at,
            error=f"{type(exc).__name__}: {exc}", detail=None,
        )
        raise
    await _record_sweep_run(
        int(project["id"]), started_at=started_at, error=None,
        detail={
            "checked": summary["checked"],
            "changed": len(summary["changed"]),
            "missing": len(summary["missing"]),
            "quota_units": summary["quota_units"],
        },
    )
    return summary


async def _sync_project_video_privacy(
    project: dict, *, limit: int = MAX_VIDEOS_PER_SWEEP
) -> dict:
    """The sweep itself. Wrapped by the recorder above."""
    summary = {
        "project_slug": project["slug"], "checked": 0, "missing": [],
        "changed": [], "became_live": [], "quota_units": 0,
    }
    candidates = await _videos_due_for_check(int(project["id"]), limit)
    if not candidates:
        return summary

    # A LIST per id, not one video. Two local rows may name the same YouTube
    # video (a re-import alongside the original), and a dict would silently keep
    # the last — the dropped row would then never be stamped, sort first forever
    # on privacy_synced_at IS NULL, and be dropped again every single sweep. One
    # answer from YouTube applies to every row that asked about it.
    by_youtube_id: dict[str, list[dict]] = {}
    for video in candidates:
        if is_youtube_backed(video):
            by_youtube_id.setdefault(video["youtube_video_id"], []).append(video)
    youtube_ids = list(by_youtube_id)
    # SCOPED, not a bare set_active_project: the ContextVar must be restored on
    # the way out, including on the exception path. A bare set leaves the root
    # asyncio context — the copy-on-create template for every Task spawned
    # afterwards — holding this project's slug, so an unrelated job that forgot
    # to bind would silently inherit it and read the wrong channel's videos.
    # comments.sync_project_comments does the same for the same reason.
    # asyncio.to_thread copies the current context, so the worker sees it.
    with scoped_active_project(project["slug"]):
        observed = await asyncio.to_thread(
            youtube_service.get_videos_privacy_status, youtube_ids
        )

    # Absence is not a value. A deleted video, one we lost read access to, and a
    # truncated response are indistinguishable here, and none of them says
    # anything about privacy — so these rows keep both their stored status and
    # their old stamp, and are reported instead of being acted on.
    summary["missing"] = [y for y in youtube_ids if y not in observed]
    summary["quota_units"] = (
        len(youtube_ids) + IDS_PER_REQUEST - 1
    ) // IDS_PER_REQUEST

    # Decide first, write once, then act on what actually moved. Splitting it
    # this way is what lets the whole sweep's stamps be one atomic write.
    observations: list[tuple[dict, str]] = []
    moved: list[tuple[dict, str, str]] = []
    for youtube_id, privacy in observed.items():
        for video in by_youtube_id[youtube_id]:
            observations.append((video, privacy))
            was = video["privacy_status"] or ""
            if was != privacy:
                moved.append((video, was, privacy))

    await _record_observations(observations)
    summary["checked"] = len(observations)

    for video, was, privacy in moved:
        summary["changed"].append({
            "video_id": video["id"], "title": video["title"],
            "from": was, "to": privacy,
        })
        await events.record_event(
            video["id"], "privacy_changed_on_youtube",
            {"old": was, "new": privacy},
        )
        logger.info(
            "Privacy changed outside this app: video %s (%r) %s -> %s",
            video["id"], video["title"], was, privacy,
        )

        # The funnel every other liveness path goes through, rather than a
        # second auto-add implementation that could disagree with it. Idempotent
        # on auto_add_considered_at, so public -> unlisted -> public adds once.
        if privacy == "public" and await _consider_for_auto_add(video):
            summary["became_live"].append(video["id"])

    if summary["missing"]:
        logger.warning(
            "Privacy sweep (project %s): YouTube did not return %d of %d "
            "videos (deleted, or no longer readable): %s",
            project["slug"], len(summary["missing"]), len(youtube_ids),
            ", ".join(sorted(summary["missing"])),
        )
    return summary


async def sync_all_projects_video_privacy() -> list[dict]:
    """Sweep every project that has a channel bound.

    One project's failure never stops the others: each is a separate channel
    with its own OAuth grant, and a revoked grant on one must not silence the
    rest. Same shape as ``comments.sync_all_projects``, for the same reason.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, slug, youtube_channel_id FROM projects "
        "WHERE youtube_channel_id IS NOT NULL ORDER BY id"
    )

    summaries: list[dict] = []
    for row in rows:
        try:
            summaries.append(await sync_project_video_privacy(dict(row)))
        except Exception as exc:
            logger.warning(
                "Privacy sweep failed for project %s: %s", row["slug"], exc
            )
            summaries.append({
                "project_slug": row["slug"],
                "error": f"{type(exc).__name__}: {exc}",
            })
    return summaries
