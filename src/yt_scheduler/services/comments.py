"""Channel-wide YouTube comment mirror.

A project is bound 1:1 to a YouTube channel, so one
``commentThreads.list(allThreadsRelatedToChannelId=...)`` page returns comments
from across the whole project for 1 quota unit. A periodic sweep upserts them
into ``youtube_comments`` and the dashboard reads that table — the page never
waits on YouTube, and a channel-wide "what has anyone said lately?" costs one
SQL query instead of one API call per video.

Distinct from :mod:`yt_scheduler.services.moderation`, which fetches comments
per video to act on blocklist matches and records only those matches. This
module records the conversation; that one records enforcement.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from yt_scheduler.config import (
    COMMENT_REPLY_REFRESH_HOURS,
    COMMENT_SYNC_MAX_PAGES,
    COMMENT_SYNC_MAX_PAGES_PER_MODERATION_BUCKET,
    COMMENT_SYNC_MAX_REPLY_FETCHES,
    COMMENT_SYNC_MAX_REPLY_PAGES,
)
from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import youtube
from yt_scheduler.services.auth import scoped_active_project

logger = logging.getLogger(__name__)


#: Upper bound on one page of the dashboard list, counted in THREADS — the unit
#: the list is paged by, because a page boundary that fell inside a thread would
#: split a conversation across "Load more" and make the running total meaningless.
#: The point is to refuse a request that would read the whole table, not to shape
#: the UI.
MAX_THREADS_PER_PAGE = 200


#: Comments the blocklist already rejected on YouTube are hidden from viewers
#: there, but ``commentThreads.list`` keeps returning them (see the claim logic
#: in :mod:`~yt_scheduler.services.moderation`) — so without this the dashboard
#: would hand the user back precisely the spam moderation exists to spare them.
#:
#: Only ``action = 'deleted'`` is excluded. ``'error'`` means the rejection
#: FAILED, so that comment is still live on YouTube and hiding it would be the
#: silent wrong answer; ``'pending'`` is an in-flight claim, not an outcome.
#:
#: One constant, used by both the listing and the count: if the two predicates
#: ever disagreed, "Showing 10 of N" would count rows the list can't show and
#: Load more would stall short of the end.
_NOT_MODERATED_AWAY = """
    NOT EXISTS (
        SELECT 1 FROM moderation_log m
        WHERE m.project_id = c.project_id
          AND m.comment_id = c.comment_id
          AND m.action = 'deleted'
    )
"""


class SweepAlreadyRunning(RuntimeError):
    """A sweep is already in flight for this project.

    Two concurrent sweeps are not merely wasteful — they corrupt the
    "gone from YouTube" inference. Each stamps its own ``swept_at``, and the
    sweep that STARTED first can easily FINISH last (it may do up to
    ``COMMENT_SYNC_MAX_REPLY_FETCHES`` sequential reply round trips while a
    second sweep does none). The later-finishing sweep then writes the newer
    stamp over the older comment set, and every comment that arrived in between
    reads as removed from YouTube until the next complete sweep.

    Refused rather than queued: the caller is either a background job whose next
    tick is minutes away, or a user who pressed Refresh twice.
    """


class ChannelNotBound(RuntimeError):
    """The project has no YouTube channel, so there is nothing to sweep.

    Raised rather than returning an empty sweep: "no comments" and "this
    project was never connected to YouTube" are different answers, and only one
    of them is worth showing the user.
    """


@dataclass(frozen=True)
class CommentRecord:
    """One comment, flattened out of a thread resource, ready to store."""

    comment_id: str
    youtube_video_id: str | None
    parent_comment_id: str | None
    author_display_name: str
    author_channel_id: str | None
    author_profile_image_url: str | None
    text_display: str
    like_count: int
    total_reply_count: int | None
    published_at: str
    youtube_updated_at: str
    #: YouTube's own moderation state. None means the API did not say and the
    #: call was not bucket-filtered — unknown, never assumed ``published``.
    moderation_status: str | None
    #: ``viewerRating`` — the rating given by whoever authorized the request,
    #: i.e. the channel owner. ``'like'`` is our thumbs-up; None is unknown.
    viewer_rating: str | None


def _record_from_comment(
    comment: dict,
    *,
    video_id: str | None,
    parent_comment_id: str | None,
    total_reply_count: int | None,
    bucket_status: str | None,
) -> CommentRecord:
    """Build a :class:`CommentRecord` from one YouTube comment resource.

    ``video_id`` is passed in rather than read from the comment: a reply
    fetched via ``comments.list`` carries no ``videoId``, and it is on the same
    video as the parent thread by definition.

    ``moderation_status`` prefers the resource's own field, which YouTube
    populates only for a channel-owner-authorized request, and otherwise takes
    ``bucket_status`` — the moderation bucket this call explicitly asked for.
    Both are statements by YouTube about this comment (one per resource, one per
    query), so neither is a substituted default; ``None`` when there is neither,
    which is the case for a reply read through the un-filtered
    ``comments.list``.
    """
    snippet = comment["snippet"]
    author_channel = snippet.get("authorChannelId") or {}
    return CommentRecord(
        comment_id=comment["id"],
        youtube_video_id=video_id,
        parent_comment_id=parent_comment_id,
        author_display_name=snippet.get("authorDisplayName", ""),
        author_channel_id=author_channel.get("value"),
        author_profile_image_url=snippet.get("authorProfileImageUrl"),
        text_display=snippet.get("textDisplay", ""),
        like_count=int(snippet.get("likeCount") or 0),
        total_reply_count=total_reply_count,
        published_at=snippet["publishedAt"],
        youtube_updated_at=snippet.get("updatedAt") or snippet["publishedAt"],
        moderation_status=snippet.get("moderationStatus") or bucket_status,
        viewer_rating=snippet.get("viewerRating"),
    )


def flatten_threads(
    threads: list[dict], *, bucket_status: str | None = None
) -> list[CommentRecord]:
    """Flatten thread resources into one record per comment.

    Top-level comments and the replies carried in each thread's preview both
    become rows — "all comments", as a flat list, is what the dashboard shows
    and what a reply to a months-old video needs in order to surface at all.

    ``bucket_status`` is the ``moderationStatus`` the sweep asked this page for;
    it stamps every comment the page returned.
    """
    records: list[CommentRecord] = []
    for thread in threads:
        snippet = thread["snippet"]
        # Absent for a comment posted on the channel rather than on a video.
        video_id = snippet.get("videoId")
        top = snippet["topLevelComment"]
        records.append(
            _record_from_comment(
                top,
                video_id=video_id,
                parent_comment_id=None,
                total_reply_count=int(snippet.get("totalReplyCount") or 0),
                bucket_status=bucket_status,
            )
        )
        for reply in (thread.get("replies") or {}).get("comments", []):
            records.append(
                _record_from_comment(
                    reply,
                    video_id=video_id,
                    parent_comment_id=top["id"],
                    total_reply_count=None,
                    bucket_status=bucket_status,
                )
            )
    return records


async def _store(project_id: int, records: list[CommentRecord]) -> tuple[int, int]:
    """Upsert records, returning ``(new, updated)``.

    ``first_seen_at`` is deliberately left alone on conflict — it records when
    WE first saw the comment, which a re-sweep must not move.

    New vs updated is decided by reading the existing ids up front rather than
    inferring it from the write: an upsert reports one affected row either way,
    and comparing timestamps to guess would be wrong for two writes inside the
    same second.
    """
    if not records:
        return 0, 0

    # The batch spans three moderation buckets read minutes apart, so one
    # comment can legitimately arrive twice — it changed bucket mid-sweep, or a
    # page boundary shifted under a new arrival. The WRITE was always safe
    # (upsert, last wins); the COUNTS were not, and they are what the summary,
    # the log line and comment_sweep_runs.detail all report. Last occurrence
    # wins: the buckets are read in sequence, so the later read is the more
    # current statement about the comment.
    records = list({r.comment_id: r for r in records}.values())

    db = await get_db()
    ids = [r.comment_id for r in records]
    existing: set[str] = set()
    # Chunked to stay well under SQLite's variable limit on a large sweep.
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        placeholders = ",".join("?" * len(chunk))
        rows = await db.execute_fetchall(
            f"SELECT comment_id FROM youtube_comments "
            f"WHERE project_id = ? AND comment_id IN ({placeholders})",
            (project_id, *chunk),
        )
        existing.update(r["comment_id"] for r in rows)

    new_count = sum(1 for r in records if r.comment_id not in existing)

    # One transaction per batch rather than one for the whole sweep: a sweep can
    # be thousands of rows, and every other writer in the process is blocked for
    # as long as the write lock is held.
    for start in range(0, len(records), 200):
        batch = records[start:start + 200]
        async with write_transaction() as wdb:
            for record in batch:
                await wdb.execute(
                    """
                    INSERT INTO youtube_comments (
                        project_id, comment_id, youtube_video_id, parent_comment_id,
                        author_display_name, author_channel_id,
                        author_profile_image_url, text_display, like_count,
                        total_reply_count, published_at, youtube_updated_at,
                        moderation_status, viewer_rating,
                        first_seen_at, last_synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              datetime('now'), datetime('now'))
                    ON CONFLICT(project_id, comment_id) DO UPDATE SET
                        youtube_video_id = excluded.youtube_video_id,
                        parent_comment_id = excluded.parent_comment_id,
                        author_display_name = excluded.author_display_name,
                        author_channel_id = excluded.author_channel_id,
                        author_profile_image_url = excluded.author_profile_image_url,
                        text_display = excluded.text_display,
                        like_count = excluded.like_count,
                        total_reply_count = excluded.total_reply_count,
                        published_at = excluded.published_at,
                        youtube_updated_at = excluded.youtube_updated_at,
                        -- A reply read through the un-filtered comments.list
                        -- carries no status at all; letting that NULL overwrite
                        -- a known one would erase the fact on every sweep.
                        moderation_status = COALESCE(
                            excluded.moderation_status, moderation_status
                        ),
                        -- Same reasoning: a call that reported no rating must
                        -- not erase one a previous call did report.
                        viewer_rating = COALESCE(
                            excluded.viewer_rating, viewer_rating
                        ),
                        last_synced_at = datetime('now')
                    """,
                    (
                        project_id,
                        record.comment_id,
                        record.youtube_video_id,
                        record.parent_comment_id,
                        record.author_display_name,
                        record.author_channel_id,
                        record.author_profile_image_url,
                        record.text_display,
                        record.like_count,
                        record.total_reply_count,
                        record.published_at,
                        record.youtube_updated_at,
                        record.moderation_status,
                        record.viewer_rating,
                    ),
                )

    return new_count, len(records) - new_count


async def _mark_seen_in_complete_sweep(
    project_id: int, comment_ids: list[str]
) -> tuple[str | None, str | None]:
    """Stamp one shared timestamp on the TOP-LEVEL comments a COMPLETE sweep saw.

    Returns ``(swept_at, previous_swept_at)`` — this sweep's stamp and the one
    before it, which together are the two-strike window the read path needs.

    Only ever called when the sweep read every bucket in full and left no thread
    owing a reply fetch. That precondition is the whole point: a comment absent
    from a *partial* sweep may simply be in the part we did not read, and
    stamping then would make the next read mistake it for one YouTube no longer
    returns.

    The caller passes top-level comments only — see the note at that call site
    for why a reply's absence from a sweep proves nothing.

    One timestamp for the batch, not ``datetime('now')`` per row — the read path
    compares each row against the newest stamp in the project, and rows written
    either side of a second boundary would split into two "sweeps".
    """
    if not comment_ids:
        return None, None

    # The read path compares each row against the project's MAX stamp, so the
    # stamp is really a sweep ORDINAL that happens to be spelled as a time. A
    # backwards clock step — NTP correction, a VM or Time Machine restore, a
    # manual change — would otherwise make the newer sweep write a SMALLER value
    # than the older one, inverting the comparison and condemning exactly the
    # comments just confirmed alive. Monotonicity is the property that matters;
    # wall-clock accuracy is not, and nothing displays this column.
    recorded, _ = await _recorded_sweep_stamps(project_id)
    on_rows = await newest_complete_sweep_at(project_id)
    # Both are real facts about the last stamp and should agree; take the later
    # so neither a lost run record nor a lost row write can let time go
    # backwards.
    floor = max([s for s in (recorded, on_rows) if s], default=None)

    swept_at = _utc_now()
    if floor is not None and swept_at <= floor:
        swept_at = _plus_one_second(floor)
        logger.warning(
            "Clock went backwards between comment sweeps for project %s "
            "(now=%s, last stamp=%s) — using %s to keep sweep ordering "
            "monotonic.",
            project_id, _utc_now(), floor, swept_at,
        )

    # ONE transaction for the whole stamp. A half-stamped project is worse than
    # an unstamped one: MAX moves to the new value while the rows the loop never
    # reached keep the old one, so every one of them reads as gone from YouTube.
    # The chunking below is only SQLite's bound-variable limit, never a
    # lock-hold budget — these are a handful of indexed UPDATEs.
    async with write_transaction() as wdb:
        for start in range(0, len(comment_ids), 500):
            chunk = comment_ids[start:start + 500]
            placeholders = ",".join("?" * len(chunk))
            await wdb.execute(
                f"UPDATE youtube_comments SET last_seen_in_sweep_at = ? "
                f"WHERE project_id = ? AND comment_id IN ({placeholders})",
                (swept_at, project_id, *chunk),
            )
    return swept_at, recorded


#: The most replies one thread's follow-up will ever hold. A thread already at
#: this many is not "short" — it is as complete as we are willing to make it, and
#: continuing to call it short is what made the old code re-request it forever.
_MAX_FETCHABLE_REPLIES = COMMENT_SYNC_MAX_REPLY_PAGES * 100


@dataclass(frozen=True)
class ReplyFetchCandidate:
    """A thread whose replies this sweep should read, and why."""

    top_comment_id: str
    youtube_video_id: str | None
    #: True when we hold fewer replies than YouTube reports; False when the
    #: count already matches and this is a staleness refresh.
    is_incomplete: bool


async def _threads_needing_reply_fetch(
    project_id: int, threads: list[dict]
) -> tuple[list[ReplyFetchCandidate], int]:
    """Threads whose replies are worth reading, best first, plus how many are
    capped.

    Two distinct reasons, deliberately not collapsed:

    * **Incomplete** — we hold fewer replies than ``totalReplyCount``. A thread
      resource carries only a preview, so a busy thread arrives short.
    * **Stale** — the count already matches, but the replies have not been
      re-read within ``COMMENT_REPLY_REFRESH_HOURS``. This is the ONLY way a
      reply's ``moderationStatus`` can ever be corrected: the held and
      likely-spam buckets list threads by their top-level comment, so a reply
      held after we stored it is never mentioned again by any other call.

    Incomplete threads sort ahead of stale ones — missing content beats stale
    metadata — and within each group the least recently refreshed comes first,
    so a limited budget rotates through the channel instead of starving the
    same threads every sweep.

    A thread already holding :data:`_MAX_FETCHABLE_REPLIES` is never reported
    incomplete: another fetch cannot return more, so calling it short is what
    made the old gating re-request it on every sweep forever. Those are counted
    separately and returned as the second element, because "we will not read
    more of this" is a fact the sweep has to state rather than hide.
    """
    if not threads:
        return [], 0

    db = await get_db()
    wanted: dict[str, tuple[str | None, int]] = {}
    for thread in threads:
        snippet = thread["snippet"]
        top_id = snippet["topLevelComment"]["id"]
        wanted[top_id] = (
            snippet.get("videoId"),
            int(snippet.get("totalReplyCount") or 0),
        )

    if not wanted:
        return [], 0

    held: dict[str, int] = {}
    refreshed: dict[str, str | None] = {}
    ids = list(wanted)
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        placeholders = ",".join("?" * len(chunk))
        rows = await db.execute_fetchall(
            f"SELECT parent_comment_id, COUNT(*) AS n FROM youtube_comments "
            f"WHERE project_id = ? AND parent_comment_id IN ({placeholders}) "
            f"GROUP BY parent_comment_id",
            (project_id, *chunk),
        )
        held.update({r["parent_comment_id"]: int(r["n"]) for r in rows})

        rows = await db.execute_fetchall(
            f"SELECT comment_id, replies_refreshed_at FROM youtube_comments "
            f"WHERE project_id = ? AND comment_id IN ({placeholders})",
            (project_id, *chunk),
        )
        refreshed.update({r["comment_id"]: r["replies_refreshed_at"] for r in rows})

    stale_before = _utc_minus_hours(COMMENT_REPLY_REFRESH_HOURS)
    candidates: list[tuple[bool, str, ReplyFetchCandidate]] = []
    at_cap = 0
    for top_id, (video_id, total) in wanted.items():
        stored = held.get(top_id, 0)
        if stored >= _MAX_FETCHABLE_REPLIES:
            at_cap += 1
            continue
        incomplete = stored < total
        # NULL sorts first as "": never refreshed is the most due, not the least.
        last_refreshed = refreshed.get(top_id) or ""
        # Nothing to read and nothing that could have gone stale.
        if not incomplete and total == 0:
            continue
        if not incomplete and last_refreshed > stale_before:
            continue
        candidates.append((
            incomplete, last_refreshed,
            ReplyFetchCandidate(top_id, video_id, incomplete),
        ))

    # Incomplete first (True > False), then least-recently-refreshed.
    candidates.sort(key=lambda c: (not c[0], c[1]))
    return [c[2] for c in candidates], at_cap


#: The bucket that IS the sweep. Its failure is the sweep's failure and
#: propagates; the others are supplementary and degrade to a reported error.
_PRIMARY_BUCKET = "published"

#: Bound in another module and nothing else ties them together. If the primary
#: ever fell out of the listable set, the supplementary loop below would sweep
#: it a second time — full quota cost, every record counted twice.
if _PRIMARY_BUCKET not in youtube.LISTABLE_MODERATION_STATUSES:
    raise RuntimeError(
        f"_PRIMARY_BUCKET {_PRIMARY_BUCKET!r} is not in "
        f"youtube.LISTABLE_MODERATION_STATUSES "
        f"{youtube.LISTABLE_MODERATION_STATUSES}"
    )

#: The moderation buckets a sweep must read before it may call itself complete.
#: Named explicitly so `all(...)` over the outcomes can never be vacuously True:
#: an empty outcome dict would otherwise claim completeness while no held or
#: likely-spam comment had been read at all — and then condemn every one of them.
_SUPPLEMENTARY_BUCKETS = tuple(
    s for s in youtube.LISTABLE_MODERATION_STATUSES if s != _PRIMARY_BUCKET
)

#: One sweep per project at a time — see :class:`SweepAlreadyRunning`. Keyed by
#: project so two different channels still sweep concurrently.
_sweep_locks: dict[int, asyncio.Lock] = {}


async def _sweep_extra_buckets(
    channel_id: str,
) -> tuple[list[CommentRecord], dict[str, dict]]:
    """Read the moderation buckets viewers cannot see.

    Returns the flattened records plus a per-bucket outcome the summary carries
    verbatim. A bucket that fails is recorded and skipped rather than aborting:
    these are additive information, and losing the whole comment sweep because
    YouTube declined one filter would trade the feature for the feed.

    That is also why the outcome distinguishes ``ok`` from an empty result. "No
    held comments" and "we could not ask" are different answers, and only one of
    them means the dashboard is telling the truth.
    """
    records: list[CommentRecord] = []
    outcomes: dict[str, dict] = {}

    for status in youtube.LISTABLE_MODERATION_STATUSES:
        if status == _PRIMARY_BUCKET:
            continue
        try:
            threads, hit_cap = await asyncio.to_thread(
                youtube.list_channel_comment_threads,
                channel_id,
                max_pages=COMMENT_SYNC_MAX_PAGES_PER_MODERATION_BUCKET,
                moderation_status=status,
            )
        except Exception as exc:
            outcomes[status] = {
                "ok": False,
                "threads": 0,
                "pages_truncated": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
            continue
        records.extend(flatten_threads(threads, bucket_status=status))
        outcomes[status] = {
            "ok": True,
            "threads": len(threads),
            "pages_truncated": hit_cap,
            "error": None,
        }
    return records, outcomes


async def _record_sweep_run(
    project_id: int,
    *,
    started_at: str,
    finished: bool,
    was_complete: bool,
    error: str | None,
    detail: dict | None,
    swept_at: str | None = None,
    previous_swept_at: str | None = None,
) -> None:
    """Persist the outcome of one sweep, overwriting the project's last.

    The sweep's normal home is a 4-hourly background job, so its failures happen
    with no page open — a toast or a log line is not a surface. Writing the
    outcome is what lets the dashboard say "the last sync had a problem" hours
    later, instead of rendering the stale mirror under a reassuring
    "Synced 4 hours ago".

    Never raises. This records what happened; it must not become a new way for
    the sweep to fail, and least of all on the path that is already reporting a
    failure.
    """
    # Serialized OUTSIDE the guard below. The swallow exists to stop a DB
    # failure from breaking the sweep, NOT to hide a summary that cannot be
    # encoded — that would silently stop all recording and leave the dashboard
    # showing the last run that did record, under a reassuring "Synced N hours
    # ago". The failure path always passes detail=None, so this can only raise
    # on the success path, where the sweep's own work is already committed.
    detail_json = json.dumps(detail) if detail is not None else None
    try:
        async with write_transaction() as db:
            await db.execute(
                """
                INSERT INTO comment_sweep_runs (
                    project_id, started_at, finished_at, ok, was_complete,
                    error, detail, swept_at, previous_swept_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    ok = excluded.ok,
                    was_complete = excluded.was_complete,
                    error = excluded.error,
                    detail = excluded.detail,
                    -- A sweep that stamped nothing (failed, or not complete)
                    -- must LEAVE the window alone. Clearing it would reset the
                    -- two-strike history and forget that a comment has already
                    -- been missed once.
                    swept_at = COALESCE(excluded.swept_at, swept_at),
                    previous_swept_at = COALESCE(
                        excluded.previous_swept_at, previous_swept_at
                    )
                """,
                (
                    project_id,
                    started_at,
                    _utc_now() if finished else None,
                    int(finished and error is None),
                    int(was_complete),
                    error,
                    detail_json,
                    swept_at,
                    previous_swept_at,
                ),
            )
    except Exception:
        logger.exception(
            "Could not record the comment sweep outcome for project %s", project_id
        )


_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def _utc_now() -> str:
    """SQLite's ``datetime('now')`` format, produced in Python.

    Matches what every other timestamp column holds, so the UI's one formatter
    (`static/js/datetime.js`) parses it identically. Fixed width, so
    lexicographic ordering is chronological ordering.
    """
    return datetime.now(timezone.utc).strftime(_TIMESTAMP_FORMAT)


def _plus_one_second(stamp: str) -> str:
    """The next representable sweep stamp after ``stamp``."""
    parsed = datetime.strptime(stamp, _TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    return (parsed + timedelta(seconds=1)).strftime(_TIMESTAMP_FORMAT)


def _utc_minus_hours(hours: int) -> str:
    """A stamp ``hours`` in the past, for staleness comparisons.

    Derived from :func:`_utc_now` rather than reading the clock again, so this
    module has exactly one notion of "now" — a second one would drift from the
    stamps it is being compared against.
    """
    parsed = datetime.strptime(_utc_now(), _TIMESTAMP_FORMAT).replace(
        tzinfo=timezone.utc
    )
    return (parsed - timedelta(hours=hours)).strftime(_TIMESTAMP_FORMAT)


async def sync_project_comments(project: dict) -> dict:
    """Mirror one project's channel comments into the local table, recording the
    outcome either way.

    Returns a summary the UI renders directly. ``pages_truncated`` and
    ``threads_with_unfetched_replies`` are part of that summary on purpose: a
    sweep that stopped at a budget must say so, or a partial mirror reads as a
    complete one.

    Raises :class:`ChannelNotBound` when the project has no channel, and lets
    YouTube errors propagate so the caller can surface the real failure — but
    persists the failure first, because the caller here is usually a background
    job with nobody watching.
    """
    project_id = int(project["id"])
    lock = _sweep_locks.setdefault(project_id, asyncio.Lock())
    # Checked rather than awaited. Acquiring an uncontended asyncio.Lock does
    # not yield, so nothing can slip between the check and the acquire.
    if lock.locked():
        raise SweepAlreadyRunning(
            f"A comment sweep is already running for project "
            f"'{project['slug']}'. Wait for it to finish rather than starting a "
            f"second one — concurrent sweeps corrupt the 'gone from YouTube' "
            f"watermark."
        )

    async with lock:
        started_at = _utc_now()
        try:
            summary = await _sweep_project_comments(project)
        except Exception as exc:
            await _record_sweep_run(
                project_id,
                started_at=started_at,
                finished=False,
                was_complete=False,
                error=f"{type(exc).__name__}: {exc}",
                detail=None,
            )
            raise

        await _record_sweep_run(
            project_id,
            started_at=started_at,
            finished=True,
            was_complete=summary["sweep_was_complete"],
            error=None,
            detail=summary,
            swept_at=summary["swept_at"],
            previous_swept_at=summary["previous_swept_at"],
        )
        return summary


async def _sweep_project_comments(project: dict) -> dict:
    """The sweep itself. Wrapped by :func:`sync_project_comments`, which is the
    only caller and exists to guarantee the outcome is recorded."""
    channel_id = project.get("youtube_channel_id")
    if not channel_id:
        raise ChannelNotBound(
            f"Project '{project['slug']}' has no YouTube channel connected, "
            f"so there are no comments to fetch."
        )

    project_id = int(project["id"])

    with scoped_active_project(project["slug"]):
        # The Google client is synchronous; a channel sweep is many sequential
        # round trips and would stall the loop for all of them.
        threads, hit_page_cap = await asyncio.to_thread(
            youtube.list_channel_comment_threads,
            channel_id,
            max_pages=COMMENT_SYNC_MAX_PAGES,
            moderation_status=_PRIMARY_BUCKET,
        )
        thread_records = flatten_threads(threads, bucket_status=_PRIMARY_BUCKET)

        # The buckets viewers cannot see. Read separately because YouTube's
        # default filter is `published` — these are NOT a subset of the sweep
        # above, and without them a held or likely-spam comment renders on the
        # dashboard as an ordinary live one.
        extra_records, bucket_outcomes = await _sweep_extra_buckets(channel_id)
        thread_records.extend(extra_records)

        new_count, updated_count = await _store(project_id, thread_records)

        pending, threads_at_reply_cap = await _threads_needing_reply_fetch(
            project_id, threads
        )
        to_fetch = pending[:COMMENT_SYNC_MAX_REPLY_FETCHES]
        reply_records: list[CommentRecord] = []
        reply_fetch_errors: list[dict] = []
        refreshed_thread_ids: list[str] = []
        replies_truncated = 0
        for candidate in to_fetch:
            top_id = candidate.top_comment_id
            # One unreadable thread must not cost the whole sweep: the threads
            # above are already stored, and aborting here would throw away a
            # good sweep over one bad follow-up. It does mean we did not see
            # everything, which `sweep_was_complete` accounts for below.
            try:
                replies, hit_reply_cap = await asyncio.to_thread(
                    youtube.list_comment_replies,
                    top_id,
                    max_pages=COMMENT_SYNC_MAX_REPLY_PAGES,
                )
            except Exception as exc:
                reply_fetch_errors.append({
                    "parent_comment_id": top_id,
                    "error": f"{type(exc).__name__}: {exc}",
                })
                logger.warning(
                    "Could not fetch replies for comment %s (project %s): %s",
                    top_id, project["slug"], exc,
                )
                continue
            if hit_reply_cap:
                replies_truncated += 1
                logger.warning(
                    "Thread %s (project %s) has more replies than the %d-page cap "
                    "reads; storing the first %d and not asking again.",
                    top_id, project["slug"], COMMENT_SYNC_MAX_REPLY_PAGES,
                    len(replies),
                )
            # Only a read that actually completed counts as a refresh. Marking a
            # truncated one fresh would park the thread for a day holding replies
            # we know are partial.
            refreshed_thread_ids.append(top_id)
            reply_records.extend(
                _record_from_comment(
                    reply,
                    video_id=candidate.youtube_video_id,
                    parent_comment_id=top_id,
                    total_reply_count=None,
                    # comments.list takes no moderationStatus filter, so the
                    # resource's own field is the only source here — and it is
                    # the only path by which a reply held AFTER we first stored
                    # it ever gets corrected.
                    bucket_status=None,
                )
                for reply in replies
            )

    # A thread's preview replies were already written above, and the follow-up
    # call returns them again. Dropping them here spares a redundant write and,
    # more importantly, keeps the reported counts honest — re-storing identical
    # rows would inflate "updated" with work that never happened.
    # An owner-authorized thread read should always carry both fields. If one is
    # absent the upsert's COALESCE preserves the stored value — correct, but it
    # means that value can no longer be corrected by this path, so it must not
    # happen silently: a pinned `viewer_rating` would keep a thread permanently
    # marked as answered.
    for record in thread_records:
        missing = [
            name for name, value in (
                ("moderationStatus", record.moderation_status),
                ("viewerRating", record.viewer_rating),
            ) if value is None
        ]
        if missing:
            logger.warning(
                "Thread read for project %s returned no %s for comment %s — the "
                "stored value is preserved and can no longer be corrected here.",
                project["slug"], "/".join(missing), record.comment_id,
            )

    already_written = {r.comment_id for r in thread_records}
    reply_records = [r for r in reply_records if r.comment_id not in already_written]

    reply_new, reply_updated = await _store(project_id, reply_records)
    await _mark_replies_refreshed(project_id, refreshed_thread_ids)

    # Only threads a future sweep can still make progress on. A thread at the
    # per-thread reply cap is excluded upstream: counting it here would keep the
    # sweep permanently incomplete and so permanently suspend "gone from
    # YouTube" detection for the whole project, over replies that are never
    # flagged missing anyway.
    # Counted over INCOMPLETE threads only — content we know is missing and did
    # not get to. A thread that is merely due for a staleness refresh is not
    # unread content: the refresh puts every reply-bearing thread on the due
    # list once a day, so on any channel with more of them than the per-sweep
    # budget that list is never empty. Counting those here would make every
    # sweep permanently incomplete, which permanently suspends "gone from
    # YouTube" detection and leaves the warning banner nagging forever. A
    # rotation running normally is not a failure.
    incomplete_pending = sum(1 for c in pending if c.is_incomplete)
    incomplete_fetched = sum(1 for c in to_fetch if c.is_incomplete)
    unfetched_replies = max(0, incomplete_pending - incomplete_fetched)
    # Reported separately so the rotation is visible without being an alarm.
    refreshes_deferred = max(
        0, (len(pending) - incomplete_pending) - (len(to_fetch) - incomplete_fetched)
    )

    # A sweep that returns nothing at all while the mirror holds comments would
    # mark EVERY one of them "gone from YouTube" in a single tick. That is a
    # plausible truth (a wiped channel) and a plausible fault (an auth or API
    # change that answers empty instead of erroring), and the two are
    # indistinguishable from here — so the inference is declined and the reason
    # recorded, rather than guessed either way.
    stored_comments = await _count_stored_comments(project_id)
    suspicious_empty_sweep = not threads and stored_comments > 0

    # The empty-sweep check above is this same guard with its threshold at 100%.
    # 999 of 1000 comments vanishing in one tick is no more believable than
    # 1000 of 1000, and there are ordinary causes: a video flipped back to
    # unlisted (a state this app's own publish workflow reaches), a takedown, a
    # region block, or pagination ending early. Declining to conclude is always
    # safe — the next sweep decides — whereas a wrong conclusion badges the
    # user's live comments as removed AND stops their threads asking for a reply.
    seen_top_level = {
        r.comment_id for r in thread_records if r.parent_comment_id is None
    }
    would_condemn = await _count_stamped_top_level_absent_from(
        project_id, seen_top_level
    )
    previously_stamped = await _count_stamped_top_level(project_id)
    mass_disappearance = (
        would_condemn > _MASS_DISAPPEARANCE_MIN_COMMENTS
        and previously_stamped > 0
        and would_condemn / previously_stamped > _MASS_DISAPPEARANCE_FRACTION
    )

    # "Complete" has to mean we saw the channel's whole listable surface. Any
    # budget hit, bucket failure, or unread reply thread leaves comments
    # unread, and a comment we did not read is not a comment YouTube stopped
    # returning.
    # `all()` over an empty dict is True, so the bucket coverage is checked
    # explicitly: a sweep that read no supplementary bucket at all must never
    # call itself complete and condemn every held comment on the strength of it.
    buckets_all_read = set(bucket_outcomes) == set(_SUPPLEMENTARY_BUCKETS) and all(
        o["ok"] and not o["pages_truncated"] for o in bucket_outcomes.values()
    )
    sweep_was_complete = (
        not hit_page_cap
        and not unfetched_replies
        and not reply_fetch_errors
        and not suspicious_empty_sweep
        and not mass_disappearance
        and buckets_all_read
    )
    if mass_disappearance:
        logger.warning(
            "Comment sweep for project %s did not return %d of %d previously-seen "
            "top-level comments (>%.0f%%) — declining to treat this as a complete "
            "sweep, so none of them is marked gone from YouTube.",
            project["slug"], would_condemn, previously_stamped,
            _MASS_DISAPPEARANCE_FRACTION * 100,
        )
    if suspicious_empty_sweep:
        logger.warning(
            "Comment sweep for project %s returned no threads while %d comment(s) "
            "are stored — not treating this as a complete sweep, so nothing is "
            "marked gone from YouTube on the strength of it.",
            project["slug"], stored_comments,
        )
    swept_at = previous_swept_at = None
    if sweep_was_complete:
        swept_at, previous_swept_at = await _mark_seen_in_complete_sweep(
            project_id,
            # TOP-LEVEL COMMENTS ONLY. A complete sweep genuinely returns every
            # thread, so a top-level comment's absence is evidence. Its replies
            # are NOT evidence of anything: a thread resource carries only a
            # preview of its replies, and the follow-up is gated on
            # stored-count < totalReplyCount — so a fully-stored thread is never
            # re-read, and every reply past the preview would be condemned as
            # "gone from YouTube" on the very next sweep. Replies therefore keep
            # a NULL stamp, which the read path treats as unknown.
            [r.comment_id for r in thread_records if r.parent_comment_id is None],
        )

    summary = {
        "project_slug": project["slug"],
        "threads": len(threads),
        "comments_seen": len(thread_records) + len(reply_records),
        "new": new_count + reply_new,
        "updated": updated_count + reply_updated,
        "pages_truncated": hit_page_cap,
        "reply_fetches": len(to_fetch),
        "threads_with_unfetched_replies": unfetched_replies,
        "reply_fetch_errors": reply_fetch_errors,
        "reply_refreshes": sum(1 for c in to_fetch if not c.is_incomplete),
        "threads_at_reply_cap": threads_at_reply_cap,
        "refreshes_deferred": refreshes_deferred,
        "threads_with_replies_truncated": replies_truncated,
        "suspicious_empty_sweep": suspicious_empty_sweep,
        "mass_disappearance": (
            {"absent": would_condemn, "of": previously_stamped}
            if mass_disappearance else None
        ),
        "moderation_buckets": bucket_outcomes,
        # Only a complete sweep can tell "YouTube stopped returning this" from
        # "we did not get that far", so say which kind of sweep this was.
        "sweep_was_complete": sweep_was_complete,
        "swept_at": swept_at,
        "previous_swept_at": previous_swept_at,
    }
    for status, outcome in bucket_outcomes.items():
        if not outcome["ok"]:
            logger.warning(
                "Comment sweep for project %s could not read the %s bucket: %s",
                project["slug"], status, outcome["error"],
            )
        elif outcome["pages_truncated"]:
            logger.warning(
                "Comment sweep for project %s stopped at the %d-page cap in the "
                "%s bucket — this sweep is not complete, so nothing is marked "
                "gone from YouTube on the strength of it.",
                project["slug"], COMMENT_SYNC_MAX_PAGES_PER_MODERATION_BUCKET,
                status,
            )
    if hit_page_cap:
        logger.warning(
            "Comment sweep for project %s stopped at the %d-page cap — older "
            "threads were not read this tick.",
            project["slug"], COMMENT_SYNC_MAX_PAGES,
        )
    if summary["threads_with_unfetched_replies"]:
        logger.warning(
            "Comment sweep for project %s left %d thread(s) with replies "
            "unfetched at the %d-fetch cap; the next sweep picks them up.",
            project["slug"], summary["threads_with_unfetched_replies"],
            COMMENT_SYNC_MAX_REPLY_FETCHES,
        )
    return summary


#: A comment's thread. YouTube's model is exactly two levels — a reply to a
#: reply is still parented to the top-level comment — so the whole hierarchy is
#: this one expression, and there is no recursion to write.
_THREAD_KEY = "COALESCE(c.parent_comment_id, c.comment_id)"


async def _resolve_local_videos(project_id: int, youtube_video_ids: set[str]) -> dict:
    """Map YouTube video id -> the local row that labels and links it.

    A second query rather than a LEFT JOIN: ``videos.youtube_video_id`` has a
    non-unique index, so a join could multiply a comment into several rows if
    two local items ever pointed at the same YouTube video.
    """
    if not youtube_video_ids:
        return {}

    db = await get_db()
    placeholders = ",".join("?" * len(youtube_video_ids))
    rows = await db.execute_fetchall(
        f"SELECT id, youtube_video_id, title, episode_number FROM videos "
        f"WHERE project_id = ? AND youtube_video_id IN ({placeholders}) "
        f"ORDER BY created_at",
        (project_id, *youtube_video_ids),
    )
    resolved: dict[str, dict] = {}
    for row in rows:
        # First writer wins on the vanishingly unlikely duplicate; the row is
        # only used to label and link, never to decide anything.
        resolved.setdefault(
            row["youtube_video_id"],
            {
                "local_video_id": row["id"],
                "video_title": row["title"],
                "episode_number": row["episode_number"],
            },
        )
    return resolved


#: A disappearance is only believable if it is both small in absolute terms and
#: small as a share of what we hold. Either test alone misbehaves: a fraction
#: alone condemns 1-of-2 on a tiny channel, an absolute alone lets a big channel
#: lose thousands quietly.
_MASS_DISAPPEARANCE_MIN_COMMENTS = 10
_MASS_DISAPPEARANCE_FRACTION = 0.25


async def _mark_replies_refreshed(project_id: int, top_comment_ids: list[str]) -> None:
    """Record that these threads' replies were just read in full.

    Written only for threads the sweep actually re-read, so a thread it skipped
    stays due. This is what keeps the refresh cycle rotating through the channel
    instead of re-reading the same threads every sweep — and what stops a thread
    with more replies than one follow-up can hold from being asked about forever.
    """
    if not top_comment_ids:
        return

    now = _utc_now()
    async with write_transaction() as wdb:
        for start in range(0, len(top_comment_ids), 500):
            chunk = top_comment_ids[start:start + 500]
            placeholders = ",".join("?" * len(chunk))
            await wdb.execute(
                f"UPDATE youtube_comments SET replies_refreshed_at = ? "
                f"WHERE project_id = ? AND comment_id IN ({placeholders})",
                (now, project_id, *chunk),
            )


async def _count_stamped_top_level(project_id: int) -> int:
    """Top-level comments a complete sweep has previously seen."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT COUNT(*) AS n FROM youtube_comments WHERE project_id = ? "
        "AND parent_comment_id IS NULL AND last_seen_in_sweep_at IS NOT NULL",
        (project_id,),
    )
    return int(rows[0]["n"])


async def _count_stamped_top_level_absent_from(
    project_id: int, seen_comment_ids: set[str]
) -> int:
    """How many previously-seen top-level comments this sweep did NOT return.

    Counted in Python rather than as a `NOT IN (...)` query: the seen set can
    hold thousands of ids, well past what one statement should bind, and the
    stamped set is bounded by the same page budget.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT comment_id FROM youtube_comments WHERE project_id = ? "
        "AND parent_comment_id IS NULL AND last_seen_in_sweep_at IS NOT NULL",
        (project_id,),
    )
    return sum(1 for r in rows if r["comment_id"] not in seen_comment_ids)


async def _count_stored_comments(project_id: int) -> int:
    """Every mirrored comment, moderation filter deliberately NOT applied.

    This is the "is an empty sweep plausible?" yardstick, so it must count what
    the mirror holds, not what the dashboard chooses to show.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT COUNT(*) AS n FROM youtube_comments WHERE project_id = ?",
        (project_id,),
    )
    return int(rows[0]["n"])


async def last_sweep_run(project_id: int) -> dict | None:
    """The recorded outcome of this project's last sweep, or None if none ran.

    Returned to the UI so a background sweep that failed hours ago is still
    visible now. None means no sweep has ever been recorded, which reads as
    "never synced" — not as "the last one was fine".
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT started_at, finished_at, ok, was_complete, error, detail "
        "FROM comment_sweep_runs WHERE project_id = ?",
        (project_id,),
    )
    if not rows:
        return None
    row = dict(rows[0])
    row["ok"] = bool(row["ok"])
    row["was_complete"] = bool(row["was_complete"])
    # Stored verbatim so a new summary field needs no migration; a row written
    # by a failed sweep has none at all.
    row["detail"] = json.loads(row["detail"]) if row["detail"] else None
    return row


async def newest_complete_sweep_at(project_id: int) -> str | None:
    """The newest sweep stamp in this project, or None if none exists.

    Used to keep stamps monotonic. NOT the yardstick for "missing" — that is
    :func:`missing_watermark_at`, which is deliberately one sweep behind this.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT MAX(last_seen_in_sweep_at) AS t FROM youtube_comments "
        "WHERE project_id = ?",
        (project_id,),
    )
    return rows[0]["t"]


async def _recorded_sweep_stamps(project_id: int) -> tuple[str | None, str | None]:
    """``(swept_at, previous_swept_at)`` from the sweep record, or ``(None, None)``."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT swept_at, previous_swept_at FROM comment_sweep_runs "
        "WHERE project_id = ?",
        (project_id,),
    )
    if not rows:
        return None, None
    return rows[0]["swept_at"], rows[0]["previous_swept_at"]


async def missing_watermark_at(project_id: int) -> str | None:
    """The yardstick for "YouTube stopped returning this comment".

    The stamp of the sweep BEFORE the most recent stamping one — so a comment
    must be missed by the last **two** stamping sweeps before it is called gone.

    One missed sweep is not proof. The three moderation buckets are read minutes
    apart, so a comment moving into an already-read bucket is in none of them;
    `order=time` pagination can skip a thread when the window shifts under a new
    arrival; and any future concurrency or partial-write bug lands here too. All
    of those are transient — the next sweep sees the comment again — and two
    strikes discards every one of them for the price of reporting a genuine
    removal one sweep later. For a badge that asserts the user's content was
    removed, that is the right trade.

    Read from ``comment_sweep_runs`` rather than derived from the comment rows,
    because the rows cannot answer it: each sweep overwrites the stamp on every
    comment it saw, so the previous sweep's value stops appearing anywhere. None
    until two sweeps have stamped, which correctly flags nothing.
    """
    _, previous = await _recorded_sweep_stamps(project_id)
    return previous


async def _comments_in_threads(
    project_id: int, thread_keys: list[str], *, missing_watermark: str | None
) -> list[dict]:
    """Every visible comment belonging to the given threads, in reading order.

    The moderation join is safe to make a LEFT JOIN because
    ``idx_moderation_log_comment_unique`` is UNIQUE on (project_id, comment_id):
    a comment cannot be multiplied into several rows by it.

    Only ``action`` values that survive :data:`_NOT_MODERATED_AWAY` can appear —
    ``'error'`` (the blocklist matched, the rejection FAILED, so the comment is
    still live on YouTube) and ``'pending'`` (an in-flight claim). Both are
    reported so the UI can say which, rather than rendering a comment we tried
    and failed to remove as an ordinary one.
    """
    if not thread_keys:
        return []

    db = await get_db()
    placeholders = ",".join("?" * len(thread_keys))
    rows = await db.execute_fetchall(
        f"""
        SELECT c.comment_id, c.youtube_video_id, c.parent_comment_id,
               c.author_display_name, c.author_channel_id,
               c.author_profile_image_url, c.text_display, c.like_count,
               c.total_reply_count, c.published_at, c.youtube_updated_at,
               c.first_seen_at, c.moderation_status, c.viewer_rating,
               c.last_seen_in_sweep_at,
               {_THREAD_KEY} AS thread_key,
               (c.author_channel_id IS NOT NULL
                AND c.author_channel_id = p.youtube_channel_id) AS is_channel_owner,
               -- Reporting a comment in Studio, or its author deleting it,
               -- hides it from the channel without putting it in any listable
               -- bucket — it just stops coming back. The mirror never deletes,
               -- so falling out of a COMPLETE sweep is the only evidence there
               -- is. NULL (never covered by a complete sweep) compares as NULL,
               -- i.e. not missing, which is the honest answer for "unknown".
               --
               -- Only top-level comments are ever stamped, so replies land here
               -- as NULL and are never called gone — a thread's replies arrive
               -- as a truncated preview, so their absence proves nothing.
               (? IS NOT NULL
                AND c.parent_comment_id IS NULL
                AND c.last_seen_in_sweep_at IS NOT NULL
                AND c.last_seen_in_sweep_at < ?) AS is_missing_from_youtube,
               m.action AS moderation_action,
               m.matched_keyword AS moderation_matched_keyword
        FROM youtube_comments c
        JOIN projects p ON p.id = c.project_id
        LEFT JOIN moderation_log m
               ON m.project_id = c.project_id AND m.comment_id = c.comment_id
        WHERE c.project_id = ? AND {_NOT_MODERATED_AWAY}
          AND {_THREAD_KEY} IN ({placeholders})
        ORDER BY c.published_at ASC, c.id ASC
        """,
        (missing_watermark, missing_watermark, project_id, *thread_keys),
    )
    return [dict(r) for r in rows]


async def list_recent_threads(
    project_id: int, *, limit: int, offset: int = 0
) -> list[dict]:
    """Comment threads for a project, most recently active first.

    Paged by THREAD, so a conversation is never split across a page boundary —
    the flat, comment-paged listing this replaced sorted a reply away from the
    comment it answered, which read as "nobody replied to this".

    A thread orders by its newest *visible* comment, so a reply on a months-old
    video brings its thread back up. Within a thread the order is chronological,
    matching YouTube.

    ``top_level_comment`` is None when the thread's parent is not visible —
    either the blocklist rejected it (its replies are not automatically
    rejected with it) or it was never mirrored. Those replies are still real
    comments, so they are shown under a stated gap rather than dropped.
    """
    db = await get_db()
    key_rows = await db.execute_fetchall(
        f"""
        SELECT {_THREAD_KEY} AS thread_key,
               -- Ordering and the thread's stated age are about the conversation
               -- VIEWERS can see. A held or likely-spam comment must not bump a
               -- thread to the top and date it, because the badge explaining why
               -- is on the comment, not the header — the thread would look
               -- freshly active for a reason the reader cannot find.
               MAX(CASE WHEN c.moderation_status IS NULL
                          OR c.moderation_status = 'published'
                        THEN c.published_at END) AS last_visible_at,
               -- A thread whose every comment is hidden still has to sort
               -- somewhere deterministic rather than as NULL.
               MAX(c.published_at) AS last_any_at
        FROM youtube_comments c
        WHERE c.project_id = ? AND {_NOT_MODERATED_AWAY}
        GROUP BY thread_key
        ORDER BY COALESCE(last_visible_at, last_any_at) DESC, thread_key DESC
        LIMIT ? OFFSET ?
        """,
        (project_id, limit, offset),
    )
    thread_keys = [r["thread_key"] for r in key_rows]
    if not thread_keys:
        return []

    comments = await _comments_in_threads(
        project_id,
        thread_keys,
        missing_watermark=await missing_watermark_at(project_id),
    )
    videos = await _resolve_local_videos(
        project_id, {c["youtube_video_id"] for c in comments if c["youtube_video_id"]}
    )

    by_thread: dict[str, list[dict]] = {key: [] for key in thread_keys}
    for comment in comments:
        comment["is_channel_owner"] = bool(comment["is_channel_owner"])
        comment["is_reply"] = comment["parent_comment_id"] is not None
        comment["is_missing_from_youtube"] = bool(comment["is_missing_from_youtube"])
        local = videos.get(comment["youtube_video_id"] or "")
        comment["local_video_id"] = local["local_video_id"] if local else None
        comment["video_title"] = local["video_title"] if local else None
        comment["episode_number"] = local["episode_number"] if local else None
        by_thread[comment.pop("thread_key")].append(comment)

    threads = []
    for row in key_rows:
        key = row["thread_key"]
        members = by_thread[key]
        # "Needs reply" is about people who are actually waiting, so a comment
        # YouTube is holding or scored as spam does not create the obligation —
        # and must not mask the genuine question beneath it. An UNKNOWN status
        # (NULL: pre-migration, or a bucket sweep that failed) counts as visible
        # on purpose: showing a needless badge is a smaller failure than hiding
        # a real question behind a status we never actually read.
        visible = [
            c for c in members
            if c["moderation_status"] in (None, "published")
            and not c["is_missing_from_youtube"]
        ]
        top = next((c for c in members if c["parent_comment_id"] is None), None)
        replies = [c for c in members if c["parent_comment_id"] is not None]
        # Every comment in a thread sits on the same video by construction —
        # `flatten_threads` stamps a reply with its parent's video id, because
        # `comments.list` does not report one. So the thread carries the video,
        # and the UI states it once instead of on every row.
        video_source = top or (replies[0] if replies else None)
        threads.append({
            "thread_key": key,
            "last_activity_at": row["last_visible_at"] or row["last_any_at"],
            "top_level_comment": top,
            "parent_unavailable": top is None,
            "replies": replies,
            "visible_comment_count": len(members),
            # YouTube's own count, kept beside ours: the two disagree while a
            # sweep still owes this thread a reply follow-up, and a user looking
            # at 1 of 4 replies deserves to know the difference.
            "total_reply_count": top["total_reply_count"] if top else None,
            "owner_has_replied": any(c["is_channel_owner"] for c in members),
            # "The last word is theirs, and we have not acknowledged it."
            # Deliberately not "has the owner ever spoken": a viewer who answers
            # your answer leaves the ball in your court again, and that thread
            # must not read as handled. A thumbs-up from the channel counts as
            # acknowledgement — it is a real response, and a thread you
            # deliberately liked instead of answering should stop nagging.
            #
            # Only a *positive* rating can do this: YouTube reports a dislike as
            # 'none', so the absence of a like is never evidence of anything.
            "awaiting_owner_reply": (
                bool(visible)
                and not visible[-1]["is_channel_owner"]
                and visible[-1]["viewer_rating"] != "like"
            ),
            "owner_liked_last_word": bool(visible) and visible[-1]["viewer_rating"] == "like",
            "youtube_video_id": video_source["youtube_video_id"] if video_source else None,
            "local_video_id": video_source["local_video_id"] if video_source else None,
            "video_title": video_source["video_title"] if video_source else None,
            "episode_number": video_source["episode_number"] if video_source else None,
        })
    return threads


async def count_threads(project_id: int) -> int:
    """How many threads the listing can actually show — the 'load more' cut-off.

    Counts exactly what :func:`list_recent_threads` pages over, moderation
    exclusion included, so the total can never promise threads Load more will
    never reach. A thread whose only visible comments are orphaned replies
    counts once, the same as it renders.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        f"SELECT COUNT(DISTINCT {_THREAD_KEY}) AS n FROM youtube_comments c "
        f"WHERE c.project_id = ? AND {_NOT_MODERATED_AWAY}",
        (project_id,),
    )
    return int(rows[0]["n"])


async def last_synced_at(project_id: int) -> str | None:
    """When the newest row in this project was last confirmed against YouTube.

    NULL means no sweep has ever stored a comment for this project — which the
    UI must show as "never synced", not as "no comments".
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT MAX(last_synced_at) AS t FROM youtube_comments WHERE project_id = ?",
        (project_id,),
    )
    return rows[0]["t"]


async def sync_all_projects() -> list[dict]:
    """Sweep every project that has a channel bound. Used by the scheduler job.

    One project's failure never stops the others: each project is a separate
    channel with its own OAuth grant, and a revoked grant on one must not
    silence the rest.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, slug, youtube_channel_id FROM projects "
        "WHERE youtube_channel_id IS NOT NULL ORDER BY id"
    )

    summaries: list[dict] = []
    for row in rows:
        try:
            summaries.append(await sync_project_comments(dict(row)))
        except Exception as exc:
            logger.warning(
                "Comment sweep failed for project %s: %s", row["slug"], exc
            )
            summaries.append({
                "project_slug": row["slug"],
                "error": f"{type(exc).__name__}: {exc}",
            })
    return summaries
