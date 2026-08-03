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
import logging
from dataclasses import dataclass

from yt_scheduler.config import (
    COMMENT_SYNC_MAX_PAGES,
    COMMENT_SYNC_MAX_REPLY_FETCHES,
)
from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import youtube
from yt_scheduler.services.auth import scoped_active_project

logger = logging.getLogger(__name__)


#: Upper bound on one page of the dashboard list. The point is to refuse a
#: request that would read the whole table, not to shape the UI.
MAX_COMMENTS_PER_PAGE = 200


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


def _record_from_comment(
    comment: dict,
    *,
    video_id: str | None,
    parent_comment_id: str | None,
    total_reply_count: int | None,
) -> CommentRecord:
    """Build a :class:`CommentRecord` from one YouTube comment resource.

    ``video_id`` is passed in rather than read from the comment: a reply
    fetched via ``comments.list`` carries no ``videoId``, and it is on the same
    video as the parent thread by definition.
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
    )


def flatten_threads(threads: list[dict]) -> list[CommentRecord]:
    """Flatten thread resources into one record per comment.

    Top-level comments and the replies carried in each thread's preview both
    become rows — "all comments", as a flat list, is what the dashboard shows
    and what a reply to a months-old video needs in order to surface at all.
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
            )
        )
        for reply in (thread.get("replies") or {}).get("comments", []):
            records.append(
                _record_from_comment(
                    reply,
                    video_id=video_id,
                    parent_comment_id=top["id"],
                    total_reply_count=None,
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
                        first_seen_at, last_synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
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
                    ),
                )

    return new_count, len(records) - new_count


async def _threads_missing_replies(
    project_id: int, threads: list[dict]
) -> list[tuple[str, str | None]]:
    """``(top_level_comment_id, video_id)`` for threads we hold fewer replies of
    than YouTube says exist.

    A thread resource carries only a preview of its replies, so a busy thread
    arrives incomplete. Comparing YouTube's ``totalReplyCount`` against the rows
    we already hold makes the follow-up self-limiting: once a thread is fully
    stored it stops being asked about, and a thread that gains a reply next
    month asks again by itself. Newest first, because that is the order the
    reply-fetch budget should be spent in.
    """
    if not threads:
        return []

    db = await get_db()
    wanted: dict[str, tuple[str | None, int]] = {}
    for thread in threads:
        snippet = thread["snippet"]
        total = int(snippet.get("totalReplyCount") or 0)
        if total <= 0:
            continue
        top_id = snippet["topLevelComment"]["id"]
        wanted[top_id] = (snippet.get("videoId"), total)

    if not wanted:
        return []

    held: dict[str, int] = {}
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

    return [
        (top_id, video_id)
        for top_id, (video_id, total) in wanted.items()
        if held.get(top_id, 0) < total
    ]


async def sync_project_comments(project: dict) -> dict:
    """Mirror one project's channel comments into the local table.

    Returns a summary the UI renders directly. ``pages_truncated`` and
    ``threads_with_unfetched_replies`` are part of that summary on purpose: a
    sweep that stopped at a budget must say so, or a partial mirror reads as a
    complete one.

    Raises :class:`ChannelNotBound` when the project has no channel, and lets
    YouTube errors propagate so the caller can surface the real failure.
    """
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
        )

        thread_records = flatten_threads(threads)
        new_count, updated_count = await _store(project_id, thread_records)

        pending = await _threads_missing_replies(project_id, threads)
        to_fetch = pending[:COMMENT_SYNC_MAX_REPLY_FETCHES]
        reply_records: list[CommentRecord] = []
        for top_id, video_id in to_fetch:
            replies = await asyncio.to_thread(youtube.list_comment_replies, top_id)
            reply_records.extend(
                _record_from_comment(
                    reply,
                    video_id=video_id,
                    parent_comment_id=top_id,
                    total_reply_count=None,
                )
                for reply in replies
            )

    # A thread's preview replies were already written above, and the follow-up
    # call returns them again. Dropping them here spares a redundant write and,
    # more importantly, keeps the reported counts honest — re-storing identical
    # rows would inflate "updated" with work that never happened.
    already_written = {r.comment_id for r in thread_records}
    reply_records = [r for r in reply_records if r.comment_id not in already_written]

    reply_new, reply_updated = await _store(project_id, reply_records)

    summary = {
        "project_slug": project["slug"],
        "threads": len(threads),
        "comments_seen": len(thread_records) + len(reply_records),
        "new": new_count + reply_new,
        "updated": updated_count + reply_updated,
        "pages_truncated": hit_page_cap,
        "reply_fetches": len(to_fetch),
        "threads_with_unfetched_replies": max(0, len(pending) - len(to_fetch)),
    }
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


async def list_recent_comments(
    project_id: int, *, limit: int, offset: int = 0
) -> list[dict]:
    """Newest comments for a project, with the video each one sits on.

    The local video is resolved in a second query rather than a LEFT JOIN:
    ``videos.youtube_video_id`` has a non-unique index, so a join could
    multiply a comment into several rows if two local items ever pointed at the
    same YouTube video.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        f"""
        SELECT c.comment_id, c.youtube_video_id, c.parent_comment_id,
               c.author_display_name, c.author_channel_id,
               c.author_profile_image_url, c.text_display, c.like_count,
               c.total_reply_count, c.published_at, c.youtube_updated_at,
               c.first_seen_at,
               (c.author_channel_id IS NOT NULL
                AND c.author_channel_id = p.youtube_channel_id) AS is_channel_owner
        FROM youtube_comments c
        JOIN projects p ON p.id = c.project_id
        WHERE c.project_id = ? AND {_NOT_MODERATED_AWAY}
        ORDER BY c.published_at DESC, c.id DESC
        LIMIT ? OFFSET ?
        """,
        (project_id, limit, offset),
    )
    comments = [dict(r) for r in rows]

    video_ids = {c["youtube_video_id"] for c in comments if c["youtube_video_id"]}
    titles: dict[str, dict] = {}
    if video_ids:
        placeholders = ",".join("?" * len(video_ids))
        video_rows = await db.execute_fetchall(
            f"SELECT id, youtube_video_id, title, episode_number FROM videos "
            f"WHERE project_id = ? AND youtube_video_id IN ({placeholders}) "
            f"ORDER BY created_at",
            (project_id, *video_ids),
        )
        for row in video_rows:
            # First writer wins on the vanishingly unlikely duplicate; the row
            # is only used to label and link, never to decide anything.
            titles.setdefault(
                row["youtube_video_id"],
                {
                    "local_video_id": row["id"],
                    "video_title": row["title"],
                    "episode_number": row["episode_number"],
                },
            )

    for comment in comments:
        comment["is_channel_owner"] = bool(comment["is_channel_owner"])
        comment["is_reply"] = comment["parent_comment_id"] is not None
        local = titles.get(comment["youtube_video_id"] or "")
        comment["local_video_id"] = local["local_video_id"] if local else None
        comment["video_title"] = local["video_title"] if local else None
        comment["episode_number"] = local["episode_number"] if local else None
    return comments


async def count_comments(project_id: int) -> int:
    """How many comments the listing can actually show — the 'load more' cut-off.

    Counts exactly what :func:`list_recent_comments` returns, moderation
    exclusion included, so the total can never promise rows the list won't hand
    back.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        f"SELECT COUNT(*) AS n FROM youtube_comments c "
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
