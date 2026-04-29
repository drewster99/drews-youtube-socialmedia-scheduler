"""Comment moderation — keyword-based filtering."""

from __future__ import annotations

import asyncio
import logging
import re

from yt_scheduler.database import get_db
from yt_scheduler.services import youtube

logger = logging.getLogger(__name__)


async def get_blocklist(project_id: int = 1) -> list[dict]:
    """Get all blocked keywords for a project."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM blocklist WHERE project_id = ? ORDER BY keyword",
        (project_id,),
    )
    return [dict(r) for r in rows]


async def add_keyword(keyword: str, is_regex: bool = False, project_id: int = 1) -> None:
    """Add a keyword to a project's blocklist."""
    db = await get_db()
    await db.execute(
        "INSERT OR IGNORE INTO blocklist (project_id, keyword, is_regex) "
        "VALUES (?, ?, ?)",
        (project_id, keyword, int(is_regex)),
    )
    await db.commit()


async def remove_keyword(keyword_id: int, project_id: int = 1) -> None:
    """Remove a keyword from a project's blocklist."""
    db = await get_db()
    await db.execute(
        "DELETE FROM blocklist WHERE id = ? AND project_id = ?",
        (keyword_id, project_id),
    )
    await db.commit()


def matches_blocklist(text: str, blocklist: list[dict]) -> str | None:
    """Check if text matches any keyword in the blocklist.

    Returns the matched keyword or None.
    """
    text_lower = text.lower()
    for entry in blocklist:
        keyword = entry["keyword"]
        if entry["is_regex"]:
            if re.search(keyword, text, re.IGNORECASE):
                return keyword
        else:
            if keyword.lower() in text_lower:
                return keyword
    return None


async def _list_comment_threads_async(video_id: str, max_results: int = 200) -> list[dict]:
    """``youtube.list_comment_threads`` is sync; run it in a thread so we don't
    block the event loop and so any RuntimeError surfaces cleanly."""
    return await asyncio.to_thread(
        youtube.list_comment_threads, video_id, max_results=max_results
    )


async def check_video_comments(video_id: str, *, project_id: int = 1, blocklist: list[dict] | None = None) -> list[dict]:
    """Check all comments on a video against the blocklist.

    Returns the list of actions taken. Errors fetching comments propagate as
    ``RuntimeError`` so the caller can surface them rather than silently
    appearing as "no matches".
    """
    if blocklist is None:
        blocklist = await get_blocklist(project_id=project_id)
    if not blocklist:
        return []

    db = await get_db()
    actions: list[dict] = []

    try:
        threads = await _list_comment_threads_async(video_id, max_results=200)
    except Exception as exc:
        # Re-raise so the caller can decide how to surface (e.g. surfaced in
        # the run-now response, logged for the periodic job).
        raise RuntimeError(f"Failed to list comments for {video_id}: {exc}") from exc

    for thread in threads:
        top_comment = thread["snippet"]["topLevelComment"]
        actions.extend(
            await _process_one(db, project_id, video_id, top_comment, blocklist)
        )

        for reply in thread.get("replies", {}).get("comments", []):
            actions.extend(
                await _process_one(db, project_id, video_id, reply, blocklist)
            )

    await db.commit()
    return actions


async def _process_one(
    db,
    project_id: int,
    video_id: str,
    comment: dict,
    blocklist: list[dict],
) -> list[dict]:
    comment_id = comment["id"]
    text = comment["snippet"].get("textDisplay", "")
    author = comment["snippet"].get("authorDisplayName", "")
    matched = matches_blocklist(text, blocklist)
    if not matched:
        return []
    # Skip if we've already logged anything for this comment in this project.
    # YouTube's commentThreads.list still returns already-rejected comments,
    # and re-running moderation would otherwise insert a duplicate row every
    # tick (every 30 min by default) — both for new errors and for
    # already-deleted successes.
    cursor = await db.execute(
        "SELECT 1 FROM moderation_log WHERE project_id = ? AND comment_id = ? LIMIT 1",
        (project_id, comment_id),
    )
    if await cursor.fetchone() is not None:
        return []

    try:
        await asyncio.to_thread(youtube.moderate_comment, comment_id, "rejected")
        action = "deleted"
        error: str | None = None
    except Exception as exc:
        logger.error(
            "Failed to reject comment %s on video %s (project %s): %s",
            comment_id, video_id, project_id, exc,
        )
        action = "error"
        error = f"{type(exc).__name__}: {exc}"
    # Log the attempt either way so the user can see why moderation 'didn't
    # work' — silently swallowing the failure was the historical bug.
    await db.execute(
        """INSERT INTO moderation_log
        (project_id, video_id, comment_id, author, comment_text, matched_keyword, action)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (project_id, video_id, comment_id, author, text[:500], matched, action),
    )
    return [{
        "comment_id": comment_id,
        "author": author,
        "text": text[:100],
        "matched": matched,
        "action": action,
        "error": error,
    }]


async def check_all_videos(project_id: int = 1) -> dict:
    """Run moderation against every uploaded/published video for a project.

    Returns a structured summary the UI can render directly.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM videos WHERE project_id = ? AND status IN "
        "('uploaded', 'published', 'ready', 'captioned')",
        (project_id,),
    )
    blocklist = await get_blocklist(project_id=project_id)

    results = {
        "checked": len(rows),
        "matched": 0,
        "actions_by_video": {},
        "errors": [],
    }
    for row in rows:
        video_id = row["id"]
        try:
            actions = await check_video_comments(
                video_id, project_id=project_id, blocklist=blocklist
            )
        except RuntimeError as exc:
            results["errors"].append({"video_id": video_id, "error": str(exc)})
            continue
        if actions:
            results["actions_by_video"][video_id] = actions
            results["matched"] += len(actions)
    return results


async def get_moderation_log(limit: int = 50, project_id: int = 1) -> list[dict]:
    """Get recent moderation actions for a project."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM moderation_log WHERE project_id = ? "
        "ORDER BY created_at DESC LIMIT ?",
        (project_id, limit),
    )
    return [dict(r) for r in rows]
