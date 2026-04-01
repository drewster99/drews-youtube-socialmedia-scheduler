"""Comment moderation — keyword-based filtering."""

from __future__ import annotations

import json
import re

from youtube_publisher.database import get_db
from youtube_publisher.services import youtube


async def get_blocklist() -> list[dict]:
    """Get all blocked keywords."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM blocklist ORDER BY keyword")
    return [dict(r) for r in rows]


async def add_keyword(keyword: str, is_regex: bool = False) -> None:
    """Add a keyword to the blocklist."""
    db = await get_db()
    await db.execute(
        "INSERT OR IGNORE INTO blocklist (keyword, is_regex) VALUES (?, ?)",
        (keyword, int(is_regex)),
    )
    await db.commit()


async def remove_keyword(keyword_id: int) -> None:
    """Remove a keyword from the blocklist."""
    db = await get_db()
    await db.execute("DELETE FROM blocklist WHERE id = ?", (keyword_id,))
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


async def check_video_comments(video_id: str) -> list[dict]:
    """Check all comments on a video against the blocklist.

    Returns list of actions taken.
    """
    blocklist = await get_blocklist()
    if not blocklist:
        return []

    db = await get_db()
    actions = []

    try:
        threads = youtube.list_comment_threads(video_id, max_results=200)
    except Exception:
        return []

    for thread in threads:
        top_comment = thread["snippet"]["topLevelComment"]
        comment_id = top_comment["id"]
        comment_text = top_comment["snippet"].get("textDisplay", "")
        author = top_comment["snippet"].get("authorDisplayName", "")

        matched = matches_blocklist(comment_text, blocklist)
        if matched:
            try:
                youtube.moderate_comment(comment_id, status="rejected")
                await db.execute(
                    """INSERT INTO moderation_log
                    (video_id, comment_id, author, comment_text, matched_keyword, action)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (video_id, comment_id, author, comment_text[:500], matched, "deleted"),
                )
                actions.append({
                    "comment_id": comment_id,
                    "author": author,
                    "text": comment_text[:100],
                    "matched": matched,
                })
            except Exception:
                pass

        # Also check replies
        replies = thread.get("replies", {}).get("comments", [])
        for reply in replies:
            reply_id = reply["id"]
            reply_text = reply["snippet"].get("textDisplay", "")
            reply_author = reply["snippet"].get("authorDisplayName", "")

            matched = matches_blocklist(reply_text, blocklist)
            if matched:
                try:
                    youtube.moderate_comment(reply_id, status="rejected")
                    await db.execute(
                        """INSERT INTO moderation_log
                        (video_id, comment_id, author, comment_text, matched_keyword, action)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (video_id, reply_id, reply_author, reply_text[:500], matched, "deleted"),
                    )
                    actions.append({
                        "comment_id": reply_id,
                        "author": reply_author,
                        "text": reply_text[:100],
                        "matched": matched,
                    })
                except Exception:
                    pass

    await db.commit()
    return actions


async def check_all_videos() -> dict[str, list[dict]]:
    """Check comments on all tracked videos."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM videos WHERE status IN ('uploaded', 'published', 'ready')"
    )
    results = {}
    for row in rows:
        video_id = row["id"]
        actions = await check_video_comments(video_id)
        if actions:
            results[video_id] = actions
    return results


async def get_moderation_log(limit: int = 50) -> list[dict]:
    """Get recent moderation actions."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM moderation_log ORDER BY created_at DESC LIMIT ?", (limit,)
    )
    return [dict(r) for r in rows]
