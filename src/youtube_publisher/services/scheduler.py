"""Background scheduler for comment moderation and publish checks."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from youtube_publisher.config import CAPTION_CHECK_INTERVAL_MINUTES, COMMENT_CHECK_INTERVAL_MINUTES
from youtube_publisher.database import get_db
from youtube_publisher.services import moderation, youtube

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def check_captions_job() -> None:
    """Check for videos waiting on captions."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, title FROM videos WHERE status = 'uploaded'"
    )

    for row in rows:
        video_id = row["id"]
        try:
            captions = youtube.list_captions(video_id)
            # Look for auto-generated captions
            auto_captions = [
                c for c in captions
                if c["snippet"].get("trackKind") == "ASR"
            ]
            if auto_captions:
                # Download the first auto-caption track
                caption_text = youtube.download_caption(auto_captions[0]["id"], fmt="srt")
                await db.execute(
                    "UPDATE videos SET transcript = ?, status = 'captioned', updated_at = datetime('now') WHERE id = ?",
                    (caption_text, video_id),
                )
                await db.commit()
                logger.info(f"Captions ready for video {video_id}: {row['title']}")
        except Exception as e:
            logger.warning(f"Failed to check captions for {video_id}: {e}")


async def moderate_comments_job() -> None:
    """Run comment moderation on all tracked videos."""
    try:
        results = await moderation.check_all_videos()
        for video_id, actions in results.items():
            if actions:
                logger.info(
                    f"Moderated {len(actions)} comments on video {video_id}"
                )
    except Exception as e:
        logger.warning(f"Comment moderation job failed: {e}")


def start_scheduler() -> None:
    """Start the background scheduler."""
    scheduler.add_job(
        check_captions_job,
        "interval",
        minutes=CAPTION_CHECK_INTERVAL_MINUTES,
        id="check_captions",
        replace_existing=True,
    )
    scheduler.add_job(
        moderate_comments_job,
        "interval",
        minutes=COMMENT_CHECK_INTERVAL_MINUTES,
        id="moderate_comments",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started")


def stop_scheduler() -> None:
    """Stop the background scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
