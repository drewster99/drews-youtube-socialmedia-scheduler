"""Background scheduler for scheduled publishing, comment moderation, and caption checks."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from youtube_publisher.config import CAPTION_CHECK_INTERVAL_MINUTES, COMMENT_CHECK_INTERVAL_MINUTES
from youtube_publisher.database import get_db
from youtube_publisher.services import moderation, youtube

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def publish_video_job(video_id: str) -> dict:
    """Publish a video and fire all approved social posts.

    This is the core scheduled action:
    1. Flip video from unlisted → public
    2. Send all approved social posts
    3. Log results

    Returns a summary dict.
    """
    from youtube_publisher.services.social import get_poster

    db = await get_db()
    results = {"video_id": video_id, "published": False, "social_results": {}}

    # Step 1: Flip to public
    try:
        youtube.update_video_metadata(video_id, privacy_status="public")
        await db.execute(
            """UPDATE videos SET privacy_status = 'public', status = 'published',
            updated_at = datetime('now') WHERE id = ?""",
            (video_id,),
        )
        await db.commit()
        results["published"] = True
        logger.info(f"Video {video_id} is now public")
    except Exception as e:
        logger.error(f"Failed to publish video {video_id}: {e}")
        results["publish_error"] = str(e)
        # Don't fire social posts if video didn't go public
        return results

    # Step 2: Fire all approved social posts
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE video_id = ? AND status = 'approved'",
        (video_id,),
    )

    for row in rows:
        post = dict(row)
        platform = post["platform"]
        poster = get_poster(platform)

        if not await poster.is_configured():
            results["social_results"][platform] = {"status": "skipped", "reason": "not configured"}
            continue

        try:
            post_result = await poster.post(post["content"], post.get("media_path"))
            await db.execute(
                """UPDATE social_posts
                SET status = 'posted', posted_at = datetime('now'), post_url = ?
                WHERE id = ?""",
                (post_result.get("url", ""), post["id"]),
            )
            results["social_results"][platform] = {
                "status": "posted",
                "url": post_result.get("url", ""),
            }
            logger.info(f"Posted to {platform}: {post_result.get('url', '')}")
        except Exception as e:
            await db.execute(
                "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
                (str(e), post["id"]),
            )
            results["social_results"][platform] = {"status": "failed", "error": str(e)}
            logger.error(f"Failed to post to {platform}: {e}")

    await db.commit()
    return results


async def schedule_publish(video_id: str, publish_at: datetime) -> str:
    """Schedule a video to be published at a specific time.

    The video stays unlisted until the scheduled time, then:
    - Flips to public
    - Fires all approved social posts

    Returns the job ID.
    """
    job_id = f"publish_{video_id}"

    # Remove existing schedule for this video if any
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)

    # Ensure publish_at is timezone-aware
    if publish_at.tzinfo is None:
        publish_at = publish_at.replace(tzinfo=timezone.utc)

    scheduler.add_job(
        publish_video_job,
        "date",
        run_date=publish_at,
        args=[video_id],
        id=job_id,
        replace_existing=True,
        misfire_grace_time=300,  # 5 min grace period
    )

    # Update DB
    db = await get_db()
    await db.execute(
        "UPDATE videos SET publish_at = ?, status = 'scheduled', updated_at = datetime('now') WHERE id = ?",
        (publish_at.isoformat(), video_id),
    )
    await db.commit()

    logger.info(f"Scheduled video {video_id} to publish at {publish_at.isoformat()}")
    return job_id


async def cancel_scheduled_publish(video_id: str) -> bool:
    """Cancel a scheduled publish."""
    job_id = f"publish_{video_id}"
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)

        db = await get_db()
        await db.execute(
            "UPDATE videos SET publish_at = NULL, status = 'ready', updated_at = datetime('now') WHERE id = ?",
            (video_id,),
        )
        await db.commit()
        logger.info(f"Cancelled scheduled publish for {video_id}")
        return True
    return False


def get_scheduled_jobs() -> list[dict]:
    """List all scheduled publish jobs."""
    jobs = []
    for job in scheduler.get_jobs():
        if job.id.startswith("publish_"):
            video_id = job.id.replace("publish_", "")
            jobs.append({
                "video_id": video_id,
                "job_id": job.id,
                "run_date": job.next_run_time.isoformat() if job.next_run_time else None,
            })
    return jobs


async def restore_scheduled_jobs() -> None:
    """On startup, re-schedule any videos that have a publish_at in the future."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, publish_at FROM videos WHERE status = 'scheduled' AND publish_at IS NOT NULL"
    )

    now = datetime.now(timezone.utc)
    for row in rows:
        try:
            publish_at = datetime.fromisoformat(row["publish_at"])
            if publish_at.tzinfo is None:
                publish_at = publish_at.replace(tzinfo=timezone.utc)

            if publish_at > now:
                await schedule_publish(row["id"], publish_at)
                logger.info(f"Restored scheduled publish for {row['id']} at {publish_at}")
            else:
                # Missed the window — publish immediately
                logger.warning(f"Missed publish window for {row['id']}, publishing now")
                await publish_video_job(row["id"])
        except Exception as e:
            logger.error(f"Failed to restore schedule for {row['id']}: {e}")


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
            auto_captions = [
                c for c in captions
                if c["snippet"].get("trackKind") == "ASR"
            ]
            if auto_captions:
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
                logger.info(f"Moderated {len(actions)} comments on video {video_id}")
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
