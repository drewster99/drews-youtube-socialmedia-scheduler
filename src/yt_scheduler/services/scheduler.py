"""Background scheduler for scheduled publishing, comment moderation, and caption checks."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from yt_scheduler.config import CAPTION_CHECK_INTERVAL_MINUTES, COMMENT_CHECK_INTERVAL_MINUTES
from yt_scheduler.database import get_db
from yt_scheduler.services import events, moderation, transcripts as transcript_service, youtube

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# Prevents concurrent publish + post regeneration from racing
_publish_locks: dict[str, asyncio.Lock] = {}


def get_publish_lock(video_id: str) -> asyncio.Lock:
    """Get or create a per-video lock to prevent concurrent publish/regenerate races."""
    if video_id not in _publish_locks:
        _publish_locks[video_id] = asyncio.Lock()
    return _publish_locks[video_id]


async def _claim_post_for_send(post_id: int) -> bool:
    """Atomically transition a post from ``approved`` → ``sending``.

    Returns True if THIS caller won the claim and should now send;
    False if someone else already did. Stops a race where
    ``publish_video_job`` and ``_send_scheduled_post`` both fire at the
    same instant for the same post — both used to read ``status='approved'``,
    both posted to the platform, both wrote ``status='posted'`` (1 DB row,
    2 actual social posts).
    """
    db = await get_db()
    cursor = await db.execute(
        "UPDATE social_posts SET status = 'sending' "
        "WHERE id = ? AND status = 'approved'",
        (post_id,),
    )
    await db.commit()
    return cursor.rowcount > 0


async def _release_post_to_approved(post_id: int) -> None:
    """Roll a claimed post back to 'approved' so the user can retry.

    Used when the send fails for a reason that's not the credential
    being broken — network blip, transient platform error, etc.
    Credential auth errors keep the 'failed' status because retrying
    won't help until the user re-OAuths.
    """
    db = await get_db()
    await db.execute(
        "UPDATE social_posts SET status = 'approved' "
        "WHERE id = ? AND status = 'sending'",
        (post_id,),
    )
    await db.commit()


async def publish_video_job(video_id: str) -> dict:
    """Publish a video and fire all approved social posts.

    This is the core scheduled action:
    1. Flip video from unlisted → public
    2. Send all approved social posts
    3. Log results

    Uses a per-video lock to prevent races with post regeneration.

    Returns a summary dict.
    """
    from yt_scheduler.services.social import (
        get_poster,
        get_poster_for_account,
    )

    async def _resolve_poster(post_row: dict, project_id: int):
        sa_id = post_row.get("social_account_id")
        if sa_id:
            return await get_poster_for_account(int(sa_id))
        cursor = await db.execute(
            "SELECT social_account_id FROM project_social_defaults "
            "WHERE project_id = ? AND platform = ?",
            (project_id, post_row["platform"]),
        )
        default_row = await cursor.fetchone()
        if default_row is not None and default_row["social_account_id"] is not None:
            return await get_poster_for_account(int(default_row["social_account_id"]))
        return get_poster(post_row["platform"])

    lock = get_publish_lock(video_id)
    async with lock:
        db = await get_db()
        results: dict = {"video_id": video_id, "published": False, "social_results": {}}

        cursor = await db.execute(
            "SELECT project_id FROM videos WHERE id = ?", (video_id,)
        )
        video_row = await cursor.fetchone()
        project_id = int(video_row["project_id"]) if video_row else 1

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
            await events.record_event(
                video_id,
                "published",
                {"platform": "youtube", "url": f"https://youtu.be/{video_id}"},
            )
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
            post_id = post["id"]

            if platform not in results["social_results"]:
                results["social_results"][platform] = []

            # Atomic claim: another worker (e.g. _send_scheduled_post for
            # the same post_id) might fire concurrently. Whoever wins the
            # status transition sends; the loser sees rowcount=0 and
            # skips, so we never hit the platform twice.
            if not await _claim_post_for_send(post_id):
                results["social_results"][platform].append(
                    {"post_id": post_id, "status": "skipped", "reason": "already claimed by another worker"}
                )
                continue

            try:
                poster = await _resolve_poster(post, project_id)
            except ValueError as exc:
                await _release_post_to_approved(post_id)
                results["social_results"][platform].append(
                    {"post_id": post_id, "status": "skipped", "reason": str(exc)}
                )
                continue

            if not await poster.is_configured():
                await _release_post_to_approved(post_id)
                results["social_results"][platform].append(
                    {"post_id": post_id, "status": "skipped", "reason": "not configured"}
                )
                continue

            try:
                post_result = await poster.post(post["content"], post.get("media_path"))
                await db.execute(
                    """UPDATE social_posts
                    SET status = 'posted', posted_at = datetime('now'), post_url = ?
                    WHERE id = ?""",
                    (post_result.get("url", ""), post_id),
                )
                results["social_results"][platform].append({
                    "post_id": post_id,
                    "status": "posted",
                    "url": post_result.get("url", ""),
                })
                await events.record_event(
                    video_id,
                    "social_post_published",
                    {
                        "platform": platform,
                        "social_account_id": post.get("social_account_id"),
                        "post_url": post_result.get("url", ""),
                        "posted_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                logger.info(f"Posted to {platform}: {post_result.get('url', '')}")
            except Exception as e:
                from yt_scheduler.services.social import CredentialAuthError
                from yt_scheduler.services.social_credentials import mark_needs_reauth

                if isinstance(e, CredentialAuthError) and e.uuid:
                    await mark_needs_reauth(e.uuid)
                await db.execute(
                    "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
                    (str(e), post_id),
                )
                results["social_results"][platform].append(
                    {"post_id": post_id, "status": "failed", "error": str(e)}
                )
                logger.error(f"Failed to post to {platform}: {e}")

        await db.commit()
        return results


# --- Per-post social scheduling (Phase 10) ---------------------------------

_post_locks: dict[int, asyncio.Lock] = {}


def _post_lock(post_id: int) -> asyncio.Lock:
    if post_id not in _post_locks:
        _post_locks[post_id] = asyncio.Lock()
    return _post_locks[post_id]


async def _send_scheduled_post(post_id: int) -> None:
    """APScheduler-fired worker for an individual scheduled post."""
    from yt_scheduler.services.social import (
        get_poster,
        get_poster_for_account,
    )

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE id = ?", (post_id,)
    )
    if not rows:
        logger.warning("Scheduled post %s vanished before firing", post_id)
        return
    post = dict(rows[0])
    if post.get("status") in ("posted", "sending"):
        # Already sent or another worker is sending right now — abort.
        return

    if not await _claim_post_for_send(post_id):
        # Either it wasn't 'approved' (e.g. user already manually sent
        # or unscheduled it) or another worker beat us to the claim.
        return

    try:
        sa_id = post.get("social_account_id")
        if sa_id:
            poster = await get_poster_for_account(int(sa_id))
        else:
            cursor = await db.execute(
                "SELECT v.project_id FROM social_posts sp "
                "JOIN videos v ON v.id = sp.video_id WHERE sp.id = ?",
                (post_id,),
            )
            row = await cursor.fetchone()
            project_id = int(row["project_id"]) if row else 1
            cursor = await db.execute(
                "SELECT social_account_id FROM project_social_defaults "
                "WHERE project_id = ? AND platform = ?",
                (project_id, post["platform"]),
            )
            default_row = await cursor.fetchone()
            if default_row is not None and default_row["social_account_id"] is not None:
                poster = await get_poster_for_account(int(default_row["social_account_id"]))
            else:
                poster = get_poster(post["platform"])
    except ValueError as exc:
        await db.execute(
            "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
            (f"credential resolution failed: {exc}", post_id),
        )
        await db.commit()
        return

    if not await poster.is_configured():
        await db.execute(
            "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
            (f"{post['platform']} not configured", post_id),
        )
        await db.commit()
        return
    try:
        result = await poster.post(post["content"], post.get("media_path"))
        await db.execute(
            "UPDATE social_posts SET status = 'posted', posted_at = datetime('now'), "
            "post_url = ?, scheduler_job_id = NULL WHERE id = ?",
            (result.get("url", ""), post_id),
        )
        await db.commit()
        await events.record_event(
            post["video_id"],
            "social_post_published",
            {
                "platform": post["platform"],
                "social_account_id": post.get("social_account_id"),
                "post_url": result.get("url", ""),
                "posted_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as exc:
        from yt_scheduler.services.social import CredentialAuthError
        from yt_scheduler.services.social_credentials import mark_needs_reauth

        if isinstance(exc, CredentialAuthError) and exc.uuid:
            await mark_needs_reauth(exc.uuid)
        logger.error("Failed to send scheduled post %s: %s", post_id, exc)
        await db.execute(
            "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
            (str(exc), post_id),
        )
        await db.commit()


async def schedule_social_post(post_id: int, when: datetime) -> str:
    """Register or replace a per-post DateTrigger.

    Holds a per-post lock and removes any existing job before creating the new
    one so concurrent reschedule calls can't double-fire.
    """
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)

    async with _post_lock(post_id):
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT scheduler_job_id, video_id, platform, content "
            "FROM social_posts WHERE id = ?",
            (post_id,),
        )
        if not rows:
            raise ValueError(f"Social post {post_id} not found")
        old_job_id = rows[0]["scheduler_job_id"]
        if old_job_id:
            existing = scheduler.get_job(old_job_id)
            if existing:
                scheduler.remove_job(old_job_id)

        job_id = f"social_post_{post_id}"
        scheduler.add_job(
            _send_scheduled_post,
            "date",
            run_date=when,
            args=[post_id],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=300,
        )

        await db.execute(
            "UPDATE social_posts SET scheduled_at = ?, scheduler_job_id = ?, "
            "status = 'approved' WHERE id = ?",
            (when.isoformat(), job_id, post_id),
        )
        await db.commit()
        await events.record_event(
            rows[0]["video_id"],
            "social_post_scheduled",
            {
                "platform": rows[0]["platform"],
                "scheduled_at": when.isoformat(),
                "text": rows[0]["content"],
            },
        )
        return job_id


async def cancel_scheduled_post(post_id: int) -> bool:
    """Tear down the DateTrigger for a post; returns True if one was active."""
    async with _post_lock(post_id):
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT scheduler_job_id FROM social_posts WHERE id = ?", (post_id,)
        )
        if not rows or not rows[0]["scheduler_job_id"]:
            return False
        job_id = rows[0]["scheduler_job_id"]
        existing = scheduler.get_job(job_id)
        if existing:
            scheduler.remove_job(job_id)
        await db.execute(
            "UPDATE social_posts SET scheduled_at = NULL, scheduler_job_id = NULL "
            "WHERE id = ?",
            (post_id,),
        )
        await db.commit()
        return True


async def restore_scheduled_posts() -> None:
    """Re-register pending per-post jobs after a server restart."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at FROM social_posts "
        "WHERE scheduled_at IS NOT NULL AND status != 'posted'"
    )
    now = datetime.now(timezone.utc)
    for row in rows:
        try:
            when = datetime.fromisoformat(row["scheduled_at"])
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            if when > now:
                await schedule_social_post(int(row["id"]), when)
            else:
                # Missed window — fire immediately.
                await _send_scheduled_post(int(row["id"]))
        except Exception as exc:
            logger.error("Failed to restore scheduled post %s: %s", row["id"], exc)


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
    """On startup, re-schedule any videos that have a publish_at in the future
    plus any individually-scheduled social posts."""
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

    await restore_scheduled_posts()


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
                # Store SRT canonically — preserves segment timestamps for
                # YouTube round-trip + chapter detection.
                caption_text = youtube.download_caption(auto_captions[0]["id"], fmt="srt")
                transcript_id = await transcript_service.upsert_transcript_for_source(
                    video_id, "youtube", caption_text
                )
                await db.execute(
                    """UPDATE videos SET
                        transcript = ?,
                        transcript_id = ?,
                        transcript_source = 'youtube',
                        transcript_created_at = COALESCE(transcript_created_at, datetime('now')),
                        transcript_updated_at = datetime('now'),
                        status = 'captioned',
                        updated_at = datetime('now')
                    WHERE id = ?""",
                    (caption_text, transcript_id, video_id),
                )
                await db.commit()
                logger.info(f"Captions ready for video {video_id}: {row['title']}")
        except Exception as e:
            logger.warning(f"Failed to check captions for {video_id}: {e}")


async def moderate_comments_job() -> None:
    """Run comment moderation on all tracked videos."""
    try:
        results = await moderation.check_all_videos()
        actions_by_video = results.get("actions_by_video", {})
        for video_id, actions in actions_by_video.items():
            if actions:
                logger.info(f"Moderated {len(actions)} comments on video {video_id}")
        if results.get("checked"):
            logger.info(
                "Moderation tick: checked %s, matched %s",
                results.get("checked", 0),
                results.get("matched", 0),
            )
        # Surface auth-shaped errors loudly so they don't drown in the
        # generic per-video error list. Project Settings → YouTube card
        # also re-renders 'Needs re-auth' on next load (its needs_reauth
        # flag is computed live from a channels().list() probe), but
        # that requires the user to look. Logging gives ops a signal too.
        errors = results.get("errors") or []
        auth_failures = [
            err for err in errors
            if any(needle in (err.get("error") or "").lower() for needle in (
                "not authenticated", "invalid_grant", "unauthorized",
                "401", "credentials", "refresh",
            ))
        ]
        if auth_failures:
            logger.error(
                "Moderation: YouTube auth failures on %d video(s); "
                "the project's YouTube credential likely needs re-auth. "
                "First error: %s",
                len(auth_failures), auth_failures[0]["error"],
            )
        elif errors:
            logger.warning(
                "Moderation: %d video(s) errored (non-auth); first: %s",
                len(errors), errors[0]["error"],
            )
    except Exception as e:
        logger.warning(f"Comment moderation job failed: {e}")


def start_scheduler(
    caption_interval: int | None = None,
    comment_interval: int | None = None,
) -> None:
    """Start the background scheduler.

    Intervals can be overridden (e.g. from DB settings). Falls back to config defaults.
    """
    cap_mins = caption_interval or CAPTION_CHECK_INTERVAL_MINUTES
    mod_mins = comment_interval or COMMENT_CHECK_INTERVAL_MINUTES

    scheduler.add_job(
        check_captions_job,
        "interval",
        minutes=cap_mins,
        id="check_captions",
        replace_existing=True,
    )
    scheduler.add_job(
        moderate_comments_job,
        "interval",
        minutes=mod_mins,
        id="moderate_comments",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started (captions every {cap_mins}m, moderation every {mod_mins}m)")


def stop_scheduler() -> None:
    """Stop the background scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
