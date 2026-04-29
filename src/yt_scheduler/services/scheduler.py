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
            "SELECT project_id, item_type, url FROM videos WHERE id = ?", (video_id,)
        )
        video_row = await cursor.fetchone()
        project_id = int(video_row["project_id"]) if video_row else 1
        item_type = (video_row["item_type"] if video_row else "episode") or "episode"
        item_url = (video_row["url"] if video_row else "") or ""

        # Step 1: YouTube publish step. Type-aware:
        # - episode/short/segment: required. The video MUST exist on YouTube;
        #   we flip it from unlisted -> public.
        # - hook: optional. If item_url looks like a YouTube URL (i.e. the
        #   item was uploaded to YT), flip privacy. Otherwise skip — the
        #   hook's video will be uploaded directly to social media.
        # - standalone: skipped entirely. Items live only in social posts.
        needs_youtube_publish = item_type in ("episode", "short", "segment") or (
            item_type == "hook" and "youtu.be" in item_url
        )
        if needs_youtube_publish:
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
                # Auth-shaped failures need user action. Surface them in
                # the Log so the user sees "Credential for YouTube is
                # invalid — Update" instead of having to find the warning
                # in the server logs.
                err_text = str(e).lower()
                if any(needle in err_text for needle in (
                    "invalid_grant", "unauthorized", "401",
                    "credentials", "refresh", "not authenticated",
                )):
                    await events.record_event(
                        video_id,
                        "credential_invalid",
                        {
                            "scope": "youtube",
                            "account_label": "YouTube channel",
                            "error": str(e),
                        },
                    )
                # Don't fire social posts if a required YouTube publish failed.
                return results
        else:
            # Item type doesn't require YouTube; mark the local row published
            # so the same downstream UI ("video is published") works
            # uniformly for non-YT items.
            await db.execute(
                """UPDATE videos SET status = 'published',
                updated_at = datetime('now') WHERE id = ?""",
                (video_id,),
            )
            await db.commit()
            results["published"] = True
            results["youtube_skipped"] = True
            await events.record_event(
                video_id,
                "published",
                {"platform": None, "item_type": item_type, "url": item_url or None},
            )
            logger.info(
                "Video %s (%s) skipped YouTube publish step", video_id, item_type
            )

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

            # Duplicate guard: even after the user approved this post, if
            # an identical content was sent to the same platform+account
            # in the last 30 days we refuse to post it twice. The user
            # can override by manually re-sending with confirm_dup=true.
            from yt_scheduler.services.social import find_recent_duplicate_post

            dup = await find_recent_duplicate_post(
                platform=post["platform"],
                social_account_id=post.get("social_account_id"),
                content=post.get("content") or "",
                media_path=post.get("media_path"),
                exclude_post_id=post_id,
            )
            if dup is not None:
                await _release_post_to_approved(post_id)
                results["social_results"][platform].append({
                    "post_id": post_id,
                    "status": "skipped",
                    "reason": f"duplicate of post {dup['id']} ({dup.get('posted_at') or ''})",
                })
                logger.warning(
                    "publish_video_job: skipped post %s — duplicate of #%s",
                    post_id, dup.get("id"),
                )
                await events.record_event(
                    video_id,
                    "social_post_skipped_duplicate",
                    {
                        "platform": platform,
                        "social_account_id": post.get("social_account_id"),
                        "previous_post_id": dup.get("id"),
                        "previous_post_url": dup.get("post_url"),
                    },
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
                from yt_scheduler.routers.social_routes import _decode_media_paths
                post_result = await poster.post(
                    post["content"],
                    media_paths=_decode_media_paths(post),
                )
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

    # Pre-flight: never post a YouTube link to a non-public video. With
    # the unified video+post scheduling, post jobs fire independently
    # of publish_video_job — so if YouTube publish failed at fire time
    # (auth expired, quota exhausted), the per-post jobs would still
    # cheerfully announce a video that's actually still unlisted. The
    # social_posts.video_id column is the dependency link; we re-read
    # the video's privacy state right before claiming and bail with an
    # actionable error if YouTube isn't actually public yet.
    video_id = post.get("video_id")
    if video_id:
        cursor = await db.execute(
            "SELECT privacy_status, status FROM videos WHERE id = ?", (video_id,)
        )
        vrow = await cursor.fetchone()
        if vrow and (vrow["privacy_status"] != "public" or vrow["status"] != "published"):
            err = (
                f"YouTube video is still {vrow['privacy_status']!r} (status "
                f"{vrow['status']!r}); refusing to post a link to a non-public "
                "video. Re-publish the video and use Send to retry."
            )
            await db.execute(
                "UPDATE social_posts SET status = 'failed', error = ?, "
                "scheduler_job_id = NULL WHERE id = ?",
                (err, post_id),
            )
            await db.commit()
            await events.record_event(
                video_id,
                "social_post_failed_video_not_public",
                {
                    "platform": post["platform"],
                    "social_account_id": post.get("social_account_id"),
                    "video_privacy": vrow["privacy_status"],
                    "video_status": vrow["status"],
                },
            )
            logger.warning(
                "_send_scheduled_post: post %s blocked — video %s is %s/%s",
                post_id, video_id, vrow["privacy_status"], vrow["status"],
            )
            return

    if not await _claim_post_for_send(post_id):
        # Either it wasn't 'approved' (e.g. user already manually sent
        # or unscheduled it) or another worker beat us to the claim.
        return

    # Duplicate guard. Same rule as publish_video_job: an unattended
    # scheduler must never post identical content twice. Releasing the
    # claim leaves the post in 'approved' so the user can override
    # manually via the route's confirm_dup=true.
    from yt_scheduler.services.social import find_recent_duplicate_post

    dup = await find_recent_duplicate_post(
        platform=post["platform"],
        social_account_id=post.get("social_account_id"),
        content=post.get("content") or "",
        media_path=post.get("media_path"),
        exclude_post_id=post_id,
    )
    if dup is not None:
        await _release_post_to_approved(post_id)
        logger.warning(
            "_send_scheduled_post: skipped post %s — duplicate of #%s",
            post_id, dup.get("id"),
        )
        await events.record_event(
            post["video_id"],
            "social_post_skipped_duplicate",
            {
                "platform": post["platform"],
                "social_account_id": post.get("social_account_id"),
                "previous_post_id": dup.get("id"),
                "previous_post_url": dup.get("post_url"),
            },
        )
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
        from yt_scheduler.routers.social_routes import _decode_media_paths
        result = await poster.post(
            post["content"],
            media_paths=_decode_media_paths(post),
        )
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
        from yt_scheduler.services.social_credentials import (
            format_account_label, get_credential_by_uuid, mark_needs_reauth,
        )

        if isinstance(exc, CredentialAuthError) and exc.uuid:
            await mark_needs_reauth(exc.uuid)
            cred = await get_credential_by_uuid(exc.uuid)
            label = format_account_label(
                post["platform"],
                (cred or {}).get("username"),
            )
            # Record a user-visible event so the Log card surfaces this
            # with an "Update" link instead of just the credentialed
            # service silently dropping posts. Same payload shape used
            # for YouTube auth failures elsewhere — frontend renders a
            # single credential_invalid type for both.
            await events.record_event(
                post["video_id"],
                "credential_invalid",
                {
                    "scope": "social",
                    "platform": post["platform"],
                    "account_label": label,
                    "uuid": exc.uuid,
                    "error": str(exc),
                },
            )
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
    """Schedule a video and all its approved social posts.

    For each approved social post on the video, register a per-post
    APScheduler job staggered using the project's posting settings:
    ``post_video_delay_minutes`` for the offset of the first post,
    ``inter_post_spacing_minutes`` between subsequent posts. Same math
    the Socials Compose page uses.

    The video's schedule owns its per-post jobs — re-scheduling or
    cancelling the video re-baselines all of them. A user who wants a
    custom time for one post should re-time it AFTER scheduling the
    video, and accept that another video re-schedule will overwrite it.

    ``publish_video_job`` still walks any remaining ``approved`` posts
    at fire time as a safety net (e.g. user approved a new post after
    scheduling). The atomic claim prevents double-sends when both
    paths target the same post.
    """
    if publish_at.tzinfo is None:
        publish_at = publish_at.replace(tzinfo=timezone.utc)

    db = await get_db()

    cursor = await db.execute(
        "SELECT project_id FROM videos WHERE id = ?", (video_id,)
    )
    row = await cursor.fetchone()
    project_id = int(row["project_id"]) if row and row["project_id"] is not None else 1

    # The video's schedule owns all of its currently-pending per-post
    # jobs. Detach them all so we can re-attach with the new time and
    # current stagger settings. Hand-retimed posts the user set via the
    # per-post API after the prior schedule_publish are intentionally
    # re-baselined — re-scheduling the video is the explicit "reset
    # everything" action. Already-posted posts have scheduler_job_id
    # NULL'd by _send_scheduled_post on success, so they're skipped.
    pending = await db.execute_fetchall(
        "SELECT id FROM social_posts "
        "WHERE video_id = ? AND scheduler_job_id IS NOT NULL",
        (video_id,),
    )
    for r in pending:
        await cancel_scheduled_post(int(r["id"]))

    job_id = f"publish_{video_id}"
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)
    scheduler.add_job(
        publish_video_job,
        "date",
        run_date=publish_at,
        args=[video_id],
        id=job_id,
        replace_existing=True,
        misfire_grace_time=300,
    )

    await db.execute(
        "UPDATE videos SET publish_at = ?, status = 'scheduled', updated_at = datetime('now') WHERE id = ?",
        (publish_at.isoformat(), video_id),
    )
    await db.commit()

    # Stagger using the project's posting settings. Same math the
    # Socials Compose page uses, so the two scheduling paths produce
    # identical timing for the same posts.
    from datetime import timedelta
    from yt_scheduler.services import project_settings as _ps
    posting = await _ps.get_posting_settings(project_id)
    delay_min = int(posting.get("post_video_delay_minutes", 15) or 0)
    spacing_min = int(posting.get("inter_post_spacing_minutes", 5) or 0)

    # All approved posts get a per-post job. After the cancel above,
    # any scheduled_at values from the prior schedule are nulled, so
    # the WHERE filter on status alone selects the full set.
    approved = await db.execute_fetchall(
        "SELECT id FROM social_posts "
        "WHERE video_id = ? AND status = 'approved' "
        "ORDER BY id",
        (video_id,),
    )
    attached_count = 0
    for i, r in enumerate(approved):
        when = publish_at + timedelta(minutes=delay_min + i * spacing_min)
        try:
            await schedule_social_post(int(r["id"]), when)
            attached_count += 1
        except Exception as exc:
            logger.error(
                "Failed to auto-schedule post %s with video %s: %s",
                r["id"], video_id, exc,
            )

    logger.info(
        "Scheduled video %s for %s + %d posts (delay=%dm, spacing=%dm)",
        video_id, publish_at.isoformat(), attached_count, delay_min, spacing_min,
    )
    return job_id


async def cancel_scheduled_publish(video_id: str) -> bool:
    """Cancel the video's publish job and all of its pending per-post jobs."""
    job_id = f"publish_{video_id}"
    db = await get_db()

    cursor = await db.execute("SELECT publish_at FROM videos WHERE id = ?", (video_id,))
    row = await cursor.fetchone()
    publish_at = row["publish_at"] if row else None

    # The video schedule owns its per-post jobs; cancelling the video
    # cancels them all. Already-posted posts have scheduler_job_id=NULL
    # so they're skipped automatically.
    pending = await db.execute_fetchall(
        "SELECT id FROM social_posts "
        "WHERE video_id = ? AND scheduler_job_id IS NOT NULL",
        (video_id,),
    )
    for r in pending:
        await cancel_scheduled_post(int(r["id"]))

    existing = scheduler.get_job(job_id)
    if not existing and not publish_at:
        return False
    if existing:
        scheduler.remove_job(job_id)

    await db.execute(
        "UPDATE videos SET publish_at = NULL, status = 'ready', updated_at = datetime('now') WHERE id = ?",
        (video_id,),
    )
    await db.commit()
    logger.info(f"Cancelled scheduled publish for {video_id}")
    return True


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
