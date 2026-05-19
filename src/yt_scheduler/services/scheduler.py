"""Background scheduler for scheduled publishing, comment moderation, and caption checks."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from yt_scheduler.config import CAPTION_CHECK_INTERVAL_MINUTES, COMMENT_CHECK_INTERVAL_MINUTES
from yt_scheduler.database import get_db
from yt_scheduler.services import events, moderation, transcripts as transcript_service, youtube
from yt_scheduler.services.auth import set_active_project
from yt_scheduler.services.projects import get_project_by_id

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

        # Bind the active project so YouTube wrappers below pick the
        # right OAuth credential — without this, a scheduled publish in
        # any non-default project would publish via the default project's
        # YouTube credentials (wrong channel, or 404).
        project_row = await get_project_by_id(project_id)
        if project_row:
            set_active_project(project_row["slug"])

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
                # APScheduler runs jobs on the asyncio loop; the sync
                # google-api-python-client call below would block every
                # other concurrent request for the round-trip. to_thread
                # parks it on a worker.
                await asyncio.to_thread(
                    youtube.update_video_metadata,
                    video_id, privacy_status="public",
                )
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
                from yt_scheduler.services.social import decode_media_paths
                post_result = await poster.post(
                    post["content"],
                    media_paths=decode_media_paths(post),
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
        # Only privacy_status decides whether viewers can see the YouTube
        # link. The lifecycle `status` column drifts off 'published'
        # whenever the user flips privacy via the metadata dropdown
        # (video_routes update_video updates privacy_status without
        # bumping status), so gating on it produces false failures with
        # the contradictory "video is public, not public" message.
        if vrow and vrow["privacy_status"] != "public":
            err = (
                f"YouTube video is still {vrow['privacy_status']!r}; "
                "refusing to post a link to a non-public video. "
                "Re-publish the video and use Send to retry."
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
        from yt_scheduler.services.social import decode_media_paths
        result = await poster.post(
            post["content"],
            media_paths=decode_media_paths(post),
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
    await restore_pending_auto_actions()


_AUTO_ACTION_RESUME_WINDOW_HOURS = 24


async def restore_pending_auto_actions() -> None:
    """Re-fire ``_run_chain`` for *recently-touched* videos whose auto-action
    chain may not have completed before the last shutdown.

    The chain itself has idempotency gates (skip transcribe if transcript
    exists, skip description if one exists, skip socials if they exist), so
    re-firing is cheap when there's no work to do and recovers in-flight
    work when there is.

    We *intentionally* limit to videos whose ``updated_at`` falls within
    the last ``_AUTO_ACTION_RESUME_WINDOW_HOURS`` window. Without that
    cap, a project with hundreds of historic imports lacking transcripts
    would spawn hundreds of concurrent Whisper jobs on every server boot
    — desirable for in-flight recovery, catastrophic for cold starts.
    Older videos can be resumed by the user re-triggering the chain
    explicitly (re-import / manual transcribe button).
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        f"""SELECT id, project_id, imported_from_youtube FROM videos
        WHERE (transcript IS NULL OR transcript = ''
               OR description_generated_at IS NULL)
        AND auto_action_state IS NULL
        AND COALESCE(updated_at, created_at) >=
            datetime('now', '-{_AUTO_ACTION_RESUME_WINDOW_HOURS} hours')
        """
    )
    if not rows:
        return

    from yt_scheduler.services.auto_actions import run_post_create_actions
    restored = 0
    for row in rows:
        try:
            project_id = int(row["project_id"]) if row["project_id"] is not None else 1
            source = "import" if row["imported_from_youtube"] else "upload"
            await run_post_create_actions(row["id"], project_id, source)
            restored += 1
        except Exception as exc:
            logger.warning(
                "Could not restore auto-actions for %s: %s", row["id"], exc
            )
    if restored:
        logger.info(
            "Restored %d pending auto-action chain(s) from the last %dh",
            restored, _AUTO_ACTION_RESUME_WINDOW_HOURS,
        )

    # Resume Promo Videos chains that were mid-step at shutdown. Same
    # window cap as the standard chain, so an idle box can't burn its
    # OAuth tokens / quota replaying historic chains. Promo state lives
    # in ``videos.auto_action_state``; we re-enter at whatever step the
    # column held when the server stopped (the step's idempotency gate
    # makes the resume cheap if the work is already done).
    promo_rows = await db.execute_fetchall(
        f"""SELECT id, project_id, auto_action_state FROM videos
        WHERE auto_action_state IS NOT NULL
        AND auto_action_state NOT LIKE 'failed:%'
        AND auto_action_state != 'ready'
        AND COALESCE(updated_at, created_at) >=
            datetime('now', '-{_AUTO_ACTION_RESUME_WINDOW_HOURS} hours')
        """
    )
    if not promo_rows:
        return
    from yt_scheduler.services.auto_actions import (
        PROMO_STATE_TRANSCRIBING, retry_promo_step,
    )
    promo_restored = 0
    for row in promo_rows:
        state = row["auto_action_state"]
        # ``probing`` / ``uploading`` / ``generating_title`` happen
        # before or during the INSERT and shouldn't be reachable from
        # the videos table (no row exists yet for pre-INSERT states).
        # If we see one of those, treat it as "transcribing" so the
        # row gets its chain finished.
        if state in {"generating_title", "uploading", "probing"}:
            state = PROMO_STATE_TRANSCRIBING
        try:
            await retry_promo_step(row["id"], state)
            promo_restored += 1
        except Exception as exc:
            logger.warning(
                "Could not resume Promo chain for %s: %s", row["id"], exc,
            )
    if promo_restored:
        logger.info(
            "Resumed %d in-flight Promo chain(s) from the last %dh",
            promo_restored, _AUTO_ACTION_RESUME_WINDOW_HOURS,
        )


async def _iter_project_slugs() -> list[tuple[int, str]]:
    """Yield ``(project_id, slug)`` for every project — caption/comment jobs
    must rebind YouTube credentials per project, since each project has its
    own OAuth grant against its own channel."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, slug FROM projects ORDER BY id"
    )
    return [(int(r["id"]), r["slug"]) for r in rows]


async def check_captions_job() -> None:
    """Check for videos waiting on captions, per-project."""
    db = await get_db()
    for project_id, slug in await _iter_project_slugs():
        set_active_project(slug)

        rows = await db.execute_fetchall(
            "SELECT id, title FROM videos WHERE project_id = ? AND status = 'uploaded'",
            (project_id,),
        )
        for row in rows:
            video_id = row["id"]
            try:
                # Same rationale as elsewhere — sync YouTube SDK calls
                # inside an AsyncIOScheduler job block the loop until
                # the round-trip returns.
                captions = await asyncio.to_thread(youtube.list_captions, video_id)
                auto_captions = [
                    c for c in captions
                    if c["snippet"].get("trackKind") == "ASR"
                ]
                if auto_captions:
                    # Store SRT canonically — preserves segment timestamps for
                    # YouTube round-trip + chapter detection.
                    caption_text = await asyncio.to_thread(
                        youtube.download_caption, auto_captions[0]["id"], fmt="srt",
                    )
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
                    logger.info(f"Captions ready for video {video_id} (project {slug}): {row['title']}")
            except Exception as e:
                logger.warning(f"Failed to check captions for {video_id} (project {slug}): {e}")


async def moderate_comments_job() -> None:
    """Run comment moderation on all tracked videos, per-project."""
    for project_id, slug in await _iter_project_slugs():
        set_active_project(slug)

        try:
            results = await moderation.check_all_videos(project_id=project_id)
        except Exception as e:
            logger.warning(
                "Comment moderation job failed for project %s: %s", slug, e
            )
            continue

        actions_by_video = results.get("actions_by_video", {})
        for video_id, actions in actions_by_video.items():
            if actions:
                logger.info(
                    "Moderated %d comments on video %s (project %s)",
                    len(actions), video_id, slug,
                )
        if results.get("checked"):
            logger.info(
                "Moderation tick (project %s): checked %s, matched %s",
                slug, results.get("checked", 0), results.get("matched", 0),
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
                "Moderation (project %s): YouTube auth failures on %d video(s); "
                "the project's YouTube credential likely needs re-auth. "
                "First error: %s",
                slug, len(auth_failures), auth_failures[0]["error"],
            )
        elif errors:
            logger.warning(
                "Moderation (project %s): %d video(s) errored (non-auth); first: %s",
                slug, len(errors), errors[0]["error"],
            )


# How often to sweep social credentials looking for tokens to pre-emptively
# refresh, and the lookahead window — anything whose access token expires
# within this many seconds gets renewed now rather than lapsing into
# "needs re-auth" between posts.
_TOKEN_REFRESH_INTERVAL_MINUTES = 20
_TOKEN_REFRESH_WINDOW_SECS = 45 * 60


async def refresh_social_tokens_job() -> None:
    """Pre-emptively refresh social access tokens nearing expiry.

    Walks every (non-deleted) social credential; for those whose access token
    is within ``_TOKEN_REFRESH_WINDOW_SECS`` of expiry, runs the platform's
    refresh flow (serialised per-credential against the post-time refresh, so
    the two never present the same rotating refresh token concurrently).
    Credentials are handled independently — one bad one doesn't stop the rest.
    A *terminal* refresh failure flags the credential ``needs_reauth`` (same as
    the post path); a transient/network error is left for the next sweep.
    """
    from yt_scheduler.services.social import CredentialAuthError, get_poster_for_uuid
    from yt_scheduler.services.social_credentials import list_credentials, mark_needs_reauth

    try:
        creds = await list_credentials()
    except Exception as exc:
        logger.warning("Token refresh sweep: could not list credentials: %s", exc)
        return

    renewed = 0
    for cred in creds:
        uuid = cred.get("uuid")
        platform = cred.get("platform")
        label = cred.get("label") or f"{platform}:{(uuid or '')[:8]}"
        if not uuid or not platform:
            continue
        try:
            poster = await get_poster_for_uuid(platform, uuid)
        except Exception as exc:
            logger.warning("Token refresh sweep: skipping %s — %s", label, exc)
            continue
        try:
            if await poster.refresh_if_stale(window_secs=_TOKEN_REFRESH_WINDOW_SECS):
                renewed += 1
                logger.info("Token refresh sweep: renewed %s", label)
        except CredentialAuthError as exc:
            logger.warning("Token refresh sweep: %s needs re-auth — %s", label, exc)
            try:
                await mark_needs_reauth(uuid)
            except Exception as me:
                logger.warning("Token refresh sweep: failed to flag %s: %s", uuid, me)
        except Exception as exc:
            # Transient (network, etc.) — don't flag re-auth on flakiness;
            # the next sweep retries.
            logger.warning("Token refresh sweep: transient error on %s — %s", label, exc)
    if renewed:
        logger.info("Token refresh sweep: renewed %d credential(s)", renewed)


# F2: per-social-post debug traces (templates.render output) are kept
# only as long as they're useful for the user to inspect from the ⓘ
# button on the detail page. 24h is the spec'd retention; this job
# evicts anything older. Cascade-delete from social_posts also handles
# the case where a post is deleted before the trace expires.
_TRACE_TTL_HOURS = 24


async def prune_social_post_traces_job() -> None:
    db = await get_db()
    cur = await db.execute(
        f"DELETE FROM social_post_traces "
        f"WHERE created_at < datetime('now', '-{_TRACE_TTL_HOURS} hours')"
    )
    await db.commit()
    if getattr(cur, "rowcount", 0):
        logger.info(
            "Trace pruning: removed %d social_post_traces older than %dh",
            cur.rowcount, _TRACE_TTL_HOURS,
        )


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
    scheduler.add_job(
        refresh_social_tokens_job,
        "interval",
        minutes=_TOKEN_REFRESH_INTERVAL_MINUTES,
        id="refresh_social_tokens",
        replace_existing=True,
    )
    scheduler.add_job(
        prune_social_post_traces_job,
        "interval",
        hours=1,
        id="prune_social_post_traces",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        f"Scheduler started (captions every {cap_mins}m, moderation every {mod_mins}m, "
        f"social-token refresh every {_TOKEN_REFRESH_INTERVAL_MINUTES}m, "
        "trace pruning hourly)"
    )


def stop_scheduler() -> None:
    """Stop the background scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)


# --- Promo batch scheduling ----------------------------------------------

# Hardcoded v1 defaults (TIER_WORKFLOW.md). Each tier has its own
# independent chain anchored to the parent's publish time: ``initial``
# is the gap between the parent and the chronologically-first child
# of that tier; ``subsequent`` is the gap between consecutive children
# in that tier.
DEFAULT_PROMO_DELAYS: dict[str, dict[str, timedelta]] = {
    "hook":    {"initial": timedelta(hours=4),  "subsequent": timedelta(hours=99)},
    "short":   {"initial": timedelta(hours=18), "subsequent": timedelta(days=6)},
    "segment": {"initial": timedelta(days=3),   "subsequent": timedelta(days=9)},
}

# Quota guard for the Promo screen. Each child upload chain costs
# ≈ 100 (insert) + 50 (metadata update) = 150 YouTube quota units. The
# daily allowance is 10,000 — this threshold (25%) is what we surface
# as a "heads up" banner. Doesn't block scheduling — just warns.
PROMO_BATCH_QUOTA_WARN_FRACTION = 0.25
_YT_DAILY_QUOTA = 10_000
_YT_PROMO_UNITS_PER_VIDEO = 150


def promo_quota_for(child_count: int) -> int:
    """Estimated YouTube quota units for a batch upload of N promos."""
    return child_count * _YT_PROMO_UNITS_PER_VIDEO


def _decode_tags(raw: str | None) -> list:
    import json as _json
    if not raw:
        return []
    try:
        decoded = _json.loads(raw)
    except (ValueError, TypeError):
        return []
    if isinstance(decoded, list):
        return list(decoded)
    return []


def is_ready_for_schedule(video: dict) -> tuple[bool, list[str]]:
    """Per the spec: transcript present (any source, not whitespace),
    description present, ≥3 tags, and a thumbnail (custom or YouTube
    auto-generated).

    Returns ``(ready, missing)`` — missing is a short list of human-
    readable reasons used by the review modal's per-row readiness chip.
    """
    missing: list[str] = []
    transcript = (video.get("transcript") or "").strip()
    if not transcript:
        missing.append("transcript")
    description = (video.get("description") or "").strip()
    # The Promo upload step inserts an exact placeholder string until
    # _promo_step_description overwrites it. Match on equality so a
    # user-edited description happening to mention "Description pending
    # generation..." still counts as ready.
    placeholder = "Description pending generation."
    if not description or description == placeholder:
        missing.append("description")
    tags = _decode_tags(video.get("tags"))
    if len(tags) < 3:
        missing.append("tags (need ≥ 3)")
    has_custom_thumb = bool(video.get("thumbnail_path"))
    has_youtube_thumb = (video.get("thumbnail_source") or "").lower() == "youtube"
    if not (has_custom_thumb or has_youtube_thumb):
        missing.append("thumbnail")
    return (not missing, missing)


def _parse_iso_datetime(value) -> datetime | None:
    """Lenient ISO-8601 parser used to read ``videos.publish_at`` /
    inbound ``parent_publish_at`` parameters. Returns ``None`` on any
    parse failure so the caller can fall back to "not scheduled"
    behaviour rather than crashing the batch."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    # ``datetime.fromisoformat`` accepts trailing ``Z`` only in 3.11+.
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


async def compute_promo_batch_preview(
    parent_id: str,
    *,
    parent_publish_at: datetime | None = None,
    delays: dict[str, dict[str, timedelta]] | None = None,
) -> dict:
    """Dry-run: compute what :func:`schedule_promo_batch` would write.

    Returned shape:

    ```
    {
        "parent": {
            "id": ..., "title": ..., "publish_at": ..., "status": ...,
        },
        "rows": [
            {"video_id", "title", "item_type", "tier",
             "target_time": ISO,
             "ready": bool, "missing": [...]}, ...
        ],
        "total_span": ISO | null,    # latest target_time or null
        "warnings": [...],            # quota warning, parent readiness, etc.
        "anchor_publish_at": ISO,    # the time chains anchored against
    }
    ```

    Keeps the same per-tier independent-chain logic as the writing
    path so the review modal renders an accurate preview.
    """
    delays = delays or DEFAULT_PROMO_DELAYS
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", (parent_id,)
    )
    if not rows:
        raise ValueError(f"Parent video {parent_id} not found")
    parent = dict(rows[0])
    if parent.get("parent_item_id"):
        raise ValueError(
            f"Video {parent_id} is itself a child; only one level of "
            "parenting is supported"
        )

    parent_publish = (
        parent_publish_at or _parse_iso_datetime(parent.get("publish_at"))
    )
    parent_already_published = (parent.get("status") or "") == "published"

    warnings: list[str] = []
    parent_ready, parent_missing = is_ready_for_schedule(parent)
    if not parent_already_published and not parent_ready:
        warnings.append(
            "Parent video isn't ready: missing " + ", ".join(parent_missing)
        )

    children_rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE parent_item_id = ? "
        "ORDER BY created_at ASC",
        (parent_id,),
    )
    children = [dict(r) for r in children_rows]
    eligible = [c for c in children if (c.get("status") or "") != "published"]

    quota_units = promo_quota_for(len(eligible))
    if quota_units >= _YT_DAILY_QUOTA * PROMO_BATCH_QUOTA_WARN_FRACTION:
        warnings.append(
            f"This batch consumes ~{quota_units} YouTube quota units "
            f"({(quota_units / _YT_DAILY_QUOTA) * 100:.0f}% of the daily "
            f"10,000-unit allowance)."
        )

    rows_out: list[dict] = []
    latest_target: datetime | None = None
    if parent_publish is None:
        # Parent hasn't been scheduled yet AND the caller didn't supply
        # a tentative anchor; we can't compute per-child target times,
        # so return the children as "ready check only" entries.
        for child in eligible:
            ready, missing = is_ready_for_schedule(child)
            rows_out.append({
                "video_id": child["id"],
                "title": child.get("title") or "",
                "item_type": child.get("item_type") or child.get("tier") or "short",
                "tier": child.get("tier") or "",
                "target_time": None,
                "ready": ready,
                "missing": missing,
            })
        return {
            "parent": _public_parent(parent, parent_publish),
            "rows": rows_out,
            "total_span": None,
            "warnings": warnings + [
                "Parent has no publish time set; pick one above to "
                "compute per-child target times."
            ],
            "anchor_publish_at": None,
        }

    # Group eligible children by item_type so each tier-chain anchors
    # independently from the parent. ``item_type`` is the user-editable
    # bucket; ``tier`` is the duration-derived one. We chain on
    # ``item_type`` so manual overrides land in the picked bucket.
    chains: dict[str, list[dict]] = {"segment": [], "short": [], "hook": []}
    for child in eligible:
        bucket = (child.get("item_type") or child.get("tier") or "short").lower()
        if bucket in chains:
            chains[bucket].append(child)
        else:
            chains.setdefault(bucket, []).append(child)

    for tier_name, group in chains.items():
        if tier_name not in DEFAULT_PROMO_DELAYS:
            continue
        cfg = delays.get(tier_name, DEFAULT_PROMO_DELAYS[tier_name])
        scheduled_in_tier = [
            (c, _parse_iso_datetime(c.get("publish_at")))
            for c in group
            if c.get("publish_at")
        ]
        scheduled_in_tier = [(c, t) for c, t in scheduled_in_tier if t]
        unscheduled = [c for c in group if not c.get("publish_at")]

        # Always include already-scheduled children at their existing
        # times — schedule-all is non-destructive to manually-set or
        # previously-batched schedules. Newly-added (unscheduled)
        # children chain off the latest existing time + subsequent.
        for child, existing in scheduled_in_tier:
            rows_out.append(_preview_row(child, existing))
            if latest_target is None or existing > latest_target:
                latest_target = existing

        if not unscheduled:
            continue

        if scheduled_in_tier:
            current = max(t for _c, t in scheduled_in_tier)
            for child in unscheduled:
                current = current + cfg["subsequent"]
                rows_out.append(_preview_row(child, current))
                if latest_target is None or current > latest_target:
                    latest_target = current
        else:
            # Fresh chain: first child anchors at parent + initial,
            # the rest follow at +subsequent each.
            current = parent_publish + cfg["initial"]
            rows_out.append(_preview_row(unscheduled[0], current))
            if latest_target is None or current > latest_target:
                latest_target = current
            for child in unscheduled[1:]:
                current = current + cfg["subsequent"]
                rows_out.append(_preview_row(child, current))
                if latest_target is None or current > latest_target:
                    latest_target = current

    rows_out.sort(key=lambda r: r["target_time"] or "")
    return {
        "parent": _public_parent(parent, parent_publish),
        "rows": rows_out,
        "total_span": latest_target.isoformat() if latest_target else None,
        "warnings": warnings,
        "anchor_publish_at": parent_publish.isoformat(),
    }


def _public_parent(parent: dict, parent_publish: datetime | None) -> dict:
    return {
        "id": parent.get("id"),
        "title": parent.get("title") or "",
        "publish_at": parent_publish.isoformat() if parent_publish else None,
        "status": parent.get("status"),
        "ready": is_ready_for_schedule(parent)[0],
        "missing": is_ready_for_schedule(parent)[1],
    }


def _preview_row(child: dict, target: datetime) -> dict:
    ready, missing = is_ready_for_schedule(child)
    return {
        "video_id": child["id"],
        "title": child.get("title") or "",
        "item_type": child.get("item_type") or child.get("tier") or "short",
        "tier": child.get("tier") or "",
        "target_time": target.isoformat(),
        "ready": ready,
        "missing": missing,
    }


# --- Cascade rescheduling -------------------------------------------------

# When the user reschedules a video, related rows may need to move too:
#
# * Parent reschedule  -> auto-anchored children shift by the same delta
#   (publish_at_manual = 0 children). Manual-override children stay put.
# * Child reschedule   -> same-tier siblings AFTER this child shift by
#   the same delta, again only the auto-anchored ones.
#
# A re-entry guard prevents A-causes-B-causes-A loops: when one cascade
# fires another schedule_publish, the inner call passes cascade=False
# so the inner schedule doesn't trigger more cascades.


async def cascade_children_on_parent_shift(
    parent_id: str, old_publish_at: datetime, new_publish_at: datetime,
) -> list[str]:
    """Move every auto-anchored child of ``parent_id`` by the same delta
    the parent just moved. Skips children with ``publish_at_manual = 1``
    and children whose status is ``published``.

    Returns the list of child ids that were rescheduled.
    """
    if old_publish_at.tzinfo is None:
        old_publish_at = old_publish_at.replace(tzinfo=timezone.utc)
    if new_publish_at.tzinfo is None:
        new_publish_at = new_publish_at.replace(tzinfo=timezone.utc)
    delta = new_publish_at - old_publish_at
    if delta == timedelta():
        return []

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, publish_at FROM videos "
        "WHERE parent_item_id = ? "
        "AND publish_at IS NOT NULL "
        "AND (publish_at_manual IS NULL OR publish_at_manual = 0) "
        "AND (status IS NULL OR status != 'published')",
        (parent_id,),
    )
    shifted: list[str] = []
    for r in rows:
        child = dict(r)
        existing = _parse_iso_datetime(child.get("publish_at"))
        if existing is None:
            continue
        target = existing + delta
        await schedule_publish(child["id"], target)
        # schedule_publish stamps status='scheduled' + writes publish_at;
        # we have to re-mark publish_at_manual = 0 since schedule_publish
        # itself doesn't touch the column. Without this, a user-driven
        # cascade would leave the column as it was before — fine in this
        # path but cleaner to be explicit.
        await db.execute(
            "UPDATE videos SET publish_at_manual = 0 WHERE id = ?",
            (child["id"],),
        )
        await db.commit()
        shifted.append(child["id"])
    return shifted


async def cascade_siblings_on_shift(
    child_id: str, old_publish_at: datetime, new_publish_at: datetime,
) -> list[str]:
    """Move same-tier siblings scheduled AFTER ``child_id`` by the same
    delta. Skips manually-overridden siblings and published siblings.

    Two siblings are "same-tier" when they share ``parent_item_id`` and
    ``item_type`` (so a user-forced tier reclassification correctly
    routes the cascade through the user's pick rather than the
    duration-derived one).
    """
    if old_publish_at.tzinfo is None:
        old_publish_at = old_publish_at.replace(tzinfo=timezone.utc)
    if new_publish_at.tzinfo is None:
        new_publish_at = new_publish_at.replace(tzinfo=timezone.utc)
    delta = new_publish_at - old_publish_at
    if delta == timedelta():
        return []

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT parent_item_id, item_type FROM videos WHERE id = ?",
        (child_id,),
    )
    if not rows:
        return []
    me = dict(rows[0])
    parent_id = me.get("parent_item_id")
    item_type = me.get("item_type") or ""
    if not parent_id:
        return []

    sibling_rows = await db.execute_fetchall(
        "SELECT id, publish_at FROM videos "
        "WHERE parent_item_id = ? AND item_type = ? "
        "AND id != ? "
        "AND publish_at IS NOT NULL "
        "AND publish_at > ? "
        "AND (publish_at_manual IS NULL OR publish_at_manual = 0) "
        "AND (status IS NULL OR status != 'published')",
        (parent_id, item_type, child_id, old_publish_at.isoformat()),
    )
    shifted: list[str] = []
    for r in sibling_rows:
        sibling = dict(r)
        existing = _parse_iso_datetime(sibling.get("publish_at"))
        if existing is None:
            continue
        target = existing + delta
        await schedule_publish(sibling["id"], target)
        await db.execute(
            "UPDATE videos SET publish_at_manual = 0 WHERE id = ?",
            (sibling["id"],),
        )
        await db.commit()
        shifted.append(sibling["id"])
    return shifted


async def apply_user_reschedule(
    video_id: str, new_publish_at: datetime,
) -> dict:
    """User-driven reschedule entry point. Wraps :func:`schedule_publish`,
    stamps ``publish_at_manual = 1`` on the target row, and fires the
    appropriate cascade based on whether the target is a primary
    (parent shift) or a child (sibling shift).

    Returns ``{"cascaded_children": [...], "cascaded_siblings": [...]}``.
    """
    if new_publish_at.tzinfo is None:
        new_publish_at = new_publish_at.replace(tzinfo=timezone.utc)

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, publish_at, parent_item_id FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise ValueError(f"Video {video_id} not found")
    row = dict(rows[0])
    old_publish_at = _parse_iso_datetime(row.get("publish_at"))
    parent_item_id = row.get("parent_item_id")

    await schedule_publish(video_id, new_publish_at)
    await db.execute(
        "UPDATE videos SET publish_at_manual = 1 WHERE id = ?",
        (video_id,),
    )
    await db.commit()

    cascaded_children: list[str] = []
    cascaded_siblings: list[str] = []
    if old_publish_at is not None and old_publish_at != new_publish_at:
        if parent_item_id:
            cascaded_siblings = await cascade_siblings_on_shift(
                video_id, old_publish_at, new_publish_at,
            )
        else:
            cascaded_children = await cascade_children_on_parent_shift(
                video_id, old_publish_at, new_publish_at,
            )
    return {
        "cascaded_children": cascaded_children,
        "cascaded_siblings": cascaded_siblings,
    }


async def schedule_promo_batch(
    parent_id: str,
    *,
    parent_publish_at: datetime | None = None,
    delays: dict[str, dict[str, timedelta]] | None = None,
) -> dict:
    """Schedule every eligible child of ``parent_id`` using
    :func:`compute_promo_batch_preview` for the math, then writing
    each child via :func:`schedule_publish` (which re-registers the
    APScheduler job AND re-stages any per-post social jobs).

    Optionally also schedules the parent when it isn't already
    published and ``parent_publish_at`` is supplied.

    Raises :class:`ValueError` when the parent is missing / itself a
    child / has no readiness-OK children to schedule.
    """
    preview = await compute_promo_batch_preview(
        parent_id, parent_publish_at=parent_publish_at, delays=delays,
    )
    if not preview["rows"] and parent_publish_at is None:
        raise ValueError("No promo children eligible for scheduling")
    if not all(r["ready"] for r in preview["rows"]):
        not_ready = [r["video_id"] for r in preview["rows"] if not r["ready"]]
        raise ValueError(
            "Some children aren't ready to schedule: " + ", ".join(not_ready)
        )
    parent_info = preview["parent"]
    if parent_publish_at and not parent_info.get("ready"):
        raise ValueError(
            "Parent isn't ready: missing "
            + ", ".join(parent_info.get("missing") or [])
        )

    db = await get_db()

    scheduled: list[dict] = []
    if parent_publish_at is not None and parent_info.get("status") != "published":
        await schedule_publish(parent_id, parent_publish_at)
        # Schedule-all batches are auto-anchored; mark the parent so a
        # future cascade still moves it.
        await db.execute(
            "UPDATE videos SET publish_at_manual = 0 WHERE id = ?",
            (parent_id,),
        )
        await db.commit()
        scheduled.append({
            "video_id": parent_id, "publish_at": parent_publish_at.isoformat(),
        })

    for row in preview["rows"]:
        target = _parse_iso_datetime(row["target_time"])
        if not target:
            continue
        await schedule_publish(row["video_id"], target)
        # Batch-scheduled children are "auto", not "manual"; the
        # cascade routines will sweep them on the next parent move.
        await db.execute(
            "UPDATE videos SET publish_at_manual = 0 WHERE id = ?",
            (row["video_id"],),
        )
        await db.commit()
        scheduled.append({
            "video_id": row["video_id"],
            "publish_at": target.isoformat(),
        })

    return {"scheduled": scheduled, "warnings": preview["warnings"]}
