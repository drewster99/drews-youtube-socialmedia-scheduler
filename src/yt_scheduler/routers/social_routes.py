"""Social media posting routes."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import ai, social, templates as tmpl, youtube
from yt_scheduler.services.scheduler import get_publish_lock
from yt_scheduler.services.transcripts import srt_to_plain_text


from yt_scheduler.services import tiers


def _tier_from_iso_duration(iso: str | None) -> str:
    """Map an ISO-8601 duration (e.g. PT3M31S) to our tier naming."""
    seconds = tiers.parse_iso8601_duration(iso)
    return tiers.tier_for_duration(seconds) or ""

router = APIRouter(prefix="/api/social", tags=["social"])


@router.post("/generate-posts/{video_id}")
async def generate_posts(video_id: str, data: dict | None = None):
    """Generate social media posts for a video using a template.

    Optional body params:
        template_name: Template to use (default: "new_video")
        platforms: List of platform names to generate for (default: all in template)
    """
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    opts = data or {}
    template_name = opts.get("template_name", "new_video")
    requested_platforms = opts.get("platforms")

    video = dict(rows[0])
    template = await tmpl.get_template(template_name)
    if not template:
        raise HTTPException(404, f"Template '{template_name}' not found")

    # Build variables
    tags = json.loads(video.get("tags", "[]"))

    tier = ""
    try:
        yt = youtube.get_video(video_id)
        iso_dur = (yt or {}).get("contentDetails", {}).get("duration")
        tier = _tier_from_iso_duration(iso_dur)
    except Exception:
        tier = ""

    variables = {
        "title": video["title"],
        "url": f"https://youtu.be/{video_id}",
        "description": video.get("description", ""),
        "description_short": (video.get("description", "") or "")[:150],
        "description_medium": (video.get("description", "") or "")[:500],
        "tags": ", ".join(tags),
        "hashtags": " ".join(f"#{t.replace(' ', '')}" for t in tags[:5]),
        "thumbnail_path": video.get("thumbnail_path", ""),
        "tier": tier,
        # Stored as SRT (with timestamps); template authors typically want
        # plain text in their post copy.
        "transcript": srt_to_plain_text(video.get("transcript", "") or ""),
        "user_message": opts.get("user_message", "") or "",
    }

    # Acquire the per-video publish lock to prevent racing with a publish in progress.
    # This ensures we don't delete/recreate posts while the scheduler is sending them.
    lock = get_publish_lock(video_id)
    async with lock:
        # Remove existing drafts for this video so regeneration replaces them
        await db.execute(
            "DELETE FROM social_posts WHERE video_id = ? AND status = 'draft'",
            (video_id,),
        )

        generated = {}
        for platform, config in template["platforms"].items():
            if requested_platforms and platform not in requested_platforms:
                continue

            template_text = config.get("template", "")
            if template_text:
                try:
                    rendered = tmpl.render_template(template_text, variables)
                except Exception as e:
                    rendered = f"[Error generating: {e}]"
            else:
                rendered = ""

            # Store as draft
            await db.execute(
                """INSERT INTO social_posts (video_id, platform, content, media_type, status)
                VALUES (?, ?, ?, ?, 'draft')""",
                (video_id, platform, rendered, config.get("media", "thumbnail")),
            )

            generated[platform] = {
                "content": rendered,
                "media": config.get("media", "thumbnail"),
                "max_chars": config.get("max_chars", 500),
            }

        await db.commit()
        return generated


@router.get("/posts/{video_id}")
async def get_posts(video_id: str):
    """Get all social posts for a video."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE video_id = ? ORDER BY platform",
        (video_id,),
    )
    return [dict(r) for r in rows]


@router.put("/posts/{post_id}")
async def update_post(post_id: int, data: dict):
    """Update a social post (edit content before posting)."""
    db = await get_db()
    updates = []
    params = []

    if "content" in data:
        updates.append("content = ?")
        params.append(data["content"])
    if "status" in data:
        updates.append("status = ?")
        params.append(data["status"])
    if "media_path" in data:
        updates.append("media_path = ?")
        params.append(data["media_path"])

    if updates:
        params.append(post_id)
        await db.execute(
            f"UPDATE social_posts SET {', '.join(updates)} WHERE id = ?", params
        )
        await db.commit()

    return {"status": "ok"}


@router.post("/posts/{post_id}/send")
async def send_post(post_id: int):
    """Send a single social post."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM social_posts WHERE id = ?", (post_id,))
    if not rows:
        raise HTTPException(404, "Post not found")

    post = dict(rows[0])
    poster = social.get_poster(post["platform"])

    if not await poster.is_configured():
        raise HTTPException(400, f"{post['platform']} is not configured. Add credentials in Settings.")

    try:
        result = await poster.post(post["content"], post.get("media_path"))
        await db.execute(
            """UPDATE social_posts
            SET status = 'posted', posted_at = datetime('now'), post_url = ?
            WHERE id = ?""",
            (result.get("url", ""), post_id),
        )
        await db.commit()
        return {"status": "ok", "url": result.get("url", "")}
    except Exception as e:
        await db.execute(
            "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
            (str(e), post_id),
        )
        await db.commit()
        raise HTTPException(500, str(e))


@router.post("/posts/{post_id}/schedule")
async def schedule_post(post_id: int, data: dict):
    """Schedule an individual social post via APScheduler DateTrigger.

    Body: ``{"scheduled_at": "2026-04-25T14:00:00-07:00"}``
    """
    from datetime import datetime as dt, timezone
    from yt_scheduler.services.scheduler import schedule_social_post

    raw = data.get("scheduled_at")
    if not raw:
        raise HTTPException(400, "scheduled_at is required (ISO 8601 datetime)")
    try:
        when = dt.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(400, "Invalid datetime format") from exc
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    if when <= dt.now(timezone.utc):
        raise HTTPException(400, "scheduled_at must be in the future")

    try:
        job_id = await schedule_social_post(post_id, when)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {"status": "ok", "job_id": job_id, "scheduled_at": when.isoformat()}


@router.delete("/posts/{post_id}/schedule")
async def unschedule_post(post_id: int):
    """Cancel a scheduled per-post job."""
    from yt_scheduler.services.scheduler import cancel_scheduled_post

    cancelled = await cancel_scheduled_post(post_id)
    return {"status": "ok", "cancelled": cancelled}


@router.post("/posts/{video_id}/send-all")
async def send_all_posts(video_id: str):
    """Send all approved posts for a video."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE video_id = ? AND status = 'approved'",
        (video_id,),
    )

    results = {}
    for row in rows:
        post = dict(row)
        poster = social.get_poster(post["platform"])

        if not await poster.is_configured():
            results[post["platform"]] = {"status": "skipped", "reason": "not configured"}
            continue

        try:
            result = await poster.post(post["content"], post.get("media_path"))
            await db.execute(
                """UPDATE social_posts
                SET status = 'posted', posted_at = datetime('now'), post_url = ?
                WHERE id = ?""",
                (result.get("url", ""), post["id"]),
            )
            results[post["platform"]] = {"status": "posted", "url": result.get("url", "")}
        except Exception as e:
            await db.execute(
                "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
                (str(e), post["id"]),
            )
            results[post["platform"]] = {"status": "failed", "error": str(e)}

    await db.commit()
    return results
