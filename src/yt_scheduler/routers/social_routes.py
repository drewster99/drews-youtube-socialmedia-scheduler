"""Social media posting routes."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import social, templates as tmpl, youtube
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
        template_name: Template to use (default: "announce_video")
        platforms: List of platform names to generate for (default: all in template)
    """
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    opts = data or {}
    template_name = opts.get("template_name", "announce_video")
    requested_platforms = opts.get("platforms")

    video = dict(rows[0])
    project_id = int(video.get("project_id") or 1)
    template = await tmpl.get_template(template_name, project_id=project_id)
    if not template:
        raise HTTPException(404, f"Template '{template_name}' not found")

    cursor = await db.execute(
        "SELECT platform, social_account_id FROM project_social_defaults "
        "WHERE project_id = ?",
        (project_id,),
    )
    defaults: dict[str, int] = {
        row["platform"]: int(row["social_account_id"])
        for row in await cursor.fetchall()
        if row["social_account_id"] is not None
    }

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

        generated: list[dict] = []
        for slot in template.get("slots", []):
            if slot.get("is_disabled"):
                continue
            platform = slot["platform"]
            if requested_platforms and platform not in requested_platforms:
                continue

            template_text = slot.get("body", "") or ""
            if template_text:
                try:
                    rendered = tmpl.render_template(template_text, variables)
                except Exception as e:
                    rendered = f"[Error generating: {e}]"
            else:
                rendered = ""

            # Routing precedence: slot binding → project default → none
            sa_id = slot.get("social_account_id") or defaults.get(platform)
            media = slot.get("media", "thumbnail")
            max_chars = slot.get("max_chars", 500)

            await db.execute(
                """INSERT INTO social_posts
                       (video_id, platform, content, media_type, status, social_account_id)
                VALUES (?, ?, ?, ?, 'draft', ?)""",
                (video_id, platform, rendered, media, sa_id),
            )

            generated.append({
                "slot_id": slot.get("id"),
                "platform": platform,
                "content": rendered,
                "media": media,
                "max_chars": max_chars,
                "social_account_id": sa_id,
            })

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


async def _resolve_poster_for_post(post: dict) -> social.SocialPoster:
    """Pick the right poster for a row in ``social_posts``.

    Routing order:
    1. ``post.social_account_id`` if set → that exact credential.
    2. Project default for the post's platform (resolved via the video's
       ``project_id``).
    3. The first active credential for the platform (legacy fallback).
    """
    sa_id = post.get("social_account_id")
    if sa_id:
        return await social.get_poster_for_account(int(sa_id))

    db = await get_db()
    cursor = await db.execute(
        "SELECT v.project_id "
        "FROM social_posts sp JOIN videos v ON v.id = sp.video_id "
        "WHERE sp.id = ?",
        (post["id"],),
    )
    row = await cursor.fetchone()
    project_id = int(row["project_id"]) if row is not None else 1

    cursor = await db.execute(
        "SELECT social_account_id FROM project_social_defaults "
        "WHERE project_id = ? AND platform = ?",
        (project_id, post["platform"]),
    )
    default_row = await cursor.fetchone()
    if default_row is not None and default_row["social_account_id"] is not None:
        return await social.get_poster_for_account(int(default_row["social_account_id"]))

    return social.get_poster(post["platform"])


async def _credential_for_post(post: dict) -> dict | None:
    """Resolve which credential row will be used to send this post — for
    the pre-check that fails fast on a known-broken credential. Mirrors
    the routing precedence in :func:`_resolve_poster_for_post` (slot
    binding → project default → first active for platform)."""
    from yt_scheduler.services.social_credentials import (
        get_credential_by_id,
        get_first_active_credential,
    )

    db = await get_db()
    sa_id = post.get("social_account_id")
    if sa_id:
        return await get_credential_by_id(int(sa_id))

    cursor = await db.execute(
        "SELECT v.project_id FROM social_posts sp "
        "JOIN videos v ON v.id = sp.video_id WHERE sp.id = ?",
        (post["id"],),
    )
    row = await cursor.fetchone()
    project_id = int(row["project_id"]) if row is not None else 1
    cursor = await db.execute(
        "SELECT social_account_id FROM project_social_defaults "
        "WHERE project_id = ? AND platform = ?",
        (project_id, post["platform"]),
    )
    default_row = await cursor.fetchone()
    if default_row is not None and default_row["social_account_id"] is not None:
        return await get_credential_by_id(int(default_row["social_account_id"]))

    return await get_first_active_credential(post["platform"])


@router.post("/posts/{post_id}/send")
async def send_post(post_id: int):
    """Send a single social post."""
    from yt_scheduler.services.social_credentials import mark_needs_reauth

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM social_posts WHERE id = ?", (post_id,))
    if not rows:
        raise HTTPException(404, "Post not found")

    post = dict(rows[0])

    # Pre-check: if the credential we'll resolve to is already flagged
    # as needing re-auth, fail fast instead of burning a round trip
    # against the platform that we know will reject us again.
    cred = await _credential_for_post(post)
    if cred is not None and cred.get("needs_reauth"):
        raise HTTPException(
            401,
            f"{post['platform']} credential needs re-authentication. "
            "Reconnect from Settings before retrying.",
        )

    try:
        poster = await _resolve_poster_for_post(post)
    except ValueError as exc:
        raise HTTPException(400, f"{post['platform']}: {exc}") from exc

    if not await poster.is_configured():
        raise HTTPException(
            400, f"{post['platform']} is not configured. Add credentials in Settings."
        )

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
    except social.CredentialAuthError as e:
        if e.uuid:
            await mark_needs_reauth(e.uuid)
        await db.execute(
            "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
            (f"Credential needs re-auth: {e}", post_id),
        )
        await db.commit()
        raise HTTPException(
            401,
            f"{post['platform']} credential needs re-authentication. "
            "Reconnect from Settings.",
        ) from e
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
    from yt_scheduler.services.social_credentials import mark_needs_reauth

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE video_id = ? AND status = 'approved'",
        (video_id,),
    )

    results = {}
    for row in rows:
        post = dict(row)
        try:
            poster = await _resolve_poster_for_post(post)
        except ValueError as exc:
            results[post["platform"]] = {"status": "skipped", "reason": str(exc)}
            continue

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
        except social.CredentialAuthError as e:
            if e.uuid:
                await mark_needs_reauth(e.uuid)
            await db.execute(
                "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
                (f"Credential needs re-auth: {e}", post["id"]),
            )
            results[post["platform"]] = {
                "status": "needs_reauth",
                "error": "Credential needs re-authentication. Reconnect from Settings.",
            }
        except Exception as e:
            await db.execute(
                "UPDATE social_posts SET status = 'failed', error = ? WHERE id = ?",
                (str(e), post["id"]),
            )
            results[post["platform"]] = {"status": "failed", "error": str(e)}

    await db.commit()
    return results
