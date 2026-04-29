"""Social media posting routes."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query

from yt_scheduler.database import get_db
from yt_scheduler.services import events, social, templates as tmpl, youtube
from yt_scheduler.services.scheduler import cancel_scheduled_post, get_publish_lock
from yt_scheduler.services.transcripts import srt_to_plain_text


from yt_scheduler.services import tiers


def _tier_from_iso_duration(iso: str | None) -> str:
    """Map an ISO-8601 duration (e.g. PT3M31S) to our tier naming."""
    seconds = tiers.parse_iso8601_duration(iso)
    return tiers.tier_for_duration(seconds) or ""

router = APIRouter(prefix="/api/social", tags=["social"])


async def _build_render_context(db, video: dict) -> dict:
    """Assemble everything the renderer needs for one item: project row, parent
    item row (if any), images, custom variables at every scope, and the
    self-level built-ins.

    Returned dict keys:
        ``variables``   — merged variable dict for ``templates.render``.
        ``video_path``  — primary video file (for ``{{video}}`` directive).
        ``thumb_path``  — thumbnail file (for ``{{thumbnail}}`` directive).
        ``images``      — pre-sorted ``item_images`` rows for ``{{image:*}}``.
    """
    video_id = video["id"]
    project_id = int(video.get("project_id") or 1)

    proj_rows = await db.execute_fetchall(
        "SELECT id, project_url, youtube_channel_id FROM projects WHERE id = ?",
        (project_id,),
    )
    project_row = dict(proj_rows[0]) if proj_rows else {}

    parent: dict | None = None
    if video.get("parent_item_id"):
        parent_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (video["parent_item_id"],)
        )
        if parent_rows:
            parent = dict(parent_rows[0])

    image_rows = await db.execute_fetchall(
        "SELECT shortname, path, alt_text, order_index FROM item_images "
        "WHERE video_id = ? ORDER BY order_index, id",
        (video_id,),
    )
    images = [dict(r) for r in image_rows]

    global_rows = await db.execute_fetchall(
        "SELECT key, value FROM global_variables"
    )
    global_vars = {r["key"]: r["value"] for r in global_rows}

    project_var_rows = await db.execute_fetchall(
        "SELECT key, value FROM project_variables WHERE project_id = ?",
        (project_id,),
    )
    project_vars = {r["key"]: r["value"] for r in project_var_rows}

    parent_item_vars: dict[str, str] = {}
    if parent is not None:
        parent_var_rows = await db.execute_fetchall(
            "SELECT key, value FROM item_variables WHERE video_id = ?",
            (parent["id"],),
        )
        parent_item_vars = {r["key"]: r["value"] for r in parent_var_rows}

    self_var_rows = await db.execute_fetchall(
        "SELECT key, value FROM item_variables WHERE video_id = ?", (video_id,)
    )
    self_item_vars = {r["key"]: r["value"] for r in self_var_rows}

    tags = json.loads(video.get("tags") or "[]")
    tier = video.get("tier") or ""
    if not tier:
        # Best-effort YouTube duration lookup. Empty string for items without
        # a YT counterpart (standalone, hook-without-YT) — templates that
        # care can use {{tier??}}.
        try:
            yt = youtube.get_video(video_id)
            iso_dur = (yt or {}).get("contentDetails", {}).get("duration")
            tier = _tier_from_iso_duration(iso_dur)
        except Exception:
            tier = ""

    description = video.get("description") or ""

    self_builtins: dict[str, object] = {
        "title": video.get("title") or "",
        "description": description,
        "description_short": description[:150],
        "description_medium": description[:500],
        "tags": ", ".join(tags),
        "hashtags": " ".join(f"#{t.replace(' ', '')}" for t in tags[:5]),
        "thumbnail_path": video.get("thumbnail_path") or "",
        "tier": tier,
        "transcript": srt_to_plain_text(video.get("transcript") or ""),
        # URL family — read directly from columns; resolution is just
        # "self.url -> empty" / "parent.url -> empty" / "project.project_url
        # -> empty". The migration backfilled videos.url for existing rows.
        "url": video.get("url") or "",
        "episode_url": (parent or {}).get("url") or "",
        "project_url": project_row.get("project_url") or "",
    }

    variables = tmpl.merge_variables(
        global_vars=global_vars,
        project_vars=project_vars,
        parent_item_vars=parent_item_vars,
        self_builtins=self_builtins,
        self_item_vars=self_item_vars,
    )

    return {
        "variables": variables,
        "video_path": video.get("video_file_path") or "",
        "thumb_path": video.get("thumbnail_path") or "",
        "images": images,
    }


def _decode_media_paths(post_row: dict) -> list[str]:
    """Pull a media-paths list out of a social_posts row.

    Prefers the new JSON-array column (``media_paths``) populated by the
    post-generation paths; falls back to the legacy single-string column
    (``media_path``) for any row written before migration 010 or by an
    older code path that hasn't been updated. Empty / NULL ⇒ ``[]``.
    """
    raw = post_row.get("media_paths")
    if raw:
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, list):
                return [str(p) for p in decoded if p]
        except (TypeError, ValueError):
            pass
    legacy = post_row.get("media_path")
    return [str(legacy)] if legacy else []


def _legacy_media_for_slot(slot: dict, ctx: dict) -> str | None:
    """Backwards-compat fallback when the template body uses NO media
    directives. Reads the slot's legacy ``media`` field
    (``thumbnail | video | none``) and returns the corresponding path.
    Once Phase E lands and templates have migrated to directives, this
    helper can be removed.
    """
    media_kind = (slot.get("media") or "thumbnail").lower()
    if media_kind == "video":
        return ctx["video_path"] or None
    if media_kind == "thumbnail":
        return ctx["thumb_path"] or None
    return None


@router.post("/generate-posts/{video_id}")
async def generate_posts(
    video_id: str,
    data: dict | None = None,
    confirm_overwrite_scheduled: bool = Query(default=False),
):
    """Generate social media posts for a video using a template.

    Optional body params:
        template_name: Template to use (default: "announce_video")
        platforms: List of platform names to generate for (default: all in template)
        user_message: Free-form text bound to the ``{{user_message}}`` variable.

    If any unsent post for this video is currently scheduled (has a pending
    APScheduler job), the route returns 409 unless ``?confirm_overwrite_scheduled=true``
    is passed. On confirm, scheduled posts are cancelled (APScheduler jobs torn
    down) before the regenerate.
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

    ctx = await _build_render_context(db, video)
    ctx["variables"]["user_message"] = opts.get("user_message", "") or ""

    # Acquire the per-video publish lock to prevent racing with a publish in progress.
    lock = get_publish_lock(video_id)
    async with lock:
        # Refuse to silently nuke scheduled posts. Without this, a regenerate
        # would DELETE 'approved' rows whose APScheduler jobs would then
        # become orphans firing against rows that no longer exist.
        scheduled_rows = await db.execute_fetchall(
            "SELECT id, platform, scheduled_at FROM social_posts "
            "WHERE video_id = ? AND scheduler_job_id IS NOT NULL",
            (video_id,),
        )
        if scheduled_rows and not confirm_overwrite_scheduled:
            raise HTTPException(
                409,
                {
                    "scheduled_overwrite": True,
                    "needs_confirm": True,
                    "scheduled": [
                        {
                            "post_id": int(r["id"]),
                            "platform": r["platform"],
                            "scheduled_at": r["scheduled_at"],
                        }
                        for r in scheduled_rows
                    ],
                },
            )
        for r in scheduled_rows:
            await cancel_scheduled_post(int(r["id"]))

        # Replace unsent posts on regenerate. Posts that already went out
        # ('posted') stay for the audit trail; in-flight scheduled posts
        # ('sending') stay because the per-post scheduler holds its own
        # per-post lock — not the per-video publish lock — so deleting a
        # 'sending' row here would race with an active send.
        await db.execute(
            "DELETE FROM social_posts "
            "WHERE video_id = ? AND status NOT IN ('posted', 'sending')",
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
            media_paths: list[str] = []
            if template_text:
                try:
                    cleaned, media_paths, _alts = tmpl.extract_media_directives(
                        template_text,
                        video_path=ctx["video_path"],
                        thumbnail_path=ctx["thumb_path"],
                        images=ctx["images"],
                    )
                    rendered = tmpl.render(cleaned, ctx["variables"])
                except Exception as e:
                    rendered = f"[Error generating: {e}]"
                    media_paths = []
            else:
                rendered = ""

            # Trim leading/trailing whitespace at the boundary — AI blocks
            # frequently emit a stray leading space or trailing newline,
            # which would render as whitespace on the destination
            # platform and would also cause near-identical content to
            # bypass the dedup check by accident.
            rendered = rendered.strip()

            # If the template body had NO media directives, fall back to
            # the slot's legacy `media` setting (thumbnail / video / none)
            # so existing templates without directives still attach media.
            if not media_paths:
                fallback = _legacy_media_for_slot(slot, ctx)
                if fallback:
                    media_paths = [fallback]

            sa_id = slot.get("social_account_id") or defaults.get(platform)
            media = slot.get("media", "thumbnail")
            max_chars = slot.get("max_chars", 500)
            media_paths_json = json.dumps(media_paths) if media_paths else None
            primary_media = media_paths[0] if media_paths else None

            await db.execute(
                """INSERT INTO social_posts
                       (video_id, platform, content, media_path, media_paths,
                        media_type, status, social_account_id)
                VALUES (?, ?, ?, ?, ?, ?, 'draft', ?)""",
                (
                    video_id, platform, rendered,
                    primary_media, media_paths_json,
                    media, sa_id,
                ),
            )

            generated.append({
                "slot_id": slot.get("id"),
                "platform": platform,
                "content": rendered,
                "media": media,
                "media_paths": media_paths,
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
        # Same trim-at-write rule as generate_posts: leading/trailing
        # whitespace would render as visible whitespace on the
        # destination platform and would silently bypass the dedup
        # matcher on near-identical content.
        updates.append("content = ?")
        params.append((data["content"] or "").strip())
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


def _duplicate_payload(prev: dict, platform: str) -> dict:
    """Shape the 409 body the UI uses to render a 'post anyway?' dialog."""
    snippet = (prev.get("content") or "")
    if len(snippet) > 200:
        snippet = snippet[:200] + "…"
    return {
        "duplicate": True,
        "platform": platform,
        "previous": {
            "id": prev.get("id"),
            "video_id": prev.get("video_id"),
            "posted_at": prev.get("posted_at"),
            "post_url": prev.get("post_url"),
            "content_preview": snippet,
        },
        "needs_confirm": True,
    }


@router.post("/posts/{post_id}/send")
async def send_post(post_id: int, confirm_dup: bool = Query(default=False)):
    """Send a single social post.

    Returns 409 with a duplicate payload if the same (platform, account,
    content) was sent within the last 30 days. Pass ``?confirm_dup=true``
    to override after the user confirms.
    """
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

    if not confirm_dup:
        dup = await social.find_recent_duplicate_post(
            platform=post["platform"],
            social_account_id=cred["id"] if cred else post.get("social_account_id"),
            content=post.get("content") or "",
            media_path=post.get("media_path"),
            exclude_post_id=post_id,
        )
        if dup is not None:
            raise HTTPException(409, _duplicate_payload(dup, post["platform"]))

    try:
        poster = await _resolve_poster_for_post(post)
    except ValueError as exc:
        raise HTTPException(400, f"{post['platform']}: {exc}") from exc

    if not await poster.is_configured():
        raise HTTPException(
            400, f"{post['platform']} is not configured. Add credentials in Settings."
        )

    try:
        result = await poster.post(
            post["content"],
            media_paths=_decode_media_paths(post),
        )
        await db.execute(
            """UPDATE social_posts
            SET status = 'posted', posted_at = datetime('now'), post_url = ?
            WHERE id = ?""",
            (result.get("url", ""), post_id),
        )
        await db.commit()
        from datetime import datetime as _dt, timezone as _tz
        await events.record_event(
            post["video_id"],
            "social_post_published",
            {
                "platform": post["platform"],
                "social_account_id": post.get("social_account_id"),
                "post_url": result.get("url", ""),
                "posted_at": _dt.now(_tz.utc).isoformat(),
            },
        )
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
async def schedule_post(
    post_id: int, data: dict, confirm_dup: bool = Query(default=False)
):
    """Schedule an individual social post via APScheduler DateTrigger.

    Body: ``{"scheduled_at": "2026-04-25T14:00:00-07:00"}``

    Returns 409 with a duplicate payload if an identical post was sent
    in the last 30 days. ``?confirm_dup=true`` overrides.
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

    if not confirm_dup:
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT * FROM social_posts WHERE id = ?", (post_id,)
        )
        if rows:
            post = dict(rows[0])
            cred = await _credential_for_post(post)
            dup = await social.find_recent_duplicate_post(
                platform=post["platform"],
                social_account_id=cred["id"] if cred else post.get("social_account_id"),
                content=post.get("content") or "",
                exclude_post_id=post_id,
            )
            if dup is not None:
                raise HTTPException(409, _duplicate_payload(dup, post["platform"]))

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
async def send_all_posts(
    video_id: str, confirm_dup: bool = Query(default=False)
):
    """Send all approved posts for a video.

    Pre-flights every approved post against the duplicate check. If any
    are duplicates of recent successful sends, returns 409 with a
    ``duplicates`` array — one entry per offending post — so the UI can
    show 'these N posts look like resends, post anyway?'. Pass
    ``?confirm_dup=true`` to skip the check after the user confirms.
    """
    from yt_scheduler.services.social_credentials import mark_needs_reauth

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE video_id = ? AND status = 'approved'",
        (video_id,),
    )

    if not confirm_dup:
        duplicates: list[dict] = []
        for row in rows:
            post = dict(row)
            cred = await _credential_for_post(post)
            dup = await social.find_recent_duplicate_post(
                platform=post["platform"],
                social_account_id=cred["id"] if cred else post.get("social_account_id"),
                content=post.get("content") or "",
                media_path=post.get("media_path"),
                exclude_post_id=int(post["id"]),
            )
            if dup is not None:
                duplicates.append({
                    "post_id": int(post["id"]),
                    **_duplicate_payload(dup, post["platform"]),
                })
        if duplicates:
            raise HTTPException(409, {
                "duplicate": True,
                "duplicates": duplicates,
                "needs_confirm": True,
            })

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
            result = await poster.post(
                post["content"],
                media_paths=_decode_media_paths(post),
            )
            await db.execute(
                """UPDATE social_posts
                SET status = 'posted', posted_at = datetime('now'), post_url = ?
                WHERE id = ?""",
                (result.get("url", ""), post["id"]),
            )
            results[post["platform"]] = {"status": "posted", "url": result.get("url", "")}
            from datetime import datetime as _dt, timezone as _tz
            await events.record_event(
                video_id,
                "social_post_published",
                {
                    "platform": post["platform"],
                    "social_account_id": post.get("social_account_id"),
                    "post_url": result.get("url", ""),
                    "posted_at": _dt.now(_tz.utc).isoformat(),
                },
            )
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
