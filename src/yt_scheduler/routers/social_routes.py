"""Social media posting routes."""

from __future__ import annotations

import asyncio
import json
import re

from fastapi import APIRouter, HTTPException, Query

from yt_scheduler.config import media_filename, media_url
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

# {{video}} on a Threads slot: the Threads API only accepts media by public URL,
# which a localhost app can't provide, so we skip the slot instead of silently
# posting text-only. (Future: host the file on R2 and use the VIDEO container.)
_VIDEO_DIRECTIVE_RE = re.compile(r"\{\{\s*video\s*\}\}", re.IGNORECASE)
# After media directives are stripped and AI blocks resolved, a leftover
# {{name}} is an unresolved variable. (Required {{name!}} would have raised;
# {{name??default}} would have substituted the default — so plain {{name}} is
# the only thing that survives the renderer.)
_UNRESOLVED_VAR_RE = re.compile(r"\{\{(\w+)\}\}")
# Body-side scan for the URL family (url / episode_url / project_url) so we
# can warn when a slot referenced one of them but the resolved value was
# empty. Matches the three placeholder forms: {{name}}, {{name!}},
# {{name??default}} — all three render to the empty string when the
# variable is present in the dict with an empty value.
_URL_VAR_REF_RE = re.compile(r"\{\{(url|episode_url|project_url)(?:!|\?\?[^}]*)?\}\}")


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
        from yt_scheduler.services.auth import set_active_project
        from yt_scheduler.services.projects import get_project_by_id
        try:
            project_for_yt = await get_project_by_id(project_id)
            if project_for_yt:
                set_active_project(project_for_yt["slug"])
            # Sync google-api-python-client call; offload off the loop
            # to keep concurrent generate-posts requests responsive.
            yt = await asyncio.to_thread(youtube.get_video, video_id)
            iso_dur = (yt or {}).get("contentDetails", {}).get("duration")
            tier = _tier_from_iso_duration(iso_dur)
        except Exception:
            tier = ""

    description = video.get("description") or ""

    # URL family — read directly from columns; resolution is just
    # "self.url -> empty" / "parent.url -> empty" / "project.project_url
    # -> empty". Migration 010 backfilled videos.url for pre-existing rows
    # and migration 015 covers any imports that were created in the window
    # before services/imports.py started setting it on INSERT.
    url_value = video.get("url") or ""
    episode_url_value = (parent or {}).get("url") or ""
    project_url_value = project_row.get("project_url") or ""

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
        "url": url_value,
        "episode_url": episode_url_value,
        "project_url": project_url_value,
    }

    # Names that resolved to empty string. The renderer treats these as
    # ordinary {{name}} hits and silently substitutes "", which means a
    # template body that referenced {{url}} would render with no URL and
    # the user would never know. generate_posts uses this set to emit a
    # warning when a slot body actually mentioned one of these names.
    empty_url_keys: set[str] = set()
    if not url_value:
        empty_url_keys.add("url")
    if not episode_url_value:
        empty_url_keys.add("episode_url")
    if not project_url_value:
        empty_url_keys.add("project_url")

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
        "empty_url_keys": empty_url_keys,
    }


from yt_scheduler.services.social import decode_media_paths as _decode_media_paths  # noqa: E402


def _post_public(row: dict) -> dict:
    """Project a ``social_posts`` row for the API: expose attached media as
    ``/media/...`` URLs + display filenames instead of absolute disk paths.
    The raw ``media_path`` / ``media_paths`` columns are kept out of the
    response (the ``PUT`` endpoint still accepts them in request bodies)."""
    out = dict(row)
    paths = _decode_media_paths(row)
    out["media_urls"] = [media_url(p) for p in paths]
    out["media_filenames"] = [media_filename(p) for p in paths]
    out.pop("media_path", None)
    out.pop("media_paths", None)
    return out


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
        unresolved: ``{name: "empty" | "literal"}`` — how to handle template
            variables that have no value. ``"empty"`` substitutes an empty
            string; ``"literal"`` (or omitting a name) leaves ``{{name}}`` in
            the post. Passing this key (even ``{}``) acknowledges the
            unresolved set so generation proceeds.
        unresolved_ack: ``true`` to proceed even with unresolved variables
            (treating them all as literal).

    Returns ``{"posts": [...], "warnings": [...]}``.

    409 responses (nothing is written/deleted before either gate):
      * ``{"unresolved": ["name", ...]}`` — template has variables with no
        value and ``unresolved`` / ``unresolved_ack`` was not provided.
      * ``{"scheduled_overwrite": true, ...}`` — regenerating would cancel
        pending scheduled posts; pass ``?confirm_overwrite_scheduled=true``.
    """
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    opts = data or {}
    template_name = opts.get("template_name", "announce_video")
    requested_platforms = opts.get("platforms")
    # G1 — slot-level filtering. When the caller knows specific
    # template_slot ids it wants to (re)generate (the E1 per-slot
    # picker does), they're sent here. A template with two Mastodon
    # slots routed to different accounts can now be partially
    # regenerated (only one slot), where ``requested_platforms`` alone
    # could only express "all Mastodon slots or none". Optional;
    # callers passing only ``platforms`` keep working unchanged.
    requested_slot_ids_raw = opts.get("slot_ids")
    requested_slot_ids: set[int] | None
    if requested_slot_ids_raw is None:
        requested_slot_ids = None
    else:
        try:
            requested_slot_ids = {int(x) for x in requested_slot_ids_raw}
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, "slot_ids must be a list of integers") from exc
    unresolved_choices = opts.get("unresolved") or {}
    forced_empty = {name: "" for name, choice in unresolved_choices.items() if choice == "empty"}
    unresolved_ack = bool(opts.get("unresolved_ack")) or ("unresolved" in opts)

    video = dict(rows[0])
    project_id = int(video.get("project_id") or 1)
    template = await tmpl.get_template(template_name, project_id=project_id)
    if not template:
        raise HTTPException(404, f"Template '{template_name}' not found")

    # Resolve the project's editable default for ``{{ai: …}}`` blocks once
    # per (re)generate. Each ``tmpl.render`` call below uses this so user
    # edits in Project Settings → LLM prompt templates take effect on the
    # next generate. Atomic per generate — a mid-flight edit is picked up
    # on the next call, not partway through this one.
    from yt_scheduler.services import prompts as prompt_service
    default_ai_system = (await prompt_service.get_prompt_with_fallback(
        "ai_block_default_system_prompt", project_id=project_id,
    ))["system"]

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
        # Resolve the platform set we're actually about to (re)generate so
        # both the scheduled-overwrite guard and the DELETE below stay
        # scoped to those platforms. Without this, a partial-platform
        # regenerate (e.g. user only ticks Twitter in the picker) would
        # nuke approved/draft rows on the OTHER platforms — silently
        # losing prior approvals and confusing the user with a "will
        # cancel N scheduled posts" dialog about platforms they aren't
        # touching.
        # G1: build the set of slot_ids we'll actually (re)generate.
        # The scheduled-overwrite guard and the DELETE-before-regen
        # below scope to these slot_ids when available, falling back
        # to platform-based scoping only for legacy rows where
        # social_posts.slot_id is NULL.
        slot_ids_to_regen: set[int] = set()
        platforms_to_regen: set[str] = set()
        for slot in template.get("slots", []):
            if slot.get("is_disabled"):
                continue
            p = slot["platform"]
            sid = slot.get("id")
            if requested_platforms and p not in requested_platforms:
                continue
            if requested_slot_ids is not None and (sid is None or int(sid) not in requested_slot_ids):
                continue
            platforms_to_regen.add(p)
            if sid is not None:
                slot_ids_to_regen.add(int(sid))
        if not platforms_to_regen:
            return {"posts": [], "warnings": []}
        platform_placeholders = ",".join("?" * len(platforms_to_regen))
        slot_placeholders = (
            ",".join("?" * len(slot_ids_to_regen)) if slot_ids_to_regen else ""
        )
        slot_params = tuple(sorted(slot_ids_to_regen))

        # Match clause shared between the scheduled-overwrite SELECT
        # and the DELETE below. When we know slot_ids: a row matches
        # if its slot_id is in the set OR (slot_id IS NULL AND its
        # platform is in the set) — the second branch is the legacy-
        # row fallback. When no slot_ids are known: match on platform
        # alone (back-compat with callers that don't send slot_ids).
        if slot_ids_to_regen:
            match_clause = (
                f"(slot_id IN ({slot_placeholders}) "
                f" OR (slot_id IS NULL AND platform IN ({platform_placeholders})))"
            )
            match_params = (*slot_params, *sorted(platforms_to_regen))
        else:
            match_clause = f"platform IN ({platform_placeholders})"
            match_params = tuple(sorted(platforms_to_regen))

        # Scheduled-overwrite guard goes BEFORE the (potentially slow, AI-bearing)
        # render pass so a "confirm and retry" round-trip doesn't re-run AI blocks.
        # It's non-destructive (a SELECT + raise), so checking it early is safe.
        scheduled_rows = await db.execute_fetchall(
            f"SELECT id, platform, scheduled_at FROM social_posts "
            f"WHERE video_id = ? AND {match_clause} "
            f"AND scheduler_job_id IS NOT NULL",
            (video_id, *match_params),
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

        # Render every slot up-front (no DB writes yet) so we can surface
        # unresolved-variable problems *before* deleting any existing posts.
        #
        # F4: across-slot parallelism. ``tmpl.render`` is synchronous and
        # internally fires one Claude round-trip per ``{{ai: ...}}`` block —
        # so a 5-slot template with one inner+outer block per slot was
        # serializing 10 sequential round-trips. We now collect the
        # per-slot "needs rendering" closures up-front, fire them
        # concurrently with asyncio.gather (each in a worker thread so
        # the anthropic SDK's blocking IO doesn't stall the loop), then
        # post-process results back in the request task. Within a single
        # slot the nested-{{ai:}} walker stays sequential — that's a
        # data dependency we can't parallelize.
        warnings: list[str] = []
        unresolved_names: set[str] = set()
        empty_url_refs: set[str] = set()

        # Pre-render pass — early skips, URL-ref scan, build the list of
        # slots we'll actually render. No AI calls, no DB writes here.
        render_targets: list[dict] = []
        for slot in template.get("slots", []):
            if slot.get("is_disabled"):
                continue
            platform = slot["platform"]
            sid_for_filter = slot.get("id")
            if requested_platforms and platform not in requested_platforms:
                continue
            if requested_slot_ids is not None and (
                sid_for_filter is None
                or int(sid_for_filter) not in requested_slot_ids
            ):
                continue

            body = slot.get("body", "") or ""

            # Scan the body for url-family references before rendering so we
            # know which ones got referenced even if they substitute to "".
            url_refs_in_body = {m.group(1) for m in _URL_VAR_REF_RE.finditer(body)}
            for name in url_refs_in_body & ctx.get("empty_url_keys", set()):
                empty_url_refs.add(name)

            if platform == "threads" and _VIDEO_DIRECTIVE_RE.search(body):
                warnings.append(
                    "Threads slot skipped — {{video}} attachments aren't supported "
                    "on Threads yet (its API posts text only)."
                )
                continue

            slot_max = slot.get("max_chars")
            if not slot_max:
                raise HTTPException(
                    500, f"Slot {slot.get('id')} ({platform}) has no max_chars"
                )

            cleaned_body = ""
            media_paths: list[str] = []
            if body:
                try:
                    cleaned_body, media_paths, _alts = tmpl.extract_media_directives(
                        body,
                        video_path=ctx["video_path"],
                        thumbnail_path=ctx["thumb_path"],
                        images=ctx["images"],
                    )
                except Exception as e:
                    # Media-directive extraction failed before any AI
                    # call would have run — record an error trace and
                    # carry on with an empty render. Same shape the old
                    # exception path produced.
                    error_trace: list[dict] = [{"kind": "error", "message": str(e)}]
                    render_targets.append({
                        "slot": slot,
                        "platform": platform,
                        "slot_max": int(slot_max),
                        "cleaned_body": "",
                        "media_paths": [],
                        "rendered": f"[Error generating: {e}]",
                        "trace": error_trace,
                        "skip_render": True,
                    })
                    continue

            render_targets.append({
                "slot": slot,
                "platform": platform,
                "slot_max": int(slot_max),
                "cleaned_body": cleaned_body,
                "media_paths": media_paths,
                "rendered": None,  # filled in by the parallel render
                "trace": [] if body else None,
                "skip_render": not body,
            })

        # Parallel render across slots — each tmpl.render runs in its own
        # worker thread so anthropic's sync client doesn't block the
        # event loop. Slots already filled (early-skip / extract error
        # path) are passed through untouched.
        async def _render_target(target: dict) -> dict:
            if target.get("skip_render"):
                if target.get("rendered") is None:
                    target["rendered"] = ""
                return target
            slot_vars = {
                **ctx["variables"], **forced_empty,
                "max_chars": str(target["slot_max"]),
            }
            try:
                rendered = await asyncio.to_thread(
                    tmpl.render,
                    target["cleaned_body"],
                    slot_vars,
                    default_system_prompt=default_ai_system,
                    trace=target["trace"],
                )
            except Exception as e:
                rendered = f"[Error generating: {e}]"
                target["media_paths"] = []
                if target["trace"] is not None:
                    target["trace"].append({"kind": "error", "message": str(e)})
            target["rendered"] = rendered
            return target

        await asyncio.gather(*(_render_target(t) for t in render_targets))

        # Post-render pass — strip whitespace, scan for unresolved
        # variables, apply per-slot media fallback, and assemble the
        # ``prepared`` list the INSERT loop below consumes. Synchronous
        # again because nothing here hits Claude.
        prepared: list[dict] = []
        for target in render_targets:
            slot = target["slot"]
            platform = target["platform"]
            rendered = (target.get("rendered") or "").strip()
            unresolved_names.update(_UNRESOLVED_VAR_RE.findall(rendered))

            media_paths = target.get("media_paths") or []
            if not media_paths:
                fallback = _legacy_media_for_slot(slot, ctx)
                if fallback:
                    media_paths = [fallback]

            if platform == "threads" and media_paths:
                warnings.append(
                    "Threads slot will post text-only — Threads can't attach media yet, "
                    "so its image/video attachment was dropped."
                )
                media_paths = []

            prepared.append({
                "slot": slot,
                "platform": platform,
                "rendered": rendered,
                "media_paths": media_paths,
                "max_chars": int(target["slot_max"]),
                "trace": target.get("trace"),
            })

        # Unresolved-variable gate — bail before any destructive DB op.
        if unresolved_names and not unresolved_ack:
            raise HTTPException(409, {"unresolved": sorted(unresolved_names)})

        # URL-family soft warnings. These are not gating (the post still
        # renders fine, just with no link) — they exist so the user sees
        # WHY their post came out without the URL they expected. One
        # message per offending name, regardless of how many slots tripped.
        _EMPTY_URL_HINTS = {
            "url": (
                "{{url}} resolved to empty for this item — set the URL on "
                "the item or, for an imported YouTube video, the import "
                "should have populated it automatically (file a bug if "
                "this is one)."
            ),
            "episode_url": (
                "{{episode_url}} resolved to empty — this item has no "
                "parent episode, or the parent has no URL set."
            ),
            "project_url": (
                "{{project_url}} resolved to empty — set the project URL "
                "in Project settings."
            ),
        }
        for name in sorted(empty_url_refs):
            warnings.append(_EMPTY_URL_HINTS[name])

        for r in scheduled_rows:
            await cancel_scheduled_post(int(r["id"]))

        # Replace unsent posts on the slots being regenerated. Posts
        # that already went out ('posted') stay for the audit trail;
        # in-flight scheduled posts ('sending') stay because the per-post
        # scheduler holds its own per-post lock — not the per-video
        # publish lock — so deleting a 'sending' row here would race with
        # an active send. Approved rows on OTHER slots also stay, which
        # is the bug fix: a single-slot regenerate must not delete the
        # user's previously-approved rows on neighboring slots.
        await db.execute(
            f"DELETE FROM social_posts "
            f"WHERE video_id = ? AND {match_clause} "
            f"AND status NOT IN ('posted', 'sending')",
            (video_id, *match_params),
        )

        generated: list[dict] = []
        for item in prepared:
            slot = item["slot"]
            platform = item["platform"]
            sa_id = slot.get("social_account_id") or defaults.get(platform)
            media = slot.get("media", "thumbnail")
            media_paths = item["media_paths"]
            media_paths_json = json.dumps(media_paths) if media_paths else None
            primary_media = media_paths[0] if media_paths else None

            slot_id_for_insert = slot.get("id")
            cur = await db.execute(
                """INSERT INTO social_posts
                       (video_id, platform, content, media_path, media_paths,
                        media_type, status, social_account_id, max_chars,
                        slot_id)
                VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)""",
                (
                    video_id, platform, item["rendered"],
                    primary_media, media_paths_json,
                    media, sa_id, item["max_chars"],
                    int(slot_id_for_insert) if slot_id_for_insert is not None else None,
                ),
            )
            post_id = cur.lastrowid

            # Persist the per-slot debug trace (F2). Cascade-deleted with
            # the post; pruned by the scheduler job to keep ~24h worth.
            slot_trace = item.get("trace")
            if slot_trace and post_id is not None:
                await db.execute(
                    "INSERT INTO social_post_traces (post_id, trace_json) "
                    "VALUES (?, ?)",
                    (int(post_id), json.dumps(slot_trace)),
                )

            generated.append({
                "slot_id": slot.get("id"),
                "platform": platform,
                "content": item["rendered"],
                "media": media,
                "media_urls": [media_url(p) for p in media_paths],
                "media_filenames": [media_filename(p) for p in media_paths],
                "max_chars": item["max_chars"],
                "social_account_id": sa_id,
            })

        await db.commit()
        return {"posts": generated, "warnings": warnings}


@router.get("/posts/{post_id}/trace")
async def get_post_trace(post_id: int):
    """Return the debug-log trace for a generated social post (F3).

    Pruned hourly to ~24h by services.scheduler. 404 when the row has
    been pruned, cascade-deleted, or never had a trace recorded (e.g.
    a post created before F2 landed).
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT trace_json, created_at FROM social_post_traces WHERE post_id = ?",
        (post_id,),
    )
    if not rows:
        raise HTTPException(404, "No trace for this post (may have expired).")
    row = dict(rows[0])
    try:
        trace = json.loads(row["trace_json"])
    except json.JSONDecodeError:
        trace = []
    return {
        "post_id": post_id,
        "created_at": row["created_at"],
        "trace": trace,
    }


@router.get("/posts/{video_id}")
async def get_posts(video_id: str):
    """Get all social posts for a video."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM social_posts WHERE video_id = ? ORDER BY platform",
        (video_id,),
    )
    return [_post_public(dict(r)) for r in rows]


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
    if "media_paths" in data:
        # Accept a list (re-attach a different set) or null/[] (clear). Keep
        # the legacy single-string ``media_path`` column in sync so old read
        # paths and the duplicate matcher don't see a stale attachment.
        raw = data["media_paths"]
        cleaned = [p for p in raw if p] if isinstance(raw, list) else []
        updates.append("media_paths = ?")
        params.append(json.dumps(cleaned) if cleaned else None)
        updates.append("media_path = ?")
        params.append(cleaned[0] if cleaned else None)

    if updates:
        params.append(post_id)
        await db.execute(
            f"UPDATE social_posts SET {', '.join(updates)} WHERE id = ?", params
        )
        await db.commit()

    return {"status": "ok"}


@router.post("/posts/{post_id}/shorten")
async def shorten_post(post_id: int, data: dict | None = None):
    """Ask the model to shorten a generated post to at most ``target_chars``
    (defaults to the post's ``max_chars``), preserving meaning and every URL.

    Applies the result in place and returns
    ``{"content": <new>, "previous": <old>, "char_count": <int>, "warning": <str|null>}``
    so the caller can offer an Undo.
    """
    from yt_scheduler.services import ai
    from yt_scheduler.services import prompts as prompt_service

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM social_posts WHERE id = ?", (post_id,))
    if not rows:
        raise HTTPException(404, "Post not found")
    post = dict(rows[0])
    old = (post.get("content") or "").strip()
    if not old:
        raise HTTPException(400, "Nothing to shorten — the post is empty.")

    opts = data or {}
    target = opts.get("target_chars")
    try:
        target = int(target) if target is not None else (post.get("max_chars") or 280)
    except (TypeError, ValueError):
        target = post.get("max_chars") or 280
    if target < 1:
        raise HTTPException(400, "target_chars must be a positive number")

    # Resolve the project the post belongs to (via its video) so we read the
    # right per-project prompt customisation. Posts that predate the
    # project layer fall back to the default project.
    project_id = 1
    video_id = post.get("video_id")
    if video_id:
        v_rows = await db.execute_fetchall(
            "SELECT project_id FROM videos WHERE id = ?", (video_id,),
        )
        if v_rows and v_rows[0]["project_id"]:
            project_id = int(v_rows[0]["project_id"])

    seed = await prompt_service.get_prompt_with_fallback(
        "shorten_post_prompt", project_id=project_id,
    )
    # Render through the same engine the rest of the app uses so the user
    # can edit the prompt and rely on the same {{variable}} semantics. The
    # body and system both go through render() — system prompts may also
    # want {{target_chars}} or future variables baked in.
    variables: dict[str, object] = {
        "target_chars": str(target),
        "post_text": old,
    }
    user_prompt = tmpl.render(seed["body"], variables)
    system_prompt = (
        tmpl.render(seed["system"], variables) if seed["system"] else None
    )
    try:
        new = ai.call_ai_block(
            user_prompt,
            system=system_prompt,
            max_tokens=512,
        ).strip()
    except Exception as e:
        raise HTTPException(502, f"Couldn't shorten the post: {e}")
    if not new:
        raise HTTPException(502, "The model returned an empty result.")

    orig_urls = set(re.findall(r"https?://\S+", old))
    warning = None
    if orig_urls and not orig_urls.issubset(set(re.findall(r"https?://\S+", new))):
        warning = "A link may have changed — double-check before posting."

    await db.execute("UPDATE social_posts SET content = ? WHERE id = ?", (new, post_id))
    await db.commit()
    return {"content": new, "previous": old, "char_count": len(new), "warning": warning}


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
        return {"status": "ok", "url": result.get("url", ""), "warning": result.get("warning")}
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
            results[post["platform"]] = {
                "status": "posted", "url": result.get("url", ""), "warning": result.get("warning"),
            }
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
