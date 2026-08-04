"""Social media posting routes."""

from __future__ import annotations

import asyncio
import json
import logging
import re

from fastapi import APIRouter, HTTPException, Query

from yt_scheduler.config import (
    is_managed_media_path,
    media_filename,
    media_url,
    require_managed_media_paths,
)
from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.models.social_post import mark_failed, mark_posted
from yt_scheduler.services import events, send_failures, social, templates as tmpl
from yt_scheduler.services.social import decode_media_paths as _decode_media_paths
from yt_scheduler.services.scheduler import cancel_scheduled_post, get_publish_lock
from yt_scheduler.services.render_context import (
    RenderContextError,
    build_render_context as _build_render_context,
)



# Statuses a client is permitted to set via PUT /api/social/posts/{id}.
# Statuses set exclusively by server-side operations (sending, posted, failed)
# are intentionally excluded — only draft↔approved transitions are client-driven.
_CLIENT_WRITABLE_POST_STATUSES: frozenset[str] = frozenset({"draft", "approved"})

# Statuses DELETE /api/social/posts/{id} will remove. Neither has anything
# pending: a draft was never sent, and a failed send already cleared its
# scheduling columns. The three excluded statuses each have something in flight
# or on the record — see remove_post.
_REMOVABLE_POST_STATUSES: frozenset[str] = frozenset({"draft", "failed"})



router = APIRouter(prefix="/api/social", tags=["social"])

logger = logging.getLogger(__name__)

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
    project_id = video.get("project_id")
    if project_id in (None, 0):
        raise HTTPException(409, "Video has no project_id (data integrity error).")
    project_id = int(project_id)
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

    # The builder is a service now, so it raises a service error; this route
    # is the layer that decides what that means over HTTP.
    try:
        ctx = await _build_render_context(db, video)
    except RenderContextError as exc:
        raise HTTPException(409, str(exc)) from exc
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

            slot_max = slot.get("max_chars")
            if not slot_max:
                raise HTTPException(
                    500, f"Slot {slot.get('id')} ({platform}) has no max_chars"
                )

            # The exact variable dict this slot will render with — sections
            # must resolve against it (not just ctx) so {{#user_message}} /
            # forced-empty choices behave identically in both passes.
            slot_vars = {
                **ctx["variables"], **forced_empty,
                "max_chars": str(slot_max),
            }

            # Whether the author declared media in the raw body — checked
            # before section resolution so a directive dropped by a false
            # section still counts as "author took manual control" and the
            # legacy media fallback below stays disabled for this slot.
            body_declared_media = tmpl.body_declares_media(body)

            # Resolve {{#name}}/{{^name}} sections BEFORE the media pass so
            # a directive inside a dropped section never attaches media, and
            # BEFORE the URL-reference scan so {{#url}}-guarded references
            # to an empty URL don't produce a false warning.
            if body:
                try:
                    body = tmpl.resolve_sections(body, slot_vars)
                except tmpl.SectionTagError as e:
                    error_trace: list[dict] = [{"kind": "error", "message": str(e)}]
                    render_targets.append({
                        "slot": slot,
                        "platform": platform,
                        "slot_max": int(slot_max),
                        "slot_vars": slot_vars,
                        "body_declared_media": body_declared_media,
                        "cleaned_body": "",
                        "media_paths": [],
                        "rendered": f"[Error generating: {e}]",
                        "trace": error_trace,
                        "skip_render": True,
                    })
                    continue

            # Scan the body for url-family references before rendering so we
            # know which ones got referenced even if they substitute to "".
            url_refs_in_body = {m.group(1) for m in _URL_VAR_REF_RE.finditer(body)}
            for name in url_refs_in_body & ctx.get("empty_url_keys", set()):
                empty_url_refs.add(name)

            if not social.platform_accepts_attached_media(platform) and \
                    _VIDEO_DIRECTIVE_RE.search(body):
                warnings.append(
                    f"{platform} slot skipped — {{{{video}}}} attachments aren't "
                    f"supported on {platform} (its API posts text only)."
                )
                continue

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
                    error_trace = [{"kind": "error", "message": str(e)}]
                    render_targets.append({
                        "slot": slot,
                        "platform": platform,
                        "slot_max": int(slot_max),
                        "slot_vars": slot_vars,
                        "body_declared_media": body_declared_media,
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
                "slot_vars": slot_vars,
                "body_declared_media": body_declared_media,
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
            try:
                rendered = await asyncio.to_thread(
                    tmpl.render,
                    target["cleaned_body"],
                    target["slot_vars"],
                    default_system_prompt=default_ai_system,
                    trace=target["trace"],
                    # Once the user has acknowledged unresolved names via the
                    # 409 flow below, honor that choice by leaving them
                    # literal; the first attempt is strict so the 409 can
                    # list every undefined name up-front.
                    on_undefined="literal" if unresolved_ack else "error",
                )
            except tmpl.UndefinedTemplateVariables as e:
                # Feed the names into the 409 unresolved gate below — this
                # is a user-fixable template/variable mismatch, not a
                # render crash.
                target["undefined_names"] = list(e.names)
                rendered = ""
                target["media_paths"] = []
                if target["trace"] is not None:
                    target["trace"].append({"kind": "error", "message": str(e)})
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
            unresolved_names.update(target.get("undefined_names") or [])
            unresolved_names.update(_UNRESOLVED_VAR_RE.findall(rendered))

            media_paths = target.get("media_paths") or []
            # The legacy per-slot media fallback only applies when the body
            # never declared media at all. A directive that a dropped
            # {{#…}} section removed means "media only under this
            # condition" — don't resurrect it.
            if not media_paths and not target.get("body_declared_media"):
                fallback = _legacy_media_for_slot(slot, ctx)
                if fallback:
                    media_paths = [fallback]

            if media_paths and not social.platform_accepts_attached_media(platform):
                warnings.append(
                    f"{platform} slot will post text-only — {platform} can't attach "
                    "media, so its image/video attachment was dropped."
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
        # Rendering (to_thread) already happened; this whole regenerate —
        # delete old drafts + insert the new slot posts and their traces — is
        # one atomic DB-only critical section.
        async with write_transaction() as db:
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


@router.get("/failed-posts")
async def list_failed_posts():
    """Social posts whose most recent send attempt failed, newest first.

    Powers the app-wide failed-sends banner (``static/js/failed-sends-banner.js``,
    loaded by ``base.html`` on every page): a failed send must stay visible from
    wherever the user is standing until it is retried, skipped or deleted, not
    flash once in a toast. The ``social_posts`` table is the single source of
    truth — a post the user has given up on becomes ``'skipped'``, which leaves
    this list because it is genuinely no longer failing, not because it is hidden.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """SELECT sp.id, sp.video_id, sp.platform, sp.error, sp.failed_at,
                  sp.social_account_id, sp.smart_queue_item_id,
                  -- Both drive what the row can offer: whether the automatic
                  -- retry is already working on it, and whether Skip needs to
                  -- say that the schedule is a separate decision.
                  sp.retryable, sp.retry_until,
                  v.title AS video_title,
                  p.slug AS project_slug
           FROM social_posts sp
           JOIN videos v ON v.id = sp.video_id
           JOIN projects p ON p.id = v.project_id
           WHERE sp.status = 'failed'
           -- By when it FAILED, not by sp.id — id is creation order, so a post
           -- written weeks ago and a post written minutes ago sort by the wrong
           -- thing entirely. That is how a five-day-old failure came to head the
           -- banner while four failures from the same afternoon sat below it,
           -- which is the exact confusion migration 044 added failed_at to end.
           -- NULL means a pre-migration row, i.e. old by definition, so it goes
           -- last; `IS NULL` rather than NULLS LAST keeps this portable.
           ORDER BY sp.failed_at IS NULL, sp.failed_at DESC, sp.id DESC""",
    )
    posts = []
    for row in rows:
        post = dict(row)
        # The server vends the ready page URL: the video-detail route 404s
        # unless the slug actually owns the video, so the banner must not
        # guess a slug.
        slug = post.pop("project_slug")
        post["page_url"] = f"/projects/{slug}/videos/{post['video_id']}"
        posts.append(post)
    return posts


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
        status = data["status"]
        if status not in _CLIENT_WRITABLE_POST_STATUSES:
            raise HTTPException(
                400,
                f"Invalid status {status!r}. "
                f"Allowed values: {sorted(_CLIENT_WRITABLE_POST_STATUSES)}",
            )
        updates.append("status = ?")
        params.append(status)
    if "media_path" in data:
        # Reject any path outside the managed media dir: these become files we
        # upload to social platforms, so an absolute/traversal path would let a
        # client publish an arbitrary readable file (keys, secrets) to an account.
        single = data["media_path"]
        if single and not is_managed_media_path(single):
            raise HTTPException(
                400,
                f"media_path must be inside the managed media directory: {single!r}",
            )
        updates.append("media_path = ?")
        params.append(single)
    if "media_paths" in data:
        # Accept a list (re-attach a different set) or null/[] (clear). Keep
        # the legacy single-string ``media_path`` column in sync so old read
        # paths and the duplicate matcher don't see a stale attachment.
        raw = data["media_paths"]
        cleaned = [p for p in raw if p] if isinstance(raw, list) else []
        try:
            require_managed_media_paths(cleaned)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        updates.append("media_paths = ?")
        params.append(json.dumps(cleaned) if cleaned else None)
        updates.append("media_path = ?")
        params.append(cleaned[0] if cleaned else None)

    if updates:
        params.append(post_id)
        async with write_transaction() as db:
            await db.execute(
                f"UPDATE social_posts SET {', '.join(updates)} WHERE id = ?", params
            )

    return {"status": "ok"}


@router.post("/posts/{post_id}/skip")
async def skip_failed_post(post_id: int) -> dict:
    """Give up on a failed send: mark it ``'skipped'``.

    Not a "dismissed" flag. A post the user is never sending is not a failed
    post that happens to be hidden — it is a post that has been given up on, and
    ``'skipped'`` is what this codebase already calls that
    (``smart_queue_disposition``'s ``remove`` sets exactly this). It leaves the
    failed-sends banner because it is genuinely no longer failing, not because
    it is filtered out, so there is one state with one meaning.

    The error text, ``failed_at`` and the content are all kept: this records a
    decision, it does not rewrite what happened.

    Only a failed post can be skipped here — on any other status this endpoint
    would be silently changing something the user is not looking at.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT status, smart_queue_item_id FROM social_posts WHERE id = ?",
        (post_id,),
    )
    if not rows:
        raise HTTPException(404, "Post not found")

    async with write_transaction() as wdb:
        # The status predicate is in the UPDATE, not only in the check above.
        # Read-then-write left a window in which the retry job could claim the
        # row into 'sending' and start posting, after which this write turned it
        # 'skipped' — and `pre_claimed` meant the sender never re-checked
        # ownership, so it sent the post the user had just skipped.
        cursor = await wdb.execute(
            "UPDATE social_posts SET status = 'skipped', "
            # Clearing the retry plan is the point: a skipped post must not be
            # picked up by the retry job a minute later.
            "retryable = 0, next_retry_at = NULL, retry_until = NULL "
            "WHERE id = ? AND status = 'failed'",
            (post_id,),
        )
    if not cursor.rowcount:
        raise HTTPException(
            409,
            f"Post {post_id} is '{rows[0]['status']}', not 'failed' \u2014 only a "
            f"failed send can be skipped. It may have started sending; refresh "
            f"to see its current state.",
        )

    # Skipping the LAST live posting of a smart-queue item leaves the item
    # 'scheduled' with nothing that could ever send \u2014 which the queue reads as
    # "still pending", so it counts toward the scheduled total forever AND
    # permanently blocks that video from being selected again. That is the same
    # zombie `smart_queue_reconcile_handlers._retire_emptied_items` exists to
    # prevent, and `smart_queue_disposition._remove` retires the item for
    # exactly this reason. Before the banner gained Skip there was no way to
    # reach a queue-owned post from here, so it could not arise.
    item_id = rows[0]["smart_queue_item_id"]
    if item_id is not None:
        from yt_scheduler.services.smart_queue_reconcile_handlers import (
            _retire_emptied_items,
        )

        await _retire_emptied_items(
            [int(item_id)], "every posting was skipped from the failed-sends banner"
        )
    return {"id": post_id, "status": "skipped"}


@router.delete("/posts/{post_id}")
async def remove_post(post_id: int):
    """Remove a draft or failed social post.

    Those two are removable because neither has anything pending: a draft was
    never sent, and every failure path already cleared the row's scheduling
    columns, so nothing will pick it up again. For a failed post outside a smart
    queue this is the only exit at all — the app-wide failed-sends banner is
    driven by ``status``, so the row IS the notification, and nothing retries it.

    The other three stay: ``posted`` is the audit trail of something the world
    has already seen, ``sending`` is mid-flight, and ``approved`` may have a live
    per-post DateTrigger behind it.

    A post owned by a smart queue item is refused outright, whatever its status.
    The queue derives an item's state from its posting rows (see
    ``smart_queue.list_queues``: ``LEFT JOIN social_posts`` … ``ELSE i.state``),
    so deleting the row would leave the item bucketed as 'scheduled' forever
    with nothing left to post, and its video never eligible to be queued again.
    Those items already have a purpose-built exit — the queue's missed-postings
    screen, whose ``remove`` disposition moves the ITEM to 'removed' — and that
    state machine belongs to the queue, not to this endpoint.

    The status guard is repeated inside the DELETE rather than trusting the
    read: a send that flips the row out of a removable status in between must
    win, and a conditional DELETE lets it, where a plain delete-by-id would drop
    a post that is already on the wire. Queue ownership needs no such re-check —
    ``smart_queue_item_id`` is only ever written when the row is inserted.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT p.status, p.platform, p.video_id, p.content, p.error, "
        "       p.scheduler_job_id, p.smart_queue_item_id, i.queue_id "
        "  FROM social_posts p "
        "  LEFT JOIN smart_queue_items i ON i.id = p.smart_queue_item_id "
        " WHERE p.id = ?",
        (post_id,),
    )
    if not rows:
        raise HTTPException(404, f"Social post {post_id} not found")
    post = dict(rows[0])
    removable = sorted(_REMOVABLE_POST_STATUSES)
    if post["status"] not in _REMOVABLE_POST_STATUSES:
        raise HTTPException(
            409,
            f"Only {' or '.join(removable)} posts can be removed — post "
            f"{post_id} is '{post['status']}'.",
        )
    if post["smart_queue_item_id"] is not None:
        raise HTTPException(
            409,
            f"Post {post_id} belongs to smart queue {post['queue_id']} and can't "
            "be removed here — deleting it would strand the queue item. Use the "
            "queue's missed-postings screen, which can post it now, reschedule "
            "it to the end, or remove the item from the queue.",
        )

    # Neither removable status is supposed to carry a job (scheduling stamps
    # 'approved'; every failure path NULLs the scheduling columns), but PUT lets
    # a client set the status back to 'draft' without clearing scheduler_job_id,
    # and a trigger left behind would fire against a row that no longer exists.
    cancelled_schedule = False
    if post["scheduler_job_id"]:
        cancelled_schedule = await cancel_scheduled_post(post_id)

    placeholders = ", ".join("?" for _ in removable)
    async with write_transaction() as db:
        cursor = await db.execute(
            f"DELETE FROM social_posts WHERE id = ? AND status IN ({placeholders})",
            (post_id, *removable),
        )
        removed = (cursor.rowcount or 0) > 0

    if not removed:
        raise HTTPException(
            409, f"Post {post_id} changed status while being removed — nothing removed."
        )

    notes = []
    if cancelled_schedule:
        notes.append("cancelled its pending schedule")
    if post["error"]:
        # A failed post's error text was the whole content of the banner entry
        # this removal just silenced; it should outlive the row somewhere.
        notes.append(f"discarded error: {post['error']}")

    # The row is gone for good and nothing else records that it existed, so the
    # server log is the only place this is recoverable from.
    logger.info(
        "Removed %s social post %s (%s, video %s, %d chars)%s",
        post["status"],
        post_id,
        post["platform"],
        post["video_id"],
        len(post["content"] or ""),
        (" — " + "; ".join(notes)) if notes else "",
    )
    return {"status": "ok", "cancelled_schedule": cancelled_schedule}


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
    try:
        user_prompt = await tmpl.async_render(seed["body"], variables)
        system_prompt = (
            await tmpl.async_render(seed["system"], variables)
            if seed["system"] else None
        )
    except tmpl.MissingRequiredVariable as exc:
        raise HTTPException(400, {"missing_required": exc.name}) from exc
    except tmpl.UndefinedTemplateVariables as exc:
        raise HTTPException(
            400,
            {
                "undefined_variables": exc.names,
                "hint": "The shorten prompt only receives target_chars and "
                        "post_text — remove other bare variables from it.",
            },
        ) from exc
    except tmpl.SectionTagError as exc:
        raise HTTPException(400, {"section_error": str(exc)}) from exc
    try:
        new = (await asyncio.to_thread(
            ai.call_ai_block,
            user_prompt,
            system=system_prompt,
            max_tokens=512,
        )).strip()
    except Exception as e:
        raise HTTPException(502, f"Couldn't shorten the post: {e}")
    if not new:
        raise HTTPException(502, "The model returned an empty result.")

    orig_urls = set(re.findall(r"https?://\S+", old))
    warning = None
    if orig_urls and not orig_urls.issubset(set(re.findall(r"https?://\S+", new))):
        warning = "A link may have changed — double-check before posting."

    async with write_transaction() as db:
        await db.execute("UPDATE social_posts SET content = ? WHERE id = ?", (new, post_id))
    return {"content": new, "previous": old, "char_count": len(new), "warning": warning}


async def _project_id_for_post(db, post: dict) -> int:
    """Resolve the project that owns ``post`` via its video.

    Raises ``ValueError`` if the post's video link can't be resolved. We must
    NOT silently fall back to the default project (id 1) here: that would route
    the post — and its send credential — to the wrong account. ``project_id`` is
    NOT NULL in the schema and ``social_posts.video_id`` is a NOT NULL FK, so a
    missing row is a data-integrity fault worth surfacing, not papering over.
    """
    cursor = await db.execute(
        "SELECT v.project_id "
        "FROM social_posts sp JOIN videos v ON v.id = sp.video_id "
        "WHERE sp.id = ?",
        (post["id"],),
    )
    row = await cursor.fetchone()
    if row is None or row["project_id"] is None:
        raise ValueError(
            f"could not resolve a project for post {post['id']} — its video link "
            "is missing. Bind the post to a social account or fix its video link, "
            "then retry."
        )
    return int(row["project_id"])


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
    project_id = await _project_id_for_post(db, post)

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

    try:
        project_id = await _project_id_for_post(db, post)
    except ValueError:
        # Best-effort pre-check only: if the project can't be resolved we return
        # None (skip the pre-check) rather than route to the wrong account. The
        # real send via _resolve_poster_for_post raises the same error and
        # surfaces it to the user.
        return None
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
            media_paths=_decode_media_paths(post),
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

    # Claim atomically, exactly as the scheduler side does. Without it, a manual
    # Send racing a firing DateTrigger for the same post publishes twice: both
    # read status='approved', both post, both write 'posted' — one row, two live
    # posts. The claim machinery existed and was used on one side only.
    from yt_scheduler.services.scheduler import _claim_post_for_send

    # from_failed: this endpoint is the user pressing Send or Retry, including
    # from the failed-sends banner — where every row is 'failed' by definition,
    # so a claim that only took 'approved' 409'd on every click and the banner's
    # Retry button could never work at all.
    if not await _claim_post_for_send(post_id, from_failed=True):
        raise HTTPException(
            409,
            "That post is already being sent, or has been. Refresh to see its "
            "current state.",
        )

    try:
        result = await poster.post(
            post["content"],
            media_paths=_decode_media_paths(post),
        )
        # Its own handler: if recording raises, the post is ALREADY live. The
        # outer handler would write status='failed' with a database error,
        # inviting a re-send that the duplicate guard cannot catch (the row is
        # then neither 'posted' nor 'sending').
        try:
            await mark_posted(post_id, post_url=result.get("url", ""))
        except Exception as record_exc:
            logger.exception(
                "Post %s went out on %s but could not be recorded",
                post_id, post["platform"],
            )
            async with write_transaction() as _db:
                await _db.execute(
                    "UPDATE social_posts SET status = 'posted', error = ? WHERE id = ?",
                    ("Sent, but recording the result failed — the post IS live. "
                     "Do not send it again.", post_id),
                )
            raise HTTPException(
                500,
                f"The {post['platform']} post went out, but recording it failed. "
                "It is marked posted — do not send it again.",
            ) from record_exc
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
        # Flag the credential so Settings shows a Reconnect button. Prefer the
        # UUID the error carried, but fall back to the credential we already
        # resolved above: telling the user to reconnect while leaving
        # needs_reauth unset would surface the prompt with no button to act on.
        uuid_to_flag = e.uuid or (cred.get("uuid") if cred else None)
        if uuid_to_flag:
            await mark_needs_reauth(uuid_to_flag)
        await mark_failed(post_id, error=f"Credential needs re-auth: {e}")
        raise HTTPException(
            401,
            f"{post['platform']} credential needs re-authentication. "
            "Reconnect from Settings.",
        ) from e
    except HTTPException:
        # Already shaped for the client (including the posted-but-unrecorded
        # case above, which must NOT be rewritten to 'failed').
        raise
    except Exception as e:
        logger.exception("Send failed for post %s", post_id)
        # Also releases the claim taken above; otherwise the row sits 'sending'
        # forever with nothing able to retry it.
        await mark_failed(
            post_id, error=str(e), **await send_failures.retry_plan(post_id, e)
        )
        raise HTTPException(500, str(e))


async def _refuse_if_queue_owned(post_id: int, action: str) -> None:
    """Queue-owned posts are scheduled by Accept, and only by Accept.

    Letting the generic per-post routes move one desynchronizes
    ``social_posts.scheduled_at`` from its ``smart_queue_items`` row, which is
    what every "when does this go out" read consults. ``remove_post`` already
    refuses queue posts for exactly this reason; these two didn't.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT smart_queue_item_id FROM social_posts WHERE id = ?", (post_id,),
    )
    if rows and rows[0]["smart_queue_item_id"] is not None:
        raise HTTPException(
            409,
            f"This post is scheduled by a smart queue — {action} it from the "
            "queue's screen instead, so the item and the post stay in step.",
        )


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

    await _refuse_if_queue_owned(post_id, "reschedule")

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
                media_paths=_decode_media_paths(post),
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

    await _refuse_if_queue_owned(post_id, "unschedule")
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
                media_paths=_decode_media_paths(post),
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

    # A list, not a dict keyed by platform: a template can route two posts to two
    # different accounts on the SAME platform, and keying by platform silently
    # dropped one account's result.
    results: list[dict] = []

    def _entry(post: dict, cred: dict | None, **fields) -> dict:
        return {
            "post_id": int(post["id"]),
            "platform": post["platform"],
            "account_label": (cred or {}).get("label"),
            **fields,
        }

    from yt_scheduler.services.scheduler import _claim_post_for_send

    for row in rows:
        post = dict(row)
        cred = await _credential_for_post(post)
        try:
            poster = await _resolve_poster_for_post(post)
        except ValueError as exc:
            results.append(_entry(post, cred, status="skipped", reason=str(exc)))
            continue

        if not await poster.is_configured():
            results.append(
                _entry(post, cred, status="skipped", reason="not configured")
            )
            continue

        # Claim, exactly as send_post and the two scheduler paths do. This loop
        # was the one sender of four that did not, and it is the one most likely
        # to race: `rows` is a snapshot taken before the first send, and a batch
        # with media on the wire can run for minutes, during which a per-post
        # DateTrigger or publish_video_job can fire for a row still in that
        # snapshot — sending it, while this loop then sends it again from the
        # stale list. The duplicate pre-flight above runs once, before the
        # batch, so it cannot catch it either.
        #
        # Taken AFTER the two skip branches above so a post we never attempt is
        # left in 'approved' rather than needing a release.
        if not await _claim_post_for_send(int(post["id"])):
            results.append(
                _entry(
                    post, cred, status="skipped",
                    reason="already being sent by another worker",
                )
            )
            continue

        try:
            result = await poster.post(
                post["content"],
                media_paths=_decode_media_paths(post),
            )
            # Commit each post's terminal state inside the loop so a crash
            # part-way through a batch can't lose an already-sent post's status.
            await mark_posted(post["id"], post_url=result.get("url", ""))
            results.append(_entry(
                post, cred,
                status="posted",
                url=result.get("url", ""),
                warning=result.get("warning"),
            ))
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
            # See send_post: flag the resolved credential when the error
            # didn't carry a UUID, so the Reconnect button actually appears.
            uuid_to_flag = e.uuid or (cred.get("uuid") if cred else None)
            if uuid_to_flag:
                await mark_needs_reauth(uuid_to_flag)
            await mark_failed(post["id"], error=f"Credential needs re-auth: {e}")
            results.append(_entry(
                post, cred,
                status="needs_reauth",
                error="Credential needs re-authentication. Reconnect from Settings.",
            ))
        except Exception as e:
            logger.exception("Send failed for post %s", post["id"])
            await mark_failed(
                post["id"], error=str(e),
                **await send_failures.retry_plan(post["id"], e),
            )
            results.append(_entry(post, cred, status="failed", error=str(e)))

    return results
