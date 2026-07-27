"""Assemble everything the renderer needs to turn a template into a post.

Lives in the service layer because more than one caller needs it: the
generate-posts route and the smart queue's Accept both render a template
against one video. It was previously a private helper inside social_routes,
which would have meant a service importing a router to reuse it.
"""

from __future__ import annotations

import asyncio
import json
import logging

from yt_scheduler.services import templates as tmpl
from yt_scheduler.services import tiers, youtube
from yt_scheduler.services.transcripts import transcript_prompt_variables

logger = logging.getLogger(__name__)


class RenderContextError(Exception):
    """The video can't be rendered — e.g. it has no project, so we can't tell
    which channel's URLs and prompts belong in the post."""


def _tier_from_iso_duration(iso: str | None) -> str:
    """Map an ISO-8601 duration (e.g. PT3M31S) to our tier naming."""
    seconds = tiers.parse_iso8601_duration(iso)
    return tiers.tier_for_duration(seconds) or ""


async def build_render_context(db, video: dict) -> dict:
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
    # project_id is NOT NULL in the schema; a missing/zero value is a
    # data-integrity fault. Surface it rather than silently rendering with the
    # default project's URL/prompts (the old `or 1`), which would produce a post
    # with the wrong channel's links and voice.
    project_id = video.get("project_id")
    if project_id in (None, 0):
        raise RenderContextError(
            f"Video {video_id} has no project_id (data integrity error)."
        )
    project_id = int(project_id)

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
        except Exception as exc:
            # Best-effort enrichment: leaving {{tier}} empty is acceptable, but
            # log so a real failure (expired creds, quota, a bug) is diagnosable
            # rather than silently rendering the wrong/empty tier.
            logger.warning("Tier lookup failed for video %s; leaving tier empty: %s", video_id, exc)
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

    parent_url_value = (parent or {}).get("url") or ""
    parent_title_value = (parent or {}).get("title") or ""
    parent_description_value = (parent or {}).get("description") or ""
    parent_tags_value = ""
    if parent is not None:
        try:
            parent_tag_list = json.loads(parent.get("tags") or "[]")
            parent_tags_value = ", ".join(parent_tag_list)
        except json.JSONDecodeError:
            parent_tags_value = ""

    # ``parent_context_block`` collapses the parent fields into a single
    # ready-to-paste paragraph for prompts that want a one-line opt-in
    # ({{parent_context_block??}}) instead of stitching the four fields
    # themselves. Empty string when the video has no parent, which lets
    # the ??-default form swallow it.
    parent_context_block_value = ""
    if parent is not None and parent_title_value:
        parts = [
            "This is a promo clip from a parent video:",
            f"Parent title: {parent_title_value}",
        ]
        if parent_url_value:
            parts.append(f"Parent URL: {parent_url_value}")
        if parent_description_value:
            parts.append(
                f"Parent description: {parent_description_value[:500]}"
            )
        if parent_tags_value:
            parts.append(f"Parent tags: {parent_tags_value}")
        parts.append(
            "Where natural, reference or link back to the parent so the "
            "promo helps viewers find it."
        )
        parent_context_block_value = "\n".join(parts)

    self_builtins: dict[str, object] = {
        "title": video.get("title") or "",
        "description": description,
        "description_short": description[:150],
        "description_medium": description[:500],
        "tags": ", ".join(tags),
        "hashtags": " ".join(f"#{t.replace(' ', '')}" for t in tags[:5]),
        "thumbnail_path": video.get("thumbnail_path") or "",
        "tier": tier,
        **transcript_prompt_variables(video.get("transcript")),
        "url": url_value,
        "episode_url": episode_url_value,
        "project_url": project_url_value,
        "parent_url": parent_url_value,
        "parent_title": parent_title_value,
        "parent_description": parent_description_value,
        "parent_tags": parent_tags_value,
        "parent_context_block": parent_context_block_value,
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
