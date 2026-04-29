"""Project CRUD and helpers.

Projects are the top-level container for videos, templates, blocklist entries,
moderation log, and per-project settings. Every install has at least one project
("Default") created on first run.
"""

from __future__ import annotations

import re

import aiosqlite

from yt_scheduler.database import get_db

DEFAULT_PROJECT_SLUG = "default"
_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def slugify(name: str) -> str:
    """Turn a human name into a URL slug."""
    s = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return s or "project"


async def ensure_default_project() -> int:
    """Create the Default project if it doesn't exist; return its id."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT id FROM projects WHERE slug = ?", (DEFAULT_PROJECT_SLUG,)
    )
    row = await cursor.fetchone()
    if row is not None:
        return int(row[0])

    cursor = await db.execute(
        "INSERT INTO projects (name, slug) VALUES (?, ?)",
        ("Default", DEFAULT_PROJECT_SLUG),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def list_projects() -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        """SELECT p.id, p.name, p.slug, p.youtube_channel_id,
                  p.channel_thumbnail_url, p.channel_banner_url,
                  p.created_at, p.updated_at,
                  (SELECT COUNT(*) FROM videos WHERE project_id = p.id)         AS video_count,
                  (SELECT COUNT(*) FROM videos
                   WHERE project_id = p.id AND publish_at IS NOT NULL
                     AND status != 'published')                                  AS scheduled_count
           FROM projects p ORDER BY p.created_at"""
    )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def get_project_by_slug(slug: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, name, slug, youtube_channel_id, project_url, "
        "channel_thumbnail_url, channel_banner_url, created_at, updated_at "
        "FROM projects WHERE slug = ?",
        (slug,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_project_by_id(project_id: int) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, name, slug, youtube_channel_id, project_url, "
        "channel_thumbnail_url, channel_banner_url, created_at, updated_at "
        "FROM projects WHERE id = ?",
        (project_id,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def create_project(
    name: str, slug: str | None = None, *, project_url: str | None = None
) -> dict:
    """Create a new project. Slug is auto-derived from name when not given.

    Slug is immutable after creation; renaming changes only the display name.

    ``project_url`` is the value behind ``{{project_url}}`` in templates and
    is optional at create time. For YouTube-bound projects, the OAuth flow
    seeds it from ``snippet.customUrl``; for GitHub or social-only projects
    the user supplies it explicitly.
    """
    name = name.strip()
    if not name:
        raise ValueError("Project name is required")
    chosen_slug = slug.strip() if slug else slugify(name)
    if not _SLUG_PATTERN.match(chosen_slug):
        raise ValueError(
            "Slug must start with a letter or digit and contain only "
            "lowercase letters, digits, and hyphens"
        )

    db = await get_db()
    try:
        cursor = await db.execute(
            "INSERT INTO projects (name, slug, project_url) VALUES (?, ?, ?)",
            (name, chosen_slug, (project_url or "").strip() or None),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise ValueError(f"A project with slug '{chosen_slug}' already exists") from exc

    return await get_project_by_id(int(cursor.lastrowid))  # type: ignore[return-value]


async def update_project_url(project_id: int, project_url: str | None) -> dict:
    """Set or clear the project's ``project_url`` (the value behind
    ``{{project_url}}`` in templates). Pass ``None`` or empty string to
    clear."""
    cleaned = (project_url or "").strip() or None
    db = await get_db()
    await db.execute(
        "UPDATE projects SET project_url = ?, updated_at = datetime('now') WHERE id = ?",
        (cleaned, project_id),
    )
    await db.commit()
    project = await get_project_by_id(project_id)
    if project is None:
        raise ValueError(f"Project {project_id} not found")
    return project


async def rename_project(project_id: int, name: str) -> dict:
    """Update the project's display name; slug is intentionally untouched."""
    name = name.strip()
    if not name:
        raise ValueError("Project name is required")
    db = await get_db()
    await db.execute(
        "UPDATE projects SET name = ?, updated_at = datetime('now') WHERE id = ?",
        (name, project_id),
    )
    await db.commit()
    project = await get_project_by_id(project_id)
    if project is None:
        raise ValueError(f"Project {project_id} not found")
    return project


async def delete_project(project_id: int) -> None:
    """Delete a project. Refuses to delete the Default project."""
    project = await get_project_by_id(project_id)
    if project is None:
        return
    if project["slug"] == DEFAULT_PROJECT_SLUG:
        raise ValueError("The Default project cannot be deleted")
    db = await get_db()
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    await db.commit()


async def set_project_youtube_channel(project_id: int, channel_id: str | None) -> None:
    db = await get_db()
    await db.execute(
        "UPDATE projects SET youtube_channel_id = ?, updated_at = datetime('now') "
        "WHERE id = ?",
        (channel_id, project_id),
    )
    await db.commit()
