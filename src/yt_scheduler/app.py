"""FastAPI application."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from yt_scheduler import build_info
from yt_scheduler.config import ensure_dirs
from yt_scheduler.database import close_db, get_db
from yt_scheduler.routers import (
    auth_routes,
    expand_routes,
    global_variable_routes,
    import_routes,
    item_image_routes,
    item_variable_routes,
    media_routes,
    oauth_routes,
    project_routes,
    project_variable_routes,
    promo_routes,
    settings_routes,
    social_credentials_routes,
    social_routes,
    template_routes,
    transcript_routes,
    video_routes,
)
from yt_scheduler.services.auth import backfill_channel_assets, backfill_channel_ids
from yt_scheduler.services.keychain_migration import (
    migrate_to_per_credential_bundles,
)
from yt_scheduler.services.projects import (
    DEFAULT_PROJECT_SLUG,
    ensure_default_project,
    get_project_by_slug,
)
from yt_scheduler.services.scheduler import (
    restore_scheduled_jobs,
    start_scheduler,
    stop_scheduler,
)
from yt_scheduler.services.templates import ensure_default_template

logger = logging.getLogger(__name__)


_BUILD_HEADER = "X-DYS-Build-Id"
_BUILD_KIND_HEADER = "X-DYS-Build-Kind"


class BuildIdentityMiddleware(BaseHTTPMiddleware):
    """Stamps every response with our build_id and warns when the inbound
    ``X-DYS-Build-Id`` header doesn't match — that means a client tab (or the
    .app shell) was loaded against a different server build than the one
    answering now.
    """

    async def dispatch(self, request: Request, call_next):
        ours = build_info.BUILD_ID
        their_id = request.headers.get(_BUILD_HEADER)
        if their_id and their_id != ours:
            logger.warning(
                "Build mismatch: client sent %r, server is %r (kind=%s, version=%s)",
                their_id, ours, build_info.BUILD_KIND, build_info.VERSION,
            )
        response = await call_next(request)
        response.headers[_BUILD_HEADER] = ours
        response.headers[_BUILD_KIND_HEADER] = build_info.BUILD_KIND
        return response


# Anything slower than this gets logged at WARNING so it stands out when
# scanning logs for "what's making the UI feel sluggish." Tuned to surface
# the kind of half-second-plus stalls that block other requests sharing
# the event loop (sync keychain calls, sync HTTP, etc.), without spamming
# the log for normal sub-100ms responses.
_SLOW_REQUEST_MS = 250


class TimingMiddleware(BaseHTTPMiddleware):
    """Log wall-clock duration of every request.

    Sits at INFO for normal traffic and bumps to WARNING when a single
    request crosses ``_SLOW_REQUEST_MS`` — the threshold beyond which a
    response is long enough that it has likely held up other concurrent
    requests sharing the event loop. ``/static/*`` is skipped to keep
    page-load noise out of the log; static files are served by Starlette
    and never block real handlers anyway.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/static/"):
            return await call_next(request)
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.warning(
                "%s %s — raised after %.0fms", request.method, path, elapsed_ms,
            )
            raise
        elapsed_ms = (time.perf_counter() - start) * 1000
        level = logging.WARNING if elapsed_ms >= _SLOW_REQUEST_MS else logging.INFO
        logger.log(
            level,
            "%s %s → %d in %.0fms",
            request.method, path, response.status_code, elapsed_ms,
        )
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    db = await get_db()
    await migrate_to_per_credential_bundles()
    await ensure_default_project()
    await backfill_channel_ids()
    await backfill_channel_assets()
    # Seed the built-in templates into EVERY project that doesn't have
    # them yet. Earlier installs (and the create-project route before
    # we wired template seeding into create_project) could leave a
    # project with an empty templates table, breaking the Templates
    # page and the posting-settings defaults that point at
    # 'announce_video'. Cheap startup pass — the per-project
    # ensure_default_template skips when both built-in names already
    # exist in that project.
    project_rows = await db.execute_fetchall("SELECT id FROM projects")
    for project_row in project_rows:
        await ensure_default_template(project_id=int(project_row["id"]))

    # Read scheduler intervals from DB settings (saved via Settings UI), fall back to config defaults
    settings_rows = await db.execute_fetchall(
        "SELECT key, value FROM settings WHERE key IN "
        "('comment_check_interval', 'caption_check_interval')"
    )
    db_settings = {r["key"]: r["value"] for r in settings_rows}

    caption_interval = None
    comment_interval = None
    try:
        if "caption_check_interval" in db_settings:
            caption_interval = int(db_settings["caption_check_interval"])
    except (ValueError, TypeError):
        logger.warning("Invalid caption_check_interval in settings, using default")
    try:
        if "comment_check_interval" in db_settings:
            comment_interval = int(db_settings["comment_check_interval"])
    except (ValueError, TypeError):
        logger.warning("Invalid comment_check_interval in settings, using default")

    start_scheduler(caption_interval=caption_interval, comment_interval=comment_interval)
    await restore_scheduled_jobs()
    yield
    stop_scheduler()
    await close_db()


app = FastAPI(
    title="Drew's Video + Socials Scheduler",
    version=build_info.VERSION,
    lifespan=lifespan,
)
app.add_middleware(BuildIdentityMiddleware)
# Added LAST so it wraps everything else — the duration we log includes
# the build-identity middleware and any future middleware so the number
# matches what the client actually waited for.
app.add_middleware(TimingMiddleware)

# Make sure the data directories exist before anything tries to use them.
# Migration runs in the lifespan instead — triggering it at module load means
# a stray ``python -c "import yt_scheduler.app"`` touches real data, a footgun.
ensure_dirs()

# Static files and templates
static_dir = Path(__file__).parent / "static"
templates_dir = Path(__file__).parent / "templates_html"

class _RevalidatingStaticFiles(StaticFiles):
    """StaticFiles that asks the browser to revalidate via etag every request.

    Browsers fall back to heuristic freshness when no Cache-Control is set,
    which can leave a CSS or template-driven asset stuck on a stale copy long
    after the file has been edited and the server is serving the new bytes.
    ``no-cache`` keeps the entry in the disk cache but forces a conditional
    request, so unchanged files come back as 304 — cheap, and the next edit
    is visible immediately.
    """

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        response.headers.setdefault("Cache-Control", "no-cache")
        return response


app.mount("/static", _RevalidatingStaticFiles(directory=str(static_dir)), name="static")

html_templates = Jinja2Templates(directory=str(templates_dir))


@app.get("/api/build")
async def api_build():
    """Build identity for the .app + browser to compare against their own."""
    return build_info.as_dict()


# API routes
app.include_router(project_routes.router)
app.include_router(auth_routes.router)
app.include_router(video_routes.router)
app.include_router(transcript_routes.router)
app.include_router(social_routes.router)
app.include_router(template_routes.router)
app.include_router(expand_routes.router)
app.include_router(settings_routes.router)
app.include_router(oauth_routes.router)
app.include_router(social_credentials_routes.router)
app.include_router(import_routes.router)
app.include_router(promo_routes.router)
app.include_router(global_variable_routes.router)
app.include_router(project_variable_routes.router)
app.include_router(item_variable_routes.router)
app.include_router(item_image_routes.router)
app.include_router(media_routes.router)


# --- HTML pages -------------------------------------------------------------


async def _project_context(slug: str) -> dict:
    project = await get_project_by_slug(slug)
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")
    return project


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home — projects list, upcoming items, recent activity."""
    return html_templates.TemplateResponse(request, "home.html")


@app.get("/settings", response_class=HTMLResponse)
async def general_settings_page(request: Request):
    """General Settings — Anthropic key, model, intervals, background service."""
    return html_templates.TemplateResponse(request, "settings.html")


@app.get("/projects/{slug}", response_class=HTMLResponse)
async def project_dashboard(request: Request, slug: str):
    project = await _project_context(slug)
    return html_templates.TemplateResponse(
        request, "dashboard.html", context={"current_project": project}
    )


@app.get("/projects/{slug}/videos/{video_id}", response_class=HTMLResponse)
async def project_video_detail_page(request: Request, slug: str, video_id: str):
    project = await _project_context(slug)
    # Pull the video title for the sidebar's "current video" entry so
    # the user has a "you are here" marker without having to read the
    # page header. Missing row → no sidebar entry; the page itself
    # surfaces the 404 once the JS fetch fails.
    from yt_scheduler.database import get_db
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, title FROM videos WHERE id = ?", (video_id,)
    )
    current_video = dict(rows[0]) if rows else None
    return html_templates.TemplateResponse(
        request,
        "video_detail.html",
        context={
            "current_project": project,
            "video_id": video_id,
            "current_video": current_video,
        },
    )


@app.get("/projects/{slug}/videos/{parent_id}/promos", response_class=HTMLResponse)
async def project_promo_videos_page(request: Request, slug: str, parent_id: str):
    """Promo Videos screen — three tier sections + multi-file Add."""
    project = await _project_context(slug)
    from yt_scheduler.database import get_db
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, title FROM videos WHERE id = ?", (parent_id,)
    )
    current_video = dict(rows[0]) if rows else None
    return html_templates.TemplateResponse(
        request,
        "promo_videos.html",
        context={
            "current_project": project,
            "parent_id": parent_id,
            "current_video": current_video,
        },
    )


@app.get(
    "/projects/{slug}/videos/{parent_id}/promos/generate",
    response_class=HTMLResponse,
)
async def project_generate_from_source_page(
    request: Request, slug: str, parent_id: str,
):
    """Full-screen Generate-from-source review page.

    The picker + Generate button + proposal grid + Previously-dismissed
    section all live here — replaces the old in-page modal. URL gains
    ``?job_id=…`` when a Generate is in flight so the page is
    refreshable and browser-back-friendly.
    """
    project = await _project_context(slug)
    from yt_scheduler.config import media_url
    from yt_scheduler.database import get_db
    from yt_scheduler.routers.video_routes import _resolve_video_file
    from yt_scheduler.services import media as media_service
    import asyncio as _asyncio

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, title, video_file_path FROM videos WHERE id = ?",
        (parent_id,),
    )
    current_video = dict(rows[0]) if rows else None

    # Render the parent's preview-friendly metadata into the page so the
    # proposal cards can show their inline <video> / YouTube-iframe
    # previews even in the resume-via-?job_id= path and the Restore-
    # without-fresh-Generate path. Without this, the in-flight previewData
    # would be the only source — and that's only set after a /preview
    # call lands inside the page session.
    parent_meta: dict = {
        "video_file_url": None, "browser_playable": None, "youtube_id": "",
    }
    if current_video:
        parent_path = _resolve_video_file(current_video.get("video_file_path"))
        if parent_path is not None and parent_path.exists():
            probe = await _asyncio.to_thread(
                media_service.probe_video_file, parent_path,
            )
            parent_meta["video_file_url"] = media_url(
                current_video.get("video_file_path")
            )
            parent_meta["browser_playable"] = media_service.is_browser_playable(
                probe.codec_name if probe else None,
                probe.container if probe else None,
            )
        # 11-char id is the YouTube convention used elsewhere in the app.
        if len(parent_id) == 11:
            parent_meta["youtube_id"] = parent_id

    return html_templates.TemplateResponse(
        request,
        "generate_review.html",
        context={
            "current_project": project,
            "parent_id": parent_id,
            "current_video": current_video,
            "parent_meta": parent_meta,
        },
    )


@app.get("/projects/{slug}/templates", response_class=HTMLResponse)
async def project_templates_page(request: Request, slug: str):
    project = await _project_context(slug)
    return html_templates.TemplateResponse(
        request, "templates.html", context={"current_project": project}
    )


@app.get("/projects/{slug}/templates/{name}", response_class=HTMLResponse)
async def project_template_edit_page(request: Request, slug: str, name: str):
    project = await _project_context(slug)
    return html_templates.TemplateResponse(
        request,
        "template_edit.html",
        context={"current_project": project, "template_name": name},
    )


@app.get("/projects/{slug}/moderation", response_class=HTMLResponse)
async def project_moderation_page(request: Request, slug: str):
    project = await _project_context(slug)
    return html_templates.TemplateResponse(
        request, "moderation.html", context={"current_project": project}
    )


@app.get("/projects/{slug}/settings", response_class=HTMLResponse)
async def project_settings_page(request: Request, slug: str):
    project = await _project_context(slug)
    return html_templates.TemplateResponse(
        request, "project_settings.html", context={"current_project": project}
    )


@app.get("/projects/{slug}/socials-compose", response_class=HTMLResponse)
async def project_socials_compose(request: Request, slug: str):
    """Socials-from-template composer wizard (req #24)."""
    project = await _project_context(slug)
    return html_templates.TemplateResponse(
        request, "socials_compose.html", context={"current_project": project}
    )


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    """Upload form. Reached only from the Dashboard's 'Upload new video' button —
    the sidebar entry was removed per req #4."""
    return html_templates.TemplateResponse(request, "upload.html")


# --- Backwards-compatibility redirects --------------------------------------
# Pre-rename URLs land on the Default project so existing bookmarks still work.

@app.get("/videos/{video_id}")
async def legacy_video_redirect(video_id: str):
    return RedirectResponse(
        url=f"/projects/{DEFAULT_PROJECT_SLUG}/videos/{video_id}", status_code=307
    )


@app.get("/templates")
async def legacy_templates_redirect():
    return RedirectResponse(
        url=f"/projects/{DEFAULT_PROJECT_SLUG}/templates", status_code=307
    )


@app.get("/templates/{name}")
async def legacy_template_edit_redirect(name: str):
    return RedirectResponse(
        url=f"/projects/{DEFAULT_PROJECT_SLUG}/templates/{name}", status_code=307
    )


@app.get("/moderation")
async def legacy_moderation_redirect():
    return RedirectResponse(
        url=f"/projects/{DEFAULT_PROJECT_SLUG}/moderation", status_code=307
    )
