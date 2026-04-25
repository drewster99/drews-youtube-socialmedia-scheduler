"""FastAPI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from yt_scheduler.config import UPLOAD_DIR, ensure_dirs
from yt_scheduler.database import close_db, get_db
from yt_scheduler.routers import (
    auth_routes,
    import_routes,
    oauth_routes,
    project_routes,
    settings_routes,
    social_routes,
    template_routes,
    transcript_routes,
    video_routes,
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    ensure_dirs()
    db = await get_db()
    await ensure_default_project()
    await ensure_default_template()

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


app = FastAPI(title="Drew's YT Scheduler", version="0.1.0", lifespan=lifespan)

# Static files and templates
static_dir = Path(__file__).parent / "static"
templates_dir = Path(__file__).parent / "templates_html"

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

html_templates = Jinja2Templates(directory=str(templates_dir))

# API routes
app.include_router(project_routes.router)
app.include_router(auth_routes.router)
app.include_router(video_routes.router)
app.include_router(transcript_routes.router)
app.include_router(social_routes.router)
app.include_router(template_routes.router)
app.include_router(settings_routes.router)
app.include_router(oauth_routes.router)
app.include_router(import_routes.router)


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
    return html_templates.TemplateResponse(
        request,
        "video_detail.html",
        context={"current_project": project, "video_id": video_id},
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
