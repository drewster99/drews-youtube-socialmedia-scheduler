"""FastAPI application."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from youtube_publisher.config import UPLOAD_DIR, ensure_dirs
from youtube_publisher.database import close_db, get_db
from youtube_publisher.routers import auth_routes, video_routes, social_routes, template_routes, settings_routes
from youtube_publisher.services.scheduler import restore_scheduled_jobs, start_scheduler, stop_scheduler
from youtube_publisher.services.templates import ensure_default_template


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    ensure_dirs()
    await get_db()
    await ensure_default_template()
    start_scheduler()
    await restore_scheduled_jobs()
    yield
    stop_scheduler()
    await close_db()


app = FastAPI(title="YouTube Publisher", version="0.1.0", lifespan=lifespan)

# Static files and templates
static_dir = Path(__file__).parent / "static"
templates_dir = Path(__file__).parent / "templates_html"

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

html_templates = Jinja2Templates(directory=str(templates_dir))

# API routes
app.include_router(auth_routes.router)
app.include_router(video_routes.router)
app.include_router(social_routes.router)
app.include_router(template_routes.router)
app.include_router(settings_routes.router)


# HTML pages
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return html_templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    return html_templates.TemplateResponse("upload.html", {"request": request})


@app.get("/videos/{video_id}", response_class=HTMLResponse)
async def video_detail_page(request: Request, video_id: str):
    return html_templates.TemplateResponse("video_detail.html", {"request": request, "video_id": video_id})


@app.get("/templates", response_class=HTMLResponse)
async def templates_page(request: Request):
    return html_templates.TemplateResponse("templates.html", {"request": request})


@app.get("/templates/{name}", response_class=HTMLResponse)
async def template_edit_page(request: Request, name: str):
    return html_templates.TemplateResponse("template_edit.html", {"request": request, "template_name": name})


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    return html_templates.TemplateResponse("settings.html", {"request": request})


@app.get("/moderation", response_class=HTMLResponse)
async def moderation_page(request: Request):
    return html_templates.TemplateResponse("moderation.html", {"request": request})
