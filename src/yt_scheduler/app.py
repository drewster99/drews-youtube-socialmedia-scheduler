"""FastAPI application."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from yt_scheduler import build_info
from yt_scheduler.config import HOST, PID_FILE, PORT, ensure_dirs
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
    uploads_routes,
    video_routes,
)
from yt_scheduler.services.auth import backfill_channel_assets, backfill_channel_ids
from yt_scheduler.services.keychain_acl_repair import repair_keychain_acls
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


_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class SameOriginCSRFMiddleware(BaseHTTPMiddleware):
    """Reject state-changing requests whose Origin/Referer doesn't match Host.

    The app has no user auth and binds to localhost — any browser tab the
    user has open at a malicious page could otherwise issue a POST to
    127.0.0.1:8008 and act on their data. Modern browsers attach ``Origin``
    on every cross-origin non-safe request; if that origin's host doesn't
    match the Host header we're answering on, drop the request.

    Requests with no Origin and no Referer are accepted: non-browser
    clients (curl, CLI tooling) can't be tricked by a malicious page
    and a script run locally can already do anything anyway. Paired
    with ``TrustedHostMiddleware`` so a DNS-rebinding attacker can't
    forge a same-origin ``Origin: http://evil.com`` + ``Host: evil.com``.
    """

    async def dispatch(self, request: Request, call_next):
        if request.method in _SAFE_METHODS:
            return await call_next(request)

        host = request.headers.get("host", "")
        origin = request.headers.get("origin")
        referer = request.headers.get("referer")
        source = origin or referer
        if source is None:
            return await call_next(request)

        try:
            source_host = urlparse(source).netloc
        except ValueError:
            source_host = ""

        if source_host != host:
            logger.warning(
                "CSRF: rejecting %s %s — Origin/Referer host %r != Host %r",
                request.method, request.url.path, source_host, host,
            )
            return JSONResponse(
                {"detail": "Cross-origin request blocked."},
                status_code=403,
            )
        return await call_next(request)


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
    """Startup and shutdown.

    DB open + schema migrations + the per-credential keychain migration +
    ensure_default_project are critical: a failure there must crash boot
    rather than serve a half-broken app. Backfills, per-project template
    seeding, and restore_scheduled_jobs are best-effort — a bad row in
    one shouldn't keep the app from coming up.
    """
    import os as _os

    db = await get_db()
    # Repair stale Keychain ACLs BEFORE any in-process secret read. The
    # per-credential migration below reads via the Security framework, which
    # would otherwise trigger one "python3.12 wants to use…" prompt per item
    # for secrets written under the old `/usr/bin/security` scheme.
    await repair_keychain_acls()
    await migrate_to_per_credential_bundles()
    await ensure_default_project()

    # Write the PID file only after the critical startup path succeeds. If boot
    # crashes before this point the shutdown cleanup below never runs, so writing
    # earlier would leave a stale PID that blocks import-all on the next attempt.
    PID_FILE.write_text(str(_os.getpid()))

    try:
        await backfill_channel_ids()
    except Exception:
        logger.exception("backfill_channel_ids failed at startup; continuing")
    try:
        await backfill_channel_assets()
    except Exception:
        logger.exception("backfill_channel_assets failed at startup; continuing")

    # Warm the ffmpeg encoder-capability cache once, off the loop. Without this
    # the first clip cut pays a synchronous `ffmpeg -encoders` spawn, and two
    # concurrent cuts each spawn their own.
    try:
        from yt_scheduler.services import media as _media

        await asyncio.to_thread(_media.hardware_encoder_available, "h264")
    except Exception:
        logger.exception("ffmpeg encoder probe failed at startup; continuing")

    # Seed the built-in templates into EVERY project that doesn't have
    # them yet. Per-project so one bad project doesn't skip the rest.
    project_rows = await db.execute_fetchall("SELECT id FROM projects")
    for project_row in project_rows:
        try:
            await ensure_default_template(project_id=int(project_row["id"]))
        except Exception:
            logger.exception(
                "ensure_default_template failed for project %s; continuing",
                project_row["id"],
            )

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
    try:
        await restore_scheduled_jobs()
    except Exception:
        logger.exception(
            "restore_scheduled_jobs failed; scheduler is running but pre-"
            "existing jobs were not restored. New scheduling works.",
        )

    # Sweep preview files left behind by a Generate job that was running
    # when the server was killed — the _GENERATE_JOBS dict is in-memory,
    # so on restart we have no list of job_ids to clean per-job. Without
    # this, those files accumulate on disk forever.
    try:
        from yt_scheduler.services import clipper as _clipper
        removed = _clipper.cleanup_orphan_generate_previews()
        if removed:
            logger.info("Swept %d orphan Generate preview file(s) on startup", removed)
    except Exception:
        logger.exception("Orphan-preview sweep failed at startup; continuing")

    # Pending Replace-Source uploads that survived a previous process
    # being killed. The in-memory _PENDING_FINALIZES dict doesn't
    # survive a restart, so any source_pending_* file on disk is by
    # definition unreachable and must be cleaned up.
    try:
        from yt_scheduler.routers import video_routes as _video_routes
        removed = _video_routes.cleanup_orphan_pending_source_files()
        if removed:
            logger.info(
                "Swept %d orphan pending source-file upload(s) on startup",
                removed,
            )
    except Exception:
        logger.exception(
            "Orphan pending-source sweep failed at startup; continuing",
        )

    # Chunked-upload partials (upload_*.partial) from a previous run.
    # The chunked-upload service's in-memory table doesn't survive a
    # restart, so any .partial file is unreachable and must go.
    try:
        from yt_scheduler.services import chunked_uploads as _chunked
        removed = _chunked.cleanup_orphan_partial_uploads()
        if removed:
            logger.info(
                "Swept %d orphan chunked-upload partial(s) on startup", removed,
            )
    except Exception:
        logger.exception(
            "Orphan chunked-upload sweep failed at startup; continuing",
        )

    yield
    stop_scheduler()
    await close_db()
    PID_FILE.unlink(missing_ok=True)


app = FastAPI(
    title="Drew's Video + Socials Scheduler",
    version=build_info.VERSION,
    lifespan=lifespan,
)
app.add_middleware(SameOriginCSRFMiddleware)
app.add_middleware(BuildIdentityMiddleware)
# Starlette runs middleware in reverse-add order: TrustedHost first
# (outermost) rejects DNS-rebinding before CSRF even sees it, then
# CSRF, then BuildIdentity stamps headers, then Timing wraps everything
# so the duration we log includes every other middleware too.
_trusted_hosts = [
    "127.0.0.1", f"127.0.0.1:{PORT}",
    "localhost", f"localhost:{PORT}",
    "testserver",  # Starlette TestClient default Host
]
if HOST not in {"0.0.0.0", "::", "", "127.0.0.1", "localhost"}:
    _trusted_hosts.extend([HOST, f"{HOST}:{PORT}"])
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_trusted_hosts)
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
app.include_router(uploads_routes.router)


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
    # Filter by project so a CSRF-driven popup can't trick the user into
    # acting on another project's video through this project's chrome.
    from yt_scheduler.database import get_db
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, title FROM videos WHERE id = ? AND project_id = ?",
        (video_id, int(project["id"])),
    )
    if not rows:
        raise HTTPException(404, f"Video '{video_id}' not found in project '{slug}'")
    current_video = dict(rows[0])
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
        "SELECT id, title, episode_number FROM videos WHERE id = ? AND project_id = ?",
        (parent_id, int(project["id"])),
    )
    if not rows:
        raise HTTPException(404, f"Video '{parent_id}' not found in project '{slug}'")
    current_video = dict(rows[0])
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
        "SELECT id, title, video_file_path, source_file_origin FROM videos "
        "WHERE id = ? AND project_id = ?",
        (parent_id, int(project["id"])),
    )
    if not rows:
        raise HTTPException(404, f"Video '{parent_id}' not found in project '{slug}'")
    current_video = dict(rows[0])

    # Render the parent's preview-friendly metadata into the page so the
    # proposal cards can show their inline <video> / YouTube-iframe
    # previews even in the resume-via-?job_id= path and the Restore-
    # without-fresh-Generate path. Without this, the in-flight previewData
    # would be the only source — and that's only set after a /preview
    # call lands inside the page session.
    parent_meta: dict = {
        "video_file_url": None, "browser_playable": None, "youtube_id": "",
        # Pre-computed so a low-resolution / lossy source is visible
        # before the user clicks Generate. Without this the warnings
        # only render after /preview returns — by which time Claude
        # has already burned tokens proposing clips from a source the
        # user might have wanted to swap first.
        "warnings": [],
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
            parent_meta["warnings"] = media_service.source_quality_warnings(
                width=probe.width if probe else None,
                height=probe.height if probe else None,
                source_origin=current_video.get("source_file_origin"),
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
