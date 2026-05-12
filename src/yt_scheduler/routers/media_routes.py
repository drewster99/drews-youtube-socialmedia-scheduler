"""Serve user data files (thumbnails, uploaded videos, item images, clips…).

This replaces a raw ``StaticFiles`` directory mount over ``UPLOAD_DIR`` with an
explicit handler so that:

* the browser never learns the server's absolute filesystem layout — the API
  hands out ``/media/<name>`` URLs, not disk paths — which keeps the client
  portable and makes a future remotely-hosted server / CLI client viable;
* there's a single, auditable place that decides what's served.

``UPLOAD_DIR`` is flat (no sub-directories), so a single path segment — the
bare filename — is all that's ever valid. Anything with a separator, ``..``,
a leading slash, etc. is rejected. Range requests (and the resulting ``206``)
are handled by Starlette's ``FileResponse``; conditional ``304`` responses are
intentionally *not* implemented here (that logic lives in ``StaticFiles``,
which we're deliberately avoiding) — ``Cache-Control: no-cache`` forces a
revalidation each time, which is cheap for local-disk reads.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from yt_scheduler import config

router = APIRouter(tags=["media"])


@router.get(config.MEDIA_URL_PREFIX + "/{filename}")
async def serve_media(filename: str) -> FileResponse:
    """Stream a single file from ``UPLOAD_DIR`` by its bare filename."""
    if not filename or "\x00" in filename or Path(filename).name != filename:
        raise HTTPException(status_code=404)

    base = config.UPLOAD_DIR.resolve()
    target = (config.UPLOAD_DIR / filename).resolve()
    if not target.is_relative_to(base) or not target.is_file():
        raise HTTPException(status_code=404)

    return FileResponse(target, headers={"Cache-Control": "no-cache"})
