"""Chunked upload HTTP wire protocol.

Endpoints:

* ``POST   /api/uploads/init``                      — open a slot
* ``POST   /api/uploads/{id}/chunk/{offset}``       — append bytes
* ``POST   /api/uploads/{id}/finalize``             — declare complete
* ``DELETE /api/uploads/{id}``                      — cancel

See :mod:`yt_scheduler.services.chunked_uploads` for rationale and
state shape. Domain endpoints (Replace Source, New Upload, New Item)
consume the resulting ``upload_id`` rather than accepting a multipart
body themselves.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, HTTPException, Request

from yt_scheduler.services import chunked_uploads

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/uploads", tags=["uploads"])


@router.post("/init")
async def init_upload(
    payload: dict = Body(...),
) -> dict:
    """Open a new upload slot.

    Body: ``{"filename": str, "size": int}``.
    Returns ``{"upload_id": str, "chunk_size": int}``.

    Errors: 400 for missing fields / oversized declarations, 413 when
    ``size`` exceeds the global cap.
    """
    filename = payload.get("filename")
    size = payload.get("size")
    if not isinstance(filename, str) or not filename:
        raise HTTPException(400, "filename (str) is required")
    if not isinstance(size, int) or size <= 0:
        raise HTTPException(400, "size (positive int, bytes) is required")
    try:
        return await chunked_uploads.init_upload(filename, size)
    except chunked_uploads.UploadTooLarge as exc:
        raise HTTPException(413, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/{upload_id}/chunk/{offset}")
async def append_chunk(
    upload_id: str,
    offset: int,
    request: Request,
) -> dict:
    """Append the raw request body at ``offset``.

    ``offset`` must equal the upload's current ``received_bytes`` — the
    server explicitly rejects out-of-order / overlapping chunks to
    avoid silent corruption. The body is raw octet-stream; no
    multipart, no custom headers (small chunks slide under Safari's
    ``xhr.send(file)`` stream-exhaustion bug).

    Returns ``{"received_bytes": int}`` (the new running total).

    Errors: 404 unknown / expired upload, 409 offset mismatch or upload
    finalized, 413 chunk would exceed declared size, 400 chunk over
    the per-chunk cap.
    """
    # Content-Length FIRST. The per-chunk cap is enforced inside
    # append_chunk — which runs after ``await request.body()`` has already
    # materialized the whole body in RAM, so a single rogue multi-GB PATCH
    # was buffered in full before anything rejected it. Checking the declared
    # length costs nothing and refuses it before a byte is read.
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            declared_bytes = int(declared)
        except ValueError:
            raise HTTPException(400, "Invalid Content-Length header") from None
        if declared_bytes > chunked_uploads.CHUNK_SIZE_BYTES:
            raise HTTPException(
                400,
                f"Chunk is {declared_bytes} bytes; the per-chunk cap is "
                f"{chunked_uploads.CHUNK_SIZE_BYTES}.",
            )
    data = await request.body()
    if not data:
        # Empty PATCH could be a probe; reject so the client doesn't
        # accidentally "complete" an upload with no bytes.
        raise HTTPException(400, "Chunk body is empty")
    try:
        received = await chunked_uploads.append_chunk(upload_id, offset, data)
        return {"received_bytes": received}
    except chunked_uploads.UploadNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except chunked_uploads.UploadTooLarge as exc:
        raise HTTPException(413, str(exc)) from exc
    except chunked_uploads.UploadOffsetMismatch as exc:
        # The true offset rides along so the client can resync instead of
        # abandoning a multi-GB transfer over one lost acknowledgement.
        raise HTTPException(
            409,
            {"detail": str(exc), "received_bytes": exc.received_bytes},
        ) from exc
    except chunked_uploads.UploadConflict as exc:
        # "Offset mismatch" / "already finalized" are conflicts on the
        # upload state, not bad input — 409 lets clients distinguish.
        raise HTTPException(409, str(exc)) from exc


@router.post("/{upload_id}/finalize")
async def finalize_upload(upload_id: str) -> dict:
    """Mark the upload as complete.

    Returns ``{"upload_id", "size", "filename"}``. After this call the
    upload is ready to be passed to a domain endpoint (Replace Source,
    New Upload, New Item). ``path`` is server-internal so it isn't
    surfaced over the wire.

    Errors: 404 unknown / expired upload, 409 incomplete (received
    bytes ≠ declared size).
    """
    try:
        info = await chunked_uploads.finalize_upload(upload_id)
    except chunked_uploads.UploadNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except chunked_uploads.UploadConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    return {
        "upload_id": upload_id,
        "size": info["size"],
        "filename": info["filename"],
    }


@router.delete("/{upload_id}")
async def cancel_upload(upload_id: str) -> dict:
    """Drop an upload + unlink its on-disk file.

    Idempotent: returns ``{"status": "cancelled"}`` when the entry
    existed, ``{"status": "gone"}`` when it was already removed.
    """
    removed = await chunked_uploads.cancel_upload(upload_id)
    return {"status": "cancelled" if removed else "gone"}
