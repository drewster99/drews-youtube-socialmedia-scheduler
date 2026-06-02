"""Chunked upload service — single-pass server-side disk write.

Consumers (Replace Source, New Upload, New Item) call into a domain
endpoint with an ``upload_id`` once the bytes are on disk. Each
upload is initialised with the expected total size, the client then
appends ~8 MB chunks via PATCH-style POSTs (offset in URL, raw bytes
as body), and finalize moves the partial file to its canonical name.

Why this exists:

* Avoids FastAPI / Starlette's ``UploadFile`` → ``SpooledTemporaryFile``
  → handler-copy double-pass on multi-GB sources.
* Avoids Safari's ``xhr.send(file)`` body-stream-exhaustion bug —
  each chunk is a small ``Blob.slice(...)`` with no custom headers.
* Reusable: one wire protocol drives all the file-upload domain
  endpoints in the app instead of each one re-implementing
  multipart-buffered-and-recopied.

State lives in-process; a server restart drops all in-flight
uploads and :func:`cleanup_orphan_partial_uploads` removes the
``upload_*.partial`` files left on disk so they don't accumulate.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from pathlib import Path

from yt_scheduler.config import (
    UPLOAD_DIR,
    safe_upload_ext,
)

logger = logging.getLogger(__name__)


# Tuning knobs. 8 MB chunks balance HTTP round-trip overhead against
# Safari's per-request stream-exhaustion risk: small enough that the
# engine doesn't appear to double-read, large enough that an 8 GB
# upload completes in ~1000 requests instead of hundreds of
# thousands.
CHUNK_SIZE_BYTES: int = 8 * 1024 * 1024
_UPLOAD_TTL_SECONDS: float = 30 * 60  # 30 min idle → evicted
_MAX_UPLOAD_BYTES: int = 10 * 1024**3  # 10 GiB cap on any single upload


# upload_id → {
#   "filename": str,
#   "size": int,
#   "received_bytes": int,
#   "path": str,
#   "ext": str,
#   "expires_at": float,        # monotonic deadline
#   "lock": asyncio.Lock,        # serialises chunk appends per upload
#   "finalized": bool,
# }
_UPLOADS: dict[str, dict] = {}
# Guards the _UPLOADS dict shape (insert / pop / iterate). Per-upload
# locks live on the entry itself and serialise chunk-level work.
_DICT_LOCK: asyncio.Lock = asyncio.Lock()


def _evict_stale_locked() -> None:
    """Drop expired entries + unlink their files. Caller holds
    :data:`_DICT_LOCK`.

    "Expired" = past ``expires_at``. The deadline is bumped on every
    successful chunk append, so an active upload never expires mid-
    flight unless the client genuinely walks away.
    """
    now = time.monotonic()
    for upload_id in list(_UPLOADS.keys()):
        entry = _UPLOADS[upload_id]
        if entry["expires_at"] < now:
            _UPLOADS.pop(upload_id, None)
            try:
                Path(entry["path"]).unlink(missing_ok=True)
            except OSError as exc:
                logger.debug(
                    "Could not unlink expired upload %s: %s", entry["path"], exc,
                )


class UploadNotFound(LookupError):
    """Upload id is unknown, expired, or already consumed."""


class UploadConflict(ValueError):
    """Chunk offset doesn't match expected, or upload state is wrong
    for the requested operation."""


class UploadTooLarge(ValueError):
    """The declared size or accumulated bytes would exceed the global
    cap."""


async def init_upload(filename: str, size: int) -> dict:
    """Reserve a new upload slot.

    Returns ``{"upload_id": str, "chunk_size": int}`` — the client
    splits the file into chunks of at most ``chunk_size`` bytes and
    POSTs them via :func:`append_chunk`.

    Raises ``ValueError`` for an empty filename or out-of-range size.
    """
    if not filename:
        raise ValueError("filename is required")
    if size <= 0:
        raise ValueError("size must be > 0")
    if size > _MAX_UPLOAD_BYTES:
        raise UploadTooLarge(
            f"size {size} exceeds {_MAX_UPLOAD_BYTES} cap"
        )

    upload_id = secrets.token_hex(16)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # ``.partial`` while accumulating chunks; renamed to upload_<id>.<ext>
    # by :func:`finalize_upload` once the byte count matches ``size``.
    # The ``.partial`` suffix is what the startup orphan sweep keys on.
    path = UPLOAD_DIR / f"upload_{upload_id}.partial"
    path.touch()

    entry = {
        "filename": filename,
        "size": int(size),
        "received_bytes": 0,
        "path": str(path),
        "ext": safe_upload_ext(filename),
        "expires_at": time.monotonic() + _UPLOAD_TTL_SECONDS,
        "lock": asyncio.Lock(),
        "finalized": False,
    }
    async with _DICT_LOCK:
        _evict_stale_locked()
        _UPLOADS[upload_id] = entry
    return {"upload_id": upload_id, "chunk_size": CHUNK_SIZE_BYTES}


def _append_to_file(path: str, data: bytes) -> None:
    """Open-append-close. Synchronous; runs inside ``asyncio.to_thread``
    so the event loop isn't blocked on the syscall."""
    with open(path, "ab") as fh:
        fh.write(data)


async def append_chunk(
    upload_id: str, offset: int, data: bytes,
) -> int:
    """Append ``data`` at ``offset`` (which must equal the upload's
    current ``received_bytes``).

    Returns the new ``received_bytes`` count.

    Concurrency: the per-upload lock serialises chunk appends for a
    single upload, but different uploads run in parallel. The disk
    write itself runs in a thread so the event loop is free for other
    requests during the I/O.
    """
    async with _DICT_LOCK:
        _evict_stale_locked()
        entry = _UPLOADS.get(upload_id)
        if entry is None:
            raise UploadNotFound(f"Upload {upload_id!r} not found or expired")
        if entry["finalized"]:
            raise UploadConflict("Upload already finalized")
        entry_lock = entry["lock"]

    async with entry_lock:
        # Re-check identity under the dict lock — an evict could have
        # fired between the two acquisitions. If so, the file may be
        # gone and the entry is dead.
        async with _DICT_LOCK:
            if _UPLOADS.get(upload_id) is not entry:
                raise UploadNotFound(
                    f"Upload {upload_id!r} was evicted during the chunk "
                    "operation; restart the upload."
                )

        if offset != entry["received_bytes"]:
            raise UploadConflict(
                f"Out-of-order chunk: offset={offset}, expected="
                f"{entry['received_bytes']}",
            )
        if entry["received_bytes"] + len(data) > entry["size"]:
            raise UploadTooLarge(
                f"Chunk would exceed declared size: "
                f"{entry['received_bytes']} + {len(data)} > {entry['size']}",
            )
        if len(data) > CHUNK_SIZE_BYTES:
            raise UploadConflict(
                f"Chunk size {len(data)} exceeds the {CHUNK_SIZE_BYTES} "
                "cap announced by /init",
            )

        await asyncio.to_thread(_append_to_file, entry["path"], data)
        entry["received_bytes"] += len(data)
        entry["expires_at"] = time.monotonic() + _UPLOAD_TTL_SECONDS
        return entry["received_bytes"]


async def finalize_upload(upload_id: str) -> dict:
    """Mark the upload as complete — checks that every byte arrived
    and renames ``upload_<id>.partial`` → ``upload_<id>.<ext>``.

    Returns ``{"path", "filename", "size", "ext"}``. The consumer
    (domain endpoint) then moves the file to its own canonical name
    (e.g. ``source_pending_<hex>.<ext>``) and calls
    :func:`consume_upload` so the entry leaves the in-memory table.

    Idempotent on already-finalized uploads — returns the same dict.
    """
    async with _DICT_LOCK:
        _evict_stale_locked()
        entry = _UPLOADS.get(upload_id)
        if entry is None:
            raise UploadNotFound(f"Upload {upload_id!r} not found or expired")
        entry_lock = entry["lock"]

    async with entry_lock:
        async with _DICT_LOCK:
            if _UPLOADS.get(upload_id) is not entry:
                raise UploadNotFound(
                    f"Upload {upload_id!r} was evicted during finalize"
                )

        if entry["finalized"]:
            return {
                "path": entry["path"],
                "filename": entry["filename"],
                "size": entry["size"],
                "ext": entry["ext"],
            }
        if entry["received_bytes"] != entry["size"]:
            raise UploadConflict(
                f"Incomplete upload: received {entry['received_bytes']} of "
                f"{entry['size']} bytes; finalize refused",
            )

        partial = Path(entry["path"])
        final = partial.with_name(f"upload_{upload_id}{entry['ext']}")
        # Rename overwrites on POSIX, but ``final`` shouldn't already
        # exist — upload_id is a fresh 16-byte hex per init.
        partial.rename(final)
        entry["path"] = str(final)
        entry["finalized"] = True
        return {
            "path": str(final),
            "filename": entry["filename"],
            "size": entry["size"],
            "ext": entry["ext"],
        }


async def consume_upload(upload_id: str) -> dict:
    """Pop the upload entry — used by a domain endpoint once it has
    moved / renamed the file to its own canonical name.

    The entry must be finalized; consumers should call
    :func:`finalize_upload` first (or the client should hit
    ``/finalize`` before calling the domain endpoint). Returns the
    last-known entry dict so the caller has the path + filename.
    """
    async with _DICT_LOCK:
        entry = _UPLOADS.get(upload_id)
        if entry is None:
            raise UploadNotFound(f"Upload {upload_id!r} not found or expired")
        if not entry["finalized"]:
            raise UploadConflict(
                "Upload not finalized — call /finalize before consuming"
            )
        _UPLOADS.pop(upload_id, None)
    return entry


async def cancel_upload(upload_id: str) -> bool:
    """Drop the upload + unlink its file. Returns ``True`` when the
    entry existed, ``False`` when it was already gone (so the caller
    can treat the cancel as idempotent).
    """
    async with _DICT_LOCK:
        entry = _UPLOADS.pop(upload_id, None)
    if entry is None:
        return False
    try:
        Path(entry["path"]).unlink(missing_ok=True)
    except OSError as exc:
        logger.debug("Could not unlink cancelled upload: %s", exc)
    return True


def cleanup_orphan_partial_uploads() -> int:
    """Delete every ``upload_*.partial`` file in UPLOAD_DIR.

    Called on startup so partials that survived a previous process
    being killed don't accumulate on disk. Finalized uploads use
    ``upload_<id>.<ext>`` (without the ``.partial`` suffix), and
    consumer-renamed names (``source_<hex>.<ext>``, etc.) are also
    safe — this sweep only targets the ``.partial`` shape.

    Returns the number of files removed (for logging).
    """
    removed = 0
    try:
        for p in UPLOAD_DIR.glob("upload_*.partial"):
            try:
                p.unlink()
                removed += 1
            except OSError as exc:
                logger.debug("Could not remove orphan upload %s: %s", p, exc)
    except OSError as exc:
        logger.debug("Orphan upload sweep failed: %s", exc)
    return removed


def _UPLOADS_FOR_TESTS() -> dict[str, dict]:
    """Test-only accessor — production code never reaches into the
    table directly."""
    return _UPLOADS
