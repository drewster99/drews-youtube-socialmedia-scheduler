"""SQLite database setup and access.

Schema lives in `migrations/NNN_*.sql` and is applied via `yt_scheduler.migrations`.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar

import aiosqlite

from yt_scheduler.config import DB_PATH, ensure_dirs
from yt_scheduler.migrations import apply_migrations

logger = logging.getLogger(__name__)

# Wait this many milliseconds for a competing writer's lock before raising
# SQLITE_BUSY. The app runs one shared connection, but the CLI subcommands and
# the model-resolution reader open their own connections to the same file, so
# without a busy timeout a concurrent writer makes those fail instantly.
BUSY_TIMEOUT_MS = 5000

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    """Get or create the database connection.

    On first connection, runs any pending migrations and stamps the baseline
    on existing databases.
    """
    global _db
    if _db is None:
        ensure_dirs()
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        # SQLite default is foreign_keys = OFF per connection. We DEPEND
        # on ON DELETE CASCADE for several FKs (template_slots → templates,
        # social_post_traces → social_posts, project-scoped tables, etc.) —
        # without this, an existing DB whose migration 002 already ran
        # would silently lose cascading deletes on the next process start
        # (the per-connection pragma doesn't survive close()).
        await _db.execute("PRAGMA foreign_keys = ON")
        # WAL lets readers (CLI subcommands, the ai model-resolution reader)
        # see a consistent snapshot without blocking the writer, and is the
        # foundation for safe concurrent access. journal_mode is persistent on
        # the file, so every later connection inherits it; busy_timeout and
        # synchronous are per-connection and must be set on each connection.
        await _db.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        # synchronous=NORMAL is the standard, safe WAL setting: durable across
        # app crashes (WAL recovers), only the last commit can be lost on an
        # OS crash / power loss — an acceptable trade for a local single-user app.
        await _db.execute("PRAGMA synchronous = NORMAL")
        async with _db.execute("PRAGMA journal_mode = WAL") as cursor:
            row = await cursor.fetchone()
        actual_mode = (row[0] if row else "") or ""
        if actual_mode.lower() != "wal":
            # WAL silently degrades to rollback journaling on some filesystems
            # (network/SMB volumes). Surface that rather than assuming it took.
            logger.warning(
                "Requested WAL journal mode but SQLite reports %r; "
                "concurrent access falls back to rollback-journal behavior "
                "(is DYS_DATA_DIR on a network volume?)",
                actual_mode,
            )
        await apply_migrations(_db)
    return _db


async def close_db() -> None:
    """Close the database connection."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None


# Serializes write_transaction() critical sections. Not reentrant — the
# ContextVar below lets a nested call join the outer transaction instead of
# re-acquiring (which would deadlock).
_write_lock = asyncio.Lock()
_in_write_txn: ContextVar[bool] = ContextVar("_in_write_txn", default=False)


@asynccontextmanager
async def write_transaction():
    """Run a read-modify-write critical section as one serialized, isolated unit.

    The app shares a SINGLE aiosqlite connection across all request handlers and
    background jobs, and that connection has no per-coroutine transaction: any
    coroutine's ``commit()`` durably flushes every other coroutine's in-flight
    writes. This context manager holds a process-wide async lock AND an explicit
    ``BEGIN IMMEDIATE``/``COMMIT`` (``ROLLBACK`` on error) so the enclosed
    statements commit or roll back atomically and can't be interleaved by
    another ``write_transaction``.

    IRON RULES:
      * NEVER ``await`` a network / ``to_thread`` call inside the block — that
        would serialize all writes behind one slow round-trip. Do that work
        before/after, passing values in/out.
      * Isolation is only complete once EVERY write site goes through this (or no
        bare ``commit()`` runs while a block is open). It is the tool for that
        conversion; on its own it does not make the rest of the codebase safe.

    Nesting is allowed and joins the outer transaction (no nested ``BEGIN``, no
    re-acquire), so it can't self-deadlock on the non-reentrant lock.
    """
    db = await get_db()
    if _in_write_txn.get():
        # Already inside an enclosing write_transaction on this task — join it.
        yield db
        return
    async with _write_lock:
        token = _in_write_txn.set(True)
        try:
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                await db.commit()
            except BaseException:
                await db.rollback()
                raise
        finally:
            _in_write_txn.reset(token)
