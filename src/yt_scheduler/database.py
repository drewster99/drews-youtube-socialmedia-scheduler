"""SQLite database setup and access.

Schema lives in `migrations/NNN_*.sql` and is applied via `yt_scheduler.migrations`.
"""

from __future__ import annotations

import aiosqlite

from yt_scheduler.config import DB_PATH, ensure_dirs
from yt_scheduler.migrations import apply_migrations

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
        await apply_migrations(_db)
    return _db


async def close_db() -> None:
    """Close the database connection."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None
