"""SQLite database setup and access."""

from __future__ import annotations

import aiosqlite

from youtube_publisher.config import DB_PATH, ensure_dirs

_db: aiosqlite.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY,                    -- YouTube video ID
    title TEXT NOT NULL,
    description TEXT DEFAULT '',
    tags TEXT DEFAULT '',                    -- JSON array
    privacy_status TEXT DEFAULT 'unlisted',  -- unlisted, private, public
    publish_at TEXT,                         -- ISO datetime for scheduled publish
    thumbnail_path TEXT,
    video_file_path TEXT,
    transcript TEXT,
    generated_description TEXT,
    pinned_links TEXT DEFAULT '',
    status TEXT DEFAULT 'draft',            -- draft, uploaded, captioned, ready, published
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS social_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL REFERENCES videos(id),
    platform TEXT NOT NULL,                 -- twitter, bluesky, mastodon, linkedin, threads
    content TEXT NOT NULL,
    media_path TEXT,
    media_type TEXT,                        -- thumbnail, clip, gif, image
    status TEXT DEFAULT 'draft',            -- draft, approved, posted, failed
    posted_at TEXT,
    post_url TEXT,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    platforms TEXT NOT NULL,                 -- JSON: {platform: {template, media, max_chars, ...}}
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS blocklist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL UNIQUE,
    is_regex INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS moderation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    comment_id TEXT NOT NULL,
    author TEXT,
    comment_text TEXT,
    matched_keyword TEXT,
    action TEXT DEFAULT 'deleted',          -- deleted, held
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


async def get_db() -> aiosqlite.Connection:
    """Get or create the database connection."""
    global _db
    if _db is None:
        ensure_dirs()
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        await _db.executescript(SCHEMA)
        await _db.commit()
    return _db


async def close_db() -> None:
    """Close the database connection."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None
