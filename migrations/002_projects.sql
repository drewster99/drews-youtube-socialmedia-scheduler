-- Project layer: introduces multi-project support.
--
-- All existing rows in videos, templates, blocklist, and moderation_log are
-- backfilled into a single "Default" project with slug='default' so existing
-- single-channel workflows keep working unchanged.

PRAGMA foreign_keys = OFF;

-- New tables ----------------------------------------------------------------

CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    youtube_channel_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE social_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT,
    credentials_ref TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (platform, username)
);

CREATE TABLE project_social_accounts (
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    social_account_id INTEGER NOT NULL REFERENCES social_accounts(id) ON DELETE CASCADE,
    PRIMARY KEY (project_id, social_account_id)
);

CREATE TABLE project_settings (
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (project_id, key)
);

-- Seed the default project ---------------------------------------------------

INSERT INTO projects (id, name, slug) VALUES (1, 'Default', 'default');

-- Rebuild existing tables with project_id ------------------------------------

CREATE TABLE _new_videos (
    id TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE DEFAULT 1,
    title TEXT NOT NULL,
    description TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    privacy_status TEXT DEFAULT 'unlisted',
    publish_at TEXT,
    thumbnail_path TEXT,
    video_file_path TEXT,
    transcript TEXT,
    generated_description TEXT,
    pinned_links TEXT DEFAULT '',
    status TEXT DEFAULT 'draft',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
INSERT INTO _new_videos (
    id, project_id, title, description, tags, privacy_status, publish_at,
    thumbnail_path, video_file_path, transcript, generated_description,
    pinned_links, status, created_at, updated_at
)
SELECT
    id, 1, title, description, tags, privacy_status, publish_at,
    thumbnail_path, video_file_path, transcript, generated_description,
    pinned_links, status, created_at, updated_at
FROM videos;
DROP TABLE videos;
ALTER TABLE _new_videos RENAME TO videos;
CREATE INDEX idx_videos_project ON videos(project_id);

CREATE TABLE _new_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE DEFAULT 1,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    platforms TEXT NOT NULL,
    applies_to TEXT NOT NULL DEFAULT '["hook","short","segment","video"]',
    is_builtin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE (project_id, name)
);
INSERT INTO _new_templates (
    id, project_id, name, description, platforms, created_at, updated_at
)
SELECT id, 1, name, description, platforms, created_at, updated_at FROM templates;
DROP TABLE templates;
ALTER TABLE _new_templates RENAME TO templates;
CREATE INDEX idx_templates_project ON templates(project_id);

CREATE TABLE _new_blocklist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE DEFAULT 1,
    keyword TEXT NOT NULL,
    is_regex INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE (project_id, keyword)
);
INSERT INTO _new_blocklist (id, project_id, keyword, is_regex, created_at)
SELECT id, 1, keyword, is_regex, created_at FROM blocklist;
DROP TABLE blocklist;
ALTER TABLE _new_blocklist RENAME TO blocklist;
CREATE INDEX idx_blocklist_project ON blocklist(project_id);

CREATE TABLE _new_moderation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE DEFAULT 1,
    video_id TEXT NOT NULL,
    comment_id TEXT NOT NULL,
    author TEXT,
    comment_text TEXT,
    matched_keyword TEXT,
    action TEXT DEFAULT 'deleted',
    created_at TEXT DEFAULT (datetime('now'))
);
INSERT INTO _new_moderation_log (
    id, project_id, video_id, comment_id, author, comment_text,
    matched_keyword, action, created_at
)
SELECT
    id, 1, video_id, comment_id, author, comment_text,
    matched_keyword, action, created_at
FROM moderation_log;
DROP TABLE moderation_log;
ALTER TABLE _new_moderation_log RENAME TO moderation_log;
CREATE INDEX idx_moderation_log_project ON moderation_log(project_id);
CREATE INDEX idx_moderation_log_video ON moderation_log(video_id);

PRAGMA foreign_keys = ON;
