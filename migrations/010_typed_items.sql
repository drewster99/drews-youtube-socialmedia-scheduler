-- Typed items + per-item / per-project URLs + multi-image media + custom variables.
--
-- Promotes the `videos` table from a YouTube-only abstraction into a typed item:
-- `episode | short | segment | hook | standalone`. Adds parent FK so a hook can
-- borrow its episode's URL. Adds `videos.url` (per-item override of the
-- {{url}} variable) and `projects.project_url` (the {{project_url}} value;
-- auto-populated from YouTube's customUrl at OAuth bind time).
--
-- New tables:
--   item_images          -- additional images per item (multi-image posts)
--   global_variables     -- install-wide custom k/v pairs
--   project_variables    -- per-project custom k/v pairs
--   item_variables       -- per-item custom k/v pairs
--
-- social_posts.media_paths: JSON array column added alongside the legacy
-- media_path column. Existing rows are copied as a 1-element array. The old
-- column stays during the transition; it'll be dropped once all callers
-- consume the array form.
--
-- See plan: ~/.claude/plans/great-ok-question-what-shimmering-hamming.md

-- 1) videos: item_type, parent_item_id, url
--
-- ALTER TABLE ... ADD COLUMN with REFERENCES is supported by SQLite as long
-- as we don't try to backfill the FK at add time. NULL values pass any FK
-- check, so we can add the column live.
ALTER TABLE videos ADD COLUMN item_type TEXT NOT NULL DEFAULT 'episode';
ALTER TABLE videos ADD COLUMN parent_item_id TEXT REFERENCES videos(id) ON DELETE SET NULL;
ALTER TABLE videos ADD COLUMN url TEXT;
CREATE INDEX idx_videos_parent ON videos(parent_item_id);
CREATE INDEX idx_videos_item_type ON videos(item_type);

-- 2) projects: project_url
ALTER TABLE projects ADD COLUMN project_url TEXT;

-- 3) item_images -- additional images attached to an item, with shortname
-- (lowercase [a-z0-9-]) used by template directives like {{image:cat}} or
-- {{image:*}}. Validated app-side (no CHECK constraint to keep migration
-- forward-compatible).
CREATE TABLE item_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    shortname TEXT NOT NULL,
    path TEXT NOT NULL,
    alt_text TEXT NOT NULL DEFAULT '',
    order_index INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (video_id, shortname)
);
CREATE INDEX idx_item_images_video ON item_images(video_id);

-- 4) global_variables -- install-wide custom k/v pairs (lowest-priority layer
-- of variable inheritance: global -> project -> parent -> self).
CREATE TABLE global_variables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 5) project_variables -- per-project custom k/v pairs.
CREATE TABLE project_variables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (project_id, key)
);
CREATE INDEX idx_project_variables_project ON project_variables(project_id);

-- 6) item_variables -- per-item custom k/v pairs.
CREATE TABLE item_variables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (video_id, key)
);
CREATE INDEX idx_item_variables_video ON item_variables(video_id);

-- 7) social_posts.media_paths -- JSON array; the new canonical column.
-- Existing single-string media_path becomes a 1-element array. NULL stays
-- NULL. Old column is intentionally kept during the transition; the next
-- migration will drop it once all readers/writers consume the array form.
ALTER TABLE social_posts ADD COLUMN media_paths TEXT;
UPDATE social_posts
   SET media_paths = json_array(media_path)
 WHERE media_path IS NOT NULL AND media_path <> '';

-- 8) Backfill videos.url for existing rows. Every videos.id pre-migration
-- IS a YouTube video id (every row was either uploaded to or imported from
-- YouTube), so the canonical URL is just `https://youtu.be/<id>`. Future
-- standalone / hook-without-YT items will leave `url` NULL or set it
-- explicitly. Resolution at render time reads videos.url directly without
-- any "derive from id" logic — keeping the renderer simple and unambiguous.
UPDATE videos SET url = 'https://youtu.be/' || id WHERE url IS NULL;

-- 9) Backfill projects.project_url for existing YouTube-bound projects to
-- the channel-id form. Phase F (oauth_routes) will overwrite with the
-- prettier @customUrl form on the next OAuth bind, but only when the user
-- hasn't manually set their own. Projects with no channel (the Default
-- project, or any future GitHub-only project on existing installs) leave
-- project_url NULL.
UPDATE projects
   SET project_url = 'https://www.youtube.com/channel/' || youtube_channel_id
 WHERE project_url IS NULL AND youtube_channel_id IS NOT NULL;
