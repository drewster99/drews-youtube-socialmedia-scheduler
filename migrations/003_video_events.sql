-- Per-video activity log + columns that drive the Regenerate button visibility
-- and import-source tracking.

CREATE TABLE video_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_video_events_video ON video_events(video_id);
CREATE INDEX idx_video_events_created ON video_events(created_at);

ALTER TABLE videos ADD COLUMN description_generated_at TEXT;
ALTER TABLE videos ADD COLUMN imported_from_youtube INTEGER NOT NULL DEFAULT 0;
