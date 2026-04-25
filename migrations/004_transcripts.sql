-- Multiple transcripts per video, with provenance metadata.
--
-- Existing single-column transcripts are migrated into one transcript row per
-- video, marked source='user_edited' (we don't know how they were generated
-- pre-rename, and the user has likely touched them). The video's
-- transcript_id pointer is set to that row.

CREATE TABLE transcripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    source_detail TEXT,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_transcripts_video ON transcripts(video_id);

ALTER TABLE videos ADD COLUMN transcript_id INTEGER REFERENCES transcripts(id) ON DELETE SET NULL;
ALTER TABLE videos ADD COLUMN transcript_is_edited INTEGER NOT NULL DEFAULT 0;
ALTER TABLE videos ADD COLUMN transcript_source TEXT;
ALTER TABLE videos ADD COLUMN transcript_created_at TEXT;
ALTER TABLE videos ADD COLUMN transcript_updated_at TEXT;

-- Migrate legacy data: every video with a non-empty transcript gets one
-- transcripts row marked user_edited, dated to the video's creation time.
INSERT INTO transcripts (video_id, source, text, created_at)
SELECT id, 'user_edited', transcript, created_at
FROM videos
WHERE transcript IS NOT NULL AND transcript != '';

-- Backfill the pointer + provenance columns for migrated rows.
UPDATE videos
SET transcript_id = (
        SELECT t.id FROM transcripts t WHERE t.video_id = videos.id
        ORDER BY t.id DESC LIMIT 1
    ),
    transcript_source = 'user_edited',
    transcript_created_at = created_at,
    transcript_updated_at = created_at
WHERE transcript IS NOT NULL AND transcript != '';
