-- Crash/restart survival for Promo-chain jobs that have not yet inserted a
-- videos row. A promo clip's pre-INSERT state (cut params + AI-supplied title)
-- lives only in the in-memory _UPLOAD_JOBS dict, so an app restart before the
-- YouTube upload + row INSERT stranded the clip: lost work plus a stuck UI
-- card (observed 2026-06-12 after a mid-batch crash). This table persists that
-- state so startup can re-spawn the chain.
--
-- youtube_video_id is stamped the instant the upload finalizes, BEFORE the
-- videos-row INSERT, so a restart in that narrow window can recognise the
-- clip already uploaded and NOT re-upload it (which would create a duplicate).
--
-- Rows are marked done/failed on completion, never deleted — an audit trail
-- and a guarantee the resumer never loses a record of work it kicked off.
CREATE TABLE IF NOT EXISTS pending_promo_jobs (
    job_id              TEXT PRIMARY KEY,
    project_id          INTEGER NOT NULL,
    parent_id           TEXT,
    forced_item_type    TEXT,
    original_filename   TEXT,
    title               TEXT,
    parent_video_path   TEXT,
    local_path          TEXT,
    cut_start_seconds   REAL,
    cut_end_seconds     REAL,
    vertical_crop       INTEGER NOT NULL DEFAULT 0,
    x_shift_normalized  REAL NOT NULL DEFAULT 0,
    audio_fade_in       REAL NOT NULL DEFAULT 0,
    audio_fade_out      REAL NOT NULL DEFAULT 0,
    youtube_video_id    TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',
    last_error          TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pending_promo_jobs_status
    ON pending_promo_jobs(status);
