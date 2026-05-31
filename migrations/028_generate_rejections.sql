-- Migration 028: track Generate-from-source rejections per parent.
--
-- When the user clicks "Cut & insert selected" on the Generate review
-- page, anything they UNchecked gets persisted here. Next time they
-- open the review page for the same parent, the "Previously dismissed"
-- section reads from this table so they can revisit and Restore a
-- past rejection.
--
-- Rejections are PURELY a UI memory mechanism — they're NOT fed into
-- Claude's next proposal call (the user wanted to be able to see
-- and reconsider past dismissals, not have them silently excluded).
-- The vision-pass assessment (x_shift_normalized, crop_classification,
-- crop_confidence) is stored alongside the range so Restore brings
-- the original work back without re-running the (paid) vision call.

CREATE TABLE IF NOT EXISTS generate_rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    start_seconds REAL NOT NULL,
    end_seconds REAL NOT NULL,
    title TEXT,
    reason TEXT,
    x_shift_normalized REAL,
    crop_classification TEXT,
    crop_confidence REAL,
    rejected_at TEXT NOT NULL DEFAULT (datetime('now')),
    -- Re-rejecting the same range replaces the row (INSERT OR REPLACE
    -- in the service helper) so we don't pile up duplicates as the
    -- user runs Generate multiple times on the same parent.
    UNIQUE(parent_id, project_id, kind, start_seconds, end_seconds)
);

-- The review page reads all rejections for a parent in one shot;
-- listing is the only access pattern that needs an index.
CREATE INDEX IF NOT EXISTS idx_generate_rejections_parent
    ON generate_rejections(parent_id, project_id, rejected_at);
