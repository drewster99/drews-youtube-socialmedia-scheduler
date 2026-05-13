-- Migration 020: per-social-post debug trace.
--
-- generate-posts threads a trace collector through templates.render
-- (F1). Persist the resulting JSON so the F3 ⓘ-button modal on the
-- detail page can show the user exactly how the slot body became the
-- text Claude returned. Keyed by post_id so cascade-delete handles
-- cleanup when a post row goes away; a separate pruning job evicts
-- rows older than 24h.

CREATE TABLE IF NOT EXISTS social_post_traces (
    post_id INTEGER PRIMARY KEY REFERENCES social_posts(id) ON DELETE CASCADE,
    trace_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_social_post_traces_created_at
    ON social_post_traces(created_at);
