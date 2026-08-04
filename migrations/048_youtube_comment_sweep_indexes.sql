-- Indexes for two predicates the threaded comment view made hot.
--
-- Migration 043 indexed (project_id, comment_id), (project_id, published_at)
-- and (project_id, youtube_video_id) — the right set for a flat, newest-first
-- list. The thread grouping and the sweep watermark introduced two more, both
-- of which currently scan every row of a project:
--
--   parent_comment_id      — `_threads_missing_replies` counts stored replies
--                            per thread on every sweep, and the mass-
--                            disappearance guard filters top-level rows.
--   last_seen_in_sweep_at  — the sweep watermark reads MAX() per project.
--
-- Invisible at a few dozen comments; a full scan per dashboard load and per
-- sweep on a channel with thousands.
--
-- Deliberately NOT indexed: COALESCE(parent_comment_id, comment_id), the thread
-- key. It is an expression, so `GROUP BY` / `COUNT(DISTINCT)` / `IN` over it are
-- project scans by construction. Fixing that needs a stored thread_key column,
-- which is a bigger change than it is currently worth — noted here so the next
-- person profiling this does not go looking for an index that cannot exist.

CREATE INDEX IF NOT EXISTS idx_youtube_comments_parent
    ON youtube_comments(project_id, parent_comment_id);

CREATE INDEX IF NOT EXISTS idx_youtube_comments_swept
    ON youtube_comments(project_id, last_seen_in_sweep_at);
