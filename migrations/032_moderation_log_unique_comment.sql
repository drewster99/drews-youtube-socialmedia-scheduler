-- Make comment moderation exactly-once per (project, comment).
--
-- _process_one used to SELECT for an existing log row, await the YouTube
-- moderate call, then INSERT. A manual "run now" overlapping the periodic sweep
-- could have both coroutines pass the SELECT before either INSERTed, so the
-- comment was moderated twice (wasted quota) and logged twice.
--
-- The DELETE must run before the index is built: the old race may already have
-- written duplicate rows, and CREATE UNIQUE INDEX would fail on them.
DELETE FROM moderation_log
WHERE id NOT IN (
    SELECT MIN(id) FROM moderation_log GROUP BY project_id, comment_id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_moderation_log_comment_unique
    ON moderation_log(project_id, comment_id);
