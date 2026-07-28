-- Template edits have to reach schedules that were already built from them.
--
-- Slot membership and post text are both decided at Accept, so a later
-- template change never reached items already on the books: adding a slot left
-- every scheduled item without it, removing one left orphaned posts that still
-- went out, and editing a slot's body changed nothing that was already
-- rendered. None of it was visible until a post you expected never appeared,
-- or one you deleted did.
--
-- Reconciliation runs as persisted jobs rather than inside the save request:
-- each one re-renders N posts with an AI round-trip apiece, which is minutes of
-- work that must not die with the HTTP connection or a restart.

CREATE TABLE IF NOT EXISTS smart_queue_reconcile_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id        INTEGER NOT NULL,
    -- slots_added | slots_removed | slot_body_changed | applies_to_removed
    kind            TEXT    NOT NULL,
    -- JSON; shape depends on kind (slot ids, video ids)
    payload         TEXT    NOT NULL DEFAULT '{}',
    -- pending | running | done | failed
    status          TEXT    NOT NULL DEFAULT 'pending',
    progress_done   INTEGER NOT NULL DEFAULT 0,
    progress_total  INTEGER NOT NULL DEFAULT 0,
    -- Human-readable summary of what happened, shown once the job finishes.
    detail          TEXT,
    last_error      TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    started_at      TEXT,
    finished_at     TEXT,
    FOREIGN KEY (queue_id) REFERENCES smart_queues(id) ON DELETE CASCADE
);

-- The worker claims the oldest pending job; the banner and the edit lock both
-- ask "is anything unfinished".
CREATE INDEX IF NOT EXISTS idx_sq_reconcile_status
    ON smart_queue_reconcile_jobs(status, id);

CREATE INDEX IF NOT EXISTS idx_sq_reconcile_queue
    ON smart_queue_reconcile_jobs(queue_id, status);
