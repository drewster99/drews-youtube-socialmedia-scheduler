-- The outcome of the last channel-comment sweep, per project.
--
-- The sweep runs on a 4-hourly background job, which means it fails with no
-- page open — the exact case the working agreement calls unsurfaced. Until now
-- a revoked token, a declined moderation bucket, or a sweep that stopped at its
-- page cap reached only server.log; the dashboard went on rendering the stale
-- mirror and saying "Synced 4 hours ago", which is the misleading-fine state.
--
-- One row per project, overwritten each sweep: this exists so the UI can show
-- what happened LAST, not to accumulate history. server.log keeps the history.
--
-- `finished_at` is NULL for a sweep that raised part-way, which is how a sweep
-- that died is told apart from one that finished badly. No row is written at
-- sweep START, so "currently running" is deliberately NOT representable here —
-- an in-flight sweep is guarded by an in-process lock, not by this table.
-- `ok` is whether the sweep ran to completion at all; `was_complete` is the
-- stricter fact that it also read the channel's whole listable surface (no
-- truncation, no bucket error, no thread left owing replies) — only that kind
-- of sweep may conclude a comment has disappeared from YouTube.
--
-- `detail` is the summary dict as JSON, stored verbatim so a new field in the
-- summary needs no migration and nothing about a sweep is silently dropped.
--
-- `swept_at` / `previous_swept_at` are the last two stamps written to
-- `youtube_comments.last_seen_in_sweep_at`, and they are here because that
-- history CANNOT be recovered from the comments table. A comment is only called
-- "gone from YouTube" once it has been missed by the last TWO stamping sweeps,
-- so the yardstick is the older of these two — but every sweep overwrites the
-- stamp on every comment it sees, so the previous sweep's value stops appearing
-- on any row. What is left there is the newest stamp plus whatever stale value
-- the missing comment still carries, and a value can never be older than
-- itself. Hence: record the sweeps, not only their effects.
--
-- Both are NULL until a sweep has actually stamped, and a sweep that stamps
-- nothing leaves them untouched rather than clearing them.

CREATE TABLE IF NOT EXISTS comment_sweep_runs (
    project_id INTEGER PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    ok INTEGER NOT NULL DEFAULT 0,
    was_complete INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    detail TEXT,
    swept_at TEXT,
    previous_swept_at TEXT
);
