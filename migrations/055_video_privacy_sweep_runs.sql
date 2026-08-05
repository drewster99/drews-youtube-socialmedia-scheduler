-- The outcome of the last video-privacy sweep, per project.
--
-- The sweep runs unattended on a timer, so a failure happens with nobody
-- watching. Logging it is not enough: nothing on any page would say the check
-- had stopped working, and the two guards that read `videos.privacy_status`
-- would go on trusting a value nothing had verified for days. That is the same
-- unsurfaced-error case `comment_sweep_runs` exists for, and this table is
-- deliberately its twin rather than a new shape to learn.
--
-- Why a table and not "derive it from privacy_synced_at": a stamp that is old
-- cannot distinguish "the sweep is broken" from "this video is far down a
-- rotation that is working fine" — the sweep is capped per run, so the oldest
-- stamp in a large library is legitimately old at all times. The failure has to
-- be recorded as its own fact.
--
-- `consecutive_failures` is what makes the surface honest. One failed sweep is
-- routine — a laptop asleep, a token mid-refresh, a flaky network — and a banner
-- for every one of those is a banner nobody reads. What matters is a check that
-- has stopped working, so the UI waits for a run of them.

CREATE TABLE IF NOT EXISTS video_privacy_sweep_runs (
    project_id INTEGER PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    ok INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    -- Reset to 0 by any successful sweep. Never cleared by a failure, so a
    -- flapping check still accumulates rather than resetting itself to
    -- invisibility on each brief recovery.
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    -- When this project's privacy was last read from YouTube SUCCESSFULLY.
    -- Distinct from finished_at, which moves on a failed run too: the pair is
    -- what lets the UI say "failing for 6 hours" rather than only "failing".
    last_success_at TEXT,
    detail TEXT
);
