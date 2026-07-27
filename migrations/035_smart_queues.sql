-- Migration 035: smart queues — project-wide social posting of promo clips.
--
-- A queue holds an ordered list of videos and a weekly recurrence. At each
-- recurrence slot it posts the next video to every enabled slot of its
-- template, attaching the video file itself.
--
-- See SMART_QUEUE.md for the full design.

CREATE TABLE smart_queues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,

    -- RESTRICT, not CASCADE: a queue cannot function without its template, and
    -- silently destroying a queue (and its pending schedule) because someone
    -- tidied up a template would be far worse than a refused delete that says
    -- which queue is using it.
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE RESTRICT,

    -- IANA zone name, e.g. 'America/Los_Angeles'. Required because "9:00am"
    -- has to be interpreted against something. A fixed UTC offset would drift
    -- an hour across a DST boundary; occurrences are enumerated as local dates
    -- and converted individually, so each stamped instant is right for its own
    -- date.
    timezone TEXT NOT NULL,

    -- Selection filters. item_type is deliberately absent: the template's
    -- applies_to is the single source of truth for which types a queue can
    -- touch, so a queue can never disagree with its own template.
    min_duration_seconds REAL NOT NULL DEFAULT 0,
    max_duration_seconds REAL NOT NULL DEFAULT 180,
    orientations TEXT NOT NULL DEFAULT '["portrait","square"]',
    exclude_already_posted INTEGER NOT NULL DEFAULT 1,

    auto_add_on_live INTEGER NOT NULL DEFAULT 1,

    -- post_late | reschedule_end | remove. missed_grace_hours applies only to
    -- post_late and is NULL otherwise, rather than carrying a meaningless
    -- number for the other two.
    missed_policy TEXT NOT NULL DEFAULT 'post_late',
    missed_grace_hours INTEGER,

    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (project_id, name)
);
CREATE INDEX idx_smart_queues_project ON smart_queues(project_id);

-- One row per (weekday, time). Any number per day, including zero. Weekly
-- recurrence only — no every-other-week, no blackout dates.
CREATE TABLE smart_queue_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES smart_queues(id) ON DELETE CASCADE,
    -- 0 = Monday, matching date.weekday(). The CHECK is the backstop for the
    -- read side: occurrences() walks forward a day at a time looking for a
    -- matching weekday, so a value no date can ever equal (7, -1) marches the
    -- calendar to the year 9999 and dies with OverflowError. Column affinity
    -- runs BEFORE the CHECK, so 3.0 and '3' land as INTEGER 3 and pass here —
    -- _parse_weekday is what refuses those and bools.
    weekday INTEGER NOT NULL
        CHECK (typeof(weekday) = 'integer' AND weekday BETWEEN 0 AND 6),
    time_of_day TEXT NOT NULL,       -- 'HH:MM', local to the queue's timezone
    UNIQUE (queue_id, weekday, time_of_day)
);
CREATE INDEX idx_smart_queue_slots_queue ON smart_queue_slots(queue_id);

-- An item is an OCCURRENCE, not a membership: one row per time a video is
-- added to a queue. No unique constraint on (queue_id, video_id), because
-- recycling a previously-posted clip appends a new row and the history keeps
-- both. That is what makes "exclude already posted" a filter rather than a
-- special case.
CREATE TABLE smart_queue_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES smart_queues(id) ON DELETE CASCADE,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    -- UTC ISO, same convention as videos.publish_at and
    -- social_posts.scheduled_at. NULL while the item is still 'queued'.
    scheduled_at TEXT,
    -- queued | scheduled | posted | failed | skipped | removed
    --
    -- 'queued' means in the queue with no posting time yet — what auto-add
    -- appends when a video goes live. Accept is the only thing that promotes
    -- it to 'scheduled', so a posting time is decided in one place.
    state TEXT NOT NULL DEFAULT 'scheduled',
    -- Why it was skipped or failed, in the user's words. NULL otherwise.
    reason TEXT,
    added_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_smart_queue_items_queue ON smart_queue_items(queue_id, position);
CREATE INDEX idx_smart_queue_items_video ON smart_queue_items(video_id);
CREATE INDEX idx_smart_queue_items_state ON smart_queue_items(queue_id, state);

-- Ties a posting back to the queue item that produced it without duplicating
-- any posting machinery: queue items create ordinary social_posts rows, so
-- send, retry, duplicate detection, history, and the missed-backlog guard all
-- apply unchanged. SET NULL so deleting a queue keeps its posting history.
ALTER TABLE social_posts ADD COLUMN smart_queue_item_id INTEGER
    REFERENCES smart_queue_items(id) ON DELETE SET NULL;
CREATE INDEX idx_social_posts_smart_queue_item
    ON social_posts(smart_queue_item_id);
