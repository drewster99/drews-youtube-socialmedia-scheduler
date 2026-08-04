-- "I have dealt with this thread", for the cases YouTube cannot tell us about.
--
-- The dashboard decides a thread needs a reply when its newest visible comment
-- is not the channel's and the channel has not thumbs-upped it. That covers the
-- two signals the Data API exposes, and misses every other way a human resolves
-- a conversation:
--
--   * the CREATOR HEART — a real acknowledgement in the YouTube UI with NO
--     representation in the Data API at all. A hearted comment is byte-identical
--     to an untouched one from here, so it nags forever.
--   * replying from the YouTube app on a phone in a way we have not swept yet.
--   * a comment that simply does not warrant an answer.
--
-- Keyed by thread, not by comment: "needs a reply" is a property of the
-- conversation (its newest visible comment), so anything else would be marking
-- a row that is not the one being judged. Its own table rather than a column on
-- `youtube_comments` because a thread whose top-level comment the blocklist
-- rejected still renders — and has no row to hang the flag on.
--
-- `handled_at` is COMPARED against the thread's newest activity rather than
-- cleared by it: a thread counts as handled only while nothing has happened
-- since. A new reply therefore un-handles it automatically, with no writer to
-- remember and no sweep to keep in sync — the same discipline that makes
-- `mark_failed` clearing `dismissed_at` safe. You can silence THIS exchange,
-- never the next one.

CREATE TABLE IF NOT EXISTS comment_thread_state (
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    -- COALESCE(parent_comment_id, comment_id) — the same key the listing groups
    -- on. Not a foreign key: an orphan thread's key names a comment we do not
    -- hold, which is exactly the case a column on youtube_comments could not
    -- express.
    thread_key TEXT NOT NULL,
    handled_at TEXT NOT NULL,
    PRIMARY KEY (project_id, thread_key)
);
