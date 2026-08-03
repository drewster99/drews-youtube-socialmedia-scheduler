-- Local store of every YouTube comment on a project's channel.
--
-- Comments were previously only ever fetched live, per video, from the video
-- detail page — so "what has anyone said anywhere on this channel lately?" had
-- no answer short of opening every video in turn. moderation_log is not that
-- store: it records only the comments that MATCHED a blocklist keyword, which
-- is the small hostile subset, not the conversation.
--
-- A project is bound 1:1 to a youtube_channel_id, so one
-- commentThreads.list(allThreadsRelatedToChannelId=...) page covers the whole
-- project for 1 quota unit. A periodic sweep upserts into this table and the
-- dashboard reads it — the page renders from SQLite and never waits on YouTube.
--
-- first_seen_at is when WE first stored the row, deliberately distinct from
-- published_at (when YouTube says it was written). A comment that arrives on an
-- old thread is new to us the moment we see it, and "everything whose
-- first_seen_at is past the last notification watermark" is the query a push
-- notifier will want. Nothing reads it yet.

CREATE TABLE IF NOT EXISTS youtube_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,

    -- YouTube's comment id. Unique per channel; scoped by project anyway so a
    -- project delete can never strand another project's rows.
    comment_id TEXT NOT NULL,

    -- The YouTube video the comment sits on. NOT a videos.id: the channel may
    -- hold videos this app never tracked, and those comments still count.
    -- Joined to videos.youtube_video_id (migration 037) at read time when a
    -- local row exists.
    --
    -- NULL states a real thing, not a missing one: a channel-wide sweep also
    -- returns comments posted on the CHANNEL rather than on any video, and
    -- those have no video id to record. Dropping them to keep this column NOT
    -- NULL would lose real comments the user wants to see.
    youtube_video_id TEXT,

    -- NULL for a top-level comment; the top-level comment's id for a reply.
    -- This is the row's kind, stated rather than inferred.
    parent_comment_id TEXT,

    author_display_name TEXT NOT NULL,
    -- Nullable: YouTube omits the author channel for some commenters. It is
    -- what identifies the channel owner's own replies at read time (compared
    -- against projects.youtube_channel_id), so NULL there means "not known to
    -- be the owner", which is the honest answer.
    author_channel_id TEXT,
    author_profile_image_url TEXT,

    -- textDisplay, as returned with textFormat=plainText.
    text_display TEXT NOT NULL,
    like_count INTEGER NOT NULL DEFAULT 0,

    -- totalReplyCount, top-level rows only; NULL on a reply row. Used to spot
    -- threads whose replies the thread preview truncated.
    total_reply_count INTEGER,

    -- RFC3339 UTC, verbatim from YouTube. Z-suffixed, so lexicographic
    -- ordering is chronological ordering.
    published_at TEXT NOT NULL,
    -- Changes when the author edits the comment; drives the upsert's decision
    -- to rewrite the text.
    youtube_updated_at TEXT NOT NULL,

    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_synced_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- The sweep re-reads the same comments every tick, so the upsert target must be
-- exact. This index is what makes ON CONFLICT work.
CREATE UNIQUE INDEX IF NOT EXISTS idx_youtube_comments_unique
    ON youtube_comments(project_id, comment_id);

-- The dashboard's only query: newest first, one project.
CREATE INDEX IF NOT EXISTS idx_youtube_comments_recent
    ON youtube_comments(project_id, published_at DESC);

-- Per-video lookups (a video's comment count, and the title join's reverse).
CREATE INDEX IF NOT EXISTS idx_youtube_comments_video
    ON youtube_comments(project_id, youtube_video_id);
