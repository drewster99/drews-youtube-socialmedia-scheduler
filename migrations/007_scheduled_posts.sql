-- Per-post scheduling support: each social_post can have its own DateTrigger
-- in APScheduler, decoupled from the video's publish time.

ALTER TABLE social_posts ADD COLUMN scheduled_at TEXT;
ALTER TABLE social_posts ADD COLUMN scheduler_job_id TEXT;
ALTER TABLE social_posts ADD COLUMN social_account_id INTEGER REFERENCES social_accounts(id) ON DELETE SET NULL;

CREATE INDEX idx_social_posts_scheduled ON social_posts(scheduled_at);
