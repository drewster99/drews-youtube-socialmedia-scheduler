-- A scheduled YouTube publish that failed, recorded where the UI can see it.
--
-- When publish_video_job's YouTube step failed, the failure lived in one log
-- line (plus a Log event only when it looked auth-shaped). The video kept
-- status='scheduled' with a past publish_at — which no page reads as trouble —
-- and its approved social posts sat unsent. Nothing retried until the next app
-- RESTART, whose missed-publish recovery could be days away. A video could
-- quietly miss its date while the app looked idle and content.
--
-- Same design as social_posts.failed_at / error (the failed-sends banner):
-- the row itself is the single source of truth, a banner reads it app-wide,
-- and clearing happens only when something real resolves it — a successful
-- publish, a fresh schedule, or the user cancelling the schedule.
--
-- Both NULL means "no failed publish outstanding". They are set and cleared
-- together; publish_error without a stamp would be an unclearable ghost.

ALTER TABLE videos ADD COLUMN publish_failed_at TEXT;
ALTER TABLE videos ADD COLUMN publish_error TEXT;
