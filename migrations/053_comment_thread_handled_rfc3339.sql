-- Convert comment_thread_state.handled_at to the format it is COMPARED against.
--
-- It was written with SQLite's `YYYY-MM-DD HH:MM:SS`, but the read path compares
-- it as a string against `youtube_comments.published_at`, which is YouTube's
-- RFC3339 `YYYY-MM-DDTHH:MM:SSZ`. At index 10 that is a space (0x20) against a
-- 'T' (0x54), and space sorts LOWER — so `handled_at >= last_any_at` was false
-- for every thread whose newest activity fell on the SAME DATE, however much
-- later in the day the mark was made.
--
-- The effect was that marking a thread handled worked only when its newest
-- comment was from an earlier date, and silently did nothing for anything active
-- today: the API returned success, the row was written, and the thread went on
-- saying "Needs reply". The most recently active thread is precisely the one a
-- user clears last, so the failure showed up at the end of the list.
--
-- Fixed columns are only comparable to columns in the SAME shape. Rather than
-- normalising at every read, the stored value now matches its comparand
-- exactly, and existing rows are converted here: replace the separator, append
-- the zone. A row already in RFC3339 (none, at the time of writing, but the
-- guard costs nothing) is left alone.

UPDATE comment_thread_state
   SET handled_at = REPLACE(handled_at, ' ', 'T') || 'Z'
 WHERE handled_at LIKE '% %';
