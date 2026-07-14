-- Migration 033: a human-readable progress line for background auto-action work.
--
-- ``auto_action_state`` says WHICH step is running ('transcribing',
-- 'generating_desc', …) but has no room for how far along it is. On-device
-- transcription of a long episode runs for minutes, and the only signal the
-- transcriber gives us is a (finalized_seconds, total_seconds) callback — so
-- without somewhere to put it, the UI can only say "Transcribing…" and hope
-- the user waits.
--
-- The Generate-from-source flow solved this by baking the percent into an
-- in-memory job dict, which works because that screen owns the job for its
-- lifetime. The video detail page can't: the user is explicitly allowed to
-- navigate away and come back, so the progress line has to outlive the
-- request that started it. Hence a column rather than another in-memory map.
--
-- NULL means "no progress line" — either nothing is running, or the running
-- step reports no granular progress (the Whisper backends don't).

ALTER TABLE videos ADD COLUMN auto_action_progress_message TEXT;
