-- Track when auto-tag generation last ran so the auto-action chain
-- can skip the step on restart (mirrors description_generated_at).
ALTER TABLE videos ADD COLUMN tags_generated_at TEXT;
