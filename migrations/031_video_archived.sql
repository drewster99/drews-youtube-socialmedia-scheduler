-- Non-destructive cleanup for duplicate / unwanted promo clips: a reversible
-- "archived" flag instead of deletion (we never delete user data). An archived
-- clip drops off the Promo Videos page but stays in the DB and on YouTube, and
-- can be restored from the page's "Show archived" view.
ALTER TABLE videos ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;
ALTER TABLE videos ADD COLUMN archived_at TEXT;

-- One-time backfill: archive an imported clip that exactly duplicates a
-- generated one. The Generate-from-source dedup could not see imported clips
-- (they carry no real cut range -- 0/0 or NULL -- because a YouTube import
-- cannot know the source boundaries), so it re-cut moments an import already
-- covered. Archive the range-less import whenever a same-parent, same-kind
-- clip with the IDENTICAL title AND a real (non-degenerate) cut range exists,
-- keeping the properly ranged generated version. Reversible via Unarchive.
UPDATE videos SET archived = 1, archived_at = datetime('now')
WHERE COALESCE(archived, 0) = 0
  AND parent_item_id IS NOT NULL
  AND (cut_start_seconds IS NULL OR cut_start_seconds = cut_end_seconds)
  AND EXISTS (
      SELECT 1 FROM videos sibling
      WHERE sibling.parent_item_id = videos.parent_item_id
        AND sibling.item_type      = videos.item_type
        AND sibling.title          = videos.title
        AND sibling.id            <> videos.id
        AND sibling.cut_start_seconds IS NOT NULL
        AND sibling.cut_end_seconds   IS NOT NULL
        AND sibling.cut_start_seconds <> sibling.cut_end_seconds
  );
