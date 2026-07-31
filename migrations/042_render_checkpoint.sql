-- Per-slot render checkpoint so a retry reuses an already-rendered slot
-- instead of re-paying for the Anthropic round-trip that produced it.
--
-- Accept renders each enabled slot for a video one at a time, and each render
-- can fire a real {{ai:}} call (real money). If a later slot raises a
-- TRANSIENT error the whole video is abandoned and every already-rendered slot
-- is discarded; the next attempt (a fresh Accept, or a rerender reconcile job)
-- re-renders all of them from scratch, re-paying for the ones that already
-- succeeded. This table lets _render_slot hand back a prior success instead.
--
-- input_hash is a SHA-256 over EVERYTHING that determines the rendered output
-- (video id, slot id, the media-stripped slot body, the resolved variables,
-- the default AI system prompt, the media paths, and the effective model). A
-- reuse is allowed ONLY when the current inputs hash to the stored value, so a
-- template body edit — the whole reason reconciliation exists — changes the
-- hash and forces a fresh render. The cache is never the source of truth for
-- what gets scheduled: social_posts.content stays authoritative; this row only
-- spares a re-render when the inputs are provably identical.
--
-- Keyed on (video_id, slot_id): one live render per slot per video. A body
-- edit keeps the slot id, so its row is overwritten (input_hash no longer
-- matches → miss → re-render → replace). Both foreign keys cascade-delete so a
-- removed slot or video leaves no orphan checkpoint behind.

CREATE TABLE IF NOT EXISTS render_checkpoint (
    video_id          TEXT    NOT NULL,
    slot_id           INTEGER NOT NULL,
    input_hash        TEXT    NOT NULL,
    content           TEXT    NOT NULL,
    -- JSON array of the media file paths the render resolved. Used only to
    -- gate reuse: a cached render whose media file was since cleaned up must
    -- MISS rather than hand back a dangling path.
    media_paths_json  TEXT    NOT NULL DEFAULT '[]',
    created_at        TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (video_id, slot_id),
    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
    FOREIGN KEY (slot_id) REFERENCES template_slots(id) ON DELETE CASCADE
);
