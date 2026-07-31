-- Persist the audio edge-ramp lengths on a dismissed clip proposal.
--
-- A word-stream proposal carries audio_fade_in / audio_fade_out: short gain
-- ramps placed in the inter-word gaps so a cut doesn't start or end on an
-- audible pop. The review page already SENDS these when a clip is dismissed,
-- but generate_rejections had nowhere to store them, so Restore brought the
-- clip back with zero fades and the cut popped. These columns close that gap.
--
-- NULL means "no fade recorded" — pre-migration rejection rows, which the cut
-- path already treats as 0 (its existing default). New rejections store the
-- real values.

ALTER TABLE generate_rejections ADD COLUMN audio_fade_in REAL;
ALTER TABLE generate_rejections ADD COLUMN audio_fade_out REAL;
