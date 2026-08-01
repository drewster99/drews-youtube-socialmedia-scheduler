"""Word-stream clip selection and edge refinement.

The clip pipeline picks ranges from a transcript by INDEX, not by timestamps or
anchor text: the LLM is shown a numbered list of complete-thought *units* and
returns ``first_index``/``last_index``. We resolve those to sample-accurate times
from the underlying word timestamps, then place the cut edges in the natural
inter-word gaps with short audio ramps.

Why index-based: LLMs are reliable at picking a thought and pointing at it with a
small integer, but unreliable at copying long verbatim anchor text or doing
timestamp arithmetic — the previous anchor-matching scheme mis-located clips for
exactly that reason. All precision (unit boundaries, edge silence, fades) is
derived here from the word timing the transcriber provides.

This module is pure/deterministic and has no media or network dependencies; the
ffmpeg cut that consumes its output lives in ``media.py``.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

from .transcription import TranscriptWord

# --- Unit segmentation tuning ---

_PAUSE_GAP_SECONDS = 0.45      # silence between words that ends a unit
_SOFT_WORD_CAP = 22           # prefer to break here at a clause boundary
_HARD_WORD_CAP = 32           # force a break even mid-clause (continuous speech)
_SENTENCE_END = re.compile(r'[.!?]["”]?$')

# --- Edge-ramp tuning ---
#
# Quantum-aware, derived from clip_proto/pipeline.snap_edges + detect_quantum.
# Apple SpeechAnalyzer quantizes word timestamps to a 60ms grid (Whisper 20ms), so
# a boundary word's stamped start can land up to one quantum after its true onset;
# the HEAD floor guarantees a quantum of lead-in so the first word isn't shaved.

HEAD_PAD_SECONDS = 0.12       # max lead-in silence kept before the first word
QUANTUM_FLOOR_SECONDS = 0.02  # Whisper grid; also the floor when word timings are absent
QUANTUM_CEIL_SECONDS = 0.08   # clamp so a sparse transcript can't over-detect the grid

# Cosine-S (raised-cosine) fade applied at each cut edge, decoupled from the pad.
# The pad only puts the cut in the inter-word INTERVAL, which is not guaranteed
# silent (our word's decay, the next word's onset on a late grid stamp, or a
# breath can sit there), so a fixed short fade smooths each boundary as insurance.
EDGE_FADE_SECONDS = 0.20  # 200ms cosine fade on both edges

# Tail extension + (a) end-snap. The tail takes the ACTUAL inter-word pause,
# floored at TAIL_MIN and capped at TAIL_MAX: a real pause is used in full up to
# the cap; a contiguous tail (no pause) falls back to the floor, bleeding into the
# next word (the cosine fade covers it). (a) end-snap first moves the clip's END
# forward to the nearest unit whose trailing gap is a real pause, so it stops on a
# beat rather than mid-breath — bounded by END_SNAP_MAX_UNITS, and (in the
# validator) never past the kind's duration band.
PAUSE_THRESHOLD_SECONDS = 0.12   # trailing silence that counts as a real pause (for (a))
END_SNAP_MAX_UNITS = 10          # how far (a) may search forward for a pause
TAIL_MIN_SECONDS = 0.30          # minimum tail extension (floor)
TAIL_MAX_SECONDS = 0.50          # take the full pause up to here (cap)


@dataclass
class ClipUnit:
    """One complete-thought unit the LLM selects by 1-based index."""

    index: int
    text: str
    start: float
    end: float
    words: list[TranscriptWord] = field(default_factory=list)

    @property
    def duration(self) -> float:
        return self.end - self.start


def collapse_repeat_loops(words: list[TranscriptWord], max_reps: int = 2) -> list[TranscriptWord]:
    """Strip transcriber repetition hallucinations (e.g. a word or phrase emitted
    dozens of times back-to-back, which some models produce on hard audio).

    Detects a k-gram (k up to 6) that repeats consecutively three or more times
    and keeps only the first ``max_reps`` copies. Real speech rarely repeats a
    phrase verbatim more than twice, so this is safe and removes the degenerate
    spans that would otherwise become a single multi-minute unit.
    """
    keys = [re.sub(r"[^a-z0-9]", "", w.word.lower()) for w in words]
    out: list[TranscriptWord] = []
    i, n = 0, len(words)
    while i < n:
        collapsed = False
        for k in range(1, 7):
            if i + 2 * k > n or keys[i:i + k] != keys[i + k:i + 2 * k]:
                continue
            reps = 2
            while i + (reps + 1) * k <= n and keys[i:i + k] == keys[i + reps * k:i + (reps + 1) * k]:
                reps += 1
            if reps >= 3:
                out.extend(words[i:i + max_reps * k])
                i += reps * k
                collapsed = True
                break
        if not collapsed:
            out.append(words[i])
            i += 1
    return out


def build_units(words: list[TranscriptWord]) -> list[ClipUnit]:
    """Group a flat word stream into complete-thought units.

    A unit boundary falls at a word ending in sentence punctuation, a pause gap
    of at least ``_PAUSE_GAP_SECONDS``, or a hard word cap (so continuous speech
    with no pauses still splits). Each unit therefore begins and ends on a real
    word with a real timestamp.
    """
    words = collapse_repeat_loops(words)
    # Last line of defense before the cut math: a non-finite or out-of-order
    # stamp here would flow into ClipUnit.start/end and then into the ffmpeg
    # -ss/-to timestamps as "nan". The transcriber should already drop these,
    # but build_units is the boundary that feeds media.py so guard it too.
    words = [
        w for w in words
        if math.isfinite(w.start) and math.isfinite(w.end) and w.end >= w.start
    ]
    units: list[ClipUnit] = []
    cur: list[TranscriptWord] = []

    def flush() -> None:
        if not cur:
            return
        text = " ".join(w.word.strip() for w in cur).strip()
        text = re.sub(r"\s+([,.!?])", r"\1", text)
        units.append(ClipUnit(index=len(units) + 1, text=text,
                              start=cur[0].start, end=cur[-1].end, words=list(cur)))
        cur.clear()

    for i, w in enumerate(words):
        cur.append(w)
        ends_sentence = bool(_SENTENCE_END.search(w.word.strip()))
        gap_after = (words[i + 1].start - w.end) if i + 1 < len(words) else 1e9
        if ends_sentence or gap_after >= _PAUSE_GAP_SECONDS:
            flush()
        elif len(cur) >= _SOFT_WORD_CAP and gap_after >= 0.15:
            flush()
        elif len(cur) >= _HARD_WORD_CAP:
            flush()
    flush()
    return units


def numbered_units_block(
    units: list[ClipUnit],
    covered_ranges: list[tuple[float, float]] | None = None,
) -> str:
    """The transcript as the LLM sees it, one unit per line.

    The duration is a length (not an absolute position) so the model can keep a
    clip inside its kind's window without doing timestamp math.

    ``covered_ranges`` are cut ranges of the SAME kind that already exist on
    this parent. Units overlapping one are tagged ``[IN-CLIP]``, which puts
    "you have already been here" into the coordinate system the model is
    actually reasoning in. A list of existing titles cannot do that — it asks
    the model to map a title back to a place in the transcript.

    Same-kind only, deliberately: a hook living inside an existing segment is
    legitimate, so marking segment-covered lines would warn the model off
    ground it should still be using.
    """
    ranges = covered_ranges or []
    out = []
    for u in units:
        covered = any(u.start < end and u.end > start for start, end in ranges)
        out.append(
            f"{u.index}\t({round(u.duration)}s){' [IN-CLIP]' if covered else ''}\t{u.text}"
        )
    return "\n".join(out)


@dataclass
class ResolvedClip:
    first_index: int
    last_index: int
    start: float
    end: float
    text: str

    @property
    def duration(self) -> float:
        return self.end - self.start


def resolve_unit_range(units: list[ClipUnit], first_index: int,
                       last_index: int) -> ResolvedClip | None:
    """Resolve a 1-based inclusive unit range to word-accurate content bounds and
    the exact stitched text. Returns ``None`` for an out-of-range/inverted range
    (a hallucinated index) so the caller can drop it."""
    if not (1 <= first_index <= last_index <= len(units)):
        return None
    a, b = units[first_index - 1], units[last_index - 1]
    text = " ".join(u.text for u in units[first_index - 1:last_index])
    return ResolvedClip(first_index, last_index, a.start, b.end, text)


@dataclass
class ClipEdges:
    """Cut points and ramp lengths for ffmpeg. The clip spans
    ``[final_start, final_end]``; fade IN over the first ``fade_in`` seconds and
    OUT over the last ``fade_out`` seconds."""

    final_start: float
    final_end: float
    fade_in: float
    fade_out: float


def detect_quantum(units: list[ClipUnit]) -> float:
    """Infer the transcriber's timing grid from the data: ~0.06s for Apple
    SpeechAnalyzer, ~0.02s for Whisper.

    A word cannot be narrower than one grid cell, so the smallest word duration
    is the quantum. Clamped to a safe band and falling back to the floor when no
    word timings are available (e.g. cue-only units). Stable across the whole
    transcript, so every clip on a given parent snaps to the same grid.
    """
    durations = [
        w.end - w.start
        for u in units
        for w in u.words
        if (w.end - w.start) > 1e-4
    ]
    if not durations:
        return QUANTUM_FLOOR_SECONDS
    return min(QUANTUM_CEIL_SECONDS, max(QUANTUM_FLOOR_SECONDS, min(durations)))


def gap_after_unit(units: list[ClipUnit], index: int) -> float:
    """Silence (seconds) after the 1-based unit ``index``. The transcript's end
    counts as an infinite pause — there is no next word to run into."""
    if index >= len(units):
        return float("inf")
    return units[index].start - units[index - 1].end


def snap_clip_end_to_pause(
    units: list[ClipUnit], last_index: int,
    *, max_extra_units: int = END_SNAP_MAX_UNITS,
    threshold: float = PAUSE_THRESHOLD_SECONDS,
) -> int:
    """(a) Move a clip's END forward to the nearest unit followed by a real
    pause, so it stops on a beat rather than mid-breath.

    Bounded search: at most ``max_extra_units`` units forward. Returns the
    original ``last_index`` unchanged if it already ends on a pause, or if no
    pause is within reach (better an imperfect end than an unbounded overrun).
    """
    n = len(units)
    for idx in range(last_index, min(last_index + max_extra_units, n) + 1):
        if gap_after_unit(units, idx) >= threshold:
            return idx
    return last_index


def compute_edges(
    units: list[ClipUnit], first_index: int, last_index: int,
    *, quantum: float | None = None,
) -> ClipEdges:
    """Place the cut edges around the boundary words.

    HEAD: reach back into the lead-in silence up to ``HEAD_PAD``, floored at one
    quantum — Apple's 60ms grid means a word's stamped start can sit a quantum
    late, so the floor keeps the first word's onset from being shaved.

    TAIL: extend by the ACTUAL inter-word pause, floored at ``TAIL_MIN`` and
    capped at ``TAIL_MAX``. A real pause is used in full up to the cap; a
    contiguous tail (no pause) falls back to the floor, which bleeds into the
    next word — the cosine fade covers that. The transcript end is an unlimited
    pause (takes the cap). (End-snapping the last unit to a real pause is done
    upstream in the validator; this just computes the cut for whatever end it is
    handed.)

    The fade is a fixed ``EDGE_FADE_SECONDS`` cosine ramp, decoupled from the
    room — the room decides where the cut lands, the fade smooths it. Pass
    ``quantum`` to reuse a grid already detected for this transcript.
    """
    q = detect_quantum(units) if quantum is None else quantum
    a, b = units[first_index - 1], units[last_index - 1]

    # Room to the nearest neighbouring word. A missing neighbour means the clip
    # touches the transcript edge (unlimited room).
    prev_end = units[first_index - 2].end if first_index > 1 else None
    next_start = units[last_index].start if last_index < len(units) else None
    head_gap = (a.start - prev_end) if prev_end is not None else a.start
    tail_gap = (next_start - b.end) if next_start is not None else TAIL_MAX_SECONDS

    head_room = min(HEAD_PAD_SECONDS, max(q, head_gap))
    tail_room = min(TAIL_MAX_SECONDS, max(TAIL_MIN_SECONDS, tail_gap))

    return ClipEdges(
        final_start=round(max(0.0, a.start - head_room), 3),
        final_end=round(b.end + tail_room, 3),
        fade_in=EDGE_FADE_SECONDS,
        fade_out=EDGE_FADE_SECONDS,
    )
