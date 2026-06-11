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

MAX_RAMP_SECONDS = 0.50       # cap so a long pause doesn't become dead air
MIN_RAMP_SECONDS = 0.02       # fallback when a neighbour word is contiguous/absent


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


def numbered_units_block(units: list[ClipUnit]) -> str:
    """The transcript as the LLM sees it: ``<index>\\t(<dur>s)\\t<text>`` per line.

    The duration is a length (not an absolute position) so the model can keep a
    clip inside its kind's window without doing timestamp math.
    """
    return "\n".join(
        f"{u.index}\t({round(u.duration)}s)\t{u.text}" for u in units
    )


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


def compute_edges(units: list[ClipUnit], first_index: int, last_index: int) -> ClipEdges:
    """Place the cut edges in the inter-word gaps with short ramps.

    START: cut at the prior word's end and ramp UP across the gap, reaching full
    gain at our first word's onset. END: ramp DOWN from our last word's end across
    the gap and cut at the next word's onset. Ramp length is the gap itself,
    capped at ``MAX_RAMP_SECONDS``. Anchoring on the neighbouring words' boundaries
    means a neighbouring phrase is naturally excluded (it belongs to the adjacent
    unit, which ends before our cut point).
    """
    a, b = units[first_index - 1], units[last_index - 1]
    prev_end = units[first_index - 2].end if first_index > 1 else None
    next_start = units[last_index].start if last_index < len(units) else None

    head_gap = (a.start - prev_end) if prev_end is not None else 0.0
    if head_gap > 0:
        fade_in = min(head_gap, MAX_RAMP_SECONDS)
        final_start = a.start - fade_in
    else:
        fade_in = MIN_RAMP_SECONDS
        final_start = a.start - MIN_RAMP_SECONDS

    tail_gap = (next_start - b.end) if next_start is not None else 0.0
    if tail_gap > 0:
        fade_out = min(tail_gap, MAX_RAMP_SECONDS)
        final_end = b.end + fade_out
    else:
        fade_out = MIN_RAMP_SECONDS
        final_end = b.end + MIN_RAMP_SECONDS

    return ClipEdges(
        final_start=round(max(0.0, final_start), 3),
        final_end=round(final_end, 3),
        fade_in=round(fade_in, 3),
        fade_out=round(fade_out, 3),
    )
