"""Generate-from-source clip proposals.

Each kind (hook / short / segment) is its own Claude call. The prompt is
split three ways:

- **System, in code** (``_build_index_system_text``) — the role, the
  transcript's line format, and the tool contract. Not editable, so no
  prompt edit can break the output format.
- **System, editable** — the editorial middle: what makes a good clip of
  this kind and how its title should read. Lives in ``prompt_templates``
  under ``promo_clip_proposals_<kind>`` (seed default in
  ``services/prompts``) and is spliced in by
  ``editorial_block_for_kind``.
- **User** (``_build_index_user_text``) — the run's material: parent
  title, clips already covered, the count asked for, the transcript.

The Claude response is structured: the model is forced to call the
``propose_clips`` tool with a strict JSON-Schema-validated payload, so
we never have to text-parse a JSON blob.

Per-kind constraints. The bands touch at their endpoints, but nothing
reclassifies across them: each kind is its own Claude call, the validator
runs with a fixed ``kind``, and a proposal outside that kind's window is
DROPPED, not handed to the neighbouring kind. Adjacency is not a safety
net — a 90 s "hook" is discarded even when shorts were also requested.

| Kind    | Min (s) | Max (s)             | Default output cap |
|---------|---------|---------------------|--------------------|
| hook    | 5       | 60                  | 20                 |
| short   | 60      | 180                 | 15                 |
| segment | 180     | 75% of parent       | 8                  |

The caps are ``_DEFAULT_MAX_PER_KIND``; the user can override each one in
the Generate modal, up to ``MAX_PROPOSALS_PER_KIND_CAP``.

Server-side post-validation drops any proposal that:

- falls outside the kind's length band,
- starts before 0 or ends past the parent's duration,
- overlaps an existing same-kind cut range on this parent by more than
  ``_MAX_OVERLAP_FRACTION`` of the shorter of the two clips,
- has a title near-identical to an existing same-kind clip on this parent
  (imported clips carry no cut range, so the range check can't see them),
- exceeds that kind's output cap.

A parent too short to host a kind is ineligible for it — see
``is_parent_eligible_for_kind``. The calling endpoint pre-flights this and
never asks for a kind it can't satisfy.
"""

from __future__ import annotations

import asyncio
import difflib
import logging
import math
import re
import secrets
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal

from yt_scheduler.config import CLIP_PROPOSAL_TIMEOUT_SECONDS, UPLOAD_DIR
from yt_scheduler.services import ai, clip_edges, media as media_service
from yt_scheduler.services.background import spawn_background
from yt_scheduler.services.clip_edges import ClipEdges, ClipUnit

logger = logging.getLogger(__name__)

ClipKind = Literal["hook", "short", "segment"]

_PER_KIND_BOUNDS: dict[ClipKind, tuple[float, float | None]] = {
    "hook": (5.0, 60.0),
    "short": (60.0, 180.0),
    # No fixed max for segments — capped at a fraction of the parent instead
    # (see _SEGMENT_MAX_FRACTION_OF_PARENT_DURATION) so a segment can be long
    # but not the whole video.
    "segment": (180.0, None),
}

# A "segment" can run long, but not be (nearly) the entire parent video.
_SEGMENT_MAX_FRACTION_OF_PARENT_DURATION: float = 0.75

# Parent must be at least kind_max + this much longer than the longest
# clip we'd cut. Generated promos are most useful when they're materially
# shorter than the parent; without this guard a 61-second parent could
# emit a 60-second "hook" that's effectively the whole video.
_PARENT_HEADROOM_SECONDS: float = 15.0

# Absolute ceiling we'll honour for a per-kind cap, whatever the caller asks
# for — also the upper bound on the Generate modal's number input.
MAX_PROPOSALS_PER_KIND_CAP: int = 50
# How many proposals to request per kind when the caller didn't say. Read it
# through ``default_max_proposals_for_kind``, never directly.
_DEFAULT_MAX_PER_KIND: dict[ClipKind, int] = {"hook": 20, "short": 15, "segment": 8}

# Output budget for one proposal call, derived rather than picked: a proposal
# costs ~120 output tokens (measured), so a full batch at the cap needs ~6k.
# 500 apiece is deliberate slack — roughly 4x measured — so a batch of longer
# reasons and titles still can't run into the ceiling.
#
# This is a CEILING, not an expectation — real responses land at 1-6k. The
# Anthropic SDK conflates the two: for a non-streaming call it estimates
# duration as ``3600 * max_tokens / 128_000`` and refuses above ~21,333,
# which punishes exactly the generous ceiling you want in order to never
# truncate. That guess is skipped entirely when the caller supplies its own
# timeout, which ``propose_clips_for_kind_indexed`` does
# (``config.CLIP_PROPOSAL_TIMEOUT_SECONDS``), so the number below answers to
# our needs rather than to the SDK's heuristic.
_OUTPUT_TOKENS_PER_PROPOSAL: int = 500
PROPOSAL_MAX_OUTPUT_TOKENS: int = (
    MAX_PROPOSALS_PER_KIND_CAP * _OUTPUT_TOKENS_PER_PROPOSAL
)
# When a kind already has cut clips on this parent, we ask Claude for a few
# extra candidates so that after post-LLM dedup/overlap removal we still have a
# full set of fresh ones. The final output is still capped at the base max.
_EXISTING_OVERREQUEST_BONUS: int = 5

# Overlap with an existing same-kind range that exceeds this fraction of
# the SHORTER of the two clips → drop the proposal. Measuring against the
# shorter clip (not just the proposal) keeps a long proposal from fully
# swallowing a short existing clip and still passing because the overlap
# was a small fraction of its own length. The threshold is loose on
# purpose: small head/tail overlaps that produce a meaningfully different
# clip are fine; near-duplicates are not.
_MAX_OVERLAP_FRACTION: float = 0.5

# Threshold for treating a proposed title as a duplicate of an existing
# same-kind clip's title. High on purpose: it should catch near-identical
# titles ("Claude Nuked My Database" vs "Claude Nuked My Production
# Database") without suppressing genuinely different clips that happen to
# share a few words.
_TITLE_SIMILARITY_THRESHOLD: float = 0.8


class RejectionReason(str, Enum):
    """Why the server refused a clip Claude proposed.

    A code rather than a bare sentence so the UI can group and the tests can
    assert on the decision instead of on its wording.
    """

    INVALID_INDICES = "invalid_indices"
    INDEX_OUT_OF_BOUNDS = "index_out_of_bounds"
    DURATION_OUT_OF_BAND = "duration_out_of_band"
    DUPLICATE_TITLE = "duplicate_title"
    TITLE_LENGTH = "title_length"
    OVERLAPS_EXISTING = "overlaps_existing"
    OVER_OUTPUT_CAP = "over_output_cap"


@dataclass(frozen=True)
class RejectedProposal:
    """A clip Claude proposed that the server refused, and why.

    Exists so a discarded proposal reaches the screen. Before this, the count
    on the review page was the only signal — 23 proposals arriving and 7
    surviving looked identical to Claude finding 7, and the reason lived only
    in the server log.
    """

    kind: ClipKind
    reason: RejectionReason
    detail: str
    title: str = ""
    first_index: int | None = None
    last_index: int | None = None
    duration_seconds: float | None = None


@dataclass(frozen=True)
class KindProposals:
    """Everything that happened to one kind's proposal pass.

    ``error`` is the absence of a run (the Claude call failed); rejections are
    a successful run's refusals. They are different facts and must not be
    collapsed — an error means the count is unknown, not zero. ``raw_count``
    is what Claude actually returned, which is what lets the UI say "23
    proposed, 7 kept" instead of "no proposals".
    """

    kind: ClipKind
    accepted: list[ProposedClip]
    rejected: list[RejectedProposal]
    error: str | None = None
    raw_count: int = 0

    def __post_init__(self) -> None:
        if self.error is not None and (self.accepted or self.rejected):
            raise ValueError(
                f"KindProposals for {self.kind} has an error and also results; "
                "a failed pass produced neither."
            )
        # Every proposal Claude sent is either accepted or rejected. The UI
        # subtracts these to say "N proposed, M kept", so a proposal that
        # vanished without a reason would make that arithmetic quietly lie.
        if self.error is None and self.raw_count != len(self.accepted) + len(self.rejected):
            raise ValueError(
                f"KindProposals for {self.kind}: raw_count={self.raw_count} but "
                f"{len(self.accepted)} accepted + {len(self.rejected)} rejected — "
                "a proposal was discarded without being recorded."
            )

    @classmethod
    def failed(cls, kind: ClipKind, error: str) -> "KindProposals":
        return cls(kind=kind, accepted=[], rejected=[], error=error)


@dataclass(frozen=True)
class ProposedClip:
    """One candidate range Claude wants us to cut.

    Times are in seconds, sample-accurate (the prompts instruct Claude not
    to round). ``title`` and ``reason`` flow straight from the model and
    are surfaced in the preview cards.
    """

    kind: ClipKind
    start_seconds: float
    end_seconds: float
    title: str
    reason: str
    # ``rating`` is the model's 1-4 self-score; the fade lengths drive the
    # audio ramps at cut time (see media.extract_clip).
    rating: int | None = None
    audio_fade_in: float = 0.0
    audio_fade_out: float = 0.0

    @property
    def duration_seconds(self) -> float:
        return self.end_seconds - self.start_seconds


def _format_duration_human(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _overlap_seconds(
    a_start: float, a_end: float, b_start: float, b_end: float,
) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


_PUNCT_STRIP_RE = re.compile(r"[^\w\s]")


def _normalize_title_for_match(title: str) -> str:
    """Lower-case, strip punctuation, collapse whitespace — so casing and
    punctuation differences never defeat the duplicate-title check."""
    cleaned = _PUNCT_STRIP_RE.sub(" ", title.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _titles_similar(a: str, b: str) -> bool:
    """True when two clip titles are close enough to call duplicates.

    Compares normalized titles two ways and accepts either: character-level
    ``SequenceMatcher`` ratio (catches insertions — "Claude Nuked My
    [Production] Database") and word-set Jaccard (catches reorderings).
    This guard exists because imported clips carry no cut range, so the
    range-overlap check can't see them; their title is the only signal.
    """
    norm_a = _normalize_title_for_match(a)
    norm_b = _normalize_title_for_match(b)
    if not norm_a or not norm_b:
        return False
    if norm_a == norm_b:
        return True
    words_a = set(norm_a.split())
    words_b = set(norm_b.split())
    jaccard = len(words_a & words_b) / len(words_a | words_b)
    if jaccard >= _TITLE_SIMILARITY_THRESHOLD:
        return True  # high word overlap / reordering
    # High character similarity counts as a duplicate ONLY when one title's words
    # are a subset of the other's — i.e. a genuine insertion ("Claude Nuked My
    # [Production] Database"). Without this gate, two short titles that differ by
    # a single CONTENT word ("…Should Quit" vs "…Should Stay", "Top 5" vs
    # "Top 10") score a high char-ratio and a legit, distinct clip gets dropped.
    if words_a.issubset(words_b) or words_b.issubset(words_a):
        char_ratio = difflib.SequenceMatcher(None, norm_a, norm_b).ratio()
        return char_ratio >= _TITLE_SIMILARITY_THRESHOLD
    return False


def is_parent_eligible_for_kind(
    parent_duration_seconds: float, kind: ClipKind,
) -> bool:
    """True when a parent of this duration can host this kind of clip.

    Used by the preview endpoint to disable kinds in the modal up front
    rather than having Claude propose ranges that we'd then reject — and
    by the confirm step as a defensive guard.
    """
    min_s, max_s = _PER_KIND_BOUNDS[kind]
    if max_s is None:
        # Segments have no fixed ceiling — the ceiling is a fraction of the
        # parent. The parent must therefore be long enough that the FRACTION
        # still clears the kind's minimum, not merely longer than the minimum
        # itself: at a 0.75 fraction and a 180 s floor, a 200 s parent would
        # otherwise read as eligible while every proposal it could produce
        # (max 150 s) is below the floor.
        longest_possible_segment = (
            parent_duration_seconds * _SEGMENT_MAX_FRACTION_OF_PARENT_DURATION
        )
        return (
            parent_duration_seconds >= min_s + _PARENT_HEADROOM_SECONDS
            and longest_possible_segment >= min_s
        )
    return parent_duration_seconds >= max_s + _PARENT_HEADROOM_SECONDS


@dataclass(frozen=True)
class ClipKindBand:
    """One kind's duration window, shaped for display.

    The Generate modal used to spell these numbers out in its own markup,
    which drifted the moment the bounds moved: it offered "Shorts (45–75 s)"
    while the validator enforced a different band, so the user was told one
    thing and filtered by another. Rendering the label from here makes the
    advertised window and the enforced window the same fact.
    """

    kind: ClipKind
    min_seconds: float
    max_seconds: float | None
    max_fraction_of_parent_duration: float | None
    default_max_proposals: int
    label: str


def default_max_proposals_for_kind(kind: ClipKind) -> int:
    """How many proposals to ask for when the caller didn't say.

    One rule for every caller — the API's missing-key default, the number
    box the modal renders, and the internal fallback all read this. They
    used to be three separate constants, so the modal offered 8 of a kind
    whose declared default was 15.

    Subscript, not ``.get`` with a default: an unknown kind is a bug, and
    every other per-kind lookup here (``_PER_KIND_BOUNDS``,
    ``CLIP_EDITORIAL_PROMPT_KEYS``) already raises on one.
    """
    return _DEFAULT_MAX_PER_KIND[kind]


def clip_kind_bands() -> list[ClipKindBand]:
    """The per-kind duration windows, in declaration order, for the UI."""
    bands: list[ClipKindBand] = []
    for kind, (min_s, max_s) in _PER_KIND_BOUNDS.items():
        if max_s is not None:
            label = f"{min_s:g}–{max_s:g} s"
            fraction = None
        else:
            fraction = _SEGMENT_MAX_FRACTION_OF_PARENT_DURATION
            label = f"{min_s:g} s+, max {fraction:.0%} of video"
        bands.append(ClipKindBand(
            kind=kind, min_seconds=min_s, max_seconds=max_s,
            max_fraction_of_parent_duration=fraction,
            default_max_proposals=default_max_proposals_for_kind(kind),
            label=label,
        ))
    return bands


# --- Word-stream (index) proposal path -------------------------------------
#
# When word-level transcription is available we show Claude a NUMBERED list of
# complete-thought units and have it return integer index ranges, rather than
# anchor text + timestamps. Indexing is robust where anchor-matching was not
# (LLMs copy long text imperfectly and can't do timestamp math); all precision
# is recovered here from the word timing via ``clip_edges``.

_INDEX_PROPOSAL_TOOL = {
    "name": "propose_clips",
    "description": (
        "Submit proposed clip ranges by transcript UNIT INDEX. Returns no "
        "value; the caller reads the tool input."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "proposals": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "first_index": {
                            "type": "integer",
                            "description": "1-based index of the unit where the clip STARTS.",
                        },
                        "last_index": {
                            "type": "integer",
                            "description": "1-based index of the unit where the clip ENDS (inclusive).",
                        },
                        "start_echo": {
                            "type": "string",
                            "description": "First ~6 words of the first unit, verbatim. A cross-check only; the indices are authoritative.",
                        },
                        "end_echo": {
                            "type": "string",
                            "description": "Last ~6 words of the last unit, verbatim. A cross-check only; the indices are authoritative.",
                        },
                        "title": {
                            "type": "string",
                            "description": "A punchy working title for the clip (see length/tone guidance).",
                        },
                        "reason": {
                            "type": "string",
                            "description": "One sentence on why this range stands alone.",
                        },
                        "rating": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 4,
                            # Bounded in the schema, not just the prose: rating
                            # decides which clips survive the output cap and
                            # which of two overlapping clips wins, so a stray
                            # 0 or 9 would silently reorder the batch.
                            "description": "1-4 self-score (4 = best), judging content and title together.",
                        },
                    },
                    "required": ["first_index", "last_index", "start_echo", "end_echo", "title", "reason", "rating"],
                },
            },
        },
        "required": ["proposals"],
    },
}

CLIP_EDITORIAL_PROMPT_KEYS: dict[ClipKind, str] = {
    "hook": "promo_clip_proposals_hook",
    "short": "promo_clip_proposals_short",
    "segment": "promo_clip_proposals_segment",
}


async def editorial_block_for_kind(kind: ClipKind, *, project_id: int) -> str:
    """The user-editable editorial guidance for one kind, rendered.

    Lives in ``prompt_templates`` (seed default in ``services/prompts``) so
    the voice — what makes a good clip of this kind, how its title should
    read — is editable in Project Settings, while the mechanical contract
    around it stays in code. A blank saved body raises
    ``EmptyPromptBodyError`` from the resolver rather than quietly
    generating against half a prompt.
    """
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services import templates as template_service

    body = await prompt_service.get_prompt_body_with_fallback(
        CLIP_EDITORIAL_PROMPT_KEYS[kind], project_id=project_id,
    )
    return await template_service.async_render(body, {"kind": kind})


_CHECK_RANGE_TOOL = {
    "name": "check_range",
    "description": (
        "Check ONE candidate clip before proposing it. Returns the clip's real "
        "length in seconds, whether the title and length are inside the limits "
        "for this kind, whether the title duplicates a clip that already "
        "exists, and whether the range overlaps one. Issue as many of these as "
        "you like in parallel — one call per candidate — then call "
        "propose_clips once with the ones that passed. It cannot see your "
        "other candidates in the same turn, so two candidates that overlap "
        "each other will both pass here; do not propose overlapping ranges."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "first_index": {
                "type": "integer",
                "description": "1-based index of the unit where the clip STARTS.",
            },
            "last_index": {
                "type": "integer",
                "description": "1-based index of the unit where the clip ENDS (inclusive).",
            },
            "title": {
                "type": "string",
                "description": "The title you intend to give this clip.",
            },
        },
        "required": ["first_index", "last_index", "title"],
    },
}

# How many assistant turns a single kind may take before we stop. Each round is
# one API call: the model fires a batch of check_range calls, we answer them
# all in one user turn, it revises. Bounded because a model that never calls
# propose_clips would otherwise loop forever on our money.
MAX_PROPOSAL_ROUNDS: int = 6


def _build_index_system_text(kind: ClipKind, editorial_block: str) -> str:
    """The per-kind instruction: role, the transcript's line format, the
    editorial guidance, and how to answer.

    Deliberately free of run-specific data (no parent title, no transcript,
    no counts) so it is byte-identical across every call for this kind —
    the task belongs in the system prompt, the material in the user turn.

    ``editorial_block`` is the rendered, user-editable middle section; the
    sections around it are code so no prompt edit can break the output
    format. Kept synchronous and pure: the caller does the async fetch.
    """
    return (
        f"You select {kind} clips from a podcast transcript for posting as "
        "standalone vertical videos.\n\n"
        "## Input format\n"
        "The user message gives the parent video's title, any clips already "
        "made from it, and the transcript.\n"
        "The transcript is a NUMBERED list of complete-thought units, one per "
        "line:\n"
        "    <index>\\t(<duration>s)\\t<text>\n"
        "<duration> is that unit's own length in seconds, rounded. A clip is a "
        "contiguous run of units from first_index through last_index "
        "inclusive.\n\n"
        f"{editorial_block.strip()}\n\n"
        "## How to answer\n"
        "Check every candidate before you commit to it. Call check_range with "
        "one candidate per call — issue them all in parallel in a single turn, "
        "up to 30 at once. Each reply gives you the clip's real length, whether "
        "the title and length are inside the limits, and whether it duplicates "
        "or overlaps a clip that already exists, ending in PASS or FAIL.\n"
        "Fix what comes back FAIL and check again, or drop it. Prefer moving to "
        "a different moment over trimming a clip until it fits — a clip cut to "
        "length that no longer makes sense on its own is worse than one fewer "
        "clip. check_range cannot see your other candidates, so it will not "
        "tell you when two of yours overlap each other; keep them apart "
        "yourself.\n\n"
        "Then call propose_clips ONCE with the candidates that passed. "
        "Reference units by INDEX NUMBER only — never write timestamps and "
        "never retype the transcript text. For each clip supply:\n"
        "- first_index and last_index.\n"
        "- start_echo and end_echo: the first / last ~6 words of those two "
        "units, verbatim. A sanity check only — the indices are "
        "authoritative.\n"
        "- title and reason.\n"
        "- rating, 1-4 (4 = best), judging content and title together.\n\n"
        "Return FEWER clips than asked for (even zero) if there aren't that "
        "many strong ones — do not pad, do not overlap."
    )


def _build_index_user_text(
    kind: ClipKind, units: list[ClipUnit], *, parent_title: str,
    max_proposals: int, existing_titles: list[str] | None = None,
) -> str:
    """The run's material: parent title, what's already covered, the count
    asked for, and the numbered transcript.

    ``existing_titles`` are the titles of same-kind clips already on this
    parent. They're injected as an "already covered" list so the model
    avoids re-proposing the same moment up front — the post-hoc dedup is
    lexical (title/range), so it can't catch a clip that re-covers the same
    point in different words at a different timestamp. Prevention here is
    the only thing that catches that semantic repeat.
    """
    already_covered = ""
    titles = [t.strip() for t in (existing_titles or []) if t and t.strip()]
    if titles:
        bullets = "\n".join(f"- {t}" for t in titles)
        already_covered = (
            "## Already covered — do NOT repeat\n"
            f"These {kind} clips already exist for this video. Do not propose "
            "the same moment or point again, even phrased differently or from a "
            f"slightly different timestamp:\n{bullets}\n\n"
        )
    # The count goes ahead of the transcript: after several hundred numbered
    # lines it would be the easiest instruction in the prompt to lose.
    return (
        f"Parent video: \"{parent_title}\"\n\n"
        f"{already_covered}"
        f"Propose UP TO {max_proposals} {kind} clips.\n\n"
        "## Transcript\n"
        f"{clip_edges.numbered_units_block(units)}"
    )


def _echo_matches(echo: str, unit_text: str) -> bool:
    """Loose verbatim cross-check between a model echo and a unit's text.

    Normalizes both (lower-case, drop non-word punctuation, collapse runs) and
    returns True when the echo is a substring of the unit text. A failed match
    is only a logged sanity signal — the indices remain authoritative.
    """
    def norm(s: str) -> str:
        return re.sub(r"[^a-z0-9 ]", "", s.lower())
    e = re.sub(r"\s+", " ", norm(echo)).strip()
    u = re.sub(r"\s+", " ", norm(unit_text)).strip()
    if not e:
        return True
    return e in u or u in e


# --- Shared range checks -----------------------------------------------
#
# ONE implementation, used by both the check_range tool the model calls before
# submitting AND the server-side validator that has the final say. If those
# two ever disagreed, the model would be told a range passes and then watch it
# be refused — so they are not allowed to be separate code.
#
# check_range is an AID, never the enforcement: the model can ignore a FAIL,
# and _validate_indexed_proposals still decides.

# Title word bounds. The proposal's title IS the published title — the promo
# chain skips its AI-title step when one is supplied — so a title outside
# these bounds ships as-is if nothing checks it.
TITLE_WORD_BOUNDS: tuple[int, int] = (2, 12)


def _resolved_max_seconds(kind: ClipKind, parent_duration_seconds: float) -> float | None:
    """This kind's upper bound, with segment's parent-relative cap resolved."""
    max_s = _PER_KIND_BOUNDS[kind][1]
    if max_s is None and parent_duration_seconds > 0:
        return _SEGMENT_MAX_FRACTION_OF_PARENT_DURATION * parent_duration_seconds
    return max_s


def check_title(title: str) -> tuple[RejectionReason, str] | None:
    """Word-count bounds on a proposed title, or None when it's fine."""
    words = len(title.split())
    lo, hi = TITLE_WORD_BOUNDS
    if not title.strip():
        return (RejectionReason.TITLE_LENGTH, "title is empty")
    if words < lo:
        return (RejectionReason.TITLE_LENGTH,
                f"title is {words} word{'' if words == 1 else 's'}, minimum is {lo}")
    if words > hi:
        return (RejectionReason.TITLE_LENGTH,
                f"title is {words} words, maximum is {hi}")
    return None


def check_duration(
    kind: ClipKind, duration: float, parent_duration_seconds: float,
) -> tuple[RejectionReason, str] | None:
    """Duration against this kind's band, or None when it's inside."""
    min_s = _PER_KIND_BOUNDS[kind][0]
    max_s = _resolved_max_seconds(kind, parent_duration_seconds)
    if duration < min_s:
        return (RejectionReason.DURATION_OUT_OF_BAND,
                f"{duration:.1f}s is under the {kind} window [{min_s:g}s, {max_s:g}s]")
    if max_s is not None and duration > max_s:
        return (RejectionReason.DURATION_OUT_OF_BAND,
                f"{duration:.1f}s is over the {kind} window [{min_s:g}s, {max_s:g}s]")
    return None


def check_title_duplicate(
    title: str, existing_titles: list[str],
) -> tuple[RejectionReason, str] | None:
    """Near-match against titles already on the parent, or None."""
    match = next(
        (t for t in existing_titles if title.strip() and _titles_similar(title, t)),
        None,
    )
    if match is None:
        return None
    return (RejectionReason.DUPLICATE_TITLE,
            f"title duplicates an existing clip, {match!r}")


def check_overlap(
    start: float, end: float, ranges: list[tuple[float, float]],
) -> tuple[RejectionReason, str] | None:
    """Overlap against already-cut ranges, measured symmetrically against the
    SHORTER of the two clips so full containment counts. None when clear."""
    length = end - start
    for s, e in ranges:
        shorter = min(length, e - s)
        if shorter <= 0:
            continue
        if _overlap_seconds(start, end, s, e) > _MAX_OVERLAP_FRACTION * shorter:
            return (RejectionReason.OVERLAPS_EXISTING,
                    f"overlaps {s:.1f}s-{e:.1f}s by more than "
                    f"{_MAX_OVERLAP_FRACTION:.0%} of the shorter clip")
    return None


@dataclass(frozen=True)
class RangeCheck:
    """One check_range verdict, rendered for the model."""

    passed: bool
    text: str


def check_clip_range(
    *, kind: ClipKind, units: list[ClipUnit], first_index: int, last_index: int,
    title: str, parent_duration_seconds: float,
    existing_ranges: list[tuple[float, float]],
    existing_titles: list[str],
) -> RangeCheck:
    """Evaluate one candidate range and render the verdict the model reads.

    Reports EVERY line, pass or fail, so the model can see what it got right
    as well as what it didn't — a bare FAIL teaches it less.

    Deliberately stateless: it cannot see the other candidates in the same
    batch, so within-batch overlap and duplicate titles are still resolved
    server-side. The tool description says so.
    """
    lines: list[str] = []
    failures: list[str] = []

    def line(label: str, measured: str, problem: tuple[RejectionReason, str] | None) -> None:
        if problem is None:
            lines.append(f"  {label}: {measured} - OK")
        else:
            lines.append(f"  {label}: {measured} - FAIL: {problem[1]}")
            failures.append(problem[1])

    resolved = clip_edges.resolve_unit_range(units, first_index, last_index)
    if resolved is None:
        lines.append(
            f"  index range: units {first_index}-{last_index} - FAIL: out of "
            f"bounds, the transcript has {len(units)} units"
        )
        return RangeCheck(passed=False, text=_render_check(
            kind, first_index, last_index, title, lines, ok=False))

    lines.append(f"  index range: units {first_index}-{last_index} of {len(units)} - OK")
    word_count = len(title.split())
    line(
        "title",
        f"{word_count} word{'' if word_count == 1 else 's'} / "
        f"{len(title)} char{'' if len(title) == 1 else 's'}",
        check_title(title),
    )
    line(
        "title duplicate",
        f"checked against {len(existing_titles)} existing {kind} title"
        f"{'' if len(existing_titles) == 1 else 's'}",
        check_title_duplicate(title, existing_titles),
    )
    line(
        "clip length", f"{resolved.duration:.1f} seconds",
        check_duration(kind, resolved.duration, parent_duration_seconds),
    )
    edges = clip_edges.compute_edges(units, first_index, last_index)
    line(
        "overlap check",
        f"checked against {len(existing_ranges)} existing clip"
        f"{'' if len(existing_ranges) == 1 else 's'}",
        check_overlap(edges.final_start, edges.final_end, existing_ranges),
    )
    return RangeCheck(
        passed=not failures,
        text=_render_check(kind, first_index, last_index, title, lines, ok=not failures),
    )


def _render_check(
    kind: ClipKind, first_index: int, last_index: int, title: str,
    lines: list[str], *, ok: bool,
) -> str:
    return (
        "Check - inputs:\n"
        f"  kind: {kind}\n"
        f"  title: \"{title}\"\n"
        f"  first_index: {first_index}\n"
        f"  last_index: {last_index}\n\n"
        "Check - result:\n"
        + "\n".join(lines)
        + f"\n\nOverall result: {'PASS' if ok else 'FAIL'}"
    )


def _validate_indexed_proposals(
    raw_proposals: list[dict], *, kind: ClipKind, units: list[ClipUnit],
    existing_ranges: list[tuple[float, float]], max_proposals: int,
    parent_duration_seconds: float,
    existing_titles: list[str] | None = None,
) -> tuple[list[ProposedClip], list[RejectedProposal]]:
    """Resolve index ranges to clips, returning what survived AND what didn't.

    Two passes, because the checks are of two different kinds:

    1. **Independent** — bad indices, out-of-band duration, a title that
       duplicates one already on the parent. These judge a proposal on its
       own and are evaluated for every entry.
    2. **Mutual** — overlap and within-batch title duplication. These are
       contests between candidates, so the survivors are ranked first
       (rating, then length, then earliest in the transcript) and taken
       greedily.
       Ranking is what makes the output cap keep the BEST N rather than the
       first N, and what decides which of two overlapping clips wins.

    Rejections are returned rather than only logged: a proposal discarded in
    silence is indistinguishable to the user from one Claude never made.

    ``existing_titles``: titles of same-kind clips already on this parent.
    The only duplicate signal available for imported clips, which carry no
    cut range for the overlap check to see.
    """
    rejected: list[RejectedProposal] = []

    def _reject(
        reason: RejectionReason, detail: str, *, title: str = "",
        first_index: int | None = None, last_index: int | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        logger.info("Dropping clip proposal %r: %s", title or "<untitled>", detail)
        rejected.append(RejectedProposal(
            kind=kind, reason=reason, detail=detail, title=title,
            first_index=first_index, last_index=last_index,
            duration_seconds=duration_seconds,
        ))

    # --- Pass 1: independent checks -------------------------------------
    # (sort keys..., title, raw_title, reason, rating, edges, first, last)
    Candidate = tuple[
        float, float, float, str, str, str, int | None, ClipEdges, int, int,
    ]
    candidates: list[Candidate] = []
    for entry in raw_proposals:
        if not isinstance(entry, dict):
            _reject(
                RejectionReason.INVALID_INDICES,
                f"entry is a {type(entry).__name__}, not an object",
            )
            continue
        try:
            first_index = int(entry["first_index"])
            last_index = int(entry["last_index"])
        except (KeyError, TypeError, ValueError):
            _reject(
                RejectionReason.INVALID_INDICES,
                f"missing or non-integer first_index/last_index in {entry!r}",
            )
            continue
        raw_title = str(entry.get("title") or "").strip()
        title = raw_title or f"Untitled {kind}"

        resolved = clip_edges.resolve_unit_range(units, first_index, last_index)
        if resolved is None:
            _reject(
                RejectionReason.INDEX_OUT_OF_BOUNDS,
                f"index range {first_index}-{last_index} is out of bounds "
                f"(the transcript has {len(units)} units)",
                title=title, first_index=first_index, last_index=last_index,
            )
            continue

        # Same primitives check_range renders for the model, so a range it was
        # told passes cannot be refused here for a reason it never saw.
        problem = (
            check_duration(kind, resolved.duration, parent_duration_seconds)
            or check_title(raw_title)
            # Only REAL titles dedup: the synthetic "Untitled <kind>" fallback
            # must not collide two distinct untitled clips.
            or (check_title_duplicate(raw_title, list(existing_titles or []))
                if raw_title else None)
        )
        if problem is not None:
            _reject(
                problem[0], problem[1],
                title=title, first_index=first_index, last_index=last_index,
                duration_seconds=resolved.duration,
            )
            continue

        # Echo cross-check (prototype parity): a mismatch is logged but not
        # rejected — the unit indices are authoritative.
        start_echo = str(entry.get("start_echo") or "")
        end_echo = str(entry.get("end_echo") or "")
        if start_echo and not _echo_matches(start_echo, units[first_index - 1].text):
            logger.info(
                "Clip proposal %r: start_echo %r doesn't match unit %d; trusting index.",
                title, start_echo[:60], first_index,
            )
        if end_echo and not _echo_matches(end_echo, units[last_index - 1].text):
            logger.info(
                "Clip proposal %r: end_echo %r doesn't match unit %d; trusting index.",
                title, end_echo[:60], last_index,
            )

        raw_rating = entry.get("rating")
        rating = int(raw_rating) if isinstance(raw_rating, (int, float)) else None
        edges = clip_edges.compute_edges(units, first_index, last_index)
        candidates.append((
            # Sort key first: best rating, then longest, then earliest in the
            # transcript — never random, so a rerun is reproducible.
            -(rating if rating is not None else 0),
            -(edges.final_end - edges.final_start),
            float(first_index),
            title, raw_title, str(entry.get("reason") or "").strip(),
            rating, edges, first_index, last_index,
        ))

    # --- Pass 2: mutual contests, best-first -----------------------------
    candidates.sort(key=lambda c: (c[0], c[1], c[2]))

    out: list[ProposedClip] = []
    accepted_ranges: list[tuple[float, float]] = []
    accepted_titles: list[str] = []
    for (_r, _len, _idx, title, raw_title, reason_text,
         rating, edges, first_index, last_index) in candidates:
        duration = edges.final_end - edges.final_start
        # Within-batch title duplication is a contest too — the better-ranked
        # of the pair is already in accepted_titles by the time we get here.
        duplicate_of = next(
            (t for t in accepted_titles if raw_title and _titles_similar(raw_title, t)),
            None,
        )
        if duplicate_of is not None:
            _reject(
                RejectionReason.DUPLICATE_TITLE,
                f"title duplicates {duplicate_of!r}, already accepted in this batch",
                title=title, first_index=first_index, last_index=last_index,
                duration_seconds=duration,
            )
            continue

        # Overlap guard — against already-cut clips and better-ranked proposals
        # from this batch. Symmetric: measured against the SHORTER of the two
        # clips, so a long proposal that fully contains a short existing clip is
        # also dropped (against its own length alone, containment would pass).
        conflict = check_overlap(
            edges.final_start, edges.final_end, existing_ranges + accepted_ranges,
        )
        if conflict is not None:
            _reject(
                conflict[0], conflict[1],
                title=title, first_index=first_index, last_index=last_index,
                duration_seconds=duration,
            )
            continue

        # Checked last, once the clip has cleared every other test: a
        # proposal that would have been refused anyway must be told the real
        # reason, not that it merely ranked too low.
        if len(out) >= max_proposals:
            _reject(
                RejectionReason.OVER_OUTPUT_CAP,
                f"beyond the {max_proposals}-proposal cap for {kind} "
                "(lower-ranked than the ones kept)",
                title=title, first_index=first_index, last_index=last_index,
                duration_seconds=duration,
            )
            continue

        out.append(ProposedClip(
            kind=kind,
            start_seconds=edges.final_start,
            end_seconds=edges.final_end,
            title=title,
            reason=reason_text,
            rating=rating,
            audio_fade_in=edges.fade_in,
            audio_fade_out=edges.fade_out,
        ))
        accepted_ranges.append((edges.final_start, edges.final_end))
        if raw_title:
            accepted_titles.append(raw_title)
    return out, rejected


async def propose_clips_for_kind_indexed(
    *,
    kind: ClipKind,
    units: list[ClipUnit],
    parent_title: str,
    parent_duration_seconds: float,
    existing_ranges: list[tuple[float, float]],
    project_id: int,
    existing_titles: list[str] | None = None,
    max_proposals: int | None = None,
) -> KindProposals:
    """Word-stream proposal: one per-kind Claude call over the numbered units.

    ``project_id`` resolves this project's editable editorial block for the
    kind; a blank saved prompt raises rather than generating against half a
    system message.

    Returns everything that happened to this kind — what was accepted, what
    was refused and why, and whether the call itself failed. A caller that
    only reads ``.accepted`` cannot tell those apart, which is the bug this
    return type exists to prevent.
    """
    if not is_parent_eligible_for_kind(parent_duration_seconds, kind) or not units:
        # A legitimately empty result, NOT a failure: the parent is too short
        # for this kind, or there is nothing to read.
        return KindProposals(kind=kind, accepted=[], rejected=[])

    if max_proposals is None or max_proposals <= 0:
        base_max = default_max_proposals_for_kind(kind)
    else:
        base_max = min(max_proposals, MAX_PROPOSALS_PER_KIND_CAP)

    # Over-request a few extra candidates when this kind already has cut clips on
    # the parent: the new prompt gives Claude only unit indices + spans (no
    # timestamps), so we can't tell it which ranges to avoid — instead we ask for
    # more and drop duplicates/overlaps post-LLM, capping the output at base_max.
    ask_max = base_max
    if existing_ranges:
        ask_max = min(base_max + _EXISTING_OVERREQUEST_BONUS, MAX_PROPOSALS_PER_KIND_CAP)

    system_text = _build_index_system_text(
        kind, await editorial_block_for_kind(kind, project_id=project_id),
    )
    user_text = _build_index_user_text(
        kind, units, parent_title=parent_title, max_proposals=ask_max,
        existing_titles=existing_titles,
    )

    model = await ai._resolve_model()
    kwargs: dict[str, object] = {
        "model": model,
        "max_tokens": PROPOSAL_MAX_OUTPUT_TOKENS,
        # Explicit, so the SDK uses our budget instead of guessing one from
        # max_tokens and refusing the call before it leaves the machine.
        "timeout": CLIP_PROPOSAL_TIMEOUT_SECONDS,
        "system": system_text,
        "messages": [{"role": "user", "content": user_text}],
        "tools": [_CHECK_RANGE_TOOL, _INDEX_PROPOSAL_TOOL],
        # "any" not a named tool: the model must use A tool, but it picks which,
        # so it can check candidates before committing. Pinning propose_clips
        # would make check_range unreachable no matter what the prompt says.
        "tool_choice": {"type": "any"},
    }
    logger.info("Clip-proposal (index) request: kind=%s units=%d model=%s",
                kind, len(units), model)

    client = ai.get_client()
    messages: list[dict] = list(kwargs["messages"])  # type: ignore[arg-type]
    checks_answered = 0
    entries: list | None = None
    stop_reason = None

    for round_number in range(1, MAX_PROPOSAL_ROUNDS + 1):
        try:
            message = await ai.create_message_async(
                client, label=f"clip-proposal:{kind}:r{round_number}",
                **{**kwargs, "messages": messages},
            )
        except Exception as exc:
            # Carried, not swallowed: returning an empty list here would render
            # identically to "Claude found nothing good", which is a different
            # fact and one the user cannot act on.
            logger.exception("Claude clip-proposal (index) call failed for %s", kind)
            return KindProposals.failed(kind, f"{type(exc).__name__}: {exc}")

        stop_reason = getattr(message, "stop_reason", None)
        blocks = list(getattr(message, "content", []) or [])
        proposal_block = next(
            (b for b in blocks
             if getattr(b, "type", None) == "tool_use"
             and getattr(b, "name", "") == "propose_clips"),
            None,
        )
        if proposal_block is not None:
            candidate = (getattr(proposal_block, "input", None) or {}).get("proposals")
            if not isinstance(candidate, list):
                return KindProposals.failed(kind, (
                    f"propose_clips carried no 'proposals' array (got "
                    f"{type(candidate).__name__}, stop_reason={stop_reason!r})."
                ))
            entries = candidate
            break

        check_blocks = [
            b for b in blocks
            if getattr(b, "type", None) == "tool_use"
            and getattr(b, "name", "") == "check_range"
        ]
        if not check_blocks:
            # Neither tool, despite tool_choice="any" — a truncated response or
            # a prose answer. A failure, not an empty result.
            return KindProposals.failed(kind, (
                f"Claude called neither check_range nor propose_clips in round "
                f"{round_number} (stop_reason={stop_reason!r}) — the response "
                "was truncated or answered in prose."
            ))

        # Answer every check in ONE user turn, so a batch of parallel checks
        # costs a single round-trip rather than one apiece.
        results = []
        for block in check_blocks:
            args = getattr(block, "input", None) or {}
            try:
                verdict = check_clip_range(
                    kind=kind, units=units,
                    first_index=int(args["first_index"]),
                    last_index=int(args["last_index"]),
                    title=str(args.get("title") or ""),
                    parent_duration_seconds=parent_duration_seconds,
                    existing_ranges=existing_ranges,
                    existing_titles=list(existing_titles or []),
                ).text
            except (KeyError, TypeError, ValueError) as exc:
                verdict = (
                    f"Check - result:\n  FAIL: could not read the range you sent "
                    f"({type(exc).__name__}: {exc}).\n\nOverall result: FAIL"
                )
            results.append({
                "type": "tool_result",
                "tool_use_id": getattr(block, "id", ""),
                "content": verdict,
            })
        checks_answered += len(results)
        messages = messages + [
            {"role": "assistant", "content": blocks},
            {"role": "user", "content": results},
        ]
        logger.info(
            "Clip-proposal (index) %s round %d: answered %d check_range call%s",
            kind, round_number, len(results), "" if len(results) == 1 else "s",
        )

    if entries is None:
        return KindProposals.failed(kind, (
            f"Claude used {checks_answered} check_range calls across "
            f"{MAX_PROPOSAL_ROUNDS} rounds without ever calling propose_clips."
        ))

    raw_proposals = list(entries)

    accepted, rejected = _validate_indexed_proposals(
        raw_proposals, kind=kind, units=units,
        existing_ranges=existing_ranges, max_proposals=base_max,
        parent_duration_seconds=parent_duration_seconds,
        existing_titles=existing_titles,
    )
    logger.info(
        "Clip-proposal (index) for %s: %d raw -> %d accepted, %d rejected "
        "(asked up to %d, %d check_range calls answered)",
        kind, len(raw_proposals), len(accepted), len(rejected), ask_max,
        checks_answered,
    )
    return KindProposals(
        kind=kind, accepted=accepted, rejected=rejected,
        raw_count=len(entries),
    )


# Cap on simultaneously-running ffmpeg cut jobs. Precise cuts re-encode
# from the leading GOP forward. Two paths:
#
#   * Software (libx264) — CPU-bound, scales with cores. 8 in flight is
#     comfortable on Apple Silicon (M-series wide cores) without thrash.
#   * Hardware (videotoolbox) — uses the Media Engine block, whose
#     aggregate throughput on a single 4K source is effectively fixed
#     (measured ~2.6x realtime total): N concurrent cuts each run at
#     ~2.6/N x, so concurrency does NOT speed the batch up — it only
#     divides a fixed pie and lengthens each cut's wall time. A long
#     (multi-minute) segment at high concurrency can therefore approach
#     the per-cut ffmpeg timeout for no aggregate gain. 2 keeps each
#     cut's latency low (and well clear of the timeout) at the same
#     total throughput. This lane is shared by the ffmpeg landscape cut
#     and the Swift clipcrop recrop — both drive videotoolbox.
#
# Each cut acquires whichever lane it's actually going to use, so a
# generate confirm with vertical crops gets up to 2 hardware encodes in
# flight while non-crop cuts keep filling the 8 software slots
# independently.
#
# Lazily initialised on first use so that the semaphores are always
# created on the running event loop — avoids "bound to a different loop"
# errors when tests spin up a fresh loop per test or a server restart
# creates a new loop in-process.


# An asyncio.Semaphore binds to the loop that first awaits it, so caching one
# across a loop swap makes a *contended* acquire raise "attached to a different
# loop" — rare, load-dependent, and exactly the failure the lazy construction
# was supposed to avoid. Key the cache by loop instead.
_CUT_SEMAPHORES: dict[tuple[int, str], asyncio.Semaphore] = {}
_SOFTWARE_CUT_CONCURRENCY: int = 8
_HARDWARE_CUT_CONCURRENCY: int = 2


def _cut_semaphore(lane: str, size: int) -> asyncio.Semaphore:
    key = (id(asyncio.get_running_loop()), lane)
    sem = _CUT_SEMAPHORES.get(key)
    if sem is None:
        sem = asyncio.Semaphore(size)
        # Bounded: one entry per (loop, lane), and loops are not created in a
        # hot path. Stale entries from a dead loop are a few objects.
        _CUT_SEMAPHORES[key] = sem
    return sem


def _get_software_cut_semaphore() -> asyncio.Semaphore:
    return _cut_semaphore("software", _SOFTWARE_CUT_CONCURRENCY)


def _get_hardware_cut_semaphore() -> asyncio.Semaphore:
    return _cut_semaphore("hardware", _HARDWARE_CUT_CONCURRENCY)


# In-flight Generate-from-source preview jobs. Same pattern as
# auto_actions._UPLOAD_JOBS — keyed by job_id, fields read by the
# client's polling endpoint and updated by the background task.
#
# State machine: pending → transcribing? → proposing → done|failed.
# ``transcribing`` only appears when the parent had no usable transcript
# at preview-start; the chain fires a fresh whisper run inline. ``done``
# state carries ``proposals: dict[kind → list[ProposedClip-as-dict]]``
# which the client renders.
#
# Terminal jobs (``done`` / ``failed``) live for ``_GENERATE_JOB_TTL_SECONDS``
# past the moment they entered the terminal state.
#
# The TTL has to cover two competing pressures:
#
# * Long enough that a user reviewing 24 proposals at a leisurely pace
#   (dismissing some, re-watching previews) doesn't have the job evict
#   underneath them — the confirm endpoint cross-checks vertical_crop
#   against the job's crop snapshot, so a missing job downgrades the
#   security posture (the confirm endpoint forces vertical_crop=false
#   for the missing-job case to avoid a tampered crop request slipping
#   through).
# * Short enough that the dict can't grow unboundedly on a long-running
#   install. With single-user usage and one Generate per parent video,
#   even a one-hour TTL keeps the dict to maybe a dozen entries.
#
# 6 hours: a leisurely review (and stepping away from it) must NOT evict the
# job and delete the preview files out from under the still-open review page,
# which would leave the proposal cards showing empty/404 video players. The dict
# stays tiny on single-user usage, so the generous window costs nothing.
# _evict_stale_generate_jobs runs on every read/write of the dict, so no
# separate timer is needed.
_GENERATE_JOBS: dict[str, dict] = {}
_GENERATE_JOB_TTL_SECONDS: float = 6 * 60 * 60  # 6 hours


def _evict_stale_generate_jobs() -> None:
    """Drop terminal-state jobs that have exceeded their TTL.

    Cheap O(N) sweep — N is bounded by typical user activity (a few
    dozen at most) and this runs on every poll/write of the dict, so
    stale entries are reaped opportunistically without a background
    timer.
    """
    import time

    now = time.monotonic()
    stale = [
        job_id for job_id, job in _GENERATE_JOBS.items()
        if job.get("state") in ("done", "failed")
        and job.get("_terminal_at") is not None
        and (now - float(job["_terminal_at"])) > _GENERATE_JOB_TTL_SECONDS
    ]
    for job_id in stale:
        _GENERATE_JOBS.pop(job_id, None)
        # Function is defined later in the module — Python resolves it
        # at call time, so the forward reference is fine. Wrapped to
        # tolerate the case where the cleanup function isn't reachable
        # for any reason; eviction is best-effort.
        try:
            cleanup_generate_previews(job_id)
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass


def _mark_terminal(job: dict) -> None:
    """Stamp the terminal-state timestamp so eviction can age the job out."""
    import time
    job["_terminal_at"] = time.monotonic()


def get_generate_job(job_id: str) -> dict | None:
    """Read of a generate job's current state for the polling endpoint."""
    _evict_stale_generate_jobs()
    job = _GENERATE_JOBS.get(job_id)
    if job is None:
        return None
    public_keys = {
        "job_id", "parent_id", "project_id", "state", "last_error",
        "kinds", "crop_vertical", "proposals", "progress_message",
        # Deny-by-default: the job dict also holds parent_video_path, an
        # absolute filesystem path the browser must never see. Add new fields
        # here deliberately — a field not listed is silently dropped.
        "rejected", "raw_counts", "kind_errors",
    }
    return {k: v for k, v in job.items() if k in public_keys}


def _format_ffmpeg_timestamp(seconds: float) -> str:
    """ffmpeg accepts a bare seconds-with-decimal string, but using
    HH:MM:SS.mmm reads more clearly in logs and is round-trip safe."""
    seconds = max(0.0, float(seconds))
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


async def _run_cut(
    *, parent_video_path: Path, proposal: ProposedClip, out_name: str, vertical_crop: bool,
) -> tuple[Path, bool]:
    """Cut a proposal to ``out_name`` in UPLOAD_DIR. Shared by the preview cut and
    the re-cut fallback so both route identically.

    * **Crop-on** kinds → the all-Swift ``clipcrop`` (YOLO head-tracking stacked/
      single 9:16, native-resolution, audio fades). It owns its own hardware
      encode, so it always takes the hardware lane. Returns ``uncertain=True`` when
      clipcrop's croppability guard flagged the clip (b-roll / screen content).
    * **Crop-off** (segments) → the ffmpeg landscape cut (no crop).

    Raises on any failure — NO silent fallback to a center crop (rule C). The
    caller owns the filename and how the error/uncertain flag surface.
    """
    out_path = UPLOAD_DIR / out_name
    if vertical_crop:
        async with _get_hardware_cut_semaphore():
            ok = False
            try:
                _, uncertain = await asyncio.to_thread(
                    media_service.extract_clip_stacked,
                    parent_video_path,
                    proposal.start_seconds,
                    proposal.end_seconds,
                    output_name=out_name,
                    fade_in=proposal.audio_fade_in,
                    fade_out=proposal.audio_fade_out,
                )
                ok = True
            finally:
                if not ok:
                    out_path.unlink(missing_ok=True)
        return out_path, uncertain

    # Cold cache spawns `ffmpeg -encoders`; keep that subprocess off the event loop
    # even though app startup normally warms it.
    will_use_hardware = await asyncio.to_thread(
        media_service.hardware_encoder_available, "h264"
    )
    semaphore = (
        _get_hardware_cut_semaphore() if will_use_hardware
        else _get_software_cut_semaphore()
    )
    async with semaphore:
        ok = False
        try:
            await asyncio.to_thread(
                media_service.extract_clip,
                parent_video_path,
                _format_ffmpeg_timestamp(proposal.start_seconds),
                _format_ffmpeg_timestamp(proposal.end_seconds),
                output_name=out_name,
                precise=True,
                encoder="auto",
                audio_fade_in=proposal.audio_fade_in,
                audio_fade_out=proposal.audio_fade_out,
            )
            ok = True
        finally:
            if not ok:
                out_path.unlink(missing_ok=True)
    return out_path, False


async def cut_clip_from_parent(
    *,
    parent_video_path: Path,
    proposal: ProposedClip,
    vertical_crop: bool = False,
    x_shift_normalized: float = 0.0,  # deprecated/unused — YOLO owns crop geometry
) -> Path:
    """Cut ``proposal`` out of ``parent_video_path`` to a new MP4 in
    UPLOAD_DIR. Returns the absolute path of the new file.

    Sample-accurate (precise=True). ``vertical_crop`` requests a 9:16
    (1080×1920) output with optional ``x_shift_normalized`` to follow
    a non-center subject. Inert since the Claude-vision pass was replaced by
    the on-device clipcrop recrop — kept only so stored rejection rows and the
    confirm payload still round-trip.

    Encoder selection + concurrency:

    * Hardware (videotoolbox) is preferred when ffmpeg was built with
      it; the cut is gated by :func:`_get_hardware_cut_semaphore`.
    * Software (libx264) is the fallback; gated by
      :func:`_get_software_cut_semaphore`.

    The two semaphores are independent, so a mixed batch (some hardware
    cuts, some software) fills both lanes at once. Output extension is
    always ``.mp4`` regardless of the parent's container — the
    YouTube-upload step that runs next prefers MP4 anyway.
    """
    out_name = f"clip_{proposal.kind}_{secrets.token_hex(6)}.mp4"
    path, _ = await _run_cut(
        parent_video_path=parent_video_path, proposal=proposal,
        out_name=out_name, vertical_crop=vertical_crop,
    )
    return path


# Deterministic prefix so review-page cleanups (eviction, Cancel,
# Confirm) can find every preview file for a job by glob without
# tracking each path on the job dict.
_PREVIEW_PREFIX = "gen_preview_"


def _preview_filename(job_id: str, kind: str, idx: int) -> str:
    return f"{_PREVIEW_PREFIX}{job_id}_{kind}_{idx}.mp4"


async def cut_preview_for_proposal(
    *,
    job_id: str,
    parent_video_path: Path,
    proposal: ProposedClip,
    idx: int,
    vertical_crop: bool = False,
    x_shift_normalized: float = 0.0,  # deprecated/unused — YOLO owns crop geometry
) -> tuple[Path, bool]:
    """Cut a proposal to a .mp4 so the review page can play the actual
    clip the user will import.

    Same parameters the final cut uses: ``precise=True`` (sample-
    accurate), ``encoder="auto"`` (videotoolbox when ffmpeg has it
    built in and ``vertical_crop=True``, libx264 otherwise), full
    duration. The file Confirm hands to the promo chain is THIS one
    — there is no re-cut. Calling it "preview" is a historical
    naming choice, kept because the filename pattern is what cleanup
    globs.

    Lane choice mirrors ``cut_clip_from_parent`` so the two paths
    never compete for the same encoder — they're the same code.

    Filename pattern (``gen_preview_<job_id>_<kind>_<idx>.mp4``) is
    deterministic so the Confirm endpoint can look up the file for an
    accepted proposal and rename it for the promo chain, and the
    cleanup sweep can glob the unadopted (rejected / failed)
    remainder without bookkeeping per proposal on the job dict.
    """
    out_name = _preview_filename(job_id, proposal.kind, idx)
    return await _run_cut(
        parent_video_path=parent_video_path, proposal=proposal,
        out_name=out_name, vertical_crop=vertical_crop,
    )


def cleanup_generate_previews(job_id: str) -> None:
    """Delete every preview file for ``job_id``. Safe to call repeatedly
    (missing files are ignored). Logged at debug since cleanup runs on
    Confirm + job eviction (and the startup-sweep wildcard variant on
    server boot) — multiple legitimate paths for the same files.
    """
    try:
        for path in UPLOAD_DIR.glob(f"{_PREVIEW_PREFIX}{job_id}_*.mp4"):
            try:
                path.unlink()
            except OSError as exc:
                logger.debug("Could not remove preview %s: %s", path, exc)
    except OSError as exc:
        logger.debug("Preview cleanup for %s failed: %s", job_id, exc)


def cleanup_orphan_generate_previews() -> int:
    """Delete every ``gen_preview_*.mp4`` preview and every ``.cutpart_*.mp4``
    cut temp in UPLOAD_DIR, regardless of job_id. Run on startup so files that
    survived a previous process being killed (``_GENERATE_JOBS`` is in-memory;
    restart wipes the dict and there's no list of job_ids to glob against)
    don't accumulate on disk forever. The ``.cutpart_*`` temps are the
    in-progress cut files written by ``media.extract_clip`` before its atomic
    rename — only ones leaked by a killed process reach here.

    Returns the number of files removed (for logging).
    """
    removed = 0
    try:
        for pattern in (f"{_PREVIEW_PREFIX}*.mp4", ".cutpart_*.mp4"):
            for path in UPLOAD_DIR.glob(pattern):
                try:
                    path.unlink()
                    removed += 1
                except OSError as exc:
                    logger.debug("Could not remove orphan cut file %s: %s", path, exc)
    except OSError as exc:
        logger.debug("Orphan preview sweep failed: %s", exc)
    return removed


async def propose_all_clips(
    *,
    kinds: list[ClipKind],
    units: list[ClipUnit],
    parent_title: str,
    parent_duration_seconds: float,
    existing_ranges_per_kind: dict[ClipKind, list[tuple[float, float]]],
    project_id: int,
    max_per_kind: dict[ClipKind, int] | None = None,
    existing_titles_per_kind: dict[ClipKind, list[str]] | None = None,
) -> dict[ClipKind, KindProposals]:
    """Fan out one Claude call per requested kind, in parallel.

    Returns a dict keyed by kind, in the same order ``kinds`` was passed.
    Kinds the parent is ineligible for come back with empty lists so the UI
    can render "0 proposals" rather than the request silently disappearing.

    One kind failing does NOT take the others down. Beyond the obvious UX
    reason, a bare ``gather`` propagates the first exception without
    cancelling its siblings — the other Claude calls would keep running and
    bill tokens for results nobody reads.

    ``units`` is the word-stream segmentation built from the on-device
    transcriber's word timing — always the index-based proposal path (there is
    no anchor-text fallback). ``max_per_kind`` is the user-selected per-kind
    cap; when ``None`` (or a kind missing from it) the per-kind default applies.
    """
    if not kinds:
        return {}

    caps = max_per_kind or {}

    async def _one(k: ClipKind) -> KindProposals:
        return await propose_clips_for_kind_indexed(
            kind=k,
            units=units,
            parent_title=parent_title,
            parent_duration_seconds=parent_duration_seconds,
            existing_ranges=existing_ranges_per_kind.get(k, []),
            project_id=project_id,
            existing_titles=(existing_titles_per_kind or {}).get(k, []),
            max_proposals=caps.get(k),
        )

    settled = await asyncio.gather(
        *(_one(k) for k in kinds), return_exceptions=True,
    )
    out: dict[ClipKind, KindProposals] = {}
    for k, result in zip(kinds, settled):
        if isinstance(result, BaseException):
            logger.exception(
                "Clip proposal pass failed for kind=%s", k, exc_info=result,
            )
            out[k] = KindProposals.failed(k, f"{type(result).__name__}: {result}")
        else:
            out[k] = result
    return out


# --- Rejection persistence (migration 028) -----------------------------
#
# Generate-from-source rejection memory. When the user un-checks a
# proposal on the review page and clicks "Cut & insert selected", the
# unchecked entries are persisted here so the next visit to the review
# page can show a "Previously dismissed" section with Restore buttons.
#
# Not fed into Claude's prompt — these are pure UI memory.


async def store_rejections(
    *,
    parent_id: str,
    project_id: int,
    rejected: list[dict],
) -> int:
    """Insert (or replace) the given rejected proposals for a parent.

    Each entry should look like the public proposal dict but is only
    required to carry ``kind`` / ``start_seconds`` / ``end_seconds``.
    Optional fields are stored when present so Restore brings the
    original assessment back without re-running vision.

    Returns the count of entries actually written (rows where the
    required fields were valid).
    """
    from yt_scheduler.database import write_transaction

    if not rejected:
        return 0
    written = 0
    async with write_transaction() as db:
        for entry in rejected:
            if not isinstance(entry, dict):
                continue
            kind = entry.get("kind")
            if kind not in ("hook", "short", "segment"):
                continue
            try:
                start = float(entry["start_seconds"])
                end = float(entry["end_seconds"])
            except (KeyError, TypeError, ValueError):
                continue
            # Same defensive non-finite guard the cut path uses.
            if not (math.isfinite(start) and math.isfinite(end)):
                continue
            if end <= start:
                continue

            await db.execute(
                """INSERT INTO generate_rejections (
                    parent_id, project_id, kind, start_seconds, end_seconds,
                    title, reason, x_shift_normalized,
                    crop_classification, crop_confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(parent_id, project_id, kind, start_seconds, end_seconds)
                DO UPDATE SET
                    title = excluded.title,
                    reason = excluded.reason,
                    x_shift_normalized = excluded.x_shift_normalized,
                    crop_classification = excluded.crop_classification,
                    crop_confidence = excluded.crop_confidence,
                    rejected_at = datetime('now')""",
                (
                    parent_id, project_id, kind, start, end,
                    str(entry.get("title") or "").strip() or None,
                    str(entry.get("reason") or "").strip() or None,
                    _maybe_float(entry.get("x_shift_normalized")),
                    _maybe_str(entry.get("crop_classification")),
                    _maybe_float(entry.get("crop_confidence")),
                ),
            )
            written += 1
    return written


async def list_rejections(
    *,
    parent_id: str,
    project_id: int,
) -> list[dict]:
    """Return every rejection for a parent as public-dict-shaped rows.

    Newest first so the UI can show "you last dismissed this 2 minutes
    ago" implicitly via order. Each row is shaped like
    :func:`proposal_to_public_dict` output minus the ``vertical_crop``
    flag (which is a per-Generate selection, not a property of the
    rejection itself — the review page applies the current selection
    when a rejection is Restored).
    """
    from yt_scheduler.database import get_db

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, kind, start_seconds, end_seconds, title, reason, "
        "x_shift_normalized, crop_classification, crop_confidence, "
        "rejected_at "
        "FROM generate_rejections "
        "WHERE parent_id = ? AND project_id = ? "
        "ORDER BY rejected_at DESC, id DESC",
        (parent_id, project_id),
    )
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        d["duration_seconds"] = float(d["end_seconds"]) - float(d["start_seconds"])
        out.append(d)
    return out


def _maybe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _maybe_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def proposal_to_public_dict(
    p: ProposedClip,
    *,
    crop_vertical: bool = False,
) -> dict:
    """JSON-safe representation of a proposal for the preview response.

    ``crop_vertical`` mirrors the per-kind 9:16 toggle. Crop geometry and the
    review-UI "uncertain" badge now come from the Swift clipcrop recrop at cut
    time (the caller sets ``crop_uncertain`` on this dict from clipcrop's
    croppability flag) — there is no Claude-vision assessment attached here.
    """
    return {
        "kind": p.kind,
        "start_seconds": p.start_seconds,
        "end_seconds": p.end_seconds,
        "duration_seconds": p.duration_seconds,
        "title": p.title,
        "reason": p.reason,
        "rating": p.rating,
        # Audio edge ramps; carried through so the final cut applies the
        # same fades as the preview.
        "audio_fade_in": p.audio_fade_in,
        "audio_fade_out": p.audio_fade_out,
        "vertical_crop": crop_vertical,
        # Deprecated/inert — kept so the rejection-store columns (migration 028)
        # still receive a value; geometry is owned by clipcrop now.
        "x_shift_normalized": 0.0,
    }


async def start_generate_job(
    *,
    parent_id: str,
    project_id: int,
    parent_video_path: str,
    parent_title: str,
    parent_duration_seconds: float,
    kinds: list[ClipKind],
    crop_vertical_for_kind: dict[ClipKind, bool],
    existing_ranges_per_kind: dict[ClipKind, list[tuple[float, float]]],
    max_per_kind: dict[ClipKind, int] | None = None,
    existing_titles_per_kind: dict[ClipKind, list[str]] | None = None,
) -> str:
    """Queue a preview job. Returns the job_id the client polls.

    The caller has already pre-flighted that the parent has a local
    file, the parent is short enough (≤4 h) and long enough for at
    least one requested kind, and that ``kinds`` is non-empty.
    Transcript availability is decided inside the background task —
    if the parent has no usable timestamped transcript, the task
    transitions through a ``transcribing`` state and re-uses
    :mod:`services.transcription` to produce one.
    """
    _evict_stale_generate_jobs()
    job_id = "gen_" + secrets.token_hex(8)
    # Normalise the per-kind cap dict so the job carries one int per
    # requested kind. Missing entries (e.g. caller didn't pass one) get
    # the default — keeping later code from having to None-check.
    normalised_max: dict[ClipKind, int] = {}
    incoming_max = max_per_kind or {}
    for k in kinds:
        raw = incoming_max.get(k)
        if isinstance(raw, int) and raw > 0:
            normalised_max[k] = min(raw, MAX_PROPOSALS_PER_KIND_CAP)
        else:
            normalised_max[k] = default_max_proposals_for_kind(k)
    _GENERATE_JOBS[job_id] = {
        "job_id": job_id,
        "parent_id": parent_id,
        "project_id": project_id,
        "parent_video_path": parent_video_path,
        "parent_title": parent_title,
        "parent_duration_seconds": parent_duration_seconds,
        "kinds": list(kinds),
        "crop_vertical": dict(crop_vertical_for_kind),
        "max_per_kind": normalised_max,
        "existing_ranges_per_kind": {
            k: list(v) for k, v in existing_ranges_per_kind.items()
        },
        "existing_titles_per_kind": {
            k: list(v) for k, v in (existing_titles_per_kind or {}).items()
        },
        "state": "pending",
        "last_error": None,
        "progress_message": None,
        "proposals": None,
    }
    spawn_background(_run_generate_job(job_id), name=f"generate-from-source:{job_id}")
    return job_id


async def _run_generate_job(job_id: str) -> None:
    """Background task: transcribe the parent on-device for fresh word timing,
    fan out per-kind index proposals, cut previews, and write the result onto
    the job dict for the polling endpoint."""
    job = _GENERATE_JOBS.get(job_id)
    if job is None:
        return

    try:
        # Always re-transcribe the parent on-device with Apple SpeechAnalyzer to
        # get FRESH word-level timing. The stored transcript only carries
        # cue-level timing, and we deliberately never persist word timing (it's
        # cheap to re-derive). There is intentionally NO fallback to another
        # backend: if the on-device transcriber is unavailable or fails, the job
        # fails loudly rather than silently producing lower-quality clips.
        job["state"] = "transcribing"
        job["progress_message"] = "Transcribing on-device (Apple Speech)…"
        from yt_scheduler.services import transcription

        def _on_transcribe_progress(done_seconds: float, total_seconds: float) -> None:
            # Runs on the transcription worker thread; a dict assignment is the
            # only shared-state write and is atomic under the GIL, so the polling
            # endpoint reads a consistent message. Bakes the percent into the
            # existing progress_message channel — no route/UI change needed.
            if total_seconds > 0:
                pct = max(0, min(100, int(round(done_seconds / total_seconds * 100))))
                job["progress_message"] = (
                    f"Transcribing on-device (Apple Speech)… {pct}%"
                )

        try:
            result = await asyncio.to_thread(
                transcription.transcribe,
                video_path=job["parent_video_path"],
                backend="macos-speech",
                language="en",
                progress_callback=_on_transcribe_progress,
            )
        except Exception as exc:
            logger.warning(
                "Generate-from-source on-device transcription failed for %s: %s",
                job["parent_id"], exc,
            )
            job["state"] = "failed"
            job["last_error"] = (
                f"On-device transcription failed ({exc}). Enable Speech "
                "Recognition for this app in System Settings → Privacy & "
                "Security → Speech Recognition, then try again."
            )
            _mark_terminal(job)
            return

        if not result.has_word_timestamps:
            job["state"] = "failed"
            job["last_error"] = (
                "On-device transcription returned no word-level timing, which "
                "Generate-from-source requires."
            )
            _mark_terminal(job)
            return

        units = clip_edges.build_units(result.all_words) or None
        if not units:
            job["state"] = "failed"
            job["last_error"] = "Transcription produced no usable speech units."
            _mark_terminal(job)
            return
        logger.info("Generate-from-source: %d word-stream units (%s).",
                    len(units), result.backend)

        # Proposing — fan out the per-kind index calls.
        job["state"] = "proposing"
        job["progress_message"] = "Asking Claude to propose clips…"
        runs = await propose_all_clips(
            kinds=job["kinds"],
            units=units,
            parent_title=job["parent_title"],
            parent_duration_seconds=job["parent_duration_seconds"],
            existing_ranges_per_kind=job["existing_ranges_per_kind"],
            project_id=int(job["project_id"]),
            max_per_kind=job.get("max_per_kind"),
            existing_titles_per_kind=job["existing_titles_per_kind"],
        )
        proposals = {k: run.accepted for k, run in runs.items()}
        # What Claude proposed but the server refused, and which kinds failed
        # outright. Both reach the review page: "23 proposed, 7 kept" is a
        # different fact from "Claude found 7", and the user can only act on
        # the difference if we say it.
        job["rejected"] = {
            k: [
                {
                    "kind": r.kind,
                    "reason": r.reason.value,
                    "detail": r.detail,
                    "title": r.title,
                    "duration_seconds": r.duration_seconds,
                }
                for r in run.rejected
            ]
            for k, run in runs.items() if run.rejected
        }
        # Failed kinds are omitted, not reported as 0: their count is unknown,
        # and a 0 would feed the UI's "N proposed" arithmetic a false zero.
        job["raw_counts"] = {
            k: run.raw_count for k, run in runs.items() if run.error is None
        }
        job["kind_errors"] = {
            k: run.error for k, run in runs.items() if run.error is not None
        }

        # Per-kind 9:16 toggles the user set in the review modal (hooks/shorts
        # default on, segments off); they ride on the job from the preview
        # endpoint. Crop-on kinds get the all-Swift clipcrop recrop at cut time
        # (YOLO head-tracking stacked/single 9:16) — there is NO Claude vision
        # pass anymore: YOLO owns the crop geometry, and clipcrop emits the
        # croppability flag that becomes the review-UI "uncertain" badge.
        crop_for_kind = job.get("crop_vertical") or {k: False for k in proposals}

        public_per_kind: dict[str, list[dict]] = {
            k: [
                proposal_to_public_dict(p, crop_vertical=crop_for_kind.get(k, False))
                for p in v
            ]
            for k, v in proposals.items()
        }

        # Cut a file per proposal so the review page plays the actual
        # clip the user will import. Same params as the final cut —
        # Confirm reuses these files instead of re-cutting. One
        # failure stashes preview_error on that proposal; the rest
        # still produce files via asyncio.gather(return_exceptions=True).
        job["state"] = "cutting_previews"
        total = sum(len(v) for v in proposals.values())
        # Internal counters only — they are NOT in the public job dict. What
        # the UI sees is progress_message, rendered from them below, so the
        # user gets "M of N" instead of a single static label. Bumped by each
        # task's wrapper as soon as ffmpeg returns.
        job["cuts_total"] = total
        job["cuts_completed"] = 0
        job["progress_message"] = f"Cutting clips… 0 of {total}"
        parent_path = Path(job["parent_video_path"])

        async def _cut_and_count(
            k: str, p_idx: int, p: ProposedClip, kind_crop: bool, x_shift: float,
        ) -> tuple[Path, bool]:
            try:
                return await cut_preview_for_proposal(
                    job_id=job_id,
                    parent_video_path=parent_path,
                    proposal=p,
                    idx=p_idx,
                    vertical_crop=kind_crop,
                    x_shift_normalized=x_shift,
                )
            finally:
                # Count failures too — completed means "we're done
                # waiting on this slot", not "succeeded". The error
                # path stashes preview_error separately.
                job["cuts_completed"] = job.get("cuts_completed", 0) + 1
                done = job["cuts_completed"]
                job["progress_message"] = f"Cutting clips… {done} of {total}"

        preview_tasks: list[tuple[str, int, asyncio.Task]] = []
        for k, v in proposals.items():
            kind_crop = crop_for_kind.get(k, False)
            for idx, p in enumerate(v):
                pub = public_per_kind[k][idx]
                preview_tasks.append((k, idx, asyncio.create_task(
                    _cut_and_count(
                        k, idx, p, kind_crop,
                        float(pub.get("x_shift_normalized") or 0.0),
                    ),
                )))
        if preview_tasks:
            preview_results = await asyncio.gather(
                *(t for _, _, t in preview_tasks),
                return_exceptions=True,
            )
            for (k, idx, _), res in zip(preview_tasks, preview_results):
                # BaseException catches CancelledError too — in 3.11+
                # it's not an Exception, so a per-task cancel would
                # otherwise fall through to the success branch and
                # produce a media_url() from the exception object.
                if isinstance(res, BaseException):
                    # Stash the error on the proposal so the UI can
                    # surface it instead of silently falling back to a
                    # misleading parent-with-#t= preview (which for a
                    # vertical-crop kind would render the landscape
                    # source and look "fine" while hiding the failure).
                    # Log untruncated server-side; bound the UI string
                    # at a generous length so the actual ffmpeg
                    # diagnostic survives (media.extract_clip now
                    # re-raises CalledProcessError as RuntimeError
                    # with the stderr tail attached).
                    full_msg = f"{type(res).__name__}: {res}"
                    logger.warning(
                        "Preview cut failed for %s[%d] in job %s: %s",
                        k, idx, job_id, full_msg,
                    )
                    public_per_kind[k][idx]["preview_error"] = full_msg[:2000]
                    continue
                cut_path, crop_uncertain = res
                from yt_scheduler.config import media_url
                public_per_kind[k][idx]["preview_url"] = media_url(str(cut_path))
                # YOLO-derived croppability flag (b-roll / screen content → neutral
                # center crop). Reuses the existing review-UI badge field.
                if crop_uncertain:
                    public_per_kind[k][idx]["crop_uncertain"] = True

        job["proposals"] = public_per_kind
        job["state"] = "done"
        job["progress_message"] = None
        _mark_terminal(job)
    except Exception as exc:
        logger.exception("Generate-from-source job %s failed", job_id)
        job["state"] = "failed"
        job["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
        _mark_terminal(job)
    finally:
        # Cancellation (server shutdown / task.cancel()) propagates a
        # BaseException that neither `except Exception` branch catches.
        # Without this finally, the job would stay non-terminal forever
        # and eviction (which only fires on done/failed) would never
        # reclaim the preview files. Mark + cleanup ourselves so
        # cancellation behaves like any other terminal state.
        if job.get("state") not in ("done", "failed"):
            job["state"] = "failed"
            job.setdefault("last_error", "Job was cancelled")
            _mark_terminal(job)
            cleanup_generate_previews(job_id)
