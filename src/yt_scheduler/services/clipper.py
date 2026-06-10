"""Generate-from-source clip proposals.

Each kind (hook / short / segment) is its own Claude call so the prompt
can be tuned per-kind via the existing prompt_templates editor. The
parent's SRT transcript goes in a shared cache-controlled message block
so when the three per-kind calls fan out in parallel they share a single
input-cache hit after the first call lands.

The Claude response is structured: the model is forced to call the
``propose_clips`` tool with a strict JSON-Schema-validated payload, so
we never have to text-parse a JSON blob.

Per-kind constraints:

| Kind    | Min (s) | Max (s)          | Output cap |
|---------|---------|------------------|------------|
| hook    | 5       | 30               | 8          |
| short   | 45      | 75               | 8          |
| segment | 60      | (parent length)  | 8          |

Server-side post-validation drops any proposal that:

- falls outside the kind's length band,
- starts before 0 or ends past the parent's duration,
- overlaps an existing same-kind cut range on this parent by more than
  ``_MAX_OVERLAP_FRACTION``,
- is the 9th+ entry returned by Claude for that kind.

A parent with fewer than ``kind_max + _PARENT_HEADROOM_SECONDS`` seconds
of duration is ineligible for that kind — the calling endpoint pre-flights
this and never asks for a kind it can't satisfy.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.services import ai, clip_edges, media as media_service
from yt_scheduler.services.background import spawn_background
from yt_scheduler.services.clip_edges import ClipUnit

logger = logging.getLogger(__name__)

ClipKind = Literal["hook", "short", "segment"]

_PER_KIND_BOUNDS: dict[ClipKind, tuple[float, float | None]] = {
    "hook": (5.0, 30.0),
    "short": (45.0, 75.0),
    # No fixed max for segments — capped at a fraction of the parent instead
    # (see _SEGMENT_MAX_PARENT_FRACTION) so a segment can be long but not the
    # whole video.
    "segment": (60.0, None),
}

# A "segment" can run long, but not be (nearly) the entire parent video.
_SEGMENT_MAX_PARENT_FRACTION: float = 0.9

# Parent must be at least kind_max + this much longer than the longest
# clip we'd cut. Generated promos are most useful when they're materially
# shorter than the parent; without this guard a 31-second parent could
# emit a 30-second "hook" that's effectively the whole video.
_PARENT_HEADROOM_SECONDS: float = 15.0

# Server-side cap. Claude is also told this in the prompt body, but we
# enforce it regardless so a prompt edit can't silently raise it.
# The Generate-from-source UI lets the user pick a per-kind cap; this
# is the default applied when the caller hasn't specified one, and
# ``MAX_PROPOSALS_PER_KIND_CAP`` is the absolute ceiling we'll honour
# (also the upper bound on the UI's number input).
DEFAULT_MAX_PROPOSALS_PER_KIND: int = 8
MAX_PROPOSALS_PER_KIND_CAP: int = 20
# Per-kind defaults matching the prototype's KIND_SPEC counts, applied when the
# caller (the Generate UI) didn't specify a cap for that kind.
_DEFAULT_MAX_PER_KIND: dict[ClipKind, int] = {"hook": 8, "short": 6, "segment": 6}
# When a kind already has cut clips on this parent, we ask Claude for a few
# extra candidates so that after post-LLM dedup/overlap removal we still have a
# full set of fresh ones. The final output is still capped at the base max.
_EXISTING_OVERREQUEST_BONUS: int = 3
# Back-compat alias used by older call sites + tests.
_OUTPUT_CAP_PER_KIND: int = DEFAULT_MAX_PROPOSALS_PER_KIND

# Overlap with an existing same-kind range that exceeds this fraction of
# the proposed clip's length → drop the proposal. The threshold is loose
# on purpose: small head/tail overlaps that produce a meaningfully
# different clip are fine; near-duplicates are not.
_MAX_OVERLAP_FRACTION: float = 0.5


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
    # Populated by the word-stream (index) proposal path; ignored by the
    # legacy anchor path. ``rating`` is the model's 1-4 self-score; the fade
    # lengths drive the audio ramps at cut time (see media.extract_clip).
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


def _normalize_anchor_text(text: str) -> str:
    """Collapse whitespace, lower-case, and strip non-word punctuation
    for fuzzy anchor matching.

    Claude can drop a trailing comma or period when quoting, change
    smart-quote variants, or write "doesn't" vs the cue's "doesn 't"
    artifact. Word characters + whitespace is the right granularity:
    permissive enough to ignore those nuisance variants but strict
    enough to keep distinct sentences distinct.
    """
    if not text:
        return ""
    stripped = _PUNCT_STRIP_RE.sub(" ", text.lower())
    return " ".join(stripped.split())


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
        return parent_duration_seconds >= min_s + _PARENT_HEADROOM_SECONDS
    return parent_duration_seconds >= max_s + _PARENT_HEADROOM_SECONDS


_PROPOSAL_TOOL = {
    "name": "propose_clips",
    "description": (
        "Submit your proposed clip ranges as structured data. Returns no "
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
                        "start_text_anchor": {
                            "type": "string",
                            "description": (
                                "Copy the EXACT text of the transcript "
                                "timeline line you want the clip to start "
                                "at — everything after the [MM:SS] anchor "
                                "on that line, verbatim. Do not summarise, "
                                "do not paraphrase, do not combine multiple "
                                "lines. The server resolves this text back "
                                "to a real timestamp by finding it in the "
                                "transcript; proposals whose anchor text "
                                "can't be located are dropped."
                            ),
                        },
                        "end_text_anchor": {
                            "type": "string",
                            "description": (
                                "Same rules as start_text_anchor, for the "
                                "LAST transcript line that should be "
                                "included in the clip. The clip's end "
                                "time is the start of the line AFTER this "
                                "anchor (so the anchor line plays in full)."
                            ),
                        },
                        "start_seconds": {
                            "type": "number",
                            "description": (
                                "Numeric start, in seconds, from the "
                                "[MM:SS] anchor on the start line. "
                                "Provided as a cross-check — if it "
                                "disagrees with the resolved anchor by "
                                "more than a few seconds, the anchor "
                                "wins. Do not estimate."
                            ),
                        },
                        "end_seconds": {
                            "type": "number",
                            "description": (
                                "Numeric end, in seconds. Same rules as "
                                "start_seconds — kept as a cross-check; "
                                "the anchor is authoritative."
                            ),
                        },
                        "title": {
                            "type": "string",
                            "description": (
                                "A punchy 4-8 word working title for the clip."
                            ),
                        },
                        "reason": {
                            "type": "string",
                            "description": (
                                "One sentence on why this range stands alone."
                            ),
                        },
                    },
                    "required": [
                        "start_text_anchor",
                        "end_text_anchor",
                        "start_seconds",
                        "end_seconds",
                        "title",
                        "reason",
                    ],
                },
            },
        },
        "required": ["proposals"],
    },
}


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

# Per-kind length window, content focus, and title tone for the index prompt.
_KIND_INDEX_GUIDANCE: dict[ClipKind, dict[str, str]] = {
    "hook": {
        "window": "5 to 30 seconds",
        "content": (
            "A hook is a single surprising, opinionated, useful, or candid "
            "moment with an immediate payoff — one clear point, no setup."
            "Since hooks are very short, include only minimal lead-in to "
            "the main point or punchline - a couple seconds at most. DO"
            "include reactions afterword, but again, very little beyond that."
            "The topic should begin right away."
        ),
        "title": (
            "The title IS the hook: 3-4 words ideally, but max 8 words, punchy and a little "
            "opinionated/divisive or questioning (never clickbait like 'You won't believe'); "
            "state the point, keep it short - few words - short words"
        ),
    },
    "short": {
        "window": "45 to 75 seconds",
        "content": (
            "A short is ONE complete mini-story or explanation: a brief setup "
            "and a satisfying payoff, understandable on its own — one coherent "
            "idea, not a grab-bag. Include minimal lead-in - a few seconds at most."
            "DO include reactions afterword, but not much beyond that."
        ),
        "title": "4-9 words, punchy and clear, opinionated or questioning; never clickbait.",
    },
    "segment": {
        "window": "at least 90 seconds (up to several minutes)",
        "content": (
            "A segment is a full, self-contained DISCUSSION of ONE topic from "
            "where it is introduced to where it wraps up, before the next topic."
            "These can be several minutes, but the sentence that starts the topic"
            " should begin within 5 seconds of the start of your selection"
        ),
        "title": (
            "5-10 words, clear, descriptive and informative — NOT divisive and "
            "NOT clickbait; name the topic - clear and brief"
        ),
    },
}


def _build_index_user_text(
    kind: ClipKind, units: list[ClipUnit], *, parent_title: str,
    max_proposals: int,
) -> str:
    """Assemble the instruction block + numbered units the model selects from."""
    spec = _KIND_INDEX_GUIDANCE[kind]
    min_s, max_s = _PER_KIND_BOUNDS[kind]
    durs = sorted(u.duration for u in units) or [1.0]
    median = durs[len(durs) // 2] or 1.0
    lo_units = max(1, round(min_s / median))
    hi_units = round((max_s if max_s is not None else min_s * 3) / median)
    visual = (
        "AUDIO ONLY: never pick a clip that depends on something visual (a "
        "chart, code on screen, a demo, 'look at this', 'right here'). If the "
        "words only make sense with a picture, skip it.\n"
    )
    return (
        f"You select {kind} clips from a podcast transcript of "
        f"\"{parent_title}\" for posting as standalone vertical videos.\n\n"
        "The transcript is a NUMBERED list of complete-thought units, one per "
        "line as `<index>\\t(<duration>s)\\t<text>`. Choose clips by referencing "
        "unit INDEX NUMBERS only — never write timestamps and never retype the "
        "text. A clip is a contiguous run of units from first_index through "
        "last_index inclusive.\n\n"
        f"## What makes a good {kind}\n"
        f"- Length: {spec['window']}.\n"
        f"- {spec['content']}\n"
        "- Self-contained: it makes sense with no other context. Starts and "
        "ends on a complete thought.\n"
        f"- {visual}\n"
        "## Title\n"
        f"- {spec['title']}\n\n"
        "## Length is a hard constraint — verify it\n"
        "You are not good at summing many numbers, so do it explicitly: add up "
        "the (Ns) values from first_index to last_index. If the total is above "
        "the maximum, drop units from the end; if below the minimum, extend. As "
        f"a rough guide that window is about {lo_units}-{hi_units} units here. A "
        "clip outside the length window is REJECTED, not trimmed for you.\n\n"
        "## Cross-check\nFor each clip also copy start_echo (the first ~6 words "
        "of first_index, verbatim) and end_echo (the last ~6 words of "
        "last_index, verbatim). These are a sanity check only — the indices are "
        "authoritative.\n\n"
        f"## Rating\nRate each clip 1-4 (4 = best), content and title together.\n\n"
        f"Propose UP TO {max_proposals} clips. Return FEWER (even zero) if there "
        "aren't that many strong ones — do not pad, do not overlap.\n\n"
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


def _validate_indexed_proposals(
    raw_proposals: list[dict], *, kind: ClipKind, units: list[ClipUnit],
    existing_ranges: list[tuple[float, float]], max_proposals: int,
    parent_duration_seconds: float,
) -> list[ProposedClip]:
    """Resolve index ranges to clips: drop bad/duplicate indices and any clip
    outside the kind's duration window; compute the gap-ramp edges."""
    min_s, max_s = _PER_KIND_BOUNDS[kind]
    # Segments have no fixed upper bound — cap at 90% of the parent so a
    # "segment" can run long but can't just be (nearly) the whole video.
    if max_s is None and parent_duration_seconds > 0:
        max_s = _SEGMENT_MAX_PARENT_FRACTION * parent_duration_seconds
    out: list[ProposedClip] = []
    accepted: list[tuple[float, float]] = []
    for entry in raw_proposals:
        if len(out) >= max_proposals:
            break
        try:
            first_index = int(entry["first_index"])
            last_index = int(entry["last_index"])
        except (KeyError, TypeError, ValueError):
            logger.info("Dropping clip proposal: missing/invalid indices %r", entry)
            continue
        title = str(entry.get("title") or "").strip() or f"Untitled {kind}"
        resolved = clip_edges.resolve_unit_range(units, first_index, last_index)
        if resolved is None:
            logger.info(
                "Dropping clip proposal %r: index range %s-%s out of bounds "
                "(have %d units).", title, first_index, last_index, len(units),
            )
            continue
        if resolved.duration < min_s or (max_s is not None and resolved.duration > max_s):
            logger.info(
                "Dropping clip proposal %r: duration %.1fs out of [%s, %s].",
                title, resolved.duration, min_s, max_s,
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
        edges = clip_edges.compute_edges(units, first_index, last_index)
        # Overlap guard — against already-cut clips and earlier proposals in
        # this same batch (the model is told not to overlap; enforce it).
        prior = existing_ranges + accepted
        if any(
            _overlap_seconds(edges.final_start, edges.final_end, s, e)
            > _MAX_OVERLAP_FRACTION * (edges.final_end - edges.final_start)
            for s, e in prior
        ):
            logger.info("Dropping clip proposal %r: overlaps an existing range.", title)
            continue
        rating = entry.get("rating")
        out.append(ProposedClip(
            kind=kind,
            start_seconds=edges.final_start,
            end_seconds=edges.final_end,
            title=title,
            reason=str(entry.get("reason") or "").strip(),
            rating=int(rating) if isinstance(rating, (int, float)) else None,
            audio_fade_in=edges.fade_in,
            audio_fade_out=edges.fade_out,
        ))
        accepted.append((edges.final_start, edges.final_end))
    return out


async def propose_clips_for_kind_indexed(
    *,
    kind: ClipKind,
    units: list[ClipUnit],
    parent_title: str,
    parent_duration_seconds: float,
    existing_ranges: list[tuple[float, float]],
    max_proposals: int | None = None,
) -> list[ProposedClip]:
    """Word-stream proposal: one per-kind Claude call over the numbered units."""
    if not is_parent_eligible_for_kind(parent_duration_seconds, kind) or not units:
        return []

    if max_proposals is None or max_proposals <= 0:
        base_max = _DEFAULT_MAX_PER_KIND.get(kind, DEFAULT_MAX_PROPOSALS_PER_KIND)
    else:
        base_max = min(max_proposals, MAX_PROPOSALS_PER_KIND_CAP)

    # Over-request a few extra candidates when this kind already has cut clips on
    # the parent: the new prompt gives Claude only unit indices + spans (no
    # timestamps), so we can't tell it which ranges to avoid — instead we ask for
    # more and drop duplicates/overlaps post-LLM, capping the output at base_max.
    ask_max = base_max
    if existing_ranges:
        ask_max = min(base_max + _EXISTING_OVERREQUEST_BONUS, MAX_PROPOSALS_PER_KIND_CAP)

    user_text = _build_index_user_text(
        kind, units, parent_title=parent_title, max_proposals=ask_max,
    )

    model = await ai._resolve_model()
    kwargs: dict[str, object] = {
        "model": model,
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": user_text}],
        "tools": [_INDEX_PROPOSAL_TOOL],
        "tool_choice": {"type": "tool", "name": "propose_clips"},
    }
    logger.info("Clip-proposal (index) request: kind=%s units=%d model=%s",
                kind, len(units), model)

    client = ai.get_client()
    try:
        message = await asyncio.to_thread(client.messages.create, **kwargs)
    except Exception as exc:
        logger.warning("Claude clip-proposal (index) call failed for %s: %s", kind, exc)
        return []

    raw_proposals: list[dict] = []
    for block in getattr(message, "content", []) or []:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", "") == "propose_clips":
            entries = (getattr(block, "input", None) or {}).get("proposals")
            if isinstance(entries, list):
                raw_proposals = [e for e in entries if isinstance(e, dict)]
            break

    proposals = _validate_indexed_proposals(
        raw_proposals, kind=kind, units=units,
        existing_ranges=existing_ranges, max_proposals=base_max,
        parent_duration_seconds=parent_duration_seconds,
    )
    logger.info("Clip-proposal (index) for %s: %d raw -> %d accepted (asked up to %d)",
                kind, len(raw_proposals), len(proposals), ask_max)
    return proposals


# Cap on simultaneously-running ffmpeg cut jobs. Precise cuts re-encode
# from the leading GOP forward. Two paths:
#
#   * Software (libx264) — CPU-bound, scales with cores. 8 in flight is
#     comfortable on Apple Silicon (M-series wide cores) without thrash.
#   * Hardware (videotoolbox) — uses the Media Engine block, which
#     serialises internally past 4 concurrent sessions. Quality does not
#     degrade beyond 4; they just queue. 4 keeps the queue shallow.
#
# Each cut acquires whichever lane it's actually going to use, so a
# generate confirm with vertical crops gets up to 4 hardware encodes in
# flight while non-crop cuts keep filling the 8 software slots
# independently.
#
# Lazily initialised on first use so that the semaphores are always
# created on the running event loop — avoids "bound to a different loop"
# errors when tests spin up a fresh loop per test or a server restart
# creates a new loop in-process.
_SOFTWARE_CUT_SEMAPHORE: asyncio.Semaphore | None = None
_HARDWARE_CUT_SEMAPHORE: asyncio.Semaphore | None = None


def _get_software_cut_semaphore() -> asyncio.Semaphore:
    global _SOFTWARE_CUT_SEMAPHORE
    if _SOFTWARE_CUT_SEMAPHORE is None:
        _SOFTWARE_CUT_SEMAPHORE = asyncio.Semaphore(8)
    return _SOFTWARE_CUT_SEMAPHORE


def _get_hardware_cut_semaphore() -> asyncio.Semaphore:
    global _HARDWARE_CUT_SEMAPHORE
    if _HARDWARE_CUT_SEMAPHORE is None:
        _HARDWARE_CUT_SEMAPHORE = asyncio.Semaphore(4)
    return _HARDWARE_CUT_SEMAPHORE


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
# 30 minutes is the compromise: covers slow review without bloating
# memory. _evict_stale_generate_jobs runs on every read/write of the
# dict, so no separate timer is needed.
_GENERATE_JOBS: dict[str, dict] = {}
_GENERATE_JOB_TTL_SECONDS: float = 30 * 60  # 30 minutes


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


# --- 3d: vision-based crop refinement -----------------------------------
#
# After the per-kind proposal calls land, any kind toggled for vertical
# crop in the modal gets a second pass: we sample ~3 keyframes per
# proposal and ask Claude vision whether the subject is centered enough
# for a plain center crop, or whether the crop window should shift, or
# whether the range should be flagged as "uncertain". The vision call's
# output is a structured assessment via the assess_crop tool.

# Cautious threshold for actually applying a shift. Below this, the
# vision pass might say "off_center, shift +0.05" — a near-zero offset
# almost certainly inside the model's noise floor. Default to center.
_MIN_SHIFT_TO_APPLY: float = 0.15

# Cap on concurrent vision-pass Claude calls. A Generate with crops on
# for hooks + shorts can produce up to 16 proposals; firing all of
# them simultaneously trips Anthropic's per-minute rate limits in
# practice. 4 in flight matches what the propose_clips fan-out does
# implicitly (one call per kind, 3 kinds max) and keeps the dominant
# input prompt cached across consecutive calls.
_VISION_CONCURRENCY: int = 4
_VISION_SEMAPHORE: asyncio.Semaphore | None = None


def _get_vision_semaphore() -> asyncio.Semaphore:
    global _VISION_SEMAPHORE
    if _VISION_SEMAPHORE is None:
        _VISION_SEMAPHORE = asyncio.Semaphore(_VISION_CONCURRENCY)
    return _VISION_SEMAPHORE


class _NullAsyncContext:
    """async-with-style no-op context manager. Used to keep
    ``assess_crop_for_proposal``'s ``async with lane`` shape uniform
    when no semaphore was supplied."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _null_async_context() -> _NullAsyncContext:
    return _NullAsyncContext()

_CROP_ASSESSMENT_TOOL = {
    "name": "assess_crop",
    "description": (
        "Submit your assessment of whether this clip range crops well to "
        "9:16 vertical, and how far to shift the crop column off center."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "classification": {
                "type": "string",
                "enum": [
                    "centered", "off_center", "drift", "multi_face", "no_face",
                ],
                "description": (
                    "centered: subject in center third throughout. "
                    "off_center: subject consistently in a side third. "
                    "drift: subject moves between thirds. "
                    "multi_face: multiple separated subjects. "
                    "no_face: no clear subject (b-roll / graphics)."
                ),
            },
            "x_shift_normalized": {
                "type": "number",
                "description": (
                    "Where to shift the 9:16 column from center, in "
                    "[-1.0, 1.0]. Negative = left, positive = right. "
                    "Set 0 for any classification other than off_center."
                ),
            },
            "confidence": {
                "type": "number",
                "description": "0–1 confidence in the classification.",
            },
        },
        "required": ["classification", "x_shift_normalized", "confidence"],
    },
}


@dataclass(frozen=True)
class CropAssessment:
    """Result of the vision pass on a single proposal range."""

    classification: Literal[
        "centered", "off_center", "drift", "multi_face", "no_face",
    ]
    x_shift_normalized: float
    confidence: float

    @property
    def uncertain(self) -> bool:
        """Show the 'uncertain crop' badge in the preview UI when set."""
        return self.classification in ("drift", "multi_face")


_NEUTRAL_ASSESSMENT = CropAssessment(
    classification="no_face", x_shift_normalized=0.0, confidence=0.0,
)


def _apply_assessment_shift(assessment: CropAssessment) -> float:
    """Map a vision assessment to the x_shift_normalized we actually
    feed ffmpeg.

    Only ``off_center`` proposals with a shift magnitude above
    :data:`_MIN_SHIFT_TO_APPLY` produce a non-zero result — everything
    else (centered / drift / multi_face / no_face) falls back to a
    plain center crop. This is the "cautious" behaviour the user asked
    for: better to give the user a center crop they can manually shift
    later than to reframe a shot they wanted as-is.
    """
    if assessment.classification != "off_center":
        return 0.0
    shift = assessment.x_shift_normalized
    if abs(shift) < _MIN_SHIFT_TO_APPLY:
        return 0.0
    return max(-1.0, min(1.0, shift))


async def assess_crop_for_proposal(
    *,
    proposal: ProposedClip,
    parent_video_path: Path,
    project_id: int,
    frame_count: int = 3,
    semaphore: asyncio.Semaphore | None = None,
) -> CropAssessment:
    """Run the vision pass for a single proposal.

    Returns ``_NEUTRAL_ASSESSMENT`` (== center crop) on any failure:
    missing frames, Claude unreachable, malformed tool response. The
    point of this pass is to *improve* the crop when it can; "no
    information" should never block a proposal.

    ``semaphore`` is honoured around the actual Claude call so the
    caller can rate-limit a fan-out (the refinement step in
    :func:`_run_generate_job` uses :data:`_VISION_SEMAPHORE`). When not
    supplied, the call runs ungated — appropriate for one-off use.
    """
    import base64
    from yt_scheduler.services import prompts as prompt_service

    frames = await asyncio.to_thread(
        media_service.extract_keyframes_in_range,
        parent_video_path,
        start_seconds=proposal.start_seconds,
        end_seconds=proposal.end_seconds,
        count=frame_count,
    )
    if not frames:
        return _NEUTRAL_ASSESSMENT
    lane = semaphore if semaphore is not None else _null_async_context()

    prompt = await prompt_service.get_prompt_with_fallback(
        "promo_clip_crop_refinement", project_id=project_id,
    )

    content: list[dict] = []
    for jpeg in frames:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(jpeg).decode("ascii"),
            },
        })
    content.append({"type": "text", "text": prompt["body"]})

    model = await ai._resolve_model()
    kwargs: dict[str, object] = {
        "model": model,
        "max_tokens": 512,
        "messages": [{"role": "user", "content": content}],
        "tools": [_CROP_ASSESSMENT_TOOL],
        "tool_choice": {"type": "tool", "name": "assess_crop"},
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    _log_crop_request(proposal, model, kwargs, content)

    try:
        async with lane:
            client = ai.get_client()
            message = await asyncio.to_thread(client.messages.create, **kwargs)
    except Exception as exc:
        logger.warning(
            "Crop-assessment vision call failed for %.1f-%.1f: %s",
            proposal.start_seconds, proposal.end_seconds, exc,
        )
        return _NEUTRAL_ASSESSMENT

    _log_crop_response(proposal, message)

    for block in getattr(message, "content", []) or []:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", "") == "assess_crop":
            tool_input = getattr(block, "input", None) or {}
            classification = tool_input.get("classification")
            if classification not in (
                "centered", "off_center", "drift", "multi_face", "no_face",
            ):
                logger.info(
                    "Crop assessment returned unknown classification %r; "
                    "falling back to neutral.", classification,
                )
                return _NEUTRAL_ASSESSMENT
            try:
                shift = float(tool_input.get("x_shift_normalized", 0.0))
                confidence = float(tool_input.get("confidence", 0.0))
            except (TypeError, ValueError):
                return _NEUTRAL_ASSESSMENT
            return CropAssessment(
                classification=classification,
                x_shift_normalized=shift,
                confidence=confidence,
            )
    return _NEUTRAL_ASSESSMENT


def _log_crop_request(
    proposal: ProposedClip, model: str, kwargs: dict, content: list[dict],
) -> None:
    """Dump the full Anthropic request for a crop-assessment call at INFO.

    Image blocks log the media type + byte length instead of the
    base64 payload (each frame is ~50-200 KB; logging the bytes would
    blow up the file with noise).
    """
    import json as _json
    try:
        sanitized: list[dict] = []
        for block in content:
            btype = block.get("type")
            if btype == "image":
                src = block.get("source") or {}
                data = src.get("data") or ""
                sanitized.append({
                    "type": "image",
                    "source": {
                        "type": src.get("type"),
                        "media_type": src.get("media_type"),
                        "data_length_bytes": len(data),
                    },
                })
            else:
                sanitized.append(block)
        system_str = kwargs.get("system") or ""
        uc_str = _json.dumps(sanitized, ensure_ascii=False, indent=2)
        tool_str = _json.dumps(kwargs.get("tools") or [], ensure_ascii=False)
        logger.info(
            "===== Claude crop-assessment REQUEST [%.1f-%.1f] =====\n"
            "model=%s tool_choice=%s max_tokens=%s\n"
            "----- system -----\n%s\n"
            "----- user content (%d blocks) -----\n%s\n"
            "----- tools -----\n%s\n"
            "===== end Claude crop-assessment REQUEST [%.1f-%.1f] =====",
            proposal.start_seconds, proposal.end_seconds,
            model,
            kwargs.get("tool_choice"),
            kwargs.get("max_tokens"),
            system_str,
            len(content),
            uc_str,
            tool_str,
            proposal.start_seconds, proposal.end_seconds,
        )
    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to log Claude crop-assessment request for %.1f-%.1f",
            proposal.start_seconds, proposal.end_seconds, exc_info=True,
        )


def _log_crop_response(proposal: ProposedClip, message) -> None:
    """Dump the full Anthropic response for a crop-assessment call at INFO."""
    import json as _json
    try:
        usage = getattr(message, "usage", None)
        usage_dict = {
            "input_tokens": getattr(usage, "input_tokens", None),
            "output_tokens": getattr(usage, "output_tokens", None),
        } if usage is not None else {}
        # Extract every tool_use block's input so the full structured
        # assessment is visible (not just the one we end up using).
        tool_uses: list[dict] = []
        for block in getattr(message, "content", []) or []:
            if getattr(block, "type", None) == "tool_use":
                tool_uses.append({
                    "name": getattr(block, "name", None),
                    "input": getattr(block, "input", None),
                })
        logger.info(
            "===== Claude crop-assessment RESPONSE [%.1f-%.1f] =====\n"
            "stop_reason=%s usage=%s\n"
            "----- tool_use blocks (n=%d) -----\n%s\n"
            "===== end Claude crop-assessment RESPONSE [%.1f-%.1f] =====",
            proposal.start_seconds, proposal.end_seconds,
            getattr(message, "stop_reason", None),
            _json.dumps(usage_dict),
            len(tool_uses),
            _json.dumps(tool_uses, ensure_ascii=False, indent=2),
            proposal.start_seconds, proposal.end_seconds,
        )
    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to log Claude crop-assessment response for %.1f-%.1f",
            proposal.start_seconds, proposal.end_seconds, exc_info=True,
        )


async def cut_clip_from_parent(
    *,
    parent_video_path: Path,
    proposal: ProposedClip,
    vertical_crop: bool = False,
    x_shift_normalized: float = 0.0,
) -> Path:
    """Cut ``proposal`` out of ``parent_video_path`` to a new MP4 in
    UPLOAD_DIR. Returns the absolute path of the new file.

    Sample-accurate (precise=True). ``vertical_crop`` requests a 9:16
    (1080×1920) output with optional ``x_shift_normalized`` to follow
    a non-center subject (3d feeds non-zero shift values from vision).

    Encoder selection + concurrency:

    * Hardware (videotoolbox) is preferred when ffmpeg was built with
      it; the cut is gated by :data:`_HARDWARE_CUT_SEMAPHORE` (4-wide).
    * Software (libx264) is the fallback; gated by
      :data:`_SOFTWARE_CUT_SEMAPHORE` (8-wide).

    The two semaphores are independent, so a mixed batch (some hardware
    cuts, some software) fills both lanes at once. Output extension is
    always ``.mp4`` regardless of the parent's container — the
    YouTube-upload step that runs next prefers MP4 anyway.
    """
    # Mirror extract_clip's encoder choice (auto + vertical_crop +
    # hardware-available → videotoolbox; otherwise libx264) so the lock
    # we hold matches the actual encoder lane we're about to use. If we
    # mis-routed (e.g. acquired the hardware lane for a software cut),
    # the system would still be correct but lane utilisation would skew.
    # Mirror extract_clip's "auto" choice: hardware whenever ffmpeg
    # has h264_videotoolbox, regardless of vertical_crop. The output-
    # resolution-aware bitrate picker inside extract_clip handles the
    # 4K case so we don't crush large sources at the 1080p bitrate.
    will_use_hardware = media_service.hardware_encoder_available("h264")
    semaphore = (
        _get_hardware_cut_semaphore() if will_use_hardware
        else _get_software_cut_semaphore()
    )
    async with semaphore:
        out_name = f"clip_{proposal.kind}_{secrets.token_hex(6)}.mp4"
        out_path = UPLOAD_DIR / out_name
        try:
            await asyncio.to_thread(
                media_service.extract_clip,
                parent_video_path,
                _format_ffmpeg_timestamp(proposal.start_seconds),
                _format_ffmpeg_timestamp(proposal.end_seconds),
                output_name=out_name,
                precise=True,
                vertical_crop=vertical_crop,
                x_shift_normalized=x_shift_normalized,
                encoder="auto",
                audio_fade_in=proposal.audio_fade_in,
                audio_fade_out=proposal.audio_fade_out,
            )
        except Exception:
            # ffmpeg can leave a partial mp4 behind on non-zero exit /
            # timeout. Without this cleanup the file leaks into
            # UPLOAD_DIR forever — visible to nothing (no row points
            # at it) and slowly fills disk on a flaky parent.
            out_path.unlink(missing_ok=True)
            raise
        return out_path


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
    x_shift_normalized: float = 0.0,
) -> Path:
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
    # Mirror extract_clip's "auto" choice: hardware whenever ffmpeg
    # has h264_videotoolbox, regardless of vertical_crop. The output-
    # resolution-aware bitrate picker inside extract_clip handles the
    # 4K case so we don't crush large sources at the 1080p bitrate.
    will_use_hardware = media_service.hardware_encoder_available("h264")
    semaphore = (
        _get_hardware_cut_semaphore() if will_use_hardware
        else _get_software_cut_semaphore()
    )
    out_name = _preview_filename(job_id, proposal.kind, idx)
    out_path = UPLOAD_DIR / out_name
    async with semaphore:
        # try/finally with a success flag so a CancelledError (which
        # inherits from BaseException, not Exception, in 3.11+) also
        # triggers the partial-file unlink. The plain `except Exception`
        # we'd otherwise want would skip past it and leak the half-
        # written mp4.
        ok = False
        try:
            await asyncio.to_thread(
                media_service.extract_clip,
                parent_video_path,
                _format_ffmpeg_timestamp(proposal.start_seconds),
                _format_ffmpeg_timestamp(proposal.end_seconds),
                output_name=out_name,
                precise=True,
                vertical_crop=vertical_crop,
                x_shift_normalized=x_shift_normalized,
                encoder="auto",
                audio_fade_in=proposal.audio_fade_in,
                audio_fade_out=proposal.audio_fade_out,
            )
            ok = True
        finally:
            if not ok:
                out_path.unlink(missing_ok=True)
    return out_path


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
    """Delete every ``gen_preview_*.mp4`` file in UPLOAD_DIR regardless
    of job_id. Run on startup so previews that survived a previous
    process being killed (``_GENERATE_JOBS`` is in-memory; restart
    wipes the dict and there's no list of job_ids to glob against)
    don't accumulate on disk forever.

    Returns the number of files removed (for logging).
    """
    removed = 0
    try:
        for path in UPLOAD_DIR.glob(f"{_PREVIEW_PREFIX}*.mp4"):
            try:
                path.unlink()
                removed += 1
            except OSError as exc:
                logger.debug("Could not remove orphan preview %s: %s", path, exc)
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
) -> dict[ClipKind, list[ProposedClip]]:
    """Fan out one Claude call per requested kind, in parallel.

    Returns a dict keyed by kind, in the same order ``kinds`` was passed.
    Kinds the parent is ineligible for are returned as empty lists so the
    UI can render "0 proposals" rather than the request silently
    disappearing.

    ``units`` is the word-stream segmentation built from the on-device
    transcriber's word timing — always the index-based proposal path (there is
    no anchor-text fallback). ``max_per_kind`` is the user-selected per-kind
    cap; when ``None`` (or a kind missing from it) the per-kind default applies.
    """
    if not kinds:
        return {}

    caps = max_per_kind or {}

    async def _one(k: ClipKind) -> tuple[ClipKind, list[ProposedClip]]:
        proposals = await propose_clips_for_kind_indexed(
            kind=k,
            units=units,
            parent_title=parent_title,
            parent_duration_seconds=parent_duration_seconds,
            existing_ranges=existing_ranges_per_kind.get(k, []),
            max_proposals=caps.get(k),
        )
        return k, proposals

    results = await asyncio.gather(*(_one(k) for k in kinds))
    return dict(results)


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
    from yt_scheduler.database import get_db

    if not rejected:
        return 0
    db = await get_db()
    written = 0
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
    if written:
        await db.commit()
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


async def delete_rejection(*, rejection_id: int) -> bool:
    """Restore a rejected proposal — i.e. drop its row.

    Returns True when a row was actually deleted (the rejection
    existed); False when the id was unknown. The caller's HTTP layer
    can map that to a 404 if it wants strictness, or just shrug.
    """
    from yt_scheduler.database import get_db

    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM generate_rejections WHERE id = ?", (int(rejection_id),),
    )
    await db.commit()
    return bool(cursor.rowcount)


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
    assessment: CropAssessment | None = None,
    vision_crashed: bool = False,
) -> dict:
    """JSON-safe representation of a proposal for the preview response.

    When the kind was toggled for vertical crop and the vision pass ran,
    the resulting :class:`CropAssessment` is attached so the UI can:

    * Show the actual ``x_shift_normalized`` that will be applied at
      cut time (after the cautious-shift threshold).
    * Render the "uncertain crop" badge for drift / multi-face cases.

    ``vision_crashed=True`` marks proposals whose vision call raised an
    unhandled exception — the UI badges them as "uncertain" with a
    distinct ``crop_classification = 'vision_error'`` so the user sees
    the same warning as drift / multi-face rather than silently
    assuming a clean center crop.

    For kinds without crop, or when vision wasn't run, ``assessment`` is
    ``None`` and the only crop-related field on the dict is the inert
    ``vertical_crop`` mirror of the kind setting.
    """
    out: dict[str, object] = {
        "kind": p.kind,
        "start_seconds": p.start_seconds,
        "end_seconds": p.end_seconds,
        "duration_seconds": p.duration_seconds,
        "title": p.title,
        "reason": p.reason,
        "rating": p.rating,
        # Audio edge ramps from the word-stream path; carried through so the
        # final cut applies the same fades as the preview (0 on the anchor path).
        "audio_fade_in": p.audio_fade_in,
        "audio_fade_out": p.audio_fade_out,
        "vertical_crop": crop_vertical,
    }
    if vision_crashed:
        out["x_shift_normalized"] = 0.0
        out["crop_classification"] = "vision_error"
        out["crop_confidence"] = 0.0
        out["crop_uncertain"] = True
    elif assessment is not None:
        out["x_shift_normalized"] = _apply_assessment_shift(assessment)
        out["crop_classification"] = assessment.classification
        out["crop_confidence"] = assessment.confidence
        out["crop_uncertain"] = assessment.uncertain
    else:
        out["x_shift_normalized"] = 0.0
    return out


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
            normalised_max[k] = _DEFAULT_MAX_PER_KIND.get(
                k, DEFAULT_MAX_PROPOSALS_PER_KIND
            )
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

        try:
            result = await asyncio.to_thread(
                transcription.transcribe,
                video_path=job["parent_video_path"],
                backend="macos-speech",
                language="en",
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
        proposals = await propose_all_clips(
            kinds=job["kinds"],
            units=units,
            parent_title=job["parent_title"],
            parent_duration_seconds=job["parent_duration_seconds"],
            existing_ranges_per_kind=job["existing_ranges_per_kind"],
            project_id=int(job["project_id"]),
            max_per_kind=job.get("max_per_kind"),
        )

        # Refinement — for kinds the user toggled for vertical crop,
        # sample keyframes and ask Claude vision whether the subject is
        # centered enough for a plain center crop. Run in parallel
        # across all (kind, proposal) pairs that need it; per-call
        # failure falls back to a neutral assessment (centered, no
        # shift) so we never block a proposal on a flaky vision call.
        # Vertical 9:16 crop is deferred to a separate later post-processing
        # step, so Generate cuts the clips uncropped (like the prototype). With
        # every kind forced non-crop here, the keyframe vision pass below never
        # runs and the cuts apply no crop; the crop infrastructure stays in the
        # tree for that later step.
        crop_for_kind = {k: False for k in proposals}
        refinement_tasks: list[tuple[str, int, asyncio.Task]] = []
        for k, props in proposals.items():
            if not crop_for_kind.get(k, False):
                continue
            for idx, prop in enumerate(props):
                task = asyncio.create_task(
                    assess_crop_for_proposal(
                        proposal=prop,
                        parent_video_path=Path(job["parent_video_path"]),
                        project_id=int(job["project_id"]),
                        semaphore=_get_vision_semaphore(),
                    )
                )
                refinement_tasks.append((k, idx, task))

        assessments: dict[tuple[str, int], CropAssessment | None] = {}
        # Crashes are distinct from neutral assessments — the UI flags
        # the former with the same "uncertain crop" badge it uses for
        # drift / multi-face, so the user can see "vision had no
        # opinion" rather than silently assuming a clean center crop.
        crashed: set[tuple[str, int]] = set()
        if refinement_tasks:
            job["state"] = "refining_crops"
            job["progress_message"] = (
                f"Checking framing on {len(refinement_tasks)} "
                f"crop{'s' if len(refinement_tasks) != 1 else ''}…"
            )
            results = await asyncio.gather(
                *(t for _, _, t in refinement_tasks),
                return_exceptions=True,
            )
            for (k, idx, _), res in zip(refinement_tasks, results):
                if isinstance(res, Exception):
                    logger.warning(
                        "Vision pass crashed for %s[%d]: %s", k, idx, res,
                    )
                    assessments[(k, idx)] = None
                    crashed.add((k, idx))
                else:
                    assessments[(k, idx)] = res

        # Build public dicts first so we have the same crop +
        # x_shift_normalized values the preview cuts must use.
        public_per_kind: dict[str, list[dict]] = {
            k: [
                proposal_to_public_dict(
                    p,
                    crop_vertical=crop_for_kind.get(k, False),
                    assessment=assessments.get((k, idx)),
                    vision_crashed=(k, idx) in crashed,
                )
                for idx, p in enumerate(v)
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
        # cuts_completed is read by the polling endpoint so the UI can
        # show "M of N" instead of just a single static label. Bumped
        # by each task's wrapper as soon as ffmpeg returns.
        job["cuts_total"] = total
        job["cuts_completed"] = 0
        job["progress_message"] = f"Cutting clips… 0 of {total}"
        parent_path = Path(job["parent_video_path"])

        async def _cut_and_count(
            k: str, p_idx: int, p: ProposedClip, kind_crop: bool, x_shift: float,
        ) -> Path:
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
                from yt_scheduler.config import media_url
                public_per_kind[k][idx]["preview_url"] = media_url(str(res))

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
