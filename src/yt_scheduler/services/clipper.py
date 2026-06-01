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
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.services import ai, media as media_service
from yt_scheduler.services.background import spawn_background

logger = logging.getLogger(__name__)

ClipKind = Literal["hook", "short", "segment"]

_PER_KIND_BOUNDS: dict[ClipKind, tuple[float, float | None]] = {
    "hook": (5.0, 30.0),
    "short": (45.0, 75.0),
    # No hard max for segments — the parent's duration is the cap, which
    # the caller passes in.
    "segment": (60.0, None),
}

_PER_KIND_PROMPT_KEY: dict[ClipKind, str] = {
    "hook": "promo_clip_proposals_hook",
    "short": "promo_clip_proposals_short",
    "segment": "promo_clip_proposals_segment",
}

# Parent must be at least kind_max + this much longer than the longest
# clip we'd cut. Generated promos are most useful when they're materially
# shorter than the parent; without this guard a 31-second parent could
# emit a 30-second "hook" that's effectively the whole video.
_PARENT_HEADROOM_SECONDS: float = 15.0

# Server-side cap. Claude is also told this in the prompt body, but we
# enforce it regardless so a prompt edit can't silently raise it.
_OUTPUT_CAP_PER_KIND: int = 8

# Overlap with an existing same-kind range that exceeds this fraction of
# the proposed clip's length → drop the proposal. The threshold is loose
# on purpose: small head/tail overlaps that produce a meaningfully
# different clip are fine; near-duplicates are not.
_MAX_OVERLAP_FRACTION: float = 0.5

# Default crop instructions injected when the caller flags a kind as
# vertically cropped. Lives here rather than in the prompt seed body so
# the seed stays kind-focused; the block is appended into
# {{crop_constraints}} via the existing render engine.
VERTICAL_CROP_PROMPT_BLOCK: str = (
    "This clip will be hard-cropped to 9:16 vertical by taking the "
    "center column of the source frame. The left and right thirds "
    "are discarded. Avoid proposing ranges where the transcript "
    "implies visual context outside the center: 'as you can see on "
    "the left', 'look at this chart', a wide shot with two speakers "
    "side-by-side, on-screen text/captions/graphics that the audio "
    "refers to, demos / hand actions on one side. When in doubt, skip "
    "the range — a great hook elsewhere beats a forced one here.\n\n"
)


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


def _format_timestamp(seconds: float) -> str:
    """``MM:SS`` (or ``H:MM:SS`` for ranges past an hour) for the
    existing-ranges block fed to the prompt."""
    seconds = max(0.0, float(seconds))
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _build_existing_ranges_block(
    existing: list[tuple[float, float]], kind: ClipKind,
) -> str:
    """Pre-format the already-cut ranges of this kind for the prompt body.

    Empty list → empty string (the body uses ``{{existing_ranges_block??}}``
    so the variable substitution renders to nothing without leaving an
    awkward "Avoid: (none)" line).
    """
    if not existing:
        return ""
    pretty = ", ".join(
        f"{_format_timestamp(s)}–{_format_timestamp(e)}" for s, e in existing
    )
    return (
        f"Avoid proposing ranges that overlap these existing {kind}s "
        f"on this parent: {pretty}.\n\n"
    )


def _overlap_seconds(
    a_start: float, a_end: float, b_start: float, b_end: float,
) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def _validate_proposals(
    raw: list[dict],
    *,
    kind: ClipKind,
    parent_duration_seconds: float,
    existing_ranges: list[tuple[float, float]],
) -> list[ProposedClip]:
    """Filter Claude's raw tool output against the kind's contract.

    Quietly drops invalid entries rather than failing the whole call —
    Claude will sometimes pad slightly past the cap or land just outside
    the band, and dropping is the right user experience (we show the user
    the proposals we accepted, not "the model misbehaved"). Caps at
    :data:`_OUTPUT_CAP_PER_KIND` after filtering.
    """
    min_s, max_s = _PER_KIND_BOUNDS[kind]
    effective_max = max_s if max_s is not None else parent_duration_seconds

    accepted: list[ProposedClip] = []
    for entry in raw:
        try:
            start = float(entry["start_seconds"])
            end = float(entry["end_seconds"])
            title = str(entry.get("title") or "").strip()
            reason = str(entry.get("reason") or "").strip()
        except (KeyError, TypeError, ValueError):
            logger.info("Dropping clip proposal with malformed fields: %r", entry)
            continue

        # NaN / infinity slip past every numeric comparison below (IEEE
        # 754: any comparison with NaN is False). They'd then crash
        # _format_ffmpeg_timestamp(NaN) at cut time with a useless
        # 'cannot convert float NaN to integer' error. Reject up front.
        if not (math.isfinite(start) and math.isfinite(end)):
            logger.info(
                "Dropping clip proposal with non-finite times: %r", entry,
            )
            continue

        if not title:
            logger.info("Dropping clip proposal with empty title: %r", entry)
            continue
        if start < 0 or end > parent_duration_seconds + 0.5:
            logger.info(
                "Dropping clip proposal outside parent bounds (%.1f-%.1f, parent=%.1f)",
                start, end, parent_duration_seconds,
            )
            continue
        duration = end - start
        if duration < min_s or duration > effective_max:
            logger.info(
                "Dropping %s proposal of %.1fs (band is %.1f-%.1f)",
                kind, duration, min_s, effective_max,
            )
            continue

        # Same-kind overlap filter.
        drop = False
        for ex_start, ex_end in existing_ranges:
            overlap = _overlap_seconds(start, end, ex_start, ex_end)
            if overlap / max(duration, 0.001) > _MAX_OVERLAP_FRACTION:
                logger.info(
                    "Dropping %s proposal %.1f-%.1f as overlap with existing %.1f-%.1f is %.0f%%",
                    kind, start, end, ex_start, ex_end, 100 * overlap / duration,
                )
                drop = True
                break
        if drop:
            continue

        accepted.append(ProposedClip(
            kind=kind,
            start_seconds=start,
            end_seconds=end,
            title=title,
            reason=reason,
        ))

        if len(accepted) >= _OUTPUT_CAP_PER_KIND:
            break

    return accepted


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
                        "start_seconds": {
                            "type": "number",
                            "description": (
                                "Sample-accurate start of the clip, in "
                                "seconds, taken from a transcript "
                                "timestamp without rounding."
                            ),
                        },
                        "end_seconds": {
                            "type": "number",
                            "description": (
                                "Sample-accurate end of the clip, in "
                                "seconds, taken from a transcript "
                                "timestamp without rounding."
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
                        "start_seconds", "end_seconds", "title", "reason",
                    ],
                },
            },
        },
        "required": ["proposals"],
    },
}


async def propose_clips_for_kind(
    *,
    kind: ClipKind,
    transcript_srt: str,
    parent_title: str,
    parent_duration_seconds: float,
    existing_ranges: list[tuple[float, float]],
    crop_vertical: bool,
    project_id: int,
) -> list[ProposedClip]:
    """Single per-kind Claude call. See module docstring for contract.

    ``existing_ranges`` is a list of (start_seconds, end_seconds) tuples for
    clips already cut from this parent under this same kind — fed to the
    prompt so Claude doesn't re-propose them, then enforced server-side.
    """
    from yt_scheduler.services import prompts as prompt_service

    if not is_parent_eligible_for_kind(parent_duration_seconds, kind):
        return []

    prompt = await prompt_service.get_prompt_with_fallback(
        _PER_KIND_PROMPT_KEY[kind], project_id=project_id,
    )
    # _PER_KIND_BOUNDS is the single source of truth for the length
    # band — render the numbers into the prompt body via variables so a
    # future bound change updates the prompt and the validator together.
    # Segments have no fixed max; we render an empty string so the
    # prompt's "No fixed maximum" prose still reads cleanly.
    min_s, max_s = _PER_KIND_BOUNDS[kind]
    rendered_body = await ai._render_template_body(
        prompt["body"],
        {
            "parent_title": parent_title,
            "parent_duration_human": _format_duration_human(parent_duration_seconds),
            "existing_ranges_block": _build_existing_ranges_block(
                existing_ranges, kind,
            ),
            "crop_constraints": (
                VERTICAL_CROP_PROMPT_BLOCK if crop_vertical else ""
            ),
            "min_seconds": f"{int(min_s)}",
            "max_seconds": f"{int(max_s)}" if max_s is not None else "",
            "max_proposals": str(_OUTPUT_CAP_PER_KIND),
        },
    )

    # Two-block user message: the transcript is the cache-controlled
    # prefix (shared across the three parallel per-kind calls), the
    # per-kind rendered body is the differing tail.
    user_content: list[dict] = [
        {
            "type": "text",
            "text": (
                "Below is the SRT transcript of the parent video. Use the "
                "timestamps as anchor points for any ranges you propose.\n\n"
                + transcript_srt
            ),
            "cache_control": {"type": "ephemeral"},
        },
        {
            "type": "text",
            "text": rendered_body,
        },
    ]

    kwargs: dict[str, object] = {
        "model": await ai._resolve_model(),
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": user_content}],
        "tools": [_PROPOSAL_TOOL],
        "tool_choice": {"type": "tool", "name": "propose_clips"},
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    client = ai.get_client()
    try:
        message = await asyncio.to_thread(client.messages.create, **kwargs)
    except Exception as exc:
        logger.warning("Claude clip-proposal call failed for %s: %s", kind, exc)
        return []

    raw_proposals: list[dict] = []
    for block in getattr(message, "content", []) or []:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", "") == "propose_clips":
            tool_input = getattr(block, "input", None) or {}
            entries = tool_input.get("proposals")
            if isinstance(entries, list):
                raw_proposals = [e for e in entries if isinstance(e, dict)]
            break

    return _validate_proposals(
        raw_proposals,
        kind=kind,
        parent_duration_seconds=parent_duration_seconds,
        existing_ranges=existing_ranges,
    )


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
_SOFTWARE_CUT_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(8)
_HARDWARE_CUT_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(4)


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
_VISION_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(_VISION_CONCURRENCY)


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

    kwargs: dict[str, object] = {
        "model": await ai._resolve_model(),
        "max_tokens": 512,
        "messages": [{"role": "user", "content": content}],
        "tools": [_CROP_ASSESSMENT_TOOL],
        "tool_choice": {"type": "tool", "name": "assess_crop"},
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

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
    will_use_hardware = (
        vertical_crop and media_service.hardware_encoder_available("h264")
    )
    semaphore = (
        _HARDWARE_CUT_SEMAPHORE if will_use_hardware
        else _SOFTWARE_CUT_SEMAPHORE
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


_PREVIEW_MAX_SECONDS = 25.0


async def cut_preview_for_proposal(
    *,
    job_id: str,
    parent_video_path: Path,
    proposal: ProposedClip,
    idx: int,
    vertical_crop: bool = False,
    x_shift_normalized: float = 0.0,
) -> Path:
    """Cut a small .mp4 preview of a proposal so the review page can
    show the actual clip (with crop applied) instead of seeking into
    the parent video.

    Tuned for throughput, not fidelity. The user is scanning a grid
    of proposals — they need to see *enough* to judge the framing and
    content, not the full 5-minute segment. We cap the cut at
    :data:`_PREVIEW_MAX_SECONDS`, use ``precise=False`` (fast
    container-level seek; may land up to one GOP early), and pass
    ``-preset ultrafast`` to libx264. Segment previews drop from
    minutes to seconds.

    Same ``vertical_crop`` + ``x_shift_normalized`` the final cut will
    use, so what the user sees is what they'll get for framing — the
    final cut from the Confirm step is the one with ``precise=True``
    + default preset, so length and quality match expectations there.

    Always uses the software lane (``encoder="software"``); the
    hardware lane stays reserved for the final cuts so the user
    doesn't see Confirm queue up behind preview work.

    Filename pattern (``gen_preview_<job_id>_<kind>_<idx>.mp4``) is
    deterministic so cleanup can glob them without bookkeeping per
    proposal on the job dict.
    """
    # Cap the cut window. The proposal's start is preserved; the end
    # is the earlier of the proposal end and start+MAX.
    duration = max(0.0, float(proposal.end_seconds) - float(proposal.start_seconds))
    preview_end = float(proposal.start_seconds) + min(duration, _PREVIEW_MAX_SECONDS)

    out_name = _preview_filename(job_id, proposal.kind, idx)
    out_path = UPLOAD_DIR / out_name
    async with _SOFTWARE_CUT_SEMAPHORE:
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
                _format_ffmpeg_timestamp(preview_end),
                output_name=out_name,
                precise=False,
                vertical_crop=vertical_crop,
                x_shift_normalized=x_shift_normalized,
                encoder="software",
                preset="ultrafast",
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
    crop_vertical_for_kind: dict[ClipKind, bool],
    transcript_srt: str,
    parent_title: str,
    parent_duration_seconds: float,
    existing_ranges_per_kind: dict[ClipKind, list[tuple[float, float]]],
    project_id: int,
) -> dict[ClipKind, list[ProposedClip]]:
    """Fan out one Claude call per requested kind, in parallel.

    Returns a dict keyed by kind, in the same order ``kinds`` was passed.
    Kinds the parent is ineligible for are returned as empty lists so the
    UI can render "0 proposals" rather than the request silently
    disappearing.
    """
    if not kinds:
        return {}

    async def _one(k: ClipKind) -> tuple[ClipKind, list[ProposedClip]]:
        proposals = await propose_clips_for_kind(
            kind=k,
            transcript_srt=transcript_srt,
            parent_title=parent_title,
            parent_duration_seconds=parent_duration_seconds,
            existing_ranges=existing_ranges_per_kind.get(k, []),
            crop_vertical=crop_vertical_for_kind.get(k, False),
            project_id=project_id,
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
    _GENERATE_JOBS[job_id] = {
        "job_id": job_id,
        "parent_id": parent_id,
        "project_id": project_id,
        "parent_video_path": parent_video_path,
        "parent_title": parent_title,
        "parent_duration_seconds": parent_duration_seconds,
        "kinds": list(kinds),
        "crop_vertical": dict(crop_vertical_for_kind),
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
    """Background task: ensure transcript, fan out per-kind proposals,
    write the result onto the job dict for the polling endpoint."""
    from yt_scheduler.database import get_db
    from yt_scheduler.services import transcripts as transcript_service

    job = _GENERATE_JOBS.get(job_id)
    if job is None:
        return

    try:
        # Read the latest transcript off the parent row. The row state
        # may have changed since the preview endpoint took the snapshot
        # (e.g. another flow finished transcribing), so do the recheck
        # under the same task rather than trusting the captured value.
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT transcript, transcript_source FROM videos WHERE id = ?",
            (job["parent_id"],),
        )
        transcript = ""
        if rows:
            transcript = rows[0]["transcript"] or ""

        if not transcript_service.has_timestamps(transcript):
            # Inline auto-transcribe. Single path — we don't try to detect
            # an in-progress transcription started by another flow. Worst
            # case the user wastes a whisper cycle; the result upserts
            # into the same transcripts.text column.
            job["state"] = "transcribing"
            job["progress_message"] = "Transcribing parent video…"
            from yt_scheduler.services import transcription

            try:
                result = await asyncio.to_thread(
                    transcription.transcribe,
                    video_path=job["parent_video_path"],
                    model="large-v3",
                )
            except Exception as exc:
                logger.warning(
                    "Generate-from-source transcription failed for %s: %s",
                    job["parent_id"], exc,
                )
                job["state"] = "failed"
                job["last_error"] = (
                    f"Could not auto-transcribe the parent video ({exc})."
                )
                return

            srt = result.to_srt()
            # Persist so the next Generate run on this parent doesn't
            # have to re-transcribe — same upsert path the manual
            # transcribe endpoint uses. Map result.backend to the
            # canonical source enum; an unknown backend stays
            # 'machine_unknown' rather than silently masquerading as
            # 'user_edited' (which would mis-key the dedup upsert).
            backend_to_source = {
                "mlx-whisper": "mlx_whisper",
                "whisper.cpp": "whispercpp",
                "macos-speech": "apple_speech",
            }
            source = backend_to_source.get(result.backend)
            if source is None:
                logger.warning(
                    "Generate-from-source: unknown transcription backend %r; "
                    "persisting as mlx_whisper as a safe default.",
                    result.backend,
                )
                source = "mlx_whisper"
            transcript_id = await transcript_service.upsert_transcript_for_source(
                job["parent_id"], source, srt,
            )
            # Re-read the row before mirroring whisper output onto it: a
            # concurrent flow (the user manually picking another
            # transcript while ours ran for minutes) could have changed
            # the active transcript. Only overwrite when the active
            # transcript is still empty / non-edited / our own source.
            cur_rows = await db.execute_fetchall(
                "SELECT transcript_id, transcript_source, transcript_is_edited "
                "FROM videos WHERE id = ?",
                (job["parent_id"],),
            )
            cur = dict(cur_rows[0]) if cur_rows else {}
            cur_edited = bool(cur.get("transcript_is_edited"))
            cur_id = cur.get("transcript_id")
            if cur_edited or (
                cur_id is not None and cur_id != transcript_id
            ):
                logger.info(
                    "Generate-from-source: parent transcript changed during "
                    "whisper run; leaving the user's selection in place "
                    "and using our fresh SRT only for the in-flight job.",
                )
            else:
                await db.execute(
                    """UPDATE videos SET
                        transcript = ?,
                        transcript_id = ?,
                        transcript_source = ?,
                        transcript_updated_at = datetime('now'),
                        transcript_is_edited = 0,
                        updated_at = datetime('now')
                    WHERE id = ?""",
                    (srt, transcript_id, source, job["parent_id"]),
                )
                await db.commit()
            transcript = srt

        # Proposing — fan out the per-kind calls.
        job["state"] = "proposing"
        job["progress_message"] = "Asking Claude to propose clips…"
        proposals = await propose_all_clips(
            kinds=job["kinds"],
            crop_vertical_for_kind=job["crop_vertical"],
            transcript_srt=transcript,
            parent_title=job["parent_title"],
            parent_duration_seconds=job["parent_duration_seconds"],
            existing_ranges_per_kind=job["existing_ranges_per_kind"],
            project_id=int(job["project_id"]),
        )

        # Refinement — for kinds the user toggled for vertical crop,
        # sample keyframes and ask Claude vision whether the subject is
        # centered enough for a plain center crop. Run in parallel
        # across all (kind, proposal) pairs that need it; per-call
        # failure falls back to a neutral assessment (centered, no
        # shift) so we never block a proposal on a flaky vision call.
        crop_for_kind = job["crop_vertical"]
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
                        semaphore=_VISION_SEMAPHORE,
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

        # Cut a small preview file per proposal so the review page can
        # show the actual clip (with crop) instead of seeking into the
        # full parent. Concurrent within the software-lane semaphore.
        # One failure doesn't kill the rest — the UI falls back to the
        # full-parent #t= preview when preview_url is missing.
        job["state"] = "cutting_previews"
        total = sum(len(v) for v in proposals.values())
        job["progress_message"] = f"Cutting {total} preview clip{'s' if total != 1 else ''}…"
        parent_path = Path(job["parent_video_path"])
        preview_tasks: list[tuple[str, int, asyncio.Task]] = []
        for k, v in proposals.items():
            kind_crop = crop_for_kind.get(k, False)
            for idx, p in enumerate(v):
                pub = public_per_kind[k][idx]
                preview_tasks.append((k, idx, asyncio.create_task(
                    cut_preview_for_proposal(
                        job_id=job_id,
                        parent_video_path=parent_path,
                        proposal=p,
                        idx=idx,
                        vertical_crop=kind_crop,
                        x_shift_normalized=float(pub.get("x_shift_normalized") or 0.0),
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
                    msg = f"{type(res).__name__}: {res}"[:400]
                    logger.warning(
                        "Preview cut failed for %s[%d] in job %s: %s",
                        k, idx, job_id, msg,
                    )
                    public_per_kind[k][idx]["preview_error"] = msg
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
