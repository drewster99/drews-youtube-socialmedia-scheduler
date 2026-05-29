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
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.services import ai, media as media_service

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
    rendered_body = ai._render_template_body(
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

# Back-compat name for tests written against the pre-3c API. New code
# should use _SOFTWARE_CUT_SEMAPHORE / _HARDWARE_CUT_SEMAPHORE directly.
_CUT_SEMAPHORE = _SOFTWARE_CUT_SEMAPHORE


# In-flight Generate-from-source preview jobs. Same pattern as
# auto_actions._UPLOAD_JOBS — keyed by job_id, fields read by the
# client's polling endpoint and updated by the background task.
#
# State machine: pending → transcribing? → proposing → done|failed.
# ``transcribing`` only appears when the parent had no usable transcript
# at preview-start; the chain fires a fresh whisper run inline. ``done``
# state carries ``proposals: dict[kind → list[ProposedClip-as-dict]]``
# which the client renders.
_GENERATE_JOBS: dict[str, dict] = {}


def get_generate_job(job_id: str) -> dict | None:
    """Read of a generate job's current state for the polling endpoint."""
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
        return out_path


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


def proposal_to_public_dict(p: ProposedClip) -> dict:
    """JSON-safe representation of a proposal for the preview response."""
    return {
        "kind": p.kind,
        "start_seconds": p.start_seconds,
        "end_seconds": p.end_seconds,
        "duration_seconds": p.duration_seconds,
        "title": p.title,
        "reason": p.reason,
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
    task = asyncio.create_task(_run_generate_job(job_id))
    _generate_tasks.add(task)
    task.add_done_callback(_generate_tasks.discard)
    return job_id


# Keep a strong ref to background tasks so the asyncio GC doesn't reap
# them before they complete.
_generate_tasks: set[asyncio.Task] = set()


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
            # transcribe endpoint uses.
            backend_to_source = {
                "mlx-whisper": "mlx_whisper",
                "whisper.cpp": "whispercpp",
                "macos-speech": "apple_speech",
            }
            source = backend_to_source.get(result.backend, "user_edited")
            transcript_id = await transcript_service.upsert_transcript_for_source(
                job["parent_id"], source, srt,
            )
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

        job["proposals"] = {
            k: [proposal_to_public_dict(p) for p in v]
            for k, v in proposals.items()
        }
        job["state"] = "done"
        job["progress_message"] = None
    except Exception as exc:
        logger.exception("Generate-from-source job %s failed", job_id)
        job["state"] = "failed"
        job["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
