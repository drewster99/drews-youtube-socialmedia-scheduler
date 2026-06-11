"""Media processing — FFmpeg clips, GIFs, thumbnails."""

from __future__ import annotations

import json
import logging
import math
import secrets
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from yt_scheduler.config import UPLOAD_DIR

logger = logging.getLogger(__name__)


# Generous cap on a single ffmpeg invocation. Precise (sample-accurate)
# re-encode of a multi-minute clip on slow software path can take a few
# minutes; longer than this is almost always a hang. A hung ffmpeg
# permanently pins one of the cut-semaphore slots, so without this cap
# a handful of corrupt inputs would starve the whole Generate-from-
# source flow with no recovery short of a process restart.
_FFMPEG_TIMEOUT_SECONDS: int = 30 * 60  # 30 minutes
# Tighter cap for one-frame keyframe extraction — should be sub-second.
_FFMPEG_FRAME_TIMEOUT_SECONDS: int = 30


@dataclass(frozen=True)
class VideoProbe:
    """Summary of an on-disk video file as seen by ffprobe.

    Fields are ``None`` when ffprobe didn't return that piece — the caller
    must treat a missing field as "unknown" rather than 0. ``width`` and
    ``height`` come from the first video stream; rotation metadata is not
    applied so they describe the encoded frame, not the display frame
    (irrelevant for fidelity comparisons since both files being compared
    use the same convention).

    ``codec_name`` is the ffprobe codec_name string for the first video
    stream (e.g. ``h264``, ``hevc``, ``vp9``, ``av1``, ``prores``).
    ``container`` is a single canonical token derived from ffprobe's
    ``format_name`` — that field is a comma-separated list (``mov,mp4,m4a,3gp,3g2,mj2``),
    so we pick the first token that's a meaningful container for our
    purposes. Used by :func:`is_browser_playable`.
    """

    duration_seconds: float | None
    width: int | None
    height: int | None
    bitrate_bps: int | None
    size_bytes: int | None
    codec_name: str | None = None
    container: str | None = None
    has_audio: bool | None = None


def probe_video_file(video_path: str | Path) -> VideoProbe | None:
    """Run ffprobe once and return duration + dimensions + bitrate.

    Returns ``None`` when ffprobe isn't installed (the caller should treat
    that as "can't validate, accept best-effort"). Returns a ``VideoProbe``
    instance otherwise — its fields are individually ``None`` for any
    metric ffprobe didn't produce. A returned probe whose ``width``,
    ``height``, ``and`` ``duration_seconds`` are all ``None`` means
    ffprobe ran but the file has no readable video stream; callers
    treat that as "not a video file" rather than "unknown".
    """
    path = Path(video_path)
    if not path.exists():
        return None
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        return VideoProbe(None, None, None, None, None)
    if result.returncode != 0:
        return VideoProbe(None, None, None, None, None)
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return VideoProbe(None, None, None, None, None)

    streams = data.get("streams") or []
    fmt = data.get("format") or {}
    width = height = None
    codec_name: str | None = None
    has_audio: bool = False
    for stream in streams:
        codec_type = stream.get("codec_type")
        # Identify the video stream by codec_type, but also accept a stream that
        # carries width/height when codec_type is absent — a video stream always
        # has dimensions, an audio stream never does. This keeps us robust to
        # ffprobe output that omits codec_type (matching the pre-`-show_streams`
        # behavior that keyed off width presence).
        is_video = codec_type == "video" or (
            codec_type is None and ("width" in stream or "height" in stream)
        )
        if is_video and width is None:
            # Use the first video stream for dimensions/codec.
            try:
                width = int(stream["width"]) if "width" in stream else None
                height = int(stream["height"]) if "height" in stream else None
            except (TypeError, ValueError):
                pass
            raw_codec = stream.get("codec_name")
            if isinstance(raw_codec, str) and raw_codec:
                codec_name = raw_codec.lower()
        elif codec_type == "audio":
            has_audio = True

    # ffprobe's format_name is a comma-separated list of compatible
    # container labels. We pick the first non-generic token. This is a
    # heuristic ("good enough for our allowlist check") rather than a
    # ground truth — for browser-playability the (codec, container) pair
    # matches one of a small handful of known-good tuples, and the first
    # token is reliably the most specific.
    container: str | None = None
    raw_format = fmt.get("format_name")
    if isinstance(raw_format, str) and raw_format:
        container = raw_format.split(",")[0].strip().lower() or None

    def _maybe_float(value: object) -> float | None:
        try:
            return float(value) if value is not None else None  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    def _maybe_int(value: object) -> int | None:
        try:
            return int(value) if value is not None else None  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    return VideoProbe(
        duration_seconds=_maybe_float(fmt.get("duration")),
        width=width,
        height=height,
        bitrate_bps=_maybe_int(fmt.get("bit_rate")),
        size_bytes=_maybe_int(fmt.get("size")),
        codec_name=codec_name,
        container=container,
        has_audio=has_audio,
    )


# Safari-friendly (codec, container) pairs. Generated/preview UI in
# the browser only embeds a <video> element for files in this allowlist;
# anything else falls back to a YouTube iframe with #t=start,end. We
# scope to Safari because the macOS app is the target browser (system
# Safari / WKWebView).
#
# Conservative — not exhaustive. Some browsers play more than this in
# practice, but Safari/WKWebView has more edge cases than Chrome and
# the YouTube fallback is fine for the uncovered cases.
_BROWSER_PLAYABLE_PAIRS: frozenset[tuple[str, str]] = frozenset({
    ("h264", "mp4"),
    ("h264", "mov"),
    ("hevc", "mp4"),
    ("hevc", "mov"),
    ("vp9", "webm"),
    ("av1", "mp4"),
    ("av1", "webm"),
})


def is_browser_playable(
    codec_name: str | None, container: str | None,
) -> bool | None:
    """True when the (codec, container) pair plays in a `<video>` element
    on the target browser (Safari / WKWebView).

    Returns ``None`` when either piece is unknown — the caller should
    treat that as "don't know, probably best to fall back to the YouTube
    embed" rather than as a positive or negative answer.
    """
    if not codec_name or not container:
        return None
    return (codec_name.lower(), container.lower()) in _BROWSER_PLAYABLE_PAIRS


def source_quality_warnings(
    *,
    width: int | None,
    height: int | None,
    source_origin: str | None,
) -> list[dict]:
    """Structured warnings about a source video's quality.

    Returned as a list of ``{"code": str, "message": str, ...}`` dicts so the
    UI can render each one independently (and so logic elsewhere — e.g. the
    Generate-from-source modal — can branch on the codes). Empty list when
    there's nothing to warn about.

    The thresholds are deliberately permissive: we only warn for the cases
    that meaningfully hurt clip output (sub-HD source pixels, or a known-
    lossy YouTube re-download). Bitrate alone is a misleading signal with
    modern codecs and is intentionally excluded.
    """
    warnings: list[dict] = []

    if width is not None and height is not None:
        small_dim = min(width, height)
        if small_dim < 1080:
            warnings.append({
                "code": "low_resolution",
                "min_dimension": small_dim,
                "width": width,
                "height": height,
                "message": (
                    f"Source is {width}×{height} — under 1080p in its short "
                    "dimension. Clips cut from it will look soft on modern "
                    "phones. Consider attaching a higher-resolution master."
                ),
            })

    if source_origin == "youtube_download":
        warnings.append({
            "code": "youtube_download_lossy",
            "message": (
                "Source was re-downloaded from YouTube (lossy transcode). "
                "Clips inherit that fidelity. Attach the original master "
                "for best results."
            ),
        })

    return warnings


# --- Hardware encoder availability (videotoolbox on macOS) ----------------
#
# Probed once at import time and cached. On Apple Silicon the
# videotoolbox encoders are 5–10× faster than libx264 for typical
# settings; the catch is they have finite parallel session slots before
# the encoder engine starts queuing internally. The exact ceiling varies
# by chip, so the project caps Generate cuts at 4 concurrent hardware
# jobs (paired with an 8-wide software fallback).
#
# An empty result means ffmpeg isn't installed or wasn't built with
# videotoolbox — both software-only situations the caller treats as
# "use libx264". Probing once at import keeps the per-cut path
# subprocess-free.


def _detect_hardware_encoders() -> frozenset[str]:
    """Return the set of available ``*_videotoolbox`` encoder names.

    Failure modes (ffmpeg missing, build without videotoolbox, weird
    output) all degrade silently to ``frozenset()`` so the software path
    becomes the no-questions-asked default.
    """
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return frozenset()
    if result.returncode != 0:
        return frozenset()
    found: set[str] = set()
    for line in result.stdout.splitlines():
        # ffmpeg prints "V....D h264_videotoolbox     VideoToolbox H.264 Encoder".
        # The second whitespace-separated token is the encoder name.
        parts = line.split()
        if len(parts) >= 2 and parts[1].endswith("_videotoolbox"):
            found.add(parts[1])
    return frozenset(found)


# Lazy module-global so test runs that just import services.media don't
# all pay the subprocess at import time. First call to
# ``hardware_encoder_available`` populates it; subsequent calls hit the
# cached value. Tests that need to override it monkey-patch the global
# directly (matches the pattern used in tests/test_vertical_crop.py).
_HARDWARE_ENCODERS: frozenset[str] | None = None


def hardware_encoder_available(codec: str = "h264") -> bool:
    """True when ffmpeg can encode ``codec`` via videotoolbox.

    ``codec`` is the bare codec name (``h264`` / ``hevc``); the function
    appends ``_videotoolbox`` to match what ffmpeg lists.
    """
    global _HARDWARE_ENCODERS
    if _HARDWARE_ENCODERS is None:
        _HARDWARE_ENCODERS = _detect_hardware_encoders()
    return f"{codec}_videotoolbox" in _HARDWARE_ENCODERS


def probe_is_video(probe: VideoProbe | None) -> bool:
    """True when ``probe`` came back with at least one of width/height/duration.

    Returns False when ffprobe ran but produced nothing usable — the
    file isn't a recognisable video. Returns True when ``probe`` is
    ``None`` (ffprobe wasn't installed), because in that case the
    caller can't tell either way and "trust the user" is the right
    posture.
    """
    if probe is None:
        return True
    return (
        probe.width is not None
        or probe.height is not None
        or probe.duration_seconds is not None
    )


# 9:16 vertical crop output dimensions. Standard "Shorts/TikTok" size.
_VERTICAL_OUTPUT_WIDTH: int = 1080
_VERTICAL_OUTPUT_HEIGHT: int = 1920

# Hardware encoder bitrate target — videotoolbox needs an explicit
# -b:v (no -crf support). 6 Mbps gives clean 1080p output for social
# clips while keeping file sizes modest.
_HARDWARE_BITRATE: str = "6M"


def _videotoolbox_bitrate_for_output(width: int | None, height: int | None) -> str:
    """Pick a sensible videotoolbox bitrate based on output resolution.

    Hand-tuned to match libx264 ``-crf 20`` perceived quality on typical
    talking-head + camera content at each resolution. The bands are
    intentionally coarse — videotoolbox quality plateaus past these
    floors. When the dimensions are unknown we conservatively assume
    1080p so we don't underbit a 4K output.
    """
    if width is None or height is None:
        return _HARDWARE_BITRATE
    # Use the larger dimension so 1080x1920 vertical and 1920x1080
    # landscape pick the same bucket.
    big = max(int(width), int(height))
    if big >= 3000:   # 4K-class (3840x2160 or close)
        return "18M"
    if big >= 2000:   # 1440p-class
        return "10M"
    if big >= 1500:   # 1080p-class (1920x1080, 1080x1920)
        return "6M"
    if big >= 1000:   # 720p-class
        return "4M"
    return "2M"       # sub-720p (640x360, 640x480, etc.)


def _vertical_crop_filter(x_shift_normalized: float) -> str:
    """ffmpeg ``-vf`` filter chain that crops to 9:16 centered + scales.

    ``x_shift_normalized`` ∈ [-1.0, 1.0] moves the crop column away from
    center toward the right (positive) or left (negative) as a fraction
    of the available room (``iw - crop_width``). 0 is dead-center, which
    is what 3c uses by default. 3d's vision pass produces non-zero
    values from face position estimates.

    The expression handles both landscape and vertical sources without
    branching: ``min(iw, ih*9/16)`` picks the largest 9:16-wide column
    that fits, so a 1080×1920 phone clip leaves the column at full
    frame and a 1920×1080 horizontal clip pulls a 608-wide strip out of
    the middle.

    Output is always scaled to 1080×1920 — downscale from 4K is sharp;
    upscale from 720p is soft (we warn about that in the source-quality
    UI).
    """
    # Clamp the shift so the crop never walks off the source frame. A NaN/Inf
    # (e.g. from a malformed model proposal) survives min()/max() unchanged and
    # would be formatted into the crop expression as the literal "nan", making
    # ffmpeg fail to parse the filter — coerce it to a centered crop instead.
    raw_shift = float(x_shift_normalized)
    if not math.isfinite(raw_shift):
        raw_shift = 0.0
    shift = max(-1.0, min(1.0, raw_shift))
    # Force the crop width to an even integer ≤ the ideal 9:16 width.
    # ffmpeg's crop filter rejects non-integer dims, and libx264 (the
    # YUV 4:2:0 default) needs both width and height even.
    # floor(.../2)*2 rounds down to the nearest even integer.
    #
    # The literal comma inside ``min(iw,ih*9/16)`` MUST be escaped as
    # ``\,`` — ffmpeg's ``-vf`` parser treats unescaped commas as
    # filter-chain separators, so the unescaped form makes ffmpeg
    # parse the expression as four bogus filters and exit with
    # "No such filter: 'ih*9/16)/2)*2:...'". Comma between
    # ``crop=...:0`` and ``scale=...`` is intentional (chains them).
    cw = "floor(min(iw\\,ih*9/16)/2)*2"
    if shift == 0.0:
        x_expr = f"floor((iw-{cw})/2)"
    else:
        x_expr = f"floor((iw-{cw})/2+({shift:.4f})*(iw-{cw})/2)"
    return (
        f"crop={cw}:ih:{x_expr}:0,"
        f"scale={_VERTICAL_OUTPUT_WIDTH}:{_VERTICAL_OUTPUT_HEIGHT}"
    )


def _ffmpeg_timestamp_to_seconds(ts: str) -> float:
    """Parse an ffmpeg timestamp (``"SS.mmm"``, ``"MM:SS"``, ``"HH:MM:SS.mmm"``)
    into seconds. Used to position the audio fade-out from the clip duration."""
    parts = ts.split(":")
    try:
        return sum(float(p) * (60 ** i) for i, p in enumerate(reversed(parts)))
    except ValueError as exc:
        raise ValueError(f"malformed ffmpeg timestamp: {ts!r}") from exc


def extract_clip(
    video_path: str | Path,
    start: str,
    end: str,
    output_name: str | None = None,
    *,
    precise: bool = True,
    vertical_crop: bool = False,
    x_shift_normalized: float = 0.0,
    encoder: Literal["auto", "hardware", "software"] = "auto",
    preset: str | None = None,
    audio_fade_in: float = 0.0,
    audio_fade_out: float = 0.0,
) -> Path:
    """Extract a video clip from ``start`` to ``end``.

    ``start`` / ``end`` are ffmpeg-style timestamps — ``"0:30"`` or
    ``"1:30:00"``.

    Both seek modes put ``-ss`` *before* ``-i`` so ffmpeg can fast-seek
    the demuxer to a nearby keyframe instead of decoding the whole
    prefix from the start of the file. For a multi-GB 4K source with
    cuts late in the timeline this is the difference between minutes
    and seconds of wall clock per cut.

    ``precise=True`` (default) leaves ``-accurate_seek`` enabled (the
    ffmpeg default for transcoding): the demuxer jumps to the nearest
    preceding keyframe and then decodes-and-discards frames until the
    requested ``start`` so the output lands sample-accurately. The
    cost is decoding the keyframe-to-start prefix (usually <2 s).

    ``precise=False`` disables ``-accurate_seek`` so the cut snaps to
    the nearest preceding keyframe — may start up to one GOP early
    but skips the prefix decode entirely. Useful for preview
    thumbnails where exact start doesn't matter.

    Hardware-accelerated decode (``-hwaccel auto``) is always
    requested. On Apple Silicon that means videotoolbox, which is
    5–10× faster than software H.264/HEVC decode for 4K sources.
    ffmpeg falls back to software decode for codecs the hardware
    doesn't support.

    ``vertical_crop=True`` crops the output to 9:16 (1080×1920) using
    :func:`_vertical_crop_filter`. ``x_shift_normalized`` shifts the
    crop column away from dead-center; values come from the 3d vision
    pass when available, default 0 for 3c's center-only behaviour.

    ``encoder`` selects the H.264 encoder:

    * ``"auto"`` — videotoolbox when ffmpeg has it built in,
      libx264 otherwise. Bitrate is matched to the OUTPUT resolution
      via :func:`_videotoolbox_bitrate_for_output` so 4K parents
      aren't crushed at the 1080p target.
    * ``"hardware"`` — force ``h264_videotoolbox``. Raises if the
      detection at module-import said it's not available.
    * ``"software"`` — force ``libx264``.

    ``preset`` is libx264-only — pass e.g. ``"ultrafast"`` to trade
    encode quality for throughput. Ignored when ``encoder``
    resolves to videotoolbox (which has its own speed model).

    Callers wrap the call in the appropriate semaphore in
    ``services/clipper`` so concurrent cuts queue rather than fight.
    """
    video_path = Path(video_path)
    if output_name is None:
        output_name = f"{video_path.stem}_clip_{start.replace(':', '')}-{end.replace(':', '')}.mp4"

    output = UPLOAD_DIR / output_name
    # Encode to a temp sibling, then atomically rename to the final name only
    # after ffmpeg fully succeeds (including the slow ``+faststart`` moov-shift
    # pass). The temp's ``.cutpart_*`` name deliberately does NOT match the
    # ``gen_preview_*`` / clip cleanup globs, so a preview-cleanup sweep (on
    # confirm, job eviction, cancellation, or a startup orphan sweep after a
    # restart) can't delete the file out from under a still-running ffmpeg — the
    # race that produced "Unable to re-open output file for shifting data /
    # No such file or directory" once cuts from a large 4K master got slow
    # enough to widen the window. Keep the ``.mp4`` extension so ffmpeg still
    # picks the mp4 muxer. Orphaned temps are reaped by the startup sweep.
    tmp_output = UPLOAD_DIR / f".cutpart_{secrets.token_hex(8)}.mp4"

    use_hardware = False
    if encoder == "hardware":
        if not hardware_encoder_available("h264"):
            raise RuntimeError(
                "Hardware encoder requested but h264_videotoolbox is not "
                "available in this ffmpeg build."
            )
        use_hardware = True
    elif encoder == "auto":
        # videotoolbox is the right tool when available regardless of
        # vertical_crop; the previous gate ("only when vertical_crop")
        # left segments stuck on slow libx264 default-preset. The
        # output-resolution-aware bitrate picker means a 4K segment no
        # longer gets crushed at 6 Mbps — it picks 18 Mbps for 4K,
        # 10 Mbps for 1440p, 6 Mbps for 1080p, etc.
        use_hardware = hardware_encoder_available("h264")
    # else "software" — leave use_hardware False.

    cmd: list[str] = ["ffmpeg", "-y"]

    # Hardware-accelerated decode. On Apple Silicon ``-hwaccel auto``
    # resolves to videotoolbox, which decodes 4K H.264/HEVC many times
    # faster than the CPU. ffmpeg falls back to software decode for
    # codecs the hardware doesn't recognise, so this is safe to apply
    # to every cut regardless of source.
    cmd.extend(["-hwaccel", "auto"])
    # Fast container-level seek — demuxer jumps to the nearest
    # preceding keyframe instead of decoding the whole prefix.
    # ``-accurate_seek`` (the default for transcoding) keeps the cut
    # sample-accurate when ``precise=True``; we disable it for the
    # fast preview path where exact start doesn't matter.
    if not precise:
        cmd.extend(["-noaccurate_seek"])
    cmd.extend(["-ss", start, "-to", end, "-i", str(video_path)])

    # Rebase the output PTS so the first frame lands at exactly t=0. A
    # seek-then-re-encode otherwise leaves the video track starting ~1 frame
    # late (an edit-list offset, e.g. start_time=0.041s at 24fps); Safari and
    # the poster-thumbnail render that leading gap as a BLACK first frame even
    # though the frame data is fine. ``setpts=PTS-STARTPTS`` zeroes it. The
    # matching audio ``asetpts`` is applied with the fades below.
    video_filters = ["setpts=PTS-STARTPTS"]
    if vertical_crop:
        video_filters.append(_vertical_crop_filter(x_shift_normalized))
    cmd.extend(["-vf", ",".join(video_filters)])

    # Probe the source once when we need it for either bitrate selection
    # (hardware encoder, non-cropped) or audio-stream detection (fades).
    # Doing this before the fade block lets both uses share the same probe.
    needs_probe = (use_hardware and not vertical_crop) or (audio_fade_in > 0 or audio_fade_out > 0)
    probe: VideoProbe | None = probe_video_file(video_path) if needs_probe else None

    # Audio edge ramps. Cubic IN (sharp attack pops up at the first word) and a
    # gentler linear OUT (the word's natural decay / room-tone tail rings out
    # rather than being slammed). Positions are relative to the OUTPUT (``-ss``
    # before ``-i`` resets timestamps to 0). No video fade by design.
    #
    # Guard: only emit -af when the source has an audio stream. Applying
    # afade to a video-only input makes ffmpeg fail the filter graph
    # ("Stream specifier ':a' matches no streams"). When the probe
    # explicitly reports no audio (has_audio is False) we skip the filter
    # and log so callers can diagnose silent parents. When the probe is
    # unavailable (has_audio is None) we still emit the filter — unknown
    # audio presence is not the same as confirmed absence.
    source_has_audio = probe.has_audio if probe is not None else None
    has_fade = audio_fade_in > 0 or audio_fade_out > 0
    if source_has_audio is False and has_fade:
        logger.info(
            "Skipping audio fades for %s — source has no audio stream.",
            video_path,
        )
    elif has_fade:
        # Only touch audio when a fade was requested, so an audio-less source
        # (or one we couldn't probe) doesn't get an `-af` that fails the filter
        # graph. The video `setpts` above already fixes the black first frame;
        # the audio `asetpts` here keeps A/V aligned to that zeroed start.
        clip_duration = _ffmpeg_timestamp_to_seconds(end) - _ffmpeg_timestamp_to_seconds(start)
        # When the probe gives us the real file duration we can derive an
        # upper bound on the actual output duration and clamp the fade-out
        # start time so it never lands past EOF.
        if probe is not None and probe.duration_seconds is not None:
            max_out_duration = min(clip_duration, probe.duration_seconds)
        else:
            max_out_duration = clip_duration
        audio_filters: list[str] = ["asetpts=PTS-STARTPTS"]
        if audio_fade_in > 0:
            audio_filters.append(f"afade=t=in:curve=cub:st=0:d={audio_fade_in:.4f}")
        if audio_fade_out > 0:
            out_st = max(0.0, max_out_duration - audio_fade_out)
            audio_filters.append(f"afade=t=out:curve=tri:st={out_st:.4f}:d={audio_fade_out:.4f}")
        cmd.extend(["-af", ",".join(audio_filters)])

    if use_hardware:
        # Pick the bitrate by what the OUTPUT will actually be: 9:16
        # crop locks output to 1080×1920 (use 1080 as the larger dim),
        # otherwise mirror the source's max dim. Probe lazily; on
        # failure we conservatively assume 1080p.
        if vertical_crop:
            bitrate = _videotoolbox_bitrate_for_output(1080, 1920)
        else:
            # probe was already obtained above when needs_probe is True
            # (hardware + not vertical_crop satisfies that condition).
            bitrate = _videotoolbox_bitrate_for_output(
                probe.width if probe else None,
                probe.height if probe else None,
            )
        cmd.extend([
            "-c:v", "h264_videotoolbox",
            "-b:v", bitrate,
            "-c:a", "aac",
            "-movflags", "+faststart",
            str(tmp_output),
        ])
    else:
        cmd.extend(["-c:v", "libx264"])
        if preset:
            cmd.extend(["-preset", preset])
        cmd.extend([
            "-c:a", "aac",
            "-movflags", "+faststart",
            str(tmp_output),
        ])

    try:
        subprocess.run(
            cmd, check=True, capture_output=True,
            timeout=_FFMPEG_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        # Belt-and-braces cleanup of the half-written temp so it doesn't
        # masquerade as a usable file. The caller will see the exception
        # and treat the cut as failed.
        Path(tmp_output).unlink(missing_ok=True)
        raise
    except subprocess.CalledProcessError as exc:
        # Default str(CalledProcessError) just shows the argv and exit
        # code — ffmpeg's real error (filter parse error, missing
        # codec, etc.) lives in stderr. Re-raise as RuntimeError with
        # the last few lines of stderr appended so callers (and the
        # UI's preview_error display) see the actual reason instead
        # of "non-zero exit status N".
        Path(tmp_output).unlink(missing_ok=True)
        stderr_text = (exc.stderr or b"").decode("utf-8", errors="replace")
        # The bottom of ffmpeg's stderr is almost always the error
        # message (preceded by status spam). Last ~6 non-empty lines
        # is a reliable signal.
        tail = "\n".join(
            line for line in stderr_text.strip().splitlines()[-6:]
            if line.strip()
        )
        raise RuntimeError(
            f"ffmpeg exit {exc.returncode}: {tail or 'no stderr captured'}"
        ) from exc

    # ffmpeg fully succeeded (including faststart) — atomically publish the
    # finished file under its final name. os.replace is atomic within a dir.
    try:
        tmp_output.replace(output)
    except OSError:
        Path(tmp_output).unlink(missing_ok=True)
        raise
    return output


def extract_gif(
    video_path: str | Path,
    start: str,
    end: str,
    width: int = 480,
    fps: int = 10,
    output_name: str | None = None,
) -> Path:
    """Extract a GIF from a video clip."""
    video_path = Path(video_path)
    if output_name is None:
        output_name = f"{video_path.stem}_gif_{start.replace(':', '')}-{end.replace(':', '')}.gif"

    output = UPLOAD_DIR / output_name

    # Two-pass for better quality GIFs
    palette = UPLOAD_DIR / f"{video_path.stem}_palette.png"

    # The palette is an intermediate that's always removed (finally), and a
    # failure in either pass must not leave a half-written .gif behind that a
    # later consumer treats as valid — mirror extract_clip's cleanup-on-error.
    try:
        # Pass 1: generate palette
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-hwaccel", "auto",
                "-ss", start, "-to", end,
                "-i", str(video_path),
                "-vf", f"fps={fps},scale={width}:-1:flags=lanczos,palettegen",
                str(palette),
            ],
            check=True,
            capture_output=True,
            timeout=_FFMPEG_TIMEOUT_SECONDS,
        )

        # Pass 2: create GIF with palette. ``-hwaccel auto`` precedes only the
        # video ``-i`` so it accelerates that decode; the palette PNG input is
        # untouched by it.
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-hwaccel", "auto",
                "-ss", start, "-to", end,
                "-i", str(video_path),
                "-i", str(palette),
                "-lavfi", f"fps={fps},scale={width}:-1:flags=lanczos [x]; [x][1:v] paletteuse",
                str(output),
            ],
            check=True,
            capture_output=True,
            timeout=_FFMPEG_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        Path(output).unlink(missing_ok=True)
        raise
    finally:
        palette.unlink(missing_ok=True)

    return output


def get_video_duration(video_path: str | Path) -> float:
    """Get video duration in seconds. Returns 0.0 if duration cannot be read.

    Every failure mode resolves to 0.0 (the documented contract) rather than
    raising: a non-zero ffprobe exit (corrupt/unreadable file), a timeout, the
    binary being missing, an empty/"N/A" duration tag, or unparseable output.
    Callers treat ``<= 0`` as "no usable duration".
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "quiet",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=_FFMPEG_FRAME_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.warning("ffprobe duration probe failed for %s: %s", video_path, exc)
        return 0.0
    if result.returncode != 0:
        logger.warning(
            "ffprobe duration probe exited %d for %s", result.returncode, video_path
        )
        return 0.0
    raw = result.stdout.strip()
    # ffprobe outputs "N/A" when the container has no duration tag.
    if not raw or raw == "N/A":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        logger.warning("ffprobe returned unparseable duration %r for %s", raw, video_path)
        return 0.0


def extract_keyframes_in_range(
    video_path: str | Path,
    *,
    start_seconds: float,
    end_seconds: float,
    count: int = 3,
    max_width: int = 1024,
) -> list[bytes]:
    """Sample ``count`` JPEG keyframes evenly across a time range.

    Used by the 3d vision pass: Claude looks at a handful of frames from
    each proposed clip range to judge whether the subject is centered
    enough for a 9:16 vertical crop, and how far to shift if not.
    ``max_width`` is the encoded JPEG width — kept small (1024) so the
    multimodal token bill stays bounded; vision can read center-of-
    frame composition fine at that size.

    Returns ``[]`` when the range is empty, the file is missing, or
    ffmpeg refused — the caller treats that as "couldn't assess, accept
    as center" rather than as an error condition.
    """
    video_path = Path(video_path)
    if count < 1 or end_seconds <= start_seconds or not video_path.exists():
        return []

    span = end_seconds - start_seconds
    # Sample inside the range, not at the exact edges — clips often
    # start on a hard cut and the very first frame can be a black frame.
    pad = min(0.5, span * 0.1)
    if count == 1:
        timestamps = [start_seconds + span / 2]
    else:
        usable = max(span - 2 * pad, 0.1)
        step = usable / (count - 1)
        timestamps = [start_seconds + pad + i * step for i in range(count)]

    frames: list[bytes] = []
    for ts in timestamps:
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-hwaccel", "auto",
                    "-ss", f"{ts:.3f}",
                    "-i", str(video_path),
                    "-frames:v", "1",
                    "-vf", f"scale='min({max_width},iw)':-2",
                    "-q:v", "3",
                    "-f", "image2pipe",
                    "-vcodec", "mjpeg",
                    "-",
                ],
                check=False,
                capture_output=True,
                timeout=_FFMPEG_FRAME_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            # A hung single-frame extract is almost certainly a corrupt
            # source. Skip that timestamp and try the next; the vision
            # pass tolerates fewer frames.
            continue
        if result.returncode == 0 and result.stdout:
            frames.append(result.stdout)
    return frames


def extract_keyframes(
    video_path: str | Path,
    count: int = 6,
    *,
    max_width: int = 1024,
) -> list[bytes]:
    """Sample ``count`` JPEG keyframes evenly across the video.

    Returns a list of JPEG bytes — already encoded, ready to attach to a
    Claude vision message. The frames are scaled to ``max_width`` to
    keep request payloads small (and the per-image cost down).

    Used by the AI description path when a video has no spoken
    transcript: we send these frames to Claude instead so it can
    describe what's visible.
    """
    video_path = Path(video_path)
    if count < 1:
        return []

    duration = get_video_duration(video_path)
    if duration <= 0:
        return []

    # Avoid the absolute ends of the file (often leader/trailer frames).
    pad = min(0.5, duration * 0.05)
    if count == 1:
        timestamps = [duration / 2]
    else:
        span = max(duration - 2 * pad, 0.1)
        step = span / (count - 1)
        timestamps = [pad + i * step for i in range(count)]

    frames: list[bytes] = []
    for ts in timestamps:
        # Use -ss before -i for fast seek, then a single frame to stdout.
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-hwaccel", "auto",
                    "-ss", f"{ts:.3f}",
                    "-i", str(video_path),
                    "-frames:v", "1",
                    "-vf", f"scale='min({max_width},iw)':-2",
                    "-q:v", "3",
                    "-f", "image2pipe",
                    "-vcodec", "mjpeg",
                    "-",
                ],
                check=False,
                capture_output=True,
                timeout=_FFMPEG_FRAME_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            continue
        if result.returncode == 0 and result.stdout:
            frames.append(result.stdout)
    return frames


def generate_thumbnail(
    video_path: str | Path,
    timestamp: str = "0:05",
    output_name: str | None = None,
) -> Path:
    """Extract a frame from a video as a thumbnail."""
    video_path = Path(video_path)
    if output_name is None:
        output_name = f"{video_path.stem}_thumb.jpg"

    output = UPLOAD_DIR / output_name

    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-hwaccel", "auto",
                "-ss", timestamp,
                "-i", str(video_path),
                "-vframes", "1",
                "-q:v", "2",
                str(output),
            ],
            check=True,
            capture_output=True,
            timeout=_FFMPEG_FRAME_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        # Don't leave a truncated/partial .jpg behind that a later consumer
        # would treat as a valid thumbnail.
        Path(output).unlink(missing_ok=True)
        raise

    return output
