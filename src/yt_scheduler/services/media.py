"""Media processing — FFmpeg clips, GIFs, thumbnails."""

from __future__ import annotations

import subprocess
from pathlib import Path

from yt_scheduler.config import UPLOAD_DIR


def extract_clip(
    video_path: str | Path,
    start: str,
    end: str,
    output_name: str | None = None,
) -> Path:
    """Extract a video clip.

    start/end: timestamps like "0:30" or "1:30:00"
    """
    video_path = Path(video_path)
    if output_name is None:
        output_name = f"{video_path.stem}_clip_{start.replace(':', '')}-{end.replace(':', '')}.mp4"

    output = UPLOAD_DIR / output_name

    subprocess.run(
        [
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-ss", start,
            "-to", end,
            "-c:v", "libx264",
            "-c:a", "aac",
            "-movflags", "+faststart",
            str(output),
        ],
        check=True,
        capture_output=True,
    )

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

    # Pass 1: generate palette
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-ss", start, "-to", end,
            "-i", str(video_path),
            "-vf", f"fps={fps},scale={width}:-1:flags=lanczos,palettegen",
            str(palette),
        ],
        check=True,
        capture_output=True,
    )

    # Pass 2: create GIF with palette
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-ss", start, "-to", end,
            "-i", str(video_path),
            "-i", str(palette),
            "-lavfi", f"fps={fps},scale={width}:-1:flags=lanczos [x]; [x][1:v] paletteuse",
            str(output),
        ],
        check=True,
        capture_output=True,
    )

    # Clean up palette
    palette.unlink(missing_ok=True)

    return output


def get_video_duration(video_path: str | Path) -> float:
    """Get video duration in seconds."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v", "quiet",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(video_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return float(result.stdout.strip())


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
        result = subprocess.run(
            [
                "ffmpeg",
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
        )
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

    subprocess.run(
        [
            "ffmpeg", "-y",
            "-ss", timestamp,
            "-i", str(video_path),
            "-vframes", "1",
            "-q:v", "2",
            str(output),
        ],
        check=True,
        capture_output=True,
    )

    return output
