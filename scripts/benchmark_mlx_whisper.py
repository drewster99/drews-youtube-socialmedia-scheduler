"""Bench mlx-whisper across multiple model sizes against every video in
the uploads directory.

For each (model, video) it captures the full transcript text + SRT and
writes them to ``scripts/_whisper_outputs/<model>/<video-stem>.{txt,srt}``,
so you can diff transcripts side-by-side after the run finishes.

Default behavior runs all five common sizes
(tiny / base / small / medium / large-v3-turbo) in order. Pass model
repos as CLI args to override.

Each model run starts with a warmup on the shortest video so model
download / cold load doesn't pollute the timing.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

UPLOADS_DIR = (
    Path.home()
    / "Library/Application Support/com.nuclearcyborg.drews-socialmedia-scheduler/uploads"
)
OUTPUT_DIR = Path(__file__).resolve().parent / "_whisper_outputs"

# All Whisper models we care about, ordered roughly small → large. Skip the
# legacy large-v1/v2 (no reason to use them over v3 / turbo).
DEFAULT_MODELS = [
    "mlx-community/whisper-tiny-mlx",
    "mlx-community/whisper-base-mlx",
    "mlx-community/whisper-small-mlx",
    "mlx-community/whisper-medium-mlx",
    "mlx-community/whisper-large-v3-turbo",
]

VIDEO_EXTS = {".mov", ".mp4", ".m4v", ".mkv", ".webm", ".avi"}


def probe_duration(path: Path) -> float | None:
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            text=True, timeout=30,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        return None
    try:
        return float(out.strip())
    except ValueError:
        return None


def transcribe(path: Path, model: str) -> tuple[float, dict]:
    """Run mlx-whisper against ``path`` with ``model`` and return
    (wall_seconds, raw_result_dict)."""
    import mlx_whisper

    t0 = time.perf_counter()
    result = mlx_whisper.transcribe(
        str(path),
        path_or_hf_repo=model,
        verbose=False,
    )
    return time.perf_counter() - t0, result


def srt_timestamp(t: float) -> str:
    h, rem = divmod(t, 3600)
    m, s = divmod(rem, 60)
    millis = int((s - int(s)) * 1000)
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d},{millis:03d}"


def to_srt(segments: list[dict]) -> str:
    lines: list[str] = []
    for i, seg in enumerate(segments, 1):
        start = srt_timestamp(seg.get("start") or 0.0)
        end = srt_timestamp(seg.get("end") or 0.0)
        text = (seg.get("text") or "").strip()
        lines.append(f"{i}\n{start} --> {end}\n{text}\n")
    return "\n".join(lines)


def save_outputs(model: str, video: Path, result: dict) -> Path:
    model_short = model.split("/")[-1]
    out_dir = OUTPUT_DIR / model_short
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = video.stem.replace(" ", "_")
    text_path = out_dir / f"{stem}.txt"
    srt_path = out_dir / f"{stem}.srt"
    text_path.write_text((result.get("text") or "").strip() + "\n", encoding="utf-8")
    srt_path.write_text(to_srt(result.get("segments") or []), encoding="utf-8")
    return text_path


def fmt_duration(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    return f"{m}:{s:02d}"


def collect_videos() -> list[tuple[Path, float]]:
    videos: list[tuple[Path, float]] = []
    for path in sorted(UPLOADS_DIR.iterdir()):
        if path.suffix.lower() not in VIDEO_EXTS:
            continue
        duration = probe_duration(path)
        if duration is None or duration < 1.0:
            continue
        videos.append((path, duration))
    videos.sort(key=lambda pair: pair[1])
    return videos


def run_model(model: str, videos: list[tuple[Path, float]]) -> list[dict]:
    print(f"\n{'='*72}\n=== Model: {model}\n{'='*72}", flush=True)
    warmup_path, warmup_duration = videos[0]
    print(
        f"Warmup: {warmup_path.name} ({fmt_duration(warmup_duration)}) — "
        "first run pays model download / load cost; not counted.",
        flush=True,
    )
    warm_wall, _ = transcribe(warmup_path, model)
    print(f"Warmup done in {warm_wall:.1f}s.\n", flush=True)

    rows: list[dict] = []
    for path, duration in videos:
        print(f"Transcribing {path.name} ({fmt_duration(duration)})…", flush=True)
        wall, result = transcribe(path, model)
        text = (result.get("text") or "").strip()
        words = len(re.findall(r"\S+", text))
        speed = duration / wall if wall > 0 else 0.0
        wpm = (words / (duration / 60.0)) if duration > 0 else 0.0
        out_path = save_outputs(model, path, result)
        rows.append({
            "model": model,
            "video": path.name,
            "duration_s": duration,
            "wall_s": wall,
            "speed": speed,
            "words": words,
            "wpm": wpm,
            "out": out_path,
        })
        print(
            f"  → {wall:.1f}s wall, {speed:.2f}× realtime, "
            f"{words} words ({wpm:.0f} WPM); saved → {out_path.relative_to(Path.cwd())}",
            flush=True,
        )
    return rows


def main() -> int:
    if not UPLOADS_DIR.exists():
        print(f"Uploads directory not found: {UPLOADS_DIR}", file=sys.stderr)
        return 1
    videos = collect_videos()
    if not videos:
        print(f"No video files found in {UPLOADS_DIR}", file=sys.stderr)
        return 1

    models = sys.argv[1:] or DEFAULT_MODELS
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    for model in models:
        all_rows.extend(run_model(model, videos))

    print("\n\n## Summary — wall time by model × video\n")
    print("| Model | Video | Duration | Wall | Speed | Words | WPM |")
    print("|---|---|---:|---:|---:|---:|---:|")
    for r in all_rows:
        print(
            f"| `{r['model'].split('/')[-1]}` "
            f"| `{r['video']}` "
            f"| {fmt_duration(r['duration_s'])} "
            f"| {r['wall_s']:.1f}s "
            f"| {r['speed']:.2f}× "
            f"| {r['words']} "
            f"| {r['wpm']:.0f} |"
        )
    print(f"\nFull transcripts under: {OUTPUT_DIR.relative_to(Path.cwd())}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
