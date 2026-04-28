"""Bench Apple SFSpeechRecognizer against the same uploads videos used
by the MLX benchmark. Saves the full transcript so we can compare it
side-by-side with the MLX outputs.

Calls the same ``_try_macos_speech`` Apple Speech path the app uses,
so what we measure here is exactly what the running app will do.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from yt_scheduler.services import transcription  # noqa: E402

UPLOADS_DIR = (
    Path.home()
    / "Library/Application Support/com.nuclearcyborg.drews-socialmedia-scheduler/uploads"
)
OUTPUT_DIR = Path(__file__).resolve().parent / "_whisper_outputs" / "apple-speech"
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
        return float(out.strip())
    except Exception:
        return None


def fmt_duration(s: float) -> str:
    m, sec = divmod(int(round(s)), 60)
    return f"{m}:{sec:02d}"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    videos: list[tuple[Path, float]] = []
    for path in sorted(UPLOADS_DIR.iterdir()):
        if path.suffix.lower() not in VIDEO_EXTS:
            continue
        d = probe_duration(path)
        if d is None or d < 1.0:
            continue
        videos.append((path, d))
    videos.sort(key=lambda p: p[1])

    rows = []
    for path, duration in videos:
        print(f"Apple Speech: {path.name} ({fmt_duration(duration)})…", flush=True)
        # _try_macos_speech expects a WAV (16 kHz mono). Use the same audio
        # extraction the app does so we measure the real codepath.
        audio_path = transcription._extract_audio(path)
        try:
            t0 = time.perf_counter()
            result = transcription._try_macos_speech(audio_path, language=None)
            wall = time.perf_counter() - t0
        finally:
            audio_path.unlink(missing_ok=True)

        if result is None or not result.segments:
            print(f"  → {wall:.1f}s wall, NO SEGMENTS RETURNED", flush=True)
            rows.append({
                "video": path.name, "duration": duration, "wall": wall,
                "words": 0, "segments": 0, "speed": duration / wall if wall > 0 else 0,
            })
            (OUTPUT_DIR / f"{path.stem.replace(' ', '_')}.txt").write_text("(no segments)\n", encoding="utf-8")
            continue

        text = " ".join(s.text.strip() for s in result.segments).strip()
        words = len(re.findall(r"\S+", text))
        speed = duration / wall if wall > 0 else 0
        wpm = words / (duration / 60) if duration > 0 else 0
        rows.append({
            "video": path.name, "duration": duration, "wall": wall,
            "words": words, "segments": len(result.segments), "speed": speed,
            "wpm": wpm,
        })
        out_path = OUTPUT_DIR / f"{path.stem.replace(' ', '_')}.txt"
        out_path.write_text(text + "\n", encoding="utf-8")
        print(
            f"  → {wall:.1f}s wall, {speed:.2f}× realtime, "
            f"{words} words ({len(result.segments)} segments, {wpm:.0f} WPM); "
            f"saved → {out_path.relative_to(Path.cwd())}", flush=True,
        )

    print("\n## Apple Speech results\n")
    print("| Video | Duration | Wall | Speed | Segments | Words | WPM |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        wpm = r.get("wpm", 0)
        print(
            f"| `{r['video']}` "
            f"| {fmt_duration(r['duration'])} "
            f"| {r['wall']:.1f}s "
            f"| {r['speed']:.2f}× "
            f"| {r['segments']} "
            f"| {r['words']} "
            f"| {wpm:.0f} |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
