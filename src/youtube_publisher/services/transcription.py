"""On-device video transcription.

Attempts backends in order of preference:
1. MLX Whisper — fastest on Apple Silicon Macs
2. faster-whisper — fast on any platform (CTranslate2 backend)
3. whisper.cpp (CLI) — if installed as a system binary
4. macOS SFSpeechRecognizer — built-in, no downloads needed

Produces both plain text transcripts and SRT subtitle files.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from youtube_publisher.config import UPLOAD_DIR

logger = logging.getLogger(__name__)


@dataclass
class TranscriptWord:
    """A single word with precise timestamps."""

    start: float  # seconds
    end: float  # seconds
    word: str
    probability: float = 1.0


@dataclass
class TranscriptSegment:
    """A single segment of transcribed text with timestamps."""

    start: float  # seconds
    end: float  # seconds
    text: str
    words: list[TranscriptWord] | None = None  # word-level timestamps if available


@dataclass
class TranscriptionResult:
    """Result of a transcription."""

    segments: list[TranscriptSegment]
    backend: str  # which engine was used
    language: str | None = None
    has_word_timestamps: bool = False

    @property
    def text(self) -> str:
        """Plain text transcript."""
        return " ".join(seg.text.strip() for seg in self.segments)

    @property
    def all_words(self) -> list[TranscriptWord]:
        """Flat list of all words with timestamps."""
        words = []
        for seg in self.segments:
            if seg.words:
                words.extend(seg.words)
        return words

    def to_srt(self, max_words_per_line: int | None = None) -> str:
        """Convert to SRT subtitle format.

        If max_words_per_line is set and word timestamps are available,
        creates shorter, more readable subtitle lines.
        """
        if max_words_per_line and self.has_word_timestamps:
            return self._word_level_srt(max_words_per_line)

        lines = []
        for i, seg in enumerate(self.segments, 1):
            start = _format_srt_time(seg.start)
            end = _format_srt_time(seg.end)
            lines.append(f"{i}\n{start} --> {end}\n{seg.text.strip()}\n")
        return "\n".join(lines)

    def _word_level_srt(self, max_words: int) -> str:
        """Generate SRT with word-level timing for shorter subtitle lines."""
        if max_words < 1:
            raise ValueError(f"max_words must be >= 1, got {max_words}")

        words = self.all_words
        if not words:
            return self.to_srt()

        lines = []
        idx = 1
        i = 0
        while i < len(words):
            chunk = words[i : i + max_words]
            text = " ".join(w.word.strip() for w in chunk).strip()
            if text:
                start = _format_srt_time(chunk[0].start)
                end = _format_srt_time(chunk[-1].end)
                lines.append(f"{idx}\n{start} --> {end}\n{text}\n")
                idx += 1
            i += max_words
        return "\n".join(lines)

    def to_vtt(self) -> str:
        """Convert to WebVTT subtitle format."""
        lines = ["WEBVTT\n"]
        for seg in self.segments:
            start = _format_vtt_time(seg.start)
            end = _format_vtt_time(seg.end)
            lines.append(f"{start} --> {end}\n{seg.text.strip()}\n")
        return "\n".join(lines)

    def to_json(self) -> list[dict]:
        """Export as JSON with full word-level detail."""
        result = []
        for seg in self.segments:
            entry = {"start": seg.start, "end": seg.end, "text": seg.text}
            if seg.words:
                entry["words"] = [
                    {"start": w.start, "end": w.end, "word": w.word, "probability": w.probability}
                    for w in seg.words
                ]
            result.append(entry)
        return result

    def save_srt(self, video_path: str | Path, max_words_per_line: int | None = 8) -> Path:
        """Save SRT file. Uses word-level timing for cleaner subtitles if available."""
        out = UPLOAD_DIR / f"{Path(video_path).stem}.srt"
        out.write_text(self.to_srt(max_words_per_line), encoding="utf-8")
        return out

    def save_vtt(self, video_path: str | Path) -> Path:
        """Save VTT file."""
        out = UPLOAD_DIR / f"{Path(video_path).stem}.vtt"
        out.write_text(self.to_vtt(), encoding="utf-8")
        return out


def _format_srt_time(seconds: float) -> str:
    """Format seconds as SRT timestamp: HH:MM:SS,mmm"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def _format_vtt_time(seconds: float) -> str:
    """Format seconds as VTT timestamp: HH:MM:SS.mmm"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def _extract_audio(video_path: Path) -> Path:
    """Extract audio from video to a temp WAV file (16kHz mono, required by Whisper)."""
    fd, tmp_path = tempfile.mkstemp(suffix=".wav")
    os.close(fd)
    audio_path = Path(tmp_path)
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-ar", "16000",
            "-ac", "1",
            "-c:a", "pcm_s16le",
            str(audio_path),
        ],
        check=True,
        capture_output=True,
    )
    return audio_path


# --- Backend: MLX Whisper ---


def _try_mlx_whisper(audio_path: Path, model: str, language: str | None) -> TranscriptionResult | None:
    """Transcribe using mlx-whisper (Apple Silicon only)."""
    if platform.machine() != "arm64" or platform.system() != "Darwin":
        return None

    try:
        import mlx_whisper
    except ImportError:
        logger.debug("mlx-whisper not installed")
        return None

    logger.info(f"Transcribing with MLX Whisper (model: {model})")
    kwargs = {
        "path_or_hf_repo": f"mlx-community/whisper-{model}-mlx",
        "word_timestamps": True,
    }
    if language:
        kwargs["language"] = language

    result = mlx_whisper.transcribe(str(audio_path), **kwargs)

    segments = []
    has_words = False
    for seg in result.get("segments", []):
        words = None
        if "words" in seg and seg["words"]:
            has_words = True
            words = [
                TranscriptWord(
                    start=w["start"], end=w["end"],
                    word=w["word"], probability=w.get("probability", 1.0),
                )
                for w in seg["words"]
            ]
        segments.append(TranscriptSegment(
            start=seg["start"], end=seg["end"], text=seg["text"], words=words,
        ))

    return TranscriptionResult(
        segments=segments,
        backend="mlx-whisper",
        language=result.get("language"),
        has_word_timestamps=has_words,
    )


# --- Backend: faster-whisper ---


def _try_faster_whisper(audio_path: Path, model: str, language: str | None) -> TranscriptionResult | None:
    """Transcribe using faster-whisper (CTranslate2 backend)."""
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.debug("faster-whisper not installed")
        return None

    logger.info(f"Transcribing with faster-whisper (model: {model})")

    # Use CPU by default; faster-whisper auto-detects CUDA if available
    whisper_model = WhisperModel(model, device="auto", compute_type="auto")

    kwargs = {"word_timestamps": True}
    if language:
        kwargs["language"] = language

    raw_segments, info = whisper_model.transcribe(str(audio_path), **kwargs)

    segments = []
    has_words = False
    for seg in raw_segments:
        words = None
        if hasattr(seg, "words") and seg.words:
            has_words = True
            words = [
                TranscriptWord(
                    start=w.start, end=w.end,
                    word=w.word, probability=w.probability,
                )
                for w in seg.words
            ]
        segments.append(TranscriptSegment(
            start=seg.start, end=seg.end, text=seg.text, words=words,
        ))

    return TranscriptionResult(
        segments=segments,
        backend="faster-whisper",
        language=info.language,
        has_word_timestamps=has_words,
    )


# --- Backend: whisper.cpp CLI ---


def _try_whisper_cpp(audio_path: Path, model: str, language: str | None) -> TranscriptionResult | None:
    """Transcribe using whisper.cpp CLI (whisper-cpp or main binary)."""
    # Look for the binary
    binary = None
    for name in ["whisper-cpp", "whisper", "main"]:
        result = subprocess.run(["which", name], capture_output=True, text=True)
        if result.returncode == 0:
            binary = result.stdout.strip()
            break

    if not binary:
        logger.debug("whisper.cpp binary not found")
        return None

    # Map model names to whisper.cpp model file paths
    # whisper.cpp expects a .bin model file; common install via Homebrew stores them in a standard location
    model_path = None
    for search_path in [
        Path.home() / ".cache" / "whisper" / f"ggml-{model}.bin",
        Path(f"/usr/local/share/whisper/ggml-{model}.bin"),
        Path(f"/opt/homebrew/share/whisper/models/ggml-{model}.bin"),
    ]:
        if search_path.exists():
            model_path = search_path
            break

    if not model_path:
        logger.debug(f"whisper.cpp model file not found for {model}")
        return None

    logger.info(f"Transcribing with whisper.cpp (model: {model})")

    # Output as JSON for structured parsing
    fd, tmp_path = tempfile.mkstemp()
    os.close(fd)
    output_base = Path(tmp_path)

    cmd = [
        binary,
        "-m", str(model_path),
        "-f", str(audio_path),
        "--output-json",
        "-of", str(output_base),
    ]
    if language:
        cmd.extend(["-l", language])

    subprocess.run(cmd, check=True, capture_output=True)

    # Clean up the base temp file (whisper.cpp writes to output_base.json instead)
    output_base.unlink(missing_ok=True)

    json_path = Path(f"{output_base}.json")
    if not json_path.exists():
        return None

    data = json.loads(json_path.read_text())
    json_path.unlink(missing_ok=True)

    segments = []
    for item in data.get("transcription", []):
        segments.append(TranscriptSegment(
            start=_parse_whisper_cpp_time(item["timestamps"]["from"]),
            end=_parse_whisper_cpp_time(item["timestamps"]["to"]),
            text=item["text"],
        ))

    return TranscriptionResult(segments=segments, backend="whisper.cpp")


def _parse_whisper_cpp_time(ts: str) -> float:
    """Parse whisper.cpp timestamp like '00:01:23.456' to seconds."""
    match = re.match(r"(\d+):(\d+):(\d+)\.(\d+)", ts)
    if not match:
        return 0.0
    h, m, s, ms = match.groups()
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000


# --- Backend: macOS SFSpeechRecognizer ---


def _try_macos_speech(audio_path: Path, language: str | None) -> TranscriptionResult | None:
    """Transcribe using macOS built-in speech recognition via a Swift helper."""
    if platform.system() != "Darwin":
        return None

    logger.info("Transcribing with macOS SFSpeechRecognizer")

    # We use a small inline Swift script via `swift` CLI
    locale = language or "en-US"
    swift_code = f"""
import Foundation
import Speech

func log(_ s: String) {{ fputs("[apple-speech] \\(s)\\n", stderr) }}

enum SpeechErr: Error {{ case notAuthorized, noRecognizer, notAvailable, timedOut }}

log("starting; audio=\\"{audio_path}\\" locale={locale}")

let status = await withCheckedContinuation {{ (continuation: CheckedContinuation<SFSpeechRecognizerAuthorizationStatus, Never>) in
    SFSpeechRecognizer.requestAuthorization {{ authStatus in
        continuation.resume(returning: authStatus)
    }}
}}
log("authorization status=\\(status.rawValue)")
guard status == .authorized else {{
    log("ERROR: authorization denied — grant in System Settings → Privacy & Security → Speech Recognition")
    exit(2)
}}

guard let recognizer = SFSpeechRecognizer(locale: Locale(identifier: "{locale}")) else {{
    log("ERROR: no recognizer for locale {locale}")
    exit(3)
}}
guard recognizer.isAvailable else {{
    log("ERROR: recognizer not available for locale {locale} — ensure the locale is downloaded in System Settings → Keyboard → Dictation")
    exit(3)
}}
log("recognizer ready; on-device support=\\(recognizer.supportsOnDeviceRecognition)")

let url = URL(fileURLWithPath: "{audio_path}")
let request = SFSpeechURLRecognitionRequest(url: url)
request.shouldReportPartialResults = false
if #available(macOS 10.15, *), recognizer.supportsOnDeviceRecognition {{
    request.requiresOnDeviceRecognition = true
    log("using on-device recognition")
}} else {{
    log("falling back to network recognition")
}}

func runRecognition() async throws -> [[String: Any]] {{
    try await withCheckedThrowingContinuation {{ (continuation: CheckedContinuation<[[String: Any]], Error>) in
        var resumed = false
        let resumeOnce: (Result<[[String: Any]], Error>) -> Void = {{ outcome in
            if resumed {{ return }}
            resumed = true
            continuation.resume(with: outcome)
        }}

        log("starting recognition task…")
        let task = recognizer.recognitionTask(with: request) {{ result, error in
            if let error = error {{
                log("recognition error: \\(error)")
                resumeOnce(.failure(error))
                return
            }}
            guard let result = result else {{ log("callback with nil result"); return }}
            if result.isFinal {{
                log("final result received (\\(result.bestTranscription.segments.count) segments)")
                var out: [[String: Any]] = []
                for segment in result.bestTranscription.segments {{
                    out.append([
                        "start": segment.timestamp,
                        "end": segment.timestamp + segment.duration,
                        "text": segment.substring
                    ])
                }}
                resumeOnce(.success(out))
            }} else {{
                log("interim result — waiting for final")
            }}
        }}

        DispatchQueue.global().asyncAfter(deadline: .now() + 120) {{
            if !resumed {{
                log("ERROR: timed out after 120s (state=\\(task.state.rawValue))")
                task.cancel()
                resumeOnce(.failure(SpeechErr.timedOut))
            }}
        }}
    }}
}}

do {{
    let resultJSON = try await runRecognition()
    log("done; emitting JSON")
    if let data = try? JSONSerialization.data(withJSONObject: resultJSON),
       let str = String(data: data, encoding: .utf8) {{
        print(str)
    }}
}} catch {{
    log("ERROR: recognition failed: \\(error)")
    exit(4)
}}
"""

    try:
        result = subprocess.run(
            ["swift", "-"],
            input=swift_code,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
        )

        # Surface the Swift helper's diagnostic lines (prefixed [apple-speech])
        # so the server log shows progress regardless of success.
        if result.stderr:
            for line in result.stderr.splitlines():
                if line.strip():
                    logger.info("SFSpeechRecognizer: %s", line)

        if result.returncode != 0:
            logger.warning(f"SFSpeechRecognizer exited rc={result.returncode}: {result.stderr}")
            return None

        data = json.loads(result.stdout.strip())
        segments = [
            TranscriptSegment(start=s["start"], end=s["end"], text=s["text"])
            for s in data
        ]

        return TranscriptionResult(segments=segments, backend="macos-speech", language=locale)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        logger.warning(f"SFSpeechRecognizer failed: {e}")
        return None


# --- Public API ---

# Whisper model sizes: tiny, base, small, medium, large-v3
# Recommended: "large-v3" for best quality, "medium" for speed/quality balance
DEFAULT_MODEL = "large-v3"


def transcribe(
    video_path: str | Path,
    model: str = DEFAULT_MODEL,
    language: str | None = None,
    backend: str | None = None,
) -> TranscriptionResult:
    """Transcribe a video file.

    Args:
        video_path: Path to the video file
        model: Whisper model size (tiny, base, small, medium, large-v3)
        language: Language code (e.g., "en"). None for auto-detect.
        backend: Force a specific backend. None for auto-detect order:
                 mlx-whisper → faster-whisper → whisper.cpp → macos-speech

    Returns:
        TranscriptionResult with segments, timestamps, and text.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Extract audio
    logger.info(f"Extracting audio from {video_path.name}...")
    audio_path = _extract_audio(video_path)

    try:
        # Try backends in order
        backends = [
            ("mlx-whisper", lambda: _try_mlx_whisper(audio_path, model, language)),
            ("faster-whisper", lambda: _try_faster_whisper(audio_path, model, language)),
            ("whisper.cpp", lambda: _try_whisper_cpp(audio_path, model, language)),
            ("macos-speech", lambda: _try_macos_speech(audio_path, language)),
        ]

        if backend:
            # Use specific backend
            backends = [(n, fn) for n, fn in backends if n == backend]
            if not backends:
                raise ValueError(
                    f"Unknown backend: {backend}. "
                    "Available: mlx-whisper, faster-whisper, whisper.cpp, macos-speech"
                )

        for name, try_fn in backends:
            try:
                result = try_fn()
                if result and result.segments:
                    logger.info(
                        f"Transcription complete ({name}): "
                        f"{len(result.segments)} segments, "
                        f"{len(result.text)} characters"
                    )
                    return result
            except Exception as e:
                logger.warning(f"Backend {name} failed: {e}")
                continue

        raise RuntimeError(
            "No transcription backend available. Install one of:\n"
            "  pip install mlx-whisper      # Apple Silicon Mac (recommended)\n"
            "  pip install faster-whisper    # Any platform\n"
            "  brew install whisper-cpp      # macOS via Homebrew\n"
            "Or use macOS built-in speech recognition (limited quality)."
        )
    finally:
        audio_path.unlink(missing_ok=True)


def list_available_backends() -> list[dict]:
    """List which transcription backends are available."""
    available = []

    # MLX Whisper
    try:
        import mlx_whisper
        available.append({"name": "mlx-whisper", "status": "available", "note": "Apple Silicon GPU acceleration"})
    except ImportError:
        if platform.machine() == "arm64" and platform.system() == "Darwin":
            available.append({"name": "mlx-whisper", "status": "installable", "note": "pip install mlx-whisper"})

    # faster-whisper
    try:
        import faster_whisper
        available.append({"name": "faster-whisper", "status": "available", "note": "CTranslate2 backend"})
    except ImportError:
        available.append({"name": "faster-whisper", "status": "installable", "note": "pip install faster-whisper"})

    # whisper.cpp
    for name in ["whisper-cpp", "whisper", "main"]:
        result = subprocess.run(["which", name], capture_output=True, text=True)
        if result.returncode == 0:
            available.append({"name": "whisper.cpp", "status": "available", "note": f"Binary: {result.stdout.strip()}"})
            break
    else:
        available.append({"name": "whisper.cpp", "status": "installable", "note": "brew install whisper-cpp"})

    # macOS Speech
    if platform.system() == "Darwin":
        available.append({"name": "macos-speech", "status": "available", "note": "Built-in, lower quality"})

    return available
