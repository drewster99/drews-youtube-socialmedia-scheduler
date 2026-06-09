"""On-device video transcription.

Attempts backends in order of preference:
1. MLX Whisper — fastest on Apple Silicon Macs
2. whisper.cpp (CLI) — if installed as a system binary
3. macOS SFSpeechRecognizer — built-in, no downloads needed

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

from yt_scheduler.config import UPLOAD_DIR

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


def _macos_speech_timeout_seconds(audio_path: Path) -> int:
    """Pick a Swift-side timeout proportional to the audio length.

    Apple Speech runs at roughly 1× real-time on Apple Silicon, so a
    fixed 120s ceiling kept timing out on multi-minute recordings.
    Formula: ``max(120, ceil(duration * 1.5) + 60)`` — covers a 1.5×
    real-time worst case plus 60s of model load / serialisation, with
    a 120s floor for very short clips and an absolute 1800s cap so a
    bad probe can't hang us forever.
    """
    from yt_scheduler.services.tiers import probe_local_duration

    duration = probe_local_duration(audio_path)
    if duration is None or duration <= 0:
        return 600  # unknown length — fall back to the previous ceiling
    timeout = int(duration * 1.5) + 60
    return max(120, min(1800, timeout))


def _macos_speech_analyzer_swift(audio_path: Path, locale: str) -> str:
    """Swift source for the macOS 26 SpeechAnalyzer/SpeechTranscriber helper.

    Emits a JSON array of ``{start, end, word}`` (word-level timing from the
    ``.audioTimeRange`` attribute) on stdout; diagnostics on stderr.
    """
    return f'''
import Foundation
import Speech
import AVFoundation
import CoreMedia

func log(_ s: String) {{ FileHandle.standardError.write(Data("[apple-speech] \\(s)\\n".utf8)) }}

let audioPath = "{audio_path}"
let localeID = "{locale}"

func ensureModel(_ transcriber: SpeechTranscriber, _ locale: Locale) async throws {{
    let installed = await SpeechTranscriber.installedLocales
    if installed.contains(where: {{ $0.identifier(.bcp47) == locale.identifier(.bcp47) }}) {{ return }}
    let reserved = await AssetInventory.reservedLocales
    if !reserved.contains(where: {{ $0.identifier == locale.identifier }}) {{
        try await AssetInventory.reserve(locale: locale)
    }}
    if let req = try await AssetInventory.assetInstallationRequest(supporting: [transcriber]) {{
        log("downloading speech assets…")
        try await req.downloadAndInstall()
        log("assets installed")
    }}
}}

func run() async throws {{
    let status = await withCheckedContinuation {{ (c: CheckedContinuation<SFSpeechRecognizerAuthorizationStatus, Never>) in
        SFSpeechRecognizer.requestAuthorization {{ c.resume(returning: $0) }}
    }}
    guard status == .authorized else {{ log("ERROR: not authorized (\\(status.rawValue))"); exit(2) }}
    guard SpeechTranscriber.isAvailable else {{ log("ERROR: SpeechTranscriber unavailable"); exit(3) }}
    guard let locale = await SpeechTranscriber.supportedLocale(equivalentTo: Locale(identifier: localeID)) else {{
        log("ERROR: locale \\(localeID) unsupported"); exit(3)
    }}
    log("using locale \\(locale.identifier)")

    let transcriber = SpeechTranscriber(
        locale: locale,
        transcriptionOptions: [],
        reportingOptions: [],
        attributeOptions: [.audioTimeRange]
    )
    try await ensureModel(transcriber, locale)

    let analyzer = SpeechAnalyzer(modules: [transcriber])
    let audioFile = try AVAudioFile(forReading: URL(fileURLWithPath: audioPath))

    let collector = Task {{ () -> [[String: Any]] in
        var words: [[String: Any]] = []
        for try await result in transcriber.results {{
            let attr = result.text
            for run in attr.runs {{
                guard let range = run.audioTimeRange else {{ continue }}
                let word = String(attr[run.range].characters)
                if word.trimmingCharacters(in: .whitespaces).isEmpty {{ continue }}
                words.append(["start": range.start.seconds, "end": range.end.seconds, "word": word])
            }}
        }}
        return words
    }}

    if let last = try await analyzer.analyzeSequence(from: audioFile) {{
        try await analyzer.finalizeAndFinish(through: last)
    }} else {{
        await analyzer.cancelAndFinishNow()
    }}

    let words = try await collector.value
    let out = try JSONSerialization.data(withJSONObject: words)
    FileHandle.standardOutput.write(out)
    log("emitted \\(words.count) words")
}}

try await run()
'''


def _macos_words_to_segments(raw_words: list[dict]) -> list[TranscriptSegment]:
    """Group Apple's word stream into sentence-ish segments, each carrying its
    ``TranscriptWord`` list, so the result has full word-level timing."""
    segments: list[TranscriptSegment] = []
    cur: list[TranscriptWord] = []

    def flush() -> None:
        if not cur:
            return
        text = " ".join(w.word.strip() for w in cur).strip()
        segments.append(TranscriptSegment(
            start=cur[0].start, end=cur[-1].end, text=text, words=list(cur)))
        cur.clear()

    for w in raw_words:
        word = str(w.get("word", ""))
        cur.append(TranscriptWord(
            start=float(w["start"]), end=float(w["end"]),
            word=word, probability=1.0))
        if word.strip().endswith((".", "!", "?")):
            flush()
    flush()
    return segments


def _try_macos_speech(audio_path: Path, language: str | None) -> TranscriptionResult | None:
    """Transcribe with Apple's on-device Speech framework.

    Uses the macOS 26 ``SpeechAnalyzer`` / ``SpeechTranscriber`` API (NOT the
    legacy ``SFSpeechRecognizer``). The modern API transcribes long-form audio in
    one pass — no sliding-window truncation — and exposes per-word timing via the
    ``.audioTimeRange`` attribute, which we surface as ``TranscriptWord`` stamps.
    """
    if platform.system() != "Darwin":
        return None

    locale = language or "en-US"
    logger.info("Transcribing with Apple SpeechAnalyzer (locale=%s)", locale)
    swift_code = _macos_speech_analyzer_swift(audio_path, locale)

    try:
        result = subprocess.run(
            ["swift", "-"],
            input=swift_code,
            capture_output=True,
            text=True,
            timeout=_macos_speech_timeout_seconds(audio_path),
        )

        if result.stderr:
            for line in result.stderr.splitlines():
                if line.strip():
                    logger.info("SpeechAnalyzer: %s", line)

        if result.returncode != 0:
            logger.warning("SpeechAnalyzer exited rc=%s: %s", result.returncode, result.stderr[-400:])
            return None

        raw_words = json.loads(result.stdout.strip() or "[]")
        if not raw_words:
            logger.warning("SpeechAnalyzer returned no words")
            return None

        segments = _macos_words_to_segments(raw_words)
        return TranscriptionResult(
            segments=segments,
            backend="macos-speech",
            language=locale,
            has_word_timestamps=True,
        )
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        logger.warning("SpeechAnalyzer failed: %s", e)
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
                 mlx-whisper → whisper.cpp → macos-speech

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
        # Try backends in order.
        #
        # ``macos-speech`` now uses the macOS 26 ``SpeechAnalyzer`` API, which
        # (unlike the old ``SFSpeechURLRecognitionRequest``) transcribes
        # long-form audio in one pass with per-word timing and runs ~70× faster
        # than realtime. It sits last in the auto-fallback order so the more
        # accurate Whisper backends are preferred when present, but it can also
        # be selected explicitly via ``backend="macos-speech"``.
        backends = [
            ("mlx-whisper", lambda: _try_mlx_whisper(audio_path, model, language)),
            ("whisper.cpp", lambda: _try_whisper_cpp(audio_path, model, language)),
            ("macos-speech", lambda: _try_macos_speech(audio_path, language)),
        ]

        if backend:
            # User explicitly picked a backend — surface its specific failure
            # rather than auto-falling-back (which would hide the real cause).
            backends = [(n, fn) for n, fn in backends if n == backend]
            if not backends:
                raise ValueError(
                    f"Unknown backend: {backend}. "
                    "Available: mlx-whisper, whisper.cpp, macos-speech"
                )
            name, try_fn = backends[0]
            try:
                result = try_fn()
            except Exception as e:
                hint = ""
                if name == "macos-speech":
                    hint = (
                        " — macOS likely killed the helper for privacy. Open "
                        "System Settings → Privacy & Security → Speech "
                        "Recognition and enable access for Drew's Video + "
                        "Socials Scheduler."
                    )
                raise RuntimeError(f"Backend {name} failed: {e}{hint}") from e
            if not (result and result.segments):
                raise RuntimeError(
                    f"Backend {name} returned no segments — the audio may "
                    "have been empty or unintelligible."
                )
            logger.info(
                "Transcription complete (%s): %d segments, %d characters",
                name, len(result.segments), len(result.text),
            )
            return result

        # Auto-pick: first backend that succeeds wins; failures are logged.
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
            "  brew install whisper-cpp      # macOS via Homebrew\n"
            "Or use macOS built-in speech recognition (limited quality)."
        )
    finally:
        audio_path.unlink(missing_ok=True)


def is_model_cached(*, backend: str | None, model: str | None) -> bool | None:
    """Return True if the given (backend, model) is already on disk so a
    transcribe call won't trigger a multi-minute download. Returns False
    when we can confirm it's missing, or None when we don't know how to
    check (non-mlx backends).

    For ``mlx-whisper`` we look for the HuggingFace cache directory
    ``models--mlx-community--whisper-{model}-mlx`` under the standard HF
    cache root (HF_HOME / HUGGINGFACE_HUB_CACHE / default ~/.cache/huggingface/hub).
    """
    if not backend or not model:
        return None
    if backend not in ("mlx-whisper", "mlx_whisper"):
        # whisper.cpp keeps its own ggml-*.bin under ~/.cache/whisper/;
        # macos-speech doesn't download anything; we don't probe these
        # because the UI only surfaces the model picker for MLX today.
        return None

    # HuggingFace caches under one of these env-honored roots, in order.
    import os
    candidates = []
    hf_home = os.environ.get("HF_HOME")
    hf_cache = os.environ.get("HUGGINGFACE_HUB_CACHE")
    if hf_cache:
        candidates.append(Path(hf_cache))
    if hf_home:
        candidates.append(Path(hf_home) / "hub")
    candidates.append(Path.home() / ".cache" / "huggingface" / "hub")

    repo_dir = f"models--mlx-community--whisper-{model}-mlx"
    for root in candidates:
        target = root / repo_dir
        if target.is_dir():
            # The directory exists once the snapshot has been pulled — we
            # don't verify per-file presence (HF marks completion via the
            # snapshots/<rev>/ symlink farm; checking that subdir gives a
            # better signal).
            snapshots = target / "snapshots"
            if snapshots.is_dir() and any(snapshots.iterdir()):
                return True
            return False
    return False


def list_available_backends() -> list[dict]:
    """List which transcription backends are available."""
    import importlib.util

    available = []

    if importlib.util.find_spec("mlx_whisper") is not None:
        available.append({"name": "mlx-whisper", "status": "available", "note": "Apple Silicon GPU acceleration"})
    elif platform.machine() == "arm64" and platform.system() == "Darwin":
        available.append({"name": "mlx-whisper", "status": "installable", "note": "pip install mlx-whisper"})

    # whisper.cpp
    for name in ["whisper-cpp", "whisper", "main"]:
        result = subprocess.run(["which", name], capture_output=True, text=True)
        if result.returncode == 0:
            available.append({"name": "whisper.cpp", "status": "available", "note": f"Binary: {result.stdout.strip()}"})
            break
    else:
        available.append({"name": "whisper.cpp", "status": "installable", "note": "brew install whisper-cpp"})

    # macOS Speech — Apple SpeechAnalyzer (macOS 26+). On-device, word-level
    # timing, long-form in one pass. Surfaced only where the API exists.
    if platform.system() == "Darwin":
        try:
            mac_major = int(platform.mac_ver()[0].split(".")[0])
        except (ValueError, IndexError):
            mac_major = 0
        if mac_major >= 26:
            available.append({"name": "macos-speech", "status": "available",
                              "note": "Apple SpeechAnalyzer (on-device)"})

    return available
