"""Regression: MLX-Whisper inference must be serialized.

Whisper's word-timestamp alignment is JIT-compiled by numba, whose default
"workqueue" threading layer is not threadsafe — concurrent entry from two
threads makes numba abort() the whole process (SIGABRT). A multi-clip
"insert all" ran several promo-chain transcriptions on parallel to_thread
workers and crashed the server mid-batch (2026-06-12). These tests pin the
fix: ``_mlx_whisper_inference_lock`` must keep at most ONE thread inside
``mlx_whisper.transcribe`` at a time. A fake mlx_whisper module stands in,
so no model download, GPU, or Apple Silicon is needed.
"""

from __future__ import annotations

import sys
import threading
import time
import types
from pathlib import Path

from yt_scheduler.services import transcription


class _FakeMlxWhisper(types.ModuleType):
    """Counts how many threads are inside transcribe() simultaneously."""

    def __init__(self) -> None:
        super().__init__("mlx_whisper")
        self._counter_lock = threading.Lock()  # guards the counters only
        self._active = 0
        self.max_active = 0

    def transcribe(self, path: str, **kwargs) -> dict:
        with self._counter_lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        # Hold the "inside inference" window open long enough that an
        # unserialized second caller would overlap and raise max_active.
        time.sleep(0.02)
        with self._counter_lock:
            self._active -= 1
        return {"segments": []}


def test_mlx_whisper_inference_is_serialized(monkeypatch, tmp_path: Path) -> None:
    fake = _FakeMlxWhisper()
    monkeypatch.setitem(sys.modules, "mlx_whisper", fake)
    # _try_mlx_whisper early-returns off Apple Silicon; pin the platform
    # probes so the lock path is exercised on any machine (incl. CI).
    monkeypatch.setattr(transcription.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(transcription.platform, "system", lambda: "Darwin")

    audio_path = tmp_path / "audio.wav"  # never opened — the fake ignores it
    results: list[object] = []
    results_lock = threading.Lock()

    def _worker() -> None:
        result = transcription._try_mlx_whisper(audio_path, "large-v3", "en")
        with results_lock:
            results.append(result)

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert all(not t.is_alive() for t in threads), "transcription deadlocked"
    # Every call must have reached the fake (returned a result), or the
    # max_active assertion below would vacuously pass on an early return.
    assert len(results) == 8 and all(r is not None for r in results)
    assert fake.max_active == 1, (
        f"mlx_whisper.transcribe entered by {fake.max_active} threads at once; "
        "_mlx_whisper_inference_lock is not serializing inference"
    )
