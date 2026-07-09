"""Regression: MLX Whisper is opt-in, and it gives its GPU memory back.

Two failures this pins, both observed in production (2026-06-29 → 2026-07-09):

1. Auto-detect used to try ``mlx-whisper`` first, so the promo chain — which
   passes ``backend=None, model=None`` — silently ran ``large-v3``. Callers that
   substituted ``model or "large-v3"`` made the 3 GB choice invisible.

2. MLX never returns freed Metal buffers to the OS, and ``mlx_whisper`` parks the
   loaded model in a class attribute. A 12-clip batch left 30.3 GB of
   nonvolatile GPU buffers resident, still held ten days later on an idle server.
"""

from __future__ import annotations

import sys
import threading
import types
from pathlib import Path

import pytest

from yt_scheduler.services import transcription


def _fake_result(backend: str) -> transcription.TranscriptionResult:
    return transcription.TranscriptionResult(
        segments=[transcription.TranscriptSegment(start=0.0, end=1.0, text="hi")],
        backend=backend,
    )


@pytest.fixture
def video(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A video path plus a stubbed audio-extract that yields a real temp file."""
    video_path = tmp_path / "clip.mp4"
    video_path.write_bytes(b"\x00" * 16)

    def fake_extract(_video_path: Path) -> Path:
        audio = tmp_path / "audio.wav"
        audio.write_bytes(b"\x00")
        return audio

    monkeypatch.setattr(transcription, "_extract_audio", fake_extract)
    return video_path


# --- 1. MLX is never auto-selected ---------------------------------------


def test_auto_detect_never_reaches_mlx_whisper(video: Path, monkeypatch) -> None:
    """Even with MLX importable and working, auto-detect must not call it."""
    def _boom(*_a, **_k):
        raise AssertionError("auto-detect must never invoke mlx-whisper")

    monkeypatch.setattr(transcription, "_try_mlx_whisper", _boom)
    monkeypatch.setattr(
        transcription, "_try_macos_speech",
        lambda *a, **k: _fake_result("macos-speech"),
    )

    result = transcription.transcribe(video_path=video)
    assert result.backend == "macos-speech"


def test_mlx_absent_from_auto_order() -> None:
    assert "mlx-whisper" not in transcription._AUTO_BACKEND_ORDER
    assert transcription._AUTO_BACKEND_ORDER[0] == "macos-speech"
    # …but still reachable by name.
    assert "mlx-whisper" in transcription._ALL_BACKENDS


def test_macos_speech_takes_no_model() -> None:
    """``POST /transcribe`` echoes ``model`` only for backends that use one — the
    UI posts its dropdown value even when Apple Speech will ignore it.
    """
    assert "macos-speech" not in transcription.WHISPER_MODEL_BACKENDS
    assert set(transcription.WHISPER_MODEL_BACKENDS) == {"mlx-whisper", "whisper.cpp"}


def test_explicit_mlx_whisper_still_runs(video: Path, monkeypatch) -> None:
    """Opt-in must keep working — the backend is retired from auto, not removed."""
    calls: list[str] = []

    def fake_mlx(_audio, model, _language):
        calls.append(model)
        return _fake_result("mlx-whisper")

    monkeypatch.setattr(transcription, "_try_mlx_whisper", fake_mlx)
    result = transcription.transcribe(
        video_path=video, backend="mlx-whisper", model="large-v3",
    )
    assert result.backend == "mlx-whisper"
    assert calls == ["large-v3"]


# --- 2. No silent model default ------------------------------------------


@pytest.mark.parametrize("backend", ["mlx-whisper", "whisper.cpp"])
def test_whisper_backend_without_model_raises(video: Path, backend: str) -> None:
    with pytest.raises(ValueError, match="requires an explicit model"):
        transcription.transcribe(video_path=video, backend=backend)


def test_unknown_backend_raises(video: Path) -> None:
    with pytest.raises(ValueError, match="Unknown backend"):
        transcription.transcribe(video_path=video, backend="faster-whisper")


def test_auto_detect_skips_whisper_cpp_when_no_model(video: Path, monkeypatch) -> None:
    """No model => whisper.cpp is skipped, not handed a default."""
    def _boom(*_a, **_k):
        raise AssertionError("whisper.cpp must not run without an explicit model")

    monkeypatch.setattr(transcription, "_try_whisper_cpp", _boom)
    monkeypatch.setattr(transcription, "_try_macos_speech", lambda *a, **k: None)
    monkeypatch.setattr(transcription, "_try_mlx_whisper", _boom)

    with pytest.raises(RuntimeError, match="no model specified"):
        transcription.transcribe(video_path=video)


# --- 3. MLX hands its memory back ----------------------------------------


class _FakeMlxWhisper(types.ModuleType):
    def __init__(self, *, raises: bool = False) -> None:
        super().__init__("mlx_whisper")
        self._raises = raises

    def transcribe(self, _path: str, **_kwargs) -> dict:
        if self._raises:
            raise RuntimeError("inference blew up")
        return {"segments": []}


@pytest.fixture
def mlx_memory_spies(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count the two memory-hygiene hooks and neutralise the real idle timer."""
    counts = {"trim": 0, "arm": 0}
    monkeypatch.setattr(transcription.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(transcription.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(
        transcription, "_mlx_trim_buffer_cache",
        lambda: counts.__setitem__("trim", counts["trim"] + 1),
    )
    monkeypatch.setattr(
        transcription, "_arm_mlx_idle_release",
        lambda: counts.__setitem__("arm", counts["arm"] + 1),
    )
    return counts


def test_mlx_run_trims_cache_and_arms_idle_release(
    tmp_path: Path, monkeypatch, mlx_memory_spies,
) -> None:
    monkeypatch.setitem(sys.modules, "mlx_whisper", _FakeMlxWhisper())
    transcription._try_mlx_whisper(tmp_path / "a.wav", "large-v3", "en")
    assert mlx_memory_spies == {"trim": 1, "arm": 1}


def test_mlx_failure_still_frees_memory(
    tmp_path: Path, monkeypatch, mlx_memory_spies,
) -> None:
    """A crashed run leaves buffers behind too — the cleanup must not be skipped."""
    monkeypatch.setitem(sys.modules, "mlx_whisper", _FakeMlxWhisper(raises=True))
    with pytest.raises(RuntimeError, match="inference blew up"):
        transcription._try_mlx_whisper(tmp_path / "a.wav", "large-v3", "en")
    assert mlx_memory_spies == {"trim": 1, "arm": 1}


def test_mlx_cleanup_never_holds_the_inference_lock(
    tmp_path: Path, monkeypatch, mlx_memory_spies,
) -> None:
    """The lock must be free once a run returns, or the idle release deadlocks."""
    monkeypatch.setitem(sys.modules, "mlx_whisper", _FakeMlxWhisper())
    transcription._try_mlx_whisper(tmp_path / "a.wav", "large-v3", "en")
    acquired = transcription._mlx_whisper_inference_lock.acquire(blocking=False)
    assert acquired, "_mlx_whisper_inference_lock was left held"
    transcription._mlx_whisper_inference_lock.release()


@pytest.fixture
def fake_mlx_runtime(monkeypatch: pytest.MonkeyPatch):
    """Stand in for ``mlx.core`` + ``mlx_whisper.transcribe.ModelHolder``.

    Without this the real ``_release_mlx_memory`` body dies on ``import mlx.core``
    wherever MLX isn't installed (CI, Linux), and any assertion about model
    dropping would pass vacuously.
    """
    class _Holder:
        model = object()
        model_path = "mlx-community/whisper-large-v3-mlx"

    core = types.SimpleNamespace(
        cleared=0,
        clear_cache=lambda: setattr(core, "cleared", core.cleared + 1),
        get_active_memory=lambda: 0,
        get_cache_memory=lambda: 0,
        set_cache_limit=lambda _n: 0,
    )
    mlx_pkg = types.ModuleType("mlx")
    mlx_pkg.core = core
    whisper_transcribe = types.ModuleType("mlx_whisper.transcribe")
    whisper_transcribe.ModelHolder = _Holder
    whisper_pkg = types.ModuleType("mlx_whisper")
    whisper_pkg.transcribe = whisper_transcribe

    monkeypatch.setitem(sys.modules, "mlx", mlx_pkg)
    monkeypatch.setitem(sys.modules, "mlx.core", core)
    monkeypatch.setitem(sys.modules, "mlx_whisper", whisper_pkg)
    monkeypatch.setitem(sys.modules, "mlx_whisper.transcribe", whisper_transcribe)
    return types.SimpleNamespace(core=core, holder=_Holder)


def test_idle_release_drops_the_model_and_clears_the_cache(fake_mlx_runtime) -> None:
    transcription._release_mlx_memory()
    assert fake_mlx_runtime.holder.model is None
    assert fake_mlx_runtime.holder.model_path is None
    assert fake_mlx_runtime.core.cleared == 1


def test_idle_release_skips_while_inference_is_running(fake_mlx_runtime) -> None:
    """Timer must not tear the model out from under a live run — it defers."""
    transcription._mlx_whisper_inference_lock.acquire()
    try:
        transcription._release_mlx_memory()  # must return immediately, doing nothing
    finally:
        transcription._mlx_whisper_inference_lock.release()

    assert fake_mlx_runtime.holder.model is not None, "weights dropped mid-inference"
    assert fake_mlx_runtime.core.cleared == 0


def test_arm_idle_release_cancels_the_previous_timer(monkeypatch) -> None:
    """Each run re-arms; stale timers must not pile up or fire early."""
    monkeypatch.setattr(transcription, "_MLX_IDLE_RELEASE_SECONDS", 3600.0)
    monkeypatch.setattr(transcription, "_release_mlx_memory", lambda: None)
    try:
        transcription._arm_mlx_idle_release()
        first = transcription._mlx_release_timer
        transcription._arm_mlx_idle_release()
        second = transcription._mlx_release_timer

        assert first is not second
        assert isinstance(second, threading.Timer)
        assert second.daemon, "idle timer must not block server shutdown"
        # cancel() sets `finished` synchronously; is_alive() would race the thread.
        assert first.finished.is_set(), "previous idle timer was not cancelled"
    finally:
        if transcription._mlx_release_timer is not None:
            transcription._mlx_release_timer.cancel()
