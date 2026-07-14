"""The transcribe-then-describe background chain.

Started when the user asks for a description on a video with no transcript.
It must:

  * transcribe, then generate the description FROM that transcript — never
    silently substitute the keyframe describer, which routes through a
    different prompt and can drop whatever a project's transcript prompt
    requires;
  * fail loudly and stop when transcription yields no usable speech;
  * persist its progress so the user can navigate away and come back.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.asyncio


async def _boot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    keychain.store_secret("anthropic", "api_key", "sk-ant-test-fake")
    importlib.import_module("yt_scheduler.app")

    # Resolve modules lazily, AFTER the sys.modules purge above — grabbing them
    # at import time would patch dead module objects (see CLAUDE.md).
    return {
        "auto_actions": importlib.import_module("yt_scheduler.services.auto_actions"),
        "ai": importlib.import_module("yt_scheduler.services.ai"),
        "database": importlib.import_module("yt_scheduler.database"),
    }


async def _insert_video(db, video_id: str, video_path: Path) -> None:
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, video_file_path, transcript) "
        "VALUES (?, 1, 'Test', 'uploaded', ?, '')",
        (video_id, str(video_path)),
    )
    await db.commit()


async def _state_of(db, video_id: str) -> dict:
    rows = await db.execute_fetchall(
        "SELECT auto_action_state, auto_action_last_error, "
        "auto_action_progress_message, generated_description "
        "FROM videos WHERE id = ?",
        (video_id,),
    )
    return dict(rows[0])


async def test_chain_transcribes_then_describes_from_the_transcript(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, ai, database = mods["auto_actions"], mods["ai"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidA", video_file)

    SRT = "1\n00:00:00,000 --> 00:00:05,000\nHello there, this is real speech.\n"

    async def fake_transcribe_and_store(video_id, path, *, backend, model,
                                        progress_callback=None):
        if progress_callback:
            progress_callback(5.0, 10.0)
        await db.execute(
            "UPDATE videos SET transcript = ? WHERE id = ?", (SRT, video_id)
        )
        await db.commit()
        return SRT

    monkeypatch.setattr(auto_actions, "transcribe_and_store", fake_transcribe_and_store)

    seen: dict[str, object] = {}

    async def fake_description(*, title, transcript, extra_instructions="",
                               project_id, prompt_variables=None, is_promo=False,
                               channel_name=""):
        seen["transcript"] = transcript
        return "A description written from the transcript."

    monkeypatch.setattr(ai, "generate_seo_description", fake_description)

    # Run the chain body directly — spawn_background would detach it and the
    # test would race the assertions.
    await auto_actions._run_transcribe_then_describe("vidA", 1, "")

    row = await _state_of(db, "vidA")
    assert row["auto_action_state"] == "ready"
    assert row["auto_action_last_error"] is None
    # Progress line is cleared on completion so no stale "42%" lingers.
    assert row["auto_action_progress_message"] is None
    assert row["generated_description"] == "A description written from the transcript."
    # The description came from the transcript we just made, not from keyframes.
    assert seen["transcript"] == SRT.strip()

    await database.close_db()


async def test_chain_fails_loudly_when_there_is_no_speech(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A silent/music-only video must NOT quietly fall through to keyframes."""
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, ai, database = mods["auto_actions"], mods["ai"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidB", video_file)

    async def fake_transcribe_and_store(video_id, path, *, backend, model,
                                        progress_callback=None):
        return ""  # transcriber ran, heard nothing

    monkeypatch.setattr(auto_actions, "transcribe_and_store", fake_transcribe_and_store)

    def _explode(*a, **kw):
        raise AssertionError("keyframe describer must not run as a silent fallback")

    monkeypatch.setattr(ai, "generate_seo_description_from_frames", _explode)
    monkeypatch.setattr(ai, "generate_seo_description", _explode)

    await auto_actions._run_transcribe_then_describe("vidB", 1, "")

    row = await _state_of(db, "vidB")
    assert row["auto_action_state"] == "failed:transcribing"
    assert "no usable speech" in (row["auto_action_last_error"] or "")
    assert row["generated_description"] in (None, "")

    await database.close_db()


async def test_chain_surfaces_a_transcription_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, ai, database = mods["auto_actions"], mods["ai"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidC", video_file)

    async def boom(video_id, path, *, backend, model, progress_callback=None):
        raise RuntimeError("Speech Recognition permission denied")

    monkeypatch.setattr(auto_actions, "transcribe_and_store", boom)
    monkeypatch.setattr(
        ai, "generate_seo_description",
        lambda **kw: (_ for _ in ()).throw(
            AssertionError("must not describe after a failed transcription")
        ),
    )

    await auto_actions._run_transcribe_then_describe("vidC", 1, "")

    row = await _state_of(db, "vidC")
    assert row["auto_action_state"] == "failed:transcribing"
    assert "Speech Recognition permission denied" in (row["auto_action_last_error"] or "")
    assert row["auto_action_progress_message"] is None

    await database.close_db()


async def test_only_one_chain_can_claim_a_video(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """spawn_background does not dedup by name, so the claim is the only thing
    stopping two tabs from running the same on-device transcription twice."""
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, database = mods["auto_actions"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidD", video_file)

    assert await auto_actions.claim_describe_chain("vidD") is True
    # Second caller arrives while the first is mid-transcription.
    assert await auto_actions.claim_describe_chain("vidD") is False

    row = await _state_of(db, "vidD")
    assert row["auto_action_state"] == "transcribing"

    await database.close_db()


async def test_claim_refuses_to_trample_an_in_flight_promo_chain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Every non-terminal state is off-limits — clicking Generate on a video the
    Promo chain is mid-way through must not hijack its state machine."""
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, database = mods["auto_actions"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidE", video_file)
    await db.execute(
        "UPDATE videos SET auto_action_state = 'uploading' WHERE id = 'vidE'"
    )
    await db.commit()

    assert await auto_actions.claim_describe_chain("vidE") is False
    row = await _state_of(db, "vidE")
    assert row["auto_action_state"] == "uploading"  # untouched

    await database.close_db()


async def test_claim_is_available_again_after_a_terminal_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A previous run that finished (or failed) must not lock the video out."""
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, database = mods["auto_actions"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidF", video_file)

    for terminal in ("ready", "failed:transcribing", "failed:generating_desc"):
        await db.execute(
            "UPDATE videos SET auto_action_state = ? WHERE id = 'vidF'", (terminal,)
        )
        await db.commit()
        assert await auto_actions.claim_describe_chain("vidF") is True, terminal

    await database.close_db()


async def test_unexpected_chain_failure_still_lands_in_a_failed_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The 202 already went out and the row already says 'transcribing'. If the
    chain dies before its own handlers (bad project settings, SQLite blip), the
    row must not sit spinning forever with the error only in the log."""
    mods = await _boot(monkeypatch, tmp_path)
    auto_actions, database = mods["auto_actions"], mods["database"]

    video_file = tmp_path / "uploads" / "v.mp4"
    video_file.write_bytes(b"\x00" * 32)

    db = await database.get_db()
    await _insert_video(db, "vidG", video_file)

    async def boom(project_id):
        raise RuntimeError("project settings are corrupt")

    monkeypatch.setattr(auto_actions, "get_project_by_id", boom)

    await auto_actions._run_transcribe_then_describe("vidG", 1, "")

    row = await _state_of(db, "vidG")
    assert row["auto_action_state"] == "failed:transcribing"
    assert "project settings are corrupt" in (row["auto_action_last_error"] or "")

    await database.close_db()
