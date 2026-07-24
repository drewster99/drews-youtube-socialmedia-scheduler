"""Regression test for scheduler.restore_pending_auto_actions.

Before the fix, the function had an early ``if not rows: return`` that
exited before the promo-chain resume block. This meant that when a server
restarted with zero pending standard auto-action rows but a promo chain
mid-step, the promo chain would never resume.

The fix removed that early return so the promo-resume path is always reached,
even with zero standard rows.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def restore_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Minimal async DB environment for restore_pending_auto_actions tests."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)

    db_module = importlib.import_module("yt_scheduler.database")
    db_conn = await db_module.get_db()

    projects = importlib.import_module("yt_scheduler.services.projects")
    await projects.ensure_default_project()

    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    yield scheduler, auto_actions, db_conn
    await db_module.close_db()


async def test_promo_resume_reached_with_zero_standard_rows(
    restore_env, monkeypatch
) -> None:
    """With no pending standard auto-action rows, the promo-resume path still
    fires for a promo video whose auto_action_state is mid-step.

    We monkeypatch retry_promo_step so we can assert it was called without
    running the full promo chain (which needs ffmpeg, YouTube credentials, etc.).
    """
    scheduler, auto_actions, db = restore_env

    # Insert a promo video whose chain was interrupted at the 'transcribing' step.
    # The row must be recent enough to fall within the _AUTO_ACTION_RESUME_WINDOW_HOURS
    # window so the query picks it up.
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, auto_action_state, "
        "                    item_type, updated_at) "
        "VALUES ('PROMO001', 1, 'Test Promo', 'uploaded', 'transcribing', "
        "        'hook', datetime('now'))"
    )
    await db.commit()

    # Make sure there are no standard rows that would match the first query
    # (no videos with transcript IS NULL or description_generated_at IS NULL and
    #  auto_action_state IS NULL within the window).  The newly inserted promo
    # video has auto_action_state set, so it's already excluded from the first
    # SELECT.  Insert a fully-complete video to be safe.
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, transcript, "
        "                    description_generated_at, auto_action_state, updated_at) "
        "VALUES ('STANDARD_DONE', 1, 'Done Video', 'published', 'full transcript', "
        "        datetime('now'), NULL, datetime('now'))"
    )
    await db.commit()

    retry_calls: list[tuple[str, str]] = []

    async def _fake_retry_promo_step(video_id: str, step: str) -> None:
        retry_calls.append((video_id, step))

    # Patch retry_promo_step on the auto_actions module (that's where it lives)
    # AND the name the scheduler imports from auto_actions inside the function.
    monkeypatch.setattr(auto_actions, "retry_promo_step", _fake_retry_promo_step)

    # Also patch run_post_create_actions so we don't inadvertently trigger
    # a real chain if a standard row slips through.
    async def _fake_run_post_create_actions(video_id, project_id, source):
        pass

    monkeypatch.setattr(
        auto_actions, "run_post_create_actions", _fake_run_post_create_actions
    )

    # Patch the import inside restore_pending_auto_actions to use our stub.
    # The function does:
    #   from yt_scheduler.services.auto_actions import PROMO_STATE_TRANSCRIBING, retry_promo_step
    # We can't easily intercept a local import inside an async function, but we
    # can ensure the module-level name points to our fake before the call, then
    # patch sys.modules so the re-import picks up the already-patched module.
    sys.modules["yt_scheduler.services.auto_actions"] = auto_actions  # type: ignore[assignment]

    await scheduler.restore_pending_auto_actions()

    assert len(retry_calls) == 1, (
        f"Expected retry_promo_step to be called once for PROMO001; "
        f"got calls: {retry_calls}"
    )
    video_id, step = retry_calls[0]
    assert video_id == "PROMO001"
    # The state was 'transcribing' — a valid PROMO_STEP_ORDER entry, so it passes through.
    assert step == "transcribing"


async def test_promo_resume_not_called_for_failed_or_ready_state(
    restore_env, monkeypatch
) -> None:
    """Promo rows in 'failed:...' or 'ready' state must be excluded from the
    resume query and retry_promo_step must NOT be called for them."""
    scheduler, auto_actions, db = restore_env

    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, auto_action_state, "
        "                    item_type, updated_at) "
        "VALUES ('PROMO_FAILED', 1, 'Failed Promo', 'uploaded', 'failed:transcribing', "
        "        'hook', datetime('now'))"
    )
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, auto_action_state, "
        "                    item_type, updated_at) "
        "VALUES ('PROMO_READY', 1, 'Ready Promo', 'uploaded', 'ready', "
        "        'hook', datetime('now'))"
    )
    await db.commit()

    retry_calls: list[tuple[str, str]] = []

    async def _fake_retry_promo_step(video_id: str, step: str) -> None:
        retry_calls.append((video_id, step))

    monkeypatch.setattr(auto_actions, "retry_promo_step", _fake_retry_promo_step)

    async def _fake_run_post_create_actions(video_id, project_id, source):
        pass

    monkeypatch.setattr(
        auto_actions, "run_post_create_actions", _fake_run_post_create_actions
    )
    sys.modules["yt_scheduler.services.auto_actions"] = auto_actions  # type: ignore[assignment]

    await scheduler.restore_pending_auto_actions()

    called_ids = {vid for vid, _ in retry_calls}
    assert "PROMO_FAILED" not in called_ids, (
        "retry_promo_step was called for a failed: row — it should be excluded"
    )
    assert "PROMO_READY" not in called_ids, (
        "retry_promo_step was called for a ready row — it should be excluded"
    )


async def test_pre_insert_states_normalized_to_transcribing(
    restore_env, monkeypatch
) -> None:
    """States like 'cutting' / 'uploading' / 'probing' / 'generating_title'
    are normalised to 'transcribing' before being passed to retry_promo_step,
    because they represent pre-INSERT states that shouldn't survive in the
    videos table."""
    scheduler, auto_actions, db = restore_env

    for state in ("cutting", "uploading", "probing", "generating_title"):
        vid = f"PROMO_{state.upper()}"
        await db.execute(
            "INSERT INTO videos (id, project_id, title, status, auto_action_state, "
            "                    item_type, updated_at) "
            "VALUES (?, 1, ?, 'uploaded', ?, 'hook', datetime('now'))",
            (vid, f"Promo {state}", state),
        )
    await db.commit()

    retry_calls: list[tuple[str, str]] = []

    async def _fake_retry_promo_step(video_id: str, step: str) -> None:
        retry_calls.append((video_id, step))

    monkeypatch.setattr(auto_actions, "retry_promo_step", _fake_retry_promo_step)

    async def _fake_run_post_create_actions(video_id, project_id, source):
        pass

    monkeypatch.setattr(
        auto_actions, "run_post_create_actions", _fake_run_post_create_actions
    )
    sys.modules["yt_scheduler.services.auto_actions"] = auto_actions  # type: ignore[assignment]

    await scheduler.restore_pending_auto_actions()

    # Every pre-INSERT state must have been normalised to 'transcribing'.
    for _vid, step in retry_calls:
        assert step == "transcribing", (
            f"Expected 'transcribing' but got {step!r} — "
            "pre-INSERT states must be normalised"
        )
    assert len(retry_calls) == 4


async def test_interrupted_description_update_is_settled_not_resumed(
    restore_env, monkeypatch
) -> None:
    """``updating_desc`` is not a chain step, so it must be settled by the
    description-update sweep BEFORE the promo-resume query sees it.

    Without that ordering, restore hands 'updating_desc' to retry_promo_step,
    which rejects the unknown step — and the row stays in a non-terminal state
    forever, unclaimable by either claim helper and with no repair path in the
    UI. It must also be settled regardless of age: the resume window doesn't
    apply, or a row stranded longer than the window stays wedged.
    """
    scheduler, auto_actions, db = restore_env

    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, description, "
        "                    auto_action_state, item_type, updated_at) "
        "VALUES ('DESCUPD0001', 1, 'Interrupted', 'ready', 'Old text', "
        "        'updating_desc', 'hook', datetime('now', '-40 hours'))"
    )
    await db.commit()

    retry_calls: list[tuple[str, str]] = []

    async def _fake_retry_promo_step(video_id: str, step: str) -> None:
        retry_calls.append((video_id, step))

    async def _fake_run_post_create_actions(video_id, project_id, source):
        pass

    monkeypatch.setattr(auto_actions, "retry_promo_step", _fake_retry_promo_step)
    monkeypatch.setattr(
        auto_actions, "run_post_create_actions", _fake_run_post_create_actions
    )
    sys.modules["yt_scheduler.services.auto_actions"] = auto_actions  # type: ignore[assignment]

    await scheduler.restore_pending_auto_actions()

    assert retry_calls == [], (
        "A description update is not a chain step and must never be handed to "
        f"retry_promo_step; got {retry_calls}"
    )
    cursor = await db.execute(
        "SELECT auto_action_state, description FROM videos WHERE id = 'DESCUPD0001'"
    )
    row = await cursor.fetchone()
    assert row["auto_action_state"] == "failed:updating_desc"
    assert row["description"] == "Old text"
