"""Render checkpoint — a retry reuses an already-rendered slot instead of re-paying.

Accept renders each enabled slot one at a time, and each render can fire a paid
Anthropic ``{{ai:}}`` round-trip. A transient failure on a later slot used to
discard every earlier success, so the retry re-rendered — and re-paid for —
slots that had already rendered fine. ``_render_slot`` now checkpoints each
success keyed by a hash of every input, so a retry reuses it — but ONLY when
nothing that feeds the render has changed, so an edit is never silently dropped.

The paid render is ``templates.async_render``; every test monkeypatches it with
a counter so "did we re-pay?" is exactly "was async_render called again?".
"""

from __future__ import annotations

import importlib


async def _insert_template_with_slots(db, bodies: list[str]) -> tuple[int, list[int]]:
    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'clips', '[\"hook\"]')"
    )
    template_id = int(cursor.lastrowid)
    slot_ids: list[int] = []
    for body in bodies:
        slot_cursor = await db.execute(
            "INSERT INTO template_slots (template_id, platform, body, media, max_chars) "
            "VALUES (?, 'bluesky', ?, 'none', 300)",
            (template_id, body),
        )
        slot_ids.append(int(slot_cursor.lastrowid))
    await db.commit()
    return template_id, slot_ids


async def _insert_video(
    db, video_id: str = "v1", title: str = "First clip",
    video_file_path: str = "/tmp/does-not-exist.mp4",
) -> dict:
    # tier is set so build_render_context doesn't fall through to a live
    # YouTube duration lookup; every other column mirrors the accept fixture.
    await db.execute(
        "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
        "privacy_status, width, height, url, video_file_path, tier) "
        "VALUES (?, 1, ?, 'hook', 60, 'public', 1080, 1920, 'https://y/x', ?, 'short')",
        (video_id, title, video_file_path),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    return dict(rows[0])


def _slot(slot_id: int, body: str) -> dict:
    return {
        "id": slot_id, "platform": "bluesky", "body": body,
        "is_disabled": 0, "social_account_id": None, "max_chars": 300,
    }


def _counting_render(calls: list[str]):
    async def fake_async_render(cleaned_body, variables=None, *,
                                default_system_prompt=None, **_kwargs):
        calls.append(cleaned_body)
        title = (variables or {}).get("title", "")
        return f"OK::{cleaned_body}::{title}"
    return fake_async_render


async def test_same_inputs_reuse_the_checkpoint(isolated_db, monkeypatch):
    """Second render of an unchanged slot is served from the checkpoint."""
    db = isolated_db
    tmpl = importlib.import_module("yt_scheduler.services.templates")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")

    calls: list[str] = []
    monkeypatch.setattr(tmpl, "async_render", _counting_render(calls))

    _tid, slot_ids = await _insert_template_with_slots(db, ["Watch: {{title}}"])
    video = await _insert_video(db)
    slot = _slot(slot_ids[0], "Watch: {{title}}")

    text1, media1 = await accept._render_slot(db, video, slot, default_ai_system=None)
    text2, media2 = await accept._render_slot(db, video, slot, default_ai_system=None)

    # The mocked async_render echoes the media-stripped body (real variable
    # substitution is exactly what we replaced), so {{title}} stays literal.
    assert text1 == text2 == "OK::Watch: {{title}}::First clip"
    assert media1 == media2 == []
    assert len(calls) == 1  # the second render never paid for a round-trip

    rows = await db.execute_fetchall("SELECT * FROM render_checkpoint")
    assert len(rows) == 1
    assert rows[0]["video_id"] == "v1"
    assert int(rows[0]["slot_id"]) == slot_ids[0]


async def test_body_edit_is_not_reused(isolated_db, monkeypatch):
    """Editing the slot body changes the hash — the stale render must not ship."""
    db = isolated_db
    tmpl = importlib.import_module("yt_scheduler.services.templates")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")

    calls: list[str] = []
    monkeypatch.setattr(tmpl, "async_render", _counting_render(calls))

    _tid, slot_ids = await _insert_template_with_slots(db, ["Watch: {{title}}"])
    video = await _insert_video(db)

    await accept._render_slot(
        db, video, _slot(slot_ids[0], "Watch: {{title}}"), default_ai_system=None
    )
    text2, _media = await accept._render_slot(
        db, video, _slot(slot_ids[0], "Now watch: {{title}}"), default_ai_system=None
    )

    assert len(calls) == 2  # the edited body re-rendered rather than reusing
    assert text2 == "OK::Now watch: {{title}}::First clip"
    # One row per (video, slot): the re-render replaced the fingerprint.
    rows = await db.execute_fetchall("SELECT * FROM render_checkpoint")
    assert len(rows) == 1


async def test_variable_change_is_not_reused(isolated_db, monkeypatch):
    """A changed context variable (here the title) invalidates the checkpoint."""
    db = isolated_db
    tmpl = importlib.import_module("yt_scheduler.services.templates")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")

    calls: list[str] = []
    monkeypatch.setattr(tmpl, "async_render", _counting_render(calls))

    _tid, slot_ids = await _insert_template_with_slots(db, ["Watch: {{title}}"])
    video = await _insert_video(db, title="Alpha")
    slot = _slot(slot_ids[0], "Watch: {{title}}")

    await accept._render_slot(db, video, slot, default_ai_system=None)
    # Same video id and slot id, but a different resolved variable set.
    await accept._render_slot(
        db, {**video, "title": "Beta"}, slot, default_ai_system=None
    )

    assert len(calls) == 2


async def test_system_prompt_change_is_not_reused(isolated_db, monkeypatch):
    """A different default AI system prompt invalidates the checkpoint."""
    db = isolated_db
    tmpl = importlib.import_module("yt_scheduler.services.templates")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")

    calls: list[str] = []
    monkeypatch.setattr(tmpl, "async_render", _counting_render(calls))

    _tid, slot_ids = await _insert_template_with_slots(db, ["Watch: {{title}}"])
    video = await _insert_video(db)
    slot = _slot(slot_ids[0], "Watch: {{title}}")

    await accept._render_slot(db, video, slot, default_ai_system="Be terse.")
    await accept._render_slot(db, video, slot, default_ai_system="Be verbose.")

    assert len(calls) == 2


async def test_missing_media_forces_a_rerender(isolated_db, monkeypatch, tmp_path):
    """A checkpoint whose media file was cleaned up must miss, not hand back a dangling path."""
    db = isolated_db
    tmpl = importlib.import_module("yt_scheduler.services.templates")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")

    calls: list[str] = []
    monkeypatch.setattr(tmpl, "async_render", _counting_render(calls))

    media_file = tmp_path / "clip.mp4"
    media_file.write_bytes(b"data")

    _tid, slot_ids = await _insert_template_with_slots(db, ["clip {{video}}"])
    video = await _insert_video(db, video_file_path=str(media_file))
    slot = _slot(slot_ids[0], "clip {{video}}")

    _t1, media1 = await accept._render_slot(db, video, slot, default_ai_system=None)
    assert media1 == [str(media_file)]
    assert len(calls) == 1

    # File still present → reused.
    await accept._render_slot(db, video, slot, default_ai_system=None)
    assert len(calls) == 1

    # File gone → the cached render is not eligible, re-render.
    media_file.unlink()
    await accept._render_slot(db, video, slot, default_ai_system=None)
    assert len(calls) == 2


async def test_transient_failure_then_retry_rerenders_only_the_failed_slot(
    isolated_db, monkeypatch
):
    """The core defect: a mid-batch transient failure discards earlier successes.

    Slot 1 renders, slot 2 fails transiently and abandons the whole video. On
    retry, slot 1 must come from the checkpoint (no second round-trip) and only
    slot 2 re-renders.
    """
    db = isolated_db
    tmpl = importlib.import_module("yt_scheduler.services.templates")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")

    _tid, slot_ids = await _insert_template_with_slots(
        db, ["Slot1 {{title}}", "SLOT2 {{title}}"]
    )
    video = await _insert_video(db)
    slots = [_slot(slot_ids[0], "Slot1 {{title}}"), _slot(slot_ids[1], "SLOT2 {{title}}")]

    calls: list[str] = []
    fail_slot2_once = {"pending": True}

    async def fake_async_render(cleaned_body, variables=None, *,
                                default_system_prompt=None, **_kwargs):
        calls.append(cleaned_body)
        if cleaned_body.startswith("SLOT2") and fail_slot2_once["pending"]:
            fail_slot2_once["pending"] = False
            raise RuntimeError("Anthropic overloaded")  # transient, not a template fault
        return f"OK::{cleaned_body}"

    monkeypatch.setattr(tmpl, "async_render", fake_async_render)

    plan1 = await accept._plan_video(db, video, slots, default_ai_system=None)
    assert plan1.transient_error is not None
    assert plan1.posts == []
    # Slot 1's success was checkpointed even though the video was abandoned.
    checkpoint_rows = await db.execute_fetchall("SELECT slot_id FROM render_checkpoint")
    assert [int(r["slot_id"]) for r in checkpoint_rows] == [slot_ids[0]]

    plan2 = await accept._plan_video(db, video, slots, default_ai_system=None)
    assert plan2.transient_error is None
    assert len(plan2.posts) == 2

    slot1_renders = [c for c in calls if c.startswith("Slot1")]
    slot2_renders = [c for c in calls if c.startswith("SLOT2")]
    assert len(slot1_renders) == 1  # rendered once, reused on retry — not re-paid
    assert len(slot2_renders) == 2  # failed, then rendered on retry
