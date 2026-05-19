"""Tests for the Promo cascade rules added in commit 4.

The cascade helpers reschedule auto-anchored siblings / children
when a user reschedules a video. Manual-override rows
(``publish_at_manual = 1``) and ``status = 'published'`` rows are
left in place.

We stub :func:`schedule_publish` so the tests don't try to register
APScheduler jobs or touch live YouTube credentials — the cascade
math is the only thing under test.
"""

from __future__ import annotations

import importlib
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    app_module = importlib.import_module("yt_scheduler.app")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    calls: list[tuple[str, datetime]] = []

    async def fake_schedule_publish(video_id, publish_at):
        # Mirror the column writes schedule_publish does so cascade
        # helpers see the new publish_at on the next SELECT.
        from yt_scheduler.config import DB_PATH

        if publish_at.tzinfo is None:
            publish_at = publish_at.replace(tzinfo=timezone.utc)
        with sqlite3.connect(str(DB_PATH)) as conn:
            conn.execute(
                "UPDATE videos SET publish_at = ?, status = 'scheduled', "
                "updated_at = datetime('now') WHERE id = ?",
                (publish_at.isoformat(), video_id),
            )
            conn.commit()
        calls.append((video_id, publish_at))
        return f"publish_{video_id}"

    monkeypatch.setattr(scheduler, "schedule_publish", fake_schedule_publish)

    with TestClient(app_module.app) as c:
        yield {
            "client": c,
            "scheduler": scheduler,
            "calls": calls,
        }


def _insert(
    video_id: str,
    *,
    parent_item_id: str | None = None,
    item_type: str = "episode",
    publish_at: str | None = None,
    publish_at_manual: int = 0,
    status: str = "uploaded",
    title: str = "Test",
) -> None:
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            """INSERT INTO videos (id, project_id, title, description, tags,
               privacy_status, status, item_type, parent_item_id,
               publish_at, publish_at_manual, transcript, thumbnail_path,
               thumbnail_source, url)
            VALUES (?, 1, ?, 'desc', '["a","b","c"]', 'unlisted', ?, ?, ?,
                    ?, ?, 'words', '/tmp/x.jpg', null, ?)""",
            (
                video_id, title, status, item_type, parent_item_id,
                publish_at, publish_at_manual,
                f"https://youtu.be/{video_id}",
            ),
        )
        conn.commit()


def _read_publish_at(video_id: str) -> str | None:
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        row = conn.execute(
            "SELECT publish_at FROM videos WHERE id = ?", (video_id,)
        ).fetchone()
    return row[0] if row else None


@pytest.mark.asyncio
async def test_parent_shift_moves_auto_children(env) -> None:
    """Parent moves +1 day → auto children move +1 day too."""
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid001", publish_at=base.isoformat())
    _insert(
        "shortauto1", parent_item_id="parentid001", item_type="short",
        publish_at=(base + timedelta(hours=18)).isoformat(),
        publish_at_manual=0,
    )
    _insert(
        "hookauto01", parent_item_id="parentid001", item_type="hook",
        publish_at=(base + timedelta(hours=4)).isoformat(),
        publish_at_manual=0,
    )

    cascade = env["scheduler"].cascade_children_on_parent_shift
    shifted = await cascade("parentid001", base, base + timedelta(days=1))

    assert set(shifted) == {"shortauto1", "hookauto01"}
    short_new = datetime.fromisoformat(_read_publish_at("shortauto1"))
    hook_new = datetime.fromisoformat(_read_publish_at("hookauto01"))
    assert short_new == base + timedelta(days=1, hours=18)
    assert hook_new == base + timedelta(days=1, hours=4)


@pytest.mark.asyncio
async def test_manual_children_stay_put_on_parent_shift(env) -> None:
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid002", publish_at=base.isoformat())
    _insert(
        "manualchld", parent_item_id="parentid002", item_type="short",
        publish_at=(base + timedelta(hours=18)).isoformat(),
        publish_at_manual=1,
    )

    cascade = env["scheduler"].cascade_children_on_parent_shift
    shifted = await cascade("parentid002", base, base + timedelta(days=2))

    assert shifted == []
    assert _read_publish_at("manualchld") == (
        base + timedelta(hours=18)
    ).isoformat()


@pytest.mark.asyncio
async def test_published_children_skipped(env) -> None:
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid003", publish_at=base.isoformat())
    _insert(
        "publishedch", parent_item_id="parentid003", item_type="short",
        publish_at=(base + timedelta(hours=18)).isoformat(),
        publish_at_manual=0,
        status="published",
    )

    shifted = await env["scheduler"].cascade_children_on_parent_shift(
        "parentid003", base, base + timedelta(days=2),
    )
    assert shifted == []


@pytest.mark.asyncio
async def test_sibling_shift_moves_later_same_tier_siblings(env) -> None:
    """Hook #2 in chain moves +14h → only hooks AFTER it move too."""
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid004", publish_at=base.isoformat())
    _insert(
        "hookchain1", parent_item_id="parentid004", item_type="hook",
        publish_at=(base + timedelta(hours=4)).isoformat(),
    )
    _insert(
        "hookchain2", parent_item_id="parentid004", item_type="hook",
        publish_at=(base + timedelta(hours=4 + 99)).isoformat(),
    )
    _insert(
        "hookchain3", parent_item_id="parentid004", item_type="hook",
        publish_at=(base + timedelta(hours=4 + 198)).isoformat(),
    )
    _insert(
        "hookchain4", parent_item_id="parentid004", item_type="hook",
        publish_at=(base + timedelta(hours=4 + 297)).isoformat(),
    )

    sibling = env["scheduler"].cascade_siblings_on_shift
    delta = timedelta(hours=14)
    old_h2 = base + timedelta(hours=4 + 99)
    shifted = await sibling("hookchain2", old_h2, old_h2 + delta)

    assert set(shifted) == {"hookchain3", "hookchain4"}
    h3_new = datetime.fromisoformat(_read_publish_at("hookchain3"))
    h4_new = datetime.fromisoformat(_read_publish_at("hookchain4"))
    assert h3_new == base + timedelta(hours=4 + 198) + delta
    assert h4_new == base + timedelta(hours=4 + 297) + delta
    # H1 (before H2) untouched.
    assert (
        datetime.fromisoformat(_read_publish_at("hookchain1"))
        == base + timedelta(hours=4)
    )


@pytest.mark.asyncio
async def test_sibling_cascade_skips_manual_override(env) -> None:
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid005", publish_at=base.isoformat())
    _insert(
        "hookcurrnt", parent_item_id="parentid005", item_type="hook",
        publish_at=(base + timedelta(hours=4)).isoformat(),
    )
    manual_time = (base + timedelta(hours=100)).isoformat()
    _insert(
        "hookmanual", parent_item_id="parentid005", item_type="hook",
        publish_at=manual_time,
        publish_at_manual=1,
    )
    _insert(
        "hookauto99", parent_item_id="parentid005", item_type="hook",
        publish_at=(base + timedelta(hours=200)).isoformat(),
        publish_at_manual=0,
    )

    sibling = env["scheduler"].cascade_siblings_on_shift
    delta = timedelta(hours=5)
    old = base + timedelta(hours=4)
    shifted = await sibling("hookcurrnt", old, old + delta)

    assert "hookmanual" not in shifted
    assert "hookauto99" in shifted
    assert _read_publish_at("hookmanual") == manual_time


@pytest.mark.asyncio
async def test_apply_user_reschedule_marks_manual_and_cascades(env) -> None:
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid006", publish_at=base.isoformat())
    _insert(
        "auto1", parent_item_id="parentid006", item_type="short",
        publish_at=(base + timedelta(hours=18)).isoformat(),
        publish_at_manual=0,
    )

    result = await env["scheduler"].apply_user_reschedule(
        "parentid006", base + timedelta(days=2),
    )
    assert result["cascaded_children"] == ["auto1"]
    # Parent now flagged manual; auto child still 0.
    from yt_scheduler.config import DB_PATH

    with sqlite3.connect(str(DB_PATH)) as conn:
        parent_flag = conn.execute(
            "SELECT publish_at_manual FROM videos WHERE id = 'parentid006'"
        ).fetchone()[0]
        child_flag = conn.execute(
            "SELECT publish_at_manual FROM videos WHERE id = 'auto1'"
        ).fetchone()[0]
    assert parent_flag == 1
    assert child_flag == 0


@pytest.mark.asyncio
async def test_cascade_other_tiers_untouched_on_sibling_shift(env) -> None:
    """A hook reschedule must not move a short / segment sibling."""
    base = datetime(2026, 4, 1, 17, 30, tzinfo=timezone.utc)
    _insert("parentid007", publish_at=base.isoformat())
    _insert(
        "hookA0001", parent_item_id="parentid007", item_type="hook",
        publish_at=(base + timedelta(hours=4)).isoformat(),
    )
    _insert(
        "hookB0001", parent_item_id="parentid007", item_type="hook",
        publish_at=(base + timedelta(hours=4 + 99)).isoformat(),
    )
    short_iso = (base + timedelta(hours=18)).isoformat()
    _insert(
        "shortX0001", parent_item_id="parentid007", item_type="short",
        publish_at=short_iso,
    )

    shifted = await env["scheduler"].cascade_siblings_on_shift(
        "hookA0001",
        base + timedelta(hours=4),
        base + timedelta(hours=4 + 6),
    )
    assert "shortX0001" not in shifted
    assert _read_publish_at("shortX0001") == short_iso
