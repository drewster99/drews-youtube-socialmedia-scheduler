"""The auto-social step must not insert a duplicate draft set on chain re-entry.

Steps 3 (tags) and 4 (description) already guard on their own output. Step 5 only
checked "a description exists" — and a description is permanent, so every
re-import, restart restore, and manual re-trigger inserted a fresh full set of
draft posts. restore_pending_auto_actions' own docstring promised to "skip
socials if they exist"; nothing implemented it.
"""

from __future__ import annotations

import asyncio
import importlib

import pytest


@pytest.fixture
async def auto_actions(isolated_db):
    module = importlib.import_module("yt_scheduler.services.auto_actions")
    await isolated_db.execute(
        "INSERT INTO videos (id, project_id, title, status, description) "
        "VALUES ('vid00000001', 1, 'v', 'published', 'a description')"
    )
    await isolated_db.commit()
    return module


async def _post_count(db, video_id: str) -> int:
    rows = await db.execute_fetchall(
        "SELECT COUNT(*) AS n FROM social_posts WHERE video_id = ?", (video_id,)
    )
    return int(rows[0]["n"])


async def _seed_post(db, video_id: str, platform: str = "twitter") -> None:
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES (?, ?, 'hello', 'draft')",
        (video_id, platform),
    )
    await db.commit()


async def test_video_has_social_posts_detects_existing_rows(
    auto_actions, isolated_db
) -> None:
    assert not await auto_actions._video_has_social_posts(isolated_db, "vid00000001")
    await _seed_post(isolated_db, "vid00000001")
    assert await auto_actions._video_has_social_posts(isolated_db, "vid00000001")


async def test_existing_socials_short_circuit_before_any_rendering(
    auto_actions, isolated_db, monkeypatch
) -> None:
    """The gate must fire before we spend Claude tokens rendering a slot set."""
    await _seed_post(isolated_db, "vid00000001")

    def explode(*args, **kwargs):
        raise AssertionError("template lookup reached despite existing social_posts")

    monkeypatch.setattr(auto_actions.tmpl, "get_template", explode)

    await auto_actions._maybe_generate_socials("vid00000001", 1, ["twitter"])

    assert await _post_count(isolated_db, "vid00000001") == 1


async def test_concurrent_chains_do_not_both_insert(
    auto_actions, isolated_db, monkeypatch
) -> None:
    """spawn_background does not dedup by name, so two chains can run at once."""
    await _seed_post(isolated_db, "vid00000001")

    monkeypatch.setattr(
        auto_actions.tmpl,
        "get_template",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not render")),
    )

    await asyncio.gather(*[
        auto_actions._maybe_generate_socials("vid00000001", 1, ["twitter"])
        for _ in range(2)
    ])

    assert await _post_count(isolated_db, "vid00000001") == 1


async def test_no_platforms_is_a_noop(auto_actions, isolated_db) -> None:
    await auto_actions._maybe_generate_socials("vid00000001", 1, [])
    assert await _post_count(isolated_db, "vid00000001") == 0
