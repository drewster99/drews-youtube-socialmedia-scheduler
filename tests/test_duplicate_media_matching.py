"""find_recent_duplicate_post must compare the FULL attachment list, and must
never cap its candidate scan.

The send path posts every entry of ``media_paths``, so two posts differing only
in their 2nd-4th attachments are not duplicates. Comparing only the legacy
single ``media_path`` treated them as identical; capping the candidate rows
would hide an older same-media post and publish it a second time.
"""

from __future__ import annotations

import importlib
import json

import pytest


@pytest.fixture
async def social(isolated_db):
    await isolated_db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('v1', 1, 't', 'published')"
    )
    await isolated_db.commit()
    return importlib.import_module("yt_scheduler.services.social")


async def _post(db, media: list[str] | None, *, content: str = "hello", age_days: int = 1):
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, media_paths, status, posted_at) "
        "VALUES ('v1', 'mastodon', ?, ?, 'posted', datetime('now', ?))",
        (content, json.dumps(media) if media is not None else None, f"-{age_days} days"),
    )
    await db.commit()


async def test_same_text_different_attachments_is_not_a_duplicate(social, isolated_db):
    await _post(isolated_db, ["/a.mp4", "/b.mp4"])

    dup = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=None, content="hello",
        media_paths=["/a.mp4", "/c.mp4"],
    )
    assert dup is None


async def test_identical_attachment_list_is_a_duplicate(social, isolated_db):
    await _post(isolated_db, ["/a.mp4", "/b.mp4"])

    dup = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=None, content="hello",
        media_paths=["/a.mp4", "/b.mp4"],
    )
    assert dup is not None


async def test_legacy_single_media_row_still_matches(social, isolated_db):
    """Migration 010 backfilled media_paths = json_array(media_path)."""
    await isolated_db.execute(
        "INSERT INTO social_posts (video_id, platform, content, media_path, status, posted_at) "
        "VALUES ('v1', 'mastodon', 'hello', '/a.mp4', 'posted', datetime('now','-1 days'))"
    )
    await isolated_db.commit()

    dup = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=None, content="hello",
        media_paths=["/a.mp4"],
    )
    assert dup is not None


async def test_no_media_bucket_is_distinct_from_media(social, isolated_db):
    await _post(isolated_db, None)

    assert await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=None, content="hello", media_paths=[],
    ) is not None
    assert await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=None, content="hello",
        media_paths=["/a.mp4"],
    ) is None


async def test_older_same_media_post_is_found_behind_many_newer_ones(social, isolated_db):
    """No LIMIT on the candidate scan: a cap here would let a duplicate publish twice."""
    await _post(isolated_db, ["/a.mp4"], age_days=20)
    for i in range(25):
        await _post(isolated_db, [f"/other{i}.mp4"], age_days=1)

    dup = await social.find_recent_duplicate_post(
        platform="mastodon", social_account_id=None, content="hello",
        media_paths=["/a.mp4"],
    )
    assert dup is not None, "older same-media duplicate hidden — would double-post"
