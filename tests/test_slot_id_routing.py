"""G1 — multi-slot routing for same-platform slots.

Two Mastodon slots in one template (different accounts) must be
addressable independently:
  * INSERT carries the slot_id so each post pins to its source slot.
  * A partial regenerate with ``slot_ids=[A]`` regenerates that slot
    only and leaves slot B's posts untouched.
  * The DELETE-before-regen scopes by slot_id, not platform, so
    re-running one of the two doesn't blow away the other.
  * Legacy rows (slot_id IS NULL — pre-021) still match by platform
    when no slot_ids are sent.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
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
    app_module = importlib.import_module("yt_scheduler.app")
    from fastapi.testclient import TestClient
    with TestClient(app_module.app) as c:
        yield c
    from yt_scheduler.database import close_db
    await close_db()


def _fake_render(template_text, variables=None, **kwargs):
    """No real Claude calls. Echo the template body with the variables
    inlined so we can assert each slot ran with its own body."""
    out = template_text
    for k, v in (variables or {}).items():
        out = out.replace("{{" + k + "}}", str(v))
    return out


async def _seed_video(c, video_id: str = "vidSLOT") -> str:
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, item_type) "
        "VALUES (?, 1, 'slot routing test', 'uploaded', 'episode')",
        (video_id,),
    )
    await db.commit()
    return video_id


async def _create_template_with_two_mastodon_slots(c) -> tuple[int, int]:
    """Build a template that has two Mastodon slots (one per fake
    account) plus the seeded built-ins from create_project. Returns
    (slot_a_id, slot_b_id)."""
    resp = c.post(
        "/api/templates",
        json={"name": "twomast", "description": "two mastodon", "platforms": {}},
    )
    assert resp.status_code == 200, resp.text

    sa = c.post(
        "/api/templates/twomast/slots",
        json={"platform": "mastodon", "body": "slot A body", "media": "none"},
    )
    assert sa.status_code == 200, sa.text
    sb = c.post(
        "/api/templates/twomast/slots",
        json={"platform": "mastodon", "body": "slot B body", "media": "none"},
    )
    assert sb.status_code == 200, sb.text
    return int(sa.json()["id"]), int(sb.json()["id"])


@pytest.mark.asyncio
async def test_generate_with_slot_ids_inserts_correct_slot_id(client) -> None:
    """Generating with ``slot_ids=[A]`` writes a row whose slot_id == A."""
    video_id = await _seed_video(client)
    slot_a, slot_b = await _create_template_with_two_mastodon_slots(client)

    from yt_scheduler.services import templates as tmpl
    with patch.object(tmpl, "render", side_effect=_fake_render):
        resp = client.post(
            f"/api/social/generate-posts/{video_id}",
            json={"template_name": "twomast", "slot_ids": [slot_a]},
        )
    assert resp.status_code == 200, resp.text

    from yt_scheduler.database import get_db
    db = await get_db()
    cur = await db.execute(
        "SELECT slot_id, content FROM social_posts WHERE video_id = ?",
        (video_id,),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    assert len(rows) == 1
    assert rows[0]["slot_id"] == slot_a
    assert "slot A body" in rows[0]["content"]


@pytest.mark.asyncio
async def test_partial_regen_by_slot_id_preserves_sibling_slot(client) -> None:
    """With both Mastodon slots already generated, regenerating ONLY
    slot A must leave slot B's row untouched — the old platform-only
    DELETE would have nuked both."""
    video_id = await _seed_video(client)
    slot_a, slot_b = await _create_template_with_two_mastodon_slots(client)

    # Seed an existing 'approved' row for slot B that should survive
    # the slot-A regenerate.
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status, slot_id) "
        "VALUES (?, 'mastodon', 'pre-approved on slot B', 'approved', ?)",
        (video_id, slot_b),
    )
    await db.commit()

    from yt_scheduler.services import templates as tmpl
    with patch.object(tmpl, "render", side_effect=_fake_render):
        resp = client.post(
            f"/api/social/generate-posts/{video_id}",
            json={"template_name": "twomast", "slot_ids": [slot_a]},
        )
    assert resp.status_code == 200, resp.text

    cur = await db.execute(
        "SELECT slot_id, status, content FROM social_posts "
        "WHERE video_id = ? ORDER BY slot_id",
        (video_id,),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    by_slot = {r["slot_id"]: r for r in rows}
    assert slot_a in by_slot
    assert by_slot[slot_a]["status"] == "draft"
    assert "slot A body" in by_slot[slot_a]["content"]

    # Slot B's pre-existing approved row MUST survive the slot-A regen.
    assert slot_b in by_slot, (
        "slot-A regenerate must NOT delete slot-B's approved row"
    )
    assert by_slot[slot_b]["status"] == "approved"
    assert by_slot[slot_b]["content"] == "pre-approved on slot B"


@pytest.mark.asyncio
async def test_legacy_slot_id_null_row_still_matches_by_platform(client) -> None:
    """A pre-021 row has slot_id=NULL. Generating against the same
    platform without sending slot_ids should still replace it (the
    back-compat path)."""
    video_id = await _seed_video(client, "vidLEG")
    slot_a, _slot_b = await _create_template_with_two_mastodon_slots(client)

    from yt_scheduler.database import get_db
    db = await get_db()
    # Legacy row: no slot_id.
    await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES (?, 'mastodon', 'legacy mastodon body', 'draft')",
        (video_id,),
    )
    await db.commit()

    from yt_scheduler.services import templates as tmpl
    with patch.object(tmpl, "render", side_effect=_fake_render):
        # No slot_ids sent — back-compat path. Platform-only filter
        # should still match the legacy NULL row + clear it.
        resp = client.post(
            f"/api/social/generate-posts/{video_id}",
            json={"template_name": "twomast", "platforms": ["mastodon"]},
        )
    assert resp.status_code == 200, resp.text

    cur = await db.execute(
        "SELECT slot_id, content FROM social_posts WHERE video_id = ?",
        (video_id,),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    # The legacy NULL row is gone; the two new draft rows (one per
    # mastodon slot) are present and tagged with their slot_ids.
    assert all(r["slot_id"] is not None for r in rows)
    assert len(rows) == 2
    contents = sorted(r["content"] for r in rows)
    assert "slot A body" in contents[0]
    assert "slot B body" in contents[1]


@pytest.mark.asyncio
async def test_slot_id_propagates_to_get_posts_response(client) -> None:
    """The ``GET /api/social/posts/{video_id}`` endpoint surfaces
    slot_id so the front-end picker can route per-account."""
    video_id = await _seed_video(client, "vidGET")
    slot_a, _slot_b = await _create_template_with_two_mastodon_slots(client)

    from yt_scheduler.services import templates as tmpl
    with patch.object(tmpl, "render", side_effect=_fake_render):
        client.post(
            f"/api/social/generate-posts/{video_id}",
            json={"template_name": "twomast", "slot_ids": [slot_a]},
        )

    resp = client.get(f"/api/social/posts/{video_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["slot_id"] == slot_a
