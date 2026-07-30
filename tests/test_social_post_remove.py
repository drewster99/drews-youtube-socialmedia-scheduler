"""DELETE /api/social/posts/{post_id} — remove a draft social post.

The removable set is exactly ``status='draft'``. Everything else is refused:
``posted`` is the record of something the world has seen, ``sending`` is
mid-flight, ``approved`` may have a live per-post job behind it, and ``failed``
is what the app-wide failed-sends banner is built from.

Also covers the two things that make the delete safe rather than merely
convenient: the ``social_post_traces`` cascade, and the status guard repeated
inside the DELETE statement so a send that claims the row mid-request wins.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.conftest import install_in_memory_keychain

VIDEO_ID = "RMV0000001"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    # Startup runs the Keychain ACL repair; keep it (and anything a route
    # reaches) off the real login Keychain.
    install_in_memory_keychain(
        monkeypatch, importlib.import_module("yt_scheduler.services.keychain")
    )
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


async def _ensure_video() -> None:
    """The one video every post and queue item here hangs off.

    Both ``social_posts.video_id`` and ``smart_queue_items.video_id`` are real
    foreign keys, so whichever helper runs first has to create it.
    """
    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "INSERT OR IGNORE INTO videos "
        "(id, project_id, title, status, imported_from_youtube) "
        f"VALUES ('{VIDEO_ID}', 1, 't', 'uploaded', 1)"
    )
    await db.commit()


async def _seed_queue_item() -> int:
    """A real smart queue + one item, so the post's FK resolves.

    Uses whichever template startup seeded — ``smart_queues.template_id`` is
    ON DELETE RESTRICT, so it has to be a live row.
    """
    from yt_scheduler.database import get_db

    await _ensure_video()
    db = await get_db()
    templates = await db.execute_fetchall("SELECT id FROM templates LIMIT 1")
    assert templates, "startup should have seeded at least one template"
    cursor = await db.execute(
        "INSERT INTO smart_queues (project_id, name, template_id, timezone) "
        "VALUES (1, 'Q', ?, 'America/Los_Angeles')",
        (int(templates[0]["id"]),),
    )
    queue_id = int(cursor.lastrowid)
    cursor = await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, ?, 0, 'scheduled')",
        (queue_id, VIDEO_ID),
    )
    item_id = int(cursor.lastrowid)
    await db.commit()
    return item_id


async def _seed_post(
    status: str,
    *,
    with_trace: bool = False,
    scheduler_job_id: str | None = None,
    smart_queue_item_id: int | None = None,
) -> int:
    """Insert a video (once) plus one social post in ``status``; return its id."""
    from yt_scheduler.database import get_db

    await _ensure_video()
    db = await get_db()
    cursor = await db.execute(
        "INSERT INTO social_posts "
        "(video_id, platform, content, status, scheduler_job_id, smart_queue_item_id) "
        "VALUES (?, 'threads', 'body text', ?, ?, ?)",
        (VIDEO_ID, status, scheduler_job_id, smart_queue_item_id),
    )
    post_id = int(cursor.lastrowid)
    if with_trace:
        await db.execute(
            "INSERT INTO social_post_traces (post_id, trace_json) VALUES (?, ?)",
            (post_id, json.dumps([{"kind": "template_body", "text": "x"}])),
        )
    await db.commit()
    return post_id


async def _count(table: str, post_id: int) -> int:
    from yt_scheduler.database import get_db

    db = await get_db()
    column = "id" if table == "social_posts" else "post_id"
    rows = await db.execute_fetchall(
        f"SELECT COUNT(*) AS n FROM {table} WHERE {column} = ?", (post_id,)
    )
    return int(rows[0]["n"])


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["draft", "failed"])
async def test_removable_statuses_are_removed(client: TestClient, status: str) -> None:
    """A failed post is removable because nothing will ever retry it — see
    test_nothing_reclaims_a_failed_post below for the standing proof."""
    post_id = await _seed_post(status)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "ok", "cancelled_schedule": False}
    assert await _count("social_posts", post_id) == 0


@pytest.mark.asyncio
async def test_removing_a_draft_takes_its_trace_with_it(client: TestClient) -> None:
    """The FK cascade is only live because database.py sets PRAGMA
    foreign_keys=ON; assert the row actually goes, not just the pragma."""
    post_id = await _seed_post("draft", with_trace=True)
    assert await _count("social_post_traces", post_id) == 1

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 200, resp.text
    assert await _count("social_post_traces", post_id) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["approved", "sending", "posted"])
async def test_statuses_with_something_pending_are_refused(
    client: TestClient, status: str
) -> None:
    post_id = await _seed_post(status)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 409, resp.text
    # The message names the offending status — an opaque 409 leaves the user
    # guessing why a button that exists refused to work.
    assert status in resp.json()["detail"]
    assert await _count("social_posts", post_id) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["draft", "failed"])
async def test_smart_queue_owned_posts_are_refused(
    client: TestClient, status: str
) -> None:
    """Deleting a queue-owned post would strand its queue item.

    smart_queue.list_queues derives an item's bucket from its posting rows
    (LEFT JOIN social_posts … ELSE i.state), so with the post gone the item is
    reported 'scheduled' forever with nothing left to send, and its video never
    becomes eligible to be queued again. Those posts have their own exit: the
    queue's missed-postings screen, which moves the ITEM to 'removed'.

    Refused for every status, not just the removable two — queue ownership is
    about the item's bookkeeping, not about what this post happens to be doing.
    """
    item_id = await _seed_queue_item()
    post_id = await _seed_post(status, smart_queue_item_id=item_id)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    # The message has to name the way out, or it's a dead end.
    assert "missed-postings" in detail
    assert await _count("social_posts", post_id) == 1


@pytest.mark.asyncio
async def test_list_endpoint_exposes_queue_ownership(client: TestClient) -> None:
    """The card gates its Remove button on ``smart_queue_item_id``. If the list
    projection ever stops returning it, every queue-owned post silently renders
    an enabled Remove that can only 409."""
    item_id = await _seed_queue_item()
    queue_owned = await _seed_post("failed", smart_queue_item_id=item_id)

    posts = client.get(f"/api/social/posts/{VIDEO_ID}").json()

    by_id = {post["id"]: post for post in posts}
    assert by_id[queue_owned]["smart_queue_item_id"] == item_id


@pytest.mark.asyncio
async def test_unknown_post_is_404(client: TestClient) -> None:
    resp = client.delete("/api/social/posts/987654")

    assert resp.status_code == 404, resp.text


@pytest.mark.asyncio
async def test_nothing_reclaims_a_failed_post(client: TestClient) -> None:
    """The premise behind allowing Remove on 'failed': nothing retries it.

    Every failure path clears ``scheduled_at``/``scheduler_job_id``, and the
    startup restore pass only looks at rows that still have a ``scheduled_at``.
    If a future change starts auto-retrying failed posts, this test fails —
    which is the point, because removing one would then be destroying work in
    progress rather than clearing a dead end.
    """
    post_id = await _seed_post("failed")
    scheduler_service = importlib.import_module("yt_scheduler.services.scheduler")

    await scheduler_service.restore_scheduled_posts()

    from yt_scheduler.database import get_db

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT status, scheduled_at, scheduler_job_id FROM social_posts WHERE id = ?",
        (post_id,),
    )
    assert rows[0]["status"] == "failed"
    assert rows[0]["scheduled_at"] is None
    assert rows[0]["scheduler_job_id"] is None
    assert scheduler_service.scheduler.get_job(f"social_post_{post_id}") is None


@pytest.mark.asyncio
async def test_pending_schedule_is_torn_down_before_the_row_goes(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A draft should not carry a job, but PUT can set status back to 'draft'
    without clearing scheduler_job_id. Leaving the trigger armed would fire it
    against a deleted row."""
    post_id = await _seed_post("draft", scheduler_job_id="social_post_1")
    social_routes = sys.modules["yt_scheduler.routers.social_routes"]
    called: list[int] = []

    async def fake_cancel(pid: int) -> bool:
        called.append(pid)
        return True

    monkeypatch.setattr(social_routes, "cancel_scheduled_post", fake_cancel)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 200, resp.text
    assert resp.json()["cancelled_schedule"] is True
    assert called == [post_id]
    assert await _count("social_posts", post_id) == 0


@pytest.mark.asyncio
async def test_no_scheduler_traffic_when_the_draft_has_no_job(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    post_id = await _seed_post("draft")
    social_routes = sys.modules["yt_scheduler.routers.social_routes"]
    called: list[int] = []

    async def fake_cancel(pid: int) -> bool:
        called.append(pid)
        return False

    monkeypatch.setattr(social_routes, "cancel_scheduled_post", fake_cancel)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 200, resp.text
    assert called == []


@pytest.mark.asyncio
async def test_a_send_claiming_the_row_mid_request_wins(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The status guard is repeated inside the DELETE, so a row that leaves
    'draft' after the read is not removed.

    cancel_scheduled_post is the real await point between the status read and
    the delete, so patching it to flip the status reproduces the race exactly.
    """
    post_id = await _seed_post("draft", scheduler_job_id="social_post_1")
    social_routes = sys.modules["yt_scheduler.routers.social_routes"]

    async def claim_row_then_report_cancelled(pid: int) -> bool:
        from yt_scheduler.database import write_transaction

        async with write_transaction() as db:
            await db.execute("UPDATE social_posts SET status = 'sending' WHERE id = ?", (pid,))
        return True

    monkeypatch.setattr(social_routes, "cancel_scheduled_post", claim_row_then_report_cancelled)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 409, resp.text
    assert await _count("social_posts", post_id) == 1


# --- UI invariants -----------------------------------------------------------
#
# There is no JS test runner here and the page JS lives inside the Jinja
# template, so text invariants over the sources are the feasible guard (same
# rationale as tests/test_template_escaping.py).

_SRC = Path(__file__).resolve().parents[1] / "src" / "yt_scheduler"
_VIDEO_DETAIL = _SRC / "templates_html" / "video_detail.html"
_STYLE = _SRC / "static" / "css" / "style.css"


def _remove_post_function() -> str:
    text = _VIDEO_DETAIL.read_text(encoding="utf-8")
    start = text.index("async function removePost(")
    end = text.index("async function ", start + 1)
    return text[start:end]


def test_remove_button_matches_the_server_rule() -> None:
    """Offered on exactly the removable statuses. A button on a posted, sending
    or approved card would just be a 409 generator."""
    text = _VIDEO_DETAIL.read_text(encoding="utf-8")
    assert "const isRemovable = p.status === 'draft' || isFailed;" in text
    assert "const removeButton = isRemovable" in text
    assert 'onclick="removePost(${p.id})"' in text


def test_queue_owned_posts_show_a_disabled_remove_with_the_reason() -> None:
    """The server refuses these, so an enabled button would only ever 409.
    Disabled-with-a-title (the same idiom as Send on an unconfigured platform)
    beats hiding it, which would leave the absence unexplained."""
    text = _VIDEO_DETAIL.read_text(encoding="utf-8")
    assert "const queueOwned = p.smart_queue_item_id != null;" in text
    assert "${queueOwned ? 'disabled data-stays-disabled' : ''}" in text
    assert "missed-postings screen" in text


def test_busy_cycle_does_not_re_enable_standing_disabled_buttons() -> None:
    """setPostCardBusy blanket-enabled every button when a cycle ended, so a
    shorten handed back a live Remove on a queue-owned post — and a live Send
    for a platform with no credentials. Both now opt out by attribute."""
    text = _VIDEO_DETAIL.read_text(encoding="utf-8")
    assert "if (!busy && b.hasAttribute('data-stays-disabled')) return;" in text
    # The unconfigured-platform Send is the other rider on this mechanism.
    assert "'disabled data-stays-disabled title=\"Configure this platform" in text


def test_remove_confirms_before_deleting() -> None:
    body = _remove_post_function()
    assert "confirm(" in body
    assert "method: 'DELETE'" in body
    # The confirm must precede the request — a confirm after the fetch would
    # be a prompt about something already deleted.
    assert body.index("confirm(") < body.index("method: 'DELETE'")


def test_removing_a_failed_post_warns_it_may_have_published() -> None:
    """A send that timed out after the platform accepted the post is recorded
    as failed, so the confirm must not claim nothing went out."""
    body = _remove_post_function()
    assert "does NOT delete anything the platform may" in body


def test_removal_refreshes_the_app_wide_banner() -> None:
    """The banner polls on its own clock; a removed post must not keep being
    named there, with a View link to a card that is gone."""
    banner = (_SRC / "static" / "js" / "failed-sends-banner.js").read_text(
        encoding="utf-8"
    )
    assert "window.refreshFailedSendsBanner = check;" in banner
    assert "window.refreshFailedSendsBanner()" in _remove_post_function()


def _danger_border_selectors(css: str) -> list[str]:
    """Selectors whose rule body sets a danger border-color."""
    found = []
    for rule in css.split("}"):
        if "{" not in rule:
            continue
        selector, _, body = rule.partition("{")
        if "border-color: var(--danger)" in body:
            found.append(selector)
    return found


@pytest.mark.parametrize("css_class", ["social-post-over-limit", "social-post-failed"])
def test_cards_needing_attention_get_the_red_outline(css_class: str) -> None:
    """Over-limit and failed both mean "this can't go out as it stands", so
    both get the border. A failed card whose only marker was the status pill
    read as fine at a glance."""
    selectors = _danger_border_selectors(_STYLE.read_text(encoding="utf-8"))
    assert any(f".{css_class}" in s for s in selectors)


def test_failed_posts_carry_the_outline_class() -> None:
    text = _VIDEO_DETAIL.read_text(encoding="utf-8")
    assert "${isFailed ? 'social-post-failed' : ''}" in text
