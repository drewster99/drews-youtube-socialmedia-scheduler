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


async def _seed_post(
    status: str,
    *,
    with_trace: bool = False,
    scheduler_job_id: str | None = None,
) -> int:
    """Insert a video (once) plus one social post in ``status``; return its id."""
    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "INSERT OR IGNORE INTO videos "
        "(id, project_id, title, status, imported_from_youtube) "
        f"VALUES ('{VIDEO_ID}', 1, 't', 'uploaded', 1)"
    )
    cursor = await db.execute(
        "INSERT INTO social_posts "
        "(video_id, platform, content, status, scheduler_job_id) "
        "VALUES (?, 'threads', 'body text', ?, ?)",
        (VIDEO_ID, status, scheduler_job_id),
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
async def test_draft_is_removed(client: TestClient) -> None:
    post_id = await _seed_post("draft")

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
@pytest.mark.parametrize("status", ["approved", "sending", "posted", "failed"])
async def test_non_draft_statuses_are_refused(client: TestClient, status: str) -> None:
    post_id = await _seed_post(status)

    resp = client.delete(f"/api/social/posts/{post_id}")

    assert resp.status_code == 409, resp.text
    # The message names the offending status — an opaque 409 leaves the user
    # guessing why a button that exists refused to work.
    assert status in resp.json()["detail"]
    assert await _count("social_posts", post_id) == 1


@pytest.mark.asyncio
async def test_unknown_post_is_404(client: TestClient) -> None:
    resp = client.delete("/api/social/posts/987654")

    assert resp.status_code == 404, resp.text


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


def test_remove_button_is_offered_on_drafts_only() -> None:
    """The server refuses any other status; a button that appears on a posted
    or failed card would just be a 409 generator."""
    text = _VIDEO_DETAIL.read_text(encoding="utf-8")
    assert "const removeButton = (p.status === 'draft')" in text
    assert 'onclick="removePost(${p.id})"' in text


def test_remove_confirms_before_deleting() -> None:
    body = _remove_post_function()
    assert "confirm(" in body
    assert "method: 'DELETE'" in body
    # The confirm must precede the request — a confirm after the fetch would
    # be a prompt about something already deleted.
    assert body.index("confirm(") < body.index("method: 'DELETE'")


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
