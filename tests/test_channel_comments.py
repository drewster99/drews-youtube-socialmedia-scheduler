"""Channel-wide comment mirror: flattening, upsert, reply follow-up, read API.

The dashboard's Recent comments section renders `youtube_comments`, so the
things worth pinning are the ones that would make it quietly wrong: a re-sweep
losing when we first saw a comment, a channel-level comment being dropped for
having no video, a truncated sweep presenting itself as a complete one, and the
reply follow-up either never running or running forever.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from tests.conftest import install_in_memory_keychain

CHANNEL_ID = "UCchannel0000000000000"


def _comment(
    comment_id: str,
    *,
    author: str = "@viewer",
    author_channel: str | None = "UCviewer000000000000",
    text: str = "great video",
    likes: int = 0,
    published: str = "2026-08-01T10:00:00Z",
    updated: str | None = None,
) -> dict:
    snippet = {
        "textDisplay": text,
        "authorDisplayName": author,
        "authorProfileImageUrl": "https://yt3.example/photo.jpg",
        "likeCount": likes,
        "publishedAt": published,
    }
    if author_channel is not None:
        snippet["authorChannelId"] = {"value": author_channel}
    if updated is not None:
        snippet["updatedAt"] = updated
    return {"id": comment_id, "snippet": snippet}


def _thread(
    top: dict,
    *,
    video_id: str | None = "vid00000001",
    replies: list[dict] | None = None,
    total_reply_count: int | None = None,
) -> dict:
    snippet: dict = {
        "topLevelComment": top,
        "totalReplyCount": (
            total_reply_count if total_reply_count is not None else len(replies or [])
        ),
    }
    if video_id is not None:
        snippet["videoId"] = video_id
    thread = {"id": f"thread-{top['id']}", "snippet": snippet}
    if replies:
        thread["replies"] = {"comments": replies}
    return thread


@pytest.fixture
async def comments(isolated_db):
    """The service module, with a channel-bound project and one local video."""
    module = importlib.import_module("yt_scheduler.services.comments")
    await isolated_db.execute(
        "UPDATE projects SET youtube_channel_id = ? WHERE id = 1", (CHANNEL_ID,)
    )
    await isolated_db.execute(
        "INSERT INTO videos (id, project_id, title, status, youtube_video_id, "
        "episode_number) VALUES ('vid00000001', 1, 'Ep 42 — Relay', 'published', "
        "'vid00000001', 42)"
    )
    await isolated_db.commit()
    return module


@pytest.fixture
def project() -> dict:
    return {"id": 1, "slug": "default", "youtube_channel_id": CHANNEL_ID}


def _install_fake_youtube(
    monkeypatch, *, threads: list[dict], hit_cap: bool = False, replies=None
) -> dict:
    """Patch the YouTube wrappers the sweep calls; record what it asked for."""
    youtube = importlib.import_module("yt_scheduler.services.youtube")
    calls: dict = {"threads": 0, "reply_parents": []}

    def fake_threads(channel_id: str, *, max_pages: int, per_page: int = 100):
        calls["threads"] += 1
        calls["channel_id"] = channel_id
        calls["max_pages"] = max_pages
        return threads, hit_cap

    def fake_replies(parent_comment_id: str, *, max_results: int = 100):
        calls["reply_parents"].append(parent_comment_id)
        return (replies or {}).get(parent_comment_id, [])

    monkeypatch.setattr(youtube, "list_channel_comment_threads", fake_threads)
    monkeypatch.setattr(youtube, "list_comment_replies", fake_replies)
    return calls


# --- flattening --------------------------------------------------------------


def test_flatten_emits_one_record_per_comment(comments) -> None:
    reply = _comment("r1", author="Drew", author_channel=CHANNEL_ID, text="thanks!")
    threads = [_thread(_comment("c1"), replies=[reply])]

    records = comments.flatten_threads(threads)

    assert [r.comment_id for r in records] == ["c1", "r1"]
    assert records[0].parent_comment_id is None
    assert records[0].total_reply_count == 1
    # A reply is on the same video as its parent — comments.list never says so.
    assert records[1].parent_comment_id == "c1"
    assert records[1].youtube_video_id == "vid00000001"
    assert records[1].total_reply_count is None


def test_channel_level_comment_is_kept_with_no_video(comments) -> None:
    """A sweep also returns comments on the channel itself. Those have no
    videoId, and dropping them would lose real comments."""
    records = comments.flatten_threads([_thread(_comment("c1"), video_id=None)])

    assert len(records) == 1
    assert records[0].youtube_video_id is None


def test_missing_author_channel_and_updated_at_are_tolerated(comments) -> None:
    """YouTube omits authorChannelId for some commenters, and updatedAt only
    appears once a comment is edited."""
    records = comments.flatten_threads(
        [_thread(_comment("c1", author_channel=None, published="2026-08-01T10:00:00Z"))]
    )

    assert records[0].author_channel_id is None
    assert records[0].youtube_updated_at == "2026-08-01T10:00:00Z"


# --- sweep + upsert ----------------------------------------------------------


async def test_sync_stores_comments_and_reports_counts(
    comments, project, isolated_db, monkeypatch
) -> None:
    calls = _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), replies=[_comment("r1", text="agreed")])],
    )

    summary = await comments.sync_project_comments(project)

    assert calls["channel_id"] == CHANNEL_ID
    assert summary["new"] == 2
    assert summary["updated"] == 0
    assert summary["pages_truncated"] is False
    rows = await isolated_db.execute_fetchall(
        "SELECT comment_id FROM youtube_comments ORDER BY comment_id"
    )
    assert [r["comment_id"] for r in rows] == ["c1", "r1"]


async def test_resweep_updates_text_without_moving_first_seen_at(
    comments, project, isolated_db, monkeypatch
) -> None:
    """first_seen_at is when WE saw it — a re-sweep must not reset it, or the
    'what is new to me' question a notifier will ask has no stable answer."""
    _install_fake_youtube(monkeypatch, threads=[_thread(_comment("c1", text="first"))])
    await comments.sync_project_comments(project)
    before = (
        await isolated_db.execute_fetchall(
            "SELECT first_seen_at FROM youtube_comments WHERE comment_id = 'c1'"
        )
    )[0]["first_seen_at"]

    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment(
                    "c1", text="first (edited)", likes=9, updated="2026-08-01T12:00:00Z"
                )
            )
        ],
    )
    summary = await comments.sync_project_comments(project)

    assert summary["new"] == 0
    assert summary["updated"] == 1
    row = (
        await isolated_db.execute_fetchall(
            "SELECT text_display, like_count, first_seen_at FROM youtube_comments "
            "WHERE comment_id = 'c1'"
        )
    )[0]
    assert row["text_display"] == "first (edited)"
    assert row["like_count"] == 9
    assert row["first_seen_at"] == before


async def test_truncated_sweep_says_so(comments, project, monkeypatch) -> None:
    """A sweep that stopped at the page cap must report it — a partial mirror
    presented as a complete one is the silent-wrong-answer case."""
    _install_fake_youtube(
        monkeypatch, threads=[_thread(_comment("c1"))], hit_cap=True
    )

    summary = await comments.sync_project_comments(project)

    assert summary["pages_truncated"] is True


async def test_project_without_channel_raises_rather_than_returning_empty(
    comments,
) -> None:
    with pytest.raises(comments.ChannelNotBound):
        await comments.sync_project_comments(
            {"id": 1, "slug": "default", "youtube_channel_id": None}
        )


# --- reply follow-up ---------------------------------------------------------


async def test_thread_with_truncated_replies_is_fetched_then_left_alone(
    comments, project, isolated_db, monkeypatch
) -> None:
    """A thread resource carries only a preview of its replies. The follow-up
    must run once and then stop: it is gated on stored-count vs totalReplyCount,
    so a fully-stored thread costs nothing on later sweeps."""
    preview = [_comment("r1"), _comment("r2")]
    full = preview + [_comment("r3"), _comment("r4")]
    threads = [_thread(_comment("c1"), replies=preview, total_reply_count=4)]

    calls = _install_fake_youtube(
        monkeypatch, threads=threads, replies={"c1": full}
    )
    first = await comments.sync_project_comments(project)

    assert calls["reply_parents"] == ["c1"]
    assert first["reply_fetches"] == 1
    # Five distinct comments exist (c1 + r1..r4) and each is counted once: the
    # follow-up returns the preview replies again, and re-storing them would
    # report writes that never happened.
    assert first["comments_seen"] == 5
    assert (first["new"], first["updated"]) == (5, 0)
    rows = await isolated_db.execute_fetchall(
        "SELECT comment_id FROM youtube_comments WHERE parent_comment_id = 'c1' "
        "ORDER BY comment_id"
    )
    assert [r["comment_id"] for r in rows] == ["r1", "r2", "r3", "r4"]

    calls = _install_fake_youtube(
        monkeypatch, threads=threads, replies={"c1": full}
    )
    second = await comments.sync_project_comments(project)

    assert calls["reply_parents"] == [], "a fully-stored thread was re-fetched"
    assert second["reply_fetches"] == 0


async def test_reply_fetch_cap_is_reported_not_swallowed(
    comments, project, monkeypatch
) -> None:
    """Hitting the per-sweep reply budget is a fact the caller has to see —
    otherwise the sweep claims completeness it does not have."""
    config = importlib.import_module("yt_scheduler.config")
    monkeypatch.setattr(config, "COMMENT_SYNC_MAX_REPLY_FETCHES", 1)
    monkeypatch.setattr(comments, "COMMENT_SYNC_MAX_REPLY_FETCHES", 1)

    threads = [
        _thread(_comment(f"c{i}"), replies=[_comment(f"c{i}r1")], total_reply_count=3)
        for i in range(3)
    ]
    calls = _install_fake_youtube(
        monkeypatch,
        threads=threads,
        replies={f"c{i}": [_comment(f"c{i}r{n}") for n in range(1, 4)] for i in range(3)},
    )

    summary = await comments.sync_project_comments(project)

    assert len(calls["reply_parents"]) == 1
    assert summary["threads_with_unfetched_replies"] == 2


# --- read path ---------------------------------------------------------------


async def test_recent_comments_newest_first_with_video_and_owner_flags(
    comments, project, monkeypatch
) -> None:
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1", published="2026-08-01T10:00:00Z"),
                replies=[
                    _comment(
                        "r1",
                        author="Drew",
                        author_channel=CHANNEL_ID,
                        published="2026-08-01T11:00:00Z",
                    )
                ],
            ),
            _thread(
                _comment("c2", published="2026-08-02T09:00:00Z"),
                video_id=None,
            ),
        ],
    )
    await comments.sync_project_comments(project)

    rows = await comments.list_recent_comments(1, limit=10)

    assert [r["comment_id"] for r in rows] == ["c2", "r1", "c1"]

    channel_level, owner_reply, top = rows
    # No videoId, so no video to name — and no local page to link to.
    assert channel_level["youtube_video_id"] is None
    assert channel_level["local_video_id"] is None
    # The channel owner's own reply is marked, so answered threads read as such.
    assert owner_reply["is_channel_owner"] is True
    assert owner_reply["is_reply"] is True
    assert top["is_channel_owner"] is False
    assert top["is_reply"] is False
    assert top["local_video_id"] == "vid00000001"
    assert top["video_title"] == "Ep 42 — Relay"
    assert top["episode_number"] == 42


async def test_paging_walks_the_whole_list_without_repeats(
    comments, project, monkeypatch
) -> None:
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment(f"c{i}", published=f"2026-08-{i + 1:02d}T10:00:00Z"),
                video_id=None,
            )
            for i in range(5)
        ],
    )
    await comments.sync_project_comments(project)

    first = await comments.list_recent_comments(1, limit=2, offset=0)
    second = await comments.list_recent_comments(1, limit=2, offset=2)
    third = await comments.list_recent_comments(1, limit=2, offset=4)

    seen = [r["comment_id"] for r in first + second + third]
    assert seen == ["c4", "c3", "c2", "c1", "c0"]
    assert await comments.count_comments(1) == 5


async def test_blocklisted_comments_are_hidden_but_failed_rejections_are_not(
    comments, project, isolated_db, monkeypatch
) -> None:
    """YouTube keeps returning comments the blocklist already rejected, so the
    sweep re-stores them every tick. Showing them would hand the user back the
    exact spam moderation exists to remove — but only where the rejection
    actually succeeded: `error` means the comment is still live."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(_comment("spam1", text="buy cheap watches"), video_id=None),
            _thread(_comment("spam2", text="also spam"), video_id=None),
            _thread(_comment("real1", text="genuinely nice video"), video_id=None),
        ],
    )
    await comments.sync_project_comments(project)
    await isolated_db.execute(
        "INSERT INTO moderation_log (project_id, video_id, comment_id, action) "
        "VALUES (1, 'v', 'spam1', 'deleted'), (1, 'v', 'spam2', 'error')"
    )
    await isolated_db.commit()

    rows = await comments.list_recent_comments(1, limit=10)

    ids = [r["comment_id"] for r in rows]
    assert "spam1" not in ids, "a rejected comment came back on the dashboard"
    assert "spam2" in ids, "a FAILED rejection was hidden — that comment is live"
    assert "real1" in ids
    # The count must match what the list can show, or 'Showing N of M' promises
    # rows Load more can never reach.
    assert await comments.count_comments(1) == len(ids) == 2


async def test_last_synced_at_is_none_before_any_sweep(comments) -> None:
    """'Never synced' and 'no comments' are different things to tell the user."""
    assert await comments.last_synced_at(1) is None


# --- HTTP surface ------------------------------------------------------------


@pytest.fixture
async def app_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)

    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    app_module = importlib.import_module("yt_scheduler.app")

    await database.get_db()
    await projects.ensure_default_project()

    transport = ASGITransport(app=app_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    await database.close_db()


async def test_get_comments_reports_never_synced_and_no_channel(app_client) -> None:
    resp = await app_client.get("/api/projects/default/comments")

    assert resp.status_code == 200
    body = resp.json()
    assert body["comments"] == []
    assert body["total"] == 0
    assert body["last_synced_at"] is None
    assert body["channel_connected"] is False


@pytest.mark.parametrize(
    "query, expected",
    [("?limit=0", 400), ("?limit=201", 400), ("?offset=-1", 400)],
)
async def test_bad_paging_is_refused_not_clamped(app_client, query, expected) -> None:
    """A silently corrected limit returns a page the caller did not ask for and
    looks like the list simply ended."""
    resp = await app_client.get(f"/api/projects/default/comments{query}")
    assert resp.status_code == expected


async def test_unknown_project_is_404(app_client) -> None:
    assert (await app_client.get("/api/projects/nope/comments")).status_code == 404
    assert (
        await app_client.post("/api/projects/nope/comments/sync")
    ).status_code == 404


async def test_sync_without_a_channel_is_a_400_with_a_reason(app_client) -> None:
    resp = await app_client.post("/api/projects/default/comments/sync")

    assert resp.status_code == 400
    assert "no YouTube channel" in resp.json()["detail"]


# --- dashboard placement -----------------------------------------------------


def test_recent_comments_renders_above_smart_schedules() -> None:
    """Placement is the request, not a detail: comments are what someone else
    is waiting on, so they sit at the top of the dashboard."""
    dashboard = (
        Path(__file__).resolve().parents[1]
        / "src/yt_scheduler/templates_html/dashboard.html"
    ).read_text()

    comments_at = dashboard.index('id="comments-section"')
    queues_at = dashboard.index('id="smart-queue-section"')
    videos_at = dashboard.index('id="video-list"')
    assert comments_at < queues_at < videos_at
