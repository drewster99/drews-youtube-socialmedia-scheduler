"""Channel-wide comment mirror: flattening, upsert, reply follow-up, read API.

The dashboard's Recent comments section renders `youtube_comments`, so the
things worth pinning are the ones that would make it quietly wrong: a re-sweep
losing when we first saw a comment, a channel-level comment being dropped for
having no video, a truncated sweep presenting itself as a complete one, and the
reply follow-up either never running or running forever.

The read path is grouped into threads, so the same standard applies to the
grouping: a reply that sorts away from the comment it answers, a page boundary
that splits a conversation, an answered thread that reads as ignored (or an
unanswered one that reads as handled), a reply whose blocklisted parent takes it
down with it, and a comment we failed to remove rendering as an ordinary one.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import datetime, timedelta, timezone
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
    viewer_rating: str | None = None,
) -> dict:
    snippet = {
        "textDisplay": text,
        "authorDisplayName": author,
        "authorProfileImageUrl": "https://yt3.example/photo.jpg",
        "likeCount": likes,
        "publishedAt": published,
    }
    if viewer_rating is not None:
        snippet["viewerRating"] = viewer_rating
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
    monkeypatch,
    *,
    threads: list[dict],
    hit_cap: bool = False,
    replies=None,
    buckets: dict[str, list[dict]] | None = None,
    bucket_errors: dict[str, Exception] | None = None,
    bucket_hit_caps: dict[str, bool] | None = None,
    reply_hit_caps: dict[str, bool] | None = None,
) -> dict:
    """Patch the YouTube wrappers the sweep calls; record what it asked for.

    ``threads`` is the `published` bucket — the sweep proper. ``buckets`` supplies
    the moderation buckets viewers cannot see, ``bucket_errors`` makes one of them
    fail (which must never take the sweep down with it), and ``bucket_hit_caps``
    makes one report that it stopped at its page cap.
    """
    youtube = importlib.import_module("yt_scheduler.services.youtube")
    calls: dict = {
        "threads": 0, "reply_parents": [], "statuses": [],
        "max_pages_by_status": {},
    }

    def fake_threads(
        channel_id: str,
        *,
        max_pages: int,
        per_page: int = 100,
        moderation_status: str = "published",
    ):
        calls["threads"] += 1
        calls["channel_id"] = channel_id
        # Per status, not one slot: the primary and supplementary buckets get
        # different caps, and a single overwritten slot could not show that.
        calls["max_pages_by_status"][moderation_status] = max_pages
        calls["statuses"].append(moderation_status)
        if (bucket_errors or {}).get(moderation_status):
            raise bucket_errors[moderation_status]
        if moderation_status == "published":
            return threads, hit_cap
        return (
            (buckets or {}).get(moderation_status, []),
            (bucket_hit_caps or {}).get(moderation_status, False),
        )

    def fake_replies(parent_comment_id: str, *, max_pages: int):
        calls["reply_parents"].append(parent_comment_id)
        calls["reply_max_pages"] = max_pages
        return (
            (replies or {}).get(parent_comment_id, []),
            (reply_hit_caps or {}).get(parent_comment_id, False),
        )

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


async def test_a_thread_too_big_to_fetch_is_not_re_requested_forever(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The old gating was purely `stored < totalReplyCount`, and the fetch
    stopped at 100 replies — so a thread with more than that could never satisfy
    it. It was re-requested on every single sweep, returned the same replies, and
    made no progress: pure quota burn, forever.
    """
    monkeypatch.setattr(comments, "_MAX_FETCHABLE_REPLIES", 3)
    preview = [_comment("r1"), _comment("r2")]
    fetched = preview + [_comment("r3")]
    threads = [_thread(_comment("c1"), video_id=None, replies=preview,
                       total_reply_count=99)]

    calls = _install_fake_youtube(
        monkeypatch, threads=threads, replies={"c1": fetched},
        reply_hit_caps={"c1": True},
    )
    first = await comments.sync_project_comments(project)

    assert calls["reply_parents"] == ["c1"], "the first fetch must still happen"
    assert first["threads_with_replies_truncated"] == 1

    # Far past the refresh window — the ONE thing that could legitimately bring
    # it back — and it still must not be asked about.
    sweep_clock(90_000)
    calls = _install_fake_youtube(
        monkeypatch, threads=threads, replies={"c1": fetched},
        reply_hit_caps={"c1": True},
    )
    second = await comments.sync_project_comments(project)

    assert calls["reply_parents"] == [], "a thread at the reply cap was re-requested"
    assert second["threads_at_reply_cap"] == 1
    # It is at a cap we chose, not an unread backlog a later sweep will clear —
    # counting it as pending would suspend "gone from YouTube" for the project
    # permanently.
    assert second["threads_with_unfetched_replies"] == 0
    assert second["sweep_was_complete"] is True


async def test_a_reply_held_after_we_stored_it_is_corrected_on_refresh(
    comments, project, isolated_db, sweep_clock, monkeypatch
) -> None:
    """The held and likely-spam buckets list threads by their TOP-LEVEL comment,
    so a reply held after we first stored it is never mentioned again by any
    other call. Re-reading the thread's replies on a clock is the only path to
    the per-reply moderationStatus — without it the dashboard shows a held reply
    as an ordinary live comment forever."""
    published_reply = _comment("r1", text="fine at first")
    threads = [_thread(_comment("c1"), video_id=None, replies=[published_reply])]
    _install_fake_youtube(monkeypatch, threads=threads)
    await comments.sync_project_comments(project)

    rows = await isolated_db.execute_fetchall(
        "SELECT moderation_status FROM youtube_comments WHERE comment_id = 'r1'"
    )
    assert rows[0]["moderation_status"] == "published"

    # YouTube now holds that reply. It vanishes from the thread preview and the
    # thread's own bucket never mentions it.
    held_reply = _comment("r1", text="fine at first")
    held_reply["snippet"]["moderationStatus"] = "heldForReview"
    threads_without_it = [_thread(_comment("c1"), video_id=None,
                                  total_reply_count=1)]

    sweep_clock(90_000)
    calls = _install_fake_youtube(
        monkeypatch, threads=threads_without_it, replies={"c1": [held_reply]}
    )
    summary = await comments.sync_project_comments(project)

    assert calls["reply_parents"] == ["c1"], "the stale thread was never re-read"
    assert summary["reply_refreshes"] + summary["reply_fetches"] >= 1
    rows = await isolated_db.execute_fetchall(
        "SELECT moderation_status FROM youtube_comments WHERE comment_id = 'r1'"
    )
    assert rows[0]["moderation_status"] == "heldForReview"


async def test_a_fresh_thread_is_not_re_read_just_because_it_could_be(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The refresh is on a clock, so it must not fire every sweep — that would
    spend the whole reply budget re-reading the same threads."""
    threads = [_thread(_comment("c1"), video_id=None, replies=[_comment("r1")])]
    _install_fake_youtube(monkeypatch, threads=threads, replies={"c1": [_comment("r1")]})
    await comments.sync_project_comments(project)

    sweep_clock(60)
    calls = _install_fake_youtube(
        monkeypatch, threads=threads, replies={"c1": [_comment("r1")]}
    )
    await comments.sync_project_comments(project)

    assert calls["reply_parents"] == []


async def test_reply_fetch_uses_the_page_cap(comments, project, monkeypatch) -> None:
    """A single-page fetch is what made a busy thread permanently short."""
    config = importlib.import_module("yt_scheduler.config")
    calls = _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), video_id=None, replies=[_comment("r1")],
                         total_reply_count=9)],
        replies={"c1": [_comment("r1")]},
    )

    await comments.sync_project_comments(project)

    assert calls["reply_max_pages"] == config.COMMENT_SYNC_MAX_REPLY_PAGES
    assert config.COMMENT_SYNC_MAX_REPLY_PAGES > 1


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


# --- YouTube's own moderation state ------------------------------------------


def test_rejected_is_refused_as_a_list_filter(comments) -> None:
    """`rejected` is a real comment state but commentThreads.list cannot filter
    on it. Asking anyway must fail loudly, not return a silently empty bucket
    that reads as 'nothing was rejected'."""
    youtube = importlib.import_module("yt_scheduler.services.youtube")

    assert "rejected" not in youtube.LISTABLE_MODERATION_STATUSES
    with pytest.raises(ValueError, match="rejected"):
        youtube.list_channel_comment_threads(
            CHANNEL_ID, max_pages=1, moderation_status="rejected"
        )


async def test_sweep_reads_the_buckets_viewers_cannot_see(
    comments, project, isolated_db, monkeypatch
) -> None:
    """YouTube's default filter is `published`, so held and likely-spam threads
    are NOT a subset of the normal sweep — without asking for them by name they
    render on the dashboard as ordinary live comments."""
    calls = _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("ok1"), video_id=None)],
        buckets={
            "heldForReview": [_thread(_comment("held1"), video_id=None)],
            "likelySpam": [_thread(_comment("spam1"), video_id=None)],
        },
    )

    summary = await comments.sync_project_comments(project)

    # Every listable bucket is asked for by name, `published` included — never
    # left to YouTube's default, so the stored status is always one we requested.
    assert sorted(calls["statuses"]) == ["heldForReview", "likelySpam", "published"]
    rows = await isolated_db.execute_fetchall(
        "SELECT comment_id, moderation_status FROM youtube_comments "
        "ORDER BY comment_id"
    )
    assert {r["comment_id"]: r["moderation_status"] for r in rows} == {
        "held1": "heldForReview",
        "ok1": "published",
        "spam1": "likelySpam",
    }
    assert summary["moderation_buckets"]["heldForReview"]["ok"] is True
    assert summary["moderation_buckets"]["likelySpam"]["threads"] == 1


async def test_a_comment_states_its_own_status_over_the_bucket_it_arrived_in(
    comments, project, isolated_db, monkeypatch
) -> None:
    """The resource's own moderationStatus is per-comment and authoritative;
    the bucket is what we asked for. When both exist, believe the comment."""
    top = _comment("c1")
    top["snippet"]["moderationStatus"] = "heldForReview"
    _install_fake_youtube(monkeypatch, threads=[_thread(top, video_id=None)])

    await comments.sync_project_comments(project)

    rows = await isolated_db.execute_fetchall(
        "SELECT moderation_status FROM youtube_comments WHERE comment_id = 'c1'"
    )
    assert rows[0]["moderation_status"] == "heldForReview"


async def test_a_reply_fetch_never_erases_a_known_status(
    comments, project, isolated_db, monkeypatch
) -> None:
    """comments.list takes no moderationStatus filter, so a reply follow-up
    carries NULL. Letting that overwrite would wipe the fact every sweep."""
    held = _comment("held1")
    held["snippet"]["moderationStatus"] = "heldForReview"
    _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), video_id=None, replies=[held],
                         total_reply_count=3)],
        replies={"c1": [held, _comment("r2"), _comment("r3")]},
    )

    await comments.sync_project_comments(project)

    rows = await isolated_db.execute_fetchall(
        "SELECT moderation_status FROM youtube_comments WHERE comment_id = 'held1'"
    )
    assert rows[0]["moderation_status"] == "heldForReview"


async def test_a_failed_bucket_is_reported_not_fatal(
    comments, project, isolated_db, monkeypatch
) -> None:
    """The extra buckets are additive. Losing the whole comment feed because
    YouTube declined one filter would trade the feature for the feed — but a
    bucket we could not read must never look like a bucket that was empty."""
    _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("ok1"), video_id=None)],
        buckets={"likelySpam": [_thread(_comment("spam1"), video_id=None)]},
        bucket_errors={"heldForReview": RuntimeError("403 forbidden")},
    )

    summary = await comments.sync_project_comments(project)

    assert summary["moderation_buckets"]["heldForReview"]["ok"] is False
    assert "403 forbidden" in summary["moderation_buckets"]["heldForReview"]["error"]
    assert summary["moderation_buckets"]["likelySpam"]["ok"] is True
    rows = await isolated_db.execute_fetchall("SELECT comment_id FROM youtube_comments")
    assert {r["comment_id"] for r in rows} == {"ok1", "spam1"}
    # A sweep that could not read everything cannot judge what is missing.
    assert summary["sweep_was_complete"] is False


async def test_primary_bucket_failure_still_fails_the_sweep(
    comments, project, monkeypatch
) -> None:
    """`published` IS the sweep. Degrading its failure to a summary line would
    turn a broken token into a silently empty Recent comments box."""
    _install_fake_youtube(
        monkeypatch,
        threads=[],
        bucket_errors={"published": RuntimeError("token revoked")},
    )

    with pytest.raises(RuntimeError, match="token revoked"):
        await comments.sync_project_comments(project)


# --- a thumbs-up counts as having answered ------------------------------------


async def test_liking_the_last_word_settles_the_thread(
    comments, project, monkeypatch
) -> None:
    """A thumbs-up is a real response. A thread you deliberately liked instead
    of answering must stop asking for a reply."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1", text="❤️", viewer_rating="like"),
                video_id=None,
            )
        ],
    )
    await comments.sync_project_comments(project)

    thread = (await comments.list_recent_threads(1, limit=10))[0]

    assert thread["top_level_comment"]["viewer_rating"] == "like"
    assert thread["owner_liked_last_word"] is True
    assert thread["awaiting_owner_reply"] is False


async def test_an_unrated_comment_still_needs_a_reply(
    comments, project, monkeypatch
) -> None:
    """YouTube reports a DISLIKE as 'none' too, so the absence of a like is
    never evidence that anything was acknowledged."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(_comment("c1", viewer_rating="none"), video_id=None),
            _thread(_comment("c2"), video_id=None),
        ],
    )
    await comments.sync_project_comments(project)

    threads = await comments.list_recent_threads(1, limit=10)

    assert all(t["awaiting_owner_reply"] for t in threads)
    assert not any(t["owner_liked_last_word"] for t in threads)


async def test_liking_an_older_comment_does_not_settle_a_newer_one(
    comments, project, monkeypatch
) -> None:
    """The like has to be on the LAST word. Liking the question and then being
    asked a follow-up leaves the thread open."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1", published="2026-08-01T10:00:00Z", viewer_rating="like"),
                video_id=None,
                replies=[_comment("r1", published="2026-08-02T10:00:00Z")],
            )
        ],
    )
    await comments.sync_project_comments(project)

    thread = (await comments.list_recent_threads(1, limit=10))[0]

    assert thread["awaiting_owner_reply"] is True
    assert thread["owner_liked_last_word"] is False


async def test_a_call_that_reports_no_rating_never_erases_a_known_like(
    comments, project, isolated_db, monkeypatch
) -> None:
    """Same hazard as moderation_status: the reply follow-up may report no
    rating at all, and letting that overwrite would drop the like every sweep."""
    liked = _comment("r1", viewer_rating="like")
    unrated = _comment("r1")
    _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), video_id=None, replies=[liked],
                         total_reply_count=3)],
        replies={"c1": [unrated, _comment("r2"), _comment("r3")]},
    )

    await comments.sync_project_comments(project)

    rows = await isolated_db.execute_fetchall(
        "SELECT viewer_rating FROM youtube_comments WHERE comment_id = 'r1'"
    )
    assert rows[0]["viewer_rating"] == "like"


# --- the sweep records what happened -----------------------------------------


async def test_a_successful_sweep_records_its_outcome(
    comments, project, monkeypatch
) -> None:
    _install_fake_youtube(monkeypatch, threads=[_thread(_comment("c1"), video_id=None)])

    await comments.sync_project_comments(project)

    run = await comments.last_sweep_run(1)
    assert run["ok"] is True
    assert run["was_complete"] is True
    assert run["error"] is None
    assert run["finished_at"] is not None
    assert run["detail"]["new"] == 1


async def test_a_sweep_that_raises_is_recorded_before_it_propagates(
    comments, project, monkeypatch
) -> None:
    """The sweep's usual caller is a 4-hourly job with no page open, so a
    failure that only raises reaches nobody. It must be on disk before it
    leaves this function."""
    _install_fake_youtube(
        monkeypatch, threads=[], bucket_errors={"published": RuntimeError("token revoked")}
    )

    with pytest.raises(RuntimeError):
        await comments.sync_project_comments(project)

    run = await comments.last_sweep_run(1)
    assert run["ok"] is False
    assert "token revoked" in run["error"]
    # A sweep that died has no finish time and no summary to itemise.
    assert run["finished_at"] is None
    assert run["detail"] is None


async def test_a_bucket_failure_is_recorded_as_an_incomplete_sweep(
    comments, project, monkeypatch
) -> None:
    _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), video_id=None)],
        bucket_errors={"likelySpam": RuntimeError("403 forbidden")},
    )

    await comments.sync_project_comments(project)

    run = await comments.last_sweep_run(1)
    assert run["ok"] is True, "the sweep itself finished"
    assert run["was_complete"] is False, "but it did not see everything"
    assert run["detail"]["moderation_buckets"]["likelySpam"]["ok"] is False


async def test_one_unreadable_reply_thread_does_not_cost_the_sweep(
    comments, project, isolated_db, monkeypatch
) -> None:
    """The threads are already stored by the time replies are fetched; aborting
    would throw away a good sweep over one bad follow-up. It does mean we did
    not see everything, so nothing may be called gone on the strength of it."""
    youtube = importlib.import_module("yt_scheduler.services.youtube")
    _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), video_id=None, replies=[_comment("r1")],
                         total_reply_count=4)],
    )

    def boom(parent_comment_id, *, max_pages):
        raise RuntimeError("quotaExceeded")

    monkeypatch.setattr(youtube, "list_comment_replies", boom)

    summary = await comments.sync_project_comments(project)

    assert summary["reply_fetch_errors"][0]["parent_comment_id"] == "c1"
    assert "quotaExceeded" in summary["reply_fetch_errors"][0]["error"]
    assert summary["sweep_was_complete"] is False
    assert summary["swept_at"] is None
    rows = await isolated_db.execute_fetchall(
        "SELECT comment_id FROM youtube_comments ORDER BY comment_id"
    )
    assert [r["comment_id"] for r in rows] == ["c1", "r1"], "the sweep still stored"


async def test_an_empty_sweep_over_a_stocked_mirror_is_refused_not_believed(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """Believing it would mark every stored comment "gone from YouTube" in one
    tick. A wiped channel and a broken call that answers empty look identical
    from here, so the inference is declined and the reason recorded."""
    _install_fake_youtube(monkeypatch, threads=[_thread(_comment("c1"), video_id=None)])
    await comments.sync_project_comments(project)
    sweep_clock()

    _install_fake_youtube(monkeypatch, threads=[])
    summary = await comments.sync_project_comments(project)

    assert summary["suspicious_empty_sweep"] is True
    assert summary["sweep_was_complete"] is False
    thread = (await comments.list_recent_threads(1, limit=10))[0]
    assert thread["top_level_comment"]["is_missing_from_youtube"] is False
    run = await comments.last_sweep_run(1)
    assert run["was_complete"] is False


async def test_a_mass_disappearance_is_declined_not_believed(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The empty-sweep guard is this same check with its threshold at 100%. A
    video flipped back to unlisted — a state this app's own publish workflow
    reaches — takes all its comments out of the listing at once, and believing
    that would condemn every one of them AND stop their threads asking for a
    reply."""
    many = [_thread(_comment(f"c{i}"), video_id=None) for i in range(20)]
    _install_fake_youtube(monkeypatch, threads=many)
    await comments.sync_project_comments(project)

    # Two further complete sweeps, each missing 15 of the 20 — enough strikes to
    # condemn them, if the guard did not decline first.
    for _ in range(2):
        sweep_clock()
        _install_fake_youtube(monkeypatch, threads=many[:5])
        summary = await comments.sync_project_comments(project)

    assert summary["mass_disappearance"] == {"absent": 15, "of": 20}
    assert summary["sweep_was_complete"] is False
    assert await _flagged_gone(comments) == set()


async def test_a_small_disappearance_is_still_believed(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The guard must not swallow ordinary removals — that is the whole feature.
    Both an absolute floor and a fraction must be exceeded before it declines."""
    many = [_thread(_comment(f"c{i}"), video_id=None) for i in range(20)]
    _install_fake_youtube(monkeypatch, threads=many)
    await comments.sync_project_comments(project)

    for _ in range(2):
        sweep_clock()
        _install_fake_youtube(monkeypatch, threads=many[:18])
        summary = await comments.sync_project_comments(project)

    assert summary["mass_disappearance"] is None
    assert summary["sweep_was_complete"] is True
    assert await _flagged_gone(comments) == {"c18", "c19"}


async def test_an_empty_sweep_over_an_empty_mirror_is_not_suspicious(
    comments, project, monkeypatch
) -> None:
    """A channel with no comments is an ordinary, complete sweep — the guard
    must not turn "nothing to see" into a permanent warning."""
    _install_fake_youtube(monkeypatch, threads=[])

    summary = await comments.sync_project_comments(project)

    assert summary["suspicious_empty_sweep"] is False
    assert summary["sweep_was_complete"] is True


async def test_no_sweep_recorded_is_none_not_a_clean_bill_of_health(
    comments,
) -> None:
    assert await comments.last_sweep_run(1) is None


async def test_a_second_sweep_is_refused_while_one_is_running(
    comments, project, monkeypatch
) -> None:
    """Two concurrent sweeps stamp two different times, and the one that
    FINISHES last writes the newer stamp over the older comment set — every
    comment that arrived in between then reads as gone from YouTube."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_sweep(_project):
        started.set()
        await release.wait()
        return {"sweep_was_complete": False, "swept_at": None,
                "previous_swept_at": None}

    monkeypatch.setattr(comments, "_sweep_project_comments", blocking_sweep)

    first = asyncio.create_task(comments.sync_project_comments(project))
    await started.wait()
    try:
        with pytest.raises(comments.SweepAlreadyRunning):
            await comments.sync_project_comments(project)
    finally:
        release.set()
        await first

    # And the lock is released, so the next sweep is allowed.
    _install_fake_youtube(monkeypatch, threads=[])
    monkeypatch.undo()


async def test_concurrent_sync_is_a_409(app_client, monkeypatch) -> None:
    """The route says which kind of refusal this is — a conflict with work
    already in flight, not a bad request."""
    module = importlib.import_module("yt_scheduler.services.comments")

    async def busy(_project):
        raise module.SweepAlreadyRunning("already sweeping")

    monkeypatch.setattr(module, "sync_project_comments", busy)

    resp = await app_client.post("/api/projects/default/comments/sync")

    assert resp.status_code == 409
    assert "already sweeping" in resp.json()["detail"]


async def test_supplementary_buckets_get_their_own_smaller_cap(
    comments, project, monkeypatch
) -> None:
    """There is no cursor — every sweep re-reads each bucket from page one — so
    a large permanent spam backlog would bill the primary's full cap forever."""
    config = importlib.import_module("yt_scheduler.config")
    calls = _install_fake_youtube(monkeypatch, threads=[])

    await comments.sync_project_comments(project)

    caps = calls["max_pages_by_status"]
    assert caps["published"] == config.COMMENT_SYNC_MAX_PAGES
    assert caps["heldForReview"] == config.COMMENT_SYNC_MAX_PAGES_PER_MODERATION_BUCKET
    assert caps["likelySpam"] == config.COMMENT_SYNC_MAX_PAGES_PER_MODERATION_BUCKET
    assert caps["heldForReview"] < caps["published"]


async def test_a_truncated_supplementary_bucket_makes_the_sweep_incomplete(
    comments, project, monkeypatch
) -> None:
    """A bucket that stopped at its cap left held comments unread, so nothing
    may be concluded about what is missing."""
    _install_fake_youtube(
        monkeypatch,
        threads=[_thread(_comment("c1"), video_id=None)],
        buckets={"likelySpam": [_thread(_comment("s1"), video_id=None)]},
        bucket_hit_caps={"likelySpam": True},
    )

    summary = await comments.sync_project_comments(project)

    assert summary["moderation_buckets"]["likelySpam"]["pages_truncated"] is True
    assert summary["sweep_was_complete"] is False
    assert summary["swept_at"] is None


async def test_a_sweep_that_read_no_supplementary_bucket_is_not_complete(
    comments, project, monkeypatch
) -> None:
    """`all()` over an empty dict is True. Without an explicit coverage check a
    sweep that read no held comments at all would claim completeness — and then
    condemn every one of them."""
    monkeypatch.setattr(comments, "_SUPPLEMENTARY_BUCKETS", ("heldForReview", "extra"))
    _install_fake_youtube(monkeypatch, threads=[_thread(_comment("c1"), video_id=None)])

    summary = await comments.sync_project_comments(project)

    assert "extra" not in summary["moderation_buckets"]
    assert summary["sweep_was_complete"] is False


# --- disappearance (report / author delete) ----------------------------------


@pytest.fixture
def sweep_clock(comments, monkeypatch):
    """Give a test explicit control of the sweep stamp clock.

    Stamps are one-second resolution, so several sweeps inside one test would
    otherwise share a stamp — and the watermark reasons over DISTINCT stamps.
    Driving the clock is also the only way to test the two-strike rule, which is
    defined in sweeps rather than in time.

    Returns a callable that advances the clock; call it between sweeps.
    """
    state = {"now": datetime(2026, 8, 1, tzinfo=timezone.utc)}
    monkeypatch.setattr(
        comments, "_utc_now", lambda: state["now"].strftime("%Y-%m-%d %H:%M:%S")
    )

    def advance(seconds: int = 60) -> None:
        state["now"] += timedelta(seconds=seconds)

    return advance


async def _flagged_gone(comments) -> set[str]:
    threads = await comments.list_recent_threads(1, limit=10)
    return {
        t["thread_key"] for t in threads
        if t["top_level_comment"] and t["top_level_comment"]["is_missing_from_youtube"]
    }


async def test_one_missed_sweep_is_not_enough_to_call_a_comment_gone(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The three moderation buckets are read minutes apart and pagination can
    shift under a new arrival, so a single miss is not proof — it is routinely
    transient. Two strikes; the cost is reporting a real removal one sweep late.
    """
    both = [_thread(_comment("stays"), video_id=None),
            _thread(_comment("reported"), video_id=None)]
    _install_fake_youtube(monkeypatch, threads=both)
    assert (await comments.sync_project_comments(project))["sweep_was_complete"]
    sweep_clock()

    _install_fake_youtube(monkeypatch, threads=[both[0]])
    await comments.sync_project_comments(project)

    assert await _flagged_gone(comments) == set()


async def test_a_comment_missed_by_two_sweeps_is_flagged_gone(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """Reporting a comment in Studio, or its author deleting it, hides it from
    the channel without putting it in ANY listable bucket — it just stops being
    returned. The mirror never deletes, so it would otherwise sit on the
    dashboard looking live forever."""
    both = [_thread(_comment("stays"), video_id=None),
            _thread(_comment("reported"), video_id=None)]
    _install_fake_youtube(monkeypatch, threads=both)
    await comments.sync_project_comments(project)

    for _ in range(2):
        sweep_clock()
        _install_fake_youtube(monkeypatch, threads=[both[0]])
        await comments.sync_project_comments(project)

    assert await _flagged_gone(comments) == {"reported"}


async def test_a_comment_that_comes_back_is_not_flagged(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """A transient miss must clear itself, not accumulate toward a verdict."""
    both = [_thread(_comment("a"), video_id=None), _thread(_comment("b"), video_id=None)]
    _install_fake_youtube(monkeypatch, threads=both)
    await comments.sync_project_comments(project)

    sweep_clock()
    _install_fake_youtube(monkeypatch, threads=[both[0]])
    await comments.sync_project_comments(project)

    sweep_clock()
    _install_fake_youtube(monkeypatch, threads=both)
    await comments.sync_project_comments(project)

    assert await _flagged_gone(comments) == set()


async def test_a_backwards_clock_cannot_invert_the_watermark(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The stamp is a sweep ORDINAL spelled as a time. An NTP correction or a VM
    restore that steps the clock back would otherwise make the newer sweep write
    a SMALLER value, condemning exactly the comments just confirmed alive."""
    threads = [_thread(_comment("c1"), video_id=None)]
    _install_fake_youtube(monkeypatch, threads=threads)
    await comments.sync_project_comments(project)
    first = await comments.newest_complete_sweep_at(1)

    # Clock jumps backwards a full day between sweeps.
    sweep_clock(-86400)
    _install_fake_youtube(monkeypatch, threads=threads)
    await comments.sync_project_comments(project)

    second = await comments.newest_complete_sweep_at(1)
    assert second > first, "the sweep stamp went backwards"
    assert await _flagged_gone(comments) == set()


async def test_replies_of_a_fully_stored_thread_are_never_called_gone(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """The regression that nearly shipped.

    A thread resource carries only a PREVIEW of its replies, and the follow-up
    is gated on stored-count < totalReplyCount — so once a thread is fully
    stored, later sweeps see the preview and nothing else. Treating "not in this
    sweep" as "gone from YouTube" would therefore condemn every reply past the
    preview of every busy thread, on the very next tick, forever.
    """
    preview = [_comment("r1"), _comment("r2")]
    full = preview + [_comment(f"r{n}") for n in range(3, 7)]
    threads = [_thread(_comment("c1"), video_id=None, replies=preview,
                       total_reply_count=6)]

    _install_fake_youtube(monkeypatch, threads=threads, replies={"c1": full})
    first = await comments.sync_project_comments(project)
    assert first["reply_fetches"] == 1, "the follow-up must have run"
    assert first["sweep_was_complete"] is True
    sweep_clock()

    # Second sweep: the thread is fully stored, so no follow-up runs and only
    # the preview replies come back.
    calls = _install_fake_youtube(monkeypatch, threads=threads, replies={"c1": full})
    second = await comments.sync_project_comments(project)
    assert calls["reply_parents"] == [], "a fully-stored thread was re-fetched"
    assert second["sweep_was_complete"] is True

    thread = (await comments.list_recent_threads(1, limit=10))[0]
    condemned = [r["comment_id"] for r in thread["replies"]
                 if r["is_missing_from_youtube"]]
    assert condemned == [], (
        f"replies {condemned} were marked gone from YouTube purely for being "
        f"past the thread preview"
    )


async def test_a_truncated_sweep_never_calls_anything_gone(
    comments, project, sweep_clock, monkeypatch
) -> None:
    """A comment absent from a PARTIAL sweep may simply be in the part we did
    not read. Inferring removal from it would invent a fact."""
    both = [_thread(_comment("a"), video_id=None), _thread(_comment("b"), video_id=None)]
    _install_fake_youtube(monkeypatch, threads=both)
    await comments.sync_project_comments(project)
    sweep_clock()

    _install_fake_youtube(monkeypatch, threads=[both[0]], hit_cap=True)
    summary = await comments.sync_project_comments(project)

    assert summary["sweep_was_complete"] is False
    assert summary["swept_at"] is None
    threads = await comments.list_recent_threads(1, limit=10)
    assert not any(t["top_level_comment"]["is_missing_from_youtube"] for t in threads)


async def test_nothing_is_missing_before_a_complete_sweep_has_ever_run(
    comments, project, monkeypatch
) -> None:
    """NULL is 'never covered by a complete sweep', which must read as unknown
    rather than as gone."""
    _install_fake_youtube(
        monkeypatch, threads=[_thread(_comment("c1"), video_id=None)], hit_cap=True
    )
    await comments.sync_project_comments(project)

    assert await comments.newest_complete_sweep_at(1) is None
    thread = (await comments.list_recent_threads(1, limit=10))[0]
    assert thread["top_level_comment"]["is_missing_from_youtube"] is False


async def test_needs_reply_ignores_comments_viewers_cannot_see(
    comments, project, monkeypatch
) -> None:
    """A held or likely-spam comment creates no obligation to answer — and must
    not mask the genuine question underneath it."""
    spam_reply = _comment("r2", text="buy watches", published="2026-08-03T10:00:00Z")
    spam_reply["snippet"]["moderationStatus"] = "likelySpam"
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1", published="2026-08-01T10:00:00Z"),
                video_id=None,
                replies=[
                    _comment("r1", author="Drew", author_channel=CHANNEL_ID,
                             published="2026-08-02T10:00:00Z"),
                    spam_reply,
                ],
            )
        ],
    )
    await comments.sync_project_comments(project)

    thread = (await comments.list_recent_threads(1, limit=10))[0]

    # Newest comment overall is the spam one; newest VISIBLE one is your reply.
    assert thread["replies"][-1]["comment_id"] == "r2"
    assert thread["replies"][-1]["moderation_status"] == "likelySpam"
    assert thread["awaiting_owner_reply"] is False


# --- read path ---------------------------------------------------------------


async def test_reply_stays_with_the_comment_it_answers(
    comments, project, monkeypatch
) -> None:
    """The bug this grouping exists to kill: sorted flat by time, a reply lands
    rows away from the comment it answers and the thread reads as unanswered."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1", published="2026-06-14T15:33:00Z"),
                replies=[
                    _comment(
                        "r1",
                        author="Drew",
                        author_channel=CHANNEL_ID,
                        published="2026-06-16T06:26:00Z",
                    )
                ],
            ),
            # Published between the two above, so a flat listing would sit
            # right between the question and its answer.
            _thread(_comment("c2", published="2026-06-15T01:18:00Z"), video_id=None),
        ],
    )
    await comments.sync_project_comments(project)

    threads = await comments.list_recent_threads(1, limit=10)

    assert [t["thread_key"] for t in threads] == ["c1", "c2"]
    answered, unanswered = threads
    assert answered["top_level_comment"]["comment_id"] == "c1"
    assert [r["comment_id"] for r in answered["replies"]] == ["r1"]
    assert answered["owner_has_replied"] is True
    assert answered["awaiting_owner_reply"] is False
    assert unanswered["owner_has_replied"] is False
    assert unanswered["awaiting_owner_reply"] is True


async def test_thread_carries_the_video_and_owner_flags(
    comments, project, monkeypatch
) -> None:
    """Every comment in a thread is on the same video by construction, so the
    video is named once on the thread rather than on every row."""
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
            _thread(_comment("c2", published="2026-08-02T09:00:00Z"), video_id=None),
        ],
    )
    await comments.sync_project_comments(project)

    threads = await comments.list_recent_threads(1, limit=10)
    channel_level, on_video = threads

    # No videoId, so no video to name — and no local page to link to.
    assert channel_level["youtube_video_id"] is None
    assert channel_level["local_video_id"] is None
    assert on_video["local_video_id"] == "vid00000001"
    assert on_video["video_title"] == "Ep 42 — Relay"
    assert on_video["episode_number"] == 42
    assert on_video["top_level_comment"]["is_channel_owner"] is False
    assert on_video["top_level_comment"]["is_reply"] is False
    assert on_video["replies"][0]["is_channel_owner"] is True
    assert on_video["replies"][0]["is_reply"] is True


async def test_a_new_reply_floats_an_old_thread_up(
    comments, project, monkeypatch
) -> None:
    """Threads order by newest activity, not by when the thread started —
    otherwise today's reply to a months-old video is unreachable."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("old", published="2026-04-01T00:00:00Z"),
                video_id=None,
                replies=[_comment("old-r", published="2026-08-03T01:00:00Z")],
            ),
            _thread(_comment("newer", published="2026-07-01T00:00:00Z"), video_id=None),
        ],
    )
    await comments.sync_project_comments(project)

    threads = await comments.list_recent_threads(1, limit=10)

    assert [t["thread_key"] for t in threads] == ["old", "newer"]
    assert threads[0]["last_activity_at"] == "2026-08-03T01:00:00Z"


async def test_a_reply_to_your_reply_puts_the_ball_back_in_your_court(
    comments, project, monkeypatch
) -> None:
    """`owner_has_replied` and `awaiting_owner_reply` are different facts. You
    answered, they answered back — the thread is not handled."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1", published="2026-08-02T06:23:00Z"),
                video_id=None,
                replies=[
                    _comment(
                        "r1",
                        author="Drew",
                        author_channel=CHANNEL_ID,
                        published="2026-08-03T01:14:00Z",
                    ),
                    _comment("r2", published="2026-08-03T01:24:00Z"),
                ],
            )
        ],
    )
    await comments.sync_project_comments(project)

    thread = (await comments.list_recent_threads(1, limit=10))[0]

    # Chronological within the thread, matching YouTube.
    assert [r["comment_id"] for r in thread["replies"]] == ["r1", "r2"]
    assert thread["owner_has_replied"] is True
    assert thread["awaiting_owner_reply"] is True


async def test_thread_reports_replies_it_does_not_hold_yet(
    comments, project, monkeypatch
) -> None:
    """"Showing 1 of 4" is a fact the reader needs to trust the thread; a
    truncated thread rendered as complete is the silent-wrong-answer case."""
    config = importlib.import_module("yt_scheduler.config")
    monkeypatch.setattr(config, "COMMENT_SYNC_MAX_REPLY_FETCHES", 0)
    monkeypatch.setattr(comments, "COMMENT_SYNC_MAX_REPLY_FETCHES", 0)
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("c1"),
                video_id=None,
                replies=[_comment("r1")],
                total_reply_count=4,
            )
        ],
    )
    await comments.sync_project_comments(project)

    thread = (await comments.list_recent_threads(1, limit=10))[0]

    assert len(thread["replies"]) == 1
    assert thread["total_reply_count"] == 4


async def test_paging_walks_whole_threads_without_repeats_or_splits(
    comments, project, monkeypatch
) -> None:
    """Paging is by thread: a page boundary inside a conversation would hide
    the reply on page 2 from the comment it answers on page 1."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment(f"c{i}", published=f"2026-08-{i + 1:02d}T10:00:00Z"),
                video_id=None,
                replies=[
                    _comment(f"c{i}r1", published=f"2026-08-{i + 1:02d}T11:00:00Z")
                ],
            )
            for i in range(5)
        ],
    )
    await comments.sync_project_comments(project)

    pages = [
        await comments.list_recent_threads(1, limit=2, offset=n) for n in (0, 2, 4)
    ]
    walked = [t for page in pages for t in page]

    assert [t["thread_key"] for t in walked] == ["c4", "c3", "c2", "c1", "c0"]
    # Every thread arrives whole, on exactly one page.
    for thread in walked:
        key = thread["thread_key"]
        assert thread["top_level_comment"]["comment_id"] == key
        assert [r["comment_id"] for r in thread["replies"]] == [f"{key}r1"]
    # 10 comments, 5 conversations — the count is the paging unit, or
    # "Showing N of M" promises pages Load more can never reach.
    assert await comments.count_threads(1) == 5


async def test_blocklisted_comments_are_hidden_but_failed_rejections_are_flagged(
    comments, project, isolated_db, monkeypatch
) -> None:
    """YouTube keeps returning comments the blocklist already rejected, so the
    sweep re-stores them every tick. Showing them would hand the user back the
    exact spam moderation exists to remove — but only where the rejection
    actually succeeded: `error` means the comment is still live, and it must say
    so rather than rendering as an ordinary comment."""
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
        "INSERT INTO moderation_log "
        "(project_id, video_id, comment_id, matched_keyword, action) VALUES "
        "(1, 'v', 'spam1', 'watches', 'deleted'), (1, 'v', 'spam2', 'spam', 'error')"
    )
    await isolated_db.commit()

    threads = await comments.list_recent_threads(1, limit=10)

    keys = [t["thread_key"] for t in threads]
    assert "spam1" not in keys, "a rejected comment came back on the dashboard"
    assert "spam2" in keys, "a FAILED rejection was hidden — that comment is live"
    assert "real1" in keys
    still_live = next(t for t in threads if t["thread_key"] == "spam2")["top_level_comment"]
    assert still_live["moderation_action"] == "error"
    assert still_live["moderation_matched_keyword"] == "spam"
    clean = next(t for t in threads if t["thread_key"] == "real1")["top_level_comment"]
    assert clean["moderation_action"] is None
    # The count must match what the list can show, or 'Showing N of M' promises
    # threads Load more can never reach.
    assert await comments.count_threads(1) == len(threads) == 2


async def test_reply_whose_parent_was_blocklisted_is_shown_under_a_stated_gap(
    comments, project, isolated_db, monkeypatch
) -> None:
    """Rejecting a top-level comment does not reject its replies, so a real
    reply can outlive its parent. Dropping it would lose a real comment;
    promoting it to top-level would present an answer as the question."""
    _install_fake_youtube(
        monkeypatch,
        threads=[
            _thread(
                _comment("spam1", text="buy cheap watches"),
                video_id=None,
                replies=[_comment("r1", text="reported, this is spam")],
            )
        ],
    )
    await comments.sync_project_comments(project)
    await isolated_db.execute(
        "INSERT INTO moderation_log (project_id, video_id, comment_id, action) "
        "VALUES (1, 'v', 'spam1', 'deleted')"
    )
    await isolated_db.commit()

    threads = await comments.list_recent_threads(1, limit=10)

    assert len(threads) == 1
    orphaned = threads[0]
    assert orphaned["thread_key"] == "spam1"
    assert orphaned["top_level_comment"] is None
    assert orphaned["parent_unavailable"] is True
    assert [r["comment_id"] for r in orphaned["replies"]] == ["r1"]
    # An orphan thread counts once, exactly as it renders.
    assert await comments.count_threads(1) == 1


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
    assert body["threads"] == []
    assert body["total_threads"] == 0
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


def test_dashboard_reads_the_thread_contract_not_the_old_flat_one() -> None:
    """A JS read of a field the server no longer sends is silent: `data.total`
    becomes `undefined`, the empty-state check fails open, and the section
    renders nothing with no error anywhere. Pin the field names."""
    dashboard = (
        Path(__file__).resolve().parents[1]
        / "src/yt_scheduler/templates_html/dashboard.html"
    ).read_text()

    assert "data.threads" in dashboard
    assert "data.total_threads" in dashboard
    for removed in ("data.comments", "data.total ", "data.total)", "data.total;"):
        assert removed not in dashboard, (
            f"dashboard still reads {removed!r}, which the threads endpoint no "
            f"longer returns — it would read as undefined, not as an error"
        )


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
