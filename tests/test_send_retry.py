"""Automatic retry of failed social sends.

The whole feature is one judgement — "can this be sent again without posting
twice?" — so that is what these pin. A wrong answer in the safe direction costs
one manual click; a wrong answer in the other direction puts a second post in
front of a real audience and cannot be undone.
"""

from __future__ import annotations

import errno
import importlib
import socket
import sys
from pathlib import Path

import httpx
import pytest

from tests.conftest import install_in_memory_keychain

send_failures = importlib.import_module("yt_scheduler.services.send_failures")


# --- classification ----------------------------------------------------------


@pytest.mark.parametrize("exc", [
    httpx.ConnectError("nodename nor servname provided"),
    httpx.ConnectTimeout("timed out connecting"),
    httpx.PoolTimeout("no free connection"),
    socket.gaierror(8, "nodename nor servname provided, or not known"),
    OSError(errno.ECONNREFUSED, "Connection refused"),
])
def test_connect_phase_failures_are_safe_to_retry(exc) -> None:
    """The request never reached the platform, so re-sending cannot duplicate.
    This is the class every one of the DNS failures fell into."""
    assert send_failures.is_safe_to_retry(exc) is True


@pytest.mark.parametrize("exc", [
    httpx.ReadTimeout("sent in full; no response"),
    httpx.ReadError("connection closed while reading"),
    httpx.WriteTimeout("unknown how much was written"),
    httpx.WriteError("broken pipe mid-request"),
    httpx.RemoteProtocolError("server disconnected without replying"),
])
def test_ambiguous_failures_are_never_retried(exc) -> None:
    """Just as transient, and the reason this feature needs care at all: the
    request may have been delivered and only the response lost, so the post can
    already exist. Retrying is how an app posts twice."""
    assert send_failures.is_safe_to_retry(exc) is False


def test_connect_timeout_does_not_drag_read_timeout_in_with_it() -> None:
    """ConnectTimeout subclasses TimeoutException, so a blanket timeout check
    would classify ReadTimeout as safe. That is the exact trap the Threads
    publish path documents; pin both sides of it."""
    assert issubclass(httpx.ConnectTimeout, httpx.TimeoutException)
    assert issubclass(httpx.ReadTimeout, httpx.TimeoutException)
    assert send_failures.is_safe_to_retry(httpx.ConnectTimeout("x")) is True
    assert send_failures.is_safe_to_retry(httpx.ReadTimeout("x")) is False


@pytest.mark.parametrize("exc", [
    RuntimeError("Threads container create failed: HTTP 500"),
    ValueError("content too long"),
    PermissionError("401 unauthorized"),
    Exception("YouTube video is still 'unlisted'"),
])
def test_everything_unrecognised_defaults_to_not_retrying(exc) -> None:
    """Unknown must mean no. A wrong 'no' costs a click; a wrong 'yes' posts
    twice. That includes plain 5xx: a server error says nothing about whether
    the request was acted on before it failed."""
    assert send_failures.is_safe_to_retry(exc) is False


def test_a_transport_failure_wrapped_by_an_sdk_is_still_seen() -> None:
    """Platform SDKs re-raise their own error type with the real cause attached;
    reading only the outer type would classify every one of them as unknown."""
    wrapped = RuntimeError("Bluesky client error")
    wrapped.__cause__ = httpx.ConnectError("dns")
    assert send_failures.is_safe_to_retry(wrapped) is True

    ambiguous = RuntimeError("Bluesky client error")
    ambiguous.__cause__ = httpx.ReadTimeout("no response")
    assert send_failures.is_safe_to_retry(ambiguous) is False


# --- backoff -----------------------------------------------------------------


def test_backoff_grows_and_then_holds() -> None:
    """A fixed interval over a 24-hour window is ~96 attempts at something that
    may be permanently broken, and platforms rate-limit repeat offenders."""
    steps = [send_failures.RETRY_BACKOFF[i] for i in range(len(send_failures.RETRY_BACKOFF))]
    assert steps == sorted(steps), "backoff must not shrink"
    # Past the end of the table the last delay repeats rather than resetting.
    beyond = send_failures.next_attempt_at(len(steps) + 5)
    last = send_failures.next_attempt_at(len(steps) - 1)
    assert beyond[:13] == last[:13]


# --- the job -----------------------------------------------------------------


@pytest.fixture
async def app_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)

    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    db = await database.get_db()
    await projects.ensure_default_project()
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidA', 1, 'My Video', 'published')"
    )
    await db.commit()
    yield scheduler, db
    await database.close_db()


async def _failed_post(db, post_id: int, **overrides) -> None:
    cols = {
        "id": post_id, "video_id": "'vidA'", "platform": "'bluesky'",
        "content": "'hello'", "status": "'failed'", "retryable": 1,
        "next_retry_at": "'2000-01-01 00:00:00'", "retry_until": "'2999-01-01 00:00:00'",
    }
    cols.update(overrides)
    await db.execute(
        f"INSERT INTO social_posts ({', '.join(cols)}) "
        f"VALUES ({', '.join(str(v) for v in cols.values())})"
    )
    await db.commit()


async def test_only_retryable_rows_that_are_due_are_picked_up(app_env, monkeypatch):
    scheduler, db = app_env
    await _failed_post(db, 1)                                    # due, retryable
    await _failed_post(db, 2, retryable=0)                       # not retryable
    await _failed_post(db, 3, next_retry_at="'2999-01-01 00:00:00'")  # not due yet
    await _failed_post(db, 4, retry_until="'2000-01-02 00:00:00'")    # window closed

    sent: list[int] = []

    async def fake_send(post_id, *, pre_claimed=False):
        sent.append(post_id)

    monkeypatch.setattr(scheduler, "_send_scheduled_post", fake_send)
    monkeypatch.setattr(
        scheduler, "_claim_failed_post_for_retry", lambda pid: _true()
    )

    await scheduler.retry_failed_sends_job()

    assert sent == [1]


async def _true() -> bool:
    return True


async def test_a_post_whose_content_already_went_out_is_never_retried(
    app_env, monkeypatch
):
    """The auto-add collision. A smart queue re-adding a video creates NEW posts
    with the same content, so a stale failure retrying afterwards is the second
    copy — and its own row cannot see that. Retiring it as skipped is what stops
    it looping on the retry path forever."""
    scheduler, db = app_env
    await _failed_post(db, 1)

    sent: list[int] = []

    async def fake_send(post_id, *, pre_claimed=False):
        sent.append(post_id)

    monkeypatch.setattr(scheduler, "_send_scheduled_post", fake_send)

    social = importlib.import_module("yt_scheduler.services.social")

    async def fake_dup(**kwargs):
        return {"id": 99}

    monkeypatch.setattr(social, "find_recent_duplicate_post", fake_dup)

    await scheduler.retry_failed_sends_job()

    assert sent == [], "retried a post whose content had already been sent"
    rows = await db.execute_fetchall(
        "SELECT status, retryable, next_retry_at, error FROM social_posts WHERE id = 1"
    )
    assert rows[0]["status"] == "skipped"
    assert not rows[0]["retryable"]
    assert rows[0]["next_retry_at"] is None
    assert "already posted" in rows[0]["error"]


async def test_losing_the_claim_means_not_sending(app_env, monkeypatch):
    """The job and a user pressing Retry can fire at the same instant. Both
    reading status='failed' would be two posts for one row."""
    scheduler, db = app_env
    await _failed_post(db, 1)

    sent: list[int] = []

    async def fake_send(post_id, *, pre_claimed=False):
        sent.append(post_id)

    async def lost_claim(post_id):
        return False

    monkeypatch.setattr(scheduler, "_send_scheduled_post", fake_send)
    monkeypatch.setattr(scheduler, "_claim_failed_post_for_retry", lost_claim)

    await scheduler.retry_failed_sends_job()

    assert sent == []


async def test_a_raise_mid_send_never_strands_the_row_we_claimed(
    app_env, monkeypatch
):
    """The claim moves the row out of 'failed' into 'sending'. If the send then
    raises somewhere that does not record a failure, the row is left 'sending' —
    which is invisible twice over: the banner lists only 'failed', and so does
    the retry query. It would sit there until the next restart.

    Whoever takes the claim owns releasing it."""
    scheduler, db = app_env
    await _failed_post(db, 1)

    async def explode(post_id, *, pre_claimed=False):
        raise RuntimeError("died between the claim and the failure handler")

    monkeypatch.setattr(scheduler, "_send_scheduled_post", explode)

    await scheduler.retry_failed_sends_job()

    rows = await db.execute_fetchall(
        "SELECT status, error FROM social_posts WHERE id = 1"
    )
    assert rows[0]["status"] != "sending", "row stranded in 'sending'"
    assert rows[0]["status"] == "failed"
    assert "died between the claim" in (rows[0]["error"] or "")


async def test_a_retryable_row_with_no_deadline_is_never_picked_up(app_env, monkeypatch):
    """retry_plan always stamps a deadline beside retryable=1, so a row without
    one is malformed. Reading a missing deadline as "no deadline" would retry it
    forever."""
    scheduler, db = app_env
    await _failed_post(db, 1, retry_until="NULL")

    sent: list[int] = []

    async def fake_send(post_id, *, pre_claimed=False):
        sent.append(post_id)

    monkeypatch.setattr(scheduler, "_send_scheduled_post", fake_send)

    await scheduler.retry_failed_sends_job()

    assert sent == []


async def test_a_broken_retry_plan_never_replaces_the_real_error(app_env, monkeypatch):
    """retry_plan runs inside the except handler for the send failure. If it
    raised, it would abort the mark_failed that records what actually went wrong
    and surface its own bookkeeping error instead."""
    database = importlib.import_module("yt_scheduler.database")

    async def broken_db():
        raise RuntimeError("database is on fire")

    monkeypatch.setattr(database, "get_db", broken_db)

    plan = await send_failures.retry_plan(1, httpx.ConnectError("dns"))

    assert plan == {"retryable": False, "next_retry_at": None, "retry_until": None}


async def test_the_claim_is_atomic(app_env):
    """Only one caller can move a row out of 'failed'."""
    scheduler, db = app_env
    await _failed_post(db, 1)

    assert await scheduler._claim_failed_post_for_retry(1) is True
    assert await scheduler._claim_failed_post_for_retry(1) is False


# --- the claim gaps found by the double-send audit ---------------------------


async def test_a_failed_post_can_be_claimed_by_a_deliberate_human_send(app_env):
    """The banner's Retry button posts to /send, and every row it lists is
    'failed'. A claim that only took 'approved' 409'd on every click, so Retry
    could never work at all."""
    scheduler, db = app_env
    await _failed_post(db, 1)

    assert await scheduler._claim_post_for_send(1, from_failed=True) is True

    rows = await db.execute_fetchall(
        "SELECT status, retryable, next_retry_at, retry_until, retry_count "
        "FROM social_posts WHERE id = 1"
    )
    assert rows[0]["status"] == "sending"
    # A deliberate manual attempt ends the current automatic run — the user has
    # taken over, so the backoff and deadline restart from this attempt.
    assert not rows[0]["retryable"]
    assert rows[0]["next_retry_at"] is None
    assert rows[0]["retry_until"] is None
    assert rows[0]["retry_count"] == 0


async def test_unattended_senders_still_refuse_a_failed_post(app_env):
    """A failed row failed for a reason nobody has looked at. Only a human may
    take it; the scheduler must not pick it up on its own."""
    scheduler, db = app_env
    await _failed_post(db, 1)

    assert await scheduler._claim_post_for_send(1) is False


async def test_a_claimed_row_that_moves_under_us_is_not_sent(app_env, monkeypatch):
    """pre_claimed skips BOTH the status guard and the claim, so without an
    ownership re-check the sender would post something the user just skipped."""
    scheduler, db = app_env
    await _failed_post(db, 1)
    # Simulate: we claimed it, then Skip landed before the re-read.
    await db.execute("UPDATE social_posts SET status = 'skipped' WHERE id = 1")
    await db.commit()

    sent: list[int] = []

    async def fake_poster(*a, **k):
        sent.append(1)

    monkeypatch.setattr(scheduler, "_claim_post_for_send", fake_poster)

    await scheduler._send_scheduled_post(1, pre_claimed=True)

    assert sent == [], "sent a post that had left our claim"
