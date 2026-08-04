"""Is a failed social send safe to retry automatically?

The only honest answer comes from the exception TYPE, and it is a narrower
question than "was this transient".

A transient failure splits in two, and conflating them is how an app spams
someone's followers:

* **Connect phase** — DNS lookup failed, connection refused, connection reset
  before the request was written. The request provably never reached the
  platform, so re-sending cannot duplicate anything. Safe.
* **Ambiguous** — a read timeout, a write timeout, a connection dropped
  mid-flight. The request may have been delivered in full and only the response
  lost, in which case the post EXISTS and a retry mints a second one. Just as
  transient, and never safe to retry blind.

`ThreadsPoster._PUBLISH_AMBIGUOUS_TRANSPORT_ERRORS` already draws this line for
one platform's publish call, with a resolve loop to settle the ambiguity. This
module generalises the classification for every platform, for the automatic
retry path that has no such loop to fall back on.

Everything else — auth, validation, a refusal because the video is not public —
is not transient at all. Retrying those burns quota, delays the real message,
and in the auth case can get an account flagged for repeated failed attempts.

Deliberately NOT based on the stored error string. That text is written for a
human and gets reworded; matching on it would be a silent behaviour change every
time someone improves a message.
"""

from __future__ import annotations

import errno
import logging
import socket
from datetime import datetime, timedelta, timezone

import httpx

#: Backoff between automatic attempts. Deliberately not a fixed interval: a
#: 24-hour window at a 15-minute tick is ~96 attempts against something that may
#: be permanently broken, and platforms rate-limit repeat offenders. The last
#: entry repeats for any further attempts.
RETRY_BACKOFF = (
    timedelta(minutes=2),
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(minutes=45),
    timedelta(hours=2),
    timedelta(hours=6),
)

#: How long automatic retrying may continue when nothing more specific applies.
#: A smart-queue post uses its queue's ``missed_grace_hours`` instead — the
#: question "is it still worth posting this late?" is editorial, and the queue is
#: where the user already answered it.
DEFAULT_RETRY_WINDOW = timedelta(hours=24)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(when: datetime) -> str:
    """The one timestamp format the DB and `static/js/datetime.js` agree on."""
    return when.strftime(_TIMESTAMP_FORMAT)


def next_attempt_at(retry_count: int) -> str:
    """When attempt number ``retry_count`` (0-based, already made) should retry."""
    index = min(max(retry_count, 0), len(RETRY_BACKOFF) - 1)
    return _stamp(_now() + RETRY_BACKOFF[index])


def retry_deadline(window: timedelta | None = None) -> str:
    """The far edge of the retry window, measured from now.

    Stamped once on the first failure and never recomputed — recomputing on each
    attempt would push the deadline ahead of every retry and the loop would never
    end.
    """
    return _stamp(_now() + (window or DEFAULT_RETRY_WINDOW))

#: The request never left. Re-sending cannot duplicate.
#:
#: ``ConnectTimeout`` is included and must be tested before the broader timeout
#: classes below, because it subclasses ``TimeoutException`` — the same trap
#: documented on the Threads publish path.
_CONNECT_PHASE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    httpx.ConnectError,      # DNS failure, refused, unreachable
    httpx.ConnectTimeout,    # never established
    httpx.PoolTimeout,       # never left our own connection pool
    socket.gaierror,         # raw getaddrinfo failure (errno 8 and friends)
)

#: The request may have arrived. NEVER retry blind — a second send would be a
#: second post. Listed explicitly rather than left to the catch-all so the
#: distinction is stated where someone editing this will read it.
_AMBIGUOUS_EXCEPTIONS: tuple[type[BaseException], ...] = (
    httpx.ReadTimeout,          # request sent in full; response never arrived
    httpx.ReadError,
    httpx.WriteTimeout,         # unknowable how much of the request was written
    httpx.WriteError,
    httpx.RemoteProtocolError,  # server dropped the connection without replying
)

#: OSError codes that mean the connection was never established. Checked because
#: several platform SDKs wrap sockets themselves and surface a bare OSError
#: rather than anything httpx-shaped — the `[Errno 8] nodename nor servname
#: provided` failures come through this way.
_CONNECT_PHASE_ERRNOS = frozenset({
    errno.ENOENT,        # 2  — surfaces from getaddrinfo on some platforms
    errno.ECONNREFUSED,  # 61 — nothing listening
    errno.ENETDOWN,
    errno.ENETUNREACH,
    errno.EHOSTDOWN,
    errno.EHOSTUNREACH,
    getattr(errno, "EAI_NONAME", 8),  # 8 — nodename nor servname provided
})


#: The answer whenever we cannot establish that retrying is safe.
_NO_RETRY = {"retryable": False, "next_retry_at": None, "retry_until": None}


async def retry_plan(post_id: int, exc: BaseException) -> dict:
    """What the retry columns should say after ``exc`` killed this send.

    Returns kwargs for :func:`models.social_post.mark_failed`. Call it only
    where a real exception is in hand: a precondition refusal (the video is not
    public, the credential needs re-auth) passes a reason string instead and
    must keep the non-retryable default — re-sending changes nothing until the
    user acts, so retrying it is pure noise.

    The window comes from the post's smart queue when it has one, because
    "is this still worth posting late?" is an editorial question the user
    already answered there. Everything else gets the default.
    """
    if not is_safe_to_retry(exc):
        return _NO_RETRY

    # Imported here: this module is deliberately importable without touching the
    # database, so the classification can be unit-tested on its own.
    from yt_scheduler.database import get_db

    try:
        db = await get_db()
        rows = await db.execute_fetchall(
            """SELECT p.retry_count, q.missed_grace_hours, q.missed_policy
                 FROM social_posts p
                 LEFT JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
                 LEFT JOIN smart_queues q ON q.id = i.queue_id
                WHERE p.id = ?""",
            (post_id,),
        )
    except Exception:
        # This runs INSIDE an except handler for the real send failure. Raising
        # here would replace the error the user needs to read with one about our
        # own bookkeeping, and would abort the mark_failed that records it at
        # all. Not retrying is always the safe answer.
        logger.exception(
            "Could not build a retry plan for post %s; treating it as not "
            "retryable.", post_id,
        )
        return _NO_RETRY
    if not rows:
        return _NO_RETRY

    row = rows[0]
    window = None
    # Only post_late means the user wants it to go out late at all; under the
    # other policies a late send is not something to keep attempting.
    if row["missed_policy"] == "post_late" and row["missed_grace_hours"]:
        window = timedelta(hours=int(row["missed_grace_hours"]))

    return {
        "retryable": True,
        "next_retry_at": next_attempt_at(int(row["retry_count"] or 0)),
        "retry_until": retry_deadline(window),
    }


def is_safe_to_retry(exc: BaseException) -> bool:
    """Whether re-sending after ``exc`` is guaranteed not to duplicate a post.

    False for anything unrecognised. The default has to be "do not retry": a
    wrong False costs one manual click, a wrong True posts twice to a real
    audience and cannot be undone.
    """
    # Ambiguity wins over everything. Some ambiguous classes share a base with
    # connect-phase ones, so testing this first is what keeps ConnectTimeout
    # from dragging ReadTimeout in behind it.
    if isinstance(exc, _AMBIGUOUS_EXCEPTIONS):
        return False
    if isinstance(exc, _CONNECT_PHASE_EXCEPTIONS):
        return True

    # A bare OSError from an SDK that does its own socket work. gaierror is an
    # OSError subclass, so it is already covered above; this catches the rest.
    if isinstance(exc, OSError) and exc.errno in _CONNECT_PHASE_ERRNOS:
        return True

    # A wrapped cause is common: SDKs re-raise their own error type with the
    # transport failure as __cause__. One level only — deeper than that and we
    # are guessing about a chain we do not understand.
    cause = exc.__cause__ or exc.__context__
    if cause is not None and cause is not exc:
        if isinstance(cause, _AMBIGUOUS_EXCEPTIONS):
            return False
        if isinstance(cause, _CONNECT_PHASE_EXCEPTIONS):
            return True
        if isinstance(cause, OSError) and cause.errno in _CONNECT_PHASE_ERRNOS:
            return True

    return False
