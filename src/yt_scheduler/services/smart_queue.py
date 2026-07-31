"""Smart queues — project-wide social posting of promo clips.

A queue holds an ordered list of videos and a weekly recurrence. At each
recurrence slot it posts the next video to every enabled slot of its template,
attaching the video file itself.

The one rule that matters structurally: **eligibility is decided here and
nowhere else.** The config screen's Auto-select and the live-transition hook
both call :func:`is_eligible`, so they cannot drift into disagreeing about
which videos belong in a queue.

See SMART_QUEUE.md for the design.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.models.video import is_youtube_backed
from yt_scheduler.services.video_dimensions import ORIENTATION_SQL, orientation_of

logger = logging.getLogger(__name__)

ORIENTATIONS = ("portrait", "landscape", "square")
MISSED_POLICIES = ("post_late", "reschedule_end", "remove")

# In the queue, but with no posting time yet — what auto-add appends when a
# video goes live. Accept is what turns it into `scheduled`, so there is one
# place a posting time is decided rather than two that could disagree.
ITEM_STATE_QUEUED = "queued"

# States a queue item can be in. `queued` and `scheduled` are the pending ones;
# the rest are terminal for that occurrence, and a recycled video gets a NEW
# row rather than a state reset (see the migration).
ITEM_STATES = (
    ITEM_STATE_QUEUED, "scheduled", "posted", "failed", "skipped", "removed",
)

# States that mean "this video is already in this queue", so it must not be
# offered again as a fresh candidate. `posted` is added on top of these when
# the exclude-already-posted filter is on — that filter is what makes
# recycling work, so it stays separate.
PENDING_ITEM_STATES = (ITEM_STATE_QUEUED, "scheduled")

#: Has this occurrence gone out? Two encodings both mean yes, and reading only
#: one of them is what caused the "0 posted" chip, the re-flow that re-dated a
#: sent video, and recycling that could never be switched on:
#:
#: * a posted ``social_posts`` row — what sending actually writes, and the only
#:   one that occurs in practice, since sending never touches the item;
#: * ``state = 'posted'`` — declared in ITEM_STATES and in the schema CHECK, so
#:   it is a legitimate way to say it even though nothing writes it today.
#:
#: Assumes the queue-items row is aliased ``i``.
ITEM_HAS_POSTED_SQL = (
    "(i.state = 'posted' OR EXISTS (SELECT 1 FROM social_posts p "
    "WHERE p.smart_queue_item_id = i.id AND p.status = 'posted'))"
)

DEFAULT_MAX_DURATION_SECONDS = 180.0
DEFAULT_ORIENTATIONS = ["portrait", "square"]


class SmartQueueError(Exception):
    """A queue could not be created or updated as asked."""


@dataclass(frozen=True)
class Eligibility:
    """Why a video may or may not join a queue.

    ``reasons`` is populated only when ``ok`` is False, and is written for the
    user, not the log — it is what the config screen shows next to a video the
    filters excluded.
    """

    ok: bool
    reasons: tuple[str, ...] = ()


#: Columns declared INTEGER that carry a yes/no. Listed because the PATCH route
#: forwards whatever the body held, and both must be normalised.
BOOLEAN_COLUMNS = ("exclude_already_posted", "auto_add_on_live")


def require_boolean(field: str, value: object) -> bool:
    """A JSON boolean, or a loud refusal.

    Deliberately narrow rather than ``bool(value)``: the string 'false' is
    truthy in Python, and SQLite would store it as TEXT, leaving a column that
    reads as True everywhere. Guessing what the caller meant is worse than
    refusing. ``bool`` subclasses ``int``, so the JSON true/false the UI sends
    and the 0/1 the service API uses both pass.
    """
    if isinstance(value, bool) or (isinstance(value, int) and value in (0, 1)):
        return bool(value)
    raise SmartQueueError(f"{field} must be true or false, got {value!r}")


def _parse_orientations(raw: str | list | None) -> list[str]:
    if isinstance(raw, list):
        values = raw
    elif raw:
        values = json.loads(raw)
    else:
        values = list(DEFAULT_ORIENTATIONS)
    unknown = [v for v in values if v not in ORIENTATIONS]
    if unknown:
        raise SmartQueueError(f"Unknown orientation(s): {', '.join(unknown)}")
    return list(values)


def resolve_timezone(name: str) -> ZoneInfo:
    """Look up an IANA zone, failing loudly on a bad name.

    Deliberately no fallback to UTC or to the system zone: silently posting an
    8am clip at 4pm because a zone name was mistyped is far worse than a
    refused save.
    """
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise SmartQueueError(f"Unknown timezone {name!r}") from exc


#: ``templates.applies_to`` is a TIER list — hook | short | segment | video —
#: while ``videos.item_type`` is a kind: episode | hook | short | segment |
#: standalone. They line up on three of four values; only the full-length one
#: is spelled differently, so a template applying to "Video" matched no episode
#: at all.
#:
#: Matched against item_type rather than tier on purpose. Tier is derived from
#: duration, so it cannot tell a full episode from a promo clip — on the live
#: data 7 episodes tier as hook/short/segment, and matching by tier would sweep
#: them into a promo queue. The kind is the question here; length already has
#: its own filter. (The template-picker UIs in socials_compose and
#: project_settings do match on tier — that divergence is noted in
#: SMART_QUEUE.md.)
_ITEM_TYPE_FOR_TIER = {"video": "episode"}


def tier_matches_item_type(tier: str, item_type: str) -> bool:
    """Whether an ``applies_to`` entry covers a video of this ``item_type``."""
    return _ITEM_TYPE_FOR_TIER.get(tier, tier) == item_type


def is_eligible(video: dict, queue: dict, applies_to: list[str]) -> Eligibility:
    """Whether ``video`` may be added to ``queue``.

    ``applies_to`` is the queue template's tier list — passed in rather than
    re-read here so a caller checking many videos loads it once. See
    :func:`tier_matches_item_type` for how it maps onto ``item_type``.

    Every condition ANDs. Unknown dimensions are their own outcome, never
    treated as a passing or failing orientation: a video we know nothing about
    must be reported as unknown, not quietly dropped from the results.
    """
    reasons: list[str] = []

    item_type = (video.get("item_type") or "").strip()
    if not any(tier_matches_item_type(t, item_type) for t in applies_to):
        reasons.append(
            f"type '{item_type or 'unset'}' is not one the template applies to"
        )

    # Liveness means "as publicly available as this item gets", and that is a
    # different column depending on what backs the item.
    #
    # YouTube-backed: privacy_status, not status — status drifts off
    # 'published' whenever privacy is flipped via the metadata dropdown.
    #
    # Everything else has no YouTube presence, so privacy_status is never
    # written and stays 'unlisted' forever. Reading it would call such an item
    # permanently not-live, which is why the publish path could not run the
    # auto-add funnel for them at all. For these, published *is* live.
    if is_youtube_backed(video):
        if (video.get("privacy_status") or "") != "public":
            reasons.append(
                f"not live on YouTube (privacy is "
                f"{video.get('privacy_status') or 'unset'})"
            )
    elif (video.get("status") or "") != "published":
        reasons.append(
            f"not published yet (status is {video.get('status') or 'unset'})"
        )

    if video.get("archived"):
        reasons.append("archived")

    duration = video.get("duration_seconds")
    if duration is None:
        reasons.append("duration unknown")
    else:
        if duration < (queue.get("min_duration_seconds") or 0):
            reasons.append(
                f"{duration:.0f}s is shorter than the "
                f"{queue['min_duration_seconds']:.0f}s minimum"
            )
        maximum = queue.get("max_duration_seconds")
        if maximum is not None and duration > maximum:
            reasons.append(f"{duration:.0f}s is longer than the {maximum:.0f}s maximum")

    orientation = orientation_of(video.get("width"), video.get("height"))
    wanted = _parse_orientations(queue.get("orientations"))
    if orientation is None:
        reasons.append("dimensions unknown, so orientation can't be determined")
    elif orientation not in wanted:
        reasons.append(f"{orientation} is not one of the selected orientations")

    return Eligibility(ok=not reasons, reasons=tuple(reasons))


async def template_applies_to(template_id: int) -> list[str]:
    """The item types a queue's template covers."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT applies_to FROM templates WHERE id = ?", (template_id,)
    )
    if not rows:
        raise SmartQueueError(f"Template {template_id} not found")
    return json.loads(rows[0]["applies_to"] or "[]")


async def candidate_videos(queue: dict) -> dict:
    """Videos the queue could take right now, plus what was excluded and why.

    Returns ``{"eligible": [...], "excluded": [...], "unknown_dimensions": N}``.
    Excluded entries carry their reasons so the config screen can account for
    every video in the project rather than showing a number with no
    explanation.
    """
    db = await get_db()
    applies_to = await template_applies_to(queue["template_id"])

    # Already pending in THIS queue is always excluded; already posted by it is
    # excluded only when the filter is on — unchecking it is how recycling
    # works.
    #
    # "Pending" has to exclude items that have posted. They keep
    # state='scheduled' for life, so listing that state alone blocked every
    # posted video unconditionally and the exclude-already-posted toggle could
    # never do anything: recycling was off no matter what the box said.
    placeholders = ",".join("?" for _ in PENDING_ITEM_STATES)
    blocked = f"(i.state IN ({placeholders}) AND NOT {ITEM_HAS_POSTED_SQL})"
    if queue.get("exclude_already_posted"):
        blocked = f"({blocked} OR {ITEM_HAS_POSTED_SQL})"

    rows = await db.execute_fetchall(
        f"""
        SELECT v.id, v.title, v.item_type, v.duration_seconds, v.privacy_status,
               v.status, v.youtube_video_id, v.archived, v.width, v.height,
               v.created_at,
               {ORIENTATION_SQL} AS orientation
          FROM videos v
         WHERE v.project_id = ?
           AND NOT EXISTS (
                 SELECT 1 FROM smart_queue_items i
                  WHERE i.video_id = v.id AND i.queue_id = ?
                    AND {blocked}
               )
         ORDER BY v.created_at
        """,
        (queue["project_id"], queue["id"], *PENDING_ITEM_STATES),
    )

    eligible: list[dict] = []
    excluded: list[dict] = []
    unknown_dimensions = 0
    for row in rows:
        video = dict(row)
        verdict = is_eligible(video, queue, applies_to)
        if verdict.ok:
            eligible.append(video)
            continue
        if video["width"] is None or video["height"] is None:
            unknown_dimensions += 1
        excluded.append({**video, "reasons": list(verdict.reasons)})

    return {
        "eligible": eligible,
        "excluded": excluded,
        "unknown_dimensions": unknown_dimensions,
    }


def occurrences(
    slots: list[dict], zone: ZoneInfo, count: int, *, after: datetime | None = None
) -> list[datetime]:
    """The next ``count`` recurrence instants, as timezone-aware UTC datetimes.

    Enumerates *local* dates and converts each individually, so an occurrence
    on the far side of a DST boundary still lands at its stated wall-clock
    time. Resolving one offset and adding seven-day increments in UTC would
    drift by an hour instead.
    """
    if count <= 0:
        return []
    if not slots:
        raise SmartQueueError(
            "This queue has no posting times, so nothing can be scheduled."
        )

    start = (after or datetime.now(timezone.utc)).astimezone(zone)
    by_weekday: dict[int, list[time]] = {}
    for slot in slots:
        parsed = _parse_time_of_day(slot["time_of_day"])
        by_weekday.setdefault(_parse_weekday(slot["weekday"]), []).append(parsed)
    for times in by_weekday.values():
        times.sort()

    out: list[datetime] = []
    day: date = start.date()
    # Every weekday is 0-6 (_parse_weekday, above), so each 7 days advanced
    # matches at least one slot and produces >= 1 result: this cannot spin.
    while len(out) < count:
        for slot_time in by_weekday.get(day.weekday(), []):
            candidate = datetime.combine(day, slot_time, tzinfo=zone)
            if candidate <= start:
                continue
            out.append(candidate.astimezone(timezone.utc))
            if len(out) == count:
                break
        day += timedelta(days=1)
    return out


async def next_free_posting_times(queue: dict, count: int) -> list[datetime]:
    """The next ``count`` posting instants for ``queue``, after everything it
    has already stamped.

    Accept, the reschedule-to-end disposition, and the config screen's forecast
    all have to answer "when is this queue next free?", and they have to answer
    it identically — otherwise the forecast promises dates Accept won't use, or
    a second Accept double-books times the first already took. One
    implementation is what makes that impossible.

    Reads ``id``, ``timezone`` and ``slots`` from the queue row.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT MAX(scheduled_at) AS last FROM smart_queue_items "
        "WHERE queue_id = ? AND state = 'scheduled'",
        (int(queue["id"]),),
    )
    zone = resolve_timezone(queue["timezone"])
    return occurrences(
        queue["slots"], zone, count, after=_parse_after(rows[0]["last"])
    )


async def already_scheduled_video_ids(
    queue_id: int, video_ids: list[str]
) -> set[str]:
    """Which of ``video_ids`` this queue already has a pending item for.

    The same rule :func:`candidate_videos` applies, asked of an explicit list,
    so Accept enforces it at the write and not only at the preview. Without it,
    re-submitting a selection the screen never refreshed appends a second item
    and a second set of posts: the schema deliberately has no unique key on
    (queue_id, video_id) because an item is an occurrence, so nothing else
    would stop the double-booking.
    """
    if not video_ids:
        return set()
    placeholders = ",".join("?" for _ in video_ids)
    state_placeholders = ",".join("?" for _ in PENDING_ITEM_STATES)
    db = await get_db()
    rows = await db.execute_fetchall(
        f"SELECT DISTINCT i.video_id FROM smart_queue_items i "
        f"WHERE i.queue_id = ? AND i.state IN ({state_placeholders}) "
        f"AND NOT {ITEM_HAS_POSTED_SQL} "
        f"AND i.video_id IN ({placeholders})",
        (queue_id, *PENDING_ITEM_STATES, *video_ids),
    )
    return {row["video_id"] for row in rows}


def _parse_after(value: str | None) -> datetime | None:
    """Interpret a stored ``scheduled_at`` as the point to schedule after.

    ``None`` (nothing scheduled yet) means "from now", which
    :func:`occurrences` handles. A stored value is always UTC ISO; a naive
    string is read as UTC to match how it was written.
    """
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    # A backlog whose last slot is already in the past must not push new items
    # into the past too.
    return max(parsed, now)


def _parse_weekday(value: object) -> int:
    """Parse a slot's weekday: a whole number, 0 (Monday) - 6 (Sunday).

    No coercion of floats or bools, unlike a bare ``int()``: ``int(3.7)`` is 3,
    which would silently post on Thursday, and ``True`` is Tuesday. Out of
    range is refused here rather than left to :func:`occurrences`, where a
    weekday no date can ever match walks the calendar to the year 9999 and
    dies with an ``OverflowError`` naming nothing.
    """
    if isinstance(value, (bool, float)):
        raise SmartQueueError(
            f"Weekday must be a whole number 0-6 (0 = Monday), got {value!r}"
        )
    try:
        weekday = int(value)
    except (TypeError, ValueError) as exc:
        raise SmartQueueError(
            f"Weekday must be a whole number 0-6 (0 = Monday), got {value!r}"
        ) from exc
    if not 0 <= weekday <= 6:
        raise SmartQueueError(
            f"Weekday must be a whole number 0-6 (0 = Monday), got {value!r}"
        )
    return weekday


def _parse_time_of_day(value: str) -> time:
    """Parse 'HH:MM'. Rejects anything else rather than guessing."""
    try:
        hour, _, minute = str(value).partition(":")
        return time(hour=int(hour), minute=int(minute))
    except (TypeError, ValueError) as exc:
        raise SmartQueueError(f"Invalid time of day {value!r}, expected HH:MM") from exc


async def _slots_for_queues(queue_ids: list[int]) -> dict[int, list[dict]]:
    """Posting times for each of ``queue_ids``, keyed by queue.

    The single place slot rows are loaded and shaped, so the one-queue read and
    the whole-project read cannot drift in ordering or in which columns reach
    the caller. Every requested id gets a key, so a queue with no posting times
    reads as ``[]`` rather than as missing.
    """
    if not queue_ids:
        return {}
    placeholders = ",".join("?" for _ in queue_ids)
    db = await get_db()
    rows = await db.execute_fetchall(
        f"SELECT queue_id, id, weekday, time_of_day FROM smart_queue_slots "
        f"WHERE queue_id IN ({placeholders}) "
        f"ORDER BY queue_id, weekday, time_of_day",
        tuple(queue_ids),
    )
    by_queue: dict[int, list[dict]] = {queue_id: [] for queue_id in queue_ids}
    for row in rows:
        by_queue[row["queue_id"]].append({
            "id": row["id"], "weekday": row["weekday"],
            "time_of_day": row["time_of_day"],
        })
    return by_queue


async def get_queue(queue_id: int) -> dict:
    """A queue with its slots attached."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM smart_queues WHERE id = ?", (queue_id,)
    )
    if not rows:
        raise SmartQueueError(f"Smart queue {queue_id} not found")
    queue = dict(rows[0])
    queue["slots"] = (await _slots_for_queues([queue_id]))[queue_id]
    return queue


async def list_queues(project_id: int) -> list[dict]:
    """Every queue in a project, each with its slots and a state summary.

    Three statements regardless of how many queues there are. Per-queue reads
    were each a separate hand-off to the one shared connection that every other
    request and background job is also queued behind.
    """
    db = await get_db()
    queues = [
        dict(row)
        for row in await db.execute_fetchall(
            "SELECT * FROM smart_queues WHERE project_id = ? ORDER BY name",
            (project_id,),
        )
    ]
    queue_ids = [queue["id"] for queue in queues]
    slots_by_queue = await _slots_for_queues(queue_ids)

    counts_by_queue: dict[int, dict[str, int]] = {}
    if queue_ids:
        placeholders = ",".join("?" for _ in queue_ids)
        # Whether an item has posted is derived from its social_posts rows, not
        # read from item.state. Only 'queued', 'scheduled' and 'removed' are
        # ever written to that column — sending updates social_posts.status and
        # leaves the item alone — so counting states here reported "0 posted"
        # forever, next to a Recent list showing the post that had gone out.
        # Same principle as missed items: the posting rows are the truth, and a
        # second copy of it in item.state could only ever drift.
        for row in await db.execute_fetchall(
            f"""
            SELECT queue_id, bucket, COUNT(*) n FROM (
                SELECT i.queue_id AS queue_id,
                       CASE
                         WHEN i.state IN ('removed', 'queued') THEN i.state
                         WHEN SUM(p.status = 'posted') > 0 THEN 'posted'
                         WHEN SUM(p.status = 'failed') > 0 THEN 'failed'
                         ELSE i.state
                       END AS bucket
                  FROM smart_queue_items i
                  LEFT JOIN social_posts p ON p.smart_queue_item_id = i.id
                 WHERE i.queue_id IN ({placeholders})
                 GROUP BY i.id
            ) GROUP BY queue_id, bucket ORDER BY queue_id, bucket
            """,
            tuple(queue_ids),
        ):
            counts_by_queue.setdefault(row["queue_id"], {})[row["bucket"]] = row["n"]

    for queue in queues:
        queue["slots"] = slots_by_queue[queue["id"]]
        queue["counts"] = counts_by_queue.get(queue["id"], {})
    return queues


async def auto_add_queues(project_id: int) -> list[tuple[dict, list[str]]]:
    """Every auto-add queue in a project, paired with its template's item types.

    One statement, and one row shape, for the question the live-transition hook
    actually asks. Going through :func:`list_queues` answered a much larger
    question — slots and item-state counts the hook then discarded — and still
    needed a :func:`template_applies_to` per queue on top.

    A queue whose template is gone is logged and omitted: it cannot answer the
    eligibility question, so it cannot take part in it. LEFT JOIN plus
    ``template_row_id`` rather than an inner join so that case is *reported*
    instead of vanishing — ``applies_to`` is NOT NULL, so testing it for NULL
    would conflate "template gone" with "column empty".
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT q.*, t.id AS template_row_id, t.applies_to AS template_applies_to
          FROM smart_queues q
          LEFT JOIN templates t ON t.id = q.template_id
         WHERE q.project_id = ? AND q.auto_add_on_live != 0
         ORDER BY q.name
        """,
        (project_id,),
    )
    pairs: list[tuple[dict, list[str]]] = []
    for row in rows:
        queue = dict(row)
        template_row_id = queue.pop("template_row_id")
        raw_applies_to = queue.pop("template_applies_to")
        if template_row_id is None:
            logger.error(
                "Smart queue %s (%r) references template %s, which is gone; "
                "it cannot take part in auto-add",
                queue["id"], queue["name"], queue["template_id"],
            )
            continue
        pairs.append((queue, json.loads(raw_applies_to or "[]")))
    return pairs


async def create_queue(
    *,
    project_id: int,
    name: str,
    template_id: int,
    timezone_name: str,
    slots: list[dict],
    min_duration_seconds: float = 0.0,
    max_duration_seconds: float | None = DEFAULT_MAX_DURATION_SECONDS,
    orientations: list[str] | None = None,
    exclude_already_posted: bool = True,
    auto_add_on_live: bool = True,
    missed_policy: str = "post_late",
    missed_grace_hours: int | None = 24,
) -> int:
    """Create a queue. Returns its id."""
    _validate_queue_fields(
        name=name, timezone_name=timezone_name, slots=slots,
        min_duration_seconds=min_duration_seconds,
        max_duration_seconds=max_duration_seconds,
        orientations=orientations, missed_policy=missed_policy,
        missed_grace_hours=missed_grace_hours,
    )
    await template_applies_to(template_id)  # raises if the template is gone

    async with write_transaction() as db:
        cursor = await db.execute(
            """
            INSERT INTO smart_queues (
                project_id, name, template_id, timezone,
                min_duration_seconds, max_duration_seconds, orientations,
                exclude_already_posted, auto_add_on_live,
                missed_policy, missed_grace_hours
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                project_id, name.strip(), template_id, timezone_name,
                min_duration_seconds, max_duration_seconds,
                json.dumps(orientations or DEFAULT_ORIENTATIONS),
                1 if exclude_already_posted else 0,
                1 if auto_add_on_live else 0,
                missed_policy,
                missed_grace_hours if missed_policy == "post_late" else None,
            ),
        )
        queue_id = int(cursor.lastrowid)
        for slot in slots:
            await db.execute(
                "INSERT INTO smart_queue_slots (queue_id, weekday, time_of_day) "
                "VALUES (?,?,?)",
                (queue_id, _parse_weekday(slot["weekday"]), slot["time_of_day"]),
            )
    return queue_id


async def update_queue(queue_id: int, changes: dict) -> None:
    """Apply a partial update. Slots, when given, replace the whole set."""
    queue = await get_queue(queue_id)
    merged = {**queue, **changes}
    slots = changes.get("slots", queue["slots"])
    _validate_queue_fields(
        name=merged["name"],
        timezone_name=merged["timezone"],
        slots=slots,
        min_duration_seconds=merged["min_duration_seconds"],
        max_duration_seconds=merged["max_duration_seconds"],
        orientations=_parse_orientations(merged["orientations"]),
        missed_policy=merged["missed_policy"],
        missed_grace_hours=merged["missed_grace_hours"],
    )
    if "template_id" in changes:
        await template_applies_to(int(changes["template_id"]))

    columns = {
        key: value for key, value in changes.items()
        if key in {
            "name", "template_id", "timezone", "min_duration_seconds",
            "max_duration_seconds", "exclude_already_posted",
            "auto_add_on_live", "missed_policy", "missed_grace_hours",
        }
    }
    for field in BOOLEAN_COLUMNS:
        if field in columns:
            columns[field] = 1 if require_boolean(field, columns[field]) else 0
    if "orientations" in changes:
        columns["orientations"] = json.dumps(
            _parse_orientations(changes["orientations"])
        )
    # post_late is the only policy the grace window means anything for; keeping
    # a stale number on the others would show a window that does nothing.
    if columns.get("missed_policy") in {"reschedule_end", "remove"}:
        columns["missed_grace_hours"] = None

    async with write_transaction() as db:
        if columns:
            assignments = ", ".join(f"{k} = ?" for k in columns)
            await db.execute(
                f"UPDATE smart_queues SET {assignments}, "
                "updated_at = datetime('now') WHERE id = ?",
                (*columns.values(), queue_id),
            )
        if "slots" in changes:
            await db.execute(
                "DELETE FROM smart_queue_slots WHERE queue_id = ?", (queue_id,)
            )
            for slot in changes["slots"]:
                await db.execute(
                    "INSERT INTO smart_queue_slots (queue_id, weekday, time_of_day) "
                    "VALUES (?,?,?)",
                    (queue_id, _parse_weekday(slot["weekday"]), slot["time_of_day"]),
                )


async def delete_queue(queue_id: int) -> int:
    """Delete a queue, cancelling anything pending. History is kept.

    Returns the number of pending items cancelled. ``social_posts`` rows
    survive with ``smart_queue_item_id`` set to NULL by the FK, so the posting
    history of a deleted queue is still readable on each video.
    """
    from yt_scheduler.services.scheduler import cancel_scheduled_post

    db = await get_db()
    pending = await db.execute_fetchall(
        "SELECT p.id FROM social_posts p "
        "JOIN smart_queue_items i ON i.id = p.smart_queue_item_id "
        "WHERE i.queue_id = ? AND p.status != 'posted'",
        (queue_id,),
    )
    post_ids = [int(row["id"]) for row in pending]
    for post_id in post_ids:
        await cancel_scheduled_post(post_id)
    async with write_transaction() as db:
        # Clear the scheduling columns ourselves. cancel_scheduled_post returns
        # early when scheduler_job_id is NULL without touching scheduled_at —
        # and that is exactly the state Accept leaves when timer registration
        # failed ("picked up on next restart"). Once the FK nulls
        # smart_queue_item_id, restore_scheduled_posts would dutifully send a
        # post for a queue that no longer exists.
        if post_ids:
            placeholders = ",".join("?" * len(post_ids))
            await db.execute(
                f"UPDATE social_posts SET scheduled_at = NULL, "
                f"scheduler_job_id = NULL WHERE id IN ({placeholders}) "
                "AND status != 'posted'",
                post_ids,
            )
        await db.execute("DELETE FROM smart_queues WHERE id = ?", (queue_id,))
    return len(pending)


def _validate_queue_fields(
    *,
    name: str,
    timezone_name: str,
    slots: list[dict],
    min_duration_seconds: float,
    max_duration_seconds: float | None,
    orientations: list[str] | None,
    missed_policy: str,
    missed_grace_hours: int | None,
) -> None:
    if not (name or "").strip():
        raise SmartQueueError("A smart queue needs a name.")
    resolve_timezone(timezone_name)
    if not slots:
        raise SmartQueueError(
            "A smart queue needs at least one posting time, or it would never post."
        )
    for slot in slots:
        _parse_weekday(slot["weekday"])
        _parse_time_of_day(slot["time_of_day"])
    if min_duration_seconds < 0:
        raise SmartQueueError("Minimum duration can't be negative.")
    if max_duration_seconds is not None and max_duration_seconds <= min_duration_seconds:
        raise SmartQueueError(
            "Maximum duration must be greater than the minimum, or nothing can match."
        )
    if not _parse_orientations(orientations):
        raise SmartQueueError(
            "Select at least one orientation, or nothing can match."
        )
    if missed_policy not in MISSED_POLICIES:
        raise SmartQueueError(
            f"Unknown missed-slot policy {missed_policy!r}; "
            f"expected one of {', '.join(MISSED_POLICIES)}"
        )
    if missed_policy == "post_late":
        if missed_grace_hours is None or missed_grace_hours <= 0:
            raise SmartQueueError(
                "'Post late' needs a positive number of hours to post within."
            )
