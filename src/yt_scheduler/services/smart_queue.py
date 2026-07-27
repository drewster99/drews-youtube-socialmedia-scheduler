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
from yt_scheduler.services.video_dimensions import ORIENTATION_SQL, orientation_of

logger = logging.getLogger(__name__)

ORIENTATIONS = ("portrait", "landscape", "square")
MISSED_POLICIES = ("post_late", "reschedule_end", "remove")

# States a queue item can be in. `scheduled` is the only pending one; the rest
# are terminal for that occurrence, and a recycled video gets a NEW row rather
# than a state reset (see the migration).
ITEM_STATES = ("scheduled", "posted", "failed", "skipped", "removed")

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


def is_eligible(video: dict, queue: dict, applies_to: list[str]) -> Eligibility:
    """Whether ``video`` may be added to ``queue``.

    ``applies_to`` is the queue template's item-type list — passed in rather
    than re-read here so a caller checking many videos loads it once.

    Every condition ANDs. Unknown dimensions are their own outcome, never
    treated as a passing or failing orientation: a video we know nothing about
    must be reported as unknown, not quietly dropped from the results.
    """
    reasons: list[str] = []

    item_type = (video.get("item_type") or "").strip()
    if item_type not in applies_to:
        reasons.append(
            f"type '{item_type or 'unset'}' is not one the template applies to"
        )

    # privacy_status is the authority on liveness, not status: status drifts
    # off 'published' whenever privacy is flipped via the metadata dropdown.
    if (video.get("privacy_status") or "") != "public":
        reasons.append(
            f"not live on YouTube (privacy is "
            f"{video.get('privacy_status') or 'unset'})"
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

    # Already scheduled by THIS queue is always excluded; already posted by it
    # is excluded only when the filter is on (unchecking it is how recycling
    # works).
    blocked_states = ["scheduled"]
    if queue.get("exclude_already_posted"):
        blocked_states.append("posted")
    placeholders = ",".join("?" for _ in blocked_states)

    rows = await db.execute_fetchall(
        f"""
        SELECT v.id, v.title, v.item_type, v.duration_seconds, v.privacy_status,
               v.archived, v.width, v.height, v.created_at,
               {ORIENTATION_SQL} AS orientation
          FROM videos v
         WHERE v.project_id = ?
           AND NOT EXISTS (
                 SELECT 1 FROM smart_queue_items i
                  WHERE i.video_id = v.id AND i.queue_id = ?
                    AND i.state IN ({placeholders})
               )
         ORDER BY v.created_at
        """,
        (queue["project_id"], queue["id"], *blocked_states),
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
        by_weekday.setdefault(int(slot["weekday"]), []).append(parsed)
    for times in by_weekday.values():
        times.sort()

    out: list[datetime] = []
    day: date = start.date()
    # A week of slots always yields at least one instant, so this cannot spin:
    # every 7 days advanced produces >= 1 result.
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


def _parse_time_of_day(value: str) -> time:
    """Parse 'HH:MM'. Rejects anything else rather than guessing."""
    try:
        hour, _, minute = str(value).partition(":")
        return time(hour=int(hour), minute=int(minute))
    except (TypeError, ValueError) as exc:
        raise SmartQueueError(f"Invalid time of day {value!r}, expected HH:MM") from exc


async def get_queue(queue_id: int) -> dict:
    """A queue with its slots attached."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM smart_queues WHERE id = ?", (queue_id,)
    )
    if not rows:
        raise SmartQueueError(f"Smart queue {queue_id} not found")
    queue = dict(rows[0])
    queue["slots"] = [
        dict(r)
        for r in await db.execute_fetchall(
            "SELECT id, weekday, time_of_day FROM smart_queue_slots "
            "WHERE queue_id = ? ORDER BY weekday, time_of_day",
            (queue_id,),
        )
    ]
    return queue


async def list_queues(project_id: int) -> list[dict]:
    """Every queue in a project, each with its slots and a state summary."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM smart_queues WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    queues = []
    for row in rows:
        queue = await get_queue(int(row["id"]))
        counts = await db.execute_fetchall(
            "SELECT state, COUNT(*) n FROM smart_queue_items "
            "WHERE queue_id = ? GROUP BY state",
            (queue["id"],),
        )
        queue["counts"] = {r["state"]: r["n"] for r in counts}
        queues.append(queue)
    return queues


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
                (queue_id, int(slot["weekday"]), slot["time_of_day"]),
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
                    (queue_id, int(slot["weekday"]), slot["time_of_day"]),
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
    for row in pending:
        await cancel_scheduled_post(int(row["id"]))
    async with write_transaction() as db:
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
        weekday = int(slot["weekday"])
        if not 0 <= weekday <= 6:
            raise SmartQueueError(f"Weekday must be 0-6 (Mon-Sun), got {weekday}")
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
