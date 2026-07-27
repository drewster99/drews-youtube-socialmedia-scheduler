"""Accept — turn a selected batch of videos into a scheduled posting plan.

Each accepted video becomes one queue item stamped with a concrete time, and
one ordinary ``social_posts`` row per template slot that can carry it. Using
the normal posts table rather than a parallel one means send, retry, duplicate
detection, history, and the missed-backlog guard all apply unchanged.

A slot that *cannot* carry a video — a clip longer than the platform's cap, a
platform that can't take an attachment at all — records a ``skipped`` row with
the reason. Skipped means "known in advance, not attempted"; ``failed`` means
"attempted and broke", and the two have to stay tellable apart in history.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import smart_queue as queue_service
from yt_scheduler.services import social, templates as tmpl
from yt_scheduler.services.render_context import (
    RenderContextError,
    build_render_context,
)

# Everything a single slot's render can legitimately fail with. Caught per
# slot so one broken body doesn't abort the whole batch: the other
# platforms still go out and the failure is visible with its real reason.
_RENDER_FAILURES = (
    RenderContextError,
    tmpl.MissingRequiredVariable,
    tmpl.UndefinedTemplateVariables,
    tmpl.SectionTagError,
    tmpl.UnknownImageShortname,
    tmpl.TooManyAIBlocksError,
    tmpl.AIBlockDepthError,
    ValueError,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SlotVerdict:
    """Whether one template slot can carry one video."""

    slot: dict
    accepted: bool
    reason: str | None = None


async def slots_accepting(video: dict, slots: list[dict]) -> list[SlotVerdict]:
    """Decide, per slot, whether this video can be posted there.

    Checked against the platform's published envelope *before* anything is
    scheduled, so a clip that can never succeed on a platform is recorded as
    skipped up front instead of failing at 9am every time it comes round.

    Only limits no encode can fix are grounds for skipping. Size, resolution,
    and codec are all fixable at send time by ``prepared_media``, so they are
    deliberately not consulted here.
    """
    duration = video.get("duration_seconds")
    verdicts: list[SlotVerdict] = []
    for slot in slots:
        if slot.get("is_disabled"):
            continue
        platform = slot["platform"]

        poster_class = social._POSTERS.get(platform)
        if poster_class is None:
            verdicts.append(SlotVerdict(slot, False, f"unknown platform {platform!r}"))
            continue
        if not poster_class.accepts_media:
            verdicts.append(SlotVerdict(
                slot, False,
                f"{platform} can't take an attached video "
                "(its API fetches media from a public URL)",
            ))
            continue

        limits = social.PLATFORM_MEDIA_LIMITS.get(platform)
        if (
            limits is not None
            and limits.max_duration_seconds is not None
            and duration is not None
            and duration > limits.max_duration_seconds
        ):
            verdicts.append(SlotVerdict(
                slot, False,
                f"{duration:.0f}s is over {platform}'s "
                f"{limits.max_duration_seconds:.0f}s limit",
            ))
            continue

        verdicts.append(SlotVerdict(slot, True))
    return verdicts


async def accept_selection(
    queue_id: int, video_ids: list[str], *, default_ai_system: str | None = None
) -> dict:
    """Schedule ``video_ids``, in the order given, onto the queue's recurrence.

    Order is the caller's: the config screen sends whatever the user is
    looking at, shuffled or not. Times are computed by enumerating the queue's
    recurrence in its own timezone, so each stamped instant is correct for its
    own date across a DST boundary.

    Already-scheduled items are untouched; new times start after the last one
    already on the books, so accepting a second batch appends rather than
    colliding.

    Returns ``{"scheduled": N, "items": [...], "skipped": [...]}``.
    """
    from yt_scheduler.services.scheduler import schedule_social_post

    if not video_ids:
        return {"scheduled": 0, "items": [], "skipped": []}

    queue = await queue_service.get_queue(queue_id)
    zone = queue_service.resolve_timezone(queue["timezone"])
    db = await get_db()

    template = await _template_by_id(int(queue["template_id"]))
    slots = template["slots"]

    # New times continue after everything already scheduled, so a second
    # Accept appends to the plan instead of double-booking its slots.
    last_rows = await db.execute_fetchall(
        "SELECT MAX(scheduled_at) AS last FROM smart_queue_items "
        "WHERE queue_id = ? AND state = 'scheduled'",
        (queue_id,),
    )
    after = queue_service._parse_after(last_rows[0]["last"])

    instants = queue_service.occurrences(
        queue["slots"], zone, len(video_ids), after=after
    )

    position_rows = await db.execute_fetchall(
        "SELECT COALESCE(MAX(position), -1) AS last FROM smart_queue_items "
        "WHERE queue_id = ?",
        (queue_id,),
    )
    next_position = int(position_rows[0]["last"]) + 1

    created_items: list[dict] = []
    skipped: list[dict] = []

    for offset, video_id in enumerate(video_ids):
        rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (video_id,)
        )
        if not rows:
            skipped.append({"video_id": video_id, "reason": "video no longer exists"})
            continue
        video = dict(rows[0])
        when = instants[offset]

        async with write_transaction() as write_db:
            cursor = await write_db.execute(
                "INSERT INTO smart_queue_items "
                "(queue_id, video_id, position, scheduled_at, state) "
                "VALUES (?,?,?,?,'scheduled')",
                (queue_id, video_id, next_position + offset, when.isoformat()),
            )
            item_id = int(cursor.lastrowid)

        verdicts = await slots_accepting(video, slots)
        posted_any = False
        for verdict in verdicts:
            if not verdict.accepted:
                await _record_skipped_slot(item_id, verdict)
                skipped.append({
                    "video_id": video_id,
                    "platform": verdict.slot["platform"],
                    "reason": verdict.reason,
                })
                continue
            try:
                post_id = await _create_post_for_slot(
                    db, video, verdict.slot, item_id,
                    default_ai_system=default_ai_system,
                )
            except _RENDER_FAILURES as exc:
                # A render failure is per-slot: the other platforms still go
                # out, and this one is visible with the real reason rather
                # than silently missing.
                await _record_skipped_slot(
                    item_id, SlotVerdict(verdict.slot, False, str(exc))
                )
                skipped.append({
                    "video_id": video_id,
                    "platform": verdict.slot["platform"],
                    "reason": str(exc),
                })
                continue
            await schedule_social_post(post_id, when)
            posted_any = True

        if not posted_any:
            # Nothing can be sent for this video, so leaving it 'scheduled'
            # would burn a posting slot on a no-op.
            async with write_transaction() as write_db:
                await write_db.execute(
                    "UPDATE smart_queue_items SET state = 'skipped', reason = ? "
                    "WHERE id = ?",
                    ("no slot could carry this video", item_id),
                )
        created_items.append({
            "id": item_id, "video_id": video_id,
            "scheduled_at": when.isoformat(),
            "posted_to_any": posted_any,
        })

    return {
        "scheduled": sum(1 for i in created_items if i["posted_to_any"]),
        "items": created_items,
        "skipped": skipped,
    }


async def rerender_pending(queue_id: int, *, default_ai_system: str | None = None) -> dict:
    """Re-render the text of every still-pending post this queue owns.

    Text is rendered at Accept, which means a later template edit doesn't
    reach posts already on the schedule. This is how you push an edit onto
    them without tearing the schedule down and rebuilding it.

    Only posts that haven't been sent are touched; a posted row is history.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT p.id AS post_id, p.slot_id, i.video_id
          FROM social_posts p
          JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
         WHERE i.queue_id = ? AND p.status NOT IN ('posted', 'sending')
        """,
        (queue_id,),
    )
    queue = await queue_service.get_queue(queue_id)
    template = await _template_by_id(int(queue["template_id"]))
    slots_by_id = {int(s["id"]): s for s in template["slots"] if s.get("id")}

    updated, errors = 0, []
    for row in rows:
        slot = slots_by_id.get(int(row["slot_id"] or 0))
        if slot is None:
            errors.append({
                "post_id": int(row["post_id"]),
                "error": "the slot this post came from no longer exists",
            })
            continue
        video_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (row["video_id"],)
        )
        if not video_rows:
            errors.append({
                "post_id": int(row["post_id"]), "error": "video no longer exists"
            })
            continue
        try:
            rendered, media_paths = await _render_slot(
                db, dict(video_rows[0]), slot, default_ai_system=default_ai_system
            )
        except _RENDER_FAILURES as exc:
            errors.append({"post_id": int(row["post_id"]), "error": str(exc)})
            continue
        async with write_transaction() as write_db:
            await write_db.execute(
                "UPDATE social_posts SET content = ?, media_paths = ? WHERE id = ?",
                (rendered, json.dumps(media_paths), int(row["post_id"])),
            )
        updated += 1
    return {"updated": updated, "errors": errors}


async def _template_by_id(template_id: int) -> dict:
    """Load a template with its slots, by id.

    ``templates.get_template`` is keyed by name; a queue holds an id, and the
    name can change under it.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, name, project_id FROM templates WHERE id = ?", (template_id,)
    )
    if not rows:
        raise queue_service.SmartQueueError(f"Template {template_id} not found")
    template = dict(rows[0])
    template["slots"] = await tmpl._list_slots(template_id)
    return template


async def _render_slot(
    db, video: dict, slot: dict, *, default_ai_system: str | None
) -> tuple[str, list[str]]:
    """Render one slot's body for one video, returning (text, media paths).

    Directive extraction is a PRE-pass, matching the generate-posts path:
    ``{{video}}`` and friends are media directives, not variables, so they
    have to come out of the body before the strict renderer sees it — left in,
    they read as undefined variables and the render fails.
    """
    context = await build_render_context(db, video)
    body = slot.get("body") or ""
    cleaned_body, media_paths, _alts = tmpl.extract_media_directives(
        body,
        video_path=context["video_path"],
        thumbnail_path=context["thumb_path"],
        images=context["images"],
    )
    rendered = await tmpl.async_render(
        cleaned_body, context["variables"], default_system_prompt=default_ai_system
    )
    return rendered.strip(), media_paths


async def _create_post_for_slot(
    db, video: dict, slot: dict, item_id: int, *, default_ai_system: str | None
) -> int:
    """Create one approved, scheduled ``social_posts`` row. Returns its id."""
    rendered, media_paths = await _render_slot(
        db, video, slot, default_ai_system=default_ai_system
    )
    async with write_transaction() as write_db:
        cursor = await write_db.execute(
            """
            INSERT INTO social_posts
                (video_id, platform, content, media_paths, status,
                 social_account_id, max_chars, slot_id, smart_queue_item_id)
            VALUES (?,?,?,?,'approved',?,?,?,?)
            """,
            (
                video["id"], slot["platform"], rendered, json.dumps(media_paths),
                slot.get("social_account_id"), slot.get("max_chars"),
                slot.get("id"), item_id,
            ),
        )
        return int(cursor.lastrowid)


async def _record_skipped_slot(item_id: int, verdict: SlotVerdict) -> None:
    """Record that a slot was deliberately not attempted, and why.

    A row rather than nothing, so history can tell "never attempted" from
    "posted and later deleted".
    """
    async with write_transaction() as db:
        await db.execute(
            """
            INSERT INTO social_posts
                (video_id, platform, content, status, error, slot_id,
                 smart_queue_item_id)
            SELECT i.video_id, ?, '', 'skipped', ?, ?, i.id
              FROM smart_queue_items i WHERE i.id = ?
            """,
            (verdict.slot["platform"], verdict.reason, verdict.slot.get("id"), item_id),
        )
