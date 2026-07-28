"""Smart queue HTTP surface.

Exercises the router against a real app instance so the wiring — route
registration, project scoping, error mapping — is covered, not just the
service functions underneath it.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for module in list(sys.modules):
        if module.startswith("yt_scheduler"):
            sys.modules.pop(module, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    app_module = importlib.import_module("yt_scheduler.app")

    db = await database.get_db()
    await projects.ensure_default_project()
    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'clips', '[\"hook\",\"short\"]')"
    )
    template_id = int(cursor.lastrowid)
    await db.commit()

    # "testserver" is the one non-loopback host TrustedHostMiddleware allows;
    # anything else is rejected with "Invalid host header" before routing.
    transport = ASGITransport(app=app_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as http:
        yield http, db, template_id
    await database.close_db()


def _payload(template_id: int, **overrides) -> dict:
    body = {
        "name": "Daily clips",
        "template_id": template_id,
        "timezone": "America/Los_Angeles",
        "slots": [{"weekday": 0, "time_of_day": "09:00"}],
    }
    body.update(overrides)
    return body


async def test_create_list_and_delete(client):
    http, _db, template_id = client
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    assert created.status_code == 200, created.text
    queue_id = created.json()["id"]

    listed = await http.get("/api/projects/default/smart-queues")
    assert [q["id"] for q in listed.json()["queues"]] == [queue_id]

    deleted = await http.delete(f"/api/projects/default/smart-queues/{queue_id}")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
    assert (await http.get("/api/projects/default/smart-queues")).json()["queues"] == []


async def test_validation_errors_reach_the_user_verbatim(client):
    """The server explains which filter was rejected; the UI shows that text,
    so a generic 400 body would lose the only useful part."""
    http, _db, template_id = client
    response = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id, slots=[])
    )
    assert response.status_code == 400
    assert "posting time" in response.json()["detail"]


async def test_unknown_timezone_is_rejected(client):
    http, _db, template_id = client
    response = await http.post(
        "/api/projects/default/smart-queues",
        json=_payload(template_id, timezone="Mars/Olympus_Mons"),
    )
    assert response.status_code == 400
    assert "timezone" in response.json()["detail"].lower()


async def test_unknown_project_is_404(client):
    http, _db, template_id = client
    response = await http.post(
        "/api/projects/nope/smart-queues", json=_payload(template_id)
    )
    assert response.status_code == 404


async def test_queue_from_another_project_is_not_reachable(client):
    """Reported as not-found rather than forbidden, so an id in another
    project isn't confirmed to exist by the error."""
    http, db, template_id = client
    await db.execute(
        "INSERT INTO projects (id, slug, name) VALUES (2, 'other', 'Other')"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    response = await http.get(f"/api/projects/other/smart-queues/{queue_id}")
    assert response.status_code == 404


async def test_candidates_preview_writes_nothing(client):
    http, db, template_id = client
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES ('vid00000001', 'vid00000001', 1, 'A clip', 'hook', 60, "
        "'public', 1080, 1920)"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    response = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates", json={}
    )
    assert response.status_code == 200
    body = response.json()
    assert [v["id"] for v in body["eligible"]] == ["vid00000001"]
    assert body["summary"] == {"total": 1, "by_type": {"hook": 1}}
    assert len(body["forecast"]) == 1
    assert body["ends_at"] is not None

    rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
    assert rows[0]["n"] == 0, "preview must not schedule anything"


async def test_candidate_overrides_do_not_persist(client):
    """The screen previews a filter change before saving it."""
    http, db, template_id = client
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES ('wide0000000', 'wide0000000', 1, 'Landscape', 'hook', 60, "
        "'public', 1920, 1080)"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    default_run = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates", json={}
    )
    assert default_run.json()["summary"]["total"] == 0

    widened = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates",
        json={"orientations": ["landscape"]},
    )
    assert widened.json()["summary"]["total"] == 1

    saved = await http.get(f"/api/projects/default/smart-queues/{queue_id}")
    assert "landscape" not in saved.json()["orientations"]


async def test_config_page_renders(client):
    http, _db, _template_id = client
    for path in ("/projects/default/smart-queues/new",):
        response = await http.get(path)
        assert response.status_code == 200
        assert "Smart schedule" in response.text


async def test_config_page_does_not_use_the_stacking_row_class(client):
    """`.form-row label` is `flex-direction: column` — it exists for the
    caption-above-input shape other screens use. This screen's rows are
    caption-beside-control, so borrowing that class put every checkbox on a
    different line from its own text."""
    http, _db, _template_id = client
    body = (await http.get("/projects/default/smart-queues/new")).text
    assert 'class="form-row"' not in body
    assert body.count('class="field-check"') == 5, "every checkbox needs it"


async def test_forecast_starts_after_the_existing_schedule(client):
    """The forecast must predict what Accept will actually do.

    It used to be computed from *now* while Accept scheduled after the last
    stamped item, so every predicted date on the screen was wrong as soon as
    the queue had anything pending — in exactly the case the screen is most
    used, with the Accept button directly beneath it.
    """
    from datetime import datetime, timedelta, timezone

    http, db, template_id = client
    for video_id, title in (("vid00000001", "Booked clip"), ("vid00000002", "Next clip")):
        await db.execute(
            "INSERT INTO videos (id, youtube_video_id, project_id, title, "
            "item_type, duration_seconds, privacy_status, width, height) "
            "VALUES (?, ?, 1, ?, 'hook', 60, 'public', 1080, 1920)",
            (video_id, video_id, title),
        )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    booked = datetime.now(timezone.utc) + timedelta(weeks=5)
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, "
        "scheduled_at, state) VALUES (?, 'vid00000001', 0, ?, 'scheduled')",
        (queue_id, booked.isoformat()),
    )
    await db.commit()

    body = (await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates", json={}
    )).json()

    assert [v["id"] for v in body["eligible"]] == ["vid00000002"]
    assert body["forecast"], "a queue with posting times must forecast candidates"
    assert datetime.fromisoformat(body["forecast"][0]) > booked, (
        f"forecast {body['forecast'][0]} lands before the already-booked "
        f"{booked.isoformat()}; Accept would schedule it later than shown"
    )


async def test_accept_with_no_ids_schedules_the_waiting_items(client):
    """A queue running on auto-add alone must be schedulable. The service
    promoted waiting items all along, but the route rejected an empty list —
    so nothing could reach it and the queue never posted."""
    http, db, template_id = client
    await db.execute(
        "INSERT INTO template_slots (template_id, platform, body, media, max_chars) "
        "VALUES (?, 'bluesky', 'Watch {{title}}', 'none', 300)",
        (template_id,),
    )
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES ('vid00000001', 'vid00000001', 1, 'Auto-added', 'hook', 60, "
        "'public', 1080, 1920)"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, 'vid00000001', 0, 'queued')",
        (queue_id,),
    )
    await db.commit()

    response = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/accept", json={}
    )

    assert response.status_code == 200, response.text
    assert response.json()["scheduled"] == 1
    rows = await db.execute_fetchall(
        "SELECT state, scheduled_at FROM smart_queue_items"
    )
    assert rows[0]["state"] == "scheduled"
    assert rows[0]["scheduled_at"] is not None


async def test_candidates_reports_the_waiting_count(client):
    """The screen can only offer to schedule waiting items if it knows they
    exist — they are not candidates, because they are already in the queue."""
    http, db, template_id = client
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES ('vid00000001', 'vid00000001', 1, 'Auto-added', 'hook', 60, "
        "'public', 1080, 1920)"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, 'vid00000001', 0, 'queued')",
        (queue_id,),
    )
    await db.commit()

    body = (await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates", json={}
    )).json()

    assert body["waiting"] == 1
    assert body["eligible"] == [], "a queued video is in the queue, not a candidate"


async def test_counts_report_posting_from_the_posts_not_the_item_state(client):
    """The dashboard chip read `smart_queue_items.state`, but sending only ever
    updates `social_posts.status` — nothing writes 'posted' to the item. So a
    queue whose video had gone out still read "48 scheduled · 0 posted", right
    above a Recent list showing the post.
    """
    http, db, template_id = client
    for video_id in ("sent0000000", "waiting0000"):
        await db.execute(
            "INSERT INTO videos (id, youtube_video_id, project_id, title, "
            "item_type, duration_seconds, privacy_status, width, height) "
            "VALUES (?, ?, 1, 'A clip', 'hook', 60, 'public', 1080, 1920)",
            (video_id, video_id),
        )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    # Both items stay 'scheduled' — that is exactly what the real code leaves
    # behind after one of them has posted.
    for position, video_id in enumerate(("sent0000000", "waiting0000")):
        cursor = await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, "
            "scheduled_at, state) VALUES (?, ?, ?, '2026-01-01T09:00:00+00:00', "
            "'scheduled')",
            (queue_id, video_id, position),
        )
        await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, "
            "smart_queue_item_id) VALUES (?, 'bluesky', 'x', ?, ?)",
            (video_id, "posted" if video_id == "sent0000000" else "approved",
             int(cursor.lastrowid)),
        )
    await db.commit()

    listed = await http.get("/api/projects/default/smart-queues")
    counts = listed.json()["queues"][0]["counts"]

    assert counts.get("posted") == 1, f"a sent post must count as posted: {counts}"
    assert counts.get("scheduled") == 1, f"only the unsent one is pending: {counts}"


async def test_reflow_leaves_already_sent_items_alone(client, monkeypatch):
    """Re-flow re-stamps pending items onto the new posting times. It selected
    on `state = 'scheduled'`, but sending never moves an item off that state —
    so a video that had already gone out was handed a fresh future occurrence,
    pushing every remaining video back one slot on every re-flow.
    """
    import importlib

    http, db, template_id = client
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    async def noop(post_id, when):
        return None

    monkeypatch.setattr(scheduler, "schedule_social_post", noop)

    for video_id in ("sent0000000", "pending0000"):
        await db.execute(
            "INSERT INTO videos (id, youtube_video_id, project_id, title, "
            "item_type, duration_seconds, privacy_status, width, height) "
            "VALUES (?, ?, 1, 'A clip', 'hook', 60, 'public', 1080, 1920)",
            (video_id, video_id),
        )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    was = "2020-01-06T09:00:00+00:00"
    item_ids = {}
    for position, video_id in enumerate(("sent0000000", "pending0000")):
        cursor = await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, "
            "scheduled_at, state) VALUES (?, ?, ?, ?, 'scheduled')",
            (queue_id, video_id, position, was),
        )
        item_ids[video_id] = int(cursor.lastrowid)
        await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, "
            "smart_queue_item_id) VALUES (?, 'bluesky', 'x', ?, ?)",
            (video_id, "posted" if video_id == "sent0000000" else "approved",
             item_ids[video_id]),
        )
    await db.commit()

    response = await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/re-flow"
    )
    assert response.status_code == 200, response.text
    assert response.json()["reflowed"] == 1, "only the unsent item moves"

    rows = {
        r["video_id"]: r["scheduled_at"]
        for r in await db.execute_fetchall(
            "SELECT video_id, scheduled_at FROM smart_queue_items"
        )
    }
    assert rows["sent0000000"] == was, "an already-posted video must not be re-dated"
    assert rows["pending0000"] != was, "the unsent one moves onto the new times"


async def test_reflow_uses_todays_slot_when_it_is_still_ahead(client, monkeypatch):
    """Deleting Mon 6:00pm and adding Mon 6:14pm at 6:11pm must schedule the
    next video for 6:14pm *today* — the slot had not passed.

    It went to the following week instead. The already-posted video was still
    state='scheduled', so it was re-flowed too and took today's occurrence,
    leaving the first genuinely pending video a week out. The symptom reads as
    "re-flow skipped today"; the cause is that a sent item was re-dated at all.
    """
    import importlib
    from datetime import datetime, timedelta, timezone

    http, db, template_id = client
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    async def noop(post_id, when):
        return None

    monkeypatch.setattr(scheduler, "schedule_social_post", noop)

    # A slot two minutes out — unambiguously still ahead, and derived from that
    # instant so it can't straddle midnight into a different weekday.
    soon = datetime.now(timezone.utc) + timedelta(minutes=2)
    created = await http.post(
        "/api/projects/default/smart-queues",
        json=_payload(
            template_id,
            timezone="UTC",
            slots=[{"weekday": soon.weekday(), "time_of_day": soon.strftime("%H:%M")}],
        ),
    )
    queue_id = created.json()["id"]

    for video_id in ("sent0000000", "next-up0000"):
        await db.execute(
            "INSERT INTO videos (id, youtube_video_id, project_id, title, "
            "item_type, duration_seconds, privacy_status, width, height) "
            "VALUES (?, ?, 1, 'A clip', 'hook', 60, 'public', 1080, 1920)",
            (video_id, video_id),
        )
    for position, video_id in enumerate(("sent0000000", "next-up0000")):
        cursor = await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, "
            "scheduled_at, state) VALUES (?, ?, ?, '2020-01-06T09:00:00+00:00', "
            "'scheduled')",
            (queue_id, video_id, position),
        )
        await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, "
            "smart_queue_item_id) VALUES (?, 'bluesky', 'x', ?, ?)",
            (video_id, "posted" if video_id == "sent0000000" else "approved",
             int(cursor.lastrowid)),
        )
    await db.commit()

    await http.post(f"/api/projects/default/smart-queues/{queue_id}/re-flow")

    rows = {
        r["video_id"]: r["scheduled_at"]
        for r in await db.execute_fetchall(
            "SELECT video_id, scheduled_at FROM smart_queue_items"
        )
    }
    scheduled = datetime.fromisoformat(rows["next-up0000"])
    assert scheduled.date() == soon.date() and scheduled.hour == soon.hour, (
        f"next video went to {rows['next-up0000']}, but the {soon:%H:%M} slot today "
        "had not passed yet"
    )


async def test_items_report_whether_they_have_gone_out(client):
    """The Upcoming list on the config screen asks this endpoint what is still
    coming. Filtering on `state` alone showed a video that had already posted
    as upcoming, because sending never moves the item off 'scheduled'.
    """
    http, db, template_id = client
    for video_id in ("sent0000000", "pending0000"):
        await db.execute(
            "INSERT INTO videos (id, youtube_video_id, project_id, title, "
            "item_type, duration_seconds, privacy_status, width, height) "
            "VALUES (?, ?, 1, 'A clip', 'hook', 60, 'public', 1080, 1920)",
            (video_id, video_id),
        )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]
    for position, video_id in enumerate(("sent0000000", "pending0000")):
        cursor = await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, "
            "scheduled_at, state) VALUES (?, ?, ?, '2026-01-01T09:00:00+00:00', "
            "'scheduled')",
            (queue_id, video_id, position),
        )
        await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, "
            "smart_queue_item_id) VALUES (?, 'bluesky', 'x', ?, ?)",
            (video_id, "posted" if video_id == "sent0000000" else "approved",
             int(cursor.lastrowid)),
        )
    await db.commit()

    items = (await http.get(
        f"/api/projects/default/smart-queues/{queue_id}/items"
    )).json()["items"]
    by_video = {item["video_id"]: item for item in items}

    assert by_video["sent0000000"]["state"] == "scheduled", (
        "precondition: sending does not move the item's state"
    )
    assert by_video["sent0000000"]["has_posted"] == 1
    assert by_video["pending0000"]["has_posted"] == 0
