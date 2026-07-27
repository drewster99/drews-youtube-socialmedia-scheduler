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
        "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
        "privacy_status, width, height) "
        "VALUES ('v1', 1, 'A clip', 'hook', 60, 'public', 1080, 1920)"
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
    assert [v["id"] for v in body["eligible"]] == ["v1"]
    assert body["summary"] == {"total": 1, "by_type": {"hook": 1}}
    assert len(body["forecast"]) == 1
    assert body["ends_at"] is not None

    rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
    assert rows[0]["n"] == 0, "preview must not schedule anything"


async def test_candidate_overrides_do_not_persist(client):
    """The screen previews a filter change before saving it."""
    http, db, template_id = client
    await db.execute(
        "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
        "privacy_status, width, height) "
        "VALUES ('wide', 1, 'Landscape', 'hook', 60, 'public', 1920, 1080)"
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


async def test_forecast_starts_after_the_existing_schedule(client):
    """The forecast must predict what Accept will actually do.

    It used to be computed from *now* while Accept scheduled after the last
    stamped item, so every predicted date on the screen was wrong as soon as
    the queue had anything pending — in exactly the case the screen is most
    used, with the Accept button directly beneath it.
    """
    from datetime import datetime, timedelta, timezone

    http, db, template_id = client
    for video_id, title in (("v1", "Booked clip"), ("v2", "Next clip")):
        await db.execute(
            "INSERT INTO videos (id, project_id, title, item_type, "
            "duration_seconds, privacy_status, width, height) "
            "VALUES (?, 1, ?, 'hook', 60, 'public', 1080, 1920)",
            (video_id, title),
        )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]

    booked = datetime.now(timezone.utc) + timedelta(weeks=5)
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, "
        "scheduled_at, state) VALUES (?, 'v1', 0, ?, 'scheduled')",
        (queue_id, booked.isoformat()),
    )
    await db.commit()

    body = (await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates", json={}
    )).json()

    assert [v["id"] for v in body["eligible"]] == ["v2"]
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
        "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
        "privacy_status, width, height) "
        "VALUES ('v1', 1, 'Auto-added', 'hook', 60, 'public', 1080, 1920)"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, 'v1', 0, 'queued')",
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
        "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
        "privacy_status, width, height) "
        "VALUES ('v1', 1, 'Auto-added', 'hook', 60, 'public', 1080, 1920)"
    )
    await db.commit()
    created = await http.post(
        "/api/projects/default/smart-queues", json=_payload(template_id)
    )
    queue_id = created.json()["id"]
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, 'v1', 0, 'queued')",
        (queue_id,),
    )
    await db.commit()

    body = (await http.post(
        f"/api/projects/default/smart-queues/{queue_id}/candidates", json={}
    )).json()

    assert body["waiting"] == 1
    assert body["eligible"] == [], "a queued video is in the queue, not a candidate"
