"""Smart queue eligibility, validation, and occurrence maths.

Eligibility is the load-bearing piece: the config screen's Auto-select and the
live-transition hook both call ``is_eligible``, so if it were implemented
twice they would eventually disagree about which videos belong in a queue.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest


@pytest.fixture
async def queue_module(isolated_db):
    return importlib.import_module("yt_scheduler.services.smart_queue"), isolated_db


def _video(**overrides) -> dict:
    """A vertical 68s hook that is live — the eligible base case."""
    video = {
        "id": "v1", "item_type": "hook", "duration_seconds": 68.0,
        "privacy_status": "public", "archived": 0,
        "width": 1080, "height": 1920,
    }
    video.update(overrides)
    return video


def _queue(**overrides) -> dict:
    queue = {
        "id": 1, "project_id": 1, "template_id": 1,
        "min_duration_seconds": 0.0, "max_duration_seconds": 180.0,
        "orientations": '["portrait","square"]',
        "exclude_already_posted": 1, "timezone": "America/Los_Angeles",
    }
    queue.update(overrides)
    return queue


APPLIES_TO = ["hook", "short", "segment"]


class TestEligibility:
    async def test_accepts_a_live_vertical_clip(self, queue_module):
        module, _ = queue_module
        assert module.is_eligible(_video(), _queue(), APPLIES_TO).ok

    async def test_rejects_a_type_the_template_does_not_cover(self, queue_module):
        module, _ = queue_module
        verdict = module.is_eligible(
            _video(item_type="episode"), _queue(), APPLIES_TO
        )
        assert not verdict.ok
        assert "template applies to" in verdict.reasons[0]

    @pytest.mark.parametrize("privacy", ["unlisted", "private", ""])
    async def test_requires_privacy_public_not_status(self, queue_module, privacy):
        """privacy_status is the authority on liveness — `status` drifts off
        'published' whenever privacy is flipped via the metadata dropdown."""
        module, _ = queue_module
        verdict = module.is_eligible(
            _video(privacy_status=privacy, status="published"), _queue(), APPLIES_TO
        )
        assert not verdict.ok
        assert any("not live" in r for r in verdict.reasons)

    async def test_rejects_archived(self, queue_module):
        module, _ = queue_module
        assert not module.is_eligible(
            _video(archived=1), _queue(), APPLIES_TO
        ).ok

    async def test_rejects_landscape_under_default_orientations(self, queue_module):
        module, _ = queue_module
        verdict = module.is_eligible(
            _video(width=1920, height=1080), _queue(), APPLIES_TO
        )
        assert not verdict.ok
        assert any("landscape" in r for r in verdict.reasons)

    async def test_unknown_dimensions_are_their_own_reason(self, queue_module):
        """Never silently pass or fail a video we know nothing about."""
        module, _ = queue_module
        verdict = module.is_eligible(
            _video(width=None, height=None), _queue(), APPLIES_TO
        )
        assert not verdict.ok
        assert any("orientation" in r for r in verdict.reasons)

    async def test_unknown_duration_is_reported_not_assumed(self, queue_module):
        module, _ = queue_module
        verdict = module.is_eligible(
            _video(duration_seconds=None), _queue(), APPLIES_TO
        )
        assert not verdict.ok
        assert any("duration unknown" in r for r in verdict.reasons)

    async def test_duration_bounds_are_inclusive_of_the_limit(self, queue_module):
        module, _ = queue_module
        assert module.is_eligible(
            _video(duration_seconds=180.0), _queue(), APPLIES_TO
        ).ok
        assert not module.is_eligible(
            _video(duration_seconds=180.1), _queue(), APPLIES_TO
        ).ok

    async def test_reports_every_failing_condition_at_once(self, queue_module):
        """One round trip should tell the user everything that's wrong."""
        module, _ = queue_module
        verdict = module.is_eligible(
            _video(
                item_type="episode", privacy_status="unlisted",
                duration_seconds=900.0, width=1920, height=1080,
            ),
            _queue(), APPLIES_TO,
        )
        assert len(verdict.reasons) == 4


class TestOccurrences:
    async def test_holds_wall_clock_time_across_a_dst_boundary(self, queue_module):
        """US DST ends 2026-11-01. 9am local must stay 9am local, which means
        the UTC instant has to shift, not the other way round."""
        module, _ = queue_module
        zone = module.resolve_timezone("America/Los_Angeles")
        slots = [{"weekday": 0, "time_of_day": "09:00"}]
        instants = module.occurrences(
            slots, zone, 3,
            after=datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc),
        )
        local = [dt.astimezone(zone) for dt in instants]
        assert [dt.hour for dt in local] == [9, 9, 9]
        # ...and the UTC hour genuinely differs either side of the boundary.
        assert instants[0].hour != instants[-1].hour

    async def test_orders_multiple_times_within_a_day(self, queue_module):
        module, _ = queue_module
        zone = module.resolve_timezone("UTC")
        slots = [
            {"weekday": 4, "time_of_day": "18:00"},
            {"weekday": 4, "time_of_day": "12:00"},
        ]
        instants = module.occurrences(
            slots, zone, 2, after=datetime(2026, 7, 27, 0, 0, tzinfo=timezone.utc)
        )
        assert instants[0] < instants[1]
        assert [dt.hour for dt in instants] == [12, 18]

    async def test_no_slots_is_an_error_not_an_empty_schedule(self, queue_module):
        """Silently returning [] would render as 'nothing scheduled' rather
        than 'this queue can never post'."""
        module, _ = queue_module
        with pytest.raises(module.SmartQueueError):
            module.occurrences([], module.resolve_timezone("UTC"), 5)

    async def test_zero_requested_is_empty(self, queue_module):
        module, _ = queue_module
        assert module.occurrences(
            [{"weekday": 0, "time_of_day": "09:00"}],
            module.resolve_timezone("UTC"), 0,
        ) == []

    @pytest.mark.parametrize("weekday", [7, -1, "sunday", None, 3.5, True])
    async def test_refuses_a_weekday_no_date_can_match(self, queue_module, weekday):
        """occurrences() reads slots straight from the database, so a row
        _validate_queue_fields never saw reaches it. 7 and -1 used to walk the
        calendar to the year 9999 (~0.7s, then an OverflowError naming
        nothing); 3.5 and True were silently coerced to Thursday and Tuesday."""
        module, _ = queue_module
        with pytest.raises(module.SmartQueueError, match="Weekday"):
            module.occurrences(
                [{"weekday": weekday, "time_of_day": "09:00"}],
                module.resolve_timezone("UTC"), 3,
                after=datetime(2026, 7, 27, tzinfo=timezone.utc),
            )

    async def test_unknown_timezone_fails_loudly(self, queue_module):
        """No fallback to UTC: posting an 8am clip at 4pm because a zone name
        was mistyped is worse than a refused save."""
        module, _ = queue_module
        with pytest.raises(module.SmartQueueError):
            module.resolve_timezone("Mars/Olympus_Mons")


class TestValidation:
    async def _template(self, db, applies_to='["hook","short","segment"]'):
        cursor = await db.execute(
            "INSERT INTO templates (project_id, name, applies_to) VALUES (1, ?, ?)",
            (f"tpl{applies_to and len(applies_to)}", applies_to),
        )
        await db.commit()
        return int(cursor.lastrowid)

    async def test_creates_and_reads_back(self, queue_module):
        module, db = queue_module
        template_id = await self._template(db)
        queue_id = await module.create_queue(
            project_id=1, name="Clips", template_id=template_id,
            timezone_name="America/Los_Angeles",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
        )
        queue = await module.get_queue(queue_id)
        assert queue["name"] == "Clips"
        assert len(queue["slots"]) == 1
        # Defaults from the design: 3 minutes, portrait + square.
        assert queue["max_duration_seconds"] == 180
        assert "portrait" in queue["orientations"]

    async def test_rejects_a_queue_with_no_posting_times(self, queue_module):
        module, db = queue_module
        template_id = await self._template(db)
        with pytest.raises(module.SmartQueueError, match="posting time"):
            await module.create_queue(
                project_id=1, name="Nope", template_id=template_id,
                timezone_name="UTC", slots=[],
            )

    async def test_rejects_inverted_duration_range(self, queue_module):
        module, db = queue_module
        template_id = await self._template(db)
        with pytest.raises(module.SmartQueueError, match="greater than"):
            await module.create_queue(
                project_id=1, name="Nope", template_id=template_id,
                timezone_name="UTC",
                slots=[{"weekday": 0, "time_of_day": "09:00"}],
                min_duration_seconds=200, max_duration_seconds=100,
            )

    @pytest.mark.parametrize("weekday", [7, -1, "sunday", None, 3.5, True])
    async def test_refuses_a_bad_weekday_on_save(self, queue_module, weekday):
        module, db = queue_module
        template_id = await self._template(db)
        with pytest.raises(module.SmartQueueError, match="Weekday"):
            await module.create_queue(
                project_id=1, name="Nope", template_id=template_id,
                timezone_name="UTC",
                slots=[{"weekday": weekday, "time_of_day": "09:00"}],
            )

    async def test_the_schema_refuses_a_weekday_outside_0_6(self, queue_module):
        """The service is not the only writer — a hand-edited database or a
        restored bundle is one too."""
        import sqlite3

        module, db = queue_module
        template_id = await self._template(db)
        queue_id = await module.create_queue(
            project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
        )
        with pytest.raises(sqlite3.IntegrityError):
            await db.execute(
                "INSERT INTO smart_queue_slots (queue_id, weekday, time_of_day) "
                "VALUES (?, 7, '09:00')",
                (queue_id,),
            )

    async def test_rejects_unknown_orientation(self, queue_module):
        module, db = queue_module
        template_id = await self._template(db)
        with pytest.raises(module.SmartQueueError):
            await module.create_queue(
                project_id=1, name="Nope", template_id=template_id,
                timezone_name="UTC",
                slots=[{"weekday": 0, "time_of_day": "09:00"}],
                orientations=["sideways"],
            )

    async def test_post_late_requires_a_grace_window(self, queue_module):
        module, db = queue_module
        template_id = await self._template(db)
        with pytest.raises(module.SmartQueueError, match="hours"):
            await module.create_queue(
                project_id=1, name="Nope", template_id=template_id,
                timezone_name="UTC",
                slots=[{"weekday": 0, "time_of_day": "09:00"}],
                missed_policy="post_late", missed_grace_hours=None,
            )

    async def test_other_policies_clear_the_grace_window(self, queue_module):
        """Keeping a stale number would show a window that does nothing."""
        module, db = queue_module
        template_id = await self._template(db)
        queue_id = await module.create_queue(
            project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
            missed_policy="post_late", missed_grace_hours=24,
        )
        await module.update_queue(queue_id, {"missed_policy": "remove"})
        assert (await module.get_queue(queue_id))["missed_grace_hours"] is None

    async def test_updating_slots_replaces_the_whole_set(self, queue_module):
        module, db = queue_module
        template_id = await self._template(db)
        queue_id = await module.create_queue(
            project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
        )
        await module.update_queue(queue_id, {
            "slots": [
                {"weekday": 3, "time_of_day": "11:00"},
                {"weekday": 4, "time_of_day": "18:00"},
            ]
        })
        slots = (await module.get_queue(queue_id))["slots"]
        assert [(s["weekday"], s["time_of_day"]) for s in slots] == [
            (3, "11:00"), (4, "18:00")
        ]


class TestCandidates:
    async def test_excludes_videos_already_scheduled_by_this_queue(
        self, queue_module
    ):
        module, db = queue_module
        cursor = await db.execute(
            "INSERT INTO templates (project_id, name, applies_to) "
            "VALUES (1, 'tpl', '[\"hook\"]')"
        )
        template_id = int(cursor.lastrowid)
        for video_id in ("keep", "taken"):
            await db.execute(
                "INSERT INTO videos (id, project_id, title, item_type, "
                "duration_seconds, privacy_status, width, height) "
                "VALUES (?, 1, 'v', 'hook', 60, 'public', 1080, 1920)",
                (video_id,),
            )
        await db.commit()
        queue_id = await module.create_queue(
            project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
        )
        await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
            "VALUES (?, 'taken', 0, 'scheduled')",
            (queue_id,),
        )
        await db.commit()

        result = await module.candidate_videos(await module.get_queue(queue_id))
        assert [v["id"] for v in result["eligible"]] == ["keep"]

    async def test_unchecking_exclude_posted_resurfaces_them(self, queue_module):
        """This is the whole recycling mechanism — no special case needed."""
        module, db = queue_module
        cursor = await db.execute(
            "INSERT INTO templates (project_id, name, applies_to) "
            "VALUES (1, 'tpl', '[\"hook\"]')"
        )
        template_id = int(cursor.lastrowid)
        await db.execute(
            "INSERT INTO videos (id, project_id, title, item_type, "
            "duration_seconds, privacy_status, width, height) "
            "VALUES ('done', 1, 'v', 'hook', 60, 'public', 1080, 1920)"
        )
        await db.commit()
        queue_id = await module.create_queue(
            project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
        )
        await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
            "VALUES (?, 'done', 0, 'posted')",
            (queue_id,),
        )
        await db.commit()

        queue = await module.get_queue(queue_id)
        assert await module.candidate_videos(queue) == {
            "eligible": [], "excluded": [], "unknown_dimensions": 0
        }

        queue["exclude_already_posted"] = 0
        assert [v["id"] for v in
                (await module.candidate_videos(queue))["eligible"]] == ["done"]

    async def test_counts_videos_with_unknown_dimensions(self, queue_module):
        """They must be accounted for, not silently missing from the total."""
        module, db = queue_module
        cursor = await db.execute(
            "INSERT INTO templates (project_id, name, applies_to) "
            "VALUES (1, 'tpl', '[\"hook\"]')"
        )
        template_id = int(cursor.lastrowid)
        await db.execute(
            "INSERT INTO videos (id, project_id, title, item_type, "
            "duration_seconds, privacy_status) "
            "VALUES ('blind', 1, 'v', 'hook', 60, 'public')"
        )
        await db.commit()
        queue_id = await module.create_queue(
            project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
            slots=[{"weekday": 0, "time_of_day": "09:00"}],
        )
        result = await module.candidate_videos(await module.get_queue(queue_id))
        assert result["unknown_dimensions"] == 1
        assert result["eligible"] == []
