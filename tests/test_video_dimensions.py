"""Cached frame dimensions, and orientation derived from them.

Only width/height are stored — orientation is derived, so there is no second
column to fall out of sync. The Python and SQL derivations must agree, since
the config screen filters in SQL and the eligibility check reads in Python.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


@pytest.fixture
async def dimensions(isolated_db):
    """Resolve the module lazily: isolated_db re-freezes config by purging
    yt_scheduler.* from sys.modules, so a module captured at import time
    would be a dead object talking to a different database."""
    return importlib.import_module("yt_scheduler.services.video_dimensions"), isolated_db


class TestOrientation:
    @pytest.mark.parametrize(
        "width,height,expected",
        [
            (1080, 1920, "portrait"),
            (1920, 1080, "landscape"),
            (1080, 1080, "square"),
            (2160, 3840, "portrait"),
        ],
    )
    async def test_derives_orientation(self, dimensions, width, height, expected):
        module, _ = dimensions
        assert module.orientation_of(width, height) == expected

    @pytest.mark.parametrize("width,height", [(None, 1920), (1080, None), (0, 0)])
    async def test_unknown_is_not_square(self, dimensions, width, height):
        """Unknown must be its own state — treating it as square would put
        videos we know nothing about into an orientation filter's results."""
        module, _ = dimensions
        assert module.orientation_of(width, height) is None

    async def test_sql_derivation_matches_python(self, dimensions):
        module, db = dimensions
        cases = [
            ("a", 1080, 1920), ("b", 1920, 1080), ("c", 1080, 1080),
            ("d", None, None), ("e", 1080, None),
        ]
        for video_id, width, height in cases:
            await db.execute(
                "INSERT INTO videos (id, project_id, title, width, height) "
                "VALUES (?, 1, 'v', ?, ?)",
                (video_id, width, height),
            )
        await db.commit()

        rows = await db.execute_fetchall(
            f"SELECT id, width, height, {module.ORIENTATION_SQL} AS orientation "
            "FROM videos ORDER BY id"
        )
        for row in rows:
            assert row["orientation"] == module.orientation_of(
                row["width"], row["height"]
            ), f"SQL and Python disagree for {row['id']}"


class TestStamping:
    async def test_missing_file_leaves_dimensions_unknown(self, dimensions):
        """No silent zero — a row with nothing to probe stays NULL."""
        module, db = dimensions
        await db.execute(
            "INSERT INTO videos (id, project_id, title, video_file_path) "
            "VALUES ('gone', 1, 'v', '/nonexistent/x.mp4')"
        )
        await db.commit()

        assert await module.stamp_dimensions("gone", "/nonexistent/x.mp4") is None
        rows = await db.execute_fetchall(
            "SELECT width, height FROM videos WHERE id = 'gone'"
        )
        assert rows[0]["width"] is None and rows[0]["height"] is None

    async def test_ensure_returns_cached_without_probing(
        self, dimensions, monkeypatch
    ):
        module, db = dimensions
        await db.execute(
            "INSERT INTO videos (id, project_id, title, width, height) "
            "VALUES ('cached', 1, 'v', 1080, 1920)"
        )
        await db.commit()

        probed = []
        monkeypatch.setattr(
            module.media_service, "probe_video_file",
            lambda p: probed.append(p),
        )
        assert await module.ensure_dimensions("cached") == (1080, 1920)
        assert probed == []

    async def test_ensure_raises_for_unknown_video(self, dimensions):
        module, _ = dimensions
        with pytest.raises(ValueError):
            await module.ensure_dimensions("no-such-video")

    async def test_backfill_skips_rows_without_a_local_file(self, dimensions):
        module, db = dimensions
        await db.execute(
            "INSERT INTO videos (id, project_id, title) VALUES ('nofile', 1, 'v')"
        )
        await db.commit()
        assert await module.backfill_video_dimensions() == 0

    async def test_backfill_stamps_from_a_real_file(
        self, dimensions, tmp_path, monkeypatch
    ):
        module, db = dimensions
        video = tmp_path / "real.mp4"
        video.write_bytes(b"x")
        await db.execute(
            "INSERT INTO videos (id, project_id, title, video_file_path) "
            "VALUES ('real', 1, 'v', ?)",
            (str(video),),
        )
        await db.commit()

        monkeypatch.setattr(
            module.media_service, "probe_video_file",
            lambda p: module.media_service.VideoProbe(
                duration_seconds=10.0, width=1080, height=1920,
                bitrate_bps=1, size_bytes=1,
            ),
        )
        assert await module.backfill_video_dimensions() == 1
        rows = await db.execute_fetchall(
            "SELECT width, height FROM videos WHERE id = 'real'"
        )
        assert (rows[0]["width"], rows[0]["height"]) == (1080, 1920)

    async def test_one_bad_file_does_not_abort_the_backfill(
        self, dimensions, tmp_path, monkeypatch
    ):
        module, db = dimensions
        good, bad = tmp_path / "good.mp4", tmp_path / "bad.mp4"
        good.write_bytes(b"x")
        bad.write_bytes(b"x")
        await db.execute(
            "INSERT INTO videos (id, project_id, title, video_file_path) "
            "VALUES ('bad', 1, 'v', ?)", (str(bad),),
        )
        await db.execute(
            "INSERT INTO videos (id, project_id, title, video_file_path) "
            "VALUES ('good', 1, 'v', ?)", (str(good),),
        )
        await db.commit()

        def flaky(path):
            # Match the file name, not the whole path: pytest's tmp_path is
            # named after the test, which itself contains "bad".
            if Path(path).name == "bad.mp4":
                raise OSError("unreadable")
            return module.media_service.VideoProbe(
                duration_seconds=1.0, width=1920, height=1080,
                bitrate_bps=1, size_bytes=1,
            )

        monkeypatch.setattr(module.media_service, "probe_video_file", flaky)
        assert await module.backfill_video_dimensions() == 1
