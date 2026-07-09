"""The active-project binding must stay task-local.

Routes call set_active_project(slug) and later `await asyncio.to_thread(...)`,
where the worker reads the slug to pick OAuth credentials. That is only safe
because _active_project_slug is a ContextVar (each request task gets its own
copy) and asyncio.to_thread snapshots the caller's context.

If someone demotes it to a module global, or replaces to_thread with a raw
executor submit that skips copy_context, concurrent requests would silently
upload to the wrong YouTube channel. These tests fail loudly if that happens.
"""

from __future__ import annotations

import asyncio
import contextvars
import importlib
from pathlib import Path


def test_active_project_slug_is_a_contextvar(isolated_data_dir: Path) -> None:
    auth = importlib.import_module("yt_scheduler.services.auth")
    assert isinstance(auth._active_project_slug, contextvars.ContextVar)


async def test_to_thread_sees_this_tasks_slug_not_a_concurrent_one(
    isolated_data_dir: Path,
) -> None:
    """Two concurrent 'requests' must each see their own project slug."""
    auth = importlib.import_module("yt_scheduler.services.auth")

    both_have_set = asyncio.Barrier(2)
    observed: dict[str, str | None] = {}

    async def request(slug: str) -> None:
        auth.set_active_project(slug)
        # Force the interleaving the reporter worried about: neither task reads
        # its slug until both have written theirs.
        await both_have_set.wait()
        observed[slug] = await asyncio.to_thread(auth._active_project_slug.get)

    await asyncio.gather(request("alpha"), request("beta"))

    assert observed == {"alpha": "alpha", "beta": "beta"}
