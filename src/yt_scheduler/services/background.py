"""Fire-and-forget task helper that doesn't swallow exceptions.

Replaces the ``asyncio.create_task + _pending.add +
add_done_callback(_pending.discard)`` pattern that several services had
copy-pasted. The bug it fixes: the discard-only callback never
inspects ``task.exception()``, so a crash inside the coroutine only
surfaces via asyncio's GC-time "Task exception was never retrieved"
warning — which is unreliable in a long-running server and impossible
to surface to the user.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)

# Strong refs so the event loop's GC doesn't drop a task mid-flight
# (asyncio only holds weak refs to tasks).
_background_tasks: set[asyncio.Task] = set()


def spawn_background(coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task:
    """Schedule ``coro`` as a fire-and-forget task with exception logging.

    Holds a strong reference until the task completes and, on done,
    retrieves ``task.exception()`` so the unhandled-task warning
    never fires. Logs failures with a full traceback under ``name``.
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _done(t: asyncio.Task) -> None:
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.error("background task %r raised", name, exc_info=exc)

    task.add_done_callback(_done)
    return task
