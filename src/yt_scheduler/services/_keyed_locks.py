"""Per-key ``asyncio.Lock`` registry with weak-ref eviction.

Replaces the ``if k not in d: d[k] = Lock()`` pattern that we'd been
hand-rolling in scheduler.py and social_credentials.py — that pattern
both has a latent race (the check and the set aren't a single
bytecode if anyone introduces an ``await`` between them later) and
grows the dict forever for keys that have come and gone.

Callers must hold a strong reference to the returned lock for as long
as they intend to use it (``async with locks.get(k):`` does this
naturally). Once nobody holds the lock anymore the
``WeakValueDictionary`` entry is GC'd, so the registry can't grow
unboundedly.
"""

from __future__ import annotations

import asyncio
import weakref
from typing import Generic, Hashable, TypeVar

K = TypeVar("K", bound=Hashable)


class KeyedLocks(Generic[K]):
    """Registry that hands out one :class:`asyncio.Lock` per key."""

    def __init__(self) -> None:
        self._locks: "weakref.WeakValueDictionary[K, asyncio.Lock]" = (
            weakref.WeakValueDictionary()
        )

    def get(self, key: K) -> asyncio.Lock:
        """Return the lock for ``key``, creating it atomically if needed.

        ``dict.setdefault`` is atomic under the GIL, so the throwaway
        Lock on contention is GC'd immediately. The caller MUST keep
        a strong reference to the returned lock across the critical
        section — ``lock = locks.get(k); async with lock: ...`` is
        the idiomatic form.
        """
        lock = self._locks.get(key)
        if lock is None:
            new_lock = asyncio.Lock()
            # setdefault returns whatever's in the dict, so two racing
            # callers both end up with the same lock object even though
            # the loser's ``new_lock`` is discarded.
            lock = self._locks.setdefault(key, new_lock)
        return lock
