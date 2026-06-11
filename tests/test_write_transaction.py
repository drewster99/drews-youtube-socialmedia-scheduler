"""Tests for the write_transaction() isolation primitive.

These prove the primitive behaves correctly so the (separate, larger) rollout
that routes every write through it can rely on it: it serializes concurrent
critical sections, is nesting-safe (no deadlock), and rolls back on error.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def db_mod(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    database = importlib.import_module("yt_scheduler.database")
    db = await database.get_db()
    await db.execute("CREATE TABLE claim (id INTEGER PRIMARY KEY, status TEXT)")
    await db.execute("INSERT INTO claim (id, status) VALUES (1, 'open')")
    await db.commit()
    yield database, db
    await database.close_db()


async def test_concurrent_claims_exactly_one_wins(db_mod):
    database, db = db_mod

    async def claim() -> bool:
        async with database.write_transaction() as conn:
            cur = await conn.execute(
                "UPDATE claim SET status = 'taken' WHERE id = 1 AND status = 'open'"
            )
            # Yield inside the block to maximize the chance of interleaving;
            # the lock must still serialize so only one UPDATE matches.
            await asyncio.sleep(0)
            return cur.rowcount > 0

    results = await asyncio.gather(*[claim() for _ in range(20)])
    assert sum(1 for r in results if r) == 1


async def test_nesting_does_not_deadlock(db_mod):
    database, db = db_mod

    async with database.write_transaction() as conn:
        await conn.execute("UPDATE claim SET status = 'a' WHERE id = 1")
        async with database.write_transaction() as conn2:
            assert conn2 is conn
            await conn2.execute("UPDATE claim SET status = 'b' WHERE id = 1")

    cur = await db.execute("SELECT status FROM claim WHERE id = 1")
    assert (await cur.fetchone())[0] == "b"


async def test_rollback_on_error(db_mod):
    database, db = db_mod

    with pytest.raises(RuntimeError):
        async with database.write_transaction() as conn:
            await conn.execute("UPDATE claim SET status = 'dirty' WHERE id = 1")
            raise RuntimeError("boom")

    cur = await db.execute("SELECT status FROM claim WHERE id = 1")
    assert (await cur.fetchone())[0] == "open"  # rolled back


async def test_lock_released_after_error(db_mod):
    database, db = db_mod
    # An error inside a block must release the lock so the next block proceeds.
    with pytest.raises(RuntimeError):
        async with database.write_transaction():
            raise RuntimeError("boom")
    async with database.write_transaction() as conn:
        await conn.execute("UPDATE claim SET status = 'after' WHERE id = 1")
    cur = await db.execute("SELECT status FROM claim WHERE id = 1")
    assert (await cur.fetchone())[0] == "after"
