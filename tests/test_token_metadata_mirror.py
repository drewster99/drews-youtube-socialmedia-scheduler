"""Token acquisition/expiry metadata: bundle is the source, the row mirrors it.

Every flow that mints or refreshes a token stamps ``acquired_at`` (and
``expires_at`` when the issuer reported one) into the Keychain bundle via
``stamp_token_metadata``; ``save_bundle`` and ``upsert_credential`` mirror
both onto ``social_accounts`` so the Settings list can show a token's age,
lifetime and expiry without a Keychain read per row.

Before this, only the Threads *refresh* path mirrored expiry — a fresh
connect recorded nothing, which is why every other credential showed NULL.
"""

from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def creds_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    creds = importlib.import_module("yt_scheduler.services.social_credentials")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield creds, db
    await database.close_db()


def test_stamp_records_acquisition_and_optionally_expiry() -> None:
    from yt_scheduler.services.social_credentials import stamp_token_metadata

    bundle: dict = {}
    before = int(time.time())
    stamp_token_metadata(bundle)
    assert before <= bundle["acquired_at"] <= int(time.time())
    assert "expires_at" not in bundle, "no issuer-reported lifetime, no expiry guess"

    stamp_token_metadata(bundle, expires_in_seconds=3600)
    assert bundle["expires_at"] == bundle["acquired_at"] + 3600


async def test_fresh_connect_records_both_dates(creds_env) -> None:
    """The INSERT path used to drop expiry on the floor — only re-auth of an
    existing row mirrored it, which is why fresh credentials showed NULL."""
    creds, _db = creds_env

    bundle = {"access_token": "tok"}
    creds.stamp_token_metadata(bundle, expires_in_seconds=60 * 24 * 3600)
    cred = await creds.upsert_credential("threads", "th:1", "drew", bundle)

    row = await creds.get_credential_by_uuid(cred["uuid"])
    assert row["token_acquired_at"] is not None
    assert row["token_expires_at"] is not None


async def test_save_bundle_mirrors_a_refresh_onto_the_row(creds_env) -> None:
    creds, _db = creds_env
    cred = await creds.upsert_credential("threads", "th:1", "drew", {"access_token": "old"})
    assert (await creds.get_credential_by_uuid(cred["uuid"]))["token_expires_at"] is None

    refreshed = {"access_token": "new"}
    creds.stamp_token_metadata(refreshed, expires_in_seconds=3600)
    await creds.save_bundle("threads", cred["uuid"], refreshed)

    row = await creds.get_credential_by_uuid(cred["uuid"])
    assert row["token_expires_at"] is not None
    assert row["token_acquired_at"] is not None


async def test_partial_bundle_save_keeps_known_dates(creds_env) -> None:
    """A bundle write that carries no token metadata (e.g. a rotating DPoP
    nonce on a pre-stamping bundle) must not erase dates already recorded."""
    creds, _db = creds_env
    stamped = {"access_token": "tok"}
    creds.stamp_token_metadata(stamped, expires_in_seconds=3600)
    cred = await creds.upsert_credential("bluesky", "did:1", "drew", stamped)
    before = await creds.get_credential_by_uuid(cred["uuid"])
    assert before["token_expires_at"] is not None

    await creds.save_bundle("bluesky", cred["uuid"], {"access_token": "tok", "dpop_nonce_pds": "n"})

    after = await creds.get_credential_by_uuid(cred["uuid"])
    assert after["token_expires_at"] == before["token_expires_at"]
    assert after["token_acquired_at"] == before["token_acquired_at"]


async def test_listing_carries_both_fields(creds_env) -> None:
    """The row mapper whitelists fields; a column it drops is invisible to the
    UI no matter what the table says — that is how expiry went missing once."""
    creds, _db = creds_env
    bundle = {"access_token": "tok"}
    creds.stamp_token_metadata(bundle, expires_in_seconds=3600)
    await creds.upsert_credential("threads", "th:1", "drew", bundle)

    rows = await creds.list_credentials()
    assert rows[0]["token_acquired_at"] is not None
    assert rows[0]["token_expires_at"] is not None
