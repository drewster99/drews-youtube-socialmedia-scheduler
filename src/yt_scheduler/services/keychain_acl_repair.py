"""One-shot repair of macOS Keychain access-control lists (ACLs).

Through 2026-06, every Keychain read and write went through the Apple-signed
``/usr/bin/security`` CLI. Writes used ``-T /usr/bin/security`` so each item's
ACL trusted that one binary, and reads ran it as a subprocess — so macOS never
prompted, because the accessing application was always already in the ACL.

The ``harden`` change moved reads and writes in-process, calling the Security
framework directly via ctypes (keeps secrets off argv, returns exact bytes).
That made the *accessing application* the embedded ``python3.12`` — which is
NOT in any pre-existing item's ACL. So on the first launch after that change,
macOS prompts "python3.12 wants to use your confidential information…" once per
stored secret (YouTube, every social platform, Anthropic) — a storm of dialogs.

This migration repairs every already-stored item exactly once: it reads the
value prompt-free via the ``security`` CLI (still trusted by the old ACL),
deletes the item, and re-adds it via the framework so the new item's default
ACL trusts the creating app (``python3.12``). Because the app is Developer-ID
signed with a stable designated requirement (identifier + team), that trust
holds across every future rebuild, so the prompts don't come back. Items
written *after* this migration are already created in-process and need no
repair.

Progress is tracked in a small state file so a crash mid-run never re-reads an
already-repaired item (whose ACL no longer trusts ``/usr/bin/security``, which
would itself prompt). The state file holds only service/account *names* and a
flag — never any secret value.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
import tempfile

from yt_scheduler.services import keychain

logger = logging.getLogger(__name__)

STATE_FILE_NAME = "keychain_acl_repair.json"

# Items whose index sentinel is exactly this are stored in the Keychain and so
# may carry a stale ACL. Any other index value is a plaintext file-fallback
# secret (no Keychain item to repair).
_KEYCHAIN_SENTINEL = "__keychain__"


def _state_path():
    """Resolve the repair-state file next to the secrets index.

    Derived from ``keychain.SECRETS_FILE`` (not a captured ``DATA_DIR``) so it
    follows the same data directory the keychain module is using, including in
    tests that point ``DYS_DATA_DIR`` at a temp dir.
    """
    return keychain.SECRETS_FILE.parent / STATE_FILE_NAME


def _load_state() -> dict:
    path = _state_path()
    if not path.exists():
        return {"done": False, "completed": []}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {"done": False, "completed": []}
    if not isinstance(data, dict):
        return {"done": False, "completed": []}
    return data


def _save_state(done: bool, completed: set[str]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"done": done, "completed": sorted(completed)}, indent=2)
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=".acl_repair.", suffix=".tmp", dir=str(path.parent),
    )
    try:
        os.fchmod(tmp_fd, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(tmp_fd, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _item_id(service: str, account: str) -> str:
    # NUL can't appear in a service or account name, so it's an unambiguous join.
    return f"{service}\x00{account}"


def _repair_all_keychain_acls() -> int:
    """Repair stale Keychain ACLs in place. Idempotent. Returns items repaired.

    Runs only on macOS. Walks the secrets index; for each Keychain-backed item
    not yet repaired it does CLI-read → delete → framework re-add, persisting
    progress after every item so an interrupted run resumes without re-reading
    an already-repaired item.
    """
    if not keychain._is_macos():
        return 0

    state = _load_state()
    if state.get("done"):
        return 0

    completed: set[str] = set(state.get("completed", []))
    index = keychain._load_secrets_file()
    repaired = 0

    for service, accounts in list(index.items()):
        if not isinstance(accounts, dict):
            continue
        for account, sentinel in list(accounts.items()):
            item_id = _item_id(service, account)
            if item_id in completed:
                continue

            if sentinel != _KEYCHAIN_SENTINEL:
                # Plaintext file-fallback value — there is no Keychain item to
                # repair. Mark done so we never touch it again.
                completed.add(item_id)
                _save_state(False, completed)
                continue

            value = keychain._keychain_get_cli(service, account)
            if value is None:
                # No Keychain item under this (service, account) — gone or
                # file-only. Marking it done keeps the resume path from ever
                # CLI-reading it again.
                completed.add(item_id)
                _save_state(False, completed)
                continue

            if not keychain._keychain_delete(service, account):
                # Couldn't remove the original; re-adding would hit the
                # duplicate path and PRESERVE the stale ACL. Leave it
                # incomplete and retry next boot rather than mark a false win.
                logger.error(
                    "Keychain ACL repair: could not delete %s/%s before re-add; "
                    "will retry next boot", service, account,
                )
                continue

            try:
                re_added = keychain._keychain_set(service, account, value)
            except keychain.KeychainWriteError as exc:
                # The original item is already deleted, so an escaping raise here
                # would lose the secret outright. Fall into the restore branch.
                logger.error(
                    "Keychain ACL repair: keychain unavailable for %s/%s after "
                    "delete (%s); restoring old-scheme item", service, account, exc,
                )
                re_added = False

            if re_added:
                readback = keychain._keychain_get(service, account)
                if readback == value:
                    completed.add(item_id)
                    repaired += 1
                    _save_state(False, completed)
                    continue
                logger.error(
                    "Keychain ACL repair: re-added %s/%s but read-back mismatched; "
                    "restoring old-scheme item", service, account,
                )
            else:
                logger.error(
                    "Keychain ACL repair: framework re-add failed for %s/%s; "
                    "restoring old-scheme item", service, account,
                )
            # Re-add (or its verification) failed after the delete — put the
            # secret back in old-scheme form so nothing is lost, and leave the
            # item incomplete so the next boot retries.
            keychain._keychain_set_cli_trusted(service, account, value)

    _save_state(True, completed)
    if repaired:
        logger.info(
            "Keychain ACL repair: rewrote %d item(s) to trust the embedded "
            "interpreter; per-item access prompts should no longer appear.",
            repaired,
        )
    return repaired


async def repair_keychain_acls() -> int:
    """Run the ACL repair off the event loop. Best-effort: never raises."""
    try:
        return await asyncio.to_thread(_repair_all_keychain_acls)
    except Exception as exc:
        logger.warning(
            "Keychain ACL repair failed (will retry next boot): %s", exc,
        )
        return 0
