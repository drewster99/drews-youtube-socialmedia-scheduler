"""Secure credential storage — macOS Keychain with file fallback.

On macOS: writes secrets directly via the Security framework (ctypes) to avoid
exposing secret values on the process argv; reads them back via the `security` CLI.
On other platforms: stores secrets in a JSON file at ~/.drews-yt-scheduler/secrets.json
with restrictive file permissions (600).

Each credential is stored as a separate Keychain item identified by
(service, account) — e.g.,
("com.nuclearcyborg.drews-socialmedia-scheduler.twitter", "api_key").
The legacy ``com.youtube-publisher.*`` namespace is read on miss and migrated
forward transparently.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import logging
import os
import platform
import stat
import subprocess
import tempfile
import threading

from yt_scheduler.config import DATA_DIR

logger = logging.getLogger(__name__)

KEYCHAIN_SERVICE_PREFIX = "com.nuclearcyborg.drews-socialmedia-scheduler"
LEGACY_KEYCHAIN_SERVICE_PREFIX = "com.youtube-publisher"
SECRETS_FILE = DATA_DIR / "secrets.json"


class KeychainWriteError(RuntimeError):
    """A keychain write could not be completed safely.

    Raised when the Security framework is unusable (missing, or its lock is held
    by a wedged call) instead of falling back to `security add-generic-password
    -w <value>`, which would expose the secret on the process argv.
    """


class SecretsIndexError(RuntimeError):
    """The secrets index exists but could not be read or parsed.

    Raised rather than treated as empty: this index is the sole account
    enumerator, so silently returning {} would make load_all_secrets /
    export_all_secrets / delete_all_secrets act as if no credentials exist.
    """

# Guards the read-modify-write cycle on the on-disk index. We don't
# hold this across the `security` subprocess call — it wraps just the
# index file mutations inside the public helpers.
_secrets_file_lock = threading.Lock()

# Serializes EVERY in-process Security.framework (SecKeychain*) call. Apple's
# legacy SecKeychain* API is not safe for concurrent use from multiple threads
# in one process: two threads inside it at once can wedge on Keychain's internal
# object mutex and never return, freezing every caller. We hit exactly this in
# 2026-06 — the asyncio event loop in SecKeychainAddGenericPassword (a synchronous
# token store) and an upload worker thread in SecKeychainItemModifyAttributesAndData
# (a YouTube credential-refresh write), both parked forever on __psynch_mutexwait,
# which froze the whole server. Holding this lock across each framework critical
# section guarantees only one thread is ever inside Security.framework, removing
# the precondition for that deadlock. Reentrant so a nested framework call on the
# same thread can't self-deadlock.
_keychain_framework_lock = threading.RLock()

# Upper bound on how long to wait for `_keychain_framework_lock` before giving up.
# A healthy keychain op completes in well under a second; a wait longer than this
# means the current holder is wedged, and blocking the caller (often the event
# loop) forever is worse than a logged fallback to the CLI/file path.
_KEYCHAIN_FRAMEWORK_LOCK_TIMEOUT_SECS = 20.0


# --- macOS Keychain ---


def _is_macos() -> bool:
    return platform.system() == "Darwin"


# Lazy-loaded handle for the macOS Security framework. Initialised once on
# first use and reused across calls so we don't pay dlopen overhead per write.
_sec_lib: ctypes.CDLL | None = None
_SEC_LIB_UNAVAILABLE = False  # set True on first failed load so we stop retrying

_ERR_SEC_DUPLICATE_ITEM = -25299


def _get_sec_lib() -> ctypes.CDLL | None:
    global _sec_lib, _SEC_LIB_UNAVAILABLE
    if _SEC_LIB_UNAVAILABLE:
        return None
    if _sec_lib is not None:
        return _sec_lib
    path = ctypes.util.find_library("Security")
    if not path:
        _SEC_LIB_UNAVAILABLE = True
        return None
    try:
        lib = ctypes.CDLL(path)
        lib.SecKeychainAddGenericPassword.restype = ctypes.c_int32
        lib.SecKeychainAddGenericPassword.argtypes = [
            ctypes.c_void_p,   # keychain (NULL = default)
            ctypes.c_uint32,   # serviceNameLength
            ctypes.c_char_p,   # serviceName
            ctypes.c_uint32,   # accountNameLength
            ctypes.c_char_p,   # accountName
            ctypes.c_uint32,   # passwordLength
            ctypes.c_void_p,   # passwordData
            ctypes.c_void_p,   # itemRef (NULL = not needed)
        ]
        lib.SecKeychainFindGenericPassword.restype = ctypes.c_int32
        lib.SecKeychainFindGenericPassword.argtypes = [
            ctypes.c_void_p,                    # keychain
            ctypes.c_uint32,                    # serviceNameLength
            ctypes.c_char_p,                    # serviceName
            ctypes.c_uint32,                    # accountNameLength
            ctypes.c_char_p,                    # accountName
            ctypes.POINTER(ctypes.c_uint32),    # passwordLength (out)
            ctypes.POINTER(ctypes.c_void_p),    # passwordData (out)
            ctypes.c_void_p,                    # itemRef (out)
        ]
        lib.SecKeychainItemModifyAttributesAndData.restype = ctypes.c_int32
        lib.SecKeychainItemModifyAttributesAndData.argtypes = [
            ctypes.c_void_p,   # item
            ctypes.c_void_p,   # attrList (NULL = no attribute changes)
            ctypes.c_uint32,   # length
            ctypes.c_void_p,   # data
        ]
        lib.SecKeychainItemFreeContent.restype = ctypes.c_int32
        lib.SecKeychainItemFreeContent.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        _sec_lib = lib
        return lib
    except OSError:
        _SEC_LIB_UNAVAILABLE = True
        return None


_cf_lib: ctypes.CDLL | None = None
_CF_LIB_UNAVAILABLE = False


def _get_cf_lib() -> ctypes.CDLL | None:
    """Lazily load CoreFoundation for CFRelease (memory management of CF refs)."""
    global _cf_lib, _CF_LIB_UNAVAILABLE
    if _CF_LIB_UNAVAILABLE:
        return None
    if _cf_lib is not None:
        return _cf_lib
    path = ctypes.util.find_library("CoreFoundation")
    if not path:
        _CF_LIB_UNAVAILABLE = True
        return None
    try:
        lib = ctypes.CDLL(path)
        lib.CFRelease.restype = None
        lib.CFRelease.argtypes = [ctypes.c_void_p]
        _cf_lib = lib
        return lib
    except OSError:
        _CF_LIB_UNAVAILABLE = True
        return None


def _cf_release(ref: ctypes.c_void_p) -> None:
    """Release a CF ref returned with the Create/Copy/RETAINED ownership rule.

    SecKeychainFindGenericPassword's itemRef out-param is CF_RETURNS_RETAINED,
    so the caller owns it and must CFRelease it to avoid leaking the item ref
    on every credential overwrite (re-OAuth / token refresh).
    """
    if not ref or not ref.value:
        return
    cf = _get_cf_lib()
    if cf is not None:
        cf.CFRelease(ref)


def _keychain_set(service: str, account: str, value: str) -> bool:
    """Store a value in macOS Keychain without exposing the secret on argv.

    Calls SecKeychainAddGenericPassword directly via ctypes rather than
    passing the secret as a `-w <value>` command-line argument to
    `security add-generic-password`, which would expose it to all local users
    via the process table (ps/libproc).

    There is no safe non-argv CLI write (`security` accepts the password only
    via `-w <value>` or an interactive TTY prompt), so when the framework is
    unusable this raises ``KeychainWriteError`` rather than leaking the secret.
    A genuine SecKeychain error code still returns False, leaving the caller's
    existing encrypted-file fallback intact.
    """
    svc_b = service.encode()
    acct_b = account.encode()
    val_b = value.encode("utf-8")

    lib = _get_sec_lib()
    if lib is None:
        raise KeychainWriteError(
            f"Security framework unavailable; refusing to write {service}/{account} "
            "via the security CLI (that would expose the secret on the process argv)"
        )
    if not _keychain_framework_lock.acquire(
        timeout=_KEYCHAIN_FRAMEWORK_LOCK_TIMEOUT_SECS
    ):
        # Another framework call is wedged. The CLI fallback would put the secret
        # on argv, which is exactly the leak the ctypes path exists to avoid —
        # and contention is precisely when it would fire. Surface instead.
        raise KeychainWriteError(
            f"Keychain framework lock not acquired within "
            f"{_KEYCHAIN_FRAMEWORK_LOCK_TIMEOUT_SECS:.0f}s for set "
            f"{service}/{account}; another keychain call appears wedged"
        )
    try:
        status = lib.SecKeychainAddGenericPassword(
            None,
            len(svc_b), svc_b,
            len(acct_b), acct_b,
            len(val_b), val_b,
            None,
        )
        if status == _ERR_SEC_DUPLICATE_ITEM:
            # Item already exists; find it and overwrite the password data.
            pw_len = ctypes.c_uint32(0)
            pw_data = ctypes.c_void_p(None)
            item_ref = ctypes.c_void_p(None)
            find_status = lib.SecKeychainFindGenericPassword(
                None,
                len(svc_b), svc_b,
                len(acct_b), acct_b,
                ctypes.byref(pw_len),
                ctypes.byref(pw_data),
                ctypes.byref(item_ref),
            )
            if find_status != 0:
                logger.warning("SecKeychainFindGenericPassword returned %d for %s/%s", find_status, service, account)
                return False
            try:
                lib.SecKeychainItemFreeContent(None, pw_data)
                mod_status = lib.SecKeychainItemModifyAttributesAndData(
                    item_ref, None, len(val_b), val_b,
                )
                if mod_status != 0:
                    logger.warning("SecKeychainItemModifyAttributesAndData returned %d for %s/%s", mod_status, service, account)
                    return False
            finally:
                # itemRef is CF_RETURNS_RETAINED — release it so the update
                # path doesn't leak a keychain item ref on every refresh.
                _cf_release(item_ref)
        elif status != 0:
            logger.warning("SecKeychainAddGenericPassword returned %d for %s/%s", status, service, account)
            return False
        return True
    except Exception:
        logger.exception("Security framework call failed for %s/%s", service, account)
        return False
    finally:
        _keychain_framework_lock.release()


def _keychain_get_cli(service: str, account: str) -> str | None:
    """Read a secret via the ``security`` CLI subprocess.

    The process that actually touches the item is ``/usr/bin/security``
    (Apple-signed), so items written under the pre-2026-06 scheme — whose ACL
    trusts ``/usr/bin/security`` via ``-T`` and NOT the embedded ``python3.12``
    — are read WITHOUT the "python3.12 wants to use your confidential
    information" prompt that an in-process Security-framework read triggers.

    The ACL-repair migration relies on this to read legacy items prompt-free
    before rewriting them with a self-trusting ACL.
    """
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", service, "-a", account, "-w"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            # `security` appends exactly one trailing newline; remove only that.
            raw = result.stdout
            return raw[:-1] if raw.endswith("\n") else raw
        return None
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        logger.warning("security find-generic-password timed out for %s/%s", service, account)
        return None


def _keychain_set_cli_trusted(service: str, account: str, value: str) -> bool:
    """Re-create an item via the CLI, trusting ``/usr/bin/security`` (restore path).

    Used only to undo a half-finished ACL repair: if the in-process framework
    re-add fails *after* the original item was deleted, we put the secret back
    in its old-scheme form (``-T /usr/bin/security``) so nothing is lost and the
    next boot can retry. The secret is on argv here, exactly like the
    pre-2026-06 write path — acceptable because this runs only on the rare
    repair-failure branch, not on the normal write path.
    """
    try:
        subprocess.run(
            ["security", "delete-generic-password", "-s", service, "-a", account],
            capture_output=True,
            timeout=15,
        )
        result = subprocess.run(
            [
                "security", "add-generic-password",
                "-s", service, "-a", account, "-w", value,
                "-U",
                "-T", "/usr/bin/security",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except subprocess.TimeoutExpired:
        logger.warning("security add-generic-password (trusted) timed out for %s/%s", service, account)
        return False


def _keychain_get(service: str, account: str) -> str | None:
    """Load a value from macOS Keychain.

    Reads via the Security framework (ctypes) so the EXACT stored bytes come
    back. The `security` CLI's ``-w`` output hex-encodes any password that
    contains a newline or non-ASCII byte (and there's no marker to tell hex
    output apart from a genuinely hex-looking secret), which silently corrupts
    such values. The framework read returns the raw bytes with no such
    ambiguity, and also preserves leading/trailing whitespace. Falls back to
    the CLI only if the framework can't be loaded (never on macOS).
    """
    svc_b = service.encode()
    acct_b = account.encode()

    lib = _get_sec_lib()
    if lib is not None:
        if _keychain_framework_lock.acquire(
            timeout=_KEYCHAIN_FRAMEWORK_LOCK_TIMEOUT_SECS
        ):
            try:
                pw_len = ctypes.c_uint32(0)
                pw_data = ctypes.c_void_p(None)
                status = lib.SecKeychainFindGenericPassword(
                    None,
                    len(svc_b), svc_b,
                    len(acct_b), acct_b,
                    ctypes.byref(pw_len),
                    ctypes.byref(pw_data),
                    None,  # itemRef not requested → nothing retained, nothing to release
                )
                if status != 0:
                    # errSecItemNotFound (-25300) or any other lookup failure → absent.
                    return None
                try:
                    raw = ctypes.string_at(pw_data, pw_len.value) if pw_data.value else b""
                finally:
                    lib.SecKeychainItemFreeContent(None, pw_data)
                return raw.decode("utf-8")
            except Exception:
                logger.exception("Security framework read failed for %s/%s; trying CLI", service, account)
                # fall through to the CLI fallback
            finally:
                _keychain_framework_lock.release()
        else:
            logger.error(
                "Keychain framework lock not acquired within %.0fs for get %s/%s; "
                "another keychain call appears wedged — using CLI fallback",
                _KEYCHAIN_FRAMEWORK_LOCK_TIMEOUT_SECS, service, account,
            )

    return _keychain_get_cli(service, account)


def _keychain_delete(service: str, account: str) -> bool:
    """Delete a value from macOS Keychain."""
    try:
        result = subprocess.run(
            ["security", "delete-generic-password", "-s", service, "-a", account],
            capture_output=True,
            timeout=15,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except subprocess.TimeoutExpired:
        logger.warning("security delete-generic-password timed out for %s/%s", service, account)
        return False


def _keychain_find_all(service: str) -> list[str]:
    """Find all account names for a Keychain service.

    Uses `security dump-keychain` which is slow but there's no better option.
    Falls back to tracking accounts in the secrets file index.
    """
    # We track account names in a local index to avoid parsing dump-keychain
    return _file_list_accounts(service)


# --- File-based storage (fallback + account index) ---


def _load_secrets_file() -> dict:
    """Load the secrets JSON index.

    A missing file is the legitimate first-run/empty state. A present-but-corrupt
    or unreadable one raises: this index enumerates every stored account, so
    treating it as empty would silently hide real credentials from load_all /
    export_all / delete_all.
    """
    if not SECRETS_FILE.exists():
        return {}
    try:
        raw = SECRETS_FILE.read_text()
    except OSError as exc:
        raise SecretsIndexError(
            f"Could not read secrets index {SECRETS_FILE}: {exc}"
        ) from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SecretsIndexError(
            f"Secrets index {SECRETS_FILE} is corrupt (invalid JSON): {exc}. "
            "Refusing to treat it as empty; fix or restore the file."
        ) from exc
    if not isinstance(data, dict):
        raise SecretsIndexError(
            f"Secrets index {SECRETS_FILE} is corrupt: expected a JSON object, "
            f"got {type(data).__name__}."
        )
    return data


def _save_secrets_file(data: dict) -> None:
    """Save the secrets JSON file with restrictive permissions.

    Writes via tempfile + ``os.replace`` so the file is never visible
    in an incomplete state (no half-written JSON survives a crash mid-
    write) and is owner-only from the moment it exists (``mkstemp``
    creates with mode 0o600, closing the brief window where the old
    ``write_text`` + ``chmod`` pair left the file 0o644).
    """
    SECRETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2)
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=".secrets.", suffix=".tmp", dir=str(SECRETS_FILE.parent),
    )
    try:
        # mkstemp already creates with 0o600; this is belt-and-braces
        # for platforms where the default may differ.
        os.fchmod(tmp_fd, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(tmp_fd, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, SECRETS_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _file_set(service: str, account: str, value: str) -> None:
    """Store a value in the secrets file."""
    with _secrets_file_lock:
        data = _load_secrets_file()
        if service not in data:
            data[service] = {}
        data[service][account] = value
        _save_secrets_file(data)


def _file_get(service: str, account: str) -> str | None:
    """Load a value from the secrets file."""
    data = _load_secrets_file()
    return data.get(service, {}).get(account)


def _file_delete(service: str, account: str) -> None:
    """Delete a value from the secrets file."""
    with _secrets_file_lock:
        data = _load_secrets_file()
        if service in data and account in data[service]:
            del data[service][account]
            if not data[service]:
                del data[service]
            _save_secrets_file(data)


def _file_list_accounts(service: str) -> list[str]:
    """List all account names for a service in the secrets file."""
    data = _load_secrets_file()
    return list(data.get(service, {}).keys())


# --- Public API ---


def _service_name(namespace: str) -> str:
    """Build a Keychain service name from a namespace."""
    return f"{KEYCHAIN_SERVICE_PREFIX}.{namespace}"


def _legacy_service_name(namespace: str) -> str:
    """Build the pre-rename Keychain service name."""
    return f"{LEGACY_KEYCHAIN_SERVICE_PREFIX}.{namespace}"


def store_secret(namespace: str, key: str, value: str) -> None:
    """Store a secret credential.

    Args:
        namespace: Credential group (e.g., "youtube", "twitter", "bluesky")
        key: Credential key (e.g., "access_token", "api_key")
        value: The secret value
    """
    service = _service_name(namespace)

    if _is_macos():
        # Write the index sentinel BEFORE calling Keychain so that a crash
        # between the two leaves a stale-but-harmless index entry rather than
        # an orphaned Keychain item that is invisible to load_all/export/delete.
        # If _save_secrets_file raises here we never reach _keychain_set, so
        # nothing is orphaned; the exception propagates and the caller retries.
        # If _keychain_set returns False (a genuine SecKeychain error code) we
        # fall through to _file_set, which overwrites the "__keychain__" sentinel
        # with the real value — correct. If it RAISES KeychainWriteError (wedged
        # or missing framework) we deliberately do NOT fall through: writing the
        # secret to disk in plaintext would trade one leak for another. The
        # sentinel is then left pointing at an item that was never written, which
        # is harmless — load_secret returns None for it and export_all_secrets
        # skips entries that don't resolve.
        with _secrets_file_lock:
            data = _load_secrets_file()
            data.setdefault(service, {})[key] = "__keychain__"
            _save_secrets_file(data)
        if _keychain_set(service, key, value):
            return
        logger.warning("Keychain store failed for %s/%s, using file fallback", namespace, key)

    _file_set(service, key, value)


def load_secret(namespace: str, key: str) -> str | None:
    """Load a secret credential.

    Tries the current Keychain service ID first, then the legacy ID
    (`com.youtube-publisher.*`) and the file fallback. When a value is found
    under the legacy ID, it is migrated forward to the new ID transparently.

    Args:
        namespace: Credential group
        key: Credential key

    Returns:
        The secret value, or None if not found.
    """
    service = _service_name(namespace)
    legacy_service = _legacy_service_name(namespace)

    if _is_macos():
        value = _keychain_get(service, key)
        if value is not None:
            return value

        # Read-fallback to legacy Keychain service ID and migrate forward.
        legacy_value = _keychain_get(legacy_service, key)
        if legacy_value is not None:
            try:
                migrated = _keychain_set(service, key, legacy_value)
            except KeychainWriteError as exc:
                # The forward-migration is opportunistic. Never let a *write*
                # problem fail this *read* — return the value and retry later.
                logger.warning(
                    "Deferred legacy migration of %s/%s: %s", namespace, key, exc
                )
                migrated = False
            if migrated:
                with _secrets_file_lock:
                    data = _load_secrets_file()
                    data.setdefault(service, {})[key] = "__keychain__"
                    _save_secrets_file(data)
                logger.info("Migrated %s/%s from legacy Keychain ID", namespace, key)
            return legacy_value

    # Fallback to file (current then legacy service name)
    value = _file_get(service, key)
    if value is None:
        value = _file_get(legacy_service, key)
        if value is not None and value != "__keychain__":
            # Move legacy file entry forward
            _file_set(service, key, value)
            _file_delete(legacy_service, key)
    if value and value != "__keychain__":
        # Migrate to Keychain if on macOS
        if _is_macos():
            try:
                migrated = _keychain_set(service, key, value)
            except KeychainWriteError as exc:
                # Opportunistic migration; a wedged Keychain must not fail a read.
                logger.warning(
                    "Deferred file→Keychain migration of %s/%s: %s",
                    namespace, key, exc,
                )
                migrated = False
            if migrated:
                with _secrets_file_lock:
                    data = _load_secrets_file()
                    if service in data and key in data[service]:
                        data[service][key] = "__keychain__"
                        _save_secrets_file(data)
                logger.info(f"Migrated {namespace}/{key} to Keychain")
        return value

    return None


def delete_secret(namespace: str, key: str) -> None:
    """Delete a secret credential."""
    service = _service_name(namespace)
    if _is_macos():
        _keychain_delete(service, key)
    _file_delete(service, key)


def load_all_secrets(namespace: str) -> dict[str, str]:
    """Load all secrets for a namespace.

    Returns:
        Dict of key -> value for all stored credentials in this namespace.
    """
    service = _service_name(namespace)
    accounts = _file_list_accounts(service)
    result = {}
    for account in accounts:
        value = load_secret(namespace, account)
        if value:
            result[account] = value
    return result


def delete_all_secrets(namespace: str) -> None:
    """Delete all secrets for a namespace."""
    service = _service_name(namespace)
    accounts = _file_list_accounts(service)
    for account in accounts:
        delete_secret(namespace, account)


def get_storage_type() -> str:
    """Return the active storage backend name."""
    return "keychain" if _is_macos() else "file"


def _namespace_from_service(service: str) -> str | None:
    """Recover the namespace from a full Keychain service name, or ``None``."""
    for prefix in (KEYCHAIN_SERVICE_PREFIX, LEGACY_KEYCHAIN_SERVICE_PREFIX):
        if service.startswith(prefix + "."):
            return service[len(prefix) + 1 :]
    return None


def export_all_secrets() -> dict[str, dict[str, str]]:
    """Resolve every stored secret as ``{namespace: {key: value}}``.

    Namespaces and keys are enumerated from the local index file; each real
    value is read via :func:`load_secret` (which transparently reads the
    Keychain and migrates the legacy namespace forward). Entries that no longer
    resolve are skipped. Both the current and legacy service prefixes are
    considered, de-duplicated by namespace.
    """
    index = _load_secrets_file()
    keys_by_namespace: dict[str, set[str]] = {}
    for service, accounts in index.items():
        namespace = _namespace_from_service(service)
        if namespace is None:
            continue
        keys_by_namespace.setdefault(namespace, set()).update(accounts.keys())

    result: dict[str, dict[str, str]] = {}
    for namespace, keys in keys_by_namespace.items():
        for key in sorted(keys):
            value = load_secret(namespace, key)
            if value is None:
                continue
            result.setdefault(namespace, {})[key] = value
    return result


def import_all_secrets(data: dict[str, dict[str, str]]) -> int:
    """Write every ``{namespace: {key: value}}`` entry via :func:`store_secret`.

    Returns the number of individual secrets written.
    """
    count = 0
    for namespace, entries in data.items():
        for key, value in entries.items():
            store_secret(namespace, key, value)
            count += 1
    return count


# --- Async wrappers for use from FastAPI / scheduler paths --------------
# The sync helpers above shell out to the macOS `security` CLI per call
# (~100ms-1s each) and do blocking file I/O on the fallback path. Calling
# them directly from async code freezes the event loop for every other
# request. Wrap with `asyncio.to_thread` at every async call site.


async def store_secret_async(namespace: str, key: str, value: str) -> None:
    import asyncio as _asyncio
    await _asyncio.to_thread(store_secret, namespace, key, value)


async def load_secret_async(namespace: str, key: str) -> str | None:
    import asyncio as _asyncio
    return await _asyncio.to_thread(load_secret, namespace, key)


async def delete_secret_async(namespace: str, key: str) -> None:
    import asyncio as _asyncio
    await _asyncio.to_thread(delete_secret, namespace, key)


async def load_all_secrets_async(namespace: str) -> dict[str, str]:
    import asyncio as _asyncio
    return await _asyncio.to_thread(load_all_secrets, namespace)


async def delete_all_secrets_async(namespace: str) -> None:
    import asyncio as _asyncio
    await _asyncio.to_thread(delete_all_secrets, namespace)


async def export_all_secrets_async() -> dict[str, dict[str, str]]:
    import asyncio as _asyncio
    return await _asyncio.to_thread(export_all_secrets)


async def import_all_secrets_async(data: dict[str, dict[str, str]]) -> int:
    import asyncio as _asyncio
    return await _asyncio.to_thread(import_all_secrets, data)
