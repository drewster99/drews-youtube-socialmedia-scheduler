"""Full encrypted export / import of the app's local state.

A ``.dysbak`` bundle contains everything needed to move the app to another Mac:
the whole data directory (``publisher.db`` as a consistent snapshot, ``templates/``,
``uploads/``, the ``secrets.json`` key-name index, legacy credential files) plus
every secret resolved out of the macOS Keychain.

File layout::

    DYSBAK1\\n
    {"kdf":"pbkdf2-sha256","iterations":600000,"salt":"<b64>","chunk_bytes":1048576}\\n
    <record><record>...

Each record is ``[4-byte BE length][12-byte nonce][AES-256-GCM ciphertext+tag]``
covering one <= ``chunk_bytes`` plaintext slice of the inner ``tar.gz`` archive.
The chunk's ordinal index is bound as GCM associated data so reordered or
truncated bundles fail to decrypt. The AES key is PBKDF2-HMAC-SHA256 over the
user's passphrase. Streaming in chunks keeps memory bounded regardless of how
large ``uploads/`` is.

Inner archive members:
  * ``manifest.json``        — bundle metadata (app id, format version, timestamp)
  * ``keychain-secrets.json``— ``{namespace: {key: value}}`` of all secrets
  * ``data/...``             — contents of the data dir (with a clean DB snapshot)
"""

from __future__ import annotations

import base64
import datetime as _dt
import json
import logging
import os
import shutil
import sqlite3
import stat
import struct
import tarfile
import tempfile
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from yt_scheduler import config
from yt_scheduler.services import keychain

logger = logging.getLogger(__name__)

MAGIC = b"DYSBAK1\n"
FORMAT_VERSION = 1
PBKDF2_ITERATIONS = 600_000
SALT_BYTES = 16
NONCE_BYTES = 12
CHUNK_BYTES = 1 << 20  # 1 MiB plaintext per GCM record
_DB_SIDE_FILES = ("publisher.db-wal", "publisher.db-shm")
_SKIP_TOP_LEVEL = {"logs"}  # never ship logs


class BackupError(Exception):
    """Raised for malformed bundles, wrong passphrases, or unsafe import state."""


# --- key derivation + streaming AEAD ---


def _derive_key(passphrase: str, salt: bytes, iterations: int) -> bytes:
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=iterations)
    return kdf.derive(passphrase.encode("utf-8"))


def _encrypt_stream(src, dst, key: bytes, chunk_bytes: int) -> None:
    aead = AESGCM(key)
    index = 0
    while True:
        chunk = src.read(chunk_bytes)
        if not chunk:
            break
        nonce = os.urandom(NONCE_BYTES)
        ct = aead.encrypt(nonce, chunk, struct.pack(">Q", index))
        dst.write(struct.pack(">I", len(nonce) + len(ct)))
        dst.write(nonce)
        dst.write(ct)
        index += 1


def _read_exactly(src, n: int) -> bytes:
    buf = src.read(n)
    if len(buf) != n:
        raise BackupError("Backup file is truncated or corrupt")
    return buf


def _decrypt_stream(src, dst, key: bytes) -> None:
    aead = AESGCM(key)
    index = 0
    while True:
        length_prefix = src.read(4)
        if not length_prefix:
            break
        if len(length_prefix) != 4:
            raise BackupError("Backup file is truncated or corrupt")
        (record_len,) = struct.unpack(">I", length_prefix)
        if record_len <= NONCE_BYTES:
            raise BackupError("Backup file is corrupt")
        record = _read_exactly(src, record_len)
        nonce, ct = record[:NONCE_BYTES], record[NONCE_BYTES:]
        try:
            plaintext = aead.decrypt(nonce, ct, struct.pack(">Q", index))
        except InvalidTag as exc:
            raise BackupError("Wrong passphrase, or the backup file is corrupt") from exc
        dst.write(plaintext)
        index += 1
    if index == 0:
        raise BackupError("Backup file contains no data")


# --- inner archive ---


def _snapshot_db(dest: Path) -> bool:
    """Write a consistent copy of the live SQLite DB to ``dest``. Returns False if no DB."""
    if not config.DB_PATH.exists():
        return False
    # Open the live DB (read/write is fine — .backup() never modifies the source)
    # and snapshot it; this is consistent even if the server is using it.
    # busy_timeout is per-connection: under WAL a live server may hold the write
    # lock, so without it a concurrent `export-all` would fail instantly with
    # "database is locked" instead of waiting for the lock to clear.
    src = sqlite3.connect(str(config.DB_PATH), timeout=5.0)
    src.execute("PRAGMA busy_timeout = 5000")
    dst = sqlite3.connect(str(dest))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return True


def _build_inner_archive(tar_path: Path) -> dict:
    data_dir = config.DATA_DIR
    summary = {"data_files": 0, "secret_count": 0, "includes_db": False}
    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        db_snapshot = scratch_dir / "publisher.db"
        has_db = _snapshot_db(db_snapshot)
        summary["includes_db"] = has_db

        secrets = keychain.export_all_secrets()
        summary["secret_count"] = sum(len(v) for v in secrets.values())
        secrets_path = scratch_dir / "keychain-secrets.json"
        secrets_path.write_text(json.dumps(secrets, indent=2))

        manifest = {
            "app": config.BUNDLE_ID,
            "format_version": FORMAT_VERSION,
            "exported_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "host": os.uname().nodename,
            "includes_db": has_db,
        }
        manifest_path = scratch_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(manifest_path, arcname="manifest.json")
            tar.add(secrets_path, arcname="keychain-secrets.json")
            if has_db:
                tar.add(db_snapshot, arcname="data/publisher.db")
            if data_dir.exists():
                for root, _dirs, files in os.walk(data_dir):
                    for name in files:
                        full = Path(root) / name
                        rel = full.relative_to(data_dir)
                        if rel.parts and rel.parts[0] in _SKIP_TOP_LEVEL:
                            continue
                        if rel.name in {"publisher.db", "server.pid"} or rel.name in _DB_SIDE_FILES:
                            continue
                        tar.add(full, arcname=str(Path("data") / rel))
                        summary["data_files"] += 1
    return summary


# --- public API ---


def export_bundle(out_path: Path, passphrase: str) -> dict:
    """Write an encrypted ``.dysbak`` bundle to ``out_path``. Returns a summary dict."""
    if not passphrase:
        raise BackupError("A passphrase is required")
    out_path = Path(out_path)
    salt = os.urandom(SALT_BYTES)
    key = _derive_key(passphrase, salt, PBKDF2_ITERATIONS)
    header = json.dumps(
        {
            "kdf": "pbkdf2-sha256",
            "iterations": PBKDF2_ITERATIONS,
            "salt": base64.b64encode(salt).decode("ascii"),
            "chunk_bytes": CHUNK_BYTES,
        },
        separators=(",", ":"),
    ).encode("ascii")

    with tempfile.TemporaryDirectory() as scratch:
        tar_path = Path(scratch) / "bundle.tar.gz"
        summary = _build_inner_archive(tar_path)
        tmp_out = out_path.with_name(out_path.name + ".partial")
        try:
            with open(tmp_out, "wb") as dst:
                dst.write(MAGIC)
                dst.write(header)
                dst.write(b"\n")
                with open(tar_path, "rb") as src:
                    _encrypt_stream(src, dst, key, CHUNK_BYTES)
            os.replace(tmp_out, out_path)
        except BaseException:
            tmp_out.unlink(missing_ok=True)
            raise

    os.chmod(out_path, stat.S_IRUSR | stat.S_IWUSR)
    summary["bytes"] = out_path.stat().st_size
    summary["path"] = str(out_path)
    return summary


def _parse_header(src) -> dict:
    magic = src.read(len(MAGIC))
    if magic != MAGIC:
        raise BackupError("Not a Drew's Scheduler backup file")
    line = bytearray()
    while True:
        ch = src.read(1)
        if not ch:
            raise BackupError("Backup file is truncated or corrupt")
        if ch == b"\n":
            break
        line.extend(ch)
        if len(line) > 4096:
            raise BackupError("Backup file header is malformed")
    try:
        header = json.loads(bytes(line).decode("ascii"))
        header["_salt_bytes"] = base64.b64decode(header["salt"])
        header["_iterations"] = int(header["iterations"])
    except (ValueError, KeyError) as exc:
        raise BackupError("Backup file header is malformed") from exc
    if not (1_000 <= header["_iterations"] <= 10_000_000) or not (8 <= len(header["_salt_bytes"]) <= 1_024):
        raise BackupError("Backup file header is malformed")
    return header


def _extracted_data_root(extracted: Path) -> Path:
    manifest_path = extracted / "manifest.json"
    if not manifest_path.exists():
        raise BackupError("Backup is missing its manifest")
    try:
        manifest = json.loads(manifest_path.read_text())
    except ValueError as exc:
        raise BackupError("Backup manifest is malformed") from exc
    if manifest.get("app") != config.BUNDLE_ID:
        raise BackupError("Backup was produced by a different application")
    if manifest.get("format_version") != FORMAT_VERSION:
        raise BackupError(f"Unsupported backup format version: {manifest.get('format_version')}")
    return extracted


def import_bundle(in_path: Path, passphrase: str) -> dict:
    """Restore an encrypted ``.dysbak`` bundle. Overwrites the data dir + Keychain.

    The previous data dir is renamed to ``<name>.pre-import-<timestamp>`` and kept.
    Returns a summary dict including ``pre_import_path`` (or ``None``).
    """
    if not passphrase:
        raise BackupError("A passphrase is required")
    in_path = Path(in_path)
    if not in_path.exists():
        raise BackupError(f"Backup file not found: {in_path}")

    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        tar_path = scratch_dir / "bundle.tar.gz"
        with open(in_path, "rb") as src:
            header = _parse_header(src)
            key = _derive_key(passphrase, header["_salt_bytes"], header["_iterations"])
            with open(tar_path, "wb") as dst:
                _decrypt_stream(src, dst, key)

        extracted = scratch_dir / "extracted"
        extracted.mkdir()
        with tarfile.open(tar_path, "r:gz") as tar:
            _safe_extract(tar, extracted)
        _extracted_data_root(extracted)

        secrets_path = extracted / "keychain-secrets.json"
        try:
            secrets = json.loads(secrets_path.read_text())
        except (OSError, ValueError) as exc:
            raise BackupError("Backup is missing or has a malformed secrets file") from exc

        new_data = extracted / "data"
        if not new_data.is_dir():
            raise BackupError("Backup is missing its data directory")

        data_dir = config.DATA_DIR
        pre_import_path: Path | None = None
        if data_dir.exists():
            ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            pre_import_path = data_dir.with_name(f"{data_dir.name}.pre-import-{ts}")
            os.rename(data_dir, pre_import_path)
        data_dir.mkdir(parents=True, exist_ok=True)
        for entry in new_data.iterdir():
            shutil.move(str(entry), str(data_dir / entry.name))
        for stale_name in (*_DB_SIDE_FILES, "server.pid"):
            stale = data_dir / stale_name
            if stale.exists():
                stale.unlink()

        secret_count = keychain.import_all_secrets(secrets)

    config.ensure_dirs()
    return {
        "secret_count": secret_count,
        "pre_import_path": str(pre_import_path) if pre_import_path else None,
        "data_dir": str(config.DATA_DIR),
    }


def _safe_extract(tar: tarfile.TarFile, dest: Path) -> None:
    """Extract ``tar`` into ``dest``, rejecting paths that escape ``dest``."""
    dest = dest.resolve()
    for member in tar.getmembers():
        target = (dest / member.name).resolve()
        if not (target == dest or dest in target.parents):
            raise BackupError(f"Backup contains an unsafe path: {member.name}")
        if member.issym() or member.islnk():
            raise BackupError(f"Backup contains a link, which is not allowed: {member.name}")
    try:
        tar.extractall(dest, filter="data")
    except TypeError:
        # `filter=` predates Python 3.11.4; our own checks above already cover safety.
        tar.extractall(dest)
