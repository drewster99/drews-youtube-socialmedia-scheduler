"""Encrypted export/import bundle round-trips — file keychain backend only."""

from __future__ import annotations

import base64
import importlib
import json
import os
import sqlite3
import struct
import sys
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Freshly imported config/keychain/backup pointed at an isolated data dir."""
    data_dir = tmp_path / "data"
    monkeypatch.setenv("DYS_DATA_DIR", str(data_dir))
    for mod in (
        "yt_scheduler.services.backup",
        "yt_scheduler.services.keychain",
        "yt_scheduler.config",
    ):
        sys.modules.pop(mod, None)
    config = importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    backup = importlib.import_module("yt_scheduler.services.backup")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    config.ensure_dirs()
    yield config, keychain, backup, tmp_path


def _seed(config, keychain) -> None:
    with sqlite3.connect(str(config.DB_PATH)) as db:
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t (v) VALUES ('hello')")
    (config.TEMPLATES_DIR / "tpl.txt").write_text("a template")
    (config.UPLOAD_DIR / "clip.bin").write_bytes(b"\x00\x01\x02video")
    keychain.store_secret("anthropic", "api_key", "sk-ant-secret-123")
    keychain.store_secret("twitter", "cred.abc", '{"access_token":"tok"}')


def test_secrets_round_trip(env) -> None:
    _config, keychain, _backup, _tmp = env
    keychain.store_secret("anthropic", "api_key", "sk-ant-xyz")
    keychain.store_secret("bluesky", "cred.111", '{"did":"did:plc:x"}')
    snapshot = keychain.export_all_secrets()
    assert snapshot["anthropic"]["api_key"] == "sk-ant-xyz"
    assert snapshot["bluesky"]["cred.111"] == '{"did":"did:plc:x"}'

    keychain.delete_secret("anthropic", "api_key")
    assert keychain.load_secret("anthropic", "api_key") is None
    assert keychain.import_all_secrets(snapshot) == 2
    assert keychain.load_secret("anthropic", "api_key") == "sk-ant-xyz"


def test_bundle_round_trip(env) -> None:
    config, keychain, backup, tmp_path = env
    _seed(config, keychain)

    out = tmp_path / "b.dysbak"
    summary = backup.export_bundle(out, "correct horse battery staple")
    assert out.exists()
    assert summary["includes_db"] is True
    assert summary["secret_count"] == 2
    # Ciphertext must not leak the secret in cleartext.
    assert b"sk-ant-secret-123" not in out.read_bytes()
    assert oct(out.stat().st_mode)[-3:] == "600"

    # Wipe the data dir + a secret, then restore.
    keychain.delete_secret("anthropic", "api_key")
    result = backup.import_bundle(out, "correct horse battery staple")
    assert result["pre_import_path"] is not None
    assert Path(result["pre_import_path"]).exists()

    with sqlite3.connect(str(config.DB_PATH)) as db:
        assert db.execute("SELECT v FROM t").fetchone()[0] == "hello"
    assert (config.TEMPLATES_DIR / "tpl.txt").read_text() == "a template"
    assert (config.UPLOAD_DIR / "clip.bin").read_bytes() == b"\x00\x01\x02video"
    assert keychain.load_secret("anthropic", "api_key") == "sk-ant-secret-123"
    assert keychain.load_secret("twitter", "cred.abc") == '{"access_token":"tok"}'


def test_wrong_passphrase_rejected(env) -> None:
    config, keychain, backup, tmp_path = env
    _seed(config, keychain)
    out = tmp_path / "b.dysbak"
    backup.export_bundle(out, "right-passphrase")
    with pytest.raises(backup.BackupError):
        backup.import_bundle(out, "wrong-passphrase")


def test_tampered_bundle_rejected(env) -> None:
    config, keychain, backup, tmp_path = env
    _seed(config, keychain)
    out = tmp_path / "b.dysbak"
    backup.export_bundle(out, "pw")
    raw = bytearray(out.read_bytes())
    raw[-1] ^= 0x01  # flip a bit in the last GCM tag
    out.write_bytes(bytes(raw))
    with pytest.raises(backup.BackupError):
        backup.import_bundle(out, "pw")


def _record_offsets(backup, raw: bytes) -> list[int]:
    """Byte offsets where each AEAD record (its 4-byte length prefix) begins."""
    pos = raw.index(b"\n", len(backup.MAGIC)) + 1  # past MAGIC + header line
    offsets = []
    while pos < len(raw):
        offsets.append(pos)
        (rlen,) = struct.unpack(">I", raw[pos:pos + 4])
        pos += 4 + rlen
    return offsets


def test_truncated_v2_bundle_rejected(env, monkeypatch) -> None:
    """A v2 bundle truncated at a record boundary (last chunk dropped) must fail
    to decrypt rather than silently restoring an incomplete archive."""
    config, keychain, backup, tmp_path = env
    # Force multiple records so there is a non-final record to strip.
    monkeypatch.setattr(backup, "CHUNK_BYTES", 256)
    (config.UPLOAD_DIR / "big.bin").write_bytes(os.urandom(4096))
    _seed(config, keychain)
    out = tmp_path / "b.dysbak"
    backup.export_bundle(out, "pw")

    raw = out.read_bytes()
    offsets = _record_offsets(backup, raw)
    assert len(offsets) >= 2  # otherwise the truncation isn't at a boundary
    out.write_bytes(raw[: offsets[-1]])  # drop the final record

    with pytest.raises(backup.BackupError):
        backup.import_bundle(out, "pw")


def test_v1_bundle_still_imports(env) -> None:
    """A bundle written in the legacy v1 framing (no aead_version, random nonce)
    must still import unchanged."""
    config, keychain, backup, tmp_path = env
    _seed(config, keychain)

    out = tmp_path / "v1.dysbak"
    salt = os.urandom(backup.SALT_BYTES)
    key = backup._derive_key("pw", salt, backup.PBKDF2_ITERATIONS)
    header = json.dumps(
        {
            "kdf": "pbkdf2-sha256",
            "iterations": backup.PBKDF2_ITERATIONS,
            "salt": base64.b64encode(salt).decode("ascii"),
            "chunk_bytes": backup.CHUNK_BYTES,
        },
        separators=(",", ":"),
    ).encode("ascii")
    with tempfile.TemporaryDirectory() as scratch:
        tar_path = Path(scratch) / "inner.tar.gz"
        backup._build_inner_archive(tar_path)
        with open(out, "wb") as dst:
            dst.write(backup.MAGIC)
            dst.write(header)
            dst.write(b"\n")
            with open(tar_path, "rb") as src:
                backup._encrypt_stream(src, dst, key, backup.CHUNK_BYTES)

    keychain.delete_secret("anthropic", "api_key")
    backup.import_bundle(out, "pw")
    assert keychain.load_secret("anthropic", "api_key") == "sk-ant-secret-123"
    with sqlite3.connect(str(config.DB_PATH)) as db:
        assert db.execute("SELECT v FROM t").fetchone()[0] == "hello"
