"""Encrypted export/import bundle round-trips — file keychain backend only."""

from __future__ import annotations

import importlib
import sqlite3
import sys
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
