"""import_bundle must never leave a half-populated data directory.

The old restore renamed the live data dir away, mkdir'd a fresh one, then moved
entries in one at a time. Any failure part-way (disk full, permissions, a raise
from import_all_secrets) left DATA_DIR partly built with no rollback, and the
user had to find and rename the .pre-import-* dir back by hand.

All tests operate on tmp dirs and the file keychain backend; the real data dir is
never touched.
"""

from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest

PASSPHRASE = "correct horse battery staple"


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
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


def _seed(config, keychain, marker: str) -> None:
    with sqlite3.connect(str(config.DB_PATH)) as db:
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t (v) VALUES (?)", (marker,))
    (config.TEMPLATES_DIR / "tpl.txt").write_text(marker)
    keychain.store_secret("anthropic", "api_key", f"sk-{marker}")


def _db_marker(config) -> str:
    with sqlite3.connect(str(config.DB_PATH)) as db:
        return db.execute("SELECT v FROM t").fetchone()[0]


def _staging_dirs(tmp_path: Path) -> list[Path]:
    return list(tmp_path.glob("*.import-staging-*"))


def _make_bundle(config, keychain, backup, tmp_path: Path) -> Path:
    """Export a bundle whose contents differ from the current live data."""
    _seed(config, keychain, "ORIGINAL")
    bundle = tmp_path / "backup.dysbak"
    backup.export_bundle(bundle, PASSPHRASE)
    return bundle


def test_successful_import_leaves_no_staging_dir(env) -> None:
    config, keychain, backup, tmp_path = env
    bundle = _make_bundle(config, keychain, backup, tmp_path)

    result = backup.import_bundle(bundle, PASSPHRASE)

    assert _db_marker(config) == "ORIGINAL"
    assert _staging_dirs(tmp_path) == []
    assert result["pre_import_path"] is not None
    assert Path(result["pre_import_path"]).is_dir()


def test_secret_import_failure_rolls_back_data_dir(env, monkeypatch) -> None:
    """A raise from import_all_secrets must restore the original data dir."""
    config, keychain, backup, tmp_path = env
    bundle = _make_bundle(config, keychain, backup, tmp_path)

    # Replace the live data with something distinguishable from the bundle.
    config.DB_PATH.unlink()
    _seed(config, keychain, "LIVE")
    assert _db_marker(config) == "LIVE"

    def boom(_secrets):
        raise RuntimeError("keychain wedged")

    monkeypatch.setattr(keychain, "import_all_secrets", boom)

    with pytest.raises(backup.BackupError) as excinfo:
        backup.import_bundle(bundle, PASSPHRASE)

    assert "rolled back" in str(excinfo.value)
    # The original live data survived intact.
    assert _db_marker(config) == "LIVE"
    assert (config.TEMPLATES_DIR / "tpl.txt").read_text() == "LIVE"
    assert _staging_dirs(tmp_path) == []


def test_swap_failure_leaves_original_in_place(env, monkeypatch) -> None:
    """If the staged->live rename fails, the original must be renamed back."""
    config, keychain, backup, tmp_path = env
    bundle = _make_bundle(config, keychain, backup, tmp_path)

    config.DB_PATH.unlink()
    _seed(config, keychain, "LIVE")

    real_rename = backup.os.rename

    def flaky_rename(src, dst):
        # Keyed on the destination, not a call counter: shutil.move also calls
        # os.rename internally, so counting would fire on the wrong operation.
        if Path(dst) == config.DATA_DIR and ".import-staging-" in Path(src).name:
            raise OSError("simulated swap failure")
        return real_rename(src, dst)

    monkeypatch.setattr(backup.os, "rename", flaky_rename)

    with pytest.raises(backup.BackupError) as excinfo:
        backup.import_bundle(bundle, PASSPHRASE)

    assert "left in place" in str(excinfo.value)
    assert _db_marker(config) == "LIVE"
    assert _staging_dirs(tmp_path) == []


def test_staging_is_a_sibling_of_the_data_dir(env, monkeypatch) -> None:
    """Staging must share a filesystem with DATA_DIR or the swap can hit EXDEV."""
    config, keychain, backup, tmp_path = env
    bundle = _make_bundle(config, keychain, backup, tmp_path)

    real_move = backup.shutil.move
    seen: list[Path] = []

    def spy_move(src, dst):
        seen.append(Path(dst))
        return real_move(src, dst)

    monkeypatch.setattr(backup.shutil, "move", spy_move)
    backup.import_bundle(bundle, PASSPHRASE)

    staged = [p for p in seen if ".import-staging-" in p.name]
    assert staged, "expected the incoming tree to be staged before the swap"
    for path in staged:
        assert path.parent == config.DATA_DIR.parent, (
            f"staging {path} is not a sibling of {config.DATA_DIR}"
        )


def test_rollback_failure_reports_both_paths(env, monkeypatch) -> None:
    """If rollback itself fails, the error must tell the user where the data is."""
    config, keychain, backup, tmp_path = env
    bundle = _make_bundle(config, keychain, backup, tmp_path)

    config.DB_PATH.unlink()
    _seed(config, keychain, "LIVE")

    def boom(_secrets):
        raise RuntimeError("keychain wedged")

    monkeypatch.setattr(keychain, "import_all_secrets", boom)

    real_rename = backup.os.rename

    def rename_failing_during_rollback(src, dst):
        # Let the swap succeed; fail only the rollback's move-aside of the
        # partially-restored dir.
        if ".import-broken-" in Path(dst).name:
            raise OSError("simulated rollback failure")
        return real_rename(src, dst)

    monkeypatch.setattr(backup.os, "rename", rename_failing_during_rollback)

    with pytest.raises(backup.BackupError) as excinfo:
        backup.import_bundle(bundle, PASSPHRASE)

    message = str(excinfo.value)
    assert "rollback failed" in message
    assert ".pre-import-" in message
    assert str(config.DATA_DIR) in message
