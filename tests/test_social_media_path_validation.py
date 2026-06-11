"""Regression tests for media-path containment validation.

A client must not be able to attach an arbitrary on-disk file to a social post
and have it uploaded to a platform (arbitrary file exfiltration). Containment to
``UPLOAD_DIR`` is enforced at the write boundary (config helpers) and again at
the send boundary (the poster).
"""

from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture
def config():
    """The live ``yt_scheduler.config`` module.

    Resolved from ``sys.modules`` at call time (not a collection-time top-level
    import) so the monkeypatch lands on the same module instance the production
    code imports — robust even when another test in the session reloads the
    ``yt_scheduler`` package via ``sys.modules.pop``.
    """
    import yt_scheduler.config  # noqa: F401 — ensure it's imported

    return sys.modules["yt_scheduler.config"]


def test_in_dir_path_is_managed(tmp_path, monkeypatch, config):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    f = tmp_path / "clip.mp4"
    f.write_text("x")
    assert config.is_managed_media_path(str(f)) is True


@pytest.mark.parametrize(
    "bad",
    ["/etc/passwd", "../../etc/passwd", "clip.mp4", "", None],
)
def test_out_of_tree_paths_rejected(tmp_path, monkeypatch, config, bad):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    assert config.is_managed_media_path(bad) is False


def test_symlink_escaping_upload_dir_rejected(tmp_path, monkeypatch, config):
    upload = tmp_path / "uploads"
    upload.mkdir()
    monkeypatch.setattr(config, "UPLOAD_DIR", upload)
    outside = tmp_path / "secret.txt"
    outside.write_text("s")
    link = upload / "evil"
    os.symlink(outside, link)
    assert config.is_managed_media_path(str(link)) is False


def test_require_managed_media_paths_raises_on_first_offender(tmp_path, monkeypatch, config):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    good = tmp_path / "a.mp4"
    good.write_text("x")
    config.require_managed_media_paths([str(good)])  # no raise
    with pytest.raises(ValueError):
        config.require_managed_media_paths([str(good), "/etc/passwd"])


def test_poster_refuses_out_of_tree_attachment(tmp_path, monkeypatch, config):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    # Resolve the poster from the live module too, for the same reload-safety.
    social = sys.modules.get("yt_scheduler.services.social")
    if social is None:
        import yt_scheduler.services.social as social  # noqa: F811
    with pytest.raises(social.MediaUploadError):
        social.SocialPoster._require_paths_managed(["/etc/passwd"], "X")
    # containment runs before existence, so a managed-but-absent path passes
    # the managed check (it then fails the separate existence check elsewhere)
    social.SocialPoster._require_paths_managed([str(tmp_path / "later.mp4")], "X")
