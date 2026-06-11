"""Regression tests for media-path containment validation.

A client must not be able to attach an arbitrary on-disk file to a social post
and have it uploaded to a platform (arbitrary file exfiltration). Containment to
``UPLOAD_DIR`` is enforced at the write boundary (config helpers) and again at
the send boundary (the poster).
"""

from __future__ import annotations

import os

import pytest

from yt_scheduler import config
from yt_scheduler.services.social import MediaUploadError, SocialPoster


def test_in_dir_path_is_managed(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    f = tmp_path / "clip.mp4"
    f.write_text("x")
    assert config.is_managed_media_path(str(f)) is True


@pytest.mark.parametrize(
    "bad",
    ["/etc/passwd", "../../etc/passwd", "clip.mp4", "", None],
)
def test_out_of_tree_paths_rejected(tmp_path, monkeypatch, bad):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    assert config.is_managed_media_path(bad) is False


def test_symlink_escaping_upload_dir_rejected(tmp_path, monkeypatch):
    upload = tmp_path / "uploads"
    upload.mkdir()
    monkeypatch.setattr(config, "UPLOAD_DIR", upload)
    outside = tmp_path / "secret.txt"
    outside.write_text("s")
    link = upload / "evil"
    os.symlink(outside, link)
    assert config.is_managed_media_path(str(link)) is False


def test_require_managed_media_paths_raises_on_first_offender(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    good = tmp_path / "a.mp4"
    good.write_text("x")
    config.require_managed_media_paths([str(good)])  # no raise
    with pytest.raises(ValueError):
        config.require_managed_media_paths([str(good), "/etc/passwd"])


def test_poster_refuses_out_of_tree_attachment(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path)
    with pytest.raises(MediaUploadError):
        SocialPoster._require_paths_managed(["/etc/passwd"], "X")
    # containment runs before existence, so a managed-but-absent path passes
    # the managed check (it then fails the separate existence check elsewhere)
    SocialPoster._require_paths_managed([str(tmp_path / "later.mp4")], "X")
