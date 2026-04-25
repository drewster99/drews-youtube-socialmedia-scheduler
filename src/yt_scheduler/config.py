"""Application configuration.

Path resolution order on macOS (which is the only supported runtime today):

1. ``DYS_DATA_DIR`` / ``DYS_LOG_DIR`` env vars — power-user override, used by
   the .app's Swift launcher to inject the correct sandbox-aware paths.
2. Apple-standard locations:
   * data → ``~/Library/Application Support/<bundle_id>/``
   * logs → ``~/Library/Logs/<bundle_id>/``
3. On non-macOS, an XDG-ish fallback under ``~/.local/share`` and
   ``~/.local/state``.

The .app builds always set the env vars from Swift via FileManager so the
sandbox container path is honoured automatically. Direct ``yt-scheduler``
runs (terminal / pip install -e) hit branches 2 / 3.
"""

from __future__ import annotations

import logging
import os
import platform
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BUNDLE_ID = "com.nuclearcyborg.drews-socialmedia-scheduler"


def _macos_app_support_dir() -> Path:
    return Path.home() / "Library" / "Application Support" / BUNDLE_ID


def _macos_log_dir() -> Path:
    return Path.home() / "Library" / "Logs" / BUNDLE_ID


def _xdg_data_dir() -> Path:
    base = os.getenv("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(base) / BUNDLE_ID


def _xdg_state_dir() -> Path:
    base = os.getenv("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(base) / BUNDLE_ID / "logs"


def _resolve_data_dir() -> Path:
    env = os.getenv("DYS_DATA_DIR") or os.getenv("YTP_DATA_DIR")
    if env:
        return Path(env)
    if platform.system() == "Darwin":
        return _macos_app_support_dir()
    return _xdg_data_dir()


def _resolve_log_dir() -> Path:
    env = os.getenv("DYS_LOG_DIR")
    if env:
        return Path(env)
    if platform.system() == "Darwin":
        try:
            target = _macos_log_dir()
            target.mkdir(parents=True, exist_ok=True)
            return target
        except OSError as exc:
            logger.warning(
                "Could not use ~/Library/Logs/%s (%s); falling back to data dir/logs",
                BUNDLE_ID, exc,
            )
            return _resolve_data_dir() / "logs"
    return _xdg_state_dir()


# Paths
DATA_DIR = _resolve_data_dir()
LOG_DIR = _resolve_log_dir()
DB_PATH = DATA_DIR / "publisher.db"
TEMPLATES_DIR = DATA_DIR / "templates"
UPLOAD_DIR = DATA_DIR / "uploads"

# YouTube OAuth scopes
YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]

# Claude API — env var as fallback; Keychain is checked at runtime via get_anthropic_api_key()
_ANTHROPIC_API_KEY_ENV = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

ANTHROPIC_NAMESPACE = "anthropic"
ANTHROPIC_API_KEY_FIELD = "api_key"


def get_anthropic_api_key() -> str:
    """Load the Anthropic API key, preferring Keychain over env var."""
    from yt_scheduler.services.keychain import load_secret

    value = load_secret(ANTHROPIC_NAMESPACE, ANTHROPIC_API_KEY_FIELD)
    if value:
        return value
    return _ANTHROPIC_API_KEY_ENV


# Server
HOST = os.getenv("DYS_HOST") or os.getenv("YTP_HOST", "127.0.0.1")
PORT = int(os.getenv("DYS_PORT") or os.getenv("YTP_PORT", "8008"))

# Scheduler
COMMENT_CHECK_INTERVAL_MINUTES = int(
    os.getenv("DYS_COMMENT_CHECK_MINUTES") or os.getenv("YTP_COMMENT_CHECK_MINUTES", "30")
)
CAPTION_CHECK_INTERVAL_MINUTES = int(
    os.getenv("DYS_CAPTION_CHECK_MINUTES") or os.getenv("YTP_CAPTION_CHECK_MINUTES", "15")
)


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    for d in [DATA_DIR, LOG_DIR, TEMPLATES_DIR, UPLOAD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
