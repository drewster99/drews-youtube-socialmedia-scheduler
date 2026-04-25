"""Application configuration."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _resolve_data_dir() -> Path:
    """Pick the data dir, performing a one-shot rename from the legacy location.

    Order of preference:
      1. Explicit env var (``DYS_DATA_DIR``, with legacy ``YTP_DATA_DIR`` fallback).
      2. ``~/.drews-yt-scheduler`` (new default).
      3. ``~/.yt-scheduler`` (legacy default) — renamed to (2) if the new
         dir doesn't exist yet.
    """
    env = os.getenv("DYS_DATA_DIR") or os.getenv("YTP_DATA_DIR")
    if env:
        return Path(env)

    home = Path.home()
    new_default = home / ".drews-yt-scheduler"
    legacy_default = home / ".yt-scheduler"

    if new_default.exists():
        return new_default

    if legacy_default.exists():
        try:
            legacy_default.rename(new_default)
            logger.info("Migrated data directory %s → %s", legacy_default, new_default)
        except OSError as exc:
            logger.warning(
                "Could not rename legacy data directory %s → %s: %s; "
                "continuing with the legacy path",
                legacy_default,
                new_default,
                exc,
            )
            return legacy_default

    return new_default


# Paths
DATA_DIR = _resolve_data_dir()
DB_PATH = DATA_DIR / "publisher.db"
CREDENTIALS_PATH = DATA_DIR / "credentials.json"
CLIENT_SECRETS_PATH = DATA_DIR / "client_secret.json"
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
    """Load the Anthropic API key, preferring Keychain over env var.

    Avoids circular import by importing keychain lazily.
    """
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
    for d in [DATA_DIR, TEMPLATES_DIR, UPLOAD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
