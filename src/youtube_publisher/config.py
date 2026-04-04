"""Application configuration."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Paths
DATA_DIR = Path(os.getenv("YTP_DATA_DIR", Path.home() / ".youtube-publisher"))
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
    from youtube_publisher.services.keychain import load_secret

    value = load_secret(ANTHROPIC_NAMESPACE, ANTHROPIC_API_KEY_FIELD)
    if value:
        return value
    return _ANTHROPIC_API_KEY_ENV

# Server
HOST = os.getenv("YTP_HOST", "127.0.0.1")
PORT = int(os.getenv("YTP_PORT", "8008"))

# Scheduler
COMMENT_CHECK_INTERVAL_MINUTES = int(os.getenv("YTP_COMMENT_CHECK_MINUTES", "30"))
CAPTION_CHECK_INTERVAL_MINUTES = int(os.getenv("YTP_CAPTION_CHECK_MINUTES", "15"))


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    for d in [DATA_DIR, TEMPLATES_DIR, UPLOAD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
