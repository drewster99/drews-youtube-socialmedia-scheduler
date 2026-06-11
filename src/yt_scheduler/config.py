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
import urllib.parse
from pathlib import Path
from urllib.parse import urlparse

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
# Written by the server at startup and removed on clean shutdown. Checked by
# import-all so it can refuse to overwrite a live data dir.
PID_FILE = DATA_DIR / "server.pid"

# Public URL prefix under which files in ``UPLOAD_DIR`` are served (see
# ``routers/media_routes.py``). The browser only ever sees these URLs, never
# the server's absolute filesystem paths — keeps the client portable across
# machines and viable for a future remotely-hosted server / CLI client.
MEDIA_URL_PREFIX = "/media"


def media_filename(path: str | None) -> str | None:
    """Return just the basename of a stored upload path, or ``None``."""
    if not path:
        return None
    return Path(path).name


def media_url(path: str | None) -> str | None:
    """Map a stored absolute upload path to its public ``/media/<name>`` URL.

    Returns ``None`` when ``path`` is falsy. The basename is URL-encoded since
    user-uploaded files can keep their original names (spaces, ``#``, etc.).
    """
    name = media_filename(path)
    if name is None:
        return None
    return f"{MEDIA_URL_PREFIX}/{urllib.parse.quote(name)}"


def is_managed_media_path(path: str | None) -> bool:
    """True iff ``path`` resolves to a location inside ``UPLOAD_DIR``.

    Symlink-safe: ``resolve()`` follows links on both sides, so a link planted
    inside ``UPLOAD_DIR`` that points outside is rejected. A relative path
    resolves against the process CWD (outside ``UPLOAD_DIR``) and is rejected.
    Empty/``None`` is ``False``. Existence is NOT required here — containment is
    the security invariant; existence is checked separately at send time.
    """
    if not path:
        return False
    try:
        base = UPLOAD_DIR.resolve()
        target = Path(path).resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    return target.is_relative_to(base)


def require_managed_media_paths(paths: list[str]) -> None:
    """Raise ``ValueError`` naming the first path that is not inside
    ``UPLOAD_DIR``. An empty list is allowed. Used to keep client-supplied
    attachment paths from pointing at arbitrary files on disk that would then
    be uploaded to a social platform."""
    for p in paths:
        if not is_managed_media_path(p):
            raise ValueError(
                f"media path is not inside the managed media directory: {p!r}"
            )


def safe_upload_ext(filename: str | None, default: str = ".mp4") -> str:
    """A safe, lowercase file extension derived from a client-supplied
    filename — used to name on-disk upload copies.

    Never trusts the client string for anything that reaches the
    filesystem: strips both ``/`` and ``\\`` path components, and falls
    back to ``default`` for anything that isn't a short alphanumeric
    extension.
    """
    if not filename:
        return default
    base = str(filename).replace("\\", "/").rsplit("/", 1)[-1]
    _head, sep, ext = base.rpartition(".")
    if not sep:
        return default
    ext = ext.lower()
    if ext and len(ext) <= 5 and ext.isalnum():
        return f".{ext}"
    return default


def sanitized_original_filename(
    filename: str | None, limit: int = 120,
) -> str | None:
    """The client-supplied filename reduced to a basename, stripped of
    non-printable characters, and truncated — safe to store and display.

    On-disk names are chosen by the app; this is purely the remembered
    "uploaded as" label. Returns ``None`` when there's nothing usable
    (so a caller passing, say, a whole serialized object stores nothing
    rather than 120 chars of junk that happens to be printable — that
    case still truncates, but ``.``/``..``/empty are dropped outright).
    """
    if not filename:
        return None
    base = str(filename).replace("\\", "/").rsplit("/", 1)[-1]
    base = "".join(ch for ch in base if ch.isprintable()).strip()
    if not base or base in (".", ".."):
        return None
    return base[:limit]


# YouTube OAuth scopes
YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]

# Claude API key lives only in secure storage (Keychain / encrypted fallback) —
# never an env var or .env file, so it can't end up in plaintext on disk.
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")

ANTHROPIC_NAMESPACE = "anthropic"
ANTHROPIC_API_KEY_FIELD = "api_key"


def get_anthropic_api_key() -> str:
    """Load the Anthropic API key from secure storage (Keychain / encrypted fallback)."""
    from yt_scheduler.services.keychain import load_secret

    return load_secret(ANTHROPIC_NAMESPACE, ANTHROPIC_API_KEY_FIELD) or ""


# Server
HOST = os.getenv("DYS_HOST") or os.getenv("YTP_HOST", "127.0.0.1")


def _parse_int_env(primary: str, legacy: str, default: int) -> int:
    """Parse an integer environment variable, raising a clear error on bad input.

    Using a bare ``int(os.getenv(...))`` at module level produces an opaque
    ``ValueError`` traceback that doesn't identify which variable is broken.
    This wrapper names the variable in the error so the user knows exactly what
    to fix without having to read a traceback.
    """
    raw = os.getenv(primary) or os.getenv(legacy)
    if raw is None:
        return default
    raw = raw.strip()
    try:
        return int(raw)
    except ValueError:
        raise ValueError(
            f"Environment variable {primary} (or legacy {legacy}) must be an integer, "
            f"got {raw!r}"
        ) from None


PORT = _parse_int_env("DYS_PORT", "YTP_PORT", 8008)


def allowed_oauth_origins() -> list[str]:
    """The set of HTTP origins the OAuth /start endpoints will accept.

    The client posts ``{"origin": ...}`` and we build ``redirect_uri``
    from it, so this list is the security boundary that prevents a
    forged POST from diverting the OAuth ``code``/``state`` to an
    attacker-controlled host. Defaults cover loopback access; extend
    via the ``DYS_OAUTH_ALLOWED_ORIGINS`` env var (comma-separated
    fully-qualified ``http(s)://host[:port]`` values) for HTTPS
    tunnels or alternative reverse proxies.
    """
    extras = [
        o.strip().rstrip("/")
        for o in (os.getenv("DYS_OAUTH_ALLOWED_ORIGINS") or "").split(",")
        if o.strip()
    ]
    hosts: list[str] = []
    if HOST not in {"0.0.0.0", "::", "", "127.0.0.1", "localhost"}:
        hosts.append(HOST)
    hosts.extend(["127.0.0.1", "localhost"])
    base = [f"http://{h}:{PORT}" for h in hosts]
    # dict-from-keys to dedupe while preserving order
    return list(dict.fromkeys([*base, *extras]))


def resolve_oauth_origin(client_origin: str) -> str:
    """Validate and canonicalize an inbound OAuth ``origin`` value.

    Raises ``HTTPException(400)`` if the origin is missing, malformed,
    or not in the allowlist.
    """
    from fastapi import HTTPException

    raw = (client_origin or "").strip().rstrip("/")
    if not raw:
        raise HTTPException(400, "origin is required")
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username:
        raise HTTPException(
            400, "origin must be a plain http(s)://host[:port] URL",
        )
    canonical = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    allowed = {o.lower() for o in allowed_oauth_origins()}
    if canonical not in allowed:
        raise HTTPException(
            400,
            f"origin {canonical!r} is not allowed; set "
            "DYS_OAUTH_ALLOWED_ORIGINS to permit it",
        )
    return canonical

# Public HTTPS "bounce" URL used as the OAuth ``redirect_uri`` for Threads.
# Meta refuses to register/redirect to plain ``http://`` URIs, so when the
# app runs locally over http the redirect has to land on an HTTPS page that
# forwards ``?code&state`` back to this server's /api/oauth/threads/callback.
# The default points at the nuclearcyborg.com static bounce page (the source
# of which lives in ``cloudflare/`` in this repo); override with the
# ``DYS_THREADS_REDIRECT_URL`` env var if you host the bounce page elsewhere.
_DEFAULT_THREADS_REDIRECT_URL = (
    "https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect"
)
THREADS_REDIRECT_URL = (
    os.getenv("DYS_THREADS_REDIRECT_URL") or _DEFAULT_THREADS_REDIRECT_URL
).strip().rstrip("/")

# Scheduler
COMMENT_CHECK_INTERVAL_MINUTES = _parse_int_env(
    "DYS_COMMENT_CHECK_MINUTES", "YTP_COMMENT_CHECK_MINUTES", 30
)
CAPTION_CHECK_INTERVAL_MINUTES = _parse_int_env(
    "DYS_CAPTION_CHECK_MINUTES", "YTP_CAPTION_CHECK_MINUTES", 15
)


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    for d in [DATA_DIR, LOG_DIR, TEMPLATES_DIR, UPLOAD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
