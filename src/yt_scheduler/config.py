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


def derived_media_dir() -> Path:
    """The one directory under ``UPLOAD_DIR`` whose contents are all disposable.

    It holds nothing but short-lived re-encodes made at send time when a source
    breached a platform's limits. No DB row points at one, none is reachable
    over ``/media`` (that route takes a bare filename and rejects a separator),
    and the writer deletes its own output when the send finishes. A file that
    outlives the process that wrote it is therefore garbage by definition —
    which is what lets the startup sweep delete on sight, where the general
    ``UPLOAD_DIR`` janitor in ROADMAP.md must identify every file first.
    Nothing else may write here.

    A function rather than a module constant because tests monkeypatch
    ``UPLOAD_DIR``; a constant frozen at import would send their derived files
    into the user's real uploads directory.
    """
    return UPLOAD_DIR / "derived"
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

# Cloudflare R2 credentials for temporary media hosting (see
# services/media_hosting.py). Both halves live in the Keychain: the access key
# id is not itself a secret — it rides in every presigned URL — but splitting a
# credential across two stores invites half of it landing in .env later.
MEDIA_HOSTING_NAMESPACE = "media_hosting"
MEDIA_HOSTING_ACCESS_KEY_ID_FIELD = "access_key_id"
MEDIA_HOSTING_SECRET_ACCESS_KEY_FIELD = "secret_access_key"


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

# --- Local upload sizing -----------------------------------------------------
#
# ONE ceiling for "how big a file may a user hand this app", used by both upload
# routes. It was previously duplicated — 10 GiB in `video_routes` and again in
# `chunked_uploads` — and a 11.7 GB podcast master hit it after the whole body
# had already been transferred. These uploads are a local file being copied
# across localhost onto the user's own disk, so the cap is a sanity bound
# against a runaway request, not a resource budget: the real limit is free disk.
#
# Enforced BEFORE the bytes are read wherever the size is knowable up front —
# the chunked path has it in `/init`, the multipart path has `Content-Length`,
# and the browser has `File.size` before it opens a socket. Discovering the
# limit only after a six-minute transfer is the failure mode this replaced.
MAX_SOURCE_FILE_BYTES = _parse_int_env(
    "DYS_MAX_SOURCE_FILE_GIB", "YTP_MAX_SOURCE_FILE_GIB", 64
) * 1024**3

# Bulk file-to-file copy buffer. 64 MiB: big enough that a multi-GB master is
# a few hundred iterations instead of tens of thousands, small enough to stay
# out of the way in RAM. Used for local disk copies, NOT for network chunking.
UPLOAD_COPY_BUFFER_BYTES = 64 * 1024 * 1024

# Wire chunk for the chunked-upload protocol, announced to the client by
# `/api/uploads/init`. Also 64 MiB: each chunk is one HTTP round trip that the
# browser materialises in RAM and the server reads whole, so this trades memory
# for round trips — an 11 GB source is ~175 requests instead of ~1400.
#
# Env-tunable because this is the only one of these numbers that crosses into
# the browser: the client sends each chunk as one ArrayBuffer body, and a
# too-large body is an engine limit we would rather turn down than rebuild for.
UPLOAD_WIRE_CHUNK_BYTES = _parse_int_env(
    "DYS_UPLOAD_CHUNK_MIB", "YTP_UPLOAD_CHUNK_MIB", 64
) * 1024 * 1024

# Scheduler
COMMENT_CHECK_INTERVAL_MINUTES = _parse_int_env(
    "DYS_COMMENT_CHECK_MINUTES", "YTP_COMMENT_CHECK_MINUTES", 30
)
CAPTION_CHECK_INTERVAL_MINUTES = _parse_int_env(
    "DYS_CAPTION_CHECK_MINUTES", "YTP_CAPTION_CHECK_MINUTES", 15
)

# Lookahead for the pre-emptive token-refresh sweep: a credential is renewed
# once its token expires within this window. Per-poster, exposed as
# SocialPoster.token_refresh_window_secs. 45 minutes suits ~2-hour tokens
# (Twitter, Bluesky). Threads tokens live 60 days and CANNOT be refreshed once
# expired, so their window is a week: renewal fires when <7 days remain
# (steady-state ~every 53 days, comfortably past Meta's 24-hour minimum token
# age), and the app merely has to run once during the final week — a sleeping
# laptop during any one sweep is a non-event.
SOCIAL_TOKEN_REFRESH_WINDOW_SECONDS = 45 * 60
THREADS_TOKEN_REFRESH_WINDOW_SECONDS = 7 * 24 * 3600

# ---------------------------------------------------------------------------
# Outbound HTTP call budgets
#
# Every outbound API call names its timeout here — never a hardcoded number at
# the call site, and never httpx's implicit 5-second default, which cut off a
# real Threads publish mid-call the first day a working token met a media
# post. These are stall detectors, not speed limits: httpx applies the value
# per socket operation (connect / read / write), so a slow-but-flowing
# transfer never trips them — only a dead connection or a provider that stops
# answering does.

DEFAULT_API_CALL_TIMEOUT_SECONDS = 120

TWITTER_BEARER_REFRESH_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
TWITTER_SIMPLE_UPLOAD_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
BLUESKY_POST_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
BLUESKY_BLOB_UPLOAD_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
MASTODON_INSTANCE_PROBE_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
LINKEDIN_MEDIA_UPLOAD_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
LINKEDIN_POST_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
THREADS_POST_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
THREADS_TOKEN_REFRESH_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
THREADS_VERIFY_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
THREADS_USERINFO_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
MEDIA_HOSTING_CONNECTION_TEST_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
# Token exchanges and userinfo fetches inside the OAuth callback flows
# (Twitter, LinkedIn, Mastodon, Threads).
OAUTH_EXCHANGE_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS
# The resolve_username identity fetches behind Settings' ↻ button.
USERNAME_RESOLVE_TIMEOUT_SECONDS = DEFAULT_API_CALL_TIMEOUT_SECONDS

# Clip-proposal calls to Claude. Not on the default: this one is a generation,
# not a fetch, so the budget has to cover the model writing its whole answer.
#
# Passing it explicitly ALSO matters for a second reason. The Anthropic SDK
# guesses a timeout for non-streaming calls from max_tokens
# (``3600 * max_tokens / 128_000``) and refuses outright above ~21,333 — it
# treats a ceiling as a prediction, at a flat ~35 tokens/sec. That guess only
# runs when the caller passed no timeout of its own (see
# ``messages.py``: ``not is_given(timeout) and client.timeout == DEFAULT``),
# so naming the budget here is what lets us set an honest ceiling instead of
# one bent around the SDK's heuristic.
CLIP_PROPOSAL_TIMEOUT_SECONDS = 900

# Per-call values in force before consolidation onto the default (2026-07-28),
# kept for reference and easy rollback:
# TWITTER_BEARER_REFRESH_TIMEOUT_SECONDS = 20
# TWITTER_SIMPLE_UPLOAD_TIMEOUT_SECONDS = 60
# BLUESKY_POST_TIMEOUT_SECONDS = 60
# BLUESKY_BLOB_UPLOAD_TIMEOUT_SECONDS = 120
# MASTODON_INSTANCE_PROBE_TIMEOUT_SECONDS = 10
# LINKEDIN_MEDIA_UPLOAD_TIMEOUT_SECONDS = 120
# LINKEDIN_POST_TIMEOUT_SECONDS = 60
# THREADS_POST_TIMEOUT_SECONDS = 60  (httpx's 5-second default before that)
# THREADS_TOKEN_REFRESH_TIMEOUT_SECONDS = 20
# THREADS_VERIFY_TIMEOUT_SECONDS = 20
# THREADS_USERINFO_TIMEOUT_SECONDS = 20
# MEDIA_HOSTING_CONNECTION_TEST_TIMEOUT_SECONDS = 60
# OAUTH_EXCHANGE_TIMEOUT_SECONDS = 20  (two userinfo fetches used 15)
# USERNAME_RESOLVE_TIMEOUT_SECONDS = 15

# Bulk-transfer budgets and chunk sizes keep their own values — a 512 MB video
# on a slow link is not "one API call", so the default deliberately does not
# apply to these.

# Twitter chunked upload (video / large files): one multipart request per
# segment, each request getting its own budget.
TWITTER_CHUNKED_UPLOAD_TIMEOUT_SECONDS = 120
TWITTER_VIDEO_UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024

# Per-request budget for the read-only status checks that settle an ambiguous
# Threads publish (response lost mid-call). Deliberately short: the checks
# retry, and the whole recovery is bounded by their attempt count.
THREADS_PUBLISH_RESOLVE_TIMEOUT_SECONDS = 15

# Threads media goes to R2 as a single streamed PUT (no protocol-level
# chunking); the chunk size is the per-read buffer of that stream — it bounds
# peak memory (one chunk resident at a time regardless of file size), is the
# socket-write unit the write timeout meters, and does not affect
# Content-Length or split the request.
MEDIA_HOSTING_UPLOAD_TIMEOUT_SECONDS = 30 * 60
MEDIA_HOSTING_UPLOAD_CHUNK_BYTES = 64 * 1024 * 1024


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    for d in [DATA_DIR, LOG_DIR, TEMPLATES_DIR, UPLOAD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
