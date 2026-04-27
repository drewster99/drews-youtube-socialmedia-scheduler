"""Detect a locally-running ngrok HTTPS tunnel.

Some OAuth providers (notably Meta / Threads) refuse to issue tokens to
flows that originated on an ``http://`` page, even when the redirect is
to ``localhost``. The standard escape hatch is ``ngrok http 8008``,
which exposes the local server at ``https://<random>.ngrok-free.app``.

ngrok keeps a JSON inventory of its tunnels at ``http://127.0.0.1:4040``
(its built-in inspector / API). We poll that endpoint to discover the
public HTTPS URL that forwards to our local server, so the UI can:

* show the user which URL to load the app at,
* and pre-fill OAuth ``origin`` parameters with the HTTPS URL when the
  user is doing a flow that needs HTTPS.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

NGROK_API_URL = "http://127.0.0.1:4040/api/tunnels"


async def detect_https_tunnel(local_port: int) -> str | None:
    """Return the ``https://...`` public URL of an ngrok tunnel that
    forwards to ``127.0.0.1:<local_port>``, or ``None`` when no such
    tunnel is found (ngrok not running, no matching tunnel, etc.).

    Errors are swallowed and logged at debug level — the caller treats
    a missing tunnel as the common case, not an exception.
    """
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(NGROK_API_URL)
    except httpx.HTTPError as exc:
        logger.debug("ngrok API unreachable at %s: %s", NGROK_API_URL, exc)
        return None

    if resp.status_code != 200:
        logger.debug("ngrok API returned HTTP %s", resp.status_code)
        return None

    try:
        body = resp.json()
    except ValueError:
        return None

    tunnels = body.get("tunnels") or []
    for tunnel in tunnels:
        public_url = tunnel.get("public_url") or ""
        if not public_url.startswith("https://"):
            continue
        config = tunnel.get("config") or {}
        addr = config.get("addr") or ""
        # ngrok stores ``addr`` as either ``http://localhost:8008`` or just
        # ``localhost:8008`` / ``8008`` depending on the version. Pull the
        # port out of whichever shape we got.
        port = _extract_port(addr)
        if port == local_port:
            return public_url
    return None


def _extract_port(addr: str) -> int | None:
    if not addr:
        return None
    parsed = urlparse(addr if "://" in addr else f"http://{addr}")
    if parsed.port is not None:
        return parsed.port
    if addr.isdigit():
        return int(addr)
    return None
