"""Stdlib-urllib async HTTP client for Bluesky.

Bluesky's edge (CloudFlare) currently rejects ``httpx``'s TLS
fingerprint with HTTP 403 on *every* request, including static URLs
like ``/robots.txt``. Python's stdlib ``urllib`` uses the system
OpenSSL default TLS stack, whose fingerprint the edge still accepts.
This module wraps ``urllib.request`` in ``asyncio.to_thread`` so the
rest of the async codebase keeps its existing await-style call shape
without blocking the event loop.

The API surface mirrors the subset of ``httpx`` previously used for
Bluesky calls: ``get`` and ``post`` (with ``data=`` form body,
``json=`` JSON body, or raw ``content=`` byte body), and a ``Response``
with ``status_code``, ``text``, ``content``, case-insensitive
``headers``, ``json()`` and ``raise_for_status()``.

This is intentionally scoped to Bluesky; the rest of the codebase
continues to use ``httpx`` directly.
"""

from __future__ import annotations

import asyncio
import json as _json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Mapping


class BlueskyHTTPError(Exception):
    """Network-layer failure talking to Bluesky (DNS, TLS, connection
    reset, timeout). Mirrors the role of ``httpx.HTTPError`` in the
    previous code path — HTTP 4xx/5xx responses are *not* raised; they
    come back as ``Response`` objects so callers can inspect
    ``status_code`` and the response body the way the old httpx code
    did."""


class _CaseInsensitiveHeaders:
    def __init__(self, raw: Mapping[str, str]) -> None:
        self._lower = {k.lower(): v for k, v in raw.items()}

    def get(self, key: str, default: Any = None) -> Any:
        return self._lower.get(key.lower(), default)

    def __getitem__(self, key: str) -> str:
        return self._lower[key.lower()]

    def __contains__(self, key: str) -> bool:
        return key.lower() in self._lower


class Response:
    """Minimal httpx-compatible response covering the call sites we use."""

    def __init__(
        self, status_code: int, content: bytes, headers: Mapping[str, str]
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = _CaseInsensitiveHeaders(headers)

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return _json.loads(self.text)

    def raise_for_status(self) -> None:
        if 400 <= self.status_code < 600:
            raise BlueskyHTTPError(
                f"HTTP {self.status_code} {self.text[:200]}"
            )


_USER_AGENT = (
    "drews-video-social-scheduler/0.1 "
    "(+https://github.com/drewster99/drews-video-social-scheduler)"
)


def _build_request(
    method: str,
    url: str,
    *,
    data: Mapping[str, Any] | None = None,
    json: Any | None = None,
    content: bytes | bytearray | memoryview | None = None,
    headers: Mapping[str, str] | None = None,
) -> urllib.request.Request:
    body: bytes | None = None
    final_headers: dict[str, str] = {"User-Agent": _USER_AGENT}
    if headers:
        final_headers.update(headers)

    if json is not None:
        body = _json.dumps(json).encode("utf-8")
        final_headers.setdefault("Content-Type", "application/json")
    elif data is not None:
        body = urllib.parse.urlencode(list(data.items())).encode("utf-8")
        final_headers.setdefault(
            "Content-Type", "application/x-www-form-urlencoded"
        )
    elif content is not None:
        body = bytes(content)

    return urllib.request.Request(
        url, data=body, headers=final_headers, method=method
    )


def _do_request_sync(req: urllib.request.Request, *, timeout: float) -> Response:
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return Response(
                status_code=int(resp.status),
                content=resp.read(),
                headers=dict(resp.headers.items()),
            )
    except urllib.error.HTTPError as e:
        # HTTPError represents an HTTP response we *did* receive (4xx/5xx).
        # Surface it as a Response so retry/branch logic in the caller can
        # inspect status_code and headers (e.g. DPoP-Nonce).
        body = b""
        try:
            body = e.read()
        except Exception:
            pass
        hdrs = dict(e.headers.items()) if e.headers is not None else {}
        return Response(status_code=int(e.code), content=body, headers=hdrs)
    except urllib.error.URLError as e:
        raise BlueskyHTTPError(str(e.reason)) from e
    except (TimeoutError, OSError) as e:
        raise BlueskyHTTPError(str(e)) from e


async def get(
    url: str,
    *,
    timeout: float = 15.0,
    headers: Mapping[str, str] | None = None,
) -> Response:
    req = _build_request("GET", url, headers=headers)
    return await asyncio.to_thread(_do_request_sync, req, timeout=timeout)


async def post(
    url: str,
    *,
    data: Mapping[str, Any] | None = None,
    json: Any | None = None,
    content: bytes | bytearray | memoryview | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 15.0,
) -> Response:
    req = _build_request(
        "POST", url, data=data, json=json, content=content, headers=headers
    )
    return await asyncio.to_thread(_do_request_sync, req, timeout=timeout)


__all__ = ["BlueskyHTTPError", "Response", "get", "post"]
