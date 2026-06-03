// Chunked upload helper — talks to /api/uploads/*.
//
// Why we don't just xhr.send(file): Safari/WebKit raises "request
// body stream exhausted" when the body is a multi-GB File combined
// with custom request headers. Slicing the file with Blob.slice()
// and sending each chunk as its own request side-steps the bug.
//
// Why we don't just FormData(file): FormData triggers FastAPI's
// UploadFile path, which buffers the entire body into a
// SpooledTemporaryFile in $TMPDIR first — an 8 GB upload ends up
// writing ~24 GB to disk. Chunked single-pass append is a single
// write of the file's actual size.
//
// Public API:
//   const { upload_id } = await chunkedUpload(file, {
//       onProgress(loaded, total),
//       signal,                    // AbortSignal — cancels mid-flight
//   });
// The caller then passes upload_id to a domain endpoint
// (POST /api/videos/upload, /items, /{id}/source-file, …).
(function () {
    'use strict';

    // Version marker — bump on every meaningful edit so we can tell
    // from the browser console whether the loaded uploads.js matches
    // the latest server build. Surfaced via:
    //   - console.log on load (visible in Web Inspector → Console)
    //   - window.UPLOADS_JS_VERSION (queryable from the console)
    //   - the value is also returned by chunkedUpload._version()
    // When bundling into the macOS app via build.sh, the build must
    // include any uploads.js edits since the previous build for this
    // to match.
    const UPLOADS_JS_VERSION = '2026-06-03-safari-multipart-fallback';
    try { console.log('[uploads.js] version:', UPLOADS_JS_VERSION); } catch (e) {}

    const RETRY_DELAYS_MS = [500, 1500, 3000];  // per-chunk retry backoff

    async function chunkedUpload(file, opts) {
        opts = opts || {};
        const onProgress = opts.onProgress || (() => {});
        const signal = opts.signal;

        // 1) Reserve a slot. Server picks the chunk size so we don't
        //    have to coordinate the tuning knob.
        let initResp;
        try {
            initResp = await fetch('/api/uploads/init', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filename: file.name, size: file.size }),
                signal,
            });
        } catch (e) {
            if (e && (e.name === 'AbortError' || (signal && signal.aborted))) {
                throw new ChunkedUploadError('cancelled', 0, true);
            }
            throw new ChunkedUploadError(e.message || 'network', 0);
        }
        if (!initResp.ok) {
            const body = await safeJson(initResp);
            throw new ChunkedUploadError(
                `init failed: ${(body && body.detail) || initResp.status}`,
                initResp.status,
            );
        }
        const { upload_id, chunk_size } = await initResp.json();

        // 2) Slice + POST each chunk. Track loaded bytes for progress.
        let offset = 0;
        while (offset < file.size) {
            if (signal && signal.aborted) {
                await cancel(upload_id);
                throw new ChunkedUploadError('cancelled', 0, /*cancelled*/ true);
            }
            const end = Math.min(offset + chunk_size, file.size);
            const slice = file.slice(offset, end);
            const received = await postChunkWithRetry(
                upload_id, offset, slice, signal,
            );
            // The server's received_bytes is authoritative. If it
            // doesn't match our local offset+slice.size, something is
            // wrong — bail rather than continue building a corrupt
            // file.
            if (received !== end) {
                await cancel(upload_id);
                throw new ChunkedUploadError(
                    `server-side byte count drift (got ${received}, expected ${end})`,
                    500,
                );
            }
            offset = end;
            onProgress(offset, file.size);
        }

        // 3) Finalize. Returns {upload_id, size, filename}.
        let finResp;
        try {
            finResp = await fetch(`/api/uploads/${encodeURIComponent(upload_id)}/finalize`, {
                method: 'POST',
                signal,
            });
        } catch (e) {
            if (e && (e.name === 'AbortError' || (signal && signal.aborted))) {
                await cancel(upload_id);
                throw new ChunkedUploadError('cancelled', 0, true);
            }
            throw new ChunkedUploadError(e.message || 'network', 0);
        }
        if (!finResp.ok) {
            const body = await safeJson(finResp);
            throw new ChunkedUploadError(
                `finalize failed: ${(body && body.detail) || finResp.status}`,
                finResp.status,
            );
        }
        return await finResp.json();
    }

    async function postChunkWithRetry(uploadId, offset, blob, signal) {
        let lastError = null;
        for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
            if (signal && signal.aborted) throw new ChunkedUploadError('cancelled', 0, true);
            try {
                const result = await postBlobXhr(
                    `/api/uploads/${encodeURIComponent(uploadId)}/chunk/${offset}`,
                    blob,
                    signal,
                );
                if (result.status >= 200 && result.status < 300) {
                    const body = result.body || {};
                    return body.received_bytes;
                }
                // 4xx are not retriable (offset mismatch, oversize,
                // unknown upload) — surface immediately so the caller
                // can decide what to do.
                if (result.status >= 400 && result.status < 500) {
                    throw new ChunkedUploadError(
                        `chunk failed: ${(result.body && result.body.detail) || result.status}`,
                        result.status,
                    );
                }
                // 5xx — retry with backoff.
                lastError = new ChunkedUploadError(`HTTP ${result.status}`, result.status);
            } catch (e) {
                if (e instanceof ChunkedUploadError) throw e;
                lastError = new ChunkedUploadError(e.message || 'network', 0);
            }
            if (attempt < RETRY_DELAYS_MS.length) {
                // Sleep interruptibly so a cancel during the backoff
                // doesn't have to wait the full delay before exiting.
                await sleepInterruptible(RETRY_DELAYS_MS[attempt], signal);
                if (signal && signal.aborted) {
                    throw new ChunkedUploadError('cancelled', 0, true);
                }
            }
        }
        throw lastError;
    }

    // Send the chunk body as an ArrayBuffer, NOT a Blob.
    //
    // Safari/WebKit has two separate bugs in this area:
    //   1. xhr.send(blob) / fetch(body: blob) → "request body stream
    //      exhausted" before the request leaves the browser. The
    //      engine reads the Blob's one-shot stream once during a
    //      pre-send pass and the actual send sees empty bytes. Fires
    //      on whole Files and on small sliced Blobs alike.
    //   2. blob.arrayBuffer() routes through WebKit's internal
    //      resource-loader machinery and fails with
    //      "WebKitBlobResource error 4" on File slices.
    //
    // We need an ArrayBuffer (defeats bug #1) but we have to materialise
    // it via the older FileReader API (defeats bug #2). FileReader has
    // been around since the original File API draft and has its own
    // code path in WebKit that doesn't go through the resource loader.
    //
    // TODO: once Safari fixes the blob.arrayBuffer() bug on File slices
    // (filed at Apple as of 2024, not yet resolved), drop the
    // FileReader wrapper and use the modern API directly.
    //
    // Memory: one chunk (~8 MB) in RAM during flight, then GC'd.
    async function postBlobXhr(url, blob, signal) {
        if (signal && signal.aborted) {
            throw new ChunkedUploadError('cancelled', 0, true);
        }
        const buffer = await readBlobAsArrayBuffer(blob, signal);
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', url);
            xhr.responseType = 'text';
            // No explicit Content-Type — the server doesn't check it
            // for chunk POSTs and an explicit header was historically
            // another trigger condition for the WebKit body bug.
            function onAbort() {
                try { xhr.abort(); } catch (e) {}
            }
            if (signal) {
                if (signal.aborted) {
                    reject(new ChunkedUploadError('cancelled', 0, true));
                    return;
                }
                signal.addEventListener('abort', onAbort, { once: true });
            }
            xhr.onload = () => {
                if (signal) signal.removeEventListener('abort', onAbort);
                let body = null;
                try { body = JSON.parse(xhr.responseText || '{}'); } catch (e) { /* leave null */ }
                resolve({ status: xhr.status, body });
            };
            xhr.onerror = () => {
                if (signal) signal.removeEventListener('abort', onAbort);
                reject(new Error('network'));
            };
            xhr.onabort = () => {
                if (signal) signal.removeEventListener('abort', onAbort);
                reject(new ChunkedUploadError('cancelled', 0, true));
            };
            xhr.send(buffer);
        });
    }

    async function cancel(uploadId) {
        try {
            await fetch(`/api/uploads/${encodeURIComponent(uploadId)}`, {
                method: 'DELETE',
            });
        } catch (e) {
            // Best-effort; the TTL will sweep it eventually.
        }
    }

    async function safeJson(resp) {
        try { return await resp.json(); } catch (e) { return null; }
    }

    function sleep(ms) {
        return new Promise(r => setTimeout(r, ms));
    }

    // Read a Blob's bytes into an ArrayBuffer via the FileReader API.
    // Equivalent to ``await blob.arrayBuffer()`` for our purposes but
    // doesn't route through the WebKit resource loader, so it avoids
    // "WebKitBlobResource error 4" on File slices.
    function readBlobAsArrayBuffer(blob, signal) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            function onAbort() {
                try { reader.abort(); } catch (e) {}
            }
            if (signal) {
                if (signal.aborted) {
                    reject(new ChunkedUploadError('cancelled', 0, true));
                    return;
                }
                signal.addEventListener('abort', onAbort, { once: true });
            }
            reader.onload = () => {
                if (signal) signal.removeEventListener('abort', onAbort);
                resolve(reader.result);
            };
            reader.onerror = () => {
                if (signal) signal.removeEventListener('abort', onAbort);
                const err = reader.error;
                reject(new Error(
                    (err && err.message) ? `read failed: ${err.message}` : 'read failed',
                ));
            };
            reader.onabort = () => {
                if (signal) signal.removeEventListener('abort', onAbort);
                reject(new ChunkedUploadError('cancelled', 0, true));
            };
            reader.readAsArrayBuffer(blob);
        });
    }

    function sleepInterruptible(ms, signal) {
        return new Promise((resolve) => {
            const t = setTimeout(() => {
                if (signal) signal.removeEventListener('abort', onAbort);
                resolve();
            }, ms);
            function onAbort() {
                clearTimeout(t);
                if (signal) signal.removeEventListener('abort', onAbort);
                resolve();
            }
            if (signal) signal.addEventListener('abort', onAbort, { once: true });
        });
    }

    function ChunkedUploadError(message, status, cancelled) {
        const e = new Error(message);
        e.name = 'ChunkedUploadError';
        e.status = status || 0;
        e.cancelled = !!cancelled;
        return e;
    }

    window.chunkedUpload = chunkedUpload;
    window.ChunkedUploadError = ChunkedUploadError;
    window.UPLOADS_JS_VERSION = UPLOADS_JS_VERSION;
})();
