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
                const resp = await fetch(
                    `/api/uploads/${encodeURIComponent(uploadId)}/chunk/${offset}`,
                    {
                        method: 'POST',
                        // Raw octet-stream body — no multipart. Small
                        // (8 MB) Blob slice doesn't hit Safari's
                        // stream-exhaustion bug.
                        body: blob,
                        signal,
                    },
                );
                if (resp.ok) {
                    const body = await resp.json();
                    return body.received_bytes;
                }
                // 4xx are not retriable (offset mismatch, oversize,
                // unknown upload) — surface immediately so the caller
                // can decide what to do.
                if (resp.status >= 400 && resp.status < 500) {
                    const body = await safeJson(resp);
                    throw new ChunkedUploadError(
                        `chunk failed: ${(body && body.detail) || resp.status}`,
                        resp.status,
                    );
                }
                // 5xx — retry with backoff.
                lastError = new ChunkedUploadError(`HTTP ${resp.status}`, resp.status);
            } catch (e) {
                if (e instanceof ChunkedUploadError) throw e;
                // AbortError from fetch when the AbortController fires —
                // propagate as cancellation, don't retry.
                if (e && (e.name === 'AbortError' || (signal && signal.aborted))) {
                    throw new ChunkedUploadError('cancelled', 0, true);
                }
                // Network error: retriable.
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
})();
