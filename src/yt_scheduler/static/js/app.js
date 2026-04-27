/* Drew's YT Scheduler — shared JS */

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    const duration = type === 'error' ? 8000 : 3000;
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 0.3s';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

/* Global fetch wrapper: surface API failures as toasts automatically AND
 * track build identity across responses.
 *
 * Build identity:
 *   - Every response carries an X-DYS-Build-Id header (added by the server's
 *     BuildIdentityMiddleware). The first response we see "captures" the
 *     server's identity. If a later response carries a different id, the
 *     server has been rebuilt while this tab was open — we surface a banner
 *     telling the user to reload, and start sending the previous id back to
 *     the server in X-DYS-Build-Id so the server's logs show "client X is
 *     stale". The header is also included in every subsequent request.
 *
 * Toasts:
 *   Every non-2xx response and every network error produces a toast unless
 *   the caller opts out with `_silent: true` on the init object. Original
 *   Response is returned unchanged.
 *
 * Error message priority: JSON `.detail` → JSON `.message` → first 200 chars
 * of the raw text body → `HTTP <status>`.
 */
(() => {
    const origFetch = window.fetch.bind(window);
    let knownBuildId = null;
    let bannerShown = false;

    window.fetch = async (input, init = {}) => {
        const silent = init && init._silent;
        if (init && '_silent' in init) {
            init = { ...init };
            delete init._silent;
        }
        // Echo our last-known build id back to the server so its mismatch
        // logging knows where the request came from.
        if (knownBuildId) {
            const headers = new Headers(init.headers || {});
            if (!headers.has('X-DYS-Build-Id')) {
                headers.set('X-DYS-Build-Id', knownBuildId);
            }
            init = { ...init, headers };
        }
        let resp;
        try {
            resp = await origFetch(input, init);
        } catch (e) {
            if (!silent) showToast(`Network error: ${e.message || e}`, 'error');
            throw e;
        }
        const serverBuild = resp.headers.get('X-DYS-Build-Id');
        if (serverBuild) {
            if (!knownBuildId) {
                knownBuildId = serverBuild;
            } else if (serverBuild !== knownBuildId && !bannerShown) {
                bannerShown = true;
                showBuildMismatchBanner(knownBuildId, serverBuild);
            }
        }
        if (!resp.ok && !silent) {
            let detail = '';
            try {
                const body = await resp.clone().text();
                if (body) {
                    try {
                        const j = JSON.parse(body);
                        const raw = j.detail ?? j.message ?? body.slice(0, 200);
                        // FastAPI's HTTPException(detail=<dict>) yields a
                        // structured detail (e.g. {private_video: true,
                        // message: "..."}). Stringifying those naively
                        // produced "[object Object]" toasts; pull the
                        // message field if present, JSON-encode otherwise.
                        if (raw && typeof raw === 'object') {
                            detail = raw.message || JSON.stringify(raw);
                        } else {
                            detail = String(raw ?? '');
                        }
                    } catch {
                        detail = body.slice(0, 200);
                    }
                }
            } catch { /* body read failed — fall through */ }
            showToast(detail || `HTTP ${resp.status}`, 'error');
        }
        return resp;
    };

    function showBuildMismatchBanner(oldId, newId) {
        const banner = document.createElement('div');
        banner.id = 'build-mismatch-banner';
        banner.innerHTML = `
            <span>The server was rebuilt — this page is out of sync. Reload to load the new UI.</span>
            <button onclick="window.location.reload()">Reload</button>
            <span class="build-ids">${oldId.slice(0, 8)} → ${newId.slice(0, 8)}</span>
        `;
        document.body.appendChild(banner);
    }
})();

// Auth status in sidebar
async function checkAuth() {
    try {
        const resp = await fetch('/auth/status', {_silent: true});
        const data = await resp.json();
        const el = document.getElementById('auth-status');
        if (data.authenticated) {
            el.innerHTML = '<span class="status-dot" style="background: #2ea043;"></span><span>YouTube Connected</span>';
        } else {
            el.innerHTML = '<span class="status-dot" style="background: #f85149;"></span><a href="/settings" style="color: #f85149;">Not Connected</a>';
        }
    } catch {
        // Ignore
    }
}

// Highlight active nav link
function highlightNav() {
    const path = window.location.pathname;
    document.querySelectorAll('.nav-link').forEach(link => {
        const href = link.getAttribute('href');
        if (path === href || (href !== '/' && path.startsWith(href))) {
            link.style.color = '#3ea6ff';
            link.style.background = 'rgba(62, 166, 255, 0.1)';
        }
    });
}

checkAuth();
highlightNav();

/* OAuth popup helper.
 *
 * Browsers block popups opened from inside an `await` chain because the
 * user-gesture token doesn't survive past the first `await`. The fix is to
 * call `window.open('about:blank', ...)` synchronously inside the click
 * handler, then redirect the already-open window to the real auth URL once
 * the start endpoint comes back.
 *
 * Usage:
 *   await openOAuthPopup('/api/oauth/twitter/start',
 *                        { client_id, client_secret, origin: location.origin });
 *
 * The promise resolves with the JSON response from the start endpoint or
 * rejects with a string message that's already been shown as a toast.
 * The popup itself postMessages back when the OAuth callback completes —
 * callers should listen for `{source: 'oauth'}` messages on `window`.
 */
async function openOAuthPopup(startUrl, body, popupName = 'oauth') {
    const popup = window.open('about:blank', popupName, 'width=720,height=780');
    if (!popup) {
        showToast('Popup blocked. Allow popups for this page and click again.', 'error');
        throw 'popup_blocked';
    }
    try {
        const resp = await fetch(startUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body || {}),
            _silent: true,
        });
        if (!resp.ok) {
            popup.close();
            const errBody = await resp.text();
            let detail = errBody;
            try { detail = JSON.parse(errBody).detail || errBody; } catch {}
            showToast('OAuth start failed: ' + detail, 'error');
            throw detail;
        }
        const json = await resp.json();
        if (!json.auth_url) {
            popup.close();
            showToast('OAuth start returned no auth_url', 'error');
            throw 'no_auth_url';
        }
        popup.location.href = json.auth_url;
        return json;
    } catch (err) {
        try { popup.close(); } catch {}
        throw err;
    }
}
