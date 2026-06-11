/* Drew's Video + Socials Scheduler — shared JS */

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
        // Built from DOM nodes (not innerHTML) so the build-id strings are set
        // as text, never parsed as HTML, and the Reload handler is attached
        // without an inline onclick.
        const banner = document.createElement('div');
        banner.id = 'build-mismatch-banner';
        const msg = document.createElement('span');
        msg.textContent = 'The server was rebuilt — this page is out of sync. Reload to load the new UI.';
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = 'Reload';
        btn.addEventListener('click', () => window.location.reload());
        const ids = document.createElement('span');
        ids.className = 'build-ids';
        ids.textContent = `${String(oldId).slice(0, 8)} → ${String(newId).slice(0, 8)}`;
        banner.append(msg, btn, ids);
        document.body.appendChild(banner);
    }
})();

// Twitter wraps every URL to ~23 chars (t.co) regardless of its real length,
// so a tweet with links is shorter to X than its raw character count suggests.
// Approximate that so over-limit warnings on link-heavy tweets don't cry wolf.
// (X's full counting is more nuanced — weighted code points etc. — but the URL
// adjustment covers the common case.)
function tweetLength(text) {
    const s = String(text || '');
    let len = s.length;
    for (const m of s.matchAll(/https?:\/\/\S+/g)) {
        len += 23 - m[0].length;
    }
    return len;
}

// Platform-aware "length that counts toward the limit" for a piece of post text.
function platformContentLength(platform, text) {
    return platform === 'twitter' ? tweetLength(text) : String(text || '').length;
}

// X's post character limit for an account: 280 on the free tier, 25,000 on X
// Premium / a verified org (verified_type is 'blue' | 'business' | 'government'
// rather than 'none'/empty).
const X_FREE_POST_LIMIT = 280;
const X_PREMIUM_POST_LIMIT = 25000;
function xIsPremium(verifiedType) {
    return !!verifiedType && verifiedType !== 'none';
}
function xPostLimit(verifiedType) {
    return xIsPremium(verifiedType) ? X_PREMIUM_POST_LIMIT : X_FREE_POST_LIMIT;
}

// Auth status in sidebar — scoped to the project we're currently
// viewing. The sidebar template stamps the current project's slug onto
// .nav-section-label[data-project] when one is in scope; without it
// (Home page, General settings) we fall back to the install-wide
// default project so the indicator still says something useful.
async function checkAuth() {
    try {
        const projectEl = document.querySelector('.nav-section-label[data-project]');
        const slug = projectEl ? projectEl.dataset.project : '';
        const url = slug
            ? `/auth/status?project_slug=${encodeURIComponent(slug)}`
            : '/auth/status';
        const resp = await fetch(url, {_silent: true});
        const data = await resp.json();
        const el = document.getElementById('auth-status');
        // When we're on a project page, label the indicator with the
        // project name so a casual glance tells the user *which*
        // project's connection is being reported.
        // The project name is user-controlled, so build the indicator from DOM
        // nodes and set the label via textContent — never innerHTML — so a name
        // like `<img src=x onerror=...>` can't execute as stored DOM XSS.
        const label = projectEl ? projectEl.textContent.trim() : 'YouTube';
        const dot = document.createElement('span');
        dot.className = 'status-dot';
        el.replaceChildren();
        if (data.authenticated) {
            dot.style.background = '#2ea043';
            const text = document.createElement('span');
            text.textContent = `${label} • Connected`;
            el.append(dot, text);
        } else if (!slug) {
            // No project in scope (Home / General settings): YouTube auth is
            // per-project, so don't raise a red "not connected" alarm here —
            // just note it neutrally and point at where projects live.
            dot.style.background = 'var(--text-muted, #717171)';
            const link = document.createElement('a');
            link.href = '/';
            link.style.color = 'var(--text-muted, #717171)';
            link.textContent = 'YouTube • per project';
            el.append(dot, link);
        } else {
            dot.style.background = '#f85149';
            const link = document.createElement('a');
            link.href = `/projects/${encodeURIComponent(slug)}/settings`;
            link.style.color = '#f85149';
            link.textContent = `${label} • Not connected`;
            el.append(dot, link);
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
