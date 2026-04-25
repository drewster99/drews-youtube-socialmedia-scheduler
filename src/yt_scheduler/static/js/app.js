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

/* Global fetch wrapper: surface API failures as toasts automatically.
 *
 * Policy: every non-2xx response AND every network error produces a toast,
 * unless the caller opts out with `_silent: true` on the init object. Existing
 * callers that inspect `resp.ok` and show their own toast still work — the
 * original Response is returned unchanged and callers can read the body as
 * before (we peek via `.clone()` so the stream isn't consumed).
 *
 * Error message priority: JSON `.detail` → JSON `.message` → first 200 chars
 * of the raw text body → `HTTP <status>`.
 */
(() => {
    const origFetch = window.fetch.bind(window);
    window.fetch = async (input, init = {}) => {
        const silent = init && init._silent;
        if (init && '_silent' in init) {
            init = { ...init };
            delete init._silent;
        }
        let resp;
        try {
            resp = await origFetch(input, init);
        } catch (e) {
            if (!silent) showToast(`Network error: ${e.message || e}`, 'error');
            throw e;
        }
        if (!resp.ok && !silent) {
            let detail = '';
            try {
                const body = await resp.clone().text();
                if (body) {
                    try {
                        const j = JSON.parse(body);
                        detail = j.detail || j.message || body.slice(0, 200);
                    } catch {
                        detail = body.slice(0, 200);
                    }
                }
            } catch { /* body read failed — fall through */ }
            showToast(detail || `HTTP ${resp.status}`, 'error');
        }
        return resp;
    };
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
