/* YouTube Publisher — shared JS */

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 0.3s';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// Auth status in sidebar
async function checkAuth() {
    try {
        const resp = await fetch('/auth/status');
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
