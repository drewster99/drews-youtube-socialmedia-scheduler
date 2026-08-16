"""The app-wide reconcile banner, driven under node.

This file had no test at all, and it is the only progress surface for work that
rewrites real posting schedules — including Accept, which moved onto the worker
and is now the job the banner shows most often.

Three things it pins, each of which was wrong at some point:

* the poll chain is single. ``clearTimeout`` cannot cancel a poll already
  awaiting its fetch, so ``refresh()`` left the old one running: it resumed,
  scheduled its own timer, and from then on two chains polled forever. Three
  buttons call ``refresh()`` now, so two in quick succession was enough.
* the bootstrap actually polls. ``addEventListener('DOMContentLoaded', poll)``
  hands the callback an *Event*, which arrives as the generation argument and
  fails the identity check on the way out — a banner that never polls at all,
  on every page that loads while the parser is still running.
* the headline claims no cause. It read "Updating schedules to match template
  changes", which is false for an Accept the user just pressed; blaming an edit
  they did not make sends them looking for the wrong thing.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "yt_scheduler"
BANNER = SRC_ROOT / "static" / "js" / "reconcile-banner.js"

# A hand-driven clock, so the 15-second idle interval costs nothing and every
# scheduled callback is countable rather than merely eventual.
_HARNESS_PREFIX = """
let pendingTimers = [];
let nextTimerId = 1;
globalThis.setTimeout = (fn, ms) => {
    const id = nextTimerId++;
    pendingTimers.push({id, fn, ms});
    return id;
};
globalThis.clearTimeout = (id) => {
    pendingTimers = pendingTimers.filter(t => t.id !== id);
};
function fireTimers() {
    const due = pendingTimers;
    pendingTimers = [];
    for (const t of due) t.fn();
    return due.length;
}
const tick = () => new Promise(resolve => setImmediate(resolve));
async function settle() { for (let i = 0; i < 20; i++) await tick(); }

let fetchCount = 0;
let statusPayload = {active: [], failed: [], completed: [], busy: false,
                     locked_queue_ids: []};
globalThis.fetch = async (url) => {
    fetchCount += 1;
    return {ok: true, json: async () => statusPayload};
};

const banner = {id: 'reconcile-banner', innerHTML: '', style: {}};
const documentListeners = new Map();
const dispatched = [];
globalThis.document = {
    readyState: 'complete',
    getElementById: (id) => (id === 'reconcile-banner' ? banner : null),
    createElement: () => ({
        set textContent(v) {
            this._v = String(v == null ? '' : v)
                .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        },
        get innerHTML() { return this._v || ''; },
    }),
    addEventListener: (type, fn) => {
        if (!documentListeners.has(type)) documentListeners.set(type, []);
        documentListeners.get(type).push(fn);
    },
    dispatchEvent: (event) => {
        dispatched.push(event);
        for (const fn of documentListeners.get(event.type) || []) fn(event);
        return true;
    },
};
globalThis.CustomEvent = class CustomEvent {
    constructor(type, init) { this.type = type; this.detail = (init || {}).detail; }
};
globalThis.window = globalThis;

function assertTrue(condition, what) {
    if (!condition) throw new Error('FAILED: ' + what);
}
function assertEqual(actual, expected, what) {
    const a = JSON.stringify(actual), b = JSON.stringify(expected);
    if (a !== b) throw new Error('FAILED: ' + what + ' — got ' + a + ', want ' + b);
}
"""

_ASSERTIONS = """
(async () => {
    // ---- the banner renders a running Accept -----------------------------
    statusPayload = {
        active: [{id: 7, queue_id: 1, project_slug: 'p', queue_name: 'Daily Shorts',
                  kind: 'accept', label: 'Scheduling accepted videos',
                  status: 'running', done: 12, total: 27,
                  detail: null, error: null}],
        failed: [], completed: [], busy: true, locked_queue_ids: [1],
    };
    window.dysReconcileBanner.refresh();
    await settle();

    assertTrue(banner.style.display === '', 'the banner is shown while work runs');
    assertTrue(banner.innerHTML.includes('12 of 27'),
               'progress is shown, got: ' + banner.innerHTML);
    assertTrue(banner.innerHTML.includes('Scheduling accepted videos'),
               "the job's own label says what it is doing");

    // The headline must not attribute a cause the user did not create.
    assertTrue(!banner.innerHTML.includes('template changes'),
               'the headline must not blame a template change for an Accept');

    // ---- the status event reaches listeners on `document` ----------------
    const events = dispatched.filter(e => e.type === 'reconcile-status');
    assertTrue(events.length === 1, 'one status event per changed poll');
    assertEqual(events[events.length - 1].detail.active[0].id, 7,
                'the event carries the status payload');

    // ---- one poll chain, no matter how many refreshes overlap ------------
    // Both refreshes are issued while the previous fetch is still in flight,
    // which is exactly what pressing two enqueueing buttons in a row does.
    const before = fetchCount;
    window.dysReconcileBanner.refresh();
    window.dysReconcileBanner.refresh();
    window.dysReconcileBanner.refresh();
    await settle();
    assertEqual(fetchCount - before, 3, 'each refresh polls immediately');

    // Now let the scheduled follow-ups run. A superseded chain must have
    // stopped, so exactly ONE timer is outstanding.
    const scheduled = fireTimers();
    assertEqual(scheduled, 1,
                'exactly one poll chain survives three overlapping refreshes');
    await settle();
    assertEqual(fireTimers(), 1, 'and it stays single on the next cycle too');

    // ---- a finished job leaves the banner --------------------------------
    statusPayload = {
        active: [], failed: [],
        completed: [{id: 7, queue_id: 1, project_slug: 'p',
                     queue_name: 'Daily Shorts', kind: 'accept',
                     label: 'Scheduling accepted videos', status: 'done',
                     done: 27, total: 27,
                     detail: 'Scheduled 27 videos.', error: null}],
        busy: false, locked_queue_ids: [],
    };
    window.dysReconcileBanner.refresh();
    await settle();
    assertTrue(banner.style.display === 'none',
               'a completed job does not keep the banner on screen');
    const last = dispatched[dispatched.length - 1];
    assertEqual(last.detail.completed[0].detail, 'Scheduled 27 videos.',
                'the completed report still reaches the page that started it');

    // ---- a failed job stays, with its message verbatim -------------------
    statusPayload = {
        active: [], completed: [],
        failed: [{id: 8, queue_id: 1, project_slug: 'p',
                  queue_name: 'Daily Shorts', kind: 'accept',
                  label: 'Scheduling accepted videos', status: 'failed',
                  done: 27, total: 27, detail: null,
                  error: 'Scheduled 25 videos. 2 videos could not be scheduled.'}],
        busy: false, locked_queue_ids: [],
    };
    window.dysReconcileBanner.refresh();
    await settle();
    assertTrue(banner.style.display === '', 'a failure keeps the banner up');
    assertTrue(banner.innerHTML.includes('2 videos could not be scheduled'),
               'the failure message is shown, got: ' + banner.innerHTML);
    assertTrue(banner.innerHTML.includes('data-dismiss-job="8"'),
               'a failure is dismissable');

    console.log('OK');
})().catch(err => { console.error(err); process.exit(1); });
"""

# Loaded with readyState 'loading' so the DOMContentLoaded branch is the one
# under test, rather than the straight-line call the other harness takes.
_BOOTSTRAP_ASSERTIONS = """
(async () => {
    assertEqual(fetchCount, 0, 'nothing polls before the document is ready');
    const listeners = documentListeners.get('DOMContentLoaded') || [];
    assertEqual(listeners.length, 1, 'the bootstrap registers exactly one listener');

    // A listener is called WITH an Event. Passing `poll` by reference lets that
    // Event arrive as the generation argument, whereupon the chain stops before
    // scheduling anything — a banner that never polls again.
    listeners[0]({type: 'DOMContentLoaded'});
    await settle();

    assertEqual(fetchCount, 1, 'the ready event starts polling');
    assertEqual(fireTimers(), 1, 'and schedules the next poll');
    await settle();
    assertEqual(fetchCount, 2, 'the chain keeps going after the first tick');
    console.log('OK');
})().catch(err => { console.error(err); process.exit(1); });
"""


def _run_node(tmp_path: Path, name: str, prefix: str, assertions: str) -> None:
    harness = tmp_path / name
    harness.write_text(prefix + BANNER.read_text() + assertions)
    result = subprocess.run(
        ["node", str(harness)], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, f"reconcile-banner.js misbehaved:\n{result.stderr}"
    assert result.stdout.strip().endswith("OK")


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_banner_polls_renders_and_keeps_one_chain(tmp_path: Path) -> None:
    _run_node(tmp_path, "banner.js", _HARNESS_PREFIX, _ASSERTIONS)


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_banner_starts_polling_from_the_dom_ready_event(tmp_path: Path) -> None:
    prefix = _HARNESS_PREFIX.replace(
        "readyState: 'complete',", "readyState: 'loading',"
    )
    assert "readyState: 'loading'" in prefix
    _run_node(tmp_path, "bootstrap.js", prefix, _BOOTSTRAP_ASSERTIONS)


def _banner_code_without_comments() -> str:
    """The banner's code with ``//`` comment text removed.

    The comments explain *why* the headline no longer names a cause, so they
    quote the wording being forbidden. Checking the raw file would therefore
    fail on its own explanation.
    """
    lines = []
    for line in BANNER.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("*"):
            continue
        lines.append(line.split("//")[0] if "//" in line and "://" not in line else line)
    return "\n".join(lines)


def test_the_banner_headline_states_no_cause() -> None:
    """Runs without node, so the rule survives a machine that lacks it.

    The same jobs are queued by a template edit, by Re-render, by Add missing
    slots and now by Accept. Naming any one of them in the headline is wrong for
    the other three — which is the identical reason the 409 message carries a
    comment refusing to say "a template change".
    """
    assert "template changes" not in _banner_code_without_comments(), (
        "the app-wide banner names a cause that is wrong for most of the jobs "
        "it shows — Accept in particular, which no template edit triggered"
    )


def test_poll_is_never_passed_as_a_bare_listener() -> None:
    """``addEventListener('...', poll)`` hands poll an Event as its generation
    argument, and the chain then stops before scheduling anything."""
    text = BANNER.read_text()
    assert not any(
        f"addEventListener({quote}{event}{quote}, poll)" in text
        for quote in ("'", '"')
        for event in ("DOMContentLoaded", "load")
    ), "poll must be arrow-wrapped so it receives its generation, not an Event"


def test_status_contract_matches_the_server() -> None:
    """The banner reads three buckets; the server has to vend all three."""
    reconcile = (SRC_ROOT / "services" / "smart_queue_reconcile.py").read_text()
    for bucket in ("active", "failed", "completed"):
        assert f'"{bucket}"' in reconcile, (
            f"status_summary no longer returns {bucket!r}, which the banner "
            "and the smart-schedule page both read"
        )
