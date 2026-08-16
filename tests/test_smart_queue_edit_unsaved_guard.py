"""Unsaved queue settings must stop Auto-select and Accept, not be discarded.

Removing a slot row on the smart-schedule page only mutates a client-side
array; the change reaches the server on Save and nowhere else. Accept never
called Save, so six posting days were removed on screen, Accept was pressed,
and 27 videos were scheduled onto the *old* days. Nothing reported anything —
from the server's point of view nothing unusual happened, because it never saw
the removal.

The fix is a refusal, not a silent save: saving inside Accept would move real,
already-visible dates and then have to ask about re-flowing in the middle of a
different operation.

The behavioural test drives the page's own JavaScript under node so it checks
the comparison rather than the existence of the word "unsaved"; the grep test
runs everywhere and holds the wiring in both call sites.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "yt_scheduler"
PAGE = SRC_ROOT / "templates_html" / "smart_queue_edit.html"

# Enough DOM for the page script to load: every element is remembered by id, so
# a value written by the test is the value the script reads back.
_DOM_STUB = """
const _elements = new Map();
function _element(id) {
    if (!_elements.has(id)) {
        const listeners = new Map();
        _elements.set(id, {
            id, value: '', textContent: '', innerHTML: '', checked: false,
            disabled: false, style: {}, dataset: {}, listeners,
            addEventListener(type, fn) { listeners.set(type, fn); },
            querySelectorAll() { return []; },
            closest() { return null; },
        });
    }
    return _elements.get(id);
}

// Buttons are tested by pressing them, not by calling the function they happen
// to call: the bug being guarded against was a missing call in a click handler.
function press(id) {
    const handler = _element(id).listeners.get('click');
    assertTrue(!!handler, 'no click handler registered on ' + id);
    return handler({target: _element(id)});
}
globalThis.document = {
    getElementById: _element,
    querySelectorAll() { return []; },
    addEventListener() {},
};
globalThis.window = {
    addEventListener() {}, scrollTo() {},
    history: { replaceState() {} },
};
globalThis.confirm = () => false;
globalThis.alert = () => {};
// The page's init() fetches on load. Reject so it takes its own error path
// instead of hanging; the assertions below are synchronous and run first.
globalThis.fetch = () => Promise.reject(new Error('no network in this test'));

function assertTrue(condition, what) {
    if (!condition) throw new Error('FAILED: ' + what);
}
function assertEqual(actual, expected, what) {
    const a = JSON.stringify(actual), b = JSON.stringify(expected);
    if (a !== b) throw new Error('FAILED: ' + what + ' — got ' + a + ', want ' + b);
}
function errorText() { return _element('sq-error').textContent; }
"""

_ASSERTIONS = """
// ---- a queue as the server last stored it -------------------------------
document.getElementById('sq-timezone').value = 'America/Chicago';
document.getElementById('sq-template').value = '7';
slots = [{weekday: 0, time_of_day: '07:00'}, {weekday: 6, time_of_day: '15:00'}];
savedServerReadSettings = serverReadSettings();

assertEqual(unsavedSettingNames(), [], 'a saved queue reports nothing unsaved');
assertTrue(refuseIfUnsaved('Accept') === false, 'a saved queue is not refused');

// ---- the edit that was silently discarded -------------------------------
slots = [{weekday: 0, time_of_day: '07:00'}];
assertEqual(unsavedSettingNames(), ['posting times'], 'a removed posting day is unsaved');

// The removal must say so on the spot. Waiting until Accept is what let the
// user believe the change had taken, right up until 27 videos landed on the
// days they had just deleted.
renderSlots();
assertTrue(_element('sq-unsaved-note').style.display !== 'none',
           'the on-screen note appears as soon as a day is removed');
assertTrue(_element('sq-unsaved-note').textContent.includes('posting times'),
           'the on-screen note names what is unsaved');

_element('sq-error').textContent = '';
assertTrue(refuseIfUnsaved('Accept') === true, 'a removed posting day refuses Accept');
assertTrue(errorText().includes('posting times'), 'the refusal names posting times');
assertTrue(errorText().includes('Save'), 'the refusal says to press Save');

// Adding a day counts too: it would post at a time the server does not have.
slots = [{weekday: 0, time_of_day: '07:00'}, {weekday: 6, time_of_day: '15:00'},
         {weekday: 2, time_of_day: '09:00'}];
assertEqual(unsavedSettingNames(), ['posting times'], 'an added posting day is unsaved');

// Changing only the time of an existing day is a change, not a reorder.
slots = [{weekday: 0, time_of_day: '07:30'}, {weekday: 6, time_of_day: '15:00'}];
assertEqual(unsavedSettingNames(), ['posting times'], 'a retimed slot is unsaved');

// Re-ordering the same slots is NOT a change — renderSlots sorts them.
slots = [{weekday: 6, time_of_day: '15:00'}, {weekday: 0, time_of_day: '07:00'}];
assertEqual(unsavedSettingNames(), [], 'the same slots in another order are saved');

// ---- the other two settings Accept reads from the database --------------
document.getElementById('sq-timezone').value = 'UTC';
assertEqual(unsavedSettingNames(), ['time zone'], 'a changed zone is unsaved');
document.getElementById('sq-timezone').value = 'America/Chicago';

document.getElementById('sq-template').value = '9';
assertEqual(unsavedSettingNames(), ['template'], 'a changed template is unsaved');
document.getElementById('sq-template').value = '7';
assertEqual(unsavedSettingNames(), [], 'restoring every field clears the warning');

// ---- pressing the real buttons ------------------------------------------
// This is the shape of the original failure: an edit on screen, then a press,
// and a server that never heard about the edit.
slots = [{weekday: 0, time_of_day: '07:00'}];

_element('sq-error').textContent = '';
press('sq-select-btn');
assertTrue(errorText().includes('posting times'),
           'the Auto-select BUTTON refuses on unsaved posting times');

lastSelection = [{id: 'video-1'}];
waitingCount = 0;
_element('sq-error').textContent = '';
press('sq-accept-btn');
assertTrue(errorText().includes('posting times'),
           'the Accept BUTTON refuses on unsaved posting times');
// Refused means refused: the batch is still there to accept after a Save.
assertEqual(lastSelection, [{id: 'video-1'}],
            'a refused Accept does not discard the selection');

// ---- more than one change reads as a sentence ---------------------------
document.getElementById('sq-timezone').value = 'UTC';
assertEqual(unsavedSettingNames(), ['posting times', 'time zone'], 'both are listed');
_element('sq-error').textContent = '';
refuseIfUnsaved('Accept');
assertTrue(errorText().includes('posting times and time zone'),
           'the refusal joins the list readably, got: ' + errorText());

// ---- a queue that has never been saved has nothing to differ from -------
savedServerReadSettings = null;
assertEqual(unsavedSettingNames(), [], 'an unsaved-ever queue reports nothing');
assertTrue(refuseIfUnsaved('Auto-select') === false,
           'Auto-select is not refused on a brand-new queue — it creates it');

// ---- the two halves that only exist across an await ---------------------
// Loading must take the snapshot and Save must refresh it. A guard that
// outlives the save it demands is worse than no guard: Save looks like it
// worked, the warning stays put, and every press is refused forever.
(async () => {
    // ---- loading the page has to take the snapshot ----------------------
    // Miss this and savedServerReadSettings stays null, unsavedSettingNames()
    // answers [] forever, and every edit is silently discarded again — the
    // original bug, restored, with the guard still visibly present in the file.
    globalThis.fetch = async () => ({ok: true, json: async () => ({
        id: 1, name: 'Daily Shorts', template_id: 7, timezone: 'America/Chicago',
        min_duration_seconds: 0, max_duration_seconds: 180,
        exclude_already_posted: true, auto_add_on_live: true,
        missed_policy: 'post_late', missed_grace_hours: 24,
        orientations: '["portrait"]', waiting: 0,
        slots: [{weekday: 0, time_of_day: '07:00'},
                {weekday: 6, time_of_day: '15:00'}],
    })});
    savedServerReadSettings = null;
    await loadQueue();
    assertTrue(savedServerReadSettings !== null,
               'loadQueue snapshots what the server holds');
    assertEqual(unsavedSettingNames(), [], 'a freshly loaded queue has nothing unsaved');
    slots = slots.slice(0, 1);
    assertEqual(unsavedSettingNames(), ['posting times'],
                'a day removed straight after load is detected');

    // ---- Save has to clear it -------------------------------------------
    globalThis.fetch = async () => ({ok: true, json: async () => ({id: 1})});
    slots = [{weekday: 3, time_of_day: '11:00'}];
    document.getElementById('sq-timezone').value = 'Europe/Berlin';
    savedServerReadSettings =
        {'posting times': [], 'time zone': 'stale', 'template': 'stale'};
    assertTrue(unsavedSettingNames().length > 0, 'precondition: something is unsaved');

    await saveQueue({offerReflow: false});

    assertEqual(unsavedSettingNames(), [], 'Save clears the unsaved settings');
    assertTrue(refuseIfUnsaved('Accept') === false, 'Accept is allowed right after Save');
    assertTrue(_element('sq-unsaved-note').style.display === 'none',
               'the on-screen warning disappears on Save');
    console.log('OK');
})().catch(err => { console.error(err); process.exit(1); });
"""


def _page_script() -> str:
    """The page's inline <script>, with its Jinja holes filled with test data."""
    text = PAGE.read_text()
    match = re.search(r"<script>\n(.*?)\n</script>", text, re.S)
    assert match, "smart_queue_edit.html no longer has a single inline <script>"
    script = match.group(1)
    script = script.replace(
        "{{ current_project.slug | tojson }}", json.dumps("test-project")
    )
    script = script.replace("{{ queue_id | tojson }}", json.dumps("1"))
    assert "{{" not in script, (
        "the page script grew a Jinja expression this harness does not "
        "substitute; add it above rather than letting node parse '{{'"
    )
    return script


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_unsaved_settings_refuse_accept_and_auto_select(tmp_path: Path) -> None:
    """Drives the page's own code, so this tests the comparison, not a string."""
    harness = tmp_path / "guard.js"
    harness.write_text(_DOM_STUB + _page_script() + _ASSERTIONS)
    result = subprocess.run(
        ["node", str(harness)], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, (
        f"the unsaved-settings guard misbehaved:\n{result.stderr}"
    )
    assert result.stdout.strip().endswith("OK")


def test_both_entry_points_still_call_the_guard() -> None:
    """The guard is worth nothing at one of the two call sites.

    Accept is where the discarded edit did its damage, but Auto-select forecasts
    a posting time per candidate from the same saved slots, so an unguarded
    Auto-select shows times the user has just edited away and makes the stale
    schedule look confirmed.
    """
    script = PAGE.read_text()
    for action in ("Accept", "Auto-select"):
        assert f"refuseIfUnsaved('{action}')" in script, (
            f"{action} no longer refuses on unsaved queue settings — an edit "
            "made on screen would be silently ignored by the server"
        )


def test_no_entry_point_saves_the_queue_on_the_user_s_behalf() -> None:
    """Auto-select may create a queue that does not exist yet; nothing may
    quietly PATCH one that does. A save there applies a posting-time change to a
    live schedule and then owes the user the re-flow question in the middle of an
    unrelated operation."""
    script = PAGE.read_text()
    implicit_saves = re.findall(r"saveQueue\(\{offerReflow:\s*false\}\)", script)
    assert len(implicit_saves) == 1, (
        "expected exactly one implicit save — the one that creates a "
        f"never-saved queue in runSelection — found {len(implicit_saves)}"
    )
