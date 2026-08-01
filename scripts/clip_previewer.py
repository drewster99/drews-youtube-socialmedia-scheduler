#!/usr/bin/env python3
"""Standalone clip previewer — hear the selected [start, end] ranges of the
SOURCE video with the cosine edge-fade applied, WITHOUT cutting, transcribing,
LLM calls, or rebuilding the app.

It serves the master video with HTTP range support (so the browser can seek a
multi-GB 4K file), plus a small page that reads ``preview_clips.json`` and plays
just the clip ranges. A **Refresh** button re-reads the JSON — so the tuning loop
is: edit ``clip_edges`` → ``gen_preview_clips.py`` → hit Refresh. No rebuild.

Run (the USER runs this — it opens a browser, touches no Keychain, no DB):

    .venv/bin/python scripts/clip_previewer.py

Then keep it running and click Refresh whenever the JSON is regenerated.

Notes:
  * The source is H.264 4K in a .mov with PCM audio; that PCM track plays in
    Safari but usually not Chrome, so this opens Safari. Override with --browser.
  * The fade is previewed in the browser via a Web Audio cosine gain envelope —
    it does NOT re-encode anything; it's the same raised-cosine curve clipcrop
    applies at cut time, so you can judge the edges before ever cutting.
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import socketserver
import sys
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIPS_JSON = HERE / "preview_clips.json"
DEFAULT_PORT = 8777
CHUNK = 1024 * 1024

PAGE = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>Clip previewer</title>
<style>
  :root { color-scheme: dark; }
  body { margin:0; font:14px -apple-system,system-ui,sans-serif; background:#111; color:#eee; }
  header { position:sticky; top:0; background:#1b1b1b; padding:10px 14px; border-bottom:1px solid #333;
           display:flex; gap:14px; align-items:center; flex-wrap:wrap; }
  button { font:inherit; padding:6px 12px; border-radius:6px; border:1px solid #444; background:#2a2a2a; color:#eee; cursor:pointer; }
  button:hover { background:#333; }
  button.play { background:#1f6f3f; border-color:#2b8; }
  #wrap { display:flex; gap:14px; padding:14px; align-items:flex-start; }
  video { width:min(52vw,720px); background:#000; border-radius:8px; }
  #list { flex:1; max-height:82vh; overflow:auto; }
  .clip { display:flex; gap:10px; align-items:center; padding:8px 10px; border:1px solid #2a2a2a; border-radius:8px; margin-bottom:6px; }
  .clip.active { border-color:#2b8; background:#12281c; }
  .clip .meta { flex:1; }
  .clip .t { font-weight:600; }
  .clip .r { color:#9aa; font-variant-numeric:tabular-nums; }
  label { display:flex; gap:6px; align-items:center; }
  #status { color:#9aa; }
  .badge { font-size:11px; color:#fc9; }
</style></head>
<body>
<header>
  <button id="refresh">↻ Refresh JSON</button>
  <label><input type="checkbox" id="fade" checked> cosine fade</label>
  <label>edge window <input id="tail" type="number" value="5" min="0.5" step="0.5" style="width:54px"> s</label>
  <button id="playall">▶ Play all</button>
  <button id="stop">■ Stop</button>
  <span id="status">loading…</span>
  <span id="now" class="r"></span>
</header>
<div id="wrap">
  <video id="v" src="/source" preload="metadata" controls></video>
  <div id="list"></div>
</div>
<script>
const video = document.getElementById('v');
const listEl = document.getElementById('list');
const statusEl = document.getElementById('status');
const nowEl = document.getElementById('now');
// playToken supersedes any in-flight playback: every stop / new clip bumps it,
// so a running requestAnimationFrame loop for an old clip sees the mismatch and
// bows out. That distinguishes "user stopped / switched" from "clip ended".
let clips = [], ctx, gain, raf = null, playToken = 0;

function cosCurve(dir){ const N=256, a=new Float32Array(N);
  for(let i=0;i<N;i++){ const x=i/(N-1), g=(1-Math.cos(Math.PI*x))/2; a[i] = dir==='in'? g : 1-g; } return a; }
const UP = cosCurve('in'), DOWN = cosCurve('out');

function ensureAudio(){
  if (ctx) return;
  ctx = new (window.AudioContext||window.webkitAudioContext)();
  const src = ctx.createMediaElementSource(video);
  gain = ctx.createGain();
  src.connect(gain).connect(ctx.destination);
}
function resetGain(){ if (gain){ const t=ctx.currentTime; gain.gain.cancelScheduledValues(t); gain.gain.setValueAtTime(1,t); } }
function clearActive(){ document.querySelectorAll('.clip.active').forEach(e=>e.classList.remove('active')); }

function stop(){
  playToken++;
  if (raf){ cancelAnimationFrame(raf); raf=null; }
  video.pause(); resetGain(); clearActive();
}

function seek(t){ return new Promise(res=>{
  if (Math.abs(video.currentTime - t) < 1e-3){ res(); return; }
  const on=()=>{ video.removeEventListener('seeked', on); res(); };
  video.addEventListener('seeked', on); video.currentTime = t;
}); }

// Play an arbitrary [fromT, toT] window WITHIN a clip. The real clip fades are
// applied only at the true clip edges; a partial window (first/last N seconds)
// gets a tiny anti-click ramp at the seam it introduces, and still ends exactly
// on the real cut when toT == clip end.
async function playRange(c, idx, fromT, toT){
  ensureAudio(); await ctx.resume();
  const token = ++playToken;
  if (raf){ cancelAnimationFrame(raf); raf=null; }
  video.pause(); resetGain(); clearActive();
  const row = document.getElementById('clip-'+idx); if (row) row.classList.add('active');
  await seek(fromT);
  if (token !== playToken) return 'cancelled';
  const atStart = Math.abs(fromT - c.start) < 1e-3, atEnd = Math.abs(toT - c.end) < 1e-3;
  const useFade = document.getElementById('fade').checked;
  const fi = atStart ? (useFade ? (c.fade_in||0) : 0) : 0.02;   // real fade at edges; else anti-click
  const fo = atEnd   ? (useFade ? (c.fade_out||0) : 0) : 0.02;
  const dur = toT - fromT, now = ctx.currentTime;
  gain.gain.cancelScheduledValues(now);
  gain.gain.setValueAtTime(fi>0?0:1, now);
  if (fi>0){ gain.gain.setValueCurveAtTime(UP, now, fi); gain.gain.setValueAtTime(1, now+fi); }
  if (fo>0 && dur>fi+fo){ gain.gain.setValueCurveAtTime(DOWN, now + (dur-fo), fo); }
  else if (dur>0){ gain.gain.setValueAtTime(0, now + dur); }     // hard-cut audio AT toT
  await video.play();
  return new Promise(res=>{
    const tick=()=>{
      if (token !== playToken){ res('cancelled'); return; }      // stopped or switched
      nowEl.textContent = 't='+video.currentTime.toFixed(3)+'s';
      if (video.currentTime >= toT){                             // reached toT -> PAUSE
        video.pause(); resetGain(); if (row) row.classList.remove('active'); res('ended'); return;
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
  });
}
function edgeWin(){ return Math.max(0.5, parseFloat(document.getElementById('tail').value) || 5); }
function playClip (c, idx){ return playRange(c, idx, c.start, c.end); }
function playStart(c, idx){ return playRange(c, idx, c.start, Math.min(c.end, c.start + edgeWin())); }
function playEnd  (c, idx){ return playRange(c, idx, Math.max(c.start, c.end - edgeWin()), c.end); }

async function playAll(){
  stop();
  for (let i=0;i<clips.length;i++){
    const r = await playClip(clips[i], i);
    if (r === 'cancelled') return;                            // user stopped / switched
    await new Promise(res=>setTimeout(res, 350));
  }
  clearActive();
}

function render(){
  listEl.innerHTML='';
  clips.forEach((c,i)=>{
    const dur=(c.end-c.start);
    const div=document.createElement('div'); div.className='clip'; div.id='clip-'+i;
    div.innerHTML = `<button class="play start">▶ first</button>
      <button class="play end">▶ last</button>
      <button class="all">▶ all</button>
      <div class="meta"><div class="t">${c.title||('clip '+(i+1))}</div>
      <div class="r">units ${c.first_index}-${c.last_index} &nbsp; ${c.start.toFixed(3)}–${c.end.toFixed(3)}s
        (${dur.toFixed(2)}s) &nbsp; <span class="badge">fade ${(c.fade_in||0)*1000|0}/${(c.fade_out||0)*1000|0}ms</span></div></div>`;
    div.querySelector('.start').onclick=()=>playStart(c,i);
    div.querySelector('.end').onclick=()=>playEnd(c,i);
    div.querySelector('.all').onclick=()=>playClip(c,i);
    listEl.appendChild(div);
  });
}

async function load(){
  stop();
  const r = await fetch('/clips.json?t='+Date.now());
  const j = await r.json();
  clips = j.clips||[];
  statusEl.textContent = `${clips.length} clips · ${j.source_label||''}`;
  render();
}

document.getElementById('refresh').onclick=load;
document.getElementById('playall').onclick=playAll;
document.getElementById('stop').onclick=stop;
load();
</script></body></html>
"""


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet
        pass

    def _bytes(self, body: bytes, ctype: str, cache_ok: bool = True):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        if not cache_ok:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _video(self):
        clips = json.loads(CLIPS_JSON.read_text())
        src = Path(clips["source"])
        if not src.exists():
            self.send_error(404, f"source not found: {src}")
            return
        size = src.stat().st_size
        ctype = "video/quicktime" if src.suffix.lower() == ".mov" else "video/mp4"
        rng = self.headers.get("Range")
        start, end = 0, size - 1
        partial = False
        if rng and rng.startswith("bytes="):
            spec = rng.split("=", 1)[1].split(",")[0]
            s, _, e = spec.partition("-")
            if s:
                start = int(s)
                end = int(e) if e else size - 1
            elif e:  # suffix range: bytes=-N
                start = max(0, size - int(e))
            end = min(end, size - 1)
            partial = True
        length = end - start + 1
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", ctype)
        self.send_header("Accept-Ranges", "bytes")
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(length))
        self.end_headers()
        with open(src, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(CHUNK, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                remaining -= len(chunk)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/":
            self._bytes(PAGE.encode(), "text/html; charset=utf-8")
        elif path == "/clips.json":
            raw = CLIPS_JSON.read_bytes()
            clips = json.loads(raw)
            clips["source_label"] = Path(clips["source"]).name
            self._bytes(json.dumps(clips).encode(), "application/json", cache_ok=False)
        elif path == "/source":
            self._video()
        else:
            self.send_error(404)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--browser", default="Safari", help="macOS app to open (Safari plays the PCM .mov)")
    args = ap.parse_args()
    if not CLIPS_JSON.exists():
        sys.exit(f"No clips file at {CLIPS_JSON} — run: .venv/bin/python scripts/gen_preview_clips.py")

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    with Server(("127.0.0.1", args.port), Handler) as httpd:
        url = f"http://127.0.0.1:{args.port}/"
        print(f"Clip previewer: {url}")
        print(f"  serving clips: {CLIPS_JSON}")
        print(f"  opening {args.browser}. Regenerate the JSON and hit Refresh to iterate. Ctrl-C to stop.")
        os.system(f"open -a {args.browser!r} {url!r}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nstopped.")


if __name__ == "__main__":
    main()
