"""
Super Scraper Dashboard — real-time monitoring UI.
Reads from the scraper output directory without touching the scraper.

Usage:
    python dashboard.py --out out --csv leads2.csv
    python dashboard.py --out out --csv leads2.csv --port 8050
"""

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime

try:
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, JSONResponse
    import uvicorn
except ImportError:
    print("Missing dependencies. Run: pip install fastapi uvicorn")
    raise SystemExit(1)

app = FastAPI()
CONFIG = {"out": "out", "csv": ""}

SCRAPER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "super_scraper.py")


# ---------------------------------------------------------------------------
# Run controller — launches super_scraper.py as a subprocess and manages
# start / pause / resume. The scraper is crash-safe and resumable: re-running
# the same command skips companies already recorded in out/state.json, so
# "resume" is simply a relaunch and "pause" is a graceful terminate.
# ---------------------------------------------------------------------------

class Controller:
    def __init__(self):
        self.proc = None
        self.thread = None
        self.lock = threading.Lock()
        self.status = "idle"      # idle | running | paused | auto_paused | done | error
        self.message = "No CSV loaded yet."
        self.csv_path = ""
        self.csv_name = ""
        self.opts = {"concurrency": 30, "details": False, "browser": False}
        self._paused_by_user = False

    # --- helpers ----------------------------------------------------------
    def _input_path(self):
        return os.path.join(CONFIG["out"], "_input.csv")

    def _log_path(self):
        return os.path.join(CONFIG["out"], "run.log")

    def total(self):
        path = self.csv_path
        if not path or not os.path.exists(path):
            return 0
        try:
            with open(path, encoding="utf-8") as f:
                return max(0, sum(1 for _ in csv.reader(f)) - 1)
        except Exception:
            return 0

    def done_count(self):
        path = os.path.join(CONFIG["out"], "state.json")
        if not os.path.exists(path):
            return 0
        try:
            return len(json.load(open(path)).get("done", []))
        except Exception:
            return 0

    def remaining(self):
        return max(0, self.total() - self.done_count())

    # --- CSV upload -------------------------------------------------------
    def save_csv(self, raw: bytes, name: str):
        """Persist an uploaded CSV. If it differs from the current input,
        archive the previous run artifacts so progress/counts stay consistent."""
        os.makedirs(CONFIG["out"], exist_ok=True)
        text = raw.decode("utf-8-sig", errors="replace")

        # validate required columns
        try:
            header = next(csv.reader(text.splitlines()))
        except StopIteration:
            return False, "CSV is empty."
        low = [h.lower() for h in header]
        if not any("website" in h for h in low):
            return False, "CSV needs a 'Website' column."
        if not any("company name" in h for h in low):
            return False, "CSV needs a 'Company Name' column."

        dest = self._input_path()
        new_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        old_hash = ""
        if os.path.exists(dest):
            old_hash = hashlib.sha256(
                open(dest, encoding="utf-8").read().encode("utf-8")).hexdigest()

        with self.lock:
            if self.status == "running":
                return False, "Stop the current run before loading a new CSV."
            if old_hash and old_hash != new_hash:
                self._archive_run()
            with open(dest, "w", encoding="utf-8") as f:
                f.write(text)
            self.csv_path = dest
            self.csv_name = name or "uploaded.csv"
            CONFIG["csv"] = dest
            rows = self.total()
            done = self.done_count()
            if done and old_hash == new_hash:
                self.status = "paused"
                self.message = f"Loaded {self.csv_name} — {done} of {rows} already done. Press Resume."
            else:
                self.status = "idle"
                self.message = f"Loaded {self.csv_name} — {rows} companies ready. Press Start."
        return True, self.message

    def _archive_run(self):
        arc = os.path.join(CONFIG["out"], "_archive", str(int(time.time())))
        os.makedirs(arc, exist_ok=True)
        for fn in ("jobs.jsonl", "jobs.csv", "report.csv", "state.json", "run.log"):
            src = os.path.join(CONFIG["out"], fn)
            if os.path.exists(src):
                shutil.move(src, os.path.join(arc, fn))

    # --- run control ------------------------------------------------------
    def start(self):
        with self.lock:
            if self.status == "running":
                return False, "Already running."
            if not self.csv_path or not os.path.exists(self.csv_path):
                return False, "Load a CSV first."
            cmd = [sys.executable, SCRAPER,
                   "--csv", self.csv_path,
                   "--out", CONFIG["out"],
                   "--concurrency", str(int(self.opts["concurrency"]))]
            if self.opts["details"]:
                cmd.append("--details")
            if self.opts["browser"]:
                cmd.append("--browser")

            resuming = self.done_count() > 0
            self._paused_by_user = False
            logf = open(self._log_path(), "a", encoding="utf-8")
            logf.write(f"\n=== {'resume' if resuming else 'start'} "
                       f"{datetime.now().isoformat()} ===\n")
            logf.flush()
            try:
                self.proc = subprocess.Popen(
                    cmd, cwd=os.path.dirname(SCRAPER),
                    stdout=logf, stderr=subprocess.STDOUT)
            except Exception as e:
                logf.close()
                self.status = "error"
                self.message = f"Failed to launch scraper: {e}"
                return False, self.message
            self.status = "running"
            self.message = ("Resuming…" if resuming else "Started.") + " Crawling in progress."
            self.thread = threading.Thread(
                target=self._monitor, args=(self.proc, logf), daemon=True)
            self.thread.start()
        return True, self.message

    def pause(self):
        with self.lock:
            if self.status != "running" or not self.proc:
                return False, "Nothing is running."
            self._paused_by_user = True
            proc = self.proc
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except Exception:
            proc.kill()
        # monitor thread sets final paused status
        return True, "Pausing…"

    def _monitor(self, proc, logf):
        code = proc.wait()
        try:
            logf.close()
        except Exception:
            pass
        with self.lock:
            if self.proc is not proc:
                return  # superseded by a newer run
            remaining = self.remaining()
            if self._paused_by_user:
                self.status = "paused"
                self.message = f"Paused. {self.done_count()} done, {remaining} remaining. Press Resume."
            elif code == 0 and remaining == 0:
                self.status = "done"
                self.message = f"Complete — all {self.done_count()} companies processed."
            elif code == 0:
                # exited cleanly but work remains (rare) — treat as resumable
                self.status = "paused"
                self.message = f"Run ended early. {remaining} companies remaining. Press Resume."
            else:
                # crashed / killed unexpectedly → auto-pause, resumable
                self.status = "auto_paused"
                self.message = (f"Auto-paused: scraper stopped unexpectedly "
                                f"(exit {code}). {remaining} remaining. {self._log_tail()} "
                                f"Press Resume to continue.")
            self.proc = None

    def _log_tail(self, n=2):
        try:
            lines = [l.strip() for l in open(self._log_path(), encoding="utf-8")
                     if l.strip()]
            return "Last log: " + " | ".join(lines[-n:]) if lines else ""
        except Exception:
            return ""

    # --- snapshot for the UI ---------------------------------------------
    def snapshot(self):
        st = self.status
        has_csv = bool(self.csv_path and os.path.exists(self.csv_path))
        if st == "running":
            label, action, enabled = "⏸  Pause", "pause", True
        elif st in ("paused", "auto_paused"):
            label, action, enabled = "▶  Resume", "start", True
        elif st == "done":
            label, action, enabled = "✓  Done", "start", False
        elif st == "error":
            label, action, enabled = "↻  Retry", "start", has_csv
        else:  # idle
            label, action, enabled = "▶  Start", "start", has_csv
        return {
            "status": st,
            "message": self.message,
            "button_label": label,
            "button_action": action,
            "button_enabled": enabled,
            "has_csv": has_csv,
            "csv_name": self.csv_name,
            "remaining": self.remaining(),
            "opts": self.opts,
        }


CONTROLLER = Controller()


# ---------------------------------------------------------------------------
# Data readers (safe — handle missing/partial files gracefully)
# ---------------------------------------------------------------------------

def read_report():
    path = os.path.join(CONFIG["out"], "report.csv")
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rows.append({
                        "company":        row.get("company", ""),
                        "website":        row.get("website", ""),
                        "career_page":    row.get("career_page", ""),
                        "ats_detected":   row.get("ats_detected", ""),
                        "method":         row.get("method", ""),
                        "jobs_found":     int(row.get("jobs_found", 0) or 0),
                        "needs_javascript": row.get("needs_javascript", "").lower() == "true",
                        "error":          row.get("error", ""),
                        "seconds":        float(row.get("seconds", 0) or 0),
                    })
                except Exception:
                    pass
    except Exception:
        pass
    return rows


def read_jobs(limit=2000):
    path = os.path.join(CONFIG["out"], "jobs.jsonl")
    jobs = []
    if not os.path.exists(path):
        return jobs
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        jobs.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass
    return jobs[-limit:] if len(jobs) > limit else jobs


def read_total():
    path = CONFIG["csv"]
    if not path or not os.path.exists(path):
        return 0
    try:
        with open(path, encoding="utf-8") as f:
            return max(0, sum(1 for _ in csv.reader(f)) - 1)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


@app.get("/api/data")
def get_data():
    report  = read_report()
    jobs    = read_jobs()
    total   = read_total() or len(report)

    success  = [r for r in report if r["jobs_found"] > 0]
    failed   = [r for r in report if r["error"]]
    no_jobs  = [r for r in report if not r["error"] and r["jobs_found"] == 0]

    ats_breakdown, method_breakdown, error_breakdown = {}, {}, {}
    for r in report:
        if r["ats_detected"]:
            ats_breakdown[r["ats_detected"]] = ats_breakdown.get(r["ats_detected"], 0) + 1
        m = r["method"] or "unknown"
        method_breakdown[m] = method_breakdown.get(m, 0) + 1
    for r in failed:
        key = r["error"].split(":")[0].strip()[:50] if ":" in r["error"] else r["error"][:50]
        error_breakdown[key] = error_breakdown.get(key, 0) + 1

    processed = len(report)
    avg_time  = sum(r["seconds"] for r in report) / max(processed, 1)

    return {
        "stats": {
            "total":        total,
            "processed":    processed,
            "progress_pct": round(processed / max(total, 1) * 100, 1),
            "jobs_total":   sum(r["jobs_found"] for r in report),
            "success_count": len(success),
            "failed_count":  len(failed),
            "no_jobs_count": len(no_jobs),
            "avg_time":      round(avg_time, 1),
        },
        "ats_breakdown":    dict(sorted(ats_breakdown.items(),    key=lambda x: -x[1])),
        "method_breakdown": dict(sorted(method_breakdown.items(), key=lambda x: -x[1])),
        "error_breakdown":  dict(sorted(error_breakdown.items(),  key=lambda x: -x[1])),
        "companies":        report,
        "jobs":             jobs,
        "control":          CONTROLLER.snapshot(),
        "last_updated":     datetime.now().isoformat(),
    }


@app.post("/api/upload")
async def upload_csv(request: Request):
    name = request.query_params.get("name", "uploaded.csv")
    raw = await request.body()
    if not raw:
        return JSONResponse({"ok": False, "message": "No file received."}, status_code=400)
    ok, msg = CONTROLLER.save_csv(raw, name)
    return JSONResponse({"ok": ok, "message": msg, "control": CONTROLLER.snapshot()},
                        status_code=200 if ok else 400)


@app.post("/api/options")
async def set_options(request: Request):
    body = await request.json()
    if CONTROLLER.status == "running":
        return JSONResponse({"ok": False, "message": "Can't change options while running."},
                            status_code=400)
    try:
        c = int(body.get("concurrency", CONTROLLER.opts["concurrency"]))
        CONTROLLER.opts["concurrency"] = max(1, min(100, c))
        CONTROLLER.opts["details"] = bool(body.get("details", CONTROLLER.opts["details"]))
        CONTROLLER.opts["browser"] = bool(body.get("browser", CONTROLLER.opts["browser"]))
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=400)
    return JSONResponse({"ok": True, "control": CONTROLLER.snapshot()})


@app.post("/api/start")
async def start_run():
    ok, msg = CONTROLLER.start()
    return JSONResponse({"ok": ok, "message": msg, "control": CONTROLLER.snapshot()},
                        status_code=200 if ok else 400)


@app.post("/api/pause")
async def pause_run():
    ok, msg = CONTROLLER.pause()
    return JSONResponse({"ok": ok, "message": msg, "control": CONTROLLER.snapshot()},
                        status_code=200 if ok else 400)


# ---------------------------------------------------------------------------
# Frontend — single-page dashboard
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Super Scraper Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background:#09090f; color:#e2e8f0; font-family:system-ui,sans-serif; }
    .card { background:#111118; border:1px solid #1e1e2e; border-radius:12px; }
    .tab-active   { background:#7c3aed; color:#fff; }
    .tab-inactive { color:#94a3b8; }
    .tab-inactive:hover { color:#fff; background:#1e1e2e; }
    .b-green  { background:#052e16; color:#4ade80; border:1px solid #166534; }
    .b-red    { background:#2d0a0a; color:#f87171; border:1px solid #7f1d1d; }
    .b-yellow { background:#2d1f00; color:#fbbf24; border:1px solid #78350f; }
    .b-blue   { background:#0c1a2e; color:#60a5fa; border:1px solid #1e3a5f; }
    .b-violet { background:#1a0a2e; color:#a78bfa; border:1px solid #4c1d95; }
    .b-teal   { background:#042020; color:#2dd4bf; border:1px solid #0f4444; }
    .b-gray   { background:#1a1a2e; color:#94a3b8; border:1px solid #374151; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
    .pulse { animation:pulse 2s infinite; }
    table { border-collapse:collapse; width:100%; }
    th { background:#0d0d14; position:sticky; top:0; z-index:10; font-weight:500; color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:.05em; }
    td,th { padding:10px 14px; text-align:left; border-bottom:1px solid #1e1e2e; font-size:13px; }
    tr:hover td { background:#16161f; }
    .tscroll { overflow-y:auto; max-height:530px; border-radius:8px; border:1px solid #1e1e2e; }
    input,select { background:#111118; border:1px solid #2d2d44; border-radius:8px; color:#e2e8f0; padding:8px 14px; font-size:13px; outline:none; }
    input:focus,select:focus { border-color:#7c3aed; }
    ::-webkit-scrollbar { width:5px; height:5px; }
    ::-webkit-scrollbar-track { background:#09090f; }
    ::-webkit-scrollbar-thumb { background:#2d2d44; border-radius:3px; }
    .bar-track { background:#1e1e2e; border-radius:999px; height:8px; }
    .bar-fill  { border-radius:999px; height:8px; transition:width .4s; }
  </style>
</head>
<body>
<div class="max-w-screen-2xl mx-auto px-5 py-5">

  <!-- Header -->
  <div class="flex items-center justify-between mb-5">
    <div class="flex items-center gap-3">
      <div class="w-9 h-9 rounded-xl bg-violet-600 flex items-center justify-center text-xl">⚡</div>
      <div>
        <h1 class="text-lg font-bold text-white tracking-tight">Super Scraper Dashboard</h1>
        <p class="text-xs text-slate-500" id="last-updated">Connecting…</p>
      </div>
    </div>
    <div class="flex items-center gap-2 card px-3 py-2">
      <div class="w-2 h-2 rounded-full bg-green-400 pulse" id="live-dot"></div>
      <span class="text-xs text-slate-400" id="live-label">Live · refreshes every 3s</span>
    </div>
  </div>

  <!-- Control Bar -->
  <div class="card p-4 mb-5">
    <div class="flex flex-wrap items-center gap-3">
      <!-- Upload -->
      <label class="px-3 py-2 rounded-lg text-sm font-medium cursor-pointer"
             style="background:#1e1e2e;color:#cbd5e1;border:1px solid #2d2d44">
        📁 <span id="csv-label">Upload CSV</span>
        <input type="file" id="csv-input" accept=".csv" class="hidden" onchange="uploadCsv(event)">
      </label>

      <!-- Single run/pause/resume button -->
      <button id="run-btn" onclick="runAction()"
              class="px-5 py-2 rounded-lg text-sm font-semibold transition-all"
              style="background:#7c3aed;color:#fff">▶  Start</button>

      <!-- Options -->
      <div class="flex items-center gap-2 text-xs text-slate-400">
        <span>Concurrency</span>
        <input type="number" id="opt-conc" value="30" min="1" max="100"
               style="width:70px;padding:6px 8px" onchange="saveOptions()">
      </div>
      <label class="flex items-center gap-1.5 text-xs text-slate-400 cursor-pointer">
        <input type="checkbox" id="opt-details" onchange="saveOptions()"> Details
      </label>
      <label class="flex items-center gap-1.5 text-xs text-slate-400 cursor-pointer">
        <input type="checkbox" id="opt-browser" onchange="saveOptions()"> JS Browser
      </label>

      <!-- Status message -->
      <div class="flex-1 text-right">
        <span id="ctrl-msg" class="text-xs text-slate-400"></span>
      </div>
    </div>
  </div>

  <!-- Stat Cards -->
  <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 mb-5">
    <div class="card p-4">
      <div class="text-xs text-slate-500 mb-1">Total</div>
      <div class="text-2xl font-bold text-white" id="s-total">—</div>
      <div class="text-xs text-slate-600">companies in CSV</div>
    </div>
    <div class="card p-4">
      <div class="text-xs text-slate-500 mb-1">Processed</div>
      <div class="text-2xl font-bold text-violet-400" id="s-proc">—</div>
      <div class="text-xs text-slate-600">crawled so far</div>
    </div>
    <div class="card p-4">
      <div class="text-xs text-slate-500 mb-1">Jobs Found</div>
      <div class="text-2xl font-bold text-green-400" id="s-jobs">—</div>
      <div class="text-xs text-slate-600">total across all</div>
    </div>
    <div class="card p-4">
      <div class="text-xs text-slate-500 mb-1">With Jobs</div>
      <div class="text-2xl font-bold text-emerald-400" id="s-succ">—</div>
      <div class="text-xs text-slate-600">success</div>
    </div>
    <div class="card p-4">
      <div class="text-xs text-slate-500 mb-1">Errors</div>
      <div class="text-2xl font-bold text-red-400" id="s-fail">—</div>
      <div class="text-xs text-slate-600">failed to crawl</div>
    </div>
    <div class="card p-4">
      <div class="text-xs text-slate-500 mb-1">No Jobs</div>
      <div class="text-2xl font-bold text-yellow-400" id="s-nojobs">—</div>
      <div class="text-xs text-slate-600">reachable, empty</div>
    </div>
  </div>

  <!-- Progress -->
  <div class="card p-4 mb-5">
    <div class="flex justify-between items-center mb-2">
      <span class="text-sm text-slate-400 font-medium">Overall Progress</span>
      <div class="flex items-center gap-3">
        <span class="text-xs text-slate-500" id="avg-time">—</span>
        <span class="text-sm font-bold text-white" id="prog-pct">0%</span>
      </div>
    </div>
    <div class="bar-track">
      <div class="bar-fill bg-violet-600" id="prog-bar" style="width:0%"></div>
    </div>
    <div class="text-xs text-slate-500 mt-2" id="prog-detail">Waiting for scraper…</div>
  </div>

  <!-- Tabs -->
  <div class="flex gap-1 mb-4 p-1 rounded-lg" style="background:#0d0d14;width:fit-content">
    <button onclick="showTab('companies')" id="tab-companies" class="px-4 py-1.5 rounded-md text-sm font-medium transition-all tab-active">Companies</button>
    <button onclick="showTab('jobs')"      id="tab-jobs"      class="px-4 py-1.5 rounded-md text-sm font-medium transition-all tab-inactive">Jobs</button>
    <button onclick="showTab('failures')"  id="tab-failures"  class="px-4 py-1.5 rounded-md text-sm font-medium transition-all tab-inactive">Failures</button>
    <button onclick="showTab('breakdown')" id="tab-breakdown" class="px-4 py-1.5 rounded-md text-sm font-medium transition-all tab-inactive">Breakdown</button>
  </div>

  <!-- COMPANIES TAB -->
  <div id="pane-companies">
    <div class="flex gap-3 mb-3">
      <input type="search" id="q-comp" placeholder="Search company, website, ATS, method…" oninput="renderCompanies()" style="flex:1">
      <select id="f-status" onchange="renderCompanies()" style="min-width:160px">
        <option value="all">All Status</option>
        <option value="success">Has Jobs</option>
        <option value="failed">Errors Only</option>
        <option value="nojobs">No Jobs</option>
      </select>
    </div>
    <div class="tscroll">
      <table>
        <thead><tr>
          <th>Company</th><th>Website</th><th>ATS</th><th>Method</th>
          <th style="text-align:right">Jobs</th><th>Time</th><th>Status / Error</th>
        </tr></thead>
        <tbody id="tb-companies"></tbody>
      </table>
    </div>
    <div class="text-xs text-slate-600 mt-2" id="comp-count"></div>
  </div>

  <!-- JOBS TAB -->
  <div id="pane-jobs" class="hidden">
    <div class="flex gap-3 mb-3">
      <input type="search" id="q-jobs" placeholder="Search title, company, location, department…" oninput="renderJobs()" style="flex:1">
      <select id="f-src" onchange="renderJobs()" style="min-width:170px">
        <option value="all">All Sources</option>
      </select>
    </div>
    <div class="tscroll">
      <table>
        <thead><tr>
          <th>Company</th><th>Title</th><th>Location</th><th>Department</th>
          <th>Type</th><th>Source</th><th>Posted</th><th>Link</th>
        </tr></thead>
        <tbody id="tb-jobs"></tbody>
      </table>
    </div>
    <div class="text-xs text-slate-600 mt-2" id="jobs-count"></div>
  </div>

  <!-- FAILURES TAB -->
  <div id="pane-failures" class="hidden">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
      <div>
        <h3 class="text-sm font-semibold text-slate-300 mb-3">Error Types</h3>
        <div id="err-breakdown" class="space-y-2"></div>
      </div>
      <div>
        <h3 class="text-sm font-semibold text-slate-300 mb-3">Failed Companies</h3>
        <div class="tscroll" style="max-height:460px">
          <table>
            <thead><tr><th>Company</th><th>Error Detail</th><th>Time</th></tr></thead>
            <tbody id="tb-failures"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- BREAKDOWN TAB -->
  <div id="pane-breakdown" class="hidden">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
      <div class="card p-5">
        <h3 class="text-sm font-semibold text-slate-300 mb-4">ATS Detected</h3>
        <div id="ats-bk" class="space-y-3"></div>
      </div>
      <div class="card p-5">
        <h3 class="text-sm font-semibold text-slate-300 mb-4">Extraction Method</h3>
        <div id="mth-bk" class="space-y-3"></div>
      </div>
    </div>
  </div>

</div>
<script>
let COMPANIES=[], JOBS=[];
let CTRL={status:'idle',button_action:'start',button_enabled:false};
let optsTouched=false;

// ---- Run controls --------------------------------------------------------
async function uploadCsv(e){
  const file=e.target.files[0];
  if(!file) return;
  setMsg('Uploading '+file.name+'…');
  try{
    const text=await file.text();
    const res=await fetch('/api/upload?name='+encodeURIComponent(file.name),
      {method:'POST',body:text});
    const data=await res.json();
    applyControl(data.control);
    setMsg(data.message);
  }catch(err){ setMsg('Upload failed: '+err); }
  e.target.value='';   // allow re-uploading the same file
}

async function runAction(){
  const action=CTRL.button_action;        // 'start' (also resume) or 'pause'
  const ep=action==='pause'?'/api/pause':'/api/start';
  const btn=document.getElementById('run-btn');
  btn.disabled=true;
  try{
    const res=await fetch(ep,{method:'POST'});
    const data=await res.json();
    applyControl(data.control);
    setMsg(data.message);
  }catch(err){ setMsg('Action failed: '+err); }
  fetchData();
}

async function saveOptions(){
  optsTouched=true;
  const body={
    concurrency: parseInt(document.getElementById('opt-conc').value||'30'),
    details: document.getElementById('opt-details').checked,
    browser: document.getElementById('opt-browser').checked,
  };
  try{ await fetch('/api/options',{method:'POST',
        headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); }
  catch(err){}
}

function setMsg(m){ document.getElementById('ctrl-msg').textContent=m||''; }

function applyControl(c){
  if(!c) return;
  CTRL=c;
  const btn=document.getElementById('run-btn');
  btn.textContent=c.button_label;
  btn.disabled=!c.button_enabled;
  btn.style.opacity=c.button_enabled?'1':'0.45';
  btn.style.cursor=c.button_enabled?'pointer':'not-allowed';
  btn.style.background = c.status==='running' ? '#b45309'
                       : (c.status==='auto_paused'||c.status==='error') ? '#b91c1c'
                       : '#7c3aed';
  document.getElementById('csv-label').textContent=c.has_csv?(c.csv_name||'CSV loaded'):'Upload CSV';
  // reflect server-side options unless the user is actively editing
  if(!optsTouched && c.opts){
    document.getElementById('opt-conc').value=c.opts.concurrency;
    document.getElementById('opt-details').checked=c.opts.details;
    document.getElementById('opt-browser').checked=c.opts.browser;
  }
}

function showTab(name){
  ['companies','jobs','failures','breakdown'].forEach(t=>{
    document.getElementById('pane-'+t).classList.add('hidden');
    const b=document.getElementById('tab-'+t);
    b.className='px-4 py-1.5 rounded-md text-sm font-medium transition-all tab-inactive';
  });
  document.getElementById('pane-'+name).classList.remove('hidden');
  document.getElementById('tab-'+name).className='px-4 py-1.5 rounded-md text-sm font-medium transition-all tab-active';
}

function bdg(text,cls){
  if(!text) return '<span class="text-slate-600 text-xs">—</span>';
  return `<span class="px-2 py-0.5 rounded-full text-xs font-medium ${cls}">${text}</span>`;
}
function methodCls(m){
  if(!m) return 'b-gray';
  if(m.startsWith('ats:')) return 'b-blue';
  if(m==='jsonld') return 'b-green';
  if(m==='heuristic') return 'b-yellow';
  if(m.startsWith('ai:')) return 'b-violet';
  if(m==='browser') return 'b-yellow';
  if(m==='embedded_json') return 'b-teal';
  return 'b-gray';
}
function barColor(m){
  if(!m) return 'bg-slate-500';
  if(m.startsWith('ats:')) return 'bg-blue-500';
  if(m==='jsonld') return 'bg-green-500';
  if(m==='heuristic') return 'bg-yellow-500';
  if(m.startsWith('ai:')) return 'bg-violet-500';
  if(m==='browser') return 'bg-orange-500';
  if(m==='embedded_json') return 'bg-teal-500';
  return 'bg-slate-500';
}
function trunc(s,n){ return s&&s.length>n?s.slice(0,n)+'…':(s||''); }
function statusCell(r){
  if(r.error) return bdg('Failed','b-red');
  if(r.jobs_found>0) return bdg('✓ '+r.jobs_found+' jobs','b-green');
  return bdg('No jobs','b-yellow');
}

function renderCompanies(){
  const q=(document.getElementById('q-comp').value||'').toLowerCase();
  const f=document.getElementById('f-status').value;
  let rows=COMPANIES;
  if(q) rows=rows.filter(r=>
    (r.company||'').toLowerCase().includes(q)||
    (r.website||'').toLowerCase().includes(q)||
    (r.ats_detected||'').toLowerCase().includes(q)||
    (r.method||'').toLowerCase().includes(q));
  if(f==='success') rows=rows.filter(r=>r.jobs_found>0);
  if(f==='failed')  rows=rows.filter(r=>r.error);
  if(f==='nojobs')  rows=rows.filter(r=>!r.error&&r.jobs_found===0);

  document.getElementById('tb-companies').innerHTML=rows.map(r=>`
    <tr>
      <td class="text-white font-medium">${trunc(r.company,36)}</td>
      <td><a href="${r.career_page||r.website}" target="_blank"
            class="text-violet-400 hover:text-violet-300 text-xs">${trunc(r.website,38)}</a></td>
      <td>${bdg(r.ats_detected,'b-blue')}</td>
      <td>${bdg(r.method,methodCls(r.method))}</td>
      <td style="text-align:right" class="font-mono text-sm ${r.jobs_found>0?'text-green-400':'text-slate-600'}">${r.jobs_found}</td>
      <td class="text-slate-500 text-xs font-mono">${r.seconds}s</td>
      <td>${r.error
        ?`<span class="text-red-400 text-xs" title="${r.error}">${trunc(r.error,65)}</span>`
        :statusCell(r)}</td>
    </tr>`).join('');
  document.getElementById('comp-count').textContent=`Showing ${rows.length} of ${COMPANIES.length} companies`;
}

function renderJobs(){
  const q=(document.getElementById('q-jobs').value||'').toLowerCase();
  const src=document.getElementById('f-src').value;
  let rows=JOBS;
  if(q) rows=rows.filter(j=>
    (j.title||'').toLowerCase().includes(q)||
    (j.company||'').toLowerCase().includes(q)||
    (j.location||'').toLowerCase().includes(q)||
    (j.department||'').toLowerCase().includes(q));
  if(src!=='all') rows=rows.filter(j=>(j.source_method||'')===src);

  const shown=rows.slice(0,1000);
  document.getElementById('tb-jobs').innerHTML=shown.map(j=>`
    <tr>
      <td class="text-slate-400 text-xs">${trunc(j.company,26)}</td>
      <td class="text-white font-medium">${trunc(j.title,55)}</td>
      <td class="text-slate-400 text-xs">${trunc(j.location,26)}</td>
      <td class="text-slate-400 text-xs">${trunc(j.department,22)}</td>
      <td>${j.employment_type?bdg(j.employment_type,'b-gray'):''}</td>
      <td>${bdg(j.source_method,methodCls(j.source_method))}</td>
      <td class="text-slate-500 text-xs font-mono">${j.posted_date||'—'}</td>
      <td>${j.url?`<a href="${j.url}" target="_blank" class="text-violet-400 hover:text-violet-300 text-xs">↗ Open</a>`:'—'}</td>
    </tr>`).join('');
  document.getElementById('jobs-count').textContent=
    `Showing ${shown.length} of ${rows.length} filtered (${JOBS.length} total loaded)`;
}

function renderBreakdown(data){
  function barSection(obj,colorFn){
    const total=Object.values(obj).reduce((a,b)=>a+b,0);
    if(!total) return '<p class="text-slate-600 text-sm">No data yet</p>';
    return Object.entries(obj).map(([k,v])=>`
      <div>
        <div class="flex justify-between text-xs mb-1">
          <span class="text-slate-300">${k}</span>
          <span class="text-slate-400">${v} &nbsp;(${Math.round(v/total*100)}%)</span>
        </div>
        <div class="bar-track"><div class="bar-fill ${colorFn(k)}" style="width:${Math.round(v/total*100)}%"></div></div>
      </div>`).join('');
  }
  document.getElementById('ats-bk').innerHTML=barSection(data.ats_breakdown,()=>'bg-blue-500');
  document.getElementById('mth-bk').innerHTML=barSection(data.method_breakdown,barColor);
}

function renderFailures(data){
  const failed=data.companies.filter(r=>r.error);
  document.getElementById('err-breakdown').innerHTML=
    Object.entries(data.error_breakdown).length
    ? Object.entries(data.error_breakdown).map(([k,v])=>`
        <div class="card p-3 flex justify-between items-center">
          <span class="text-red-400 text-sm">${k}</span>
          <span class="b-red px-2 py-0.5 rounded-full text-xs font-bold">${v}</span>
        </div>`).join('')
    : '<p class="text-slate-600 text-sm">No failures yet</p>';

  document.getElementById('tb-failures').innerHTML=failed.length
    ? failed.map(r=>`
        <tr>
          <td class="text-slate-300 text-xs">${trunc(r.company,30)}</td>
          <td class="text-red-400 text-xs" title="${r.error}">${trunc(r.error,72)}</td>
          <td class="text-slate-500 text-xs font-mono">${r.seconds}s</td>
        </tr>`).join('')
    : '<tr><td colspan="3" class="text-slate-600 text-center py-10">No failures yet 🎉</td></tr>';
}

function updateSrcFilter(jobs){
  const sources=[...new Set(jobs.map(j=>j.source_method).filter(Boolean))].sort();
  const sel=document.getElementById('f-src');
  const cur=sel.value;
  sel.innerHTML='<option value="all">All Sources</option>'+
    sources.map(s=>`<option value="${s}">${s}</option>`).join('');
  if(sources.includes(cur)) sel.value=cur;
}

async function fetchData(){
  try{
    const res=await fetch('/api/data');
    const data=await res.json();
    COMPANIES=data.companies;
    JOBS=data.jobs;
    const s=data.stats;

    document.getElementById('s-total').textContent  = s.total.toLocaleString();
    document.getElementById('s-proc').textContent   = s.processed.toLocaleString();
    document.getElementById('s-jobs').textContent   = s.jobs_total.toLocaleString();
    document.getElementById('s-succ').textContent   = s.success_count.toLocaleString();
    document.getElementById('s-fail').textContent   = s.failed_count.toLocaleString();
    document.getElementById('s-nojobs').textContent = s.no_jobs_count.toLocaleString();

    document.getElementById('prog-bar').style.width  = s.progress_pct+'%';
    document.getElementById('prog-pct').textContent  = s.progress_pct+'%';
    document.getElementById('prog-detail').textContent =
      `${s.processed.toLocaleString()} of ${s.total.toLocaleString()} companies processed`;
    document.getElementById('avg-time').textContent  = `Avg ${s.avg_time}s per company`;

    // run state drives the indicator + the single control button
    if(data.control){
      applyControl(data.control);
      if(data.control.message && CTRL.status!=='idle') setMsg(data.control.message);
    }
    const st=(data.control&&data.control.status)||'idle';
    const dotMap={running:'bg-amber-400 pulse',paused:'bg-amber-500',
      auto_paused:'bg-red-500 pulse',error:'bg-red-500',done:'bg-green-500',idle:'bg-slate-500'};
    const labelMap={running:'Running…',paused:'Paused',auto_paused:'Auto-paused',
      error:'Error',done:'Complete',idle:'Idle · refreshes every 3s'};
    document.getElementById('live-dot').className='w-2 h-2 rounded-full '+(dotMap[st]||'bg-slate-500');
    document.getElementById('live-label').textContent=labelMap[st]||'Live · refreshes every 3s';

    const d=new Date(data.last_updated);
    document.getElementById('last-updated').textContent='Last updated: '+d.toLocaleTimeString();

    updateSrcFilter(JOBS);
    renderCompanies();
    renderJobs();
    renderBreakdown(data);
    renderFailures(data);
  }catch(e){
    document.getElementById('last-updated').textContent='Waiting for scraper output…';
  }
}

fetchData();
setInterval(fetchData, 3000);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Super Scraper Dashboard")
    p.add_argument("--out",  default="out",  help="scraper output directory")
    p.add_argument("--csv",  default="",     help="original leads CSV (shows total count)")
    p.add_argument("--port", type=int, default=8050)
    args = p.parse_args()
    CONFIG["out"] = args.out
    CONFIG["csv"] = args.csv

    # Pre-load a CLI-supplied CSV into the controller so Start works immediately.
    if args.csv and os.path.exists(args.csv):
        CONTROLLER.csv_path = args.csv
        CONTROLLER.csv_name = os.path.basename(args.csv)
        rows, done = CONTROLLER.total(), CONTROLLER.done_count()
        if done:
            CONTROLLER.status = "paused"
            CONTROLLER.message = f"{args.csv}: {done} of {rows} already done. Press Resume."
        else:
            CONTROLLER.status = "idle"
            CONTROLLER.message = f"{args.csv}: {rows} companies ready. Press Start."
    else:
        CONTROLLER.message = "Upload a CSV to begin."

    import webbrowser
    print(f"Dashboard → http://localhost:{args.port}")
    webbrowser.open(f"http://localhost:{args.port}")
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
