"""Local runner: the website's GitHub jobs, run on this PC instead (GitHub account suspended, 18 Nov 2026).

  python local/runner.py                 # serve the site, keep the live tick daemon up in market hours, run every job on its schedule
  python local/runner.py run live        # run one job now (live | daily | aladin | aladin2 | sentiment | crypto | globe | blab | nexus | nexus_predict | archive)
  python local/runner.py status          # last run of every job

Same scripts, same order and the same times (IST) as the .github/workflows files. What differs, and why:
  - GitHub-only steps are left out: pushing data commits, `gh workflow run` (the chain is explicit here), run reports, the watchdog
    (it cancelled stuck GitHub runs and pruned the Actions cache; here every step has a time limit instead).
  - The "history" and "intraday" data branches are read from the local git branches of the same names.
  - Two lanes so a 2-hour nightly job never stops live prices: FAST (live, crypto, sentiment, globe) and SLOW (the rest).
    Building the site is shared, so it is done under one lock.
  - Keys come from the main project's .env (local/launch.py); nothing is printed or logged.
Logs: local/logs/<date>.log. Status: local/status.json.
"""
import json
import os
import queue
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from launch import load_keys  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LOGS, STATUS = ROOT / "local" / "logs", ROOT / "local" / "status.json"
IST = timezone(timedelta(hours=5, minutes=30))
PY = sys.executable
SITE_PORT, TICK_PORT = 8765, 8787
SERVER = os.environ.get("SAMNIDHY_ROLE") == "server"
BUILD_LOCK, STATUS_LOCK = threading.Lock(), threading.Lock()


def py(script, *args, timeout=1800):
    return ("py", script, list(args), timeout)


def build():
    return ("build", None, [], 900)


def history_branch(*files):
    return ("history", None, list(files), 600)


JOBS = {
    # live-prices.yml (+ the replay library when missing)
    "live": ("fast", [("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900),
                      py("scripts/terminal_live.py", timeout=600),
                      ("if_missing", "data/terminal/replay/index.json", ["scripts/replay_data.py"], 1800),
                      py("scripts/screener_live.py", timeout=600), py("scripts/live_technicals.py", timeout=900),
                      py("scripts/news_wire.py", timeout=300), py("scripts/nse_live_poll.py", timeout=300), build()]),
    # daily-update.yml
    "daily": ("slow", [py("scripts/update.py", timeout=2400), py("scripts/screener.py", timeout=2400), py("scripts/institutional.py", timeout=600),
                       ("sleep", 45, [], 60), py("scripts/live_technicals.py", timeout=900), py("scripts/nse_eod.py", timeout=1200),
                       py("scripts/terminal_hist.py", timeout=3600), py("scripts/indices_eod.py", timeout=900), py("scripts/aladin_e.py", timeout=900),
                       py("scripts/aladin_e_weekly.py", timeout=600), ("node", "scripts/technicals_record.mjs", [], 600),
                       py("scripts/terminal_fund.py", timeout=2400), py("scripts/check_houses.py", timeout=300), py("scripts/terminal_live.py", timeout=600),
                       py("scripts/replay_data.py", timeout=1800), py("scripts/intraday_deep.py", timeout=2400), history_branch(),
                       py("scripts/predict_model.py", timeout=2400), py("scripts/news_wire.py", timeout=300), py("scripts/nse_live_poll.py", timeout=300),
                       build()]),
    # aladin.yml (the evening daily update starts it; it starts aladin2)
    "aladin": ("slow", [history_branch(), ("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900),
                        py("scripts/aladin_fund.py", "--max-minutes", "22", timeout=1800), py("scripts/aladin_filings.py", "--max-minutes", "15", timeout=1500),
                        py("scripts/graph_impact.py", timeout=900), py("scripts/aladin_model.py", "--nightly", "--workers", "2", timeout=5400),
                        py("scripts/aladin_moves.py", timeout=600), py("scripts/paper_trader.py", timeout=600), build()]),
    # aladin2.yml
    "aladin2": ("slow", [history_branch(), ("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900),
                         ("module", "scripts.aladin2.run_nightly", ["--budget-min", "110", "--workers", "2", "--steps",
                                                                     "ingest,resolve,learn,drift,discover,forecast,weekly,paper,bot,publish"], 8400),
                         ("events_calendar", None, [], 900), py("scripts/aladin_movers.py", "--live", "--workers", "2", timeout=3600),
                         py("scripts/aladin2/check_aladin2.py", timeout=600), build()]),
    # aladin-sentiment.yml
    "sentiment": ("fast", [("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900), py("scripts/news_wire.py", timeout=300),
                           py("scripts/aladin_sentiment_engine.py", "--once", timeout=1200), build()]),
    # crypto-live.yml
    "crypto": ("fast", [py("scripts/crypto_shadow.py", timeout=600), py("scripts/crypto_live.py", timeout=600), build()]),
    # globe-data.yml
    "globe": ("fast", [py("scripts/globe_data.py", timeout=600), py("scripts/globe_layers.py", timeout=900), py("scripts/geo_tension.py", timeout=600), build()]),
    # blab.yml: the four divisions one after another, then collect
    "blab": ("slow", [history_branch("_5eNSEI.json", "_5eNSEBANK.json")]
             + [py("scripts/blab/run.py", "--division", d, "--out", f"blab_{d}.json", timeout=3600) for d in ("fundamental", "technical", "macro", "sentiment")]
             + [py("scripts/blab/run.py", "--collect", "blab_fundamental.json", "blab_technical.json", "blab_macro.json", "blab_sentiment.json", timeout=1800), build()]),
    # nexus.yml
    "nexus": ("slow", [("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900), py("scripts/brsr_profile.py", timeout=2400),
                       py("scripts/revenue_profile.py", "--limit", "250", "--max-calls", "200", timeout=3600),
                       py("scripts/revenue_graph_builder.py", "--limit", "250", "--max-calls", "420", timeout=5400),
                       py("scripts/related_party_shares.py", "--limit", "40", "--max-calls", "150", timeout=2400),
                       py("scripts/geocode_facilities.py", timeout=1800), py("scripts/check_graph.py", timeout=600), build()]),
    # nexus-predict.yml
    "nexus_predict": ("slow", [("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900), py("scripts/company_profiles.py", "--max", "1200", timeout=3600),
                               py("scripts/filing_mentions.py", "--limit", "300", timeout=3600), py("scripts/nexus_predict.py", timeout=3600), build()]),
    # history-archive.yml (monthly; written to data/terminal/archive, not pushed)
    "archive": ("slow", [("if_missing", "data/terminal/universe.json", ["scripts/nse_eod.py"], 900), py("scripts/terminal_archive.py", timeout=7200)]),
}
CHAIN = {"daily@19:00": "aladin", "aladin": "aladin2"}                 # what GitHub's explicit `gh workflow run` steps did


def now():
    return datetime.now(IST)


def log(msg):
    LOGS.mkdir(parents=True, exist_ok=True)
    line = f"{now():%H:%M:%S} {msg}"
    with open(LOGS / f"{now():%Y-%m-%d}.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def set_status(job, **kw):
    with STATUS_LOCK:
        try:
            st = json.loads(STATUS.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            st = {}
        st.setdefault(job, {}).update(kw)
        STATUS.write_text(json.dumps(st, indent=1), encoding="utf-8")


def env():
    e = dict(os.environ)
    load_keys(environ=e)
    e["PYTHONIOENCODING"] = "utf-8"
    e["PYTHONUTF8"] = "1"
    return e


def run_cmd(cmd, timeout, label):
    t = time.time()
    try:
        p = subprocess.run(cmd, cwd=ROOT, env=env(), capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
        tail = (p.stdout + p.stderr).strip().splitlines()[-3:]
        ok = p.returncode == 0
        log(f"    {'ok  ' if ok else 'FAIL'} {label} ({time.time() - t:.0f}s){'' if ok else ' | ' + ' / '.join(x[:160] for x in tail)}")
        return ok
    except subprocess.TimeoutExpired:
        log(f"    TIME {label}: stopped after {timeout}s")
        return False
    except OSError as e:
        log(f"    FAIL {label}: {type(e).__name__}")
        return False


def run_step(step):
    kind, target, args, timeout = step
    if kind == "py":
        return run_cmd([PY, target, *args], timeout, " ".join([target, *args]))
    if kind == "module":
        return run_cmd([PY, "-m", target, *args], timeout, target)
    if kind == "node":
        return run_cmd(["node", target, *args], timeout, target)
    if kind == "if_missing":
        return True if (ROOT / target).exists() else run_cmd([PY, args[0]], timeout, args[0] + " (missing " + target + ")")
    if kind == "sleep":
        time.sleep(target)
        return True
    if kind == "history":                                             # the local "history" branch -> data/terminal/archive
        out = ROOT / "data/terminal/archive"
        out.mkdir(parents=True, exist_ok=True)
        try:
            tar = subprocess.run(["git", "archive", "history", *args], cwd=ROOT, capture_output=True, timeout=timeout, check=True).stdout
            subprocess.run(["tar", "-x", "-C", str(out)], input=tar, capture_output=True, timeout=timeout, check=True)
            log("    ok   history branch -> data/terminal/archive")
            return True
        except (subprocess.SubprocessError, OSError) as e:
            log(f"    FAIL history branch: {type(e).__name__}")
            return False
    if kind == "events_calendar":
        start = (now() - timedelta(days=62)).strftime("%Y-%m")
        return run_cmd([PY, "scripts/events_calendar.py", "--start", start], timeout, "scripts/events_calendar.py")
    if kind == "build":
        with BUILD_LOCK:
            return run_cmd([PY, "scripts/embed_data.py"], 600, "embed_data.py") & run_cmd([PY, "scripts/build_site.py"], timeout, "build_site.py")
    raise ValueError(kind)


def run_job(name, why="manual"):
    lane, steps = JOBS[name]
    t = time.time()
    log(f"START {name} ({why})")
    set_status(name, started=now().isoformat(timespec="seconds"), running=True)
    fails = sum(not run_step(s) for s in steps)
    log(f"END   {name}: {len(steps) - fails}/{len(steps)} steps ok, {(time.time() - t) / 60:.1f} min")
    set_status(name, finished=now().isoformat(timespec="seconds"), running=False, steps=len(steps), failed=fails, minutes=round((time.time() - t) / 60, 1))


# ---------------- schedule (IST), the same times as the workflows and the market clock ----------------
def market_day(d):
    return d.weekday() < 5


def due(name, t, last):
    """Is `name` due at time t, given the time it last started (None = never)?"""
    m = t.hour * 60 + t.minute
    gap = lambda s: last is None or (t - last).total_seconds() >= s  # noqa: E731
    open_hours = market_day(t) and 9 * 60 <= m < 16 * 60
    if name == "live":
        return gap(300 if open_hours else 1800)
    if name == "crypto":
        return gap(900)
    if name == "sentiment":
        return gap(1800 if open_hours else 3600)
    if name == "globe":
        return gap(3600)
    return False


DAILY_AT = [("07:30", "daily"), ("09:00", "daily"), ("09:05", "nexus"), ("08:15", "blab"), ("19:00", "daily"), ("21:00", "blab"), ("03:15", "nexus_predict")]


class Runner:
    def __init__(self):
        self.lanes = {"fast": queue.Queue(), "slow": queue.Queue()}
        self.queued, self.last, self.fired = set(), {}, set()
        self.lock = threading.Lock()
        self.daemon = None

    def enqueue(self, name, why):
        with self.lock:
            if name in self.queued:
                return
            self.queued.add(name)
            self.last[name] = now()
        self.lanes[JOBS[name][0]].put((name, why))

    def worker(self, lane):
        while True:
            name, why = self.lanes[lane].get()
            try:
                run_job(name, why)
            except Exception as e:  # noqa: BLE001 - a job error must never stop the lane
                log(f"ERROR {name}: {type(e).__name__}")
            with self.lock:
                self.queued.discard(name)
            nxt = CHAIN.get(f"{name}@{why}") or (CHAIN.get(name) if name != "daily" else None)
            if nxt:
                self.enqueue(nxt, f"after {name}")

    def tick_daemon(self):
        if SERVER:                                                     # the server never uses Angel One: the PC bridge sends the data
            return
        """The Angel One tick daemon (live ticker and 1-minute charts), up from 08:55 to 15:50 on weekdays, restarted if it stops."""
        t = now()
        m = t.hour * 60 + t.minute
        want = market_day(t) and 8 * 60 + 55 <= m < 15 * 60 + 50
        alive = self.daemon is not None and self.daemon.poll() is None
        if want and not alive:
            LOGS.mkdir(parents=True, exist_ok=True)
            out = open(LOGS / f"ticks-{t:%Y-%m-%d}.log", "a", encoding="utf-8")
            self.daemon = subprocess.Popen([PY, "local/launch.py", "scripts/aladin_ticker_daemon.py", "--source", "angel", "--no-telemetry"],
                                           cwd=ROOT, env=env(), stdout=out, stderr=subprocess.STDOUT)
            log("tick daemon started (Angel One)")
        elif not want and alive:
            self.daemon.terminate()
            log("tick daemon stopped (market closed)")

    def serve_site(self):
        if SERVER:                                                     # nginx serves the site on the server, behind a password
            return
        handler = partial(SimpleHTTPRequestHandler, directory=str(ROOT / "site"))
        handler.log_message = lambda *a, **k: None
        srv = ThreadingHTTPServer(("127.0.0.1", SITE_PORT), handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        log(f"website: http://127.0.0.1:{SITE_PORT}/")

    def loop(self):
        self.serve_site()
        for lane in self.lanes:
            threading.Thread(target=self.worker, args=(lane,), daemon=True).start()
        log("runner started")
        while True:
            t = now()
            for name in ("live", "crypto", "sentiment", "globe"):
                if due(name, t, self.last.get(name)):
                    self.enqueue(name, "schedule")
            hm = t.strftime("%H:%M")
            for at, name in DAILY_AT:
                key = f"{t:%Y-%m-%d} {at} {name}"
                if hm >= at and key not in self.fired and (t - t.replace(hour=int(at[:2]), minute=int(at[3:]))).total_seconds() < 1800:
                    self.fired.add(key)
                    self.enqueue(name, at)
            if t.day == 1 and hm >= "02:00" and f"{t:%Y-%m} archive" not in self.fired:
                self.fired.add(f"{t:%Y-%m} archive")
                self.enqueue("archive", "monthly")
            try:
                self.tick_daemon()
            except OSError as e:
                log(f"tick daemon: {type(e).__name__}")
            time.sleep(20)


if __name__ == "__main__":
    os.chdir(ROOT)
    if len(sys.argv) >= 3 and sys.argv[1] == "run":
        run_job(sys.argv[2])
    elif len(sys.argv) >= 2 and sys.argv[1] == "status":
        print(STATUS.read_text(encoding="utf-8") if STATUS.exists() else "no runs yet")
    else:
        Runner().loop()
