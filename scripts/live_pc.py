"""Live prices on the owner's PC: Angel One ticks and candles, served only to this PC's browser (ws://127.0.0.1:8787/ws/ticks).

  python scripts/live_pc.py          # Mon-Fri 08:55-15:50 IST (idles otherwise); started at logon by Windows Task Scheduler

Why here and not on GitHub: live data needs a program that stays connected all day (GitHub jobs must not be used as an always-on
server), and Angel One's data is licensed to the account holder, so it is shown only on this PC. The public site keeps delayed prices.
Light: about 105 instruments (NIFTY 100 + indices) plus the stocks open in the terminal (at most 2 extra connections), no jobs.
Same messages as scripts/aladin_ticker_daemon.py: hello, snap, ticks, candles, focus. Keys from .env; never printed.
"""
import asyncio
import csv
import io
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "local")]
from launch import load_keys  # noqa: E402

import angel_feed  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
PORT = 8787
ORIGINS = ["https://aninjalive-alt.github.io", "http://127.0.0.1:8765", "http://localhost:8765", None]
N100_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv"
CACHE = ROOT / "local"
LOGF = CACHE / "logs" / "live_pc.log"


def log(msg):
    LOGF.parent.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} {msg}"
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def window(t=None):
    t = t or datetime.now(IST)
    m = t.hour * 60 + t.minute
    return t.weekday() < 5 and 8 * 60 + 55 <= m < 15 * 60 + 50


def nifty100():
    import requests
    f = CACHE / "nifty100.json"
    try:
        r = requests.get(N100_URL, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}, timeout=20)
        syms = [row["Symbol"].strip() for row in csv.DictReader(io.StringIO(r.text)) if row.get("Symbol")]
        if len(syms) >= 90:
            f.write_text(json.dumps(syms), encoding="utf-8")
            return syms
    except Exception:  # noqa: BLE001
        pass
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else []


def master():
    """Angel One's instrument list, downloaded at most once a day (about 40 MB)."""
    import requests
    f = CACHE / "scripmaster.json"
    if not f.exists() or datetime.fromtimestamp(f.stat().st_mtime, IST).date() != datetime.now(IST).date():
        log("downloading Angel One's instrument list (once a day)")
        with requests.get(angel_feed.MASTER_URL, timeout=60, stream=True) as r:
            r.raise_for_status()
            tmp = f.with_suffix(".tmp")
            with open(tmp, "wb") as out:
                for ch in r.iter_content(1 << 20):
                    out.write(ch)
            tmp.replace(f)
    data = json.loads(f.read_text(encoding="utf-8"))
    return type("R", (), {"json": lambda self: data})()


class Live:
    def __init__(self):
        self.env = dict(os.environ)
        load_keys(environ=self.env)
        self.ticks, self.pending, self.lock = {}, {}, threading.Lock()
        self.clients, self.feed, self.wanted, self.extra = set(), None, set(), 0

    def on_row(self, sym, row):
        with self.lock:
            self.pending[sym] = row

    def start_feed(self):
        self.feed = angel_feed.AngelFeed(self.env, self.on_row)
        m = master()
        self.feed.http_get = lambda url, timeout=None: m
        self.feed.prepare()
        full = self.feed.sym_token
        self.wanted = {s for s in nifty100() if s in full} | {s for _, _, s in angel_feed.INDEXES}
        self.feed.sym_token = {s: full[s] for s in self.wanted}
        self.feed.start()
        self.feed.sym_token = full
        self.extra = 0
        log(f"Angel One live: {len(self.wanted)} instruments")

    def focus(self, syms):
        new = [s for s in syms if self.feed and s in self.feed.sym_token and s not in self.wanted][:20]
        if not new or self.extra >= 2:                                 # Angel One allows 3 connections: 1 for the NIFTY 100, at most 2 for extras
            return
        self.extra += 1
        self.wanted |= set(new)
        groups = [{"exchangeType": angel_feed.EXCH_TYPE[self.feed.sym_token[s][0]], "tokens": [self.feed.sym_token[s][1]]} for s in new]
        threading.Thread(target=self.feed._run_conn, args=(90 + self.extra, groups), daemon=True).start()
        log("added: " + ", ".join(new))

    def hello(self):
        return {"type": "hello", "ok": bool(self.feed), "src": "Angel One (this PC)", "market": "open" if window() else "closed"}

    async def handler(self, ws):
        self.clients.add(ws)
        try:
            await ws.send(json.dumps(self.hello()))
            await ws.send(json.dumps({"type": "snap", "q": self.ticks}, separators=(",", ":")))
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except ValueError:
                    continue
                if m.get("type") == "candles":
                    bars = await asyncio.to_thread(self.feed.candles, m.get("sym"), m.get("iv")) if self.feed else None
                    await ws.send(json.dumps({"type": "candles", "id": m.get("id"), "sym": m.get("sym"), "iv": m.get("iv"), "bars": bars}, separators=(",", ":")))
                elif m.get("type") == "focus":
                    self.focus([s for s in (m.get("syms") or [])[:50] if isinstance(s, str)])
        finally:
            self.clients.discard(ws)

    async def pump(self):
        from websockets.asyncio.server import broadcast
        while True:
            with self.lock:
                batch, self.pending = self.pending, {}
            if batch:
                self.ticks.update(batch)
                if self.clients:
                    broadcast(self.clients, json.dumps({"type": "ticks", "q": batch}, separators=(",", ":")))
            await asyncio.sleep(0.2)

    async def supervise(self):
        while True:
            if window() and self.feed is None:
                try:
                    self.start_feed()
                except Exception as e:  # noqa: BLE001 - the feed's RuntimeErrors never contain a secret
                    log(f"start failed: {type(e).__name__}: {str(e)[:120] if isinstance(e, RuntimeError) else ''}; retrying in 60 s")
                    self.feed = None
            elif not window() and self.feed is not None:
                self.feed.stop()
                self.feed = None
                log("market window over: feed stopped")
            await asyncio.sleep(60)

    async def main(self):
        from websockets.asyncio.server import serve
        async with serve(self.handler, "127.0.0.1", PORT, origins=ORIGINS, max_size=2**22):
            log(f"serving ws://127.0.0.1:{PORT}/ws/ticks")
            await asyncio.gather(self.pump(), self.supervise())


if __name__ == "__main__":
    os.chdir(ROOT)
    asyncio.run(Live().main())
