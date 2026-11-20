"""PC bridge: the ONLY place the Angel One keys are used. Live ticks for the NIFTY 100 + indices (+ the stocks open on the
terminal), and Angel One candles on request, sent to the Azure server through an SSH tunnel.

  python local/angel_bridge.py            # runs Mon-Fri 08:55-15:50 IST, idles otherwise; reconnects by itself

Light on memory: about 105 subscribed instruments (not 3,000), no jobs on this PC.
"""
import asyncio
import csv
import io
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "local"))
from launch import load_keys  # noqa: E402

import angel_feed  # noqa: E402

SERVER, USER, KEY = "20.196.218.57", "samnidhy", str(Path.home() / ".ssh" / "samnidhy_azure")
TUNNEL_PORT = 18788
IST = timezone(timedelta(hours=5, minutes=30))
N100_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv"
N100_CACHE = ROOT / "local" / "nifty100.json"
LOG = ROOT / "local" / "logs"


def log(msg):
    LOG.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} {msg}"
    with open(LOG / "bridge.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def nifty100():
    """NSE's official NIFTY 100 list; cached copy if NSE is unreachable; else the 100 most-traded NIFTY 500 stocks."""
    import requests
    try:
        r = requests.get(N100_URL, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}, timeout=20)
        syms = [row["Symbol"].strip() for row in csv.DictReader(io.StringIO(r.text)) if row.get("Symbol")]
        if len(syms) >= 90:
            N100_CACHE.write_text(json.dumps(syms), encoding="utf-8")
            return syms, "NSE"
    except Exception:  # noqa: BLE001
        pass
    if N100_CACHE.exists():
        return json.loads(N100_CACHE.read_text(encoding="utf-8")), "cached NSE list"
    u = json.loads((ROOT / "data/terminal/universe.json").read_text(encoding="utf-8"))["stocks"]
    top = sorted((s for s in u if s.get("n500")), key=lambda s: -(s.get("val_cr") or 0))[:100]
    return [s["s"] for s in top], "top 100 NIFTY 500 by value (NSE list unreachable)"


def window(t):
    m = t.hour * 60 + t.minute
    return t.weekday() < 5 and 8 * 60 + 55 <= m < 15 * 60 + 50


class Bridge:
    def __init__(self):
        self.env = dict(os.environ)
        load_keys(environ=self.env)
        self.pending, self.lock = {}, threading.Lock()
        self.feed = angel_feed.AngelFeed(self.env, self.on_row)
        self.wanted = set()
        self.tunnel = None

    def on_row(self, sym, row):
        with self.lock:
            self.pending[sym] = row

    def master(self):
        """Angel One's instrument list, downloaded at most once a day (about 40 MB; Angel One's server can be slow)."""
        import requests
        f = ROOT / "local" / "scripmaster.json"
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

    def start_feed(self):
        m = self.master()
        self.feed.http_get = lambda url, timeout=None: m
        self.feed.prepare()                                    # login + instrument list (no secret is ever printed)
        syms, src = nifty100()
        self.wanted = {s for s in syms if s in self.feed.sym_token} | {s for _, _, s in angel_feed.INDEXES}
        full = self.feed.sym_token
        self.feed.sym_token = {s: full[s] for s in self.wanted}  # subscribe only these ...
        self.feed.start()
        self.feed.sym_token = full                              # ... but keep every stock for candles and focus lookups
        log(f"Angel One live: {len(self.wanted)} instruments ({src})")

    def focus(self, syms):
        """Stocks open on the terminal that are outside the NIFTY 100: add them with one more small connection."""
        new = [s for s in syms if s in self.feed.sym_token and s not in self.wanted][:20]
        if not new or getattr(self, "extra", 0) >= 2:                  # Angel One allows 3 connections: 1 for the NIFTY 100, at most 2 for extras
            return
        self.extra = getattr(self, "extra", 0) + 1
        self.wanted |= set(new)
        groups = [{"exchangeType": angel_feed.EXCH_TYPE[self.feed.sym_token[s][0]], "tokens": [self.feed.sym_token[s][1]]} for s in new]
        threading.Thread(target=self.feed._run_conn, args=(90 + len(self.wanted) % 9, groups), daemon=True).start()
        log("focus added: " + ", ".join(new))

    def open_tunnel(self):
        if self.tunnel and self.tunnel.poll() is None:
            return
        self.tunnel = subprocess.Popen(["ssh", "-i", KEY, "-N", "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=15",
                                        "-o", "StrictHostKeyChecking=accept-new", "-L", f"{TUNNEL_PORT}:127.0.0.1:8788", f"{USER}@{SERVER}"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        time.sleep(3)

    async def session(self):
        from websockets.asyncio.client import connect
        self.open_tunnel()
        async with connect(f"ws://127.0.0.1:{TUNNEL_PORT}", max_size=2**24, ping_interval=20) as ws:
            log("connected to the server relay")

            async def pump():
                while True:
                    with self.lock:
                        batch, self.pending = self.pending, {}
                    if batch:
                        await ws.send(json.dumps({"type": "ticks", "q": batch}, separators=(",", ":")))
                    await asyncio.sleep(0.25)

            async def listen():
                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("type") == "candles_req":
                        bars = await asyncio.to_thread(self.feed.candles, m.get("sym"), m.get("iv"))
                        await ws.send(json.dumps({"type": "candles_res", "id": m.get("id"), "bars": bars}, separators=(",", ":")))
                    elif m.get("type") == "focus":
                        self.focus(m.get("syms") or [])

            async def clock():
                while window(datetime.now(IST)):
                    await asyncio.sleep(30)
                raise SystemExit("market window over")

            await asyncio.gather(pump(), listen(), clock())

    async def run(self):
        started = False
        while True:
            if not window(datetime.now(IST)):
                if started:
                    log("market window over: stopping the feed")
                    self.feed.stop()
                    if self.tunnel:
                        self.tunnel.terminate()
                    started = False
                    self.feed = angel_feed.AngelFeed(self.env, self.on_row)
                await asyncio.sleep(60)
                continue
            try:
                if not started:
                    self.start_feed()
                    started = True
                await self.session()
            except SystemExit:
                continue
            except Exception as e:  # noqa: BLE001 - reconnect forever, never print a secret
                log(f"bridge: {type(e).__name__}{(': ' + str(e)[:120]) if isinstance(e, RuntimeError) else ''}; retrying in 10 s")   # the feed's RuntimeErrors never contain a secret
                if self.tunnel:
                    self.tunnel.terminate()
                await asyncio.sleep(10)


if __name__ == "__main__":
    os.chdir(ROOT)
    asyncio.run(Bridge().run())
