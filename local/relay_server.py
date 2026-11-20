"""Server relay: the owner's PC (Angel One bridge) pushes prices in, the website's terminal reads them out. No Angel One keys here.

  browser  <- ws://<server>/ws/ticks (nginx, password)  -> 127.0.0.1:8787   same messages as scripts/aladin_ticker_daemon.py / relay/relay.py:
           hello, snap, ticks; answers {"type": "candles"} by asking the PC
  PC       -> 127.0.0.1:8788 (reachable only through the PC's SSH tunnel)   sends {"type": "ticks", "q": {SYM: [ltp, o, h, l, pc, vol, ms]}}
           and answers {"type": "candles_req"} with {"type": "candles_res"}
Angel One data is licensed to the account holder: the site is password-protected and serves only the owner.
"""
import asyncio
import itertools
import json
import time

from websockets.asyncio.server import broadcast, serve

TICKS, BROWSERS = {}, set()
BRIDGE = {"ws": None, "since": None, "last": 0.0}
PENDING = {}
IDS = itertools.count(1)


def hello():
    up = BRIDGE["ws"] is not None
    return {"type": "hello", "ok": True, "src": "Angel One via the owner's PC" if up else "bridge offline (delayed prices)",
            "bridge": up, "bridge_last_tick_s": round(time.time() - BRIDGE["last"], 1) if BRIDGE["last"] else None}


async def ask_pc(sym, iv):
    ws = BRIDGE["ws"]
    if ws is None:
        return None
    rid = next(IDS)
    fut = asyncio.get_running_loop().create_future()
    PENDING[rid] = fut
    try:
        await ws.send(json.dumps({"type": "candles_req", "id": rid, "sym": sym, "iv": iv}))
        return await asyncio.wait_for(fut, 12)
    except (asyncio.TimeoutError, Exception):  # noqa: BLE001
        return None
    finally:
        PENDING.pop(rid, None)


async def browser(ws):
    BROWSERS.add(ws)
    try:
        await ws.send(json.dumps(hello()))
        await ws.send(json.dumps({"type": "snap", "q": TICKS}, separators=(",", ":")))
        async for raw in ws:
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            if m.get("type") == "candles":
                bars = await ask_pc(m.get("sym"), m.get("iv"))
                await ws.send(json.dumps({"type": "candles", "id": m.get("id"), "sym": m.get("sym"), "iv": m.get("iv"), "bars": bars}, separators=(",", ":")))
            elif m.get("type") == "focus" and BRIDGE["ws"] is not None:
                syms = [s for s in (m.get("syms") or [])[:20] if isinstance(s, str)]
                await BRIDGE["ws"].send(json.dumps({"type": "focus", "syms": syms}))
    finally:
        BROWSERS.discard(ws)


async def bridge(ws):
    if BRIDGE["ws"] is not None:
        await BRIDGE["ws"].close()
    BRIDGE.update(ws=ws, since=time.time())
    broadcast(BROWSERS, json.dumps(hello()))
    try:
        async for raw in ws:
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            if m.get("type") == "ticks" and isinstance(m.get("q"), dict):
                TICKS.update(m["q"])
                BRIDGE["last"] = time.time()
                broadcast(BROWSERS, json.dumps({"type": "ticks", "q": m["q"]}, separators=(",", ":")))
            elif m.get("type") == "candles_res":
                fut = PENDING.get(m.get("id"))
                if fut and not fut.done():
                    fut.set_result(m.get("bars"))
    finally:
        if BRIDGE["ws"] is ws:
            BRIDGE.update(ws=None)
            broadcast(BROWSERS, json.dumps(hello()))


async def dump():
    """data/live_extra/ticks.json every 5 s, so the jobs (and the intraday desk) can read the latest prices."""
    from pathlib import Path
    p = Path(__file__).resolve().parent.parent / "data/live_extra/ticks.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    while True:
        if TICKS:
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps({"generated_utc": time.time(), "src": "angel-bridge", "q": TICKS}, separators=(",", ":")), encoding="utf-8")
            tmp.replace(p)
        await asyncio.sleep(5)


async def main():
    async with serve(browser, "127.0.0.1", 8787, max_size=2**22), serve(bridge, "127.0.0.1", 8788, max_size=2**24):
        print("relay: browsers 127.0.0.1:8787, PC bridge 127.0.0.1:8788", flush=True)
        await dump()


if __name__ == "__main__":
    asyncio.run(main())
