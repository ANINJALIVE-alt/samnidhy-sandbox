"""Run one project script with the keys from the main project's .env (the only copy of the keys; never copied, never printed).

  python local/launch.py scripts/aladin_ticker_daemon.py --source angel
"""
import os
import runpy
import sys
from pathlib import Path

LOCAL = Path(__file__).resolve().parent.parent                       # SAMNIDHY_LOCAL
MAIN_ENV = next((p for p in (LOCAL / ".env", LOCAL.parent / "SAMNIDHY_EDUCATIONAL_DASHBOARD" / ".env") if p.exists()), LOCAL / ".env")   # server: its own .env (no Angel keys); PC: the main project's


def load_keys(path=MAIN_ENV, environ=os.environ):
    """KEY=VALUE lines -> environment (variables already set win). Returns the NAMES loaded."""
    names = []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return names
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and v and k not in environ:
            environ[k] = v
            names.append(k)
    if environ.get("ANGEL_PIN") and not environ.get("ANGEL_MPIN"):
        environ["ANGEL_MPIN"] = environ["ANGEL_PIN"]
    return names


if __name__ == "__main__":
    os.chdir(LOCAL)
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    load_keys()
    script = sys.argv[1]
    sys.argv = [script] + sys.argv[2:]
    sys.path.insert(0, str(LOCAL / Path(script).parent))
    runpy.run_path(str(LOCAL / script), run_name="__main__")
