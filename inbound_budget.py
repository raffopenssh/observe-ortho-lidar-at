"""inbound_budget — rolling-hour byte meter for the primary's metered inbound.

The primary is bandwidth-metered (AGENTS.md: keep it < 50 MB/h).  Every
bulk puller on the primary (``v2_ingest`` json_v2 blobs, ``ne_ingest`` h3b
products) asks ``allow(channel, mb_per_h, nbytes)`` before a download and
``add(channel, nbytes)`` after.  Per-channel sliding window of (ts, bytes)
samples over the last hour, persisted to a tiny JSON so a srv restart does
not reset the hour.  Single process (the ingest loop worker) writes; reads
from the other worker are fine (status lines).
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

FILE = Path("data/austria_processor/inbound_budget.json")
WINDOW_S = 3600.0
_lock = threading.Lock()
_mem: dict | None = None


def _load() -> dict:
    global _mem
    if _mem is None:
        try:
            d = json.loads(FILE.read_text())
            _mem = d if isinstance(d, dict) else {}
        except Exception:
            _mem = {}
    return _mem


def _prune(samples: list, now: float) -> list:
    cut = now - WINDOW_S
    return [s for s in samples if isinstance(s, list) and len(s) == 2 and float(s[0]) >= cut]


def used_bytes(channel: str, now: float | None = None) -> int:
    now = now or time.time()
    with _lock:
        d = _load()
        return int(sum(int(s[1]) for s in _prune(d.get(channel) or [], now)))


def used_mb(channel: str) -> float:
    return used_bytes(channel) / 1e6


def allow(channel: str, mb_per_h: float, nbytes: int) -> bool:
    """True when *nbytes* more on *channel* keeps the trailing hour ≤ budget.
    A single object larger than the whole budget is allowed once the window
    is empty (otherwise it could never be fetched)."""
    now = time.time()
    used = used_bytes(channel, now)
    cap = float(mb_per_h) * 1e6
    if used == 0:
        return True
    return used + int(nbytes) <= cap


def add(channel: str, nbytes: int) -> None:
    now = time.time()
    with _lock:
        d = _load()
        s = _prune(d.get(channel) or [], now)
        s.append([round(now, 1), int(nbytes)])
        d[channel] = s
        try:
            FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(d))
            tmp.replace(FILE)
        except Exception:
            pass


def status() -> dict:
    now = time.time()
    with _lock:
        d = _load()
        return {ch: round(sum(int(s[1]) for s in _prune(v or [], now)) / 1e6, 2) for ch, v in d.items()}
