"""mem_lru — byte-bounded, thread-safe LRU for the public tier's in-process
caches (hillshade grids, landscape_query grid25 blocks, cells chunks).

Why bytes and not entry counts: the Oct 2026 outage.  ``hillshade_tiles``
capped its grid cache at 600 *entries* assuming ~65 KB each; real decoded
grid25 blocks are ~320 KB and split KGs hold several per entry, so the two
``srv_public`` workers quietly grew to 1.33 GB anon, crossed the cgroup
``MemoryHigh`` (1.2 GB) and were reclaim-throttled into D state — the public
host answered nothing for hours while every request queued in the listen
backlog.  Entry counts lie; byte budgets don't.

Every cache registers itself in ``REGISTRY`` so ``public_api`` can report
them (``/api/v1/status.caches``) and the RSS watchdog can shed them all.
"""
from __future__ import annotations

import os
import threading
from collections import OrderedDict

REGISTRY: "dict[str, ByteLRU]" = {}


def _nbytes(v) -> int:
    """Best-effort deep size: numpy arrays, bytes, dicts/lists/tuples of those."""
    nb = getattr(v, 'nbytes', None)
    if nb is not None:
        return int(nb)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return len(v)
    if isinstance(v, dict):
        return 64 + sum(_nbytes(x) for x in v.values())
    if isinstance(v, (list, tuple)):
        return 32 + sum(_nbytes(x) for x in v)
    return 64


class ByteLRU:
    def __init__(self, name: str, max_mb: float, sizer=_nbytes, env: str | None = None):
        if env:
            try:
                max_mb = float(os.environ.get(env, max_mb))
            except ValueError:
                pass
        self.name = name
        self.max_bytes = int(max_mb * 2 ** 20)
        self._sizer = sizer
        self._d: "OrderedDict[object, tuple[object, int]]" = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        REGISTRY[name] = self

    def get(self, key, default=None):
        with self._lock:
            e = self._d.get(key)
            if e is None:
                self.misses += 1
                return default
            self._d.move_to_end(key)
            self.hits += 1
            return e[0]

    def put(self, key, val):
        sz = self._sizer(val)
        with self._lock:
            old = self._d.pop(key, None)
            if old is not None:
                self._bytes -= old[1]
            self._d[key] = (val, sz)
            self._bytes += sz
            while self._bytes > self.max_bytes and len(self._d) > 1:
                _, (_, osz) = self._d.popitem(last=False)
                self._bytes -= osz
                self.evictions += 1
        return val

    def pop(self, key):
        with self._lock:
            e = self._d.pop(key, None)
            if e is not None:
                self._bytes -= e[1]

    def pop_if(self, pred):
        with self._lock:
            for k in [k for k in self._d if pred(k)]:
                self._bytes -= self._d.pop(k)[1]

    def clear(self):
        with self._lock:
            self._d.clear()
            self._bytes = 0

    def __len__(self):
        return len(self._d)

    def __contains__(self, key):
        return key in self._d

    def stats(self) -> dict:
        return {'entries': len(self._d), 'mb': round(self._bytes / 2 ** 20, 1),
                'max_mb': round(self.max_bytes / 2 ** 20), 'hits': self.hits,
                'misses': self.misses, 'evictions': self.evictions}


def stats_all() -> dict:
    return {k: v.stats() for k, v in REGISTRY.items()}


def clear_all() -> dict:
    out = {k: len(v) for k, v in REGISTRY.items()}
    for v in REGISTRY.values():
        v.clear()
    return out


def total_mb() -> float:
    return round(sum(v._bytes for v in REGISTRY.values()) / 2 ** 20, 1)
