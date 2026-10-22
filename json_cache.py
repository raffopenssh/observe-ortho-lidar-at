"""Mtime/size-keyed cache for large, frequently re-read JSON state files.

Oct 2026: profiling the director gunicorn worker showed ~40 % of its CPU
in ``json.loads`` of the same handful of multi-MB files (12 MB
``zenodo_manifest.json``, 5 MB ``director_state.json``, 4 MB
``ne_umfeld_manifest.json`` / ``kg_list.json``) re-parsed on every
heartbeat request, every status render and per-KG inside loops.  All of
those grow with fleet progress, so the primary's CPU crept up day by day.

:func:`load` parses a file at most once per (mtime_ns, size) and hands
back the SAME object to every caller.  **Treat the result as read-only.**
Callers that mutate must keep using ``json.loads`` or ``copy.deepcopy``.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

_lock = threading.Lock()
_cache: dict[str, tuple[tuple[int, int], object]] = {}
_MAX_ENTRIES = 32


def load(path, default=None):
    """Return the parsed JSON for ``path`` (shared, read-only object).

    ``default`` is returned when the file is missing or unparsable (the
    failure is not cached, so a half-written temp file is retried)."""
    p = os.fspath(path)
    try:
        st = os.stat(p)
    except OSError:
        return default
    sig = (st.st_mtime_ns, st.st_size)
    with _lock:
        hit = _cache.get(p)
    if hit is not None and hit[0] == sig:
        return hit[1]
    try:
        with open(p, 'rb') as f:
            obj = json.loads(f.read())
    except Exception:
        return default
    with _lock:
        if len(_cache) >= _MAX_ENTRIES and p not in _cache:
            _cache.pop(next(iter(_cache)))
        _cache[p] = (sig, obj)
    return obj


def invalidate(path=None) -> None:
    with _lock:
        if path is None:
            _cache.clear()
        else:
            _cache.pop(os.fspath(path), None)


def stats() -> dict:
    with _lock:
        return {os.path.basename(k): v[0][1] for k, v in _cache.items()}
