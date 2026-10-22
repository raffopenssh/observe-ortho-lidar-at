"""product_origin — which peer uploaded (and still holds) a v2.4 product.

Written by ``POST /api/v1/manifest/push`` (the peer adds ``peer`` to the
payload), read by ``ne_ingest`` / ``v2_ingest`` to pull ``<code>_ne.h3b`` /
``<code>_v2.json.gz`` peer-first via ``GET /api/v1/processing/product``
instead of burning the ONE shared Zenodo token.  Tiny JSON map
``{key: {"peer": id, "ts": uploaded_at, "at": epoch}}``, pruned after
``TTL_DAYS`` (peers GC their copies after ``NE_KEEP_DAYS`` = 7 anyway).
fcntl-locked read-modify-write: the push handler runs on either gunicorn
worker.
"""
from __future__ import annotations

import fcntl
import json
import time
from pathlib import Path

FILE = Path("data/austria_processor/product_origin.json")
TTL_DAYS = 10


def _load_unlocked() -> dict:
    try:
        d = json.loads(FILE.read_text())
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def load() -> dict:
    return _load_unlocked()


def record(key: str, peer: str, uploaded_at: str = "") -> None:
    FILE.parent.mkdir(parents=True, exist_ok=True)
    lock = FILE.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        d = _load_unlocked()
        now = time.time()
        d[key] = {"peer": peer, "ts": uploaded_at or "", "at": now}
        cutoff = now - TTL_DAYS * 86400
        d = {k: v for k, v in d.items() if float((v or {}).get("at") or 0) >= cutoff}
        tmp = FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, sort_keys=True))
        tmp.replace(FILE)
        fcntl.flock(lf, fcntl.LOCK_UN)


def peer_for(key: str, uploaded_at: str | None = None) -> str | None:
    """Peer id that pushed *key* (only if the recorded upload stamp matches
    *uploaded_at* when given — a re-upload from another peer wins)."""
    v = _load_unlocked().get(key)
    if not v:
        return None
    if uploaded_at and v.get("ts") and v["ts"] != uploaded_at:
        return None
    return v.get("peer")
