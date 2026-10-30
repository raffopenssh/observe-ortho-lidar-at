"""zenodo_meta_refresh — re-PUT deposition *metadata* (title / description /
keywords) on every committed product so the whole archive reads with the
current property-free wording (``zenodo_client.landscape_metadata``,
``META_REV``) without re-uploading a single file or bumping the product
version.

Rules
-----
* **Never publishes.**  Only ``PUT /api/deposit/depositions/<id>`` with a
  ``metadata`` body is issued; ``actions/publish`` does not exist in this
  module and a deposition that already reports ``submitted: true`` is left
  untouched (recorded as ``published``).  ``access_right`` stays
  ``restricted`` (asserted).
* **Preserves** the per-product *Data Quality* line (grade, available /
  missing layers) by parsing it out of the old description; run-time-only
  context layers (building context) are dropped by
  ``landscape_metadata`` itself.
* **Does not touch** ``uploaded_at`` (progress counters) — the sweep state
  lives in ``data/austria_processor/meta_refresh.json`` (``done[key] =
  {rev, at}``); entries whose manifest record already carries
  ``meta_rev >= META_REV`` (uploaded by 2026-10-04+ code) are skipped.
* **Rate-limited**: ``run(max_entries)`` from the director's 5-min audit
  tick (default 30 → 2 requests each → ~720 req/h, well inside the shared
  Zenodo token budget).  Stops the tick on 429 / 5xx / auth errors.
* Skips entries uploaded < 1 h ago (a peer may still be writing them) and
  ``status=removed`` / ``_error`` / size-0 records.

CLI::

    python3 zenodo_meta_refresh.py status
    python3 zenodo_meta_refresh.py run 20
    python3 zenodo_meta_refresh.py show 01205   # current Zenodo metadata of one KG's files
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("zenodo_meta_refresh")

DATA_DIR = Path("data/austria_processor")
MANIFEST_PATH = DATA_DIR / "zenodo_manifest.json"
STATE_FILE = DATA_DIR / "meta_refresh.json"
KG_LIST = DATA_DIR / "kg_list.json"
ZENODO = "https://zenodo.org/api/deposit/depositions"
MIN_AGE_H = 1.0
PER_TICK = 30          # × 2 requests, every 5 min → ~720 req/h

_lock = threading.Lock()
_names: dict | None = None

_Q_RE = re.compile(r"Data Quality:</b>\s*(\d+)%\s*\(Grade\s+([A-F])\)", re.I)
_AV_RE = re.compile(r"Available layers:\s*([^.<\n]+)\.", re.I)
_MI_RE = re.compile(r"Missing layers:\s*([^.<\n]+)\.", re.I)
# legacy wording to detect in old deposition descriptions
STALE_RE = re.compile(r"cadastr(e|al) (parcel|unit)|BEV cadastre|\"cadastre\"", re.I)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def kg_names() -> dict:
    global _names
    if _names is None:
        try:
            _names = {str(k["kg_code"]): str(k.get("kg_name") or "") for k in json.loads(KG_LIST.read_text())}
        except Exception:
            _names = {}
    return _names


def parent_code(key: str) -> str:
    m = re.match(r"^(\d+)", key)
    return m.group(1) if m else ""


def product_code(key: str) -> str:
    m = re.match(r"^(\d+(?:-[a-z][-a-z0-9]*)?)_", key)
    return m.group(1) if m else key


def file_type(key: str) -> str:
    if key.endswith("_ne_h3b"):
        return "h3b"
    if "gpkg" in key:
        return "gpkg"
    return "json"


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {"rev": 0, "done": {}}


def save_state(st: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=STATE_FILE.parent, prefix=".meta_refresh_", suffix=".tmp")
    with os.fdopen(fd, "w") as fh:
        json.dump(st, fh, separators=(",", ":"))
    os.replace(tmp, STATE_FILE)


def _entries() -> dict:
    raw = json.loads(MANIFEST_PATH.read_text())
    return raw.get("entries", raw)


def pending(entries: dict | None = None, st: dict | None = None):
    """Return (todo_keys, stats, state).  A key is pending when committed,
    old enough and not already at META_REV (manifest ``meta_rev`` or state)."""
    from zenodo_client import META_REV
    entries = entries if entries is not None else _entries()
    st = st if st is not None else load_state()
    if int(st.get("rev") or 0) != META_REV:        # wording changed → start over
        st = {"rev": META_REV, "done": {}}
    done = st["done"]
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=MIN_AGE_H)).isoformat()
    todo, n_commit, n_done, n_fresh = [], 0, 0, 0
    for k, e in entries.items():
        if not isinstance(e, dict) or k.endswith("_error"):
            continue
        if int(e.get("size") or 0) <= 0 or str(e.get("status") or "") == "removed" or not e.get("depo_id"):
            continue
        n_commit += 1
        if int(e.get("meta_rev") or 0) >= META_REV:
            n_fresh += 1
            continue
        d = done.get(k)
        if d and int(d.get("rev") or 0) >= META_REV:
            n_done += 1
            continue
        if (e.get("uploaded_at") or "") > cutoff:
            continue
        todo.append(k)
    todo.sort(key=lambda k: (parent_code(k), k))
    stats = {"rev": META_REV, "committed": n_commit, "fresh_by_upload": n_fresh,
             "refreshed": n_done, "pending": len(todo)}
    return todo, stats, st


def _quality_from(desc: str):
    q = _Q_RE.search(desc or "")
    if not q:
        return 0.0, "", [], []
    av = _AV_RE.search(desc)
    mi = _MI_RE.search(desc)
    split = lambda m: [s.strip() for s in m.group(1).split(",") if s.strip()] if m else []  # noqa: E731
    return int(q.group(1)) / 100.0, q.group(2).upper(), split(av), split(mi)


def build_metadata(key: str, entry: dict, old_desc: str = "") -> dict:
    from zenodo_client import landscape_metadata
    code = product_code(key)
    name = kg_names().get(parent_code(key), "")
    qs, qg, av, mi = _quality_from(old_desc)
    md = landscape_metadata(code, name, entry.get("version") or "v1", file_type(key),
                            quality_score=qs, quality_grade=qg,
                            available_layers=av, missing_layers=mi)
    assert md["metadata"]["access_right"] == "restricted"
    assert not STALE_RE.search(re.sub(r"<[^>]+>", "", md["metadata"]["description"]))
    return md


def refresh_one(key: str, entry: dict, session, token: str, timeout: float = 30.0) -> str:
    """GET + PUT metadata for one deposition.  Verdict: ``ok`` / ``gone`` /
    ``published`` / ``retry`` (caller stops the tick) / ``error:…``."""
    depo = int(entry.get("depo_id") or 0)
    hdr = {"Authorization": f"Bearer {token}"} if token else {}
    r = session.get(f"{ZENODO}/{depo}", headers=hdr, timeout=timeout)
    if r.status_code in (404, 410):
        return "gone"
    if r.status_code in (401, 403, 429) or r.status_code >= 500:
        return "retry"
    if r.status_code != 200:
        return f"error:{r.status_code}"
    d = r.json()
    if d.get("submitted") or d.get("state") == "done":
        return "published"                   # never touch / never publish
    old_desc = (d.get("metadata") or {}).get("description") or ""
    md = build_metadata(key, entry, old_desc)
    r = session.put(f"{ZENODO}/{depo}", headers={**hdr, "Content-Type": "application/json"},
                    data=json.dumps(md), timeout=timeout)
    if r.status_code in (401, 403, 429) or r.status_code >= 500:
        return "retry"
    if r.status_code not in (200, 201):
        return f"error:{r.status_code}:{r.text[:120]}"
    return "ok"


def run(max_entries: int = PER_TICK, keys: list | None = None) -> dict:
    """One sweep step.  Serialised; safe to call from a director thread.
    ``keys`` forces specific manifest keys (operator / test)."""
    import requests
    from zenodo_client import DEFAULT_TOKEN, META_REV
    if not _lock.acquire(blocking=False):
        return {"busy": True}
    try:
        todo, stats, st = pending()
        if keys:
            todo = list(keys)
        out = {"ok": 0, "gone": 0, "published": 0, "errors": 0, "stopped": ""}
        sess = requests.Session()
        entries = _entries()
        for k in todo[:max_entries]:
            e = entries.get(k)
            if not isinstance(e, dict):
                continue
            try:
                v = refresh_one(k, e, sess, DEFAULT_TOKEN)
            except Exception as ex:  # noqa: BLE001
                log.debug("meta refresh %s: %s", k, ex)
                out["stopped"] = f"exception:{ex}"[:120]
                break
            if v == "retry":
                out["stopped"] = "zenodo backoff"
                break
            if v.startswith("error"):
                out["errors"] += 1
                log.warning("meta refresh %s: %s", k, v)
            else:
                out[v] += 1
            st["done"][k] = {"rev": META_REV, "at": _now(), "v": v}
        st["rev"] = META_REV
        st["last_run"] = _now()
        st["last_result"] = out
        save_state(st)
        out.update(stats)
        out["pending"] = max(0, stats["pending"] - out["ok"] - out["gone"] - out["published"] - out["errors"])
        return out
    finally:
        _lock.release()


def status() -> dict:
    try:
        todo, stats, st = pending()
    except Exception as ex:  # noqa: BLE001
        return {"error": str(ex)}
    stats["last_run"] = st.get("last_run")
    stats["last_result"] = st.get("last_result")
    return stats


def text_line() -> str:
    s = status()
    if "error" in s:
        return f"meta_refresh: error {s['error']}"
    lr = s.get("last_result") or {}
    tail = f" last={lr.get('ok', 0)}ok/{lr.get('gone', 0)}gone/{lr.get('errors', 0)}err"
    if lr.get("stopped"):
        tail += f" stopped={lr['stopped']}"
    eta = f" eta~{s['pending'] / (PER_TICK * 12):.1f}h" if s["pending"] else ""
    return (f"meta_refresh: rev={s['rev']} committed={s['committed']} fresh={s['fresh_by_upload']} "
            f"refreshed={s['refreshed']} pending={s['pending']}{eta} · "
            f"last_run={(s.get('last_run') or '-')[:16]}{tail}")


def show(code: str) -> list:
    """Print + return the live Zenodo metadata of every committed file of *code*."""
    import requests
    import zenodo_client as _zc
    rows = []
    for k, e in sorted(_entries().items()):
        if product_code(k) != code or not e.get("depo_id") or int(e.get("size") or 0) <= 0:
            continue
        r = requests.get(f"{ZENODO}/{e['depo_id']}", headers=_zc.auth_headers(), timeout=30)
        d = r.json() if r.status_code == 200 else {}
        md = d.get("metadata") or {}
        desc = re.sub(r"<[^>]+>", "", md.get("description") or "")
        stale = bool(STALE_RE.search(desc)) or "cadastre" in (md.get("keywords") or [])
        rows.append({"key": k, "depo": e["depo_id"], "http": r.status_code, "state": d.get("state"),
                     "submitted": d.get("submitted"), "access": md.get("access_right"),
                     "version": md.get("version"), "stale": stale})
        print(f"== {k} depo={e['depo_id']} HTTP {r.status_code} state={d.get('state')} submitted={d.get('submitted')} "
              f"access={md.get('access_right')} version={md.get('version')} STALE={stale}")
        print("   title:", md.get("title"))
        print("   keywords:", md.get("keywords"))
        print("   desc:", desc[:700].replace("\n", " | "))
    return rows


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "run":
        print(json.dumps(run(int(sys.argv[2]) if len(sys.argv) > 2 else PER_TICK), indent=1))
    elif cmd == "show":
        show(sys.argv[2])
    else:
        print(text_line())
