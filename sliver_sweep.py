"""sliver_sweep — close the bbox slivers umfeld's KG-bbox revision 2 opened.

Background (2026-10-27, docs/sliver-sweep.md): umfeld's ``/api/v1/kgs``
bbox used to be parcel-derived and narrower than the BEV VGD KG polygon
envelope; our products were rasterised on that bbox, so the NEC1 cells in
the strip between the two envelopes were never observed (``unseen``, all
nodata).  umfeld now serves ``bbox_new = old ∪ VGD envelope + pad``
(``bbox_revision: 2``) and lists the per-KG ``slivers`` at
``GET /api/v1/kgs/bbox_changes?min_delta=``.

This module, instead of re-running whole KGs:

* **primary / director** — pulls the change list, and for every KG that is
  *complete* and whose merged ``data/ne/<kg>.h3b`` is on disk, computes the
  exact set of NEC1 cells that are unseen AND lie outside ``bbox_old``
  (``scan``).  Those cells, grouped per side (W/E/S/N) and clustered along
  the strip, become **sweep units** — small padded bboxes
  (``<parent>-sw<k>``).  A handful of cache-only workers (``sweep_peers``,
  default 4) get the pending parents at the head of their whitelist slice
  (``assign``, called from the director's cache-only orchestrator).  Peers
  POST each unit's ``.h3b`` back (``accept_result``); once every unit of a
  parent is in, ``integrate`` merges them into the parent's file
  (``ne_ingest.merge_blocks`` — per cell the best-observed source wins, so
  interior cells are untouched) and refreshes the ingest index rows so
  ``ne_ingest.confirm_umfeld`` re-POSTs the parent to umfeld ``/contrib``
  (digest mismatch → fresh retry ladder).  KGs outside BEV coverage
  (border, no LiDAR) resolve as ``nodata`` — never a failure, never
  re-dispatched.
* **peer** — ``peer_units`` turns the director-pushed ``sweep_spec.json``
  into block-like KG dicts (fixed bbox, ``_sweep``), ``peer_report`` ships
  the result.  The processor runs them like split blocks but skips full
  GPKG / JSON / Zenodo entirely (``austria_processor`` ``_sweep`` branches).

State: ``data/austria_processor/sliver_sweep.json`` (primary only),
sweep products under ``data/ne/sweeps/<unit>.h3b``.  ``/process.txt`` line
``sweep:``.  CLI: ``python3 sliver_sweep.py refresh|scan|status|plan <kg>``.
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
import time
from pathlib import Path

import numpy as np

log = logging.getLogger("sliver_sweep")

_HERE = Path(__file__).resolve().parent
DATA_DIR = _HERE / "data" / "austria_processor"
NE_DIR = _HERE / "data" / "ne"
SWEEP_DIR = NE_DIR / "sweeps"
STATE_FILE = DATA_DIR / "sliver_sweep.json"
LOCK_FILE = DATA_DIR / "sliver_sweep.lock"
PEER_SPEC_FILE = DATA_DIR / "sweep_spec.json"          # peer side: director-pushed units
PEER_MODE_MARKER = DATA_DIR / "sweep_mode"              # peer side: processing/start {sweep:true}

REFRESH_S = 6 * 3600            # bbox_changes re-fetch (conditional GET)
MIN_DELTA_DEG = 0.0002          # ~20 m ≈ one res-12 cell: anything smaller never moves a cell centroid
PAD_M = 130.0                   # unit bbox padding around the unseen cells (context + tile overlap)
CELL_R_M = 12.0                 # res-12 cell circumradius-ish
GAP_M = 1500.0                  # split a side's cells into clusters at gaps wider than this
MAX_UNITS_PER_PARENT = 8
MAX_ATTEMPTS = 3
DISPATCH_STALE_S = 8 * 3600     # no result from the peer → back to pending
SCAN_PER_TICK = 400
INTEGRATE_PER_TICK = 6
SWEEP_PEERS_DEFAULT = 4
SWEEP_PER_PEER_DEFAULT = 3

_lock = threading.Lock()


# === SECTION: state ===

def _empty() -> dict:
    return {"revision": None, "etag": None, "fetched_at": None, "min_delta": MIN_DELTA_DEG,
            "kgs": {}, "peers": {}, "stats": {"results": 0, "integrated": 0, "last": None}}


class _Locked:
    """Cross-process read-modify-write of the state file (two gunicorn workers
    + the director thread + the ne_ingest thread all touch it)."""

    def __init__(self, write: bool = True):
        self.write = write
        self.fh = None
        self.state = None

    def __enter__(self) -> dict:
        _lock.acquire()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.fh = open(LOCK_FILE, "w")
        fcntl.flock(self.fh, fcntl.LOCK_EX)
        self.state = load_state()
        return self.state

    def __exit__(self, et, ev, tb):
        try:
            if self.write and et is None:
                _save_state(self.state)
        finally:
            try:
                fcntl.flock(self.fh, fcntl.LOCK_UN); self.fh.close()
            finally:
                _lock.release()


def load_state() -> dict:
    try:
        d = json.loads(STATE_FILE.read_text())
        if isinstance(d, dict) and "kgs" in d:
            base = _empty(); base.update(d)
            return base
    except FileNotFoundError:
        pass
    except Exception as ex:  # noqa: BLE001
        log.warning("sliver_sweep: state unreadable (%s) — starting empty", ex)
    return _empty()


def _save_state(d: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, separators=(",", ":")))
    tmp.replace(STATE_FILE)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _knob(key: str, default):
    try:
        import peer_director as _pd
        return _pd.knob(None, key, default)
    except Exception:
        return default


def enabled() -> bool:
    return bool(_knob("sliver_sweep", True))


# === SECTION: umfeld change list ===

def refresh(force: bool = False) -> dict:
    """Pull ``/api/v1/kgs/bbox_changes`` (public tier, ETag-conditional) and
    register every changed KG.  New codes start in ``wait``; a KG whose
    bbox moved again (new revision) goes back to ``wait`` for a re-scan."""
    import requests
    import umfeld
    with _Locked() as st:
        age = time.time() - (st.get("fetched_ts") or 0)
        if not force and st["kgs"] and age < REFRESH_S:
            return {"skipped": "fresh", "age_s": int(age)}
        md = float(_knob("sweep_min_delta", MIN_DELTA_DEG))
        hdrs = {"Accept-Encoding": "gzip"}
        if st.get("etag") and not force:
            hdrs["If-None-Match"] = st["etag"]
        r = requests.get(f"{umfeld.PUBLIC_BASE}/kgs/bbox_changes", params={"min_delta": md},
                         headers=hdrs, timeout=120)
        st["fetched_ts"] = time.time(); st["fetched_at"] = _now()
        if r.status_code == 304:
            return {"unchanged": True, "kgs": len(st["kgs"])}
        r.raise_for_status()
        d = (r.json() or {}).get("data") or {}
        rev = int(d.get("bbox_revision") or 0)
        added = moved = 0
        for row in d.get("changed") or []:
            code = str(row.get("kg_code") or "")
            if not code:
                continue
            cur = st["kgs"].get(code)
            ent = {"rev": rev, "bbox_old": row.get("bbox_old"), "bbox_new": row.get("bbox_new"),
                   "max_delta": row.get("max_delta_deg"), "slivers": row.get("slivers") or []}
            if cur is None:
                ent.update(state="wait", attempts=0, units=[])
                st["kgs"][code] = ent; added += 1
            elif cur.get("rev") != rev or cur.get("bbox_new") != ent["bbox_new"]:
                cur.update(ent); cur.update(state="wait", units=[], attempts=0); moved += 1
        st.update(revision=rev, etag=r.headers.get("ETag"), min_delta=md,
                  changed_total=d.get("changed_total"), to_generated_at=d.get("to_generated_at"))
        log.info("sliver_sweep: bbox_changes rev %s: %d KGs (%d new, %d moved again)", rev, len(st["kgs"]), added, moved)
        return {"revision": rev, "kgs": len(st["kgs"]), "added": added, "moved": moved}


# === SECTION: scan (primary) — unseen cells outside bbox_old → units ===

def _parent_path(code: str) -> Path:
    return NE_DIR / f"{code}.h3b"


def unseen_outside(path: Path, bbox_old: list) -> tuple[np.ndarray, np.ndarray, int]:
    """``(cell_ids, latlng[n,2], cells_total)`` of the NEC1 cells in *path*
    that are unseen (all-nodata NEO1 row) and whose centroid lies outside
    ``bbox_old``.  Reads NEO1 + lu ids only (no tree/bld rows)."""
    import h3.api.numpy_int as h3n
    import ne_ingest
    r = ne_ingest._read_compact(path, want_rows=False)
    n = int(r["obs"].shape[0])
    ids = ne_ingest._lu_ids(r["lu"], n)
    seen = ne_ingest.row_observed(r["obs"])
    un = ids[~seen]
    if not len(un):
        return un, np.zeros((0, 2)), n
    ll = np.array([h3n.cell_to_latlng(int(c)) for c in un], dtype=float).reshape(-1, 2)
    w, s, e, nn = [float(x) for x in bbox_old]
    out = (ll[:, 1] < w) | (ll[:, 1] > e) | (ll[:, 0] < s) | (ll[:, 0] > nn)
    return un[out], ll[out], n


def _units_for(ll: np.ndarray, bbox_old: list, parent: str) -> list[dict]:
    """Group unseen cells by side of the old bbox, cluster along the strip at
    gaps > GAP_M, pad → unit bboxes (lon/lat)."""
    if not len(ll):
        return []
    w, s, e, n = [float(x) for x in bbox_old]
    lat0 = (s + n) / 2
    m_lat = 1 / 111_000.0
    m_lon = 1 / (111_000.0 * max(0.2, np.cos(np.radians(lat0))))
    lat, lon = ll[:, 0], ll[:, 1]
    side = np.full(len(ll), "", dtype=object)
    side[lon < w] = "W"; side[lon > e] = "E"
    rest = side == ""
    side[rest & (lat < s)] = "S"; side[rest & (lat > n)] = "N"
    units = []
    for sd in ("W", "E", "S", "N"):
        sel = side == sd
        if not sel.any():
            continue
        axis = lat[sel] if sd in ("W", "E") else lon[sel]
        step = GAP_M * (m_lat if sd in ("W", "E") else m_lon)
        order = np.argsort(axis)
        a = axis[order]
        breaks = np.nonzero(np.diff(a) > step)[0] + 1
        groups = np.split(order, breaks)
        for g in groups:
            glat, glon = lat[sel][g], lon[sel][g]
            pad_lat = (PAD_M + CELL_R_M) * m_lat
            pad_lon = (PAD_M + CELL_R_M) * m_lon
            units.append({"side": sd, "cells": int(len(g)),
                          "bbox": [round(float(glon.min() - pad_lon), 6), round(float(glat.min() - pad_lat), 6),
                                   round(float(glon.max() + pad_lon), 6), round(float(glat.max() + pad_lat), 6)]})
    # too fragmented → merge the smallest clusters into one bbox per side
    while len(units) > MAX_UNITS_PER_PARENT:
        by_side = {}
        for u in units:
            by_side.setdefault(u["side"], []).append(u)
        sd = max(by_side, key=lambda k: len(by_side[k]))
        us = by_side[sd]
        bb = [min(u["bbox"][0] for u in us), min(u["bbox"][1] for u in us),
              max(u["bbox"][2] for u in us), max(u["bbox"][3] for u in us)]
        units = [u for u in units if u["side"] != sd] + [{"side": sd, "cells": sum(u["cells"] for u in us), "bbox": bb}]
    units.sort(key=lambda u: (u["side"], u["bbox"][1], u["bbox"][0]))
    for i, u in enumerate(units, 1):
        u.update(id=f"{parent}-sw{i}", state="pending", attempts=0)
    return units


def _nec1_source(parent: str, idx: dict | None) -> str:
    try:
        import ne_ingest
        if idx is None:
            idx = ne_ingest.load_index_ro()
        src, _d = ne_ingest._declared_of(parent, idx)
        return str(src or "")
    except Exception:
        return ""


MIN_SLIVER_DEG = 2e-5     # ~2 m: thinner umfeld sliver rects cannot hold a cell centroid


def _sliver_units(k: dict, parent: str, have: list[dict]) -> list[dict]:
    """Units from umfeld's sliver rects (bbox_new − bbox_old).  Used when the
    parent's declared layer was built by bevdirect on the OLD viewport: the
    strip's cells are then absent from our file altogether (not merely
    unseen), so the precise unseen-cell scan cannot see them.  The unit's
    NEC1 build on bbox_new brings them in; the merge re-keys the parent
    onto the larger cell set."""
    w, s, e, n = [float(x) for x in k["bbox_old"]]
    lat0 = (s + n) / 2
    m_lat = 1 / 111_000.0
    m_lon = 1 / (111_000.0 * max(0.2, np.cos(np.radians(lat0))))
    pad_lat = (PAD_M + CELL_R_M) * m_lat
    pad_lon = (PAD_M + CELL_R_M) * m_lon
    out = []
    for r in k.get("slivers") or []:
        try:
            rw, rs, re_, rn = [float(x) for x in r]
        except Exception:
            continue
        if re_ - rw < MIN_SLIVER_DEG or rn - rs < MIN_SLIVER_DEG:
            continue
        cx, cy = (rw + re_) / 2, (rs + rn) / 2
        side = "W" if cx < w else "E" if cx > e else "S" if cy < s else "N"
        bb = [round(rw - pad_lon, 6), round(rs - pad_lat, 6), round(re_ + pad_lon, 6), round(rn + pad_lat, 6)]
        if any(h["bbox"][0] <= bb[0] and h["bbox"][1] <= bb[1] and h["bbox"][2] >= bb[2] and h["bbox"][3] >= bb[3]
               for h in have):
            continue   # already enclosed by a precise unit
        out.append({"side": side, "cells": None, "bbox": bb, "sliver": True})
    out.sort(key=lambda u: (u["side"], u["bbox"][1], u["bbox"][0]))
    for i, u in enumerate(out, len(have) + 1):
        u.update(id=f"{parent}-sw{i}", state="pending", attempts=0)
    return out


def _file_digest(parent: str, idx: dict | None) -> str | None:
    """NEO1 digest of the parent's h3b per the ingest index (cheap)."""
    try:
        import ne_ingest
        if idx is None:
            idx = ne_ingest.load_index_ro()
        row, kind = ne_ingest._disk_row(parent, idx)
        if row:
            return row.get("digest") if kind == "merge" else row.get("neo1_digest")
    except Exception:
        pass
    return None


def scan(max_n: int = SCAN_PER_TICK, entries: dict | None = None, idx: dict | None = None) -> dict:
    """Advance ``wait`` KGs: complete + h3b on disk → ``clean`` (no unseen
    cell outside the old bbox) or ``pending`` (units planned)."""
    import ne_ingest
    if entries is None:
        entries = ne_ingest.load_manifest_entries()
    if idx is None:
        idx = ne_ingest.load_index_ro()
    out = {"scanned": 0, "clean": 0, "pending": 0, "cells": 0, "wait": 0}
    with _Locked() as st:
        todo = [c for c, k in st["kgs"].items() if k.get("state") == "wait"]
        # round-robin fairness: oldest scan attempt first
        todo.sort(key=lambda c: (st["kgs"][c].get("scan_tried") or 0, c))
        t0 = time.time()
        for code in todo[:max_n]:
            k = st["kgs"][code]
            k["scan_tried"] = time.time()
            p = _parent_path(code)
            if not p.exists() or not ne_ingest._coverage_complete(code, entries):
                out["wait"] += 1
                continue
            try:
                un, ll, n = unseen_outside(p, k["bbox_old"])
            except Exception as ex:  # noqa: BLE001
                log.warning("sliver_sweep: scan %s failed: %s", code, ex)
                k["scan_error"] = str(ex)[:160]
                continue
            k.pop("scan_error", None)
            k.update(scanned_at=_now(), scan_digest=_file_digest(code, idx), cells_n=int(n),
                     unseen_out=int(len(un)))
            out["scanned"] += 1
            units = _units_for(ll, k["bbox_old"], code) if len(un) else []
            src = _nec1_source(code, idx)
            k["nec1_source"] = src
            if not src.startswith("index-"):
                # bevdirect (or unknown) declared layer = clipped to the old
                # viewport → strip cells are missing, not unseen: sweep the
                # umfeld sliver rects; precise units they enclose are dropped.
                sl = _sliver_units(k, code, [])
                def _encl(u):
                    return any(h["bbox"][0] <= u["bbox"][0] and h["bbox"][1] <= u["bbox"][1]
                               and h["bbox"][2] >= u["bbox"][2] and h["bbox"][3] >= u["bbox"][3] for h in sl)
                units = sl + [u for u in units if not _encl(u)]
                for i, u in enumerate(units, 1):
                    u["id"] = f"{code}-sw{i}"
            if not units:
                k.update(state="clean", units=[])
                out["clean"] += 1
            else:
                k["units"] = units
                k.update(state="pending", attempts=0)
                out["pending"] += 1; out["cells"] += int(len(un))
            if time.time() - t0 > 40:
                break
    if out["scanned"]:
        log.info("sliver_sweep: scan %s", out)
    return out


# === SECTION: dispatch (director) ===

def n_pending(st: dict | None = None) -> int:
    st = st or load_state()
    return sum(1 for k in st["kgs"].values() if k.get("state") == "pending")


def _unit_open(u: dict) -> bool:
    return u.get("state") in ("pending", "failed") and int(u.get("attempts") or 0) < MAX_ATTEMPTS


def spec_for(code: str, k: dict) -> dict:
    """What a peer needs to run the open units of *code*."""
    return {"parent": code, "bbox_new": k.get("bbox_new"), "bbox_old": k.get("bbox_old"),
            "attempt": int(k.get("attempts") or 0),
            "units": [{"id": u["id"], "bbox": u["bbox"], "cells": u.get("cells"), "side": u.get("side")}
                      for u in k.get("units") or [] if _unit_open(u) or u.get("state") == "dispatched"]}


def assign(workers: list[str], in_progress: set | None = None) -> dict:
    """Pick the sweep workers (sticky; ``sweep_peers`` of *workers*) and give
    each up to ``sweep_per_peer`` parents.  Returns ``{peer_id: {code:
    spec}}`` for the peers that carry sweep work this tick.  Also expires
    stale dispatches."""
    if not enabled() or not workers:
        return {}
    n_peers = max(0, int(_knob("sweep_peers", SWEEP_PEERS_DEFAULT)))
    per_peer = max(1, int(_knob("sweep_per_peer", SWEEP_PER_PEER_DEFAULT)))
    in_progress = set(in_progress or ())
    now = time.time()
    with _Locked() as st:
        kgs = st["kgs"]
        # expire stale dispatches
        for code, k in kgs.items():
            if k.get("state") == "dispatched" and now - float(k.get("dispatched_ts") or 0) > DISPATCH_STALE_S:
                k["attempts"] = int(k.get("attempts") or 0) + 1
                for u in k.get("units") or []:
                    if u.get("state") == "dispatched":
                        u["state"] = "pending"; u["attempts"] = int(u.get("attempts") or 0) + 1
                k["state"] = "pending" if k["attempts"] < MAX_ATTEMPTS else "failed"
                k["error"] = f"no result from {k.get('peer')} within {DISPATCH_STALE_S // 3600} h"
                log.warning("sliver_sweep: %s dispatch to %s expired (attempt %d)", code, k.get("peer"), k["attempts"])
        wset = set(workers)
        prev = [p for p in (st.get("peers") or {}) if p in wset]
        chosen = prev[:n_peers]
        for p in sorted(wset):
            if len(chosen) >= n_peers:
                break
            if p not in chosen:
                chosen.append(p)
        # release dispatches held by peers that are no longer sweep workers
        for code, k in kgs.items():
            if k.get("state") == "dispatched" and k.get("peer") not in chosen:
                k["state"] = "pending"
                for u in k.get("units") or []:
                    if u.get("state") == "dispatched":
                        u["state"] = "pending"
        pending = sorted((c for c, k in kgs.items() if k.get("state") == "pending" and c not in in_progress
                          and any(_unit_open(u) for u in k.get("units") or [])),
                         key=lambda c: (-int(kgs[c].get("priority") or 0), -int(kgs[c].get("unseen_out") or 0), c))
        out: dict = {}
        peers_state = {}
        for pid in chosen:
            mine = [c for c, k in kgs.items() if k.get("state") == "dispatched" and k.get("peer") == pid]
            while len(mine) < per_peer and pending:
                c = pending.pop(0)
                k = kgs[c]
                k.update(state="dispatched", peer=pid, dispatched_ts=now, dispatched_at=_now())
                for u in k.get("units") or []:
                    if _unit_open(u):
                        u["state"] = "dispatched"
                mine.append(c)
            if mine:
                out[pid] = {c: spec_for(c, kgs[c]) for c in mine}
            peers_state[pid] = {"codes": mine, "ts": now}
        st["peers"] = peers_state
    return out


def release(peer_id: str, reason: str = "") -> int:
    """Hand a peer's dispatched parents back to ``pending`` without counting
    an attempt (the spec never reached the peer — e.g. the peer's app is
    still on a pre-sweep commit and answers 405 on ``/processing/sweep_spec``).
    Returns the number of parents released."""
    n = 0
    with _Locked() as st:
        for code, k in st["kgs"].items():
            if k.get("state") == "dispatched" and k.get("peer") == peer_id:
                k["state"] = "pending"
                for u in k.get("units") or []:
                    if u.get("state") == "dispatched":
                        u["state"] = "pending"
                if reason:
                    k["error"] = reason
                n += 1
        peers = st.get("peers") or {}
        if peer_id in peers:
            peers[peer_id] = {"codes": [], "ts": time.time()}
    return n


# === SECTION: results (primary) ===

def accept_result(unit: str, parent: str, peer: str, attempt: int, status: str,
                  body: bytes | None, cells_observed: int | None = None, error: str | None = None,
                  seconds: float | None = None) -> dict:
    """Store a peer's unit result.  ``status`` ∈ ok | nodata | failed."""
    status = str(status or "failed")
    with _Locked() as st:
        k = st["kgs"].get(parent)
        if k is None:
            return {"error": "unknown parent", "parent": parent}
        u = next((x for x in k.get("units") or [] if x.get("id") == unit), None)
        if u is None:
            return {"error": "unknown unit", "unit": unit}
        if status == "ok":
            if not body:
                status, error = "failed", "ok without body"
            else:
                try:
                    import ne_h3
                    SWEEP_DIR.mkdir(parents=True, exist_ok=True)
                    p = SWEEP_DIR / f"{unit}.h3b"
                    tmp = p.with_suffix(".tmp")
                    tmp.write_bytes(body)
                    chk = ne_h3.verify_h3b(tmp)
                    if not chk.get("ok"):
                        raise RuntimeError("; ".join(chk.get("errors") or [])[:200])
                    tmp.replace(p)
                    u["bytes"] = len(body)
                except Exception as ex:  # noqa: BLE001
                    status, error = "failed", f"h3b rejected: {ex}"[:200]
        u.update(state=status, peer=peer, at=_now(), cells_observed=cells_observed, seconds=seconds)
        if error:
            u["error"] = str(error)[:200]
        else:
            u.pop("error", None)
        if status == "failed":
            u["attempts"] = int(u.get("attempts") or 0) + 1
        st["stats"]["results"] = int(st["stats"].get("results") or 0) + 1
        st["stats"]["last"] = {"unit": unit, "status": status, "peer": peer, "at": _now(),
                               "cells": cells_observed, "error": (error or "")[:120]}
        units = k.get("units") or []
        if any(x.get("state") in ("pending", "dispatched") for x in units):
            return {"ok": True, "unit": unit, "status": status, "parent_state": k.get("state")}
        # every unit reported
        if any(x.get("state") == "failed" and _unit_open(x) for x in units):
            k["attempts"] = int(k.get("attempts") or 0) + 1
            k["state"] = "pending" if k["attempts"] < MAX_ATTEMPTS else "received"
            k["error"] = "; ".join(x.get("error") or "" for x in units if x.get("state") == "failed")[:200]
        else:
            k["state"] = "received"
            k.pop("error", None)
        k["received_at"] = _now()
        return {"ok": True, "unit": unit, "status": status, "parent_state": k["state"]}


def sweep_paths(parent: str, st: dict | None = None) -> list[Path]:
    """Accepted sweep h3b files of *parent* (for the ingest merge)."""
    st = st or load_state()
    k = st["kgs"].get(parent) or {}
    out = []
    for u in k.get("units") or []:
        if u.get("state") == "ok":
            p = SWEEP_DIR / f"{u['id']}.h3b"
            if p.exists():
                out.append(p)
    return out


def integrate(idx: dict, entries: dict | None = None, max_n: int = INTEGRATE_PER_TICK) -> list[dict]:
    """Merge received sweeps into their parent h3b (``ne_ingest.merge_blocks``)
    and update the ingest index so umfeld gets a re-POST.  Also re-merges a
    ``done`` parent whose file was replaced since (digest drift).  Runs in
    the ne_ingest tick; *idx* is the caller's live index (saved by it)."""
    import ne_ingest
    import ne_h3
    if entries is None:
        entries = ne_ingest.load_manifest_entries()
    done = []
    with _Locked() as st:
        cands = []
        for code, k in st["kgs"].items():
            s = k.get("state")
            if s == "received":
                cands.append(code)
            elif s == "done" and k.get("integrated_digest") and _file_digest(code, idx) not in (None, k["integrated_digest"]) \
                    and sweep_paths(code, st):
                cands.append(code)
        for code in cands[:max_n]:
            k = st["kgs"][code]
            paths = sweep_paths(code, st)
            parent_p = _parent_path(code)
            if not paths:
                # nothing observed (all nodata / out of coverage) — nothing to merge
                k.update(state="done", done_at=_now(), nodata=int(k.get("unseen_out") or 0),
                         unseen_after=int(k.get("unseen_out") or 0), integrated_digest=_file_digest(code, idx))
                done.append({"parent": code, "merged": 0, "nodata": True})
                continue
            if not parent_p.exists():
                k.update(state="wait", error="parent h3b vanished")
                continue
            tmp = NE_DIR / f".{code}.sweep_merge.h3b"
            try:
                complete = ne_ingest._coverage_complete(code, entries)
                hdr = ne_ingest.merge_blocks(code, [parent_p] + paths, tmp, complete=complete)
                os.replace(tmp, parent_p)
                ne_ingest._index_cells(parent_p)
            except Exception as ex:  # noqa: BLE001
                tmp.unlink(missing_ok=True)
                log.warning("sliver_sweep: integrate %s failed: %s", code, ex)
                k["integrate_error"] = str(ex)[:200]
                k["integrate_fails"] = int(k.get("integrate_fails") or 0) + 1
                if k["integrate_fails"] >= 3:
                    k["state"] = "failed"; k["error"] = "integrate: " + k["integrate_error"]
                continue
            k.pop("integrate_error", None)
            digest = hdr["neo1"]["digest"]
            # index rows (mirror ne_ingest.merge_dirty_parents) → confirm_umfeld sees a new digest
            stp = idx["parents"].setdefault(code, {})
            stp.update(complete=complete, digest=digest, merged_at=_now(), dirty=False,
                       cells_n=hdr["cells_n"], nec1_source=(hdr.get("nec1") or {}).get("source"),
                       nec1_digest=(hdr.get("nec1") or {}).get("digest"),
                       nec1_disagree=(len(hdr.get("nec1_disagree") or []) or None),
                       cells_observed=(hdr.get("summary") or {}).get("observed", {}).get("cells_observed"),
                       swept=[p.stem for p in paths], swept_at=_now())
            stp.pop("merge_error", None)
            fp = idx["files"].get(code)
            if fp:
                fp.update(nec1_source=stp["nec1_source"], nec1_digest=stp["nec1_digest"], neo1_digest=digest,
                          cells_n=hdr["cells_n"], merged_at=stp["merged_at"])
                fp.pop("green_fix", None)
            stp.pop("green_fix", None)
            try:
                un, _ll, _n = unseen_outside(parent_p, k["bbox_old"])
                after = int(len(un))
            except Exception:
                after = None
            k.update(state="done", done_at=_now(), integrated_digest=digest, unseen_after=after,
                     nodata=after, cells_observed=stp.get("cells_observed"))
            k.pop("error", None)
            st["stats"]["integrated"] = int(st["stats"].get("integrated") or 0) + 1
            log.info("sliver_sweep: %s integrated %d unit(s): unseen outside old bbox %s → %s, digest %s",
                     code, len(paths), k.get("unseen_out"), after, digest)
            done.append({"parent": code, "merged": len(paths), "unseen_after": after})
    return done


def tick(idx: dict, entries: dict) -> dict:
    """Primary-side periodic work (called from ``ne_ingest.run_tick``)."""
    if not enabled():
        return {"disabled": True}
    out = {}
    try:
        out["refresh"] = refresh()
    except Exception as ex:  # noqa: BLE001
        log.warning("sliver_sweep: refresh failed: %s", ex)
        out["refresh"] = {"error": str(ex)[:120]}
    try:
        out["scan"] = scan(entries=entries, idx=idx)
    except Exception as ex:  # noqa: BLE001
        log.warning("sliver_sweep: scan failed: %s", ex)
    try:
        out["integrated"] = integrate(idx, entries)
    except Exception as ex:  # noqa: BLE001
        log.warning("sliver_sweep: integrate failed: %s", ex)
    return out


# === SECTION: status / process.txt ===

def status() -> dict:
    st = load_state()
    kgs = st["kgs"]
    by = {}
    for k in kgs.values():
        by[k.get("state")] = by.get(k.get("state"), 0) + 1
    pend = [k for k in kgs.values() if k.get("state") in ("pending", "dispatched")]
    units_open = sum(1 for k in pend for u in k.get("units") or [] if _unit_open(u) or u.get("state") == "dispatched")
    cells_open = sum(int(k.get("unseen_out") or 0) for k in pend)
    done = [k for k in kgs.values() if k.get("state") == "done"]
    cells_done = sum(int(k.get("unseen_out") or 0) - int(k.get("unseen_after") or 0) for k in done)
    nodata = sum(int(k.get("unseen_after") or 0) for k in done)
    now = time.time()
    day = [k for k in done if k.get("done_at") and now - _ts(k["done_at"]) < 86400]
    rate = len(day) / 24.0
    eta_h = (len(pend) / rate) if rate > 0 else None
    # umfeld confirmation of integrated parents
    confirmed = 0
    try:
        import ne_ingest
        idx = ne_ingest.load_index_ro()
        um = idx.get("umfeld") or {}
        for code, k in kgs.items():
            if k.get("state") == "done" and k.get("integrated_digest") and (um.get(code) or {}).get("confirmed_at") \
                    and str(um[code]["confirmed_at"]) >= str(k.get("done_at") or ""):
                confirmed += 1
    except Exception:
        pass
    return {"enabled": enabled(), "revision": st.get("revision"), "fetched_at": st.get("fetched_at"),
            "kgs": len(kgs), "by_state": by, "units_open": units_open, "cells_open": cells_open,
            "cells_filled": cells_done, "nodata_cells": nodata, "done_24h": len(day), "rate_per_h": round(rate, 2),
            "eta_h": round(eta_h, 1) if eta_h is not None else None, "peers": st.get("peers") or {},
            "contrib_confirmed": confirmed, "stats": st.get("stats"),
            "knobs": {"sweep_peers": _knob("sweep_peers", SWEEP_PEERS_DEFAULT),
                      "sweep_per_peer": _knob("sweep_per_peer", SWEEP_PER_PEER_DEFAULT),
                      "sweep_min_delta": _knob("sweep_min_delta", MIN_DELTA_DEG)}}


def _ts(s: str) -> float:
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
    except Exception:
        return 0.0


def _age(s: str | None) -> str:
    if not s:
        return "-"
    d = max(0, int(time.time() - _ts(s)))
    return f"{d // 3600}h{(d % 3600) // 60:02d}m" if d >= 3600 else f"{d // 60}m"


def text_lines(v: dict | None = None) -> list[str]:
    v = v or status()
    if not v.get("enabled"):
        return ["sweep:    disabled (director_config sliver_sweep=false)"]
    by = v["by_state"]
    st = load_state()
    eta = f"{v['eta_h'] / 24:.1f}d" if v.get("eta_h") else "-"
    head = (f"sweep:    bbox rev={v.get('revision')} kgs={v['kgs']} wait={by.get('wait', 0)} clean={by.get('clean', 0)} "
            f"pending={by.get('pending', 0)} dispatched={by.get('dispatched', 0)} received={by.get('received', 0)} "
            f"done={by.get('done', 0)} failed={by.get('failed', 0)} · open units={v['units_open']} cells={v['cells_open']:,} "
            f"· filled={v['cells_filled']:,} nodata={v['nodata_cells']:,} · @{v['rate_per_h']}/h eta={eta} "
            f"· contrib re-posted={v['contrib_confirmed']}/{by.get('done', 0)}")
    lines = [head]
    peers = v.get("peers") or {}
    if peers:
        parts = []
        for pid, ps in sorted(peers.items()):
            codes = ps.get("codes") or []
            frag = []
            for c in codes:
                k = st["kgs"].get(c) or {}
                us = k.get("units") or []
                n_ok = sum(1 for u in us if u.get("state") in ("ok", "nodata"))
                frag.append(f"{c}[{n_ok}/{len(us)}u {int(k.get('unseen_out') or 0)}c {_age(k.get('dispatched_at'))}]")
            parts.append(f"{pid}: " + (" ".join(frag) if frag else "idle"))
        lines.append("    peers(%d of %s): %s" % (len(peers), v["knobs"]["sweep_peers"], " · ".join(parts)))
    else:
        lines.append(f"    peers: none assigned yet (sweep_peers={v['knobs']['sweep_peers']}, rides the cache-only orchestrator)")
    last = (v.get("stats") or {}).get("last")
    if last:
        lines.append(f"    last result: {last.get('unit')} {last.get('status')} cells={last.get('cells')} {last.get('peer')} {_age(last.get('at'))} ago"
                     + (f" — {last.get('error')}" if last.get("error") else ""))
    recent = sorted((k for k in st["kgs"].values() if k.get("state") == "done" and k.get("done_at")),
                    key=lambda k: k["done_at"], reverse=True)[:6]
    if recent:
        lines.append("    recent: " + " ".join(
            f"{next(c for c, kk in st['kgs'].items() if kk is k)}({k.get('unseen_out')}→{k.get('unseen_after')} {_age(k.get('done_at'))})"
            for k in recent))
    bad = [(c, k) for c, k in st["kgs"].items() if k.get("state") == "failed"][:5]
    if bad:
        lines.append("    failed: " + " ".join(f"{c}({(k.get('error') or '')[:60]})" for c, k in bad))
    lines.append("    debug: ?q=sweep · python3 sliver_sweep.py status|plan <kg> · docs/sliver-sweep.md")
    return lines


# === SECTION: peer side ===

def peer_store_spec(specs: dict) -> int:
    """Director PUT → replace this peer's sweep spec."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = PEER_SPEC_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"at": _now(), "specs": specs or {}}))
    tmp.replace(PEER_SPEC_FILE)
    return len(specs or {})


def peer_specs() -> dict:
    try:
        return (json.loads(PEER_SPEC_FILE.read_text()) or {}).get("specs") or {}
    except Exception:
        return {}


def peer_units(codes: list[str], kg_index: dict) -> list[dict]:
    """Block-like KG dicts for the sweep units of *codes* (whitelist order)."""
    specs = peer_specs()
    out = []
    for code in codes:
        sp = specs.get(str(code))
        if not sp:
            continue
        base = kg_index.get(str(code)) or {"kg_code": str(code), "kg_name": ""}
        units = sp.get("units") or []
        bn = sp.get("bbox_new")
        for i, u in enumerate(units):
            w, s, e, n = u["bbox"]
            d = dict(base)
            d.update(kg_code=u["id"], _parent_kg_code=str(code), _block_index=i, _block_label=u["id"].split("-", 1)[1],
                     _n_blocks=len(units), bbox={"min_lon": w, "min_lat": s, "max_lon": e, "max_lat": n},
                     lat=(s + n) / 2, lon=(w + e) / 2, _sweep=True, _sweep_attempt=int(sp.get("attempt") or 0),
                     _sweep_cells=u.get("cells"), _sweep_side=u.get("side"),
                     _parent_bbox=(tuple(float(x) for x in bn) if bn else None))
            out.append(d)
    return out


def peer_report(kg: dict, result: dict | None, seconds: float | None = None) -> dict:
    """POST the unit's outcome (+ h3b) to the director.  Never raises."""
    import requests
    unit = kg["kg_code"]; parent = kg.get("_parent_kg_code") or unit.split("-")[0]
    status, err, body, cells = "failed", None, None, None
    if result is None:
        err = "no result (crash / timeout)"
    elif result.get("out_of_coverage"):
        status = "nodata"
    elif result.get("success"):
        p = (result.get("files") or {}).get("ne_h3b")
        if p and os.path.exists(p):
            body = Path(p).read_bytes(); status = "ok"
            cells = (result.get("ne") or {}).get("cells_observed")
            if not cells:
                status, body = "nodata", None
        else:
            err = "success without ne_h3b: " + str(result.get("ne_status") or result.get("step"))[:120]
    else:
        err = str(result.get("error") or result.get("step") or "failed")[:200]
        # tiles outside BEV coverage are no failure; a LiDAR gap at the border reads the same
        if result.get("lidar_failed") and "No tiles cover" in err:
            status, err = "nodata", None
    director = (os.environ.get("ZENODO_LOCK_URL", "").strip() or "http://127.0.0.1:8000").rstrip("/")
    hdrs = {"Content-Type": "application/octet-stream"}
    try:
        tok = (_HERE / "data/admin_token").read_text().strip()
        if tok:
            hdrs["X-Admin-Token"] = tok
    except Exception:
        pass
    try:
        me = json.loads((DATA_DIR / "self.json").read_text()).get("id") or ""
    except Exception:
        me = ""
    params = {"unit": unit, "parent": parent, "peer": me, "attempt": int(kg.get("_sweep_attempt") or 0),
              "status": status, "cells": cells or 0, "error": err or "", "seconds": int(seconds or 0)}
    last = None
    for i in range(4):
        try:
            r = requests.post(director + "/api/v1/director/sweep/result", params=params, data=body or b"",
                              headers=hdrs, timeout=120)
            if r.ok:
                return {"ok": True, "status": status, "http": r.status_code, "resp": (r.json() if r.content else {})}
            last = f"HTTP {r.status_code} {r.text[:120]}"
            if r.status_code in (400, 404):
                break
        except Exception as ex:  # noqa: BLE001
            last = str(ex)[:120]
        time.sleep(10 * (i + 1))
    log.warning("sliver_sweep: report %s (%s) to director failed: %s", unit, status, last)
    return {"ok": False, "status": status, "error": last}


# === SECTION: CLI ===

if __name__ == "__main__":
    import sys
    os.environ.setdefault("SRTM_APP_IMPORT_ONLY", "1")   # app.py helpers only, no director thread
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "refresh":
        print(json.dumps(refresh(force="--force" in sys.argv)))
    elif cmd == "scan":
        n = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 10 ** 6
        print(json.dumps(scan(max_n=n)))
    elif cmd == "plan":
        st = load_state()
        for c in sys.argv[2:]:
            k = st["kgs"].get(c)
            print(c, json.dumps(k, indent=1) if k else "not in change list")
    elif cmd == "integrate":
        import ne_ingest
        idx = ne_ingest._load_index()
        print(json.dumps(integrate(idx, max_n=int(sys.argv[2]) if len(sys.argv) > 2 else 3), default=str))
        ne_ingest._save_index(idx)
    elif cmd == "reset":
        with _Locked() as st:
            for c in sys.argv[2:]:
                k = st["kgs"].get(c)
                if k:
                    k.update(state="wait", attempts=0, units=[]); k.pop("error", None)
                    print(c, "→ wait")
    else:
        print("\n".join(text_lines()))
        print(json.dumps(status(), indent=1, default=str))
