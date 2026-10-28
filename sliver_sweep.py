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
RESWEEP_COOLDOWN_S = 6 * 3600   # a done parent with struck-out (failed) units gets its units re-opened after this
HOLD_RECHECK_S = 15 * 60        # a unit on hold (openEO cell not cached) is re-checked against the Zenodo index this often
HOLD_MAX_CELLS = 40             # cap on held cells handed to the director's prewarm.repair_cells plan per tick
_CACHE_CHECK_TTL_S = 10 * 60    # CopernicusTileCache / HansenTileCache instance reuse (ZIP index parse is costly)
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



def observed_elsewhere(ids: np.ndarray) -> np.ndarray:
    """Boolean mask over *ids*: the cell already carries an observed row
    somewhere in the corpus (``data/ne/*.h3b`` via the public cells index,
    ``cells_api`` — typically the neighbour KG whose old bbox covered the
    strip).  Such cells are served and posted already (both tiers dedupe by
    cell id), so sweeping them again would only re-observe known ground."""
    ids = np.asarray(ids, dtype=np.uint64)
    if not ids.size:
        return np.zeros(0, bool)
    try:
        import h3.api.numpy_int as h3n
        import cells_api
        r10 = np.unique(np.array([h3n.cell_to_parent(int(c), 10) for c in ids], dtype=np.uint64))
        chunks, _h = cells_api.chunks_for_r10(r10)
        t = cells_api.gather(chunks, ids)
        if not len(t):
            return np.zeros(ids.size, bool)
        obs = t.obs
        seen = (obs["hm"] != 255) | (obs["cover"].sum(axis=1) > 0)
        return np.isin(ids, t.ids[seen])
    except Exception as ex:  # noqa: BLE001
        log.warning("sliver_sweep: observed_elsewhere lookup failed (%s) — nothing excluded", ex)
        return np.zeros(ids.size, bool)


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


LU_PER_TICK_DEFAULT = 3   # umfeld ``lu`` fetches per scan tick (~1.2 MB each; primary is metered)


def missing_outside(path: Path, lu_ids: np.ndarray, bbox_old: list) -> tuple[np.ndarray, np.ndarray, int]:
    """Like :func:`unseen_outside`, but against umfeld's frozen ``lu`` cell
    set (``ne_h3.fetch_declared_umfeld(kg, layer="lu")``, which covers the
    whole VGD polygon): a cell counts when it is in ``lu_ids`` and NOT
    observed in *path* — i.e. unseen there OR absent altogether.  Needed for
    parents whose declared layer was built by bevdirect on the OLD viewport:
    the strip's cells are then missing from our file, not merely unseen, so
    :func:`unseen_outside` cannot see them (and sweeping umfeld's whole
    sliver rects blind — 4 full-length edge strips per parent, 94 % of them
    without a single cell — cost 5 min of tile work per empty 1.5 km tile)."""
    import h3.api.numpy_int as h3n
    import ne_ingest
    r = ne_ingest._read_compact(path, want_rows=False)
    n = int(r["obs"].shape[0])
    ids = ne_ingest._lu_ids(r["lu"], n)
    seen_ids = ids[ne_ingest.row_observed(r["obs"])]
    lu_ids = np.unique(np.asarray(lu_ids, dtype=np.uint64))
    un = np.setdiff1d(lu_ids, seen_ids, assume_unique=True)
    if not len(un):
        return un, np.zeros((0, 2)), n
    ll = np.array([h3n.cell_to_latlng(int(c)) for c in un], dtype=float).reshape(-1, 2)
    w, s, e, nn = [float(x) for x in bbox_old]
    out = (ll[:, 1] < w) | (ll[:, 1] > e) | (ll[:, 0] < s) | (ll[:, 0] > nn)
    return un[out], ll[out], n


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


def _products_cover_bbox_new(parent: str, bbox_new, tol: float = 5e-5) -> bool:
    """True when the parent's own (non-sweep) ``_json_v2`` product bboxes in
    ``kg_v2_store`` already extend to ``bbox_new`` on every side — i.e. the
    KG actually ran on the revision-2 envelope and the strip is observed.
    Split parents: the union of the block bboxes.  Unknown → False."""
    try:
        if not bbox_new:
            return False
        import kg_v2_store, kg_splitter
        codes = [c for c in (kg_v2_store.codes_for_parent(parent) or [])
                 if not kg_splitter.is_sweep_code(c)]
        boxes = [b for b in (kg_v2_store.get_bbox(c) for c in codes) if b]
        if not boxes:
            return False
        w = min(b[0] for b in boxes); s = min(b[1] for b in boxes)
        e = max(b[2] for b in boxes); n = max(b[3] for b in boxes)
        bw, bs, be, bn = [float(x) for x in bbox_new]
        return w <= bw + tol and s <= bs + tol and e >= be - tol and n >= bn - tol
    except Exception as ex:  # noqa: BLE001
        log.debug("sliver_sweep: products_cover_bbox_new %s: %s", parent, ex)
        return False


def scan(max_n: int = SCAN_PER_TICK, entries: dict | None = None, idx: dict | None = None,
         lu_budget: int | None = None) -> dict:
    """Advance ``wait`` KGs: complete + h3b on disk → ``clean`` (no unseen
    cell outside the old bbox) or ``pending`` (units planned).

    Parents with an ``index-*`` (umfeld lu) declared layer are scanned from
    the local file alone; bevdirect-declared parents need umfeld's ``lu``
    cell set (one ~1.2 MB GET each, :func:`missing_outside`) — at most
    ``lu_budget`` (knob ``sweep_lu_per_tick``) of those per call, the rest
    stay ``wait`` for the next tick."""
    import ne_ingest
    if entries is None:
        entries = ne_ingest.load_manifest_entries()
    if idx is None:
        idx = ne_ingest.load_index_ro()
    if lu_budget is None:
        lu_budget = max(0, int(_knob("sweep_lu_per_tick", LU_PER_TICK_DEFAULT)))
    out = {"scanned": 0, "clean": 0, "pending": 0, "cells": 0, "wait": 0, "lu_fetched": 0, "lu_deferred": 0}
    with _Locked() as st:
        todo = [c for c, k in st["kgs"].items() if k.get("state") == "wait"]
        # operator ``priority`` first (same knob the dispatcher honours —
        # otherwise a bumped parent still queues behind thousands of
        # never-scanned ones), then round-robin fairness: oldest scan
        # attempt first
        todo.sort(key=lambda c: (-int(st["kgs"][c].get("priority") or 0),
                                 st["kgs"][c].get("scan_tried") or 0, c))
        t0 = time.time()
        n_done = 0
        for code in todo:
            # time cap scales with the lu budget (~3 s per umfeld ``lu``
            # GET): at 27/tick the old flat 40 s cut the scan short.
            if n_done >= max_n or time.time() - t0 > max(40, 5 * lu_budget):
                break
            k = st["kgs"][code]
            p = _parent_path(code)
            if not p.exists() or not ne_ingest._coverage_complete(code, entries):
                k["scan_tried"] = time.time()
                n_done += 1
                out["wait"] += 1
                continue
            # Products already rasterised on the new envelope (a KG run on
            # bbox_r2, e.g. a post-revision re-run) have no strip to sweep:
            # clean, without spending an lu fetch or planning units.
            if _products_cover_bbox_new(code, k.get("bbox_new")):
                k["scan_tried"] = time.time()
                n_done += 1
                k.update(state="clean", units=[], scanned_at=_now(), unseen_out=0,
                         scan_digest=_file_digest(code, idx), clean_reason="products on bbox_new")
                out["scanned"] += 1; out["clean"] += 1
                continue
            src = _nec1_source(code, idx)
            k["nec1_source"] = src
            need_lu = not src.startswith("index-")
            if need_lu and out["lu_fetched"] >= lu_budget:
                # over budget this tick: leave scan_tried alone so the parent
                # keeps its place at the head of the queue
                out["lu_deferred"] += 1
                continue
            k["scan_tried"] = time.time()
            n_done += 1
            try:
                if need_lu:
                    # bevdirect (or unknown) declared layer = clipped to the
                    # old viewport → strip cells are missing, not unseen:
                    # diff against umfeld's lu (whole VGD polygon) instead.
                    import ne_h3
                    sec = ne_h3.fetch_declared_umfeld(code, layer="lu")
                    out["lu_fetched"] += 1
                    k["lu_cells_n"] = int(sec.header.get("cells_n") or 0)
                    k["lu_bytes"] = int(len(sec.raw))
                    un, ll, n = missing_outside(p, sec.cell_ids(), k["bbox_old"])
                else:
                    un, ll, n = unseen_outside(p, k["bbox_old"])
            except Exception as ex:  # noqa: BLE001
                log.warning("sliver_sweep: scan %s failed: %s", code, ex)
                k["scan_error"] = str(ex)[:160]
                continue
            k.pop("scan_error", None)
            # cells a neighbour's product already observed are no hole
            elsewhere = observed_elsewhere(un) if len(un) else np.zeros(0, bool)
            k["seen_elsewhere"] = int(elsewhere.sum())
            un, ll = un[~elsewhere], ll[~elsewhere]
            k.update(scanned_at=_now(), scan_digest=_file_digest(code, idx), cells_n=int(n),
                     unseen_out=int(len(un)))
            out["scanned"] += 1
            units = _units_for(ll, k["bbox_old"], code) if len(un) else []
            if not units:
                k.update(state="clean", units=[])
                out["clean"] += 1
            else:
                k["units"] = units
                k.update(state="pending", attempts=0)
                out["pending"] += 1; out["cells"] += int(len(un))
    if out["scanned"]:
        log.info("sliver_sweep: scan %s", out)
    return out


# === SECTION: dispatch (director) ===

def n_pending(st: dict | None = None) -> int:
    st = st or load_state()
    return sum(1 for k in st["kgs"].values() if k.get("state") == "pending")


def _unit_open(u: dict) -> bool:
    return u.get("state") in ("pending", "failed") and int(u.get("attempts") or 0) < MAX_ATTEMPTS


_cache_chk: dict = {"ts": 0.0, "cop": None, "hansen": None}


def _missing_cells(bbox: list | tuple, year: int = 2024) -> list[dict]:
    """``[{w,s,e,n,products:[…]}]`` of *bbox* not yet in the Zenodo tile
    store — same check the director uses for the cache-ready whitelist
    (``has_cached(local_ok=False)``) and the same shape ``prewarm.repair_cells``
    ships to the frontiers (``product_repair.missing_cells``).  Empty list
    = every cell cached, i.e. a peer's "not cached" was a transient Zenodo
    fetch error."""
    from tile_cache import CopernicusTileCache, HansenTileCache
    now = time.time()
    if _cache_chk["cop"] is None or now - _cache_chk["ts"] > _CACHE_CHECK_TTL_S:
        _cache_chk.update(ts=now, cop=CopernicusTileCache(), hansen=HansenTileCache())
    cop, hansen = _cache_chk["cop"], _cache_chk["hansen"]
    w, s, e, n = (float(x) for x in bbox)
    bb = {"west": w, "south": s, "east": e, "north": n}
    out = []
    for cw, cs, ce, cn in cop._iter_cells(bb):
        cell = {"west": cw, "south": cs, "east": ce, "north": cn}
        miss = []
        for p, kw in (("ndvi", {"ndvi": True}), ("worldcover", {"landcover": True}),
                      ("sar", {"sar": True}), ("harmonics", {"harmonics": True})):
            full = {"ndvi": False, "landcover": False, "sar": False, "harmonics": False}
            full.update(kw)
            try:
                ok = cop.has_cached(cell, year=year, local_ok=False, **full)
            except Exception:
                ok = False
            if not ok:
                miss.append(p)
        try:
            if not hansen.has_cached((cw, cs, ce, cn), local_ok=False):
                miss.append("hansen")
        except Exception:
            miss.append("hansen")
        if miss:
            out.append({"w": cw, "s": cs, "e": ce, "n": cn, "products": miss})
    return out


def _is_cache_miss(error: str | None) -> bool:
    e = str(error or "")
    return "not cached" in e or "cache_miss" in e or "cache-only" in e


def held_cells(limit: int = HOLD_MAX_CELLS) -> list[dict]:
    """Cells the held sweep units wait for — merged by the director into
    ``prewarm.repair_cells`` so a frontier fetches them (the same path a
    `-partial` product repair rides; ``docs/product-repair.md``)."""
    st = load_state()
    out: dict[tuple, dict] = {}
    for k in st["kgs"].values():
        for u in k.get("units") or []:
            if u.get("state") != "hold":
                continue
            for c in u.get("hold_cells") or []:
                key = (c["w"], c["s"])
                cur = out.setdefault(key, dict(c, products=[]))
                cur["products"] = sorted(set(cur["products"]) | set(c.get("products") or []))
                if len(out) >= limit:
                    return list(out.values())
    return list(out.values())


def spec_for(code: str, k: dict) -> dict:
    """What a peer needs to run the open units of *code*."""
    # ``attempt`` is the peer's mid-run injection gate (monotonic per parent:
    # a peer injects a spec only when attempt > the one it already ran) —
    # a products re-run (``rerun_missing_products``) bumps ``rerun`` so the
    # reset attempt counter still reads as new.
    return {"parent": code, "bbox_new": k.get("bbox_new"), "bbox_old": k.get("bbox_old"),
            "attempt": int(k.get("attempts") or 0) + 10 * int(k.get("rerun") or 0),
            "units": [{"id": u["id"], "bbox": u["bbox"], "cells": u.get("cells"), "side": u.get("side")}
                      for u in k.get("units") or [] if _unit_open(u) or u.get("state") == "dispatched"]}


def assign(workers: list[str], in_progress: set | None = None,
           n_peers: int | None = None, prefer: list[str] | None = None,
           busy: set | None = None) -> dict:
    """Pick the sweep workers (sticky; ``sweep_peers`` of *workers*, or
    ``n_peers`` when the director auto-scales) and give each up to
    ``sweep_per_peer`` parents.  ``prefer`` orders the candidates for newly
    added sweeper slots (sticky previous sweepers always come first).
    Returns ``{peer_id: {code: spec}}`` for the peers that carry sweep work
    this tick.  Also expires stale dispatches.

    ``busy`` = sweep workers currently inside a KG / unit.  Pending parents
    are handed to **idle** workers first (least-loaded first) so a parent
    bumped by the operator (``priority``) doesn't queue behind another
    peer's hour-long stalled upload (63308 on at209, 2026-10-28: dispatched
    10:39Z, started 11:30Z while 7 sweepers sat idle)."""
    if not enabled() or not workers:
        return {}
    if n_peers is None:
        n_peers = int(_knob("sweep_peers", SWEEP_PEERS_DEFAULT))
    n_peers = max(0, int(n_peers))
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
        for p in list(prefer or []) + sorted(wset):
            if p not in wset:
                continue
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
        busy = set(busy or ())
        held = {pid: [c for c, k in kgs.items() if k.get("state") == "dispatched" and k.get("peer") == pid]
                for pid in chosen}
        # fill order: idle peers first, then fewest held parents, then sticky order
        fill_order = sorted(chosen, key=lambda pid: (pid in busy, len(held[pid]), chosen.index(pid)))
        for pid in fill_order:
            mine = held[pid]
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
                  seconds: float | None = None, products: str | None = None) -> dict:
    """Store a peer's unit result.  ``status`` ∈ ok | nodata | failed.

    ``products`` = outcome of the unit's add-on products (``<unit>_light_gpkg_v2``
    + ``_json_v2`` on Zenodo): ``ok``, ``upload: …`` (transient — the unit is
    re-dispatched while it has attempts left; the h3b is kept so NE cells
    never wait on Zenodo), ``verify: …`` (deterministic — accepted, logged)."""
    status = str(status or "failed")
    products = str(products or "")
    with _Locked() as st:
        k = st["kgs"].get(parent)
        if k is None:
            return {"error": "unknown parent", "parent": parent}
        u = next((x for x in k.get("units") or [] if x.get("id") == unit), None)
        if u is None:
            # The parent was re-planned (``reset`` / bbox moved again) while
            # the peer was still mid-unit on the old plan: discard quietly —
            # the re-scan dispatches fresh units; a 404 would only make the
            # peer log a FAILED report for work nobody is waiting for.
            st["stats"]["stale"] = int(st["stats"].get("stale") or 0) + 1
            return {"ok": True, "unit": unit, "status": status, "stale": True,
                    "parent_state": k.get("state")}
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
        if status == "ok" and products.startswith("upload:") \
                and int(u.get("attempts") or 0) + 1 < MAX_ATTEMPTS:
            # h3b accepted (file kept); products missing → one more try
            status, error = "failed", products
        u.update(state=status, peer=peer, at=_now(), cells_observed=cells_observed, seconds=seconds)
        if products:
            u["products"] = products[:200]
        else:
            u.pop("products", None)
        if error:
            u["error"] = str(error)[:200]
        else:
            u.pop("error", None)
        if status == "failed" and _is_cache_miss(error):
            # Same as a cache-only KG hitting a miss: no strike, the unit
            # waits for the frontier to fill the cell (hold → repair_cells
            # → _reopen_unswept).  No missing cell on the Zenodo index =
            # transient Zenodo fetch error → plain retry after HOLD_RECHECK_S.
            try:
                miss = _missing_cells(u["bbox"])
            except Exception as ex:  # noqa: BLE001
                miss = [{"error": str(ex)[:80]}]
            status = "hold"
            u.update(hold_cells=miss, hold_at=time.time(), misses=int(u.get("misses") or 0) + 1)
            u["state"] = "hold"
            st["stats"]["cache_miss"] = int(st["stats"].get("cache_miss") or 0) + 1
            log.info("sliver_sweep: %s on hold (cache miss, %d cell(s) uncached on Zenodo): %s",
                     unit, len(miss), (error or "")[:100])
        elif status == "failed":
            u["attempts"] = int(u.get("attempts") or 0) + 1
        st["stats"]["results"] = int(st["stats"].get("results") or 0) + 1
        st["stats"]["last"] = {"unit": unit, "status": status, "peer": peer, "at": _now(),
                               "cells": cells_observed, "error": (error or "")[:120],
                               "products": products[:80]}
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


def rerun_missing_products(codes: list[str] | None = None, st_locked: dict | None = None) -> list[str]:
    """Put ``ok`` units WITHOUT add-on products (``products != "ok"``: units
    swept before the products existed, or verify-dropped) back to
    ``pending`` and their parent to ``pending`` — the h3b merge is
    idempotent, so a re-run only costs the unit's segmentation.  Codes =
    None → every such parent.  Returns the parents touched."""
    def _do(st):
        out = []
        for c, k in st["kgs"].items():
            if codes is not None and c not in codes:
                continue
            if k.get("state") not in ("done", "received", "pending", "dispatched", "failed"):
                continue
            hit = False
            for u in k.get("units") or []:
                if u.get("state") == "ok" and str(u.get("products") or "") != "ok":
                    u.update(state="pending", attempts=0); u.pop("error", None); u.pop("products", None)
                    hit = True
            if hit:
                k.update(state="pending", attempts=0, rerun=int(k.get("rerun") or 0) + 1); k.pop("error", None)
                k.pop("dispatched_ts", None); k.pop("peer", None)
                out.append(c)
        return out
    if st_locked is not None:
        return _do(st_locked)
    with _Locked() as st:
        return _do(st)


RESTORE_PER_CALL = 2     # Zenodo-archive restores per sweep_paths() call (rare: disk loss)


def sweep_paths(parent: str, st: dict | None = None, entries: dict | None = None) -> list[Path]:
    """Accepted sweep h3b files of *parent* (for the ingest merge).

    Authoritative copy = ``data/ne/sweeps/<unit>.h3b`` (``accept_result``).
    If an ``ok`` unit's file is gone but the manifest has the unit's
    ``<unit>_ne_h3b`` archive (uploaded by the peer next to its light_v2 /
    json_v2 add-ons), restore it from there via ``ne_ingest.pull`` (peer
    first, Zenodo fallback; ≤ ``RESTORE_PER_CALL`` per call, verified before
    use).  Never happens on the happy path — the primary never *pulls*
    sweep h3bs otherwise."""
    st = st or load_state()
    k = st["kgs"].get(parent) or {}
    out = []
    restores = 0
    for u in k.get("units") or []:
        if u.get("state") != "ok":
            continue
        p = SWEEP_DIR / f"{u['id']}.h3b"
        if not p.exists() and restores < RESTORE_PER_CALL:
            restores += 1
            if _restore_from_archive(u["id"], p, entries):
                u["restored_at"] = _now()
        if p.exists():
            out.append(p)
    return out


def _restore_from_archive(unit: str, dest: Path, entries: dict | None = None) -> bool:
    """``<unit>_ne_h3b`` (manifest) → *dest*; False when absent / failed."""
    try:
        import ne_ingest
        import ne_h3
        if entries is None:
            entries = ne_ingest.load_manifest_entries()
        e = entries.get(f"{unit}_ne_h3b")
        if not ne_ingest._committed(e):
            return False
        blob, src, _peer = ne_ingest.pull(unit, e)
        import inbound_budget
        inbound_budget.add("sweep", len(blob))
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".tmp")
        tmp.write_bytes(blob)
        chk = ne_h3.verify_h3b(tmp)
        if not chk.get("ok"):
            tmp.unlink(missing_ok=True)
            log.warning("sliver_sweep: archived %s_ne_h3b failed verify: %s", unit,
                        "; ".join(chk.get("errors") or [])[:160])
            return False
        tmp.replace(dest)
        log.info("sliver_sweep: restored %s.h3b from %s archive (%.2f MB)", unit, src, len(blob) / 1e6)
        return True
    except Exception as ex:  # noqa: BLE001
        log.info("sliver_sweep: restore %s from archive failed: %s", unit, str(ex)[:160])
        return False


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
                    and sweep_paths(code, st, entries):
                cands.append(code)
        for code in cands[:max_n]:
            k = st["kgs"][code]
            paths = sweep_paths(code, st, entries)
            parent_p = _parent_path(code)
            if not paths:
                # nothing observed (all nodata / out of coverage / every unit on hold) — nothing to merge
                unswept = sum(int(u.get("cells") or 0) for u in (k.get("units") or []) if u.get("state") in ("failed", "hold"))
                uo = int(k.get("unseen_out") or 0)
                k.update(state="done", done_at=_now(), nodata=max(0, uo - min(uo, unswept)), unswept=unswept,
                         unseen_after=uo, integrated_digest=_file_digest(code, idx))
                if unswept:
                    k["resweep_after"] = time.time() + RESWEEP_COOLDOWN_S
                done.append({"parent": code, "merged": 0, "nodata": not unswept, "unswept": unswept})
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
            # cells of units that struck out are *unswept*, not nodata — they
            # are re-opened after RESWEEP_COOLDOWN_S (see _reopen_unswept)
            unswept = sum(int(u.get("cells") or 0) for u in (k.get("units") or []) if u.get("state") in ("failed", "hold"))
            nodata = None if after is None else max(0, after - min(after, unswept))
            k.update(state="done", done_at=_now(), integrated_digest=digest, unseen_after=after,
                     nodata=nodata, unswept=unswept or 0, cells_observed=stp.get("cells_observed"))
            if unswept:
                k["resweep_after"] = time.time() + RESWEEP_COOLDOWN_S
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
    try:
        out["reopened"] = _reopen_unswept()
    except Exception as ex:  # noqa: BLE001
        log.warning("sliver_sweep: reopen failed: %s", ex)
    return out


def _reopen_unswept() -> list[str]:
    """A ``done`` parent whose units struck out (3× cache-miss / crash) kept
    its strip unswept.  After ``RESWEEP_COOLDOWN_S`` (the frontier has had
    time to fill the missing openEO cells) those units go back to
    ``pending`` with a fresh attempt budget; the h3b merge is idempotent."""
    now = time.time()
    out = []
    with _Locked() as st:
        for code, k in st["kgs"].items():
            if k.get("state") != "done" or not k.get("unswept"):
                continue
            hit = False
            for u in k.get("units") or []:
                if u.get("state") == "hold":
                    # like the director's cache-ready whitelist: back in once the Zenodo index has the cells
                    if now - float(u.get("hold_at") or 0) < HOLD_RECHECK_S:
                        continue
                    try:
                        miss = _missing_cells(u["bbox"])
                    except Exception as ex:  # noqa: BLE001
                        log.debug("sliver_sweep: hold recheck %s: %s", u.get("id"), ex)
                        u["hold_at"] = now
                        continue
                    if miss:
                        u.update(hold_cells=miss, hold_at=now)
                        continue
                    if int(u.get("misses") or 0) >= 2 * MAX_ATTEMPTS:
                        # cells cached but the peer keeps missing them → count it as a real attempt
                        u["attempts"] = int(u.get("attempts") or 0) + 1; u["misses"] = 0
                    u["state"] = "pending"; u.pop("hold_cells", None); u.pop("error", None)
                    hit = True
                elif u.get("state") == "failed" and float(k.get("resweep_after") or 0) <= now:
                    u.update(state="pending", attempts=0); u.pop("error", None)
                    hit = True
            if hit:
                k.update(state="pending", attempts=0, resweeps=int(k.get("resweeps") or 0) + 1)
                k.pop("resweep_after", None); k.pop("error", None)
                out.append(code)
                log.info("sliver_sweep: %s re-opened %d unswept cells (resweep #%d)", code, k["unswept"], k["resweeps"])
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
    nodata = sum(int(k.get("nodata") or 0) for k in done)
    unswept = sum(int(k.get("unswept") or 0) for k in done)
    hold_units = sum(1 for k in kgs.values() for u in k.get("units") or [] if u.get("state") == "hold")
    hold_cells = len(held_cells(10 ** 6))
    now = time.time()
    day = [k for k in done if k.get("done_at") and now - _ts(k["done_at"]) < 86400]
    rate = len(day) / 24.0
    # Scan backlog (2026-10-28): ``wait`` parents whose h3b is already on
    # disk only move once ``scan`` spends an umfeld ``lu`` fetch on them
    # (``sweep_lu_per_tick``, ~30/h) — with the sweepers mostly idle this
    # upstream step, not the fleet, bounds the eta.  Count it explicitly so
    # ``eta=0.0d`` with 3 000 parents still queued cannot happen again.
    scan_backlog = sum(1 for c, k in kgs.items() if k.get("state") == "wait" and _parent_path(c).exists())
    # rate = lu fetches/24h (the index-sourced parents scan for free and
    # are long gone; what is left in the backlog is bevdirect-sourced).
    scanned_day = sum(1 for k in kgs.values() if k.get("state") != "wait" and k.get("lu_cells_n")
                      and k.get("scanned_at") and now - _ts(k["scanned_at"]) < 86400)
    scan_rate = scanned_day / 24.0
    eta_h = None
    if rate > 0:
        eta_h = len(pend) / rate
        if scan_backlog:
            eta_h += (scan_backlog / scan_rate) if scan_rate > 0 else 0.0
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
    seen_elsewhere = sum(int(k.get("seen_elsewhere") or 0) for k in kgs.values())
    prod = {"ok": 0, "missing": 0, "unknown": 0}
    for k in kgs.values():
        for u in k.get("units") or []:
            if u.get("state") != "ok":
                continue
            pr = str(u.get("products") or "")
            prod["ok" if pr == "ok" else ("unknown" if pr in ("", "unknown") else "missing")] += 1
    lu_fetched = sum(1 for k in kgs.values() if k.get("lu_cells_n"))
    return {"enabled": enabled(), "revision": st.get("revision"), "fetched_at": st.get("fetched_at"),
            "kgs": len(kgs), "by_state": by, "seen_elsewhere": seen_elsewhere, "lu_fetched": lu_fetched, "units_open": units_open, "cells_open": cells_open,
            "cells_filled": cells_done, "nodata_cells": nodata, "unswept_cells": unswept,
            "unswept_parents": sum(1 for k in done if k.get("unswept")),
            "hold_units": hold_units, "hold_cells": hold_cells, "cache_miss_total": int((st.get("stats") or {}).get("cache_miss") or 0),
            "done_24h": len(day), "rate_per_h": round(rate, 2),
            "scan_backlog": scan_backlog, "scan_rate_per_h": round(scan_rate, 2),
            "eta_h": round(eta_h, 1) if eta_h is not None else None, "peers": st.get("peers") or {},
            "contrib_confirmed": confirmed, "stats": st.get("stats"), "products": prod,
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
            f"· filled={v['cells_filled']:,} nodata={v['nodata_cells']:,}"
            + (f" unswept={v['unswept_cells']:,}({v['unswept_parents']}p)" if v.get("unswept_cells") else "")
            + (f" hold={v['hold_units']}u/{v['hold_cells']}cells→repair_cells" if v.get("hold_units") else "")
            + f" · @{v['rate_per_h']}/h eta={eta}"
            + (f" (scan backlog={v['scan_backlog']:,}p @{v['scan_rate_per_h']}/h)" if v.get("scan_backlog") else "")
            + " "
            f"· contrib re-posted={v['contrib_confirmed']}/{by.get('done', 0)}")
    pr = v.get("products") or {}
    lines = [head,
             f"    scan: umfeld lu fetched={v.get('lu_fetched', 0)} (bevdirect parents, {_knob('sweep_lu_per_tick', LU_PER_TICK_DEFAULT)}/tick) "
             f"· cells skipped as observed by a neighbour={v.get('seen_elsewhere', 0):,} "
             f"· add-on products (light_v2+json_v2): ok={pr.get('ok', 0)} missing={pr.get('missing', 0)} pre-products={pr.get('unknown', 0)}"]
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
                n_h = sum(1 for u in us if u.get("state") == "hold")
                frag.append(f"{c}[{n_ok}/{len(us)}u{' hold' + str(n_h) if n_h else ''} {int(k.get('unseen_out') or 0)}c {_age(k.get('dispatched_at'))}]")
            parts.append(f"{pid}: " + (" ".join(frag) if frag else "idle"))
        lines.append("    peers(%d, floor %s, auto-scale to idle v2 capacity): %s" % (len(peers), v["knobs"]["sweep_peers"], " · ".join(parts)))
    else:
        lines.append(f"    peers: none assigned yet (sweep_peers={v['knobs']['sweep_peers']}, rides the cache-only orchestrator)")
    last = (v.get("stats") or {}).get("last")
    if last:
        lines.append(f"    last result: {last.get('unit')} {last.get('status')} cells={last.get('cells')} products={last.get('products') or '-'} {last.get('peer')} {_age(last.get('at'))} ago"
                     + (f" — {last.get('error')}" if last.get("error") else ""))
    recent = sorted((k for k in st["kgs"].values() if k.get("state") == "done" and k.get("done_at")),
                    key=lambda k: k["done_at"], reverse=True)[:6]
    if recent:
        lines.append("    recent: " + " ".join(
            f"{next(c for c, kk in st['kgs'].items() if kk is k)}({k.get('unseen_out')}→{k.get('unseen_after')}"
            f"{'!' + str(k.get('unswept')) + 'unswept' if k.get('unswept') else ''} {_age(k.get('done_at'))})"
            for k in recent))
    bad = [(c, k) for c, k in st["kgs"].items() if k.get("state") == "failed"][:5]
    if bad:
        lines.append("    failed: " + " ".join(f"{c}({(k.get('error') or '')[:60]})" for c, k in bad))
    lines.append("    debug: ?sweep=1 (per-parent umfeld round-trip) · ?q=sweep · python3 sliver_sweep.py status|plan <kg> · docs/sliver-sweep.md")
    return lines


def detail_lines(limit: int = 30) -> list[str]:
    """``/process.txt?sweep=1`` — one row per swept parent (done first, then
    received / dispatched / pending-with-attempts), answering "did the
    sweep land AND did umfeld get the merged cells?" without reading the
    state file.  Columns: parent, unseen→after (nodata / unswept), units
    ok/total, umfeld round-trip (``confirmed`` = umfeld holds our post-sweep
    digest; ``posted`` = sent, awaiting manifest; ``queued`` = re-POST
    pending in the ne_ingest ladder; ``parked`` = needs_bevdirect / day cap /
    strike-out), add-on products."""
    import ne_ingest
    st = load_state()
    try:
        idx = ne_ingest.load_index_ro()
    except Exception:
        idx = {}
    um = idx.get("umfeld") or {}
    parents = idx.get("parents") or {}
    order = {"done": 0, "received": 1, "dispatched": 2, "pending": 3, "failed": 4}
    rows = [(c, k) for c, k in st["kgs"].items()
            if k.get("state") in order and (k.get("units") or k.get("state") in ("done", "failed"))]
    rows.sort(key=lambda t: (order[t[1]["state"]], -(_ts(t[1].get("done_at") or t[1].get("received_at")
                                                        or t[1].get("dispatched_at") or ""))))
    n_conf = n_wait = 0
    out = []
    for c, k in rows:
        us = k.get("units") or []
        n_ok = sum(1 for u in us if u.get("state") in ("ok", "nodata"))
        n_fail = sum(1 for u in us if u.get("state") == "failed")
        n_hold = sum(1 for u in us if u.get("state") == "hold")
        hold_s = ""
        if n_hold:
            cells = sorted({(c["w"], c["s"]) for u in us if u.get("state") == "hold" for c in (u.get("hold_cells") or []) if "w" in c})
            hold_s = f" hold={n_hold}(" + ("transient, retry" if not cells else "waiting cells " + " ".join(f"{w:g}/{s_:g}" for w, s_ in cells[:4])) + ")"
        prod_ok = sum(1 for u in us if u.get("state") == "ok" and str(u.get("products") or "") == "ok")
        prod_n = sum(1 for u in us if u.get("state") == "ok")
        if k["state"] == "done":
            cu = um.get(c) or {}
            want = None
            try:
                want = ne_ingest._our_digest(c, idx)
            except Exception:
                pass
            swept_at = (parents.get(c) or {}).get("swept_at") or k.get("done_at") or ""
            if want and cu.get("confirmed_digest") == want and str(cu.get("confirmed_at") or "") >= str(swept_at):
                rt = f"umfeld=confirmed {_age(cu.get('confirmed_at'))} ago"; n_conf += 1
            elif want and cu.get("posted_digest") == want:
                rt = f"umfeld=posted {_age(cu.get('posted_at'))} ago (awaiting manifest)"; n_wait += 1
            elif cu.get("needs_bevdirect"):
                rt = "umfeld=parked(needs_bevdirect)"; n_wait += 1
            elif int(cu.get("post_attempts") or 0) >= ne_ingest.POST_MAX_ATTEMPTS:
                rt = f"umfeld=struck-out({cu.get('post_attempts')} attempts)"; n_wait += 1
            else:
                rt = "umfeld=queued (re-POST pending, sweep-priority)"; n_wait += 1
            tail = f"{k.get('unseen_out')}→{k.get('unseen_after')} nodata={k.get('nodata') or 0}"
            if k.get("unswept"):
                ra = float(k.get("resweep_after") or 0)
                tail += f" UNSWEPT={k['unswept']}" + (f" (struck-out, re-open in {max(0, int(ra - time.time())) // 60}m)" if n_fail else "")
            out.append(f"    {c:<16} done {_age(k.get('done_at'))} ago · {tail} · units {n_ok}/{len(us)}"
                       f"{' fail=' + str(n_fail) if n_fail else ''}{hold_s} · {rt} · products {prod_ok}/{prod_n}")
        else:
            err = (k.get("error") or "")[:70]
            fails = "; ".join(f"{u['id']}:{(u.get('error') or '')[:50]}" for u in us if u.get("state") == "failed")[:160]
            out.append(f"    {c:<16} {k['state']}{'@' + k['peer'] if k.get('peer') else ''} "
                       f"{_age(k.get('dispatched_at') or k.get('received_at'))} · unseen={k.get('unseen_out')} "
                       f"· units {n_ok}/{len(us)}{' fail=' + str(n_fail) if n_fail else ''}{hold_s} attempts={k.get('attempts', 0)}"
                       + (f" · {fails or err}" if (fails or err) else ""))
    head = (f"sweep detail: parents={len(rows)} done={sum(1 for _, k in rows if k['state'] == 'done')} "
            f"umfeld confirmed={n_conf} pending={n_wait} · rows capped at {limit} (?sweep=N) · "
            "confirmed = umfeld manifest obs.digest == our post-sweep (green-fixed) digest")
    return [head] + out[:limit]


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
              "status": status, "cells": cells or 0, "error": err or "", "seconds": int(seconds or 0),
              # add-on products (light_v2 + json_v2 on Zenodo): "ok" | "upload: …" | "verify: …"
              "products": str((result or {}).get("sweep_products") or ("" if status != "ok" else "unknown"))[:200]}
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
    elif cmd == "rerun":
        # re-dispatch ok units that have no add-on products (all parents when no codes given)
        print(json.dumps(rerun_missing_products(sys.argv[2:] or None)))
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
