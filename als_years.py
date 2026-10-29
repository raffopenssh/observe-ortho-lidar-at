"""Real ALS acquisition year per NE cell / KG (BEV flight-block metadata).

Why: the frozen 41 B NEO1 row carries ``flight_years`` as a bitmask
(bit i = ALS 2020+i).  ~60 % of Austria was last flown before 2020, so the
mask is 0 there and conflates "flown ≤2019" with "no data"; the year
itself is lost.  umfeld-at needs the real year for the declared-vs-observed
diff.  BEV publishes the acquisition footprint per mosaic (the
``Metadatenuebersicht_SHP_DTM_DSM_<stichtag>`` shapefile listed on the
data.gv.at / data.bev.gv.at series record, ``Flugjahr`` per block) and we
vendor it as ``data/als_acquisition/flight_blocks_<dataset>.geojson``
(``als_acquisition.py``).  This module evaluates that overlay *per cell
centre* at read time — the row stays frozen, nothing is re-posted.

What v2.4 actually uses (``tile_index.DATASETS``):

* ``NEWEST_DATASET`` = ``tile_index.DEFAULT_DATASET`` (20240915 mosaic) —
  every observed height / cover / tree in the product comes from this
  mosaic, so ``als_year_newest`` = Flugjahr of the 20240915 block under
  the cell (DSM year when BEV lists DTM/DSM separately — canopy comes from
  the DSM, same rule as ``als_acquisition._parse_flugjahr``).
* ``OLDEST_DATASET`` = oldest key of ``tile_index.DATASETS`` (20220915) —
  ``dh_m`` / ``dtm_m`` are newest − oldest mosaic, so ``als_year_oldest``
  is the Flugjahr of the 20220915 block.  ``reflown`` = newest > oldest
  (otherwise both mosaics carry the same flight and ``dh_m`` is noise).
* The 20250915 overlay is vendored too but **not** used here: no v2.4
  product was built from it.

Public surface (``docs/ne-public-api.md → ALS acquisition year``):

* ``/api/v1/cells`` json / columns / csv: ``als_year_newest`` (u16,
  65535 = null) + ``als_year_oldest`` next to ``als_years``; roll-ups
  take max / min.  ``format=bin`` / ``h3b`` are the frozen rows — unchanged.
* ``/api/v1/kg/<code>`` → ``als{years, newest, oldest, source, coverage,
  …}`` (``kg_block``), cached per (kg, neo1_digest) in ``cells.sqlite``.
* ``/api/v1/status`` → ``als{kgs_with_als_year, year_hist, …}``.
* NEO1 header ``als_years{newest, oldest, source}`` on every ``/obs``
  POST (``ne_h3.build_observed`` / ``neo1_section_from_h3b``) —
  header only, digest-neutral.

Disk / bandwidth budget:

* Per request nothing is read from disk: the two overlays (~1.7 MB GeoJSON,
  ~450 polygons) live in memory as STRtrees; the per-cell years are computed
  once per inflated chunk (``cells_api.Chunk.als``) from the chunk's cached
  centres and ride the existing chunk LRU.
* The per-KG block is cached in ``cells.sqlite`` (``als_kg``) keyed on the
  product's ``neo1_digest``.  Two precisions: ``exact`` (every cell; done at
  ingest time when the freshly written file is hot in the page cache, or on
  ``--kg``) and ``chunk`` (res-10 chunk centres weighted by their cell counts
  from the already-indexed ``chunk`` table — zero file reads, the whole
  6 700-KG corpus in ~1 min; coverage within ±0.5 % of exact because the
  flight blocks are state-level polygons).  The backfill and on-demand
  misses use ``chunk``; an ingest upgrades a KG to ``exact``.
* Nothing goes over the network: no re-POST (header is digest-neutral), the
  ``/kg`` block is ~0.5 KB, and the cell fields are two u16 columns that
  gzip to a few bytes per cell (long runs of the same year).

Backfill runs as a background thread on the primary (``start_backfill_thread``);
progress is the ``als_years:`` line of ``/process.txt`` (``text_line``).
CLI: ``python3 als_years.py --backfill`` / ``--status`` / ``--kg 61047``.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)

import tile_index as _ti

NEWEST_DATASET = _ti.DEFAULT_DATASET                 # 20240915: all observed values
OLDEST_DATASET = min(_ti.DATASETS)                   # 20220915: the "oldest" of dh_m / dtm_m
NULL = 65535                                         # u16 null (same convention as elev_m)

# data.gv.at / data.bev.gv.at series record of the mosaic the products are
# built from ("Serie ALS DTM Höhenraster 1m Stichtag 15.09.2024"); the
# flight-block shapefile is listed under its "Digital transfer options".
SOURCE_ID = "5ce253fc-b7c5-4362-97af-6556c18a45d9"
SOURCE = f"data.gv.at {SOURCE_ID}"
SOURCE_URL = f"https://data.bev.gv.at/geonetwork/srv/api/records/{SOURCE_ID}"
SOURCE_FILE = f"Metadatenuebersicht_SHP_DTM_DSM_{NEWEST_DATASET}.zip"
SOURCE_NOTE = ("BEV 'Aktualität DGM - ALS' flight-block polygons (Flugjahr per block, no month/day; "
               "DSM year when DTM/DSM differ); evaluated at the cell centre, blocks are state-level "
               "campaign footprints so cells within ~100 m of a block edge may be misattributed")

DATA_DIR = Path("data")
NE_DIR = DATA_DIR / "ne"
INDEX_DB = NE_DIR / "cells.sqlite"
STATE_PATH = NE_DIR / "als_backfill.json"
ALGO = "als-years-2"                                # bump → every cached KG block is recomputed

_lock = threading.Lock()
_trees: dict[str, tuple] = {}                      # dataset -> (STRtree | None, years u16)


# === SECTION: per-point lookup ===

def _tree(dataset: str):
    with _lock:
        if dataset in _trees:
            return _trees[dataset]
    import als_acquisition as aa
    recs, tree = aa._load(dataset)
    years = np.array([(r["year_to"] or NULL) for r in recs], dtype=np.uint16) if recs else np.zeros(0, np.uint16)
    with _lock:
        _trees[dataset] = (tree, years)
    return _trees[dataset]


def available() -> bool:
    return _tree(NEWEST_DATASET)[0] is not None


def years_at_3035(e: np.ndarray, n: np.ndarray, dataset: str = NEWEST_DATASET) -> np.ndarray:
    """Flugjahr (u16, NULL where no block) of *dataset*'s mosaic at EPSG:3035
    points.  Overlapping blocks → the newer year."""
    e = np.asarray(e, dtype=np.float64); n = np.asarray(n, dtype=np.float64)
    out = np.full(e.shape, NULL, dtype=np.uint16)
    if e.size == 0:
        return out
    tree, years = _tree(dataset)
    if tree is None:
        return out
    import shapely
    pi, gi = tree.query(shapely.points(e, n), predicate="within")
    if pi.size == 0:
        return out
    y = years[gi]
    ok = y != NULL
    pi, y = pi[ok], y[ok]
    if pi.size:
        # max per point (overlaps are rare; sort so the last write wins = max)
        order = np.lexsort((y, pi))
        out[pi[order]] = y[order]
    return out


def cell_years_3035(e: np.ndarray, n: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """→ (als_year_newest, als_year_oldest) u16 arrays for cell centres."""
    return years_at_3035(e, n, NEWEST_DATASET), years_at_3035(e, n, OLDEST_DATASET)


def ids_years(ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """H3 cell ids (uint64) → (newest, oldest) via the metre-snapped
    EPSG:3035 centre (same rounding as ``ne_h3.cell_centres_3035``)."""
    ids = np.asarray(ids, dtype=np.uint64)
    if ids.size == 0:
        z = np.zeros(0, np.uint16)
        return z, z.copy()
    from h3.api import numpy_int as hn
    from pyproj import Transformer
    ll = np.array([hn.cell_to_latlng(c) for c in ids.tolist()])
    e, n = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True).transform(ll[:, 1], ll[:, 0])
    return cell_years_3035(np.round(e), np.round(n))


# === SECTION: KG block ===

def block_from_years(newest: np.ndarray, oldest: np.ndarray, weights: np.ndarray | None = None,
                     precision: str = "exact") -> dict:
    """The ``als`` block of ``/api/v1/kg/<code>`` from per-cell (or per-chunk,
    ``weights`` = cells per chunk) year arrays.

    ``newest`` = max year seen (the youngest flight touching the KG),
    ``coverage`` = share of cells under that year, ``dominant`` = the
    year covering most cells (+ ``dominant_share``), ``oldest`` = min
    year of the oldest mosaic, ``reflown_share`` = share of cells whose
    newest-mosaic year is later than their oldest-mosaic year (where
    0, ``dh_m`` compares the same flight with itself)."""
    w = np.ones(newest.shape, np.int64) if weights is None else np.asarray(weights, np.int64)
    n = int(w.sum())
    kn = newest != NULL
    ko = oldest != NULL
    out = {
        "algo": ALGO, "precision": precision, "cells_n": n, "cells_known": int(w[kn].sum()),
        "years": [], "newest": None, "oldest": None, "coverage": None, "dominant": None, "dominant_share": None,
        "source": SOURCE, "source_url": SOURCE_URL,
        "mosaics": {"newest": NEWEST_DATASET, "oldest": OLDEST_DATASET},
        "newest_hist": {}, "oldest_hist": {}, "reflown_share": None,
    }
    if n == 0:
        return out
    hn = np.bincount(newest[kn].astype(np.int64), weights=w[kn], minlength=1) if kn.any() else np.zeros(1)
    ho = np.bincount(oldest[ko].astype(np.int64), weights=w[ko], minlength=1) if ko.any() else np.zeros(1)
    yn = np.nonzero(hn)[0]; yo = np.nonzero(ho)[0]
    out["years"] = sorted(set(yn.tolist()) | set(yo.tolist()))
    if yn.size:
        out["newest_hist"] = {str(int(y)): int(hn[y]) for y in yn}
        out["newest"] = int(yn.max())
        out["coverage"] = round(float(hn[out["newest"]] / n), 4)
        dom = int(yn[np.argmax(hn[yn])])
        out["dominant"] = dom
        out["dominant_share"] = round(float(hn[dom] / n), 4)
    if yo.size:
        out["oldest_hist"] = {str(int(y)): int(ho[y]) for y in yo}
        out["oldest"] = int(yo.min())
    elif yn.size:
        out["oldest"] = int(yn.min())
    both = kn & ko
    if both.any():
        out["reflown_share"] = round(float(w[both & (newest > oldest)].sum() / w[both].sum()), 4)
    return out


def header_block(newest: np.ndarray, oldest: np.ndarray, weights=None, precision: str = "exact") -> dict:
    """``als_years`` for the NEO1 section header (``/obs`` POST): header
    only, digest-neutral, ~250 B."""
    b = block_from_years(newest, oldest, weights, precision)
    return {"newest": b["newest"], "oldest": b["oldest"], "source": SOURCE, "coverage": b["coverage"],
            "dominant": b["dominant"], "years": b["years"], "mosaics": b["mosaics"], "algo": ALGO,
            "note": "real BEV Flugjahr per flight block (DSM year); row bitmask flight_years (bit i = 2020+i) unchanged"}


def header_block_for_ids(ids: np.ndarray, e: np.ndarray | None = None, n: np.ndarray | None = None) -> dict | None:
    """Header block from cell ids — or, cheaper, from centres the caller
    already has (``ne_h3.build_observed`` → ``cell_centres_3035``)."""
    try:
        if not available():
            return None
        if e is not None and n is not None:
            return header_block(*cell_years_3035(e, n))
        return header_block(*ids_years(ids))
    except Exception as e_:  # noqa: BLE001
        log.warning("als_years header: %s", e_)
        return None


def _kg_ids(path: Path) -> tuple[dict, np.ndarray]:
    import ne_h3
    hdr = ne_h3.read_h3b_header(path)
    rec = (hdr.get("record_bytes") or {}).get("neo1", 41)
    parts = []
    for ch in hdr["chunks"]:
        o = np.frombuffer(ne_h3.read_h3b_chunk(path, hdr, ch)["obs"], np.uint8).reshape(-1, rec)
        parts.append(o[:, :8].copy().view("<u8").ravel())
    ids = np.concatenate(parts) if parts else np.zeros(0, np.uint64)
    return hdr, ids


def compute_kg(kg: str, path: Path | None = None) -> dict:
    """``precision=exact``: read every cell of ``data/ne/<kg>.h3b`` → ``als``
    block (+ ``neo1_digest``).  Reads the whole file — use at ingest time
    (file hot) or on demand for one KG; the corpus backfill uses
    :func:`compute_kg_chunks` instead."""
    kg = str(kg).zfill(5)
    p = path or (NE_DIR / f"{kg}.h3b")
    t0 = time.time()
    hdr, ids = _kg_ids(p)
    b = block_from_years(*ids_years(ids))
    b["kg"] = kg
    b["neo1_digest"] = (hdr.get("neo1") or {}).get("digest")
    b["seconds"] = round(time.time() - t0, 2)
    return b


def _chunk_rows(kgs: list[str] | None = None) -> list[tuple]:
    """(kg, r10, cells) from the ``chunk`` table of cells.sqlite (no file I/O)."""
    c = _conn()
    if c is None:
        return []
    try:
        if kgs is None:
            return c.execute("SELECT kg, r10, cells FROM chunk").fetchall()
        out = []
        for i in range(0, len(kgs), 500):
            part = kgs[i:i + 500]
            out += c.execute(f"SELECT kg, r10, cells FROM chunk WHERE kg IN ({','.join('?' * len(part))})", part).fetchall()
        return out
    except sqlite3.OperationalError:
        return []


def _digests(kgs: list[str] | None = None) -> dict:
    c = _conn()
    if c is None:
        return {}
    try:
        if kgs is None:
            return dict(c.execute("SELECT kg, neo1_digest FROM file").fetchall())
        out = {}
        for i in range(0, len(kgs), 500):
            part = kgs[i:i + 500]
            out.update(c.execute(f"SELECT kg, neo1_digest FROM file WHERE kg IN ({','.join('?' * len(part))})", part).fetchall())
        return out
    except sqlite3.OperationalError:
        return {}


def compute_kg_chunks(kgs: list[str], progress=None) -> dict[str, dict]:
    """``precision=chunk``: one STRtree pass over the res-10 chunk centres of
    *kgs* (weights = cells per chunk).  Zero file reads; ~10 µs per chunk."""
    rows = _chunk_rows(kgs)
    if not rows:
        return {}
    dg = _digests(kgs)
    kg_arr = np.array([r[0] for r in rows]); r10 = np.array([r[1] for r in rows], dtype=np.uint64)
    w = np.array([r[2] for r in rows], dtype=np.int64)
    new = np.empty(r10.size, np.uint16); old = np.empty(r10.size, np.uint16)
    step = 200_000
    for i in range(0, r10.size, step):
        new[i:i + step], old[i:i + step] = ids_years(r10[i:i + step])
        if progress:
            progress(min(i + step, r10.size), r10.size)
    order = np.argsort(kg_arr, kind="stable")
    kg_s = kg_arr[order]
    bounds = np.flatnonzero(np.concatenate([[True], kg_s[1:] != kg_s[:-1]]))
    ends = np.append(bounds[1:], kg_s.size)
    out = {}
    for b0, b1 in zip(bounds, ends):
        sel = order[b0:b1]
        kg = str(kg_s[b0])
        d = block_from_years(new[sel], old[sel], w[sel], precision="chunk")
        d["kg"] = kg; d["neo1_digest"] = dg.get(kg)
        out[kg] = d
    return out


# === SECTION: cache (cells.sqlite) ===

_SCHEMA = ("CREATE TABLE IF NOT EXISTS als_kg (kg TEXT PRIMARY KEY, neo1_digest TEXT, algo TEXT, "
           "newest INTEGER, oldest INTEGER, coverage REAL, cells_n INTEGER, json TEXT, computed_at REAL)")
_local = threading.local()
_write_lock = threading.Lock()


def _conn(write: bool = False) -> sqlite3.Connection | None:
    if not INDEX_DB.exists() and not write:
        return None
    c = getattr(_local, "c", None)
    if c is None:
        INDEX_DB.parent.mkdir(parents=True, exist_ok=True)
        c = sqlite3.connect(str(INDEX_DB), timeout=30, check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=30000")
        c.execute(_SCHEMA)
        c.commit()
        _local.c = c
    return c


def cached(kg: str) -> dict | None:
    c = _conn()
    if c is None:
        return None
    try:
        r = c.execute("SELECT json, neo1_digest, algo FROM als_kg WHERE kg=?", (str(kg).zfill(5),)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not r or r[2] != ALGO:
        return None
    d = json.loads(r[0])
    d["neo1_digest"] = r[1]
    return d


def _row(b: dict) -> tuple:
    return (b["kg"], b.get("neo1_digest"), ALGO, b.get("newest"), b.get("oldest"), b.get("coverage"),
            b.get("cells_n"), json.dumps({k: v for k, v in b.items() if k not in ("neo1_digest",)}), time.time())


def store(b: dict) -> None:
    store_many([b])


def store_many(blocks: list[dict]) -> None:
    if not blocks:
        return
    c = _conn(write=True)
    with _write_lock:
        c.executemany("INSERT OR REPLACE INTO als_kg VALUES (?,?,?,?,?,?,?,?,?)", [_row(b) for b in blocks])
        c.commit()


def remove(kg: str) -> None:
    c = _conn()
    if c is None:
        return
    with _write_lock:
        c.execute("DELETE FROM als_kg WHERE kg=?", (str(kg).zfill(5),))
        c.commit()


def _digest_of_file(kg: str) -> str | None:
    try:
        import cells_api
        h = cells_api.header(kg)
        return h["neo1_digest"] if h else None
    except Exception:  # noqa: BLE001
        return None


def kg_block(kg: str, compute: bool = True) -> dict | None:
    """``als`` block for ``/api/v1/kg/<code>``: cache hit when the cached
    ``neo1_digest`` matches the product on disk, else (re)compute (≤ ~6 s
    for the largest KG, ~0.1 s typical) and store.  None = no product."""
    kg = str(kg).zfill(5)
    if not (NE_DIR / f"{kg}.h3b").exists():
        return None
    dg = _digest_of_file(kg)
    b = cached(kg)
    if b is not None and (dg is None or b.get("neo1_digest") == dg):
        return _public(b)
    if not compute:
        return None
    b = compute_kg_chunks([kg]).get(kg)       # instant, no file read
    if b is None:
        b = compute_kg(kg)
    store(b)
    return _public(b)


def index_file(path: str | Path) -> dict | None:
    """Hook for ``cells_api.index_file`` (every ingest / merge): recompute
    the KG block so the cache never serves a stale digest."""
    p = Path(path)
    try:
        if not available():
            return None
        b = compute_kg(p.stem, p)
        store(b)
        return b
    except Exception as e:  # noqa: BLE001
        log.warning("als_years index %s: %s", p.name, e)
        return None


_PUBLIC_DROP = ("neo1_digest", "seconds", "kg", "algo")


def _public(b: dict) -> dict:
    return {k: v for k, v in b.items() if k not in _PUBLIC_DROP}


_SLIM_KEYS = ("newest", "oldest", "dominant", "dominant_share", "precision", "reflown_share")


def bulk_slim(kgs: list[str] | None = None) -> dict[str, dict]:
    """Bulk ``als`` blocks for ``/api/v1/kgs?fields=codes,als`` (umfeld-at
    saves one GET per KG per pass): one SELECT over the cache, no compute,
    no histograms.  KGs without a cached block (no product yet / backfill
    pending) are absent from the result."""
    c = _conn()
    if c is None:
        return {}
    out = {}
    try:
        rows = c.execute("SELECT kg, json FROM als_kg WHERE algo=?", (ALGO,)).fetchall()
    except sqlite3.OperationalError:
        return {}
    want = set(str(k).zfill(5) for k in kgs) if kgs is not None else None
    for kg, js in rows:
        if want is not None and kg not in want:
            continue
        d = json.loads(js)
        out[kg] = {k: d.get(k) for k in _SLIM_KEYS}
    return out


# === SECTION: status / summary ===

def status_summary() -> dict:
    """``als`` block of ``/api/v1/status``: how many KGs carry a real year,
    the newest-year histogram (KGs and cells), oldest/newest span, backfill state."""
    c = _conn()
    out = {"kgs_with_als_year": 0, "kgs_cached": 0, "kgs_products": 0, "year_hist": {}, "year_hist_cells": {},
           "newest_min": None, "newest_max": None, "source": SOURCE, "source_url": SOURCE_URL,
           "mosaics": {"newest": NEWEST_DATASET, "oldest": OLDEST_DATASET}, "algo": ALGO,
           "fields": {"cell": ["als_year_newest", "als_year_oldest"], "kg": "als", "neo1_header": "als_years"},
           "backfill": backfill_state()}
    try:
        out["kgs_products"] = len([p for p in NE_DIR.glob("*.h3b") if p.stem.isdigit()])
    except Exception:  # noqa: BLE001
        pass
    if c is None:
        return out
    try:
        rows = c.execute("SELECT newest, COUNT(*), SUM(cells_n) FROM als_kg WHERE algo=? GROUP BY newest", (ALGO,)).fetchall()
    except sqlite3.OperationalError:
        return out
    hist = {}; hist_c = {}; cached_n = 0; known = 0
    for y, n, cells in rows:
        cached_n += int(n)
        if y is None:
            continue
        known += int(n)
        hist[str(int(y))] = int(n)
        hist_c[str(int(y))] = int(cells or 0)
    out.update(kgs_with_als_year=known, kgs_cached=cached_n,
               year_hist=dict(sorted(hist.items())), year_hist_cells=dict(sorted(hist_c.items())))
    if hist:
        ys = [int(k) for k in hist]
        out["newest_min"] = min(ys); out["newest_max"] = max(ys)
    try:
        r = c.execute("SELECT MIN(oldest), AVG(coverage) FROM als_kg WHERE algo=? AND newest IS NOT NULL", (ALGO,)).fetchone()
        out["oldest_min"] = int(r[0]) if r and r[0] is not None else None
        out["coverage_mean"] = round(float(r[1]), 3) if r and r[1] is not None else None
        out["precision"] = {k: int(n) for k, n in c.execute(
            "SELECT json_extract(json, '$.precision'), COUNT(*) FROM als_kg WHERE algo=? GROUP BY 1", (ALGO,)).fetchall()}
    except sqlite3.OperationalError:
        pass
    return out


# === SECTION: backfill (primary, background) ===

_bf = {"running": False}


def backfill_state() -> dict:
    try:
        d = json.loads(STATE_PATH.read_text())
    except Exception:  # noqa: BLE001
        d = {}
    d["running_here"] = bool(_bf.get("running"))
    return d


def _save_state(d: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(d))
        os.replace(tmp, STATE_PATH)
    except Exception as e:  # noqa: BLE001
        log.debug("als_years state: %s", e)


def pending_kgs() -> list[str]:
    """Products on disk whose cached block is missing / stale (digest or algo)."""
    files = {p.stem: p for p in NE_DIR.glob("*.h3b") if p.stem.isdigit()}
    c = _conn()
    have = {}
    if c is not None:
        try:
            have = {k: (d, a) for k, d, a in c.execute("SELECT kg, neo1_digest, algo FROM als_kg").fetchall()}
        except sqlite3.OperationalError:
            have = {}
    digests = {}
    try:
        import cells_api
        cc = cells_api._idx_conn()
        if cc is not None:
            digests = dict(cc.execute("SELECT kg, neo1_digest FROM file").fetchall())
    except Exception:  # noqa: BLE001
        pass
    out = []
    for kg in sorted(files):
        h = have.get(kg)
        if h is None or h[1] != ALGO or (digests.get(kg) and h[0] != digests.get(kg)):
            out.append(kg)
    return out


def backfill(should_run=lambda: True, sleep_s: float = 0.0, batch: int = 100) -> dict:
    """Compute + cache every pending KG block at ``chunk`` precision from the
    sqlite chunk index — no product file is opened.  Progress → ``STATE_PATH``
    (read by ``text_line``).  Idempotent; safe to re-run."""
    if not available():
        _save_state({"error": "no flight-block overlay for " + NEWEST_DATASET, "finished_at": time.time()})
        return {"done": 0, "total": 0}
    todo = pending_kgs()
    st = {"started_at": time.time(), "total": len(todo), "done": 0, "errors": 0, "last_kg": None,
          "finished_at": None, "seconds": 0.0, "precision": "chunk"}
    _bf["running"] = True
    _save_state(st)
    t0 = time.time()
    try:
        for i in range(0, len(todo), batch):
            if not should_run():
                st["stopped"] = True
                break
            part = todo[i:i + batch]
            try:
                blocks = compute_kg_chunks(part)
                store_many(list(blocks.values()))
                for k in [k for k in part if k not in blocks]:
                    # not in the chunk index (index gap) → one exact read of that file
                    try:
                        store(compute_kg(k))
                    except Exception as e:  # noqa: BLE001
                        st["errors"] += 1
                        st["last_error"] = f"{k}: {e}"[:200]
            except Exception as e:  # noqa: BLE001
                st["errors"] += len(part)
                st["last_error"] = f"{part[0]}..{part[-1]}: {e}"[:200]
                log.warning("als_years backfill %s: %s", part[0], e)
            st["done"] = min(i + batch, len(todo)); st["last_kg"] = part[-1]; st["seconds"] = round(time.time() - t0, 1)
            _save_state(st)
            if sleep_s:
                time.sleep(sleep_s)
    finally:
        st["finished_at"] = time.time(); st["seconds"] = round(time.time() - t0, 1)
        _bf["running"] = False
        _save_state(st)
    log.info("als_years backfill: %d KGs in %.0fs (errors %d)", st["done"], st["seconds"], st["errors"])
    return st


def start_backfill_thread(should_run=lambda: True, lock_path: Path = NE_DIR / ".als_backfill.lock") -> bool:
    """Primary only, single-flight across gunicorn workers (fcntl).  Runs
    one backfill pass at start-up (+ a slow re-scan every 6 h for products
    that changed digest without passing through ``index_file``)."""
    import fcntl
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock_path, "w")
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False

    def _run():
        time.sleep(90)       # let srv settle; the pass itself is ~1 min of CPU, no file reads
        while True:
            try:
                if should_run():
                    backfill(should_run, sleep_s=0.2)     # ~0.5 s of CPU per 100-KG batch, then yield
            except Exception as e:  # noqa: BLE001
                log.warning("als_years backfill thread: %s", e)
            time.sleep(6 * 3600)

    threading.Thread(target=_run, daemon=True, name="als-years-backfill").start()
    _bf["lock"] = fh
    return True


# === SECTION: process.txt ===

def text_line() -> str:
    s = status_summary()
    bf = s.get("backfill") or {}
    hist = s.get("year_hist") or {}
    top = sorted(hist.items(), key=lambda kv: -kv[1])[:6]
    line = (f"als_years: kgs_with_year={s['kgs_with_als_year']}/{s['kgs_products']} products "
            f"(cached={s['kgs_cached']}) newest {s.get('newest_min')}..{s.get('newest_max')} "
            f"oldest_min={s.get('oldest_min')} coverage_mean={s.get('coverage_mean')} "
            f"src={NEWEST_DATASET}/{OLDEST_DATASET} mosaics"
            + (" exact=" + str((s.get('precision') or {}).get('exact', 0)) if s.get('precision') else ""))
    if top:
        line += " · top years [" + " ".join(f"{y}={n}" for y, n in top) + "]"
    if bf.get("total") is not None:
        state = "running" if bf.get("running_here") or not bf.get("finished_at") else "done"
        if bf.get("stopped"):
            state = "stopped"
        line += (f" · backfill {state} {bf.get('done')}/{bf.get('total')} "
                 f"{bf.get('seconds')}s errors={bf.get('errors')} (chunk-level, no file reads)")
        if bf.get("last_error"):
            line += f" last_error={bf['last_error']}"
    elif bf.get("error"):
        line += f" · backfill ERROR {bf['error']}"
    else:
        line += " · backfill not started"
    return line


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="real ALS acquisition year per NE cell / KG")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--kg", help="compute (and cache) one KG, print the block")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.kg:
        b = compute_kg(a.kg); store(b)
        print(json.dumps(b, indent=1))
    if a.backfill:
        print(json.dumps(backfill(), indent=1))
    if a.status or not (a.kg or a.backfill):
        print(json.dumps(status_summary(), indent=1))
        print(text_line())
