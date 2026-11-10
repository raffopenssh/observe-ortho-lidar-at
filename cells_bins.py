"""cells_bins — per-bin columnar export of everything we hold locally per
H3 res-12 cell (``GET /api/v1/cells?bbox=<0.025° bin>&format=bin&fields=…``).

Why: a downstream statistical index builds a per-cell AND/OR filter on the
NEO1 rows, but the 41 B row cannot tell a tall
structure from a tall tree and carries no terrain for cells we never
posted.  This module joins **all three local product forms** once per bin
and stores the result, so a downstream index can pull every bin exactly
once per generation:

* ``data/ne/<kg>.h3b``  — NEO1 rows (all 24 public fields), NET1 tree apices
  (→ canopy height percentiles), NEB1 man-made objects (→ building heights,
  storeys, areas, tallest structure of any kind);
* ``data/kg_v2_store.db`` grid25 (``_json_v2`` product) — 25 m DTM elevation +
  slope for the whole KG raster (→ terrain for **every** cell with DTM, not
  only cells with an observation row; aspect from the DTM gradient), 25 m
  dominant class / canopy / woody fractions;
* ``_json_v2`` ``new_buildings`` — roof form hint + storey estimate where
  the product carries them (sparse, honest 0 = unknown elsewhere).

Bin = ``w = i·0.025, s = j·0.025`` (half-open, WGS84), all res-12 cells
whose centre lies inside (``h3.polygon_to_cells``), **sorted by cell id**.
One zlib'd blob per bin on disk (``data/ne/bins/<i>_<j>.nex``, ~100-350 KB,
written on first request), rebuilt only when
its *generation* changes — ``gen`` = sha256 over the (kg, neo1_digest,
partial) of every product intersecting the bin, the grid25 store stamps of
the same parents, the green-fix id and ``ALGO``.  The request path checks
the generation (two sqlite lookups, < 2 ms), slices the requested columns
from the stored blob (in-process byte LRU) and lets ``public_api`` gzip it.

Wire container ``NEX1`` (little-endian)::

    b'NEX1' | u32 header_len | header JSON | column bytes …

``header.columns[]`` = ``{name, dtype, shape?, bytes, scale?, nodata?}`` in
order; ``header.ids`` = ``delta_u64`` (default: first id absolute, then
differences — ~2 KB gz for a full bin) or ``u64``.  ``header.gen`` is the
bin generation (also ``X-Cells-Gen`` / the strong ``ETag`` stem).

Standalone: imports ``cells_api`` (ids, chunks, centres), ``kg_v2_store`` +
``v21_products`` (local sqlite blobs only — no network, ever).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import struct
import threading
import time
import zlib
from collections import OrderedDict
from pathlib import Path

import numpy as np

import cells_api as ca
import ne_h3
import ne_green_fix as _green_fix
import mem_lru as _mem_lru

log = logging.getLogger('cells_bins')

# === SECTION: constants ===

ALGO = 'nex-2'                   # nex-2 (2026-11-03): + tree_h_n / tree_h ragged apex-height lists
GEN_ALGO = 'nex-1'               # part of the gen hash — NOT bumped for nex-2: frames of existing field lists are byte-identical,
                                 # so every consumer cache keyed on X-Cells-Gen stays valid; blobs lacking a nex-2 column are rebuilt on demand
RAGGED = {'tree_h': 'tree_h_n'}  # ragged column → its per-row count column (always emitted before it)
MAGIC = b'NEX1'
BLOCK_MAGIC = b'NEXB'
BIN_DEG = 0.025
BLOCK_N = 4                      # a block = 4 × 4 bins = 0.1° × 0.1°
BLOCK_DEG = BIN_DEG * BLOCK_N
BIN_DIR = ca.NE_DIR / 'bins'
MAX_BIN_CELLS = 24_000           # a 0.025° bin holds ~17 k res-12 cells
LON_I = (int(9.0 / BIN_DEG), int(17.6 / BIN_DEG) + 1)
LAT_J = (int(46.0 / BIN_DEG), int(49.2 / BIN_DEG) + 1)
BUILDING_TYPES = ('roof', 'greenhouse', 'solar_panel')
STOREY_M = 3.0                   # product rule: stories_est = max(1, round(h_max / 3))

# u8 half-metre columns: 0 = none in cell, 255 = unknown (cell never observed)
U8_UNK = 255
U16_UNK = 65535
ASPECT_FLAT = 255
ASPECT_UNK = 254
FLAT_SLOPE_DEG = 1.0

# name → (dtype, shape, scale/unit, nodata, description, source)
# dtype codes: u8 i8 u16 u64
FIELDS: 'OrderedDict[str, dict]' = OrderedDict()


def _f(name, dtype, unit, nodata, desc, src, shape=None):
    FIELDS[name] = {'dtype': dtype, 'unit': unit, 'nodata': nodata, 'desc': desc, 'src': src,
                    **({'shape': shape} if shape else {})}


_f('id', 'u64', 'H3 res-12 cell id', None, 'sorted ascending; delta-coded on the wire unless ids=u64', 'h3')
_f('obs', 'u8', 'flag', None, '1 = an observation row exists for the cell (NEO1 fields valid), 0 = not observed', 'h3b')
_f('kg', 'u8', 'index into header.kgs', 255, 'product the observation row came from (255 = none)', 'h3b')
_f('terrain_src', 'u8', 'code', 0, '0 none · 1 observation row (5 m DTM) · 2 grid25 (25 m DTM) fallback', 'join')
# --- terrain for every cell with DTM --------------------------------------
_f('elev_mean', 'u16', 'm a.s.l.', U16_UNK, 'cell-mean elevation; row value where observed, else 25 m DTM bilinear', 'join')
_f('slope_mean', 'u8', 'deg', U8_UNK, 'cell-mean slope; row value where observed, else 25 m slope (nearest)', 'join')
_f('slope_p90', 'u8', 'deg', U8_UNK, 'p90 slope where observed; on the 25 m fallback = max of the 3×3 25 m slopes under the cell', 'join')
_f('aspect_deg', 'u8', '×2 deg (0..179 → 0..358°)', ASPECT_UNK,
   'downslope direction from the 25 m DTM gradient (Horn 3×3), 255 = flat (slope < 1°), 254 = unknown; '
   'sector centre from the row when no 25 m DTM', 'join')
# --- buildings (NEB1 objects of type roof / greenhouse / solar_panel) -------
_f('bld_n', 'u8', 'count', U8_UNK, 'building-type objects whose centroid lies in the cell (0 none)', 'neb1')
_f('bld_h_max', 'u8', '×0.5 m', U8_UNK, 'tallest building object (its h_max); 0 = no building, 255 = unknown', 'neb1')
_f('bld_h_p50', 'u8', '×0.5 m', U8_UNK, 'median of the robust heights (per-object p90) of the building objects; 0 none', 'neb1')
_f('bld_area_sum', 'u16', 'm²', U16_UNK, 'summed segment area of the building objects in the cell (0 none)', 'neb1')
_f('stories_max', 'u8', 'storeys', U8_UNK, 'max over building objects of max(1, round(h_max / 3 m)) — the product rule; 0 none', 'neb1')
_f('roof_dom', 'u8', 'code', 0, '0 unknown · 1 flat · 2 pitched — majority of the product\'s new_buildings roof hints in the cell '
   '(DSM std < 1.5 m = flat); 0 wherever the product carries no hint', 'json_v2')
_f('struct_h_max', 'u8', '×0.5 m', U8_UNK, 'tallest man-made object of ANY type (incl. mast, wind_turbine, wall); 0 none', 'neb1')
# --- trees (NET1 apices h ≥ 3 m) -------------------------------------------
_f('canopy_h_p90', 'u8', '×0.5 m', U8_UNK, 'p90 of apex heights in the cell; 0 = no apex', 'net1')
_f('canopy_h_p50', 'u8', '×0.5 m', U8_UNK, 'median apex height; 0 = no apex', 'net1')
_f('tree_h_n', 'u16', 'count', None, 'exact number of apices (h ≥ 3 m) in the cell = length of its tree_h list (tree_n saturates at 255)', 'net1')
_f('tree_h', 'u8', '×0.5 m', None, 'RAGGED: the apex heights of every cell back to back (1..254 ×0.5 m, same scale as tree_h_max), '
   'SORTED ASCENDING within a cell; cell k owns tree_h[off[k]:off[k+1]] with off = [0, cumsum(tree_h_n)] — a height-range count '
   'is two binary searches per cell; requesting tree_h always emits tree_h_n right before it', 'net1', shape='ragged')
# --- 25 m grid (json_v2 landcover.grid25) for cells without a row ------------
_f('lc25', 'u8', 'class code (header.lc25_legend)', U8_UNK, 'dominant segment class of the 25 m grid cell under the centre (0 = unclassified)', 'grid25')
_f('lc25_frac', 'u8', '/255', U8_UNK, 'share of that 25 m cell in the dominant class', 'grid25')
_f('canopy25', 'u8', '/255', U8_UNK, 'canopy fraction of the 25 m cell (apex-inventory canopy mask)', 'grid25')
_f('woody25', 'u8', '/255', U8_UNK, 'woody-class fraction of the 25 m cell (tree/orchard/vineyard/hedge/garden/shrub)', 'grid25')
# --- ALS flight years (read-time overlay, als_years.py) ----------------------
_f('als_year_newest', 'u16', 'year', U16_UNK, 'real BEV flight year of the newest mosaic under the cell', 'als')
_f('als_year_oldest', 'u16', 'year', U16_UNK, 'real BEV flight year of the oldest mosaic under the cell', 'als')

# --- every NEO1 public field, raw quantisation (see /cells/dict units) -------
_NEO1_DT = {'|u1': 'u8', '|i1': 'i8', '<u2': 'u16'}
for _name, (_sf, _kind) in ca.OBS_PUBLIC.items():
    _dt = ca.OBS_DT[_sf]
    if _name == 'cover':
        _f('cover', 'u8', '/255 per group (header.groups order)', None, 'cover fraction per group; all 0 when unobserved', 'neo1', shape=[9])
        continue
    _dts = _NEO1_DT[_dt.base.str]
    _nd = {'u8': 255, 'i8': -128, 'u16': 65535}[_dts]
    if _kind in ('int', 'years'):
        _nd = None
    _f(_name, _dts, ca.UNITS.get(_name) or next((u for k, u in ca.UNITS.items() if _name in k.split('/')), ''),
       _nd, 'NEO1 row field, quantised as in format=columns', 'neo1')

NP_DT = {'u8': np.uint8, 'i8': np.int8, 'u16': np.dtype('<u2'), 'u32': np.dtype('<u4'), 'u64': np.dtype('<u8')}
DEFAULT_FIELDS = ('obs', 'kg', 'terrain_src', 'elev_mean', 'slope_mean', 'slope_p90', 'aspect_deg',
                  'bld_n', 'bld_h_max', 'bld_h_p50', 'stories_max', 'roof_dom', 'struct_h_max',
                  'canopy_h_p90', 'h_max', 'tree_h_max', 'canopy', 'cover')

_blob_lru = _mem_lru.ByteLRU('cells_bins', 96, env='CELLS_BINS_MB')
_grid_lru = _mem_lru.ByteLRU('cells_bins_grid25', 48, env='CELLS_BINS_GRID_MB')
_roof_lru = _mem_lru.ByteLRU('cells_bins_roof', 8)
_build_locks: dict = {}
_build_locks_guard = threading.Lock()


# === SECTION: bin geometry / generation ===

def bin_of(w: float, s: float, e: float, n: float, tol: float = 1e-7) -> tuple[int, int] | None:
    """(i, j) when the bbox is exactly one 0.025°-aligned bin, else None."""
    i = round(w / BIN_DEG); j = round(s / BIN_DEG)
    if (abs(i * BIN_DEG - w) > tol or abs(j * BIN_DEG - s) > tol
            or abs((i + 1) * BIN_DEG - e) > tol or abs((j + 1) * BIN_DEG - n) > tol):
        return None
    return int(i), int(j)


def bin_bbox(i: int, j: int) -> tuple[float, float, float, float]:
    return (round(i * BIN_DEG, 6), round(j * BIN_DEG, 6), round((i + 1) * BIN_DEG, 6), round((j + 1) * BIN_DEG, 6))


def block_of(w: float, s: float, e: float, n: float, tol: float = 1e-7) -> tuple[int, int] | None:
    """(bi, bj) when the bbox is exactly one 0.1°-aligned block (= 4 × 4 bins), else None."""
    bi = round(w / BLOCK_DEG); bj = round(s / BLOCK_DEG)
    if (abs(bi * BLOCK_DEG - w) > tol or abs(bj * BLOCK_DEG - s) > tol
            or abs((bi + 1) * BLOCK_DEG - e) > tol or abs((bj + 1) * BLOCK_DEG - n) > tol):
        return None
    return int(bi), int(bj)


def block_bbox(bi: int, bj: int) -> tuple[float, float, float, float]:
    return (round(bi * BLOCK_DEG, 6), round(bj * BLOCK_DEG, 6), round((bi + 1) * BLOCK_DEG, 6), round((bj + 1) * BLOCK_DEG, 6))


def block_bins(bi: int, bj: int) -> list[tuple[int, int]]:
    """The 16 bins of a block in **row-major** order: rows = latitude bands south
    → north (j ascending), within a row west → east (i ascending):
    ``k = (j - 4·bj)·4 + (i - 4·bi)``."""
    return [(BLOCK_N * bi + di, BLOCK_N * bj + dj) for dj in range(BLOCK_N) for di in range(BLOCK_N)]


def _store_stamps(parents) -> list:
    try:
        import kg_v2_store
        return kg_v2_store.parents_stamp(parents)
    except Exception as e:  # noqa: BLE001
        log.debug('cells_bins: store stamp unavailable: %s', e)
        return [(p, '') for p in sorted(parents)]


_bin_index_cache = {'key': None, 'v': None}


def bin_index() -> dict:
    """bin (i, j) → sorted KG codes whose bbox touches it (from the ``kg``
    table, same inclusive semantics as the r-tree query), cached 30 s."""
    bb = _kg_bboxes()
    key = (len(bb), hash(tuple(sorted(bb))) if len(bb) < 20000 else len(bb), int(time.time() // 30))
    if _bin_index_cache['key'] == key:
        return _bin_index_cache['v']
    per: dict = {}
    for kg, b in bb.items():
        for ij in _bins_of_bbox(*b):
            per.setdefault(ij, []).append(kg)
    v = {ij: sorted(k) for ij, k in per.items()}
    _bin_index_cache.update(key=key, v=v)
    return v


def bin_inputs(i: int, j: int) -> dict:
    """Everything the generation hash depends on, for one bin."""
    w, s, e, n = bin_bbox(i, j)
    inter = bin_index().get((i, j), [])
    files = ca._files()
    on_disk = [k for k in inter if k in files]
    missing = [k for k in inter if k not in files]
    parts = [(k, str(files[k].get('neo1_digest')), int(files[k].get('partial') or 0)) for k in on_disk]
    gen = _gen(i, j, parts, _store_stamps({k.split('-')[0] for k in inter}))
    epochs = sorted({str(files[k].get('epoch')) for k in on_disk if files[k].get('epoch')})
    return {'i': i, 'j': j, 'bbox': (w, s, e, n), 'kgs': inter, 'on_disk': on_disk, 'missing': missing,
            'partial_files': [k for k, _, p in parts if p], 'gen': gen,
            'epoch': epochs[-1] if epochs else None}


def _gen(i: int, j: int, parts: list, stamps: list) -> str:
    """Bin generation: algo + green-fix id + (kg, row digest, partial) of every
    published product in the bin + grid25 store stamps of every parent
    intersecting it (published or not)."""
    h = hashlib.sha256()
    h.update(f'{GEN_ALGO}|{_green_fix.FIX_ID}|{i},{j}\n'.encode())
    h.update(json.dumps(sorted(parts)).encode()); h.update(json.dumps(sorted(stamps)).encode())
    return h.hexdigest()[:24]


def bin_etag(gen: str, fields: list[str], ids_enc: str) -> str:
    rep = hashlib.sha256((','.join(fields) + '|' + ids_enc).encode()).hexdigest()[:8]
    return f'"b-{gen}-{rep}"'


def block_gen(gens: list[str]) -> str:
    return hashlib.sha256('|'.join(gens).encode()).hexdigest()[:24]


def block_etag(gen: str, fields: list[str], ids_enc: str) -> str:
    rep = hashlib.sha256((','.join(fields) + '|' + ids_enc).encode()).hexdigest()[:8]
    return f'"B-{gen}-{rep}"'


# === SECTION: grid25 (25 m DTM / landcover) sampling ===

def _grid25_for(parent: str) -> list[dict]:
    """Decoded grid25 blocks of a parent (terrain + landcover + derived aspect), LRU'd."""
    hit = _grid_lru.get(parent)
    if hit is not None:
        return hit
    out = []
    try:
        import kg_v2_store
        import v21_products as v21
        for code in kg_v2_store.codes_for_parent(parent) or []:
            g = kg_v2_store.get_grid25(code)
            if not g or not g.get('terrain'):
                continue
            t = v21.decode_terrain_grid25(g['terrain'])
            lc = v21.decode_landcover_grid25(g['landcover']) if g.get('landcover') else None
            el = t['elev']
            # Horn-free central gradient on the 25 m grid (NaN-tolerant edges)
            gy, gx = np.gradient(el.astype(np.float64), float(t['cell_m']))
            asp = (np.degrees(np.arctan2(gx, -gy)) + 360.0) % 360.0     # downslope: north = 0, east = 90
            gslope = np.degrees(np.arctan(np.hypot(gx, gy)))
            out.append({'code': code, 'elev': el, 'slope': t['slope'], 'asp': asp.astype(np.float32),
                        'gslope': gslope.astype(np.float32), 'x0': t['x0'], 'y0': t['y0'], 'cell': float(t['cell_m']),
                        'lc': lc})
    except Exception as e:  # noqa: BLE001
        log.warning('cells_bins: grid25 %s: %s', parent, e)
    _grid_lru.put(parent, out)
    return out


def _sample(a: np.ndarray, g: dict, E: np.ndarray, N: np.ndarray, *, bilinear: bool, ring_max: bool = False) -> np.ndarray:
    """Sample grid array at EPSG:3035 coords; NaN outside.  ``ring_max`` = max of the
    3×3 neighbourhood (for the p90-slope fallback)."""
    rows, cols = a.shape
    cell = g['cell']
    fx = (E - g['x0']) / cell - 0.5
    fy = (g['y0'] - N) / cell - 0.5
    inside = (fx > -0.5) & (fx < cols - 0.5) & (fy > -0.5) & (fy < rows - 0.5)
    out = np.full(E.shape, np.nan, np.float32)
    if not inside.any():
        return out
    ci = np.clip(np.round(fx).astype(int), 0, cols - 1)
    ri = np.clip(np.round(fy).astype(int), 0, rows - 1)
    af = a.astype(np.float32)
    out[inside] = af[ri, ci][inside]
    if ring_max:
        acc = out.copy()
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                v = af[np.clip(ri + dr, 0, rows - 1), np.clip(ci + dc, 0, cols - 1)]
                acc = np.where(inside & np.isfinite(v) & ((v > acc) | ~np.isfinite(acc)), v, acc)
        return acc
    if bilinear:
        c0 = np.clip(np.floor(fx).astype(int), 0, cols - 1); c1 = np.clip(c0 + 1, 0, cols - 1)
        r0 = np.clip(np.floor(fy).astype(int), 0, rows - 1); r1 = np.clip(r0 + 1, 0, rows - 1)
        wx = np.clip(fx - c0, 0, 1); wy = np.clip(fy - r0, 0, 1)
        bil = (af[r0, c0] * (1 - wx) * (1 - wy) + af[r0, c1] * wx * (1 - wy)
               + af[r1, c0] * (1 - wx) * wy + af[r1, c1] * wx * wy)
        ok = inside & np.isfinite(bil)
        out[ok] = bil[ok]
    return out


# === SECTION: build ===

def _u8h(v: np.ndarray, present: np.ndarray, unknown: np.ndarray) -> np.ndarray:
    """metres → u8 ×0.5 m with 0 = none, ≥1 where present, 255 unknown."""
    q = np.clip(np.round(np.nan_to_num(v, nan=0.0) * 2.0), 1, 254).astype(np.uint8)
    out = np.where(present, q, 0).astype(np.uint8)
    out[unknown] = U8_UNK
    return out


def _per_cell_stats(cell_idx: np.ndarray, vals: np.ndarray, n: int):
    """(max, p50, p90, count) per cell for rows grouped by cell_idx (sorted)."""
    mx = np.full(n, np.nan); p50 = np.full(n, np.nan); p90 = np.full(n, np.nan)
    cnt = np.bincount(cell_idx, minlength=n) if cell_idx.size else np.zeros(n, np.int64)
    if cell_idx.size == 0:
        return mx, p50, p90, cnt
    order = np.lexsort((vals, cell_idx))
    ci = cell_idx[order]; v = vals[order]
    starts = np.searchsorted(ci, np.arange(n), 'left'); ends = np.searchsorted(ci, np.arange(n), 'right')
    has = ends > starts
    k = ends - starts
    i50 = np.clip(starts + ((k - 1) // 2).astype(np.int64), 0, max(v.size - 1, 0))
    i90 = np.clip(starts + np.ceil(0.9 * k).astype(np.int64) - 1, 0, max(v.size - 1, 0))
    mx[has] = v[np.clip(ends - 1, 0, max(v.size - 1, 0))][has]
    p50[has] = v[i50][has]; p90[has] = v[i90][has]
    return mx, p50, p90, cnt


def ragged_heights(cell_idx: np.ndarray, h_m: np.ndarray, n: int) -> tuple[np.ndarray, np.ndarray]:
    """(tree_h_n u16[n], tree_h u8[total]) — apex heights ×0.5 m (1..254), grouped
    by cell in row order and sorted ascending inside each cell."""
    q = np.clip(np.round(h_m * 2.0), 1, 254).astype(np.uint8)
    o = np.lexsort((q, cell_idx))
    cnt = np.bincount(cell_idx, minlength=n)
    return np.minimum(cnt, 65535).astype('<u2'), np.ascontiguousarray(q[o])


def ragged_offsets(counts: np.ndarray) -> np.ndarray:
    off = np.zeros(counts.size + 1, np.int64); np.cumsum(counts, out=off[1:]); return off


def ragged_take(counts: np.ndarray, blob: np.ndarray, sel: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Row subset (bool mask or index array, in output order) of a ragged column."""
    counts = np.asarray(counts); blob = np.asarray(blob)
    if sel.dtype == bool:
        return counts[sel], blob[np.repeat(sel, counts)]
    c = counts[sel].astype(np.int64)
    starts = ragged_offsets(counts)[:-1][sel]
    tot = int(c.sum())
    if tot == 0:
        return counts[sel], blob[:0]
    idx = np.repeat(starts - (np.cumsum(c) - c), c) + np.arange(tot)
    return counts[sel], blob[idx]


def _roof_points(parent: str) -> tuple[np.ndarray, np.ndarray]:
    """(res-12 cell ids, is_pitched) of the product's roof-hinted objects for a parent (LRU'd;
    the json_v2 decode is the expensive part of a bin build)."""
    hit = _roof_lru.get(parent)
    if hit is not None:
        return hit
    cids = []; kinds = []
    try:
        import kg_v2_store
        hn = ca._h3n()
        for code in kg_v2_store.codes_for_parent(parent) or []:
            d = kg_v2_store.get(code) or {}
            for f in (d.get('new_buildings') or {}).get('features') or []:
                rt = f.get('roof_type_hint'); lo = f.get('centroid_lon'); la = f.get('centroid_lat')
                if rt in ('flat', 'pitched') and lo is not None and la is not None:
                    cids.append(int(hn.latlng_to_cell(float(la), float(lo), ca.RES))); kinds.append(rt == 'pitched')
    except Exception as e:  # noqa: BLE001
        log.debug('cells_bins: roof hints %s: %s', parent, e)
    out = (np.asarray(cids, np.uint64), np.asarray(kinds, bool))
    _roof_lru.put(parent, out)
    return out


def _roof_hints(parents: list[str], ids: np.ndarray) -> np.ndarray:
    """roof_dom per cell from the json_v2 ``new_buildings`` roof hints (sparse)."""
    out = np.zeros(ids.size, np.uint8)
    try:
        import kg_v2_store
    except Exception:  # noqa: BLE001
        return out
    flat = np.zeros(ids.size, np.int32); pitched = np.zeros(ids.size, np.int32)
    for p in parents:
        cid, kinds = _roof_points(p)
        if cid.size == 0:
            continue
        pos = np.searchsorted(ids, cid)
        ok = (pos < ids.size) & (ids[np.minimum(pos, ids.size - 1)] == cid)
        np.add.at(pitched, pos[ok & kinds], 1); np.add.at(flat, pos[ok & ~kinds], 1)
    out[flat > pitched] = 1; out[pitched >= flat] = 2
    out[(flat == 0) & (pitched == 0)] = 0
    return out


def build_bin(i: int, j: int, inputs: dict | None = None) -> dict:
    """Join everything for one bin → {'hdr': dict, 'cols': OrderedDict[name → ndarray]}."""
    t0 = time.time()
    inp = inputs or bin_inputs(i, j)
    w, s, e, n = inp['bbox']
    ids = np.sort(ca.ids_in_bbox(w, s, e, n))
    if ids.size > MAX_BIN_CELLS:
        raise ca.CellsError(413, f'{ids.size} cells in bin > {MAX_BIN_CELLS}')
    N = ids.size
    tb, hdrs, meta = ca.select(ids, (w, s, e, n))
    cols: 'OrderedDict[str, np.ndarray]' = OrderedDict()
    cols['id'] = ids.astype('<u8')
    # --- observation rows aligned onto the full id list ---------------------
    pos = np.searchsorted(ids, tb.ids) if len(tb) else np.zeros(0, np.int64)
    has_row = np.zeros(N, bool); has_row[pos] = True
    obs_full = np.zeros(N, ca.OBS_DT)
    obs_full['id'] = ids
    for f in ca.OBS_DT.names:
        if f == 'id':
            continue
        nd = {'u1': 255, 'i1': -128, 'u2': 65535}[ca.OBS_DT[f].base.str[1:]]
        obs_full[f] = 0 if f in ('cover', 'sn', 'tn', 'fy', 'vd') else nd
    if len(tb):
        obs_full[pos] = tb.obs
    observed = has_row & ((obs_full['hm'] != 255) | (obs_full['cover'].sum(axis=1) > 0) | (obs_full['el'] != 65535))
    unknown = ~observed
    cols['obs'] = observed.astype(np.uint8)
    kgs = list(meta.get('kgs') or [])
    kgcol = np.full(N, 255, np.uint8)
    if len(tb):
        kgcol[pos] = np.minimum(tb.kg_idx, 254)
    cols['kg'] = kgcol
    # --- terrain: row first, 25 m grid for the rest ---------------------------
    cen = ca._centres_3035(ids)
    E, Nn = cen[:, 0], cen[:, 1]
    el_row = obs_full['el'].astype(np.float64); el_row[obs_full['el'] == 65535] = np.nan
    sl_row = obs_full['sl'].astype(np.float64); sl_row[obs_full['sl'] == 255] = np.nan
    s90_row = obs_full['s90'].astype(np.float64); s90_row[obs_full['s90'] == 255] = np.nan
    el25 = np.full(N, np.nan, np.float32); sl25 = np.full(N, np.nan, np.float32); s90_25 = np.full(N, np.nan, np.float32)
    asp25 = np.full(N, np.nan, np.float32); gsl25 = np.full(N, np.nan, np.float32)
    lc25 = np.full(N, U8_UNK, np.uint8); lcf = np.full(N, U8_UNK, np.uint8)
    can25 = np.full(N, U8_UNK, np.uint8); wood25 = np.full(N, U8_UNK, np.uint8)
    parents = sorted({k.split('-')[0] for k in inp['kgs']})
    lc_legend: dict = {}
    for p in parents:
        for g in _grid25_for(p):
            need = ~np.isfinite(el25)
            if not need.any():
                break
            v = _sample(g['elev'], g, E, Nn, bilinear=True)
            m = need & np.isfinite(v)
            el25[m] = v[m]
            if g['slope'] is not None:
                sv = _sample(g['slope'], g, E, Nn, bilinear=False)
                s9 = _sample(g['slope'], g, E, Nn, bilinear=False, ring_max=True)
                sl25[m] = sv[m]; s90_25[m] = s9[m]
            asp25[m] = _sample(g['asp'], g, E, Nn, bilinear=False)[m]
            gsl25[m] = _sample(g['gslope'], g, E, Nn, bilinear=False)[m]
            lc = g.get('lc')
            if lc is not None:
                cl = _sample(lc['cls'], g, E, Nn, bilinear=False)
                mm = m & np.isfinite(cl)
                lc25[mm] = cl[mm].astype(np.uint8)
                lcf[mm] = np.clip(np.round(_sample(lc['cover_frac'], g, E, Nn, bilinear=False)[mm] * 255), 0, 255).astype(np.uint8)
                for key, dst in (('canopy_frac', can25), ('woody_frac', wood25)):
                    if lc.get(key) is not None:
                        dst[mm] = np.clip(np.round(_sample(lc[key], g, E, Nn, bilinear=False)[mm] * 255), 0, 255).astype(np.uint8)
                lc_legend.update(lc.get('legend') or {})
    row_t = np.isfinite(el_row)
    elev = np.where(row_t, el_row, el25)
    slope = np.where(np.isfinite(sl_row), sl_row, sl25)
    s90 = np.where(np.isfinite(s90_row), s90_row, s90_25)
    tsrc = np.where(row_t, 1, np.where(np.isfinite(el25), 2, 0)).astype(np.uint8)
    cols['terrain_src'] = tsrc
    cols['elev_mean'] = np.where(np.isfinite(elev), np.clip(np.round(np.nan_to_num(elev)), 0, 65534), U16_UNK).astype('<u2')
    cols['slope_mean'] = np.where(np.isfinite(slope), np.clip(np.round(np.nan_to_num(slope)), 0, 90), U8_UNK).astype(np.uint8)
    cols['slope_p90'] = np.where(np.isfinite(s90), np.clip(np.round(np.nan_to_num(s90)), 0, 90), U8_UNK).astype(np.uint8)
    asp = np.full(N, ASPECT_UNK, np.uint8)
    g_ok = np.isfinite(asp25)
    flat25 = g_ok & (gsl25 < FLAT_SLOPE_DEG)
    asp[g_ok] = np.clip(np.round(asp25[g_ok] / 2.0), 0, 179).astype(np.uint8)
    asp[flat25] = ASPECT_FLAT
    sec = obs_full['asp']
    row_only = ~g_ok & (sec != 255)
    asp[row_only & (sec == 8)] = ASPECT_FLAT
    m8 = row_only & (sec < 8)
    asp[m8] = (sec[m8].astype(np.int64) * 45 // 2).astype(np.uint8)
    cols['aspect_deg'] = asp
    # --- NEB1 objects → buildings / tallest structure -------------------------
    bld_n = np.zeros(N, np.int64); bh_max = np.full(N, np.nan); bh_p50 = np.full(N, np.nan)
    b_area = np.zeros(N, np.int64); st_max = np.zeros(N, np.int64); any_max = np.full(N, np.nan)
    if len(tb):
        ci, words = tb.gather_structures()
        if words.size:
            d = ca.decode_structures(words)
            cell = pos[ci]
            any_max, _, _, _ = _per_cell_stats(cell, d['h_max_m'], N)
            is_b = np.isin(d['type'], [ne_h3.BLD_TYPES.index(t) for t in BUILDING_TYPES if t in ne_h3.BLD_TYPES])
            if is_b.any():
                cb = cell[is_b]
                bh_max, _, _, bld_n = _per_cell_stats(cb, d['h_max_m'][is_b], N)
                _, bh_p50, _, _ = _per_cell_stats(cb, d['h_robust_m'][is_b], N)
                b_area = np.bincount(cb, weights=d['area_m2'][is_b], minlength=N).astype(np.int64)
                storeys = np.maximum(1, np.round(d['h_max_m'][is_b] / STOREY_M)).astype(np.int64)
                np.maximum.at(st_max, cb, storeys)
    has_b = bld_n > 0
    cols['bld_n'] = np.where(unknown, U8_UNK, np.minimum(bld_n, 254)).astype(np.uint8)
    cols['bld_h_max'] = _u8h(bh_max, has_b, unknown)
    cols['bld_h_p50'] = _u8h(bh_p50, has_b, unknown)
    cols['bld_area_sum'] = np.where(unknown, U16_UNK, np.minimum(b_area, 65534)).astype('<u2')
    sm = np.minimum(st_max, 254).astype(np.uint8); sm[unknown] = U8_UNK
    cols['stories_max'] = sm
    cols['roof_dom'] = _roof_hints(parents, ids)
    cols['struct_h_max'] = _u8h(any_max, np.isfinite(any_max), unknown)
    # --- NET1 apices → canopy height percentiles ------------------------------
    c90 = np.full(N, np.nan); c50 = np.full(N, np.nan)
    th_n = np.zeros(N, '<u2'); th = np.zeros(0, np.uint8)
    if len(tb):
        ci, words = tb.gather_trees()
        if words.size:
            h = ca.decode_trees(words)['h_m']
            _, c50, c90, _ = _per_cell_stats(pos[ci], h, N)
            th_n, th = ragged_heights(pos[ci], h, N)
    cols['canopy_h_p90'] = _u8h(c90, np.isfinite(c90), unknown)
    cols['canopy_h_p50'] = _u8h(c50, np.isfinite(c50), unknown)
    cols['tree_h_n'] = th_n; cols['tree_h'] = th
    cols['lc25'] = lc25; cols['lc25_frac'] = lcf; cols['canopy25'] = can25; cols['woody25'] = wood25
    # --- ALS years ------------------------------------------------------------
    ay_new = np.full(N, U16_UNK, '<u2'); ay_old = np.full(N, U16_UNK, '<u2')
    if len(tb):
        a_new, a_old = tb.als()
        ay_new[pos] = a_new; ay_old[pos] = a_old
    cols['als_year_newest'] = ay_new; cols['als_year_oldest'] = ay_old
    # --- raw NEO1 fields ------------------------------------------------------
    for name, (sf, _kind) in ca.OBS_PUBLIC.items():
        v = obs_full[sf]
        cols[name] = np.ascontiguousarray(v.astype(NP_DT[FIELDS[name]['dtype']]))
    hdr = {
        'algo': ALGO, 'bin_deg': BIN_DEG, 'ij': [i, j], 'bbox': [w, s, e, n], 'n': int(N), 'gen': inp['gen'],
        'epoch': inp['epoch'], 'kgs': kgs, 'kgs_intersecting': inp['kgs'], 'kgs_missing': inp['missing'],
        'kgs_partial': inp['partial_files'], 'complete': not inp['missing'] and not inp['partial_files'],
        'cells_observed': int(observed.sum()), 'cells_with_terrain': int((tsrc > 0).sum()),
        'groups': list(ne_h3.GROUPS), 'structure_types': list(ne_h3.BLD_TYPES), 'building_types': list(BUILDING_TYPES),
        'lc25_legend': lc_legend, 'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'build_ms': int((time.time() - t0) * 1000),
    }
    return {'hdr': hdr, 'cols': cols}


# === SECTION: blob codec + disk store ===

def _col_spec(name: str, a: np.ndarray) -> dict:
    f = FIELDS[name]
    d = {'name': name, 'dtype': f['dtype'], 'bytes': int(a.nbytes), 'unit': f['unit']}
    if f.get('shape') == 'ragged':
        d['shape'] = 'ragged'; d['counts'] = RAGGED[name]; d['sorted'] = 'ascending within row'
    elif f.get('shape'):
        d['shape'] = f['shape']
    if f.get('nodata') is not None:
        d['nodata'] = f['nodata']
    return d


def with_ragged_counts(fields) -> list[str]:
    """Field list with every ragged column's count column inserted right before it."""
    out = []
    for f in fields:
        c = RAGGED.get(f)
        if c and c not in out:
            out.append(c)
        if f not in out:
            out.append(f)
    return out


def encode(hdr: dict, cols: 'OrderedDict[str, np.ndarray]', fields: list[str] | None = None, ids_enc: str = 'delta_u64') -> bytes:
    names = ['id'] + [f for f in with_ragged_counts(fields or list(cols)) if f != 'id' and f in cols]
    parts = []; specs = []
    for nm in names:
        a = cols[nm]
        if nm == 'id' and ids_enc == 'delta_u64':
            a = np.diff(a.astype(np.int64), prepend=0).astype('<u8')
        a = np.ascontiguousarray(a)
        specs.append(_col_spec(nm, a)); parts.append(a.tobytes())
    h = dict(hdr); h['ids'] = ids_enc; h['columns'] = specs; h['fields'] = names[1:]
    hb = json.dumps(h, separators=(',', ':'), ensure_ascii=False).encode()
    return MAGIC + struct.pack('<I', len(hb)) + hb + b''.join(parts)


def decode(blob: bytes) -> tuple[dict, 'OrderedDict[str, np.ndarray]']:
    if blob[:4] != MAGIC:
        raise ValueError('not a NEX1 blob')
    ln = struct.unpack('<I', blob[4:8])[0]
    hdr = json.loads(blob[8:8 + ln])
    off = 8 + ln
    cols: 'OrderedDict[str, np.ndarray]' = OrderedDict()
    n = int(hdr['n'])
    for c in hdr['columns']:
        nb = int(c['bytes'])
        a = np.frombuffer(blob[off:off + nb], NP_DT[c['dtype']])
        if c.get('shape') and c['shape'] != 'ragged':
            a = a.reshape((n, *c['shape']))
        if c['name'] == 'id' and hdr.get('ids') == 'delta_u64':
            a = np.cumsum(a.astype(np.int64)).astype('<u8')
        cols[c['name']] = a
        off += nb
    return hdr, cols


def _path(i: int, j: int) -> Path:
    return BIN_DIR / f'{i}_{j}.nex'


def _lock_for(key):
    with _build_locks_guard:
        lk = _build_locks.get(key)
        if lk is None:
            lk = _build_locks[key] = threading.Lock()
        return lk


_stored_gen_cache: dict = {}
_stored_gen_guard = threading.Lock()


def stored_gen(i: int, j: int) -> str | None:
    """Generation of the blob on disk without inflating it (first ≤ 16 KiB →
    header only), memoised on (path, mtime, size).  None when absent / unreadable."""
    p = _path(i, j)
    try:
        st = p.stat()
    except OSError:
        return None
    key = (st.st_mtime_ns, st.st_size)
    with _stored_gen_guard:
        hit = _stored_gen_cache.get(p.name)
        if hit is not None and hit[0] == key:
            return hit[1]
    gen = None
    try:
        with open(p, 'rb') as fh:
            head = fh.read(16384)
        d = zlib.decompressobj()
        raw = d.decompress(head, 8)
        if raw[:4] == MAGIC:
            ln = struct.unpack('<I', raw[4:8])[0]
            raw += d.decompress(d.unconsumed_tail, ln) if d.unconsumed_tail else b''
            if len(raw) < 8 + ln:          # header longer than the first 16 KiB inflate → full read
                raw = zlib.decompress(p.read_bytes())
            h = json.loads(raw[8:8 + ln])
            gen = h.get('gen') if h.get('algo') == ALGO else None   # older algo → 'missing' → rebuilt first by prewarm
    except Exception as e:  # noqa: BLE001
        log.debug('cells_bins: stored_gen %s: %s', p, e)
    with _stored_gen_guard:
        if len(_stored_gen_cache) > 40_000:
            _stored_gen_cache.clear()
        _stored_gen_cache[p.name] = (key, gen)
    return gen


COMPAT_ALGOS = ('nex-1',)        # older on-disk blobs still served for field lists they contain


def load_bin(i: int, j: int, *, rebuild: bool = False, need=None) -> tuple[dict, 'OrderedDict[str, np.ndarray]', dict]:
    """Current blob for a bin: memory → disk (if its gen matches) → build.
    Returns (hdr, cols, inputs).  A blob of an older algo is accepted (its
    columns are byte-identical) unless a column in ``need`` is absent from it —
    then it is rebuilt in place (and the prewarm rebuilds all of them anyway)."""
    inp = bin_inputs(i, j)
    key = (i, j, inp['gen'])
    need = tuple(need or ())

    def ok(hdr, cols):
        return hdr.get('gen') == inp['gen'] and (hdr.get('algo') == ALGO or (hdr.get('algo') in COMPAT_ALGOS
                                                                          and all(f in cols for f in need)))
    hit = None if rebuild else _blob_lru.get(key)
    if hit is not None and ok(*hit):
        return hit[0], hit[1], inp
    with _lock_for((i, j)):
        hit = None if rebuild else _blob_lru.get(key)
        if hit is not None and ok(*hit):
            return hit[0], hit[1], inp
        p = _path(i, j)
        if not rebuild and p.exists():
            try:
                hdr, cols = decode(zlib.decompress(p.read_bytes()))
                if ok(hdr, cols):
                    _blob_lru.put(key, (hdr, cols))
                    return hdr, cols, inp
            except Exception as e:  # noqa: BLE001
                log.warning('cells_bins: unreadable %s: %s', p, e)
        b = build_bin(i, j, inp)
        hdr, cols = b['hdr'], b['cols']
        try:
            BIN_DIR.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix('.tmp')
            tmp.write_bytes(zlib.compress(encode(hdr, cols, None, 'delta_u64'), 6))
            os.replace(tmp, p)
            with _stored_gen_guard:
                _stored_gen_cache.pop(p.name, None)
        except Exception as e:  # noqa: BLE001
            log.warning('cells_bins: store %s: %s', p, e)
        _blob_lru.put(key, (hdr, cols))
        return hdr, cols, inp


# === SECTION: manifest (every bin that touches a published product) ===

_manifest_cache = {'key': None, 'v': None}
_manifest_lock = threading.Lock()


_EPS = 1e-9


def _bins_of_bbox(w, s, e, n) -> list[tuple[int, int]]:
    """Bins whose closed box touches the closed bbox (i·d ≤ e and (i+1)·d ≥ w)."""
    i0, i1 = int(np.ceil(w / BIN_DEG - 1 - _EPS)), int(np.floor(e / BIN_DEG + _EPS))
    j0, j1 = int(np.ceil(s / BIN_DEG - 1 - _EPS)), int(np.floor(n / BIN_DEG + _EPS))
    return [(i, j) for i in range(i0, i1 + 1) for j in range(j0, j1 + 1)]


_bbox_cache = {'t': 0.0, 'v': {}}


def _kg_bboxes() -> dict:
    """kg → (w, s, e, n) from the search index ``kg`` table, cached 30 s."""
    now = time.time()
    if now - _bbox_cache['t'] < 30:
        return _bbox_cache['v']
    c = ca._search_conn()
    v = {}
    if c is not None:
        # The r-tree envelope is the polygon-complete bbox revision 2 (∪ the
        # pinned r1 processing bbox) — the ``kg`` row keeps the r1 bbox for
        # the coverage oracle (search_index.RTREE_BBOX_REV).  Bins must use
        # the r-tree one, else the sliver strip a sweep unit observed (e.g.
        # 87104's southern 2 km) belongs to no bin: no rows, no grid25.
        try:
            rows = c.execute('SELECT k.kg_code, r.min_lon, r.min_lat, r.max_lon, r.max_lat FROM kg k '
                             'JOIN kg_rtree r ON r.id = k.rowid WHERE k.min_lon IS NOT NULL').fetchall()
            v = {r[0]: tuple(float(x) for x in r[1:]) for r in rows}
        except sqlite3.Error:
            v = {}
        if not v:
            try:
                rows = c.execute('SELECT kg_code, min_lon, min_lat, max_lon, max_lat FROM kg WHERE min_lon IS NOT NULL').fetchall()
                v = {r[0]: tuple(float(x) for x in r[1:]) for r in rows}
            except sqlite3.Error:
                v = {}
    _bbox_cache.update(t=now, v=v)
    return v


def manifest() -> dict:
    """{bin_deg, generation, bins: {i:[], j:[], gen:[], epoch:[], complete:[], kgs_n:[]}, stored:[...]}
    — one row per bin that intersects ≥ 1 published cells product.  Cached on
    the cells generation + store stamp."""
    files = ca._files()
    bb = _kg_bboxes()
    stamps = _store_stamps({k.split('-')[0] for k in set(files) | set(bb)})
    key = (ca.cells_generation(), hashlib.sha256(json.dumps(stamps).encode()).hexdigest()[:16], _green_fix.FIX_ID, ALGO)
    with _manifest_lock:
        if _manifest_cache['key'] == key:
            return _manifest_cache['v']
        t0 = time.time()
        stamp_of = dict(stamps)
        per_bin: dict = {}; missing_in: dict = {}
        for ij, kgs in bin_index().items():
            on = [k for k in kgs if k in files]
            if on:
                per_bin[ij] = on; missing_in[ij] = [k for k in kgs if k not in files]
        stored = {}
        if BIN_DIR.exists():
            for p in BIN_DIR.glob('*.nex'):
                try:
                    a, b_ = p.stem.split('_'); stored[(int(a), int(b_))] = int(p.stat().st_mtime)
                except ValueError:
                    pass
        I = []; J = []; G = []; EP = []; C = []; KN = []; ST = []
        for (i, j) in sorted(per_bin):
            on = sorted(per_bin[(i, j)]); miss = sorted(missing_in.get((i, j), []))
            inter = sorted(on + miss)
            parts = [(k, str(files[k].get('neo1_digest')), int(files[k].get('partial') or 0)) for k in on]
            st = [(p_, stamp_of.get(p_, '')) for p_ in {k.split('-')[0] for k in inter}]
            eps = sorted({str(files[k].get('epoch')) for k in on if files[k].get('epoch')})
            I.append(i); J.append(j); G.append(_gen(i, j, parts, st)); EP.append(eps[-1] if eps else None)
            C.append(int(not miss and not any(p for _, _, p in parts))); KN.append(len(on)); ST.append(int((i, j) in stored))
        v = {'bin_deg': BIN_DEG, 'algo': ALGO, 'n': len(I),
             'generation': hashlib.sha256('|'.join(G).encode()).hexdigest()[:16],
             'bins': {'i': I, 'j': J, 'gen': G, 'epoch': EP, 'complete': C, 'kgs_n': KN, 'stored': ST},
             'bbox_of': 'w = i·bin_deg, s = j·bin_deg, e = w + bin_deg, n = s + bin_deg (half-open)',
             'computed_ms': int((time.time() - t0) * 1000)}
        _manifest_cache.update(key=key, v=v)
        return v


# === SECTION: request-path helpers (used by cells_api) ===

def parse_fields(raw: str | None) -> list[str]:
    if not raw or raw.strip() in ('*', 'all'):
        return [f for f in FIELDS if f != 'id'] if raw and raw.strip() in ('*', 'all') else list(DEFAULT_FIELDS)
    fs = [x.strip() for x in raw.split(',') if x.strip()]
    bad = [f for f in fs if f not in FIELDS]
    if bad:
        raise ca.CellsError(400, f'unknown bin fields {bad}', fields=[f for f in FIELDS if f != 'id'])
    return with_ragged_counts([f for f in fs if f != 'id'])


def _kgs_of_bin_sorted(inp):
    return inp['kgs']


def serve(i: int, j: int, fields: list[str], ids_enc: str, *, rebuild: bool = False) -> tuple[bytes, dict, str]:
    """→ (body, headers, etag) for a bin request.  Only the ETag check needs
    the generation; the blob is read lazily (304 never touches it)."""
    hdr, cols, inp = load_bin(i, j, rebuild=rebuild, need=fields)
    body = encode(hdr, cols, fields, ids_enc)
    etag = bin_etag(hdr['gen'], fields, ids_enc)
    hx = {'X-Cells-Gen': hdr['gen'], 'X-Cells-Bin': f'{i},{j}',
          'X-Cells-Record': f'nex1/{ALGO}', 'X-Cells-Kgs': ','.join(hdr.get('kgs') or []),
          'X-Cells-Fields': ','.join(fields), 'X-Cells-Layout': f'nex1={hdr["n"]}'}
    hx = ca._with_epoch(hx, hdr.get('epoch'))  # empty bin → corpus epoch pin, never 'None'
    return body, hx, etag


def quick_etag(i: int, j: int, fields: list[str], ids_enc: str) -> tuple[str, dict]:
    inp = bin_inputs(i, j)
    return bin_etag(inp['gen'], fields, ids_enc), inp


def quick_block_etag(bi: int, bj: int, fields: list[str], ids_enc: str) -> tuple[str, list[dict]]:
    inps = [bin_inputs(i, j) for i, j in block_bins(bi, bj)]
    return block_etag(block_gen([x['gen'] for x in inps]), fields, ids_enc), inps


def serve_block(bi: int, bj: int, fields: list[str], ids_enc: str, *, rebuild: bool = False) -> tuple[bytes, dict, str, list[dict]]:
    """``block=1``: the 16 bins of a 0.1° block in one body.

    Container ``NEXB`` (little-endian)::

        b'NEXB' | u32 header_len | header JSON | frame_0 | frame_1 | … | frame_15

    ``frame_k`` is **byte-identical** to the single-bin ``NEX1`` answer of bin
    ``header.bins[k]`` for the same ``fields`` / ``ids`` encoding; ``header.bins[k]
    = {i, j, off, len, gen, n, complete}`` with ``off`` relative to the end of
    the header (``8 + header_len``), bins in row-major order
    (:func:`block_bins`).  ``header.gen`` = sha256 of the 16 bin generations.
    → (body, headers, etag, inputs)."""
    frames = []; entries = []; gens = []; kgs = set(); epochs = []; partial = False
    off = 0
    for i, j in block_bins(bi, bj):
        hdr, cols, inp = load_bin(i, j, rebuild=rebuild, need=fields)
        fb = encode(hdr, cols, fields, ids_enc)
        entries.append({'i': i, 'j': j, 'off': off, 'len': len(fb), 'gen': hdr['gen'], 'n': int(hdr['n']),
                        'complete': bool(hdr.get('complete'))})
        frames.append(fb); gens.append(hdr['gen']); kgs.update(hdr.get('kgs') or [])
        if hdr.get('epoch'):
            epochs.append(str(hdr['epoch']))
        partial = partial or bool(inp['missing'] or inp['partial_files'])
        off += len(fb)
    gen = block_gen(gens)
    w, s, e, n = block_bbox(bi, bj)
    h = {'algo': ALGO, 'container': 'nexb', 'block_deg': BLOCK_DEG, 'bin_deg': BIN_DEG, 'ij': [bi, bj],
         'bbox': [w, s, e, n], 'gen': gen, 'ids': ids_enc, 'fields': list(fields), 'order': 'row-major: j south→north, i west→east',
         'bins': entries, 'n': sum(x['n'] for x in entries), 'cells_total': sum(x['n'] for x in entries)}
    hb = json.dumps(h, separators=(',', ':'), ensure_ascii=False).encode()
    body = BLOCK_MAGIC + struct.pack('<I', len(hb)) + hb + b''.join(frames)
    etag = block_etag(gen, fields, ids_enc)
    hx = {'X-Cells-Gen': gen, 'X-Cells-Block': f'{bi},{bj}', 'X-Cells-Record': f'nexb/{ALGO}',
          'X-Cells-Kgs': ','.join(sorted(kgs)), 'X-Cells-Fields': ','.join(fields),
          'X-Cells-Layout': 'nexb=' + ','.join(str(x['n']) for x in entries)}
    hx = ca._with_epoch(hx, max(epochs) if epochs else None)
    if partial:
        hx['X-Cells-Partial'] = '1'
    return body, hx, etag, entries


def decode_block(blob: bytes) -> tuple[dict, list[bytes]]:
    """NEXB → (header, [16 NEX1 frames])."""
    if blob[:4] != BLOCK_MAGIC:
        raise ValueError('not a NEXB blob')
    ln = struct.unpack('<I', blob[4:8])[0]
    hdr = json.loads(blob[8:8 + ln])
    base = 8 + ln
    return hdr, [blob[base + e['off']:base + e['off'] + e['len']] for e in hdr['bins']]


def dict_block() -> dict:
    return {
        'what': 'columnar per-cell export of a 0.025°-aligned bin — every local product joined (cells .h3b rows, '
                'tree apices, man-made objects, 25 m DTM/landcover grid, product roof hints)',
        'request': 'GET /api/v1/cells?bbox=w,s,e,n&format=bin&fields=a,b,c[&ids=u64][&rebuild=1] with w=i·0.025, s=j·0.025, '
                   'e=w+0.025, n=s+0.025 (half-open); fields=* for everything; omitted → defaults',
        'block': {'note': 'NEXB v1 (frames with ids) — since 2026-11-03 only with ids=1 (or ids=u64); the default block=1 '
                          'answer is the id-free zstd NEXB v2, see /cells/dict → blocks_v2',
                  'request': 'GET /api/v1/cells?bbox=w,s,e,n&format=bin&block=1&ids=1&fields=… with w=bi·0.1, s=bj·0.1, e=w+0.1, n=s+0.1 '
                             '— the 16 bins of the 0.1° block in one body',
                  'container': "'NEXB' | u32 header_len | header JSON {gen, ij:[bi,bj], bbox, fields, ids, bins:[{i,j,off,len,gen,n,complete}]} "
                               '| frame_0 … frame_15; frame_k is byte-identical to the single-bin NEX1 answer for bins[k] '
                               '(off relative to the end of the header = 8 + header_len)',
                  'order': 'row-major: k = (j - 4·bj)·4 + (i - 4·bi) — rows = latitude bands south→north, west→east within a row',
                  'etag': 'strong: "B-<sha256 of the 16 bin gens>-<fields hash>"; X-Cells-Gen = that block gen; '
                          'per-bin gens in the header let you keep per-bin state',
                  'block_deg': BLOCK_DEG, 'bins_per_block': BLOCK_N * BLOCK_N},
        'precompute': 'every bin touching a published product is built/refreshed on disk by the cells-bins builder '
                      '(systemd cells-bins.timer, hourly), then the NEXB v2 blocks and the national sample; cold reads are disk → < 100 ms',
        'container': "'NEX1' | u32 header_len | header JSON {n, gen, epoch, kgs[], columns[{name,dtype,bytes,shape?,nodata?,unit}], "
                     'ids: delta_u64|u64, complete, lc25_legend, …} | column bytes in header order (little-endian, rows sorted by cell id)',
        'headers': {'ETag': 'strong: "b-<gen>-<fields hash>" — If-None-Match → 304',
                    'X-Cells-Gen': 'the bin generation; changes iff any contributing product (cells row digest, 25 m grid, roof hints) '
                                   'changed — KEY YOUR CACHE ON THIS',
                    'X-Cells-Epoch': 'newest product epoch (YYYY-MM) among the contributing cells products — informational; '
                                     'products within an epoch still get added/re-ingested, so do not key on it',
                    'X-Cells-Partial': '1 when a KG in the bin has no published cells yet (header.kgs_missing) or is partial'},
        'discovery': 'GET /api/v1/cells/bins → every bin touching a published product with its current gen (columns i,j,gen,'
                     'epoch,complete,kgs_n,stored); diff gen against your copy, pull only changed bins (≤ 4 in flight)',
        'defaults': list(DEFAULT_FIELDS), 'bin_deg': BIN_DEG, 'max_cells': MAX_BIN_CELLS,
        'fields': {k: {kk: vv for kk, vv in v.items() if kk != 'src'} | {'source': v['src']} for k, v in FIELDS.items()},
        'semantics': {
            'u8 half-metre columns': '0 = none in this (observed) cell, 1..254 = value ×0.5 m, 255 = unknown (cell never observed)',
            'terrain': 'elev_mean/slope_mean/slope_p90/aspect_deg are filled for every cell under a product raster: '
                       'from the observation row (5 m DTM) when there is one, else from the 25 m grid (terrain_src tells which)',
            'aspect_deg': 'u8 ×2°, downslope direction, 0 = N, 45 = E (90°); 255 flat (< 1° on the 25 m DTM), 254 unknown',
            'buildings': 'bld_* = objects of type roof / greenhouse / solar_panel (header.building_types); struct_h_max = any type; '
                         'stories_max uses the product rule max(1, round(h_max / 3 m))',
            'roof_dom': 'only where the product carries a roof hint (new_buildings features); 0 elsewhere — treat as unknown, not flat',
            'tree_h': 'RAGGED apex-height lists (nex-2, 2026-11-03): tree_h_n (u16, exact count, same population as the trees layer: '
                      'every apex h ≥ 3 m) then tree_h (u8 ×0.5 m, 1..254) — all cells\' lists back to back in row order, each sorted '
                      'ascending; off = [0, cumsum(tree_h_n)], cell k = tree_h[off[k]:off[k+1]]; count in [a,b) = two searchsorted per cell. '
                      'Only materialised when tree_h is in fields= (tree_h_n is auto-inserted before it); in NEXB v2 the rows of the 16 bins '
                      'concatenate like every other column. Counts are sent instead of offsets because they zstd 3× smaller',
            'grid25': 'lc25 / lc25_frac / canopy25 / woody25 sample the 25 m product grid at the cell centre — available for '
                      'cells without an observation row too',
        },
    }


# === SECTION: CLI ===

STATE_PATH = BIN_DIR / 'builder.json'


def builder_status() -> dict:
    """Last builder run (``data/ne/bins/builder.json``) + what is still missing/stale now."""
    st = {}
    try:
        st = json.loads(STATE_PATH.read_text())
    except Exception:  # noqa: BLE001
        pass
    m = manifest(); b = m['bins']
    missing = stale = 0
    for i, j, g in zip(b['i'], b['j'], b['gen']):
        sg = stored_gen(i, j)
        if sg is None:
            missing += 1
        elif sg != g:
            stale += 1
    return {'bins': m['n'], 'stored_current': m['n'] - missing - stale, 'missing': missing, 'stale': stale,
            'complete': missing == 0 and stale == 0, 'generation': m['generation'], 'last_run': st or None}


def prewarm(limit: int | None = None, verbose: bool = True, budget_s: float | None = None, jobs: int = 1,
            blocks: bool = True) -> dict:
    """Build / refresh every manifest bin whose stored generation is not current.
    Missing bins first (cold-start coverage), then stale ones; stops at
    ``limit`` builds or ``budget_s`` seconds (the daily ``cells-bins.timer``
    runs with a 30 min budget until all of Austria is on disk).  The on-disk
    generation is read from the blob header only (:func:`stored_gen`), so a
    pass over an all-current corpus costs ~2 s."""
    from concurrent.futures import ThreadPoolExecutor
    m = manifest(); b = m['bins']
    t0 = time.time()
    todo_missing = []; todo_stale = []; n_ok = 0
    for i, j, g in zip(b['i'], b['j'], b['gen']):
        sg = stored_gen(i, j)
        if sg == g:
            n_ok += 1
        elif sg is None:
            todo_missing.append((i, j))
        else:
            todo_stale.append((i, j))
    todo = todo_missing + todo_stale
    if limit:
        todo = todo[:limit]
    n_built = 0; n_err = 0; stop = False
    lock = threading.Lock()

    def one(ij):
        nonlocal n_built, n_err, stop
        if stop or (budget_s and time.time() - t0 > budget_s):
            stop = True
            return
        try:
            load_bin(*ij, rebuild=True)
            with lock:
                n_built += 1
                if verbose and n_built % 50 == 0:
                    print(f'  built {n_built}/{len(todo)} (+{n_ok} current) … {time.time() - t0:.0f}s', flush=True)
        except Exception as e:  # noqa: BLE001
            with lock:
                n_err += 1
            log.warning('cells_bins: prewarm %d_%d: %s', ij[0], ij[1], e)

    if jobs > 1:
        with ThreadPoolExecutor(max_workers=jobs) as ex:
            list(ex.map(one, todo))
    else:
        for ij in todo:
            one(ij)
            if stop:
                break
    out = {'bins': m['n'], 'built': n_built, 'errors': n_err, 'current_before': n_ok,
           'missing_before': len(todo_missing), 'stale_before': len(todo_stale),
           'remaining': len(todo_missing) + len(todo_stale) - n_built,
           'budget_s': budget_s, 'stopped_on_budget': bool(stop), 'seconds': round(time.time() - t0, 1),
           'finished_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'generation': m['generation']}
    # phase 2 + 3 (cells_blocks): prebuilt NEXB v2 blocks, then the national
    # sample — both derived from the bins just (re)built, same budget.
    if blocks and not stop:
        import cells_blocks
        left = None if budget_s is None else max(0.0, budget_s - (time.time() - t0))
        try:
            out['blocks'] = cells_blocks.prewarm_blocks(left, verbose=verbose, jobs=jobs)
            stop = bool(out['blocks'].get('stopped_on_budget'))
        except Exception as e:  # noqa: BLE001
            log.warning('cells_bins: prewarm blocks: %s', e); out['blocks'] = {'error': str(e)}
        if not stop:
            left = None if budget_s is None else max(0.0, budget_s - (time.time() - t0))
            try:
                out['sample'] = cells_blocks.build_sample(verbose=verbose, budget_s=left)
            except Exception as e:  # noqa: BLE001
                log.warning('cells_bins: prewarm sample: %s', e); out['sample'] = {'error': str(e)}
        out['seconds'] = round(time.time() - t0, 1)
        out['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    try:
        BIN_DIR.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(out, indent=1))
    except Exception as e:  # noqa: BLE001
        log.debug('cells_bins: builder state: %s', e)
    return out


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--bin', help='i,j  → build and print header')
    ap.add_argument('--bbox', help='w,s,e,n of an aligned bin')
    ap.add_argument('--block', help='bi,bj → build the 16 bins of a 0.1° block and print the NEXB header')
    ap.add_argument('--prewarm', action='store_true', help='build / refresh every bin that is missing or stale')
    ap.add_argument('--limit', type=int); ap.add_argument('--budget-s', type=float, help='stop after this many seconds')
    ap.add_argument('--jobs', type=int, default=1); ap.add_argument('--no-blocks', action='store_true', help='bins only')
    ap.add_argument('--manifest', action='store_true'); ap.add_argument('--status', action='store_true')
    ap.add_argument('--quiet', action='store_true')
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING if a.quiet else logging.INFO)
    if a.manifest:
        m = manifest(); print(json.dumps({k: v for k, v in m.items() if k != 'bins'}, indent=1)); print('stored', sum(m['bins']['stored']))
    elif a.status:
        print(json.dumps(builder_status(), indent=1))
    elif a.prewarm:
        print(json.dumps(prewarm(a.limit, verbose=not a.quiet, budget_s=a.budget_s, jobs=max(1, a.jobs), blocks=not a.no_blocks)))
    elif a.block:
        bi, bj = (int(x) for x in a.block.split(','))
        t = time.time(); body, hx, etag, _ = serve_block(bi, bj, list(DEFAULT_FIELDS), 'delta_u64')
        hdr, frames = decode_block(body)
        print(json.dumps({k: v for k, v in hdr.items() if k != 'bins'}, indent=1)); print('bins', [(e['i'], e['j'], e['n']) for e in hdr['bins']])
        print('bytes', len(body), 'ms', int((time.time() - t) * 1000), etag)
    else:
        if a.bbox:
            ij = bin_of(*[float(x) for x in a.bbox.split(',')])
        else:
            ij = tuple(int(x) for x in a.bin.split(','))
        t = time.time(); hdr, cols, _ = load_bin(*ij, rebuild=True)
        print(json.dumps(hdr, indent=1)); print('ms', int((time.time() - t) * 1000))
        import gzip
        for fs in (list(DEFAULT_FIELDS), [f for f in FIELDS if f != 'id']):
            body = encode(hdr, cols, fs); print(len(fs), 'fields raw', len(body), 'gz', len(gzip.compress(body, 6)))
