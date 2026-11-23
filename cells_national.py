"""cells_national — Austria-wide res-8 / res-9 H3 aggregate of the public
cells corpus (``GET /api/v1/cells/national?res=9&fields=…``).

Why: a cell tile layer renders zooms 9–11 (res 7–9 polygons) and an
all-Austria PMTiles archive from one in-RAM national table.  Those tiles carried no LiDAR columns because the per-cell
0.025° bins (``cells_bins``) only feed z ≥ 12.  This module reduces every
published ``data/ne/<kg>.h3b`` once per build to **one row per res-9
parent** (~1.05 M rows, ~20 MB gz) — per field the mean over the valid
res-12 children (verdict: counts per class) plus ``n_cells`` — and
rolls the same sums up to res 8.  Fetched once per epoch, not per tile.

Source = the observation rows + NET1 apices + NEB1 objects of the
``.h3b`` files only (no grid25 fallback, no roof hints — those are
bin-only).  Border cells present in several products are deduplicated
exactly like ``cells_api.gather`` (observed copy first, larger chunk,
lowest kg): chunks whose res-10 id occurs in one product only are
aggregated per KG, the rest in batches across products.

Wire container = ``NEX1`` (see ``cells_bins``): ``b'NEX1' | u32 hlen |
header JSON | column bytes``; header carries ``"res": 9`` (no ``ij`` /
``bbox``), ``n``, ``gen``, ``epoch``, ``fields[]``, ``columns[]``,
``ids: delta_u64``.  Units / dtypes / nodata are the bin ones (u8 ×0.5 m,
u8 degrees, u16 m a.s.l. …) so one decoder serves both.

Build = ``python3 cells_national.py --build`` (systemd ``cells-national.timer``,
daily; skipped when the stored generation is current).  Output
``data/ne/national/r{8,9}.nex.gz`` + ``meta.json``; the request path only
serves (and slices) those files.  Never build inside gunicorn.  The build is
incremental, resumable and parallel (section "incremental build"): only
units whose inputs changed are re-read; ``--status`` shows progress.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import struct
import threading
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np

import cells_api as ca
import cells_bins as cb
import ne_h3
import ne_green_fix as _green_fix

log = logging.getLogger('cells_national')

# === SECTION: constants / field table ===

ALGO = 'national-2'              # national-2 (2026-11-03): + tree_hh (apices per 5 m height band)
MAGIC = cb.MAGIC
NAT_DIR = ca.NE_DIR / 'national'
CACHE_DIR = NAT_DIR / 'cache'
RESES = (8, 9)
DEFAULT_RES = 9
BATCH_R10 = 2_000
CACHE_FILES = 24

U8_UNK, U16_UNK = cb.U8_UNK, cb.U16_UNK
U32_UNK = 0xFFFFFFFF
NP_DT = dict(cb.NP_DT, u32=np.dtype('<u4'))

# name → {dtype, unit, nodata, desc, agg, shape?}
#   agg: count | mean (over valid children) | sum | hist (counts per class)
FIELDS: 'OrderedDict[str, dict]' = OrderedDict()


def _f(name, dtype, unit, nodata, desc, agg, shape=None, src='neo1'):
    FIELDS[name] = {'dtype': dtype, 'unit': unit, 'nodata': nodata, 'desc': desc, 'agg': agg, 'src': src,
                    **({'shape': shape} if shape else {})}


_f('id', 'u64', 'H3 cell id (header.res)', None, 'sorted ascending; delta-coded unless ids=u64', 'id', src='h3')
_f('n_cells', 'u16', 'count', None, 'res-12 children with an observation row (deduplicated across products)', 'count')
_f('n_obs', 'u16', 'count', None, 'children actually observed (LiDAR under the cell); 0 = all rows are placeholders', 'count')
_f('verdict', 'u16', 'count per class (header.verdicts order)', None,
   'children per consistency class: consistent, forest_loss, forest_gain, sealed_new, structure_new, green_new, unknown',
   'hist', shape=[len(ca._CONSISTENCY_NAMES)])
# --- terrain (observation rows; 5 m DTM) ---------------------------------
_f('elev_mean', 'u16', 'm a.s.l.', U16_UNK, 'mean of the children\'s cell-mean elevation', 'mean')
_f('slope_mean', 'u8', 'deg', U8_UNK, 'mean of the children\'s cell-mean slope', 'mean')
_f('slope_p90', 'u8', 'deg', U8_UNK, 'mean of the children\'s p90 slope', 'mean')
_f('roughness', 'u8', 'm ×10', U8_UNK, 'mean DSM local std', 'mean')
_f('tri', 'u8', 'm ×10', U8_UNK, 'mean terrain ruggedness index', 'mean')
_f('tpi', 'i8', 'm ×4', -128, 'mean topographic position index', 'mean')
# --- heights / vegetation (observation rows) ------------------------------
_f('h_mean', 'u8', '×0.5 m', U8_UNK, 'mean of the children\'s mean nDSM height', 'mean')
_f('h_p90', 'u8', '×0.5 m', U8_UNK, 'mean of the children\'s p90 nDSM height', 'mean')
_f('h_max', 'u8', '×0.5 m', U8_UNK, 'mean of the children\'s max nDSM height', 'mean')
_f('tree_h_max', 'u8', '×0.5 m', U8_UNK, 'mean of the children\'s tallest apex', 'mean')
_f('canopy', 'u8', '/254', U8_UNK, 'mean crown-area fraction', 'mean')
_f('ndvi_mean', 'u8', 'v/254·2−1', U8_UNK, 'mean NDVI', 'mean')
_f('ndvi_amp', 'u8', '/254', U8_UNK, 'mean seasonal NDVI amplitude', 'mean')
_f('dh_m', 'i8', '×0.25 m', -128, 'mean nDSM change newest−oldest flight', 'mean')
_f('dtm_m', 'i8', '×0.25 m', -128, 'mean terrain change newest−oldest flight', 'mean')
_f('structures_cover', 'u8', '/255', U8_UNK, 'mean fraction under segmented structures (observed children)', 'mean')
_f('cover', 'u8', '/255 per group (header.groups order)', None, 'mean cover fraction per group over observed children',
   'mean', shape=[len(ne_h3.GROUPS)])
_f('tree_n', 'u32', 'count', None, 'summed tree apices h ≥ 3 m (per-cell count saturates at 255)', 'sum')
_f('structures_n', 'u32', 'count', None, 'summed segmented man-made objects (per-cell count saturates at 255)', 'sum')
_f('forest_loss_n', 'u16', 'count', None, 'children with a Hansen GFC loss year', 'sum')
# --- NEB1 objects ---------------------------------------------------------
_f('bld_n', 'u32', 'count', None, 'building-type objects (roof / greenhouse / solar_panel)', 'sum', src='neb1')
_f('bld_cells', 'u16', 'count', None, 'children holding ≥ 1 building-type object', 'sum', src='neb1')
_f('bld_h_max', 'u8', '×0.5 m', U8_UNK, 'mean over building-holding children of the tallest building (h_max); 0 = no building', 'mean', src='neb1')
_f('bld_h_p50', 'u8', '×0.5 m', U8_UNK, 'mean over building-holding children of the median robust building height', 'mean', src='neb1')
_f('bld_h_top', 'u8', '×0.5 m', U8_UNK, 'tallest building object anywhere in the cell (max, not mean)', 'max', src='neb1')
_f('bld_area_sum', 'u32', 'm²', None, 'summed segment area of the building objects', 'sum', src='neb1')
_f('struct_h_max', 'u8', '×0.5 m', U8_UNK, 'mean over structure-holding children of the tallest man-made object of any type', 'mean', src='neb1')
# --- NET1 apices ----------------------------------------------------------
_f('canopy_h_p90', 'u8', '×0.5 m', U8_UNK, 'mean over apex-holding children of the per-cell p90 apex height', 'mean', src='net1')
_f('canopy_h_p50', 'u8', '×0.5 m', U8_UNK, 'mean over apex-holding children of the per-cell median apex height', 'mean', src='net1')
_f('canopy_h_top', 'u8', '×0.5 m', U8_UNK, 'tallest apex anywhere in the cell (max)', 'max', src='net1')
_f('tree_hh', 'u16', 'count per 5 m band (header.tree_hh_bands)', None,
   'apices (h ≥ 3 m, every res-12 child, unsaturated) per height band: 0 = 3–5 m, 1 = 5–10 m, … k = 5k–5(k+1) m, 11 = 55 m+; '
   'sum = tree_n (exact)', 'hist', shape=[12], src='net1')

ALIASES = {'elev_m': 'elev_mean', 'slope': 'slope_mean', 'consistency': 'verdict', 'consistency_n': 'verdict'}
DEFAULT_FIELDS = ('n_cells', 'n_obs', 'verdict', 'elev_mean', 'slope_mean', 'h_max', 'canopy', 'bld_h_max',
                  'canopy_h_p90', 'cover')

# NEO1 struct field + nodata for every 'mean' row field (+ neo1 'sum' fields)
_ROW_SRC = {
    'elev_mean': ('el', 65535), 'slope_mean': ('sl', 255), 'slope_p90': ('s90', 255), 'roughness': ('ro', 255),
    'tri': ('tri', 255), 'tpi': ('tpi', -128), 'h_mean': ('hm', 255), 'h_p90': ('h90', 255), 'h_max': ('hx', 255),
    'tree_h_max': ('thx', 255), 'canopy': ('cf', 255), 'ndvi_mean': ('nm', 255), 'ndvi_amp': ('na', 255),
    'dh_m': ('dh', -128), 'dtm_m': ('dtm', -128),
}
_BLD_TYPE_IDX = [ne_h3.BLD_TYPES.index(t) for t in cb.BUILDING_TYPES if t in ne_h3.BLD_TYPES]


TREE_HH_BANDS = 12


def tree_hh_band(h_m: np.ndarray) -> np.ndarray:
    """5 m height band of an apex: 0 = 3–5 m, k = [5k, 5k+5) m for 1 ≤ k ≤ 10, 11 = 55 m+."""
    return np.where(h_m < 5.0, 0, np.clip(np.floor(h_m / 5.0), 1, TREE_HH_BANDS - 1)).astype(np.int64)


# === SECTION: H3 parent arithmetic ===

def parent_ids(ids: np.ndarray, res: int, child_res: int = ca.RES) -> np.ndarray:
    """Vectorised ``h3.cell_to_parent`` for ids all at ``child_res``: set the
    resolution nibble and fill the digits below ``res`` with 7."""
    a = ids.astype(np.uint64)
    a = (a & ~np.uint64(0xF << 52)) | np.uint64(res << 52)
    fill = 0
    for r in range(res + 1, child_res + 1):
        fill |= 7 << ((15 - r) * 3)
    return a | np.uint64(fill)


# === SECTION: accumulator ===

class Acc:
    """Partial per-parent sums appended per Table, reduced once at the end."""

    def __init__(self, res: int):
        self.res = res
        self.parts: list[dict] = []
        self.cells = 0
        self.stands = None           # tree_stands.StandAcc: res-10 tree sums from the same apices

    def add_table(self, t: ca.Table) -> None:
        n = len(t)
        if n == 0:
            return
        self.cells += n
        obs = t.obs
        par = parent_ids(t.ids, self.res)
        up, inv = np.unique(par, return_inverse=True)
        m = up.size
        out: dict = {'parents': up}

        def bc(mask, w=None):
            if w is None:
                return np.bincount(inv[mask], minlength=m).astype(np.int64)
            return np.bincount(inv[mask], weights=w[mask], minlength=m)

        observed = (obs['hm'] != 255) | (obs['cover'].sum(axis=1) > 0) | (obs['el'] != 65535)
        allm = np.ones(n, bool)
        out['n_cells'] = bc(allm)
        out['n_obs'] = bc(observed)
        vd = np.minimum(obs['vd'].astype(np.int64), len(ca._CONSISTENCY_NAMES) - 1)
        K = len(ca._CONSISTENCY_NAMES)
        out['verdict'] = np.bincount(inv * K + vd, minlength=m * K).reshape(m, K).astype(np.int64)
        for name, (sf, nd) in _ROW_SRC.items():
            v = obs[sf]
            ok = v != nd
            out[name + '.s'] = bc(ok, v.astype(np.float64)); out[name + '.c'] = bc(ok)
        sc = obs['sc'].astype(np.float64)
        out['structures_cover.s'] = bc(observed, sc); out['structures_cover.c'] = bc(observed)
        cov = obs['cover'].astype(np.float64)
        out['cover.s'] = np.stack([bc(observed, cov[:, g]) for g in range(cov.shape[1])], axis=1)
        out['cover.c'] = out['structures_cover.c']
        out['tree_n'] = bc(allm, obs['tn'].astype(np.float64)).astype(np.int64)
        out['structures_n'] = bc(allm, obs['sn'].astype(np.float64)).astype(np.int64)
        hl = obs['hl']
        out['forest_loss_n'] = bc((hl != 0) & (hl != 255))
        # --- NEB1 -----------------------------------------------------------
        zero_i = np.zeros(m, np.int64); zero_f = np.zeros(m, np.float64)
        out.update({'bld_n': zero_i, 'bld_cells': zero_i, 'bld_area_sum': zero_i,
                    'bld_h_max.s': zero_f, 'bld_h_max.c': zero_i, 'bld_h_p50.s': zero_f, 'bld_h_p50.c': zero_i,
                    'bld_h_top': zero_f, 'struct_h_max.s': zero_f, 'struct_h_max.c': zero_i,
                    'canopy_h_p90.s': zero_f, 'canopy_h_p90.c': zero_i, 'canopy_h_p50.s': zero_f,
                    'canopy_h_p50.c': zero_i, 'canopy_h_top': zero_f})
        ci, words = t.gather_structures()
        if words.size:
            d = ca.decode_structures(words)
            any_max, _, _, _ = cb._per_cell_stats(ci, d['h_max_m'], n)
            has = np.isfinite(any_max)
            out['struct_h_max.s'] = bc(has, np.nan_to_num(any_max)); out['struct_h_max.c'] = bc(has)
            is_b = np.isin(d['type'], _BLD_TYPE_IDX)
            if is_b.any():
                cbi = ci[is_b]
                bh_max, _, _, bld_n = cb._per_cell_stats(cbi, d['h_max_m'][is_b], n)
                _, bh_p50, _, _ = cb._per_cell_stats(cbi, d['h_robust_m'][is_b], n)
                has_b = bld_n > 0
                out['bld_n'] = bc(allm, bld_n.astype(np.float64)).astype(np.int64)
                out['bld_cells'] = bc(has_b)
                out['bld_area_sum'] = bc(allm, np.bincount(cbi, weights=d['area_m2'][is_b], minlength=n)).astype(np.int64)
                out['bld_h_max.s'] = bc(has_b, np.nan_to_num(bh_max)); out['bld_h_max.c'] = out['bld_cells']
                out['bld_h_p50.s'] = bc(has_b, np.nan_to_num(bh_p50)); out['bld_h_p50.c'] = out['bld_cells']
                top = np.zeros(m); np.maximum.at(top, inv[has_b], np.nan_to_num(bh_max)[has_b])
                out['bld_h_top'] = top
        # --- NET1 -----------------------------------------------------------
        ci, words = t.gather_trees()
        out['tree_hh'] = np.zeros((m, TREE_HH_BANDS), np.int64)
        if words.size:
            h = ca.decode_trees(words)['h_m']
            if self.stands is not None:
                self.stands.add(t, observed, ci, h)
            band = tree_hh_band(h)
            out['tree_hh'] = np.bincount(inv[ci] * TREE_HH_BANDS + band, minlength=m * TREE_HH_BANDS).reshape(m, TREE_HH_BANDS).astype(np.int64)
            hx, c50, c90, _ = cb._per_cell_stats(ci, h, n)
            has = np.isfinite(c90)
            out['canopy_h_p90.s'] = bc(has, np.nan_to_num(c90)); out['canopy_h_p90.c'] = bc(has)
            out['canopy_h_p50.s'] = bc(has, np.nan_to_num(c50)); out['canopy_h_p50.c'] = bc(has)
            top = np.zeros(m); np.maximum.at(top, inv[has], np.nan_to_num(hx)[has])
            out['canopy_h_top'] = top
        self.parts.append(out)

    def reduce(self) -> dict:
        """Concatenate the partials → one dict of per-parent arrays (sums/counts kept)."""
        if not self.parts:
            return {'parents': np.zeros(0, np.uint64)}
        keys = [k for k in self.parts[0] if k != 'parents']
        allp = np.concatenate([p['parents'] for p in self.parts])
        up, inv = np.unique(allp, return_inverse=True)
        m = up.size
        red = {'parents': up}
        for k in keys:
            a = np.concatenate([p[k] for p in self.parts])
            red[k] = _reduce_col(k, a, inv, m)
        self.parts = [{k: v for k, v in red.items()}]  # keep a single reduced partial
        return red


def _reduce_col(k: str, a: np.ndarray, inv: np.ndarray, m: int) -> np.ndarray:
    if k in ('bld_h_top', 'canopy_h_top'):
        out = np.zeros(m); np.maximum.at(out, inv, a); return out
    if a.ndim == 2:
        return np.stack([np.bincount(inv, weights=a[:, g], minlength=m) for g in range(a.shape[1])], axis=1)
    r = np.bincount(inv, weights=a.astype(np.float64), minlength=m)
    return r.astype(np.int64) if a.dtype.kind in 'iu' else r


def rollup(red: dict, res_from: int, res_to: int) -> dict:
    """Reduced res-``res_from`` sums → res-``res_to`` sums (parents of the parents)."""
    par = parent_ids(red['parents'], res_to, child_res=res_from)
    up, inv = np.unique(par, return_inverse=True)
    out = {'parents': up}
    for k, a in red.items():
        if k != 'parents':
            out[k] = _reduce_col(k, a, inv, up.size)
    return out


# === SECTION: finalise (sums → quantised columns) + codec ===

def _q(mean: np.ndarray, valid: np.ndarray, dtype: str, nodata) -> np.ndarray:
    lo, hi = {'u8': (0, 254), 'i8': (-127, 127), 'u16': (0, 65534), 'u32': (0, U32_UNK - 1)}[dtype]
    v = np.clip(np.round(np.nan_to_num(mean)), lo, hi).astype(NP_DT[dtype])
    if nodata is not None:
        v = v.copy(); v[~valid] = nodata
    return v


def finalise(red: dict) -> 'OrderedDict[str, np.ndarray]':
    cols: 'OrderedDict[str, np.ndarray]' = OrderedDict()
    cols['id'] = red['parents'].astype('<u8')
    for name, f in FIELDS.items():
        if name == 'id':
            continue
        agg = f['agg']; dt = f['dtype']
        if agg in ('count', 'sum'):
            cols[name] = _q(red[name].astype(np.float64), np.ones(red[name].shape[0], bool), dt, None)
        elif agg == 'hist':
            cols[name] = np.clip(red[name], 0, 65535).astype(NP_DT[dt])
        elif agg == 'max':
            key = {'bld_h_top': 'bld_h_top', 'canopy_h_top': 'canopy_h_top'}[name]
            v = red[key]; valid = v > 0
            cols[name] = _q(v * 2.0, valid, dt, f['nodata'])    # metres → ×0.5 m
        else:  # mean
            s = red[name + '.s']; c = red[name + '.c']
            if s.ndim == 2:
                cc = np.maximum(c, 1)[:, None]
                cols[name] = np.clip(np.round(s / cc), 0, 255).astype(np.uint8)
                continue
            valid = c > 0
            mean = s / np.maximum(c, 1)
            if f['src'] in ('neb1', 'net1'):
                mean = mean * 2.0                                  # metres → ×0.5 m like the bins
            cols[name] = _q(mean, valid, dt, f['nodata'])
    return cols


def _col_spec(name: str, a: np.ndarray) -> dict:
    f = FIELDS[name]
    d = {'name': name, 'dtype': f['dtype'], 'bytes': int(a.nbytes), 'unit': f['unit'], 'agg': f['agg']}
    if f.get('shape'):
        d['shape'] = f['shape']
    if f.get('nodata') is not None:
        d['nodata'] = f['nodata']
    return d


def encode(hdr: dict, cols: 'OrderedDict[str, np.ndarray]', fields: list[str] | None = None, ids_enc: str = 'delta_u64') -> bytes:
    names = ['id'] + [f for f in (fields or list(cols)) if f != 'id' and f in cols]
    if fields is not None and 'n_cells' in cols and 'n_cells' not in names:
        names.insert(1, 'n_cells')   # always present in slices (weight for client-side rollups)
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
    hdr = json.loads(blob[8:8 + ln]); off = 8 + ln
    cols: 'OrderedDict[str, np.ndarray]' = OrderedDict(); n = int(hdr['n'])
    for c in hdr['columns']:
        nb = int(c['bytes'])
        a = np.frombuffer(blob[off:off + nb], NP_DT[c['dtype']])
        if c.get('shape'):
            a = a.reshape((n, *c['shape']))
        if c['name'] == 'id' and hdr.get('ids') == 'delta_u64':
            a = np.cumsum(a.astype(np.int64)).astype('<u8')
        cols[c['name']] = a; off += nb
    return hdr, cols


def header_for(res: int, n: int, gen: str, epoch, extra: dict | None = None) -> dict:
    h = {'algo': ALGO, 'res': res, 'n': int(n), 'gen': gen, 'epoch': epoch, 'child_res': ca.RES,
         'groups': list(ne_h3.GROUPS), 'verdicts': list(ca._CONSISTENCY_NAMES),
         'tree_hh_bands': ['3-5'] + [f'{5 * k}-{5 * k + 5}' for k in range(1, TREE_HH_BANDS - 1)] + ['55+'],
         'building_types': list(cb.BUILDING_TYPES), 'structure_types': list(ne_h3.BLD_TYPES),
         'semantics': 'per field the mean over the res-12 children where the field is non-null '
                      '(bld_*/canopy_h_*: children holding ≥ 1 object); verdict = counts per class; '
                      'n_cells = children with a row; border cells deduplicated across products',
         'units_as': '/api/v1/cells/dict → bins.fields (u8 ×0.5 m, u8 deg, u16 m a.s.l., i8 ×0.25 m …)',
         'attribution': ca.ATTRIBUTION, 'license': 'CC-BY-4.0', 'license_url': ca.LICENSE_URL,
         'source_service': 'srtm-lidar-at'}
    if extra:
        h.update(extra)
    return h


# === SECTION: build (CLI / timer) ===

def current_gen() -> str:
    """Corpus generation of the aggregate.  Folds the corpus-wide tree-QA token
    (``tree_qa.corpus_sig``: tree_n / tree_hh / tree_h_max count the default /trees/bbox
    apex set, 2026-11-22); hourly linear-noise churn of that token is absorbed by
    :func:`minor_drift`."""
    tq = ca._tree_qa.corpus_sig()
    h = hashlib.sha256(f'{ALGO}|{_green_fix.FIX_ID}|{ca.cells_generation()}|tq|{tq["id"]}|{tq["tok"]}'.encode())
    return h.hexdigest()[:24]


def _dup_r10s() -> np.ndarray:
    c = ca._idx_conn()
    rows = c.execute('SELECT r10 FROM chunk GROUP BY r10 HAVING COUNT(*) > 1').fetchall()
    return np.asarray([int(r[0]) for r in rows], dtype=np.uint64)


# A full build is ~50 min of one core.  The generation moves on *any*
# re-ingested file, and in the endgame that is 1–4 sliver-sweep merges a
# day (~100 cells of 276 M) — a daily rebuild for nothing visible in a
# res-9 mean.  Minor drift is therefore deferred: same ALGO / green-fix /
# file count, at most MINOR_FILES files touched since the last build and
# the corpus cell total within MINOR_CELLS_FRAC, for at most MAX_DEFER_D
# days since the last build.  ``--force`` always builds.
MINOR_FILES = int(os.environ.get('CELLS_NATIONAL_MINOR_FILES', '25'))
MINOR_CELLS_FRAC = float(os.environ.get('CELLS_NATIONAL_MINOR_CELLS_FRAC', '1e-4'))
MAX_DEFER_D = float(os.environ.get('CELLS_NATIONAL_MAX_DEFER_D', '7'))
# tree QA: same rule id and the hidden-apex total / filtered-KG count within this fraction of
# the last build's (hourly linear-noise runs move a few hundred of ~300 k apices)
MINOR_TQ_FRAC = float(os.environ.get('CELLS_NATIONAL_MINOR_TQ_FRAC', '0.02'))


def _corpus_sig(files: dict) -> dict:
    return {'files': len(files), 'cells': sum(int(f.get('cells_n') or 0) for f in files.values()),
            'partial': sum(1 for f in files.values() if f.get('partial'))}


def minor_drift(meta: dict | None, files: dict, now: float | None = None) -> dict | None:
    """→ ``{changed_files, cells_delta, age_d}`` when the corpus moved so
    little since *meta*'s build that the rebuild can wait, else None."""
    import calendar
    if not meta or meta.get('algo') != ALGO or meta.get('green_fix') != _green_fix.FIX_ID:
        return None
    try:
        built = calendar.timegm(time.strptime(meta['built_at'], '%Y-%m-%dT%H:%M:%SZ'))
    except Exception:  # noqa: BLE001
        return None
    now = time.time() if now is None else now
    age_d = (now - built) / 86400.0
    if age_d > MAX_DEFER_D or int(meta.get('kgs') or -1) != len(files):
        return None
    start = float(meta.get('build_started') or built - float(meta.get('build_s') or 0))
    changed = sum(1 for f in files.values() if float(f.get('mtime') or 0) > start)
    if changed > MINOR_FILES:
        return None
    tq = ca._tree_qa.corpus_sig(); tq0 = meta.get('tree_qa') or {}
    if tq0.get('id') != tq['id']:
        return None                      # never built with this tree-QA rule (or before the fold) → build
    for k in ('kgs', 'hidden'):
        if abs(int(tq[k]) - int(tq0.get(k) or 0)) > MINOR_TQ_FRAC * max(1, int(tq0.get(k) or 0)):
            return None
    sig = _corpus_sig(files); old = meta.get('corpus') or {}
    delta = None
    if old:
        if int(old.get('partial', -1)) != sig['partial']:
            return None
        delta = sig['cells'] - int(old.get('cells') or 0)
        if abs(delta) > MINOR_CELLS_FRAC * max(1, sig['cells']):
            return None
    return {'changed_files': changed, 'cells_delta': delta, 'age_d': round(age_d, 2),
            'tree_qa_hidden_delta': int(tq['hidden']) - int(tq0.get('hidden') or 0)}


# === SECTION: incremental build — units on disk, parallel workers, scatter merge ===
#
# One national pass is ~6.3 M chunk reads through the full read-time stack (green fix →
# position frames → ne_rebin → tree QA): hours of one core.  So the build is cut into
# **units**, each reduced to its partial sums once and kept on disk under
# ``data/ne/national/parts/``:
#
#   k<kg>   the chunks of one product file whose res-10 id no other product stores
#   g<hex>  the res-10 ids stored by several products inside one res-6 parent, deduplicated
#           across those products exactly like ``cells_api.gather``
#
# A unit carries a signature of everything its sums depend on (file digest, position
# frame, per-KG tree-QA token, its duplicate-chunk set, rule ids).  A build recomputes only
# the units whose signature moved — a handful per day — and then scatter-adds all partials
# into the national arrays.  Consequences: a killed build resumes where it stopped, the
# daily build is minutes, workers run in parallel (``CELLS_NATIONAL_JOBS``), memory is one
# unit per worker + the final arrays (no concatenate / re-reduce copies), and progress is a
# file (``progress.json``, ``--status``, ``national_build:`` on /process.txt).
# All partial sums are integers or exact binary fractions, so the result does not depend on
# how the corpus is cut.

PARTS_ALGO = 'p1'
PARTS_DIR = NAT_DIR / 'parts'
PROGRESS = NAT_DIR / 'progress.json'
GROUP_RES = 6                    # duplicate res-10 ids are batched per res-6 parent (<= ~750 ids)
JOBS = max(1, int(os.environ.get('CELLS_NATIONAL_JOBS', min(2, os.cpu_count() or 1))))
MAX_ROUNDS = 3                   # re-plan rounds when the corpus moves under a running build
_MAX_KEYS = ('bld_h_top', 'canopy_h_top')
_STAND_DT = ('<u8', '<u4', '<f4', '<f8', '<u4', '<f8', '<u4', '<u4')   # tree_stands.StandAcc part tuple
_STAND_MAX = (2, 7)


def _base_sig() -> str:
    import tree_stands
    return f'{PARTS_ALGO}|{ALGO}|{tree_stands.ALGO}|{_green_fix.FIX_ID}|{ca._tree_qa.THX_ID}|{ca.RES}'


def _kg_sig(kg: str, digest) -> str:
    """Everything ``cells_api._load_chunk`` reads for one product file, as a token."""
    return f'{digest}{ca._pos_fix.tag(kg)}|{ca._tree_qa.kg_tok(kg, digest)}'


def _sha(*parts) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(p if isinstance(p, (bytes, bytearray, memoryview)) else str(p).encode()); h.update(b'\0')
    return h.hexdigest()[:20]


def plan(files: dict | None = None) -> list[dict]:
    """Every unit of the current corpus with its signature and size (index only, no chunk read)."""
    from array import array
    files = ca._files() if files is None else files
    c = ca._idx_conn()
    base = _base_sig()
    kgs = sorted(files)
    kidx = {k: i for i, k in enumerate(kgs)}
    ksig = {k: _kg_sig(k, files[k].get('neo1_digest')) for k in kgs}
    n_chunks = dict(c.execute('SELECT kg, COUNT(*) FROM chunk GROUP BY kg'))
    r = array('Q'); ki = array('I')
    for r10, kg in c.execute('SELECT r10, kg FROM chunk WHERE r10 IN '
                             '(SELECT r10 FROM chunk GROUP BY r10 HAVING COUNT(*) > 1)'):
        if kg in kidx:
            r.append(int(r10)); ki.append(kidx[kg])
    r = np.frombuffer(r, np.uint64) if len(r) else np.zeros(0, np.uint64)
    ki = np.frombuffer(ki, np.uint32).astype(np.int64) if len(ki) else np.zeros(0, np.int64)
    units = []
    # per product: its unique chunks (signature folds WHICH of its chunks are duplicates)
    o = np.lexsort((r, ki)); rk, kk = r[o], ki[o]
    b = np.searchsorted(kk, np.arange(len(kgs) + 1))
    for i, kg in enumerate(kgs):
        d = np.ascontiguousarray(rk[b[i]:b[i + 1]])
        units.append({'id': f'k{kg}', 'kind': 'k', 'kg': kg, 'dup': d, 'kgsigs': {kg: ksig[kg]},
                      'chunks': max(0, int(n_chunks.get(kg, 0)) - d.size),
                      'sig': _sha(base, ksig[kg], d.tobytes())})
    # duplicated res-10 ids, per res-6 parent
    if r.size:
        g = parent_ids(r, GROUP_RES, child_res=10)
        o = np.lexsort((ki, r, g)); gg, rg, kg_ = g[o], r[o], ki[o]
        ug, st = np.unique(gg, return_index=True)
        en = np.append(st[1:], gg.size)
        for gid, s, e in zip(ug.tolist(), st.tolist(), en.tolist()):
            inv = sorted({kgs[j] for j in np.unique(kg_[s:e]).tolist()})
            sigs = {k: ksig[k] for k in inv}
            units.append({'id': f'g{gid:x}', 'kind': 'g', 'r10s': np.unique(rg[s:e]), 'kgsigs': sigs, 'chunks': e - s,
                          'sig': _sha(base, rg[s:e].tobytes(), kg_[s:e].tobytes(), json.dumps(sigs, sort_keys=True))})
    return units


def _pack(a: np.ndarray) -> np.ndarray:
    """Narrowest lossless integer dtype (sums of small integers stored as f64 / i64 compress ~10x)."""
    a = np.asarray(a)
    if a.size == 0 or a.dtype == np.uint64:
        return a
    if a.dtype.kind == 'f':
        if not (np.isfinite(a).all() and np.array_equal(a, np.rint(a)) and np.abs(a).max() < 2 ** 52):
            return a
        a = a.astype(np.int64)
    lo, hi = int(a.min()), int(a.max())
    for dt in (np.uint8, np.uint16, np.uint32) if lo >= 0 else (np.int8, np.int16, np.int32):
        if np.iinfo(dt).min <= lo and hi <= np.iinfo(dt).max:
            return a.astype(dt)
    return a


def _unit_path(uid: str, d: Path | None = None) -> Path:
    return (d or PARTS_DIR) / uid[1:3] / f'{uid}.npz'


def _save_unit(uid: str, sig: str, red: dict | None, stand: tuple | None, d: Path | None = None) -> None:
    out = {'sig': np.array(sig)}
    for k, v in (red or {}).items():
        out[f'{k}~{v.dtype.str}'] = _pack(v)
    for i, v in enumerate(stand or ()):
        out[f's{i}'] = _pack(v)
    p = _unit_path(uid, d); p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f'{p.stem}.tmp{os.getpid()}.npz')
    np.savez_compressed(tmp, **out); os.replace(tmp, p)


def _unit_sig(uid: str, d: Path | None = None) -> str | None:
    try:
        with np.load(_unit_path(uid, d)) as z:
            return str(z['sig'])
    except Exception:  # noqa: BLE001 - missing / torn file = not built
        return None


def _load_unit(uid: str, d: Path | None = None, what: str = 'all'):
    """→ (res-9 partial dict | None, stand part tuple | None); ``what='ids'`` → ids only."""
    with np.load(_unit_path(uid, d)) as z:
        names = z.files
        red = None; stand = None
        if 'parents~<u8' in names:
            if what == 'ids':
                red = {'parents': z['parents~<u8']}
            else:
                red = {}
                for nm in names:
                    if '~' in nm:
                        k, dt = nm.split('~')
                        red[k] = z[nm].astype(dt, copy=False)
        if 's0' in names:
            if what == 'ids':
                stand = (z['s0'],)
            else:
                stand = tuple(z[f's{i}'].astype(_STAND_DT[i], copy=False) for i in range(8))
    return red, stand


_worker_ready = False


def _worker_init() -> None:
    global _worker_ready
    if _worker_ready:
        return
    ca._rebin._lru.max_bytes = max(ca._rebin._lru.max_bytes, 48 << 20)   # a KG's chunks are assigned once
    _worker_ready = True


def compute_unit(u: dict, d: Path | None = None) -> dict:
    """Read one unit's chunks as served, reduce, store.  Runs in a worker process."""
    import tree_stands
    _worker_init()
    t0 = time.time()
    ca._tree_qa.sync()

    def live() -> bool:
        for kg, s in u['kgsigs'].items():
            h = ca.header(kg)
            if h is None or _kg_sig(kg, h.get('neo1_digest')) != s:
                return False
        return True

    ok = live()
    acc = Acc(9); acc.stands = tree_stands.StandAcc()
    chunks: list = []
    if u['kind'] == 'k':
        kg = u['kg']
        h = ca.header(kg, with_chunks=True)
        if h is not None:
            h = dict(h)
            h['_r10map'] = rmap = {}
            for row in h['chunks']:
                r10 = int(row['r10'], 16)
                rmap[r10] = {'r10': r10, 'kg': kg, 'off': int(row['off']), 'len': int(row['len']),
                             'raw_len': int(row.get('raw_len') or 0), 'cells': int(row['cells']),
                             'trees': int(row.get('trees') or 0), 'bld': int(row.get('bld') or 0)}
            dup = set(u['dup'].tolist())
            for r10, row in rmap.items():
                if r10 in dup:
                    continue
                try:
                    chunks.append(ca._load_chunk(kg, row, h))
                except Exception as e:  # noqa: BLE001
                    log.warning('national: chunk %s/%x unreadable: %s', kg, r10, e)
    else:
        chunks, _h = ca.chunks_for_r10(u['r10s'])
    if chunks:
        acc.add_table(ca.gather(chunks, None))
    red = acc.parts[0] if acc.parts else None
    stand = acc.stands.parts[0] if acc.stands.parts else None
    ok = ok and live()
    _save_unit(u['id'], u['sig'] if ok else 'stale', red, stand, d)
    return {'id': u['id'], 'ok': ok, 'chunks': len(chunks), 'cells': acc.cells, 'planned': u['chunks'],
            's': round(time.time() - t0, 2)}


def _rss_mb() -> int:
    """This process + its children (the workers), MB."""
    tot = 0
    try:
        me = os.getpid()
        for p in os.listdir('/proc'):
            if not p.isdigit():
                continue
            try:
                st = open(f'/proc/{p}/stat').read().rsplit(')', 1)[1].split()
                if int(p) == me or int(st[1]) == me:
                    tot += int(st[21]) * os.sysconf('SC_PAGE_SIZE')
            except (OSError, IndexError, ValueError):
                pass
    except OSError:
        pass
    return tot >> 20


class Progress:
    """``progress.json`` (atomic, every few seconds) + one journal line per LOG_S."""
    LOG_S = 300.0

    def __init__(self, path: Path | None = None, verbose: bool = True):
        self.path = path or PROGRESS; self.verbose = verbose
        self.d = {'state': 'plan', 'pid': os.getpid(), 'started_at': _iso_now(), 'started': round(time.time(), 1), 'jobs': JOBS}
        self._t = 0.0; self._tl = time.time(); self._tp = 0.0

    def set(self, force: bool = False, **kw) -> None:
        self.d.update(kw)
        now = time.time()
        if not force and now - self._t < 5:
            return
        self._t = now
        d = self.d
        d['updated'] = round(now, 1); d['elapsed_s'] = round(now - d['started'])
        if d.get('chunks_todo'):
            run_s = max(1e-6, now - d.get('units_started', now))
            d['chunks_per_s'] = round(d.get('chunks_done', 0) / run_s, 1)
            left = d['chunks_todo'] - d.get('chunks_done', 0)
            d['eta_s'] = round(left / d['chunks_per_s']) if d['chunks_per_s'] > 0 and d['state'] == 'units' else None
        d['rss_mb'] = _rss_mb()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix('.tmp'); tmp.write_text(json.dumps(d, indent=1)); os.replace(tmp, self.path)
        except OSError:
            pass
        if force or now - self._tl >= self.LOG_S:
            self._tl = now
            log.info('national: %s', progress_line(d))
        if self.verbose and (force or now - self._tp >= 30):
            self._tp = now
            print('  ' + progress_line(d), flush=True)


def _iso_now() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def _hms(s) -> str:
    if s is None:
        return '-'
    s = int(s)
    return f'{s // 3600}h{s % 3600 // 60:02d}m' if s >= 3600 else f'{s // 60}m{s % 60:02d}s'


def progress_line(d: dict | None) -> str:
    if not d:
        return 'no build recorded'
    s = f"{d.get('state')}"
    if d.get('units_total') is not None:
        s += (f" units {d.get('units_done', 0)}/{d.get('units_todo', 0)} to build ({d.get('units_cached', 0)} of "
              f"{d['units_total']} reused), chunks {d.get('chunks_done', 0)}/{d.get('chunks_todo', 0)}")
        if d.get('chunks_todo'):
            s += f" ({100.0 * d.get('chunks_done', 0) / d['chunks_todo']:.1f}%) @{d.get('chunks_per_s', 0)}/s eta {_hms(d.get('eta_s'))}"
    if d.get('merge_done') is not None:
        s += f", merge {d['merge_done']}/{d.get('merge_total')}"
    s += f", elapsed {_hms(d.get('elapsed_s'))}, jobs {d.get('jobs')}, rss {d.get('rss_mb', 0)} MB"
    if d.get('round', 1) > 1:
        s += f", round {d['round']}"
    if d.get('error'):
        s += f", error: {d['error']}"
    return s


def read_progress() -> dict | None:
    try:
        d = json.loads(PROGRESS.read_text())
    except (OSError, ValueError):
        return None
    if d.get('state') not in ('done', 'skipped', 'failed', 'deferred'):
        try:
            os.kill(int(d.get('pid') or 0), 0)
        except (OSError, ValueError):
            d['state'] = f"dead (was {d.get('state')})"
        except PermissionError:
            pass
    return d


def text_line() -> str:
    """``national_build:`` line of /process.txt."""
    m = read_meta() or {}
    p = read_progress()
    return (f"national_build: serving gen {str(m.get('gen'))[:8]} built {m.get('built_at', '-')} "
            f"({(m.get('rows') or {}).get('9', 0)} res-9 rows, {(m.get('stands') or {}).get('hexes', 0)} stands, "
            f"{m.get('units_built', '-')} of {m.get('units', '-')} units rebuilt in {_hms(m.get('build_s'))}); last run: {progress_line(p)}")


def run_units(todo: list[dict], prog: Progress | None = None, jobs: int | None = None, d: Path | None = None) -> dict:
    """Compute + store *todo* (largest first, so the tail is short); → {'done', 'stale', 'chunks', 'cells'}."""
    jobs = JOBS if jobs is None else jobs
    todo = sorted(todo, key=lambda u: -u['chunks'])
    st = {'done': 0, 'stale': 0, 'chunks': 0, 'cells': 0, 'planned': 0}

    def note(r):
        st['done'] += 1; st['stale'] += 0 if r['ok'] else 1; st['chunks'] += r['chunks']; st['cells'] += r['cells']
        st['planned'] += r['planned']
        if prog is not None:
            prog.set(units_done=st['done'], chunks_done=st['planned'], cells_read=st['cells'], last_unit=r['id'])

    if jobs <= 1 or len(todo) < 4:
        for u in todo:
            note(compute_unit(u, d))
        return st
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor, FIRST_COMPLETED, wait
    # spawn: no forked sqlite handles / LRUs; each worker loads its own read-time snapshots once
    with ProcessPoolExecutor(max_workers=jobs, mp_context=mp.get_context('spawn'), initializer=_worker_init) as ex:
        it = iter(todo); pend = set()
        while True:
            while len(pend) < jobs * 4:                  # bounded backlog: no 10 k pickled futures
                u = next(it, None)
                if u is None:
                    break
                pend.add(ex.submit(compute_unit, u, d))
            if not pend:
                break
            fin, pend = wait(pend, return_when=FIRST_COMPLETED)
            for f in fin:
                note(f.result())
    return st


def merge_units(units: list[dict], prog: Progress | None = None, d: Path | None = None):
    """Scatter-add every unit's partial into the national arrays → generator of
    ('red9', dict) then ('stands', tuple | None).  Two sweeps so the res-9 arrays are freed
    (by the caller) before the stand arrays are allocated."""
    ids = [u['id'] for u in units]
    n = len(ids)
    P = []; S = []
    for i, uid in enumerate(ids):
        red, stand = _load_unit(uid, d, 'ids')
        if red is not None:
            P.append(red['parents'])
        if stand is not None:
            S.append(stand[0].astype(np.uint64, copy=False))
        if prog is not None:
            prog.set(merge_done=i, merge_total=3 * n)
    up = np.unique(np.concatenate(P)) if P else np.zeros(0, np.uint64)
    us = np.unique(np.concatenate(S)) if S else np.zeros(0, np.uint64)
    del P, S
    out: dict = {'parents': up}
    for i, uid in enumerate(ids):
        red, _ = _load_unit(uid, d)
        if red is not None:
            idx = np.searchsorted(up, red['parents'])
            for k, a in red.items():
                if k == 'parents':
                    continue
                dst = out.get(k)
                if dst is None:
                    dst = out[k] = np.zeros((up.size, *a.shape[1:]), a.dtype)
                if k in _MAX_KEYS:
                    dst[idx] = np.maximum(dst[idx], a)
                else:
                    dst[idx] += a                        # parents are unique inside one unit
        if prog is not None:
            prog.set(merge_done=n + i, merge_total=3 * n)
    yield 'red9', out
    del out
    so = None
    if us.size:
        so = [us] + [None] * 7
        for i, uid in enumerate(ids):
            _, stand = _load_unit(uid, d)
            if stand is not None:
                idx = np.searchsorted(us, stand[0])
                for j in range(1, 8):
                    a = stand[j]
                    if so[j] is None:
                        so[j] = np.zeros((us.size, *a.shape[1:]), a.dtype)
                    if j in _STAND_MAX:
                        so[j][idx] = np.maximum(so[j][idx], a)
                    else:
                        so[j][idx] += a
            if prog is not None:
                prog.set(merge_done=2 * n + i, merge_total=3 * n)
    yield 'stands', (tuple(so) if so is not None else None)


def _prune_parts(keep: set, d: Path | None = None) -> int:
    n = 0
    d = d or PARTS_DIR
    if d.exists():
        for p in d.glob('*/*.npz'):
            if p.stem not in keep:
                p.unlink(missing_ok=True); n += 1
    return n


def build(verbose: bool = True, force: bool = False, jobs: int | None = None, merge_only: bool = False) -> dict:
    """``force`` = build even when the generation is current / the drift is minor (units whose
    signature is unchanged are still reused — delete ``parts/`` for a from-scratch pass)."""
    import fcntl
    import tree_stands
    t0 = time.time()
    NAT_DIR.mkdir(parents=True, exist_ok=True)
    lock = open(NAT_DIR / 'build.lock', 'w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return {'skipped': True, 'reason': 'another build is running', 'progress': read_progress()}
    try:
        ca.reindex_lagging()                 # the plan reads the chunk index: bring it up to the files first
    except Exception as e:  # noqa: BLE001
        log.warning('national: reindex_lagging: %s', e)
    ca._tree_qa.sync()
    tq_sig = ca._tree_qa.corpus_sig()
    gen = current_gen()
    meta = read_meta()
    have = all((NAT_DIR / f'r{r}.nex.gz').exists() for r in RESES)
    sm = tree_stands.read_meta()         # the res-10 stand store must be of this very build
    have = have and bool(sm and meta and sm.get('algo') == tree_stands.ALGO and sm.get('gen') == meta.get('gen'))
    prog = Progress(verbose=verbose)
    if jobs is not None:
        prog.d['jobs'] = jobs
    if meta and meta.get('gen') == gen and not force and have:
        prog.set(force=True, state='skipped', gen=gen)
        return {'skipped': True, 'gen': gen}
    files = ca._files()
    if have and not force:
        d = minor_drift(meta, files)
        if d is not None:
            log.info('national: build deferred, minor corpus drift %s (serving gen %s)', d, meta.get('gen'))
            prog.set(force=True, state='deferred', gen=meta.get('gen'))
            return {'skipped': True, 'deferred': 'minor corpus drift', 'serving_gen': meta.get('gen'),
                    'corpus_gen': gen, **d, 'rebuild_by_d': MAX_DEFER_D}
    built = 0; chunks_read = 0; cells_read = 0
    try:
        for rnd in range(1, MAX_ROUNDS + 2):
            files = ca._files()
            ca._tree_qa.sync(); tq_sig = ca._tree_qa.corpus_sig(); gen = current_gen()
            units = plan(files)
            todo = [u for u in units if _unit_sig(u['id']) != u['sig']]
            prog.set(force=True, state='units' if todo else 'merge', round=rnd, gen=gen, units_total=len(units),
                     units_cached=len(units) - len(todo), units_todo=len(todo), units_done=0,
                     chunks_todo=sum(u['chunks'] for u in todo), chunks_done=0, units_started=round(time.time(), 1))
            if not todo:
                break
            if rnd > MAX_ROUNDS:
                # the corpus kept moving (ingest wave / tree-QA refresh): everything computed is kept,
                # the next run finishes — never publish a table mixed from two corpus states
                prog.set(force=True, state='deferred', error='corpus changed during every round')
                return {'skipped': True, 'deferred': 'corpus changing during the build', 'units_built': built}
            if merge_only:
                raise RuntimeError(f'{len(todo)} units not built (merge_only)')
            st = run_units(todo, prog, jobs)
            built += st['done']; chunks_read += st['chunks']; cells_read += st['cells']
            prog.set(force=True)
        kgs = sorted(files)
        epoch = ca._epoch_pin()
        built_at = _iso_now()
        out_meta = {'algo': ALGO, 'gen': gen, 'epoch': epoch, 'built_at': built_at, 'kgs': len(kgs),
                    'units': len(units), 'units_built': built, 'chunks': sum(u['chunks'] for u in units),
                    'chunks_read': chunks_read, 'dup_r10s': int(sum(u['r10s'].size for u in units if u['kind'] == 'g')),
                    'rows': {}, 'bytes_gz': {}, 'fields': [f for f in FIELDS if f != 'id'],
                    'green_fix': _green_fix.FIX_ID, 'build_started': round(t0, 1), 'corpus': _corpus_sig(files),
                    'tree_qa': {k: tq_sig[k] for k in ('id', 'kgs', 'hidden')}, 'jobs': prog.d['jobs']}
        prog.set(force=True, state='merge')
        blobs = {}
        for what, val in merge_units(units, prog):
            if what == 'red9':
                red9 = val
                out_meta['cells'] = int(red9['n_cells'].sum()) if 'n_cells' in red9 else 0
                reds = {9: red9, 8: rollup(red9, 9, 8)}
                del red9, val
                for res in RESES:
                    cols = finalise(reds.pop(res))
                    hdr = header_for(res, cols['id'].size, gen, epoch,
                                     {'built_at': built_at, 'kgs': len(kgs), 'cells_total': out_meta['cells']})
                    blobs[res] = gzip.compress(encode(hdr, cols), 6)
                    out_meta['rows'][str(res)] = int(cols['id'].size); out_meta['bytes_gz'][str(res)] = len(blobs[res])
                    if verbose:
                        print(f'  res {res}: {cols["id"].size} rows, gz {len(blobs[res]) // 1024} KB', flush=True)
                    del cols
            else:
                prog.set(force=True, state='stands')
                try:                        # res-10 stand index: same units, same apices, same gen
                    if val is not None:
                        sm = tree_stands.write_store(val, gen, built_at)
                        out_meta['stands'] = {k: sm[k] for k in ('hexes', 'apices', 'bytes')}
                except Exception as e:  # noqa: BLE001 — never lose the national table over the add-on
                    log.warning('national: tree stand store not written: %s', e)
                del val
        # publish: table files, then meta (readers key on meta.gen; the stand store is already in place)
        for res in RESES:
            p = NAT_DIR / f'r{res}.nex.gz'; tmp = p.with_suffix('.tmp')
            tmp.write_bytes(blobs[res]); os.replace(tmp, p)
        out_meta['build_s'] = round(time.time() - t0, 1)
        tmp = NAT_DIR / 'meta.tmp'
        tmp.write_text(json.dumps(out_meta, indent=1)); os.replace(tmp, NAT_DIR / 'meta.json')
        for p in CACHE_DIR.glob('*.nex.gz') if CACHE_DIR.exists() else []:
            if f'-{gen}-' not in p.name:
                p.unlink(missing_ok=True)
        out_meta['parts_pruned'] = _prune_parts({u['id'] for u in units})
        log.info('national: built gen %s in %.0fs (%s; %d of %d units rebuilt)', gen, out_meta['build_s'],
                 out_meta['rows'], built, len(units))
        prog.set(force=True, state='done', gen=gen)
        return out_meta
    except BaseException as e:
        prog.set(force=True, state='failed', error=f'{type(e).__name__}: {e}'[:300])
        raise


def read_meta() -> dict | None:
    try:
        return json.loads((NAT_DIR / 'meta.json').read_text())
    except (OSError, ValueError):
        return None


# === SECTION: request path ===

_slice_lock = threading.Lock()
_meta_cache = {'t': 0.0, 'v': None}


def meta() -> dict | None:
    now = time.time()
    if now - _meta_cache['t'] > 15:
        _meta_cache.update(t=now, v=read_meta())
    return _meta_cache['v']


def parse_fields(raw: str | None) -> list[str] | None:
    """None = every field (the stored file as is)."""
    if not raw or raw.strip() in ('*', 'all'):
        return None
    fs = []
    for x in raw.split(','):
        x = x.strip()
        if not x or x == 'id':
            continue
        x = ALIASES.get(x, x)
        if x not in FIELDS:
            raise ca.CellsError(400, f'unknown national field {x!r}', fields=[f for f in FIELDS if f != 'id'],
                                aliases=ALIASES)
        if x not in fs:
            fs.append(x)
    return fs or None


def etag_for(gen: str, res: int, fields: list[str] | None) -> str:
    rep = 'all' if fields is None else hashlib.sha256(','.join(fields).encode()).hexdigest()[:8]
    return f'"n-r{res}-{gen}-{rep}"'


def serve(res: int, fields: list[str] | None) -> tuple[bytes, dict, str, dict]:
    """→ (gzipped body, headers, etag, meta) — the stored file for ``fields=None``,
    else a column slice cached on disk per (gen, res, fields)."""
    m = meta()
    if not m or not (NAT_DIR / f'r{res}.nex.gz').exists():
        raise ca.CellsError(404, 'national aggregate not built yet', retry_after_s=ca.RETRY_AFTER_S)
    gen = m['gen']
    p = NAT_DIR / f'r{res}.nex.gz'
    if fields is None:
        gz = p.read_bytes()
    else:
        rep = hashlib.sha256(','.join(fields).encode()).hexdigest()[:8]
        cp = CACHE_DIR / f'r{res}-{gen}-{rep}.nex.gz'
        if cp.exists():
            gz = cp.read_bytes()
        else:
            with _slice_lock:
                if cp.exists():
                    gz = cp.read_bytes()
                else:
                    hdr, cols = decode(gzip.decompress(p.read_bytes()))
                    gz = gzip.compress(encode(hdr, cols, fields), 6)
                    del cols
                    CACHE_DIR.mkdir(parents=True, exist_ok=True)
                    tmp = cp.with_suffix('.tmp'); tmp.write_bytes(gz); os.replace(tmp, cp)
                    olds = sorted(CACHE_DIR.glob('*.nex.gz'), key=lambda q: q.stat().st_mtime)
                    for q in olds[:-CACHE_FILES] if len(olds) > CACHE_FILES else []:
                        q.unlink(missing_ok=True)
    n = (m.get('rows') or {}).get(str(res))
    hx = {'X-Cells-Gen': gen, 'X-Cells-Record': f'nex1/{ALGO}', 'X-Cells-Res': str(res),
          'X-Cells-Fields': ','.join(fields) if fields else ','.join(f for f in FIELDS if f != 'id'),
          'X-Cells-Layout': f'nex1={n}' if n is not None else None, 'X-Cells-Built': m.get('built_at')}
    hx = ca._with_epoch(hx, m.get('epoch'))
    return gz, hx, etag_for(gen, res, fields), m


def dict_block() -> dict:
    m = meta() or {}
    return {
        'what': 'Austria-wide aggregate of the cells corpus, one row per res-9 (or res-8) H3 parent: per field the '
                'mean over its res-12 children, verdict counts per class, n_cells — for overview zooms / national tables',
        'request': 'GET /api/v1/cells/national?res=9[&fields=bld_h_max,canopy_h_p90,slope_mean,elev_mean,verdict]'
                   '[&format=json → meta only]',
        'container': "'NEX1' | u32 header_len | header JSON {res, n, gen, epoch, fields[], columns[{name,dtype,bytes,shape?,nodata?,"
                     'unit,agg}], ids: delta_u64|u64, verdicts[], groups[]} | column bytes in header order, rows sorted by id',
        'headers': {'ETag': '"n-r<res>-<gen>-<fields hash>" (If-None-Match → 304)', 'X-Cells-Gen': 'build generation — key your copy on this',
                    'X-Cells-Epoch': 'corpus epoch pin at build time', 'X-Cells-Built': 'build time (UTC)',
                    'Cache-Control': 'public, max-age=86400 — rebuilt daily when the corpus changed; a handful of re-ingested '
                                     'products is batched for up to 7 days (X-Cells-Built says how old the table is)'},
        'res': list(RESES), 'default_res': DEFAULT_RES, 'default_fields': list(DEFAULT_FIELDS), 'aliases': ALIASES,
        'fields': {k: {kk: vv for kk, vv in v.items() if kk != 'src'} | {'source': v['src']} for k, v in FIELDS.items() if k != 'id'},
        'source': 'observation rows + tree apices + man-made objects of the published .h3b products only (no 25 m grid '
                  'fallback, no roof hints — those are bin-only); border cells deduplicated as in the bins',
        'current': {'gen': m.get('gen'), 'epoch': m.get('epoch'), 'built_at': m.get('built_at'), 'rows': m.get('rows'),
                    'bytes_gz': m.get('bytes_gz')} if m else None,
    }


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--build', action='store_true'); ap.add_argument('--force', action='store_true')
    ap.add_argument('--quiet', action='store_true')
    ap.add_argument('--check', help='compare N random res-9 rows against cells_api (res=9 rollup)', type=int)
    ap.add_argument('--jobs', type=int, help=f'worker processes (default {JOBS}, env CELLS_NATIONAL_JOBS)')
    ap.add_argument('--merge-only', action='store_true', help='fail instead of computing missing units')
    ap.add_argument('--status', action='store_true', help='progress of the running / last build + serving meta')
    ap.add_argument('--plan', action='store_true', help='units to build right now (no chunk read)')
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.status:
        print(text_line())
        print(json.dumps({'progress': read_progress(), 'meta': {k: v for k, v in (read_meta() or {}).items() if k != 'fields'}}, indent=1))
    elif a.plan:
        ca._tree_qa.sync()
        us = plan(); todo = [u for u in us if _unit_sig(u['id']) != u['sig']]
        print(json.dumps({'units': len(us), 'todo': len(todo), 'chunks_todo': sum(u['chunks'] for u in todo),
                          'chunks': sum(u['chunks'] for u in us), 'first': [u['id'] for u in todo[:20]]}, indent=1))
    elif a.build:
        print(json.dumps(build(verbose=not a.quiet, force=a.force, jobs=a.jobs, merge_only=a.merge_only), indent=1))
    else:
        print(json.dumps(read_meta(), indent=1))
