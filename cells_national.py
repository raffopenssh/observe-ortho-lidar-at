"""cells_national — Austria-wide res-8 / res-9 H3 aggregate of the public
cells corpus (``GET /api/v1/cells/national?res=9&fields=…``).

Why: umfeld-at renders zooms 9–11 of its cell tile layer (res 7–9
polygons) and the all-Austria PMTiles archive from one in-RAM national
table.  Those tiles carried no LiDAR columns because the per-cell
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
serves (and slices) those files.  Never build inside gunicorn: one pass
over 5.4 M chunks takes ~10 min and ~1 GB.
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

ALGO = 'national-1'
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
        if words.size:
            h = ca.decode_trees(words)['h_m']
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
    h = hashlib.sha256(f'{ALGO}|{_green_fix.FIX_ID}|{ca.cells_generation()}'.encode())
    return h.hexdigest()[:24]


def _dup_r10s() -> np.ndarray:
    c = ca._idx_conn()
    rows = c.execute('SELECT r10 FROM chunk GROUP BY r10 HAVING COUNT(*) > 1').fetchall()
    return np.asarray([int(r[0]) for r in rows], dtype=np.uint64)


def build(verbose: bool = True, force: bool = False) -> dict:
    t0 = time.time()
    gen = current_gen()
    meta = read_meta()
    if meta and meta.get('gen') == gen and not force and all((NAT_DIR / f'r{r}.nex.gz').exists() for r in RESES):
        return {'skipped': True, 'gen': gen}
    files = ca._files()
    kgs = sorted(files)
    dup = _dup_r10s(); dup_set = set(dup.tolist())
    acc = Acc(9)
    n_chunks = 0
    # pass A — chunks unique to one product, per KG
    for k, kg in enumerate(kgs):
        h = ca.header(kg, with_chunks=True)
        if h is None:
            continue
        chunks = []
        for row in h['chunks']:
            r10 = int(row['r10'], 16)
            if r10 in dup_set:
                continue
            try:
                chunks.append(ca._load_chunk(kg, dict(row, r10=r10), h))
            except Exception as e:  # noqa: BLE001
                log.warning('national: chunk %s/%s unreadable: %s', kg, row.get('r10'), e)
        n_chunks += len(chunks)
        if chunks:
            acc.add_table(ca.gather(chunks, None))
        if verbose and (k + 1) % 500 == 0:
            print(f'  pass A {k + 1}/{len(kgs)} kgs, {n_chunks} chunks, {acc.cells} cells, {time.time() - t0:.0f}s', flush=True)
        if (k + 1) % 1000 == 0:
            acc.reduce()
    # pass B — duplicated res-10 chunks, batched across products (dedupe inside gather)
    dup.sort()
    for b in range(0, dup.size, BATCH_R10):
        chunks, _h = ca.chunks_for_r10(dup[b:b + BATCH_R10])
        n_chunks += len(chunks)
        if chunks:
            acc.add_table(ca.gather(chunks, None))
        if verbose and (b // BATCH_R10) % 50 == 0:
            print(f'  pass B {b}/{dup.size} dup r10s, {acc.cells} cells, {time.time() - t0:.0f}s', flush=True)
    red9 = acc.reduce()
    epoch = ca._epoch_pin()
    built_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    NAT_DIR.mkdir(parents=True, exist_ok=True)
    out_meta = {'algo': ALGO, 'gen': gen, 'epoch': epoch, 'built_at': built_at, 'kgs': len(kgs), 'chunks': n_chunks,
                'cells': acc.cells, 'dup_r10s': int(dup.size), 'rows': {}, 'bytes_gz': {}, 'fields': [f for f in FIELDS if f != 'id'],
                'green_fix': _green_fix.FIX_ID}
    reds = {9: red9, 8: rollup(red9, 9, 8)}
    for res in RESES:
        cols = finalise(reds[res])
        hdr = header_for(res, cols['id'].size, gen, epoch, {'built_at': built_at, 'kgs': len(kgs), 'cells_total': acc.cells})
        blob = encode(hdr, cols)
        gz = gzip.compress(blob, 6)
        p = NAT_DIR / f'r{res}.nex.gz'; tmp = p.with_suffix('.tmp')
        tmp.write_bytes(gz); os.replace(tmp, p)
        out_meta['rows'][str(res)] = int(cols['id'].size); out_meta['bytes_gz'][str(res)] = len(gz)
        if verbose:
            print(f'  res {res}: {cols["id"].size} rows, raw {len(blob) // 1024} KB, gz {len(gz) // 1024} KB', flush=True)
    out_meta['build_s'] = round(time.time() - t0, 1)
    tmp = NAT_DIR / 'meta.tmp'
    tmp.write_text(json.dumps(out_meta, indent=1)); os.replace(tmp, NAT_DIR / 'meta.json')
    for p in CACHE_DIR.glob('*.nex.gz') if CACHE_DIR.exists() else []:
        if f'-{gen}-' not in p.name:
            p.unlink(missing_ok=True)
    log.info('national: built gen %s in %.0fs (%s)', gen, out_meta['build_s'], out_meta['rows'])
    return out_meta


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
                    'Cache-Control': 'public, max-age=86400 — rebuilt daily when the corpus changed'},
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
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.build:
        print(json.dumps(build(verbose=not a.quiet, force=a.force), indent=1))
    else:
        print(json.dumps(read_meta(), indent=1))
