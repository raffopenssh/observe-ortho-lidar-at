"""Public ``GET /api/v1/stats/unit`` — one "stats envelope" per spatial unit.

Shared contract (schema ``stats-envelope/1``) with sibling services: one JSON per H3 cell (res 5–12), KG code or small
bbox with FLAT dotted figure keys, so spreadsheets / agents can read our
observed aggregates per unit without walking cells.  Docs:
``public_docs.STATS_TXT``; design notes ``docs/ne-public-api.md → §12``.

Hot path reads only ``data/ne/*.h3b`` chunks through ``cells_api`` (chunk
LRU) + ``cells.sqlite``.  Every unit result is an additive accumulator
(``_Acc``) so coarse units (h3 res 5–6) are merged from memoised res-7
children; accumulators are persisted in ``cells.sqlite.unit_stats`` (bounded)
and fronted by a process LRU.  Work that exceeds the request budget keeps
running in a 2-thread pool and answers 202 until the cache is warm.
"""
from __future__ import annotations

import gzip
import re
import hashlib
import json
import logging
import struct
import zlib
import sqlite3
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone

import numpy as np
from flask import Blueprint, Response, request

import als_years
import cells_api as ca
import ne_h3

log = logging.getLogger('stats_unit')
stats_bp = Blueprint('stats_unit', __name__)

SCHEMA = 'stats-envelope/1'
SERVICE = 'srtm-lidar-at'
ALGO = 'su1'                       # bump → every cached accumulator is recomputed
MAX_KG = 50
MAX_BBOX_SPAN = 0.1
H3_MIN_RES, H3_MAX_RES = 5, 12
SPLIT_RES = 7                      # units coarser than this merge memoised res-7 children
BUDGET_S = 3.5                     # synchronous wait before answering 202
RETRY_AFTER_S = 6
MAX_AGE = 600
MEM_LRU_N = 4_096
DB_MAX_ROWS = 30_000               # ≈ 1 KB each → ≤ 30 MB on disk
FORMATS = ('json', 'flat', 'csv')
CELLS_MAGIC = b'NEI1'                # POST unit=cells body: magic · u32 n · n×u64 LE res-12 ids (gzip'd)
CELLS_MAX_N = 1_000_000
CELLS_MAX_SCALE = 7 ** 6            # ≥ one res-12 cell standing for a whole res-6 cell
CELLS_EXACT_AREA_MAX_R10 = 60_000   # per-r10 h3 area below this many parents, res-8 ancestor / 2401 above
CELLS_FAST_MIN_N = 2_000             # above this the row-streaming reader replaces ca.select (no chunk LRU)
CELLS_BATCH_ROWS = 8_000             # chunk rows read per batch (bounds the transient working set)
CELLS_POLL_WAIT_S = 0.25             # synchronous wait on a repeat POST / GET key= poll
CELLS_JOB_STALE_S = 180.0            # a job record older than this with no result → recompute
CELLS_JOB_TTL_S = 24 * 3600          # key= lookups answer 404 past this
BODY_LRU_N = 512                     # sha256(raw body) → parsed list meta (per worker)

GROUPS = list(ne_h3.GROUPS)
CONS = list(ca._CONSISTENCY_NAMES)       # 7 public verdict names


class StatsError(Exception):
    def __init__(self, status: int, msg: str, **extra):
        super().__init__(msg)
        self.status, self.msg, self.extra = status, msg, extra


# === SECTION: accumulator ===

def _acc_empty() -> dict:
    return {'kn': 0, 'obs': 0, 'trees': 0, 'bld': 0, 'cov': [0.0] * 9, 'cons': [0] * 7,
            'elmin': None, 'elmax': None, 'el_s': 0.0, 'el_n': 0, 'sl_s': 0.0, 'sl_n': 0,
            'p90_s': 0.0, 'p90_n': 0, 'hm_s': 0.0, 'hm_n': 0, 'hmax': None, 'can_s': 0.0, 'can_n': 0,
            'nd_s': 0.0, 'nd_n': 0, 'hl': 0, 'dh_s': 0.0, 'dh_n': 0, 'sc_s': 0.0, 'sc_n': 0,
            'area_m2': 0.0, 'als_new': {}, 'als_old': {}, 'kgs': [], 'epoch': None, 'src': None}


def _acc_from_table(t: ca.Table, hdrs: dict) -> dict:
    """Vectorised sums over a selected cell Table (no per-cell Python)."""
    a = _acc_empty()
    n = len(t)
    a['kgs'] = sorted(t.kgs)
    a['epoch'] = max((h.get('epoch') or '' for h in hdrs.values()), default='') or None
    a['src'] = max(((h.get('summary') or {}).get('source') or '' for h in hdrs.values()), default='') or None
    if n == 0:
        return a
    o = t.obs
    a['kn'] = n
    cov = o['cover'].astype(np.float64) / 255.0
    a['obs'] = int(((o['hm'] != 255) | (o['cover'].sum(axis=1) > 0)).sum())
    a['trees'] = int(o['tn'].sum()); a['bld'] = int(o['sn'].sum())
    a['cov'] = [round(float(x), 3) for x in cov.sum(axis=0)]
    a['cons'] = np.bincount(np.minimum(o['vd'], 6), minlength=7).astype(int).tolist()

    def _sum(v, bad):
        m = ~bad
        return float(v[m].sum()), int(m.sum())
    el = o['el']; m = el != 65535
    if m.any():
        a['elmin'] = float(el[m].min()); a['elmax'] = float(el[m].max())
        a['el_s'] = float(el[m].sum()); a['el_n'] = int(m.sum())
    a['sl_s'], a['sl_n'] = _sum(o['sl'].astype(np.float64), o['sl'] == 255)
    a['p90_s'], a['p90_n'] = _sum(o['h90'].astype(np.float64) / 2.0, o['h90'] == 255)
    a['hm_s'], a['hm_n'] = _sum(o['hm'].astype(np.float64) / 2.0, o['hm'] == 255)
    hx = o['hx']; m = hx != 255
    a['hmax'] = float(hx[m].max()) / 2.0 if m.any() else None
    a['can_s'], a['can_n'] = _sum(o['cf'].astype(np.float64) / 254.0, o['cf'] == 255)
    a['nd_s'], a['nd_n'] = _sum(o['nm'].astype(np.float64) / 254.0 * 2.0 - 1.0, o['nm'] == 255)
    a['hl'] = int(((o['hl'] != 0) & (o['hl'] != 255)).sum())
    a['dh_s'], a['dh_n'] = _sum(o['dh'].astype(np.float64) / 4.0, o['dh'] == -128)
    a['sc_s'], a['sc_n'] = _sum(o['sc'].astype(np.float64) / 255.0, np.zeros(n, bool))
    # area + ALS years at chunk precision: one h3 area / one flight-block lookup per
    # res-10 chunk (49 cells), weighted by the cells actually selected from it
    per_chunk = np.bincount(t.src_chunk, minlength=len(t.chunks)).astype(np.int64)
    used = np.flatnonzero(per_chunk)
    hn = ca._h3n()
    r10 = np.array([t.chunks[i].r10 for i in used], dtype=np.uint64)
    areas = np.array([hn.cell_area(int(x), unit='m^2') for x in r10.tolist()]) / 49.0
    a['area_m2'] = round(float((areas * per_chunk[used]).sum()), 1)
    try:
        yn, yo = als_years.ids_years(r10)
        for hist, ys in (('als_new', yn), ('als_old', yo)):
            d = {}
            for y, w in zip(ys.tolist(), per_chunk[used].tolist()):
                if y != als_years.NULL:
                    d[str(y)] = d.get(str(y), 0) + w
            a[hist] = d
    except Exception as e:  # noqa: BLE001
        log.debug('als years: %s', e)
    return a


def _acc_merge(parts: list[dict]) -> dict:
    a = _acc_empty()
    kgs = set(); epochs = []; srcs = []
    for p in parts:
        for k in ('kn', 'obs', 'trees', 'bld', 'el_s', 'el_n', 'sl_s', 'sl_n', 'p90_s', 'p90_n', 'hm_s', 'hm_n',
                  'can_s', 'can_n', 'nd_s', 'nd_n', 'hl', 'dh_s', 'dh_n', 'sc_s', 'sc_n', 'area_m2'):
            a[k] += p[k]
        a['cov'] = [x + y for x, y in zip(a['cov'], p['cov'])]
        a['cons'] = [x + y for x, y in zip(a['cons'], p['cons'])]
        for k, fn in (('elmin', min), ('elmax', max), ('hmax', max)):
            if p[k] is not None:
                a[k] = p[k] if a[k] is None else fn(a[k], p[k])
        for k in ('als_new', 'als_old'):
            for y, w in p[k].items():
                a[k][y] = a[k].get(y, 0) + w
        kgs.update(p['kgs'])
        if p['epoch']: epochs.append(p['epoch'])
        if p['src']: srcs.append(p['src'])
    a['kgs'] = sorted(kgs); a['epoch'] = max(epochs) if epochs else None; a['src'] = max(srcs) if srcs else None
    return a


def _figures(a: dict, *, cells_total: int, area_ha_total: float | None, ha_override: dict | None = None) -> dict:
    """Accumulator → flat dotted figures (scalar values only)."""
    kn = a['kn']
    cell_ha = (a['area_m2'] / kn / 1e4) if kn else 0.0
    f = OrderedDict()
    f['cells_n'] = int(cells_total)
    f['area_ha'] = round(area_ha_total if area_ha_total is not None else a['area_m2'] / 1e4, 2)
    f['cells_observed_n'] = int(a['obs'])
    f['trees_n'] = int(a['trees'])
    f['structures_n'] = int(a['bld'])
    f['structures.cover_pct'] = round(100.0 * a['sc_s'] / a['sc_n'], 2) if a['sc_n'] else None
    cov_ha = (ha_override or {}).get('cover_ha') or {g: round(a['cov'][i] * cell_ha, 2) for i, g in enumerate(GROUPS)}
    for g in GROUPS:
        f[f'cover_ha.{g}'] = cov_ha.get(g, 0.0)
    cons_ha = (ha_override or {}).get('consistency_ha') or {c: round(a['cons'][i] * cell_ha, 2) for i, c in enumerate(CONS)}
    for i, c in enumerate(CONS):
        f[f'consistency_ha.{c}'] = cons_ha.get(c, 0.0)
    for i, c in enumerate(CONS):
        f[f'consistency_n.{c}'] = int(a['cons'][i])
    f['terrain.elev_min_m'] = a['elmin']
    f['terrain.elev_max_m'] = a['elmax']
    f['terrain.elev_mean_m'] = round(a['el_s'] / a['el_n'], 1) if a['el_n'] else None
    f['terrain.slope_mean_deg'] = round(a['sl_s'] / a['sl_n'], 1) if a['sl_n'] else None
    f['height.mean_m'] = round(a['hm_s'] / a['hm_n'], 2) if a['hm_n'] else None
    f['height.p90_mean_m'] = round(a['p90_s'] / a['p90_n'], 2) if a['p90_n'] else None
    f['height.max_m'] = a['hmax']
    f['canopy.cover_pct'] = round(100.0 * a['can_s'] / a['can_n'], 2) if a['can_n'] else None
    f['ndvi.mean'] = round(a['nd_s'] / a['nd_n'], 3) if a['nd_n'] else None
    f['change.dh_mean_m'] = round(a['dh_s'] / a['dh_n'], 2) if a['dh_n'] else None
    f['hansen_loss_ha'] = round(a['hl'] * cell_ha, 2)
    new, old = a['als_new'], a['als_old']
    ny = int(max(new, key=int)) if new else None
    f['als.newest_year'] = ny
    f['als.oldest_year'] = int(min(old, key=int)) if old else None
    f['als.coverage_pct'] = round(100.0 * new[str(ny)] / kn, 1) if (ny is not None and kn) else None
    return f


# === SECTION: cache (process LRU + cells.sqlite.unit_stats) ===

_mem: OrderedDict = OrderedDict()
_mem_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='stats_unit')
_inflight: dict[str, Future] = {}
_inflight_lock = threading.Lock()
_DDL = ('CREATE TABLE IF NOT EXISTS unit_stats (key TEXT PRIMARY KEY, fp TEXT, js TEXT, computed_at REAL)',
        'CREATE INDEX IF NOT EXISTS unit_stats_t ON unit_stats(computed_at)')


def _db_get(key: str, fp: str) -> tuple[dict, float] | None:
    c = ca._idx_conn()
    if c is None:
        return None
    try:
        row = c.execute('SELECT fp, js, computed_at FROM unit_stats WHERE key=?', (key,)).fetchone()
    except sqlite3.Error:
        return None
    if row and row[0] == fp:
        return json.loads(row[1]), float(row[2])
    return None


def _db_put(key: str, fp: str, acc: dict, ts: float) -> None:
    try:
        c = ca._idx_conn(write=True)
        with ca._idx_write_lock:
            for ddl in _DDL:
                c.execute(ddl)
            c.execute('INSERT OR REPLACE INTO unit_stats VALUES (?,?,?,?)',
                      (key, fp, json.dumps(acc, separators=(',', ':')), ts))
            if c.execute('SELECT COUNT(*) FROM unit_stats').fetchone()[0] > DB_MAX_ROWS:
                c.execute('DELETE FROM unit_stats WHERE key IN (SELECT key FROM unit_stats ORDER BY computed_at LIMIT ?)',
                          (DB_MAX_ROWS // 10,))
            c.commit()
    except sqlite3.Error as e:
        log.debug('unit_stats persist %s: %s', key, e)


def _cached(key: str, fp: str) -> tuple[dict, float] | None:
    with _mem_lock:
        hit = _mem.get(key)
        if hit is not None and hit[0] == fp:
            _mem.move_to_end(key)
            return hit[1], hit[2]
    hit = _db_get(key, fp)
    if hit is not None:
        with _mem_lock:
            _mem[key] = (fp, hit[0], hit[1])
        return hit
    return None


def _store(key: str, fp: str, acc: dict, persist: bool = True) -> float:
    ts = time.time()
    with _mem_lock:
        _mem[key] = (fp, acc, ts)
        while len(_mem) > MEM_LRU_N:
            _mem.popitem(last=False)
    if persist:
        _db_put(key, fp, acc, ts)
    return ts


def _fingerprint_bbox(w, s, e, n) -> tuple[str, list[str], list[str]]:
    """(fp, kgs_on_disk, kgs_missing) for every KG whose bbox intersects."""
    inter = ca._kgs_intersecting(w, s, e, n)
    fs = ca._files()
    on, miss = [], []
    for k in inter:
        f = fs.get(k)
        (on if f else miss).append(k)
    parts = [f"{k}:{int(float(fs[k].get('mtime') or 0))}:{int(fs[k].get('partial') or 0)}" for k in on]
    fp = hashlib.sha256((ALGO + ca._green_fix.FIX_ID + '|' + ','.join(parts) + '|m' + str(len(miss))).encode()).hexdigest()[:16]
    return fp, on, miss


def _get_or_compute(key: str, fp: str, fn, *, wait_s: float, persist: bool = True) -> tuple[dict, float] | None:
    """Cached accumulator or run ``fn`` (single-flight per key) for ≤ wait_s;
    None = still computing (caller answers 202)."""
    hit = _cached(key, fp)
    if hit is not None:
        return hit
    with _inflight_lock:
        fut = _inflight.get(key)
        if fut is None:
            def _run():
                try:
                    acc = fn()
                    return acc, _store(key, fp, acc, persist)
                finally:
                    with _inflight_lock:
                        _inflight.pop(key, None)
            fut = _pool.submit(_run)
            _inflight[key] = fut
    try:
        return fut.result(timeout=max(0.0, wait_s))
    except TimeoutError:            # concurrent.futures.TimeoutError is the builtin since 3.11
        return None


# === SECTION: units ===

def _h3_children_ids(cid: int, res: int) -> np.ndarray:
    return np.asarray(ca._h3n().cell_to_children(cid, res), dtype=np.uint64)


def _h3_bbox(cid: int) -> tuple[float, float, float, float]:
    import h3
    b = h3.cell_to_boundary(h3.int_to_str(cid))
    lats = [p[0] for p in b]; lons = [p[1] for p in b]
    return (round(min(lons), 6), round(min(lats), 6), round(max(lons), 6), round(max(lats), 6))


def _acc_for_ids(ids: np.ndarray, bbox) -> dict:
    t, hdrs, meta = ca.select(ids, bbox)
    a = _acc_from_table(t, hdrs)
    a['miss'] = meta.get('kgs_missing') or []
    a['pend'] = meta.get('kgs_pending') or []
    a['pfiles'] = meta.get('partial_files') or []
    a['want'] = int(ids.size)
    return a


def _h3_acc(cid: int, res: int, wait_s: float) -> tuple[dict, float] | None:
    """Accumulator for an H3 unit; res < SPLIT_RES merges memoised res-7 children."""
    bbox = _h3_bbox(cid)
    fp, _on, _miss = _fingerprint_bbox(*bbox)
    key = f'h3:{cid:x}'
    if res >= SPLIT_RES:
        return _get_or_compute(key, fp, lambda: _acc_for_ids(_h3_children_ids(cid, ca.RES), bbox),
                               wait_s=wait_s, persist=res <= 9)
    hit = _cached(key, fp)
    if hit is not None:
        return hit
    deadline = time.time() + wait_s
    parts = []
    for ch in _h3_children_ids(cid, SPLIT_RES).tolist():
        r = _h3_acc(int(ch), SPLIT_RES, max(0.0, deadline - time.time()))
        if r is None:
            # keep the remaining children warming in the background, answer 202 now
            for rest in _h3_children_ids(cid, SPLIT_RES).tolist():
                _h3_acc(int(rest), SPLIT_RES, 0.0)
            return None
        parts.append(r[0])
    a = _acc_merge(parts)
    for k in ('miss', 'pend', 'pfiles'):
        a[k] = sorted({x for p in parts for x in p.get(k, [])})
    a['want'] = sum(p.get('want', 0) for p in parts)
    return a, _store(key, fp, a, persist=True)


def _kg_acc_compute(kg: str) -> dict:
    h = ca.header(kg, with_chunks=True)
    if h is None:
        raise StatsError(404, 'gone')
    chunks = []
    for row in h['chunks']:
        r = dict(row); r['r10'] = int(str(r['r10']), 16) if isinstance(r['r10'], str) else int(r['r10'])
        try:
            chunks.append(ca._load_chunk(kg, r, h))
        except Exception as ex:  # noqa: BLE001
            log.warning('stats: chunk %s/%x unreadable: %s', kg, r['r10'], ex)
    t = ca.gather(chunks, None)
    a = _acc_from_table(t, {kg: h})
    a['kgs'] = [kg]
    a['summary'] = {k: (h.get('summary') or {}).get(k) for k in ('cover_ha', 'consistency_ha', 'cells_observed')}
    a['want'] = int(h.get('cells_n') or len(t))
    a['pfiles'] = [kg] if h.get('partial_cells') else []
    a['miss'] = []; a['pend'] = []
    return a


def _kg_acc(kg: str, wait_s: float) -> tuple[dict, float] | None:
    f = ca._files().get(kg)
    fp = hashlib.sha256(f"{ALGO}{ca._green_fix.FIX_ID}|{kg}:{int(float(f.get('mtime') or 0))}".encode()).hexdigest()[:16]
    return _get_or_compute(f'kg:{kg}', fp, lambda: _kg_acc_compute(kg), wait_s=wait_s)


# === SECTION: envelope ===

_ATTR = ca.ATTRIBUTION


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _envelope(unit: dict, acc: dict | None, ts: float | None, *, cells_total: int, area_ha: float | None,
              note: str | None = None, ha_override: dict | None = None) -> dict:
    now = time.time()
    if acc is None:
        acc = _acc_empty()
    fig = _figures(acc, cells_total=cells_total, area_ha_total=area_ha, ha_override=ha_override)
    kn = int(acc['kn'])
    want = int(acc.get('want') or cells_total or 0)
    complete = bool(kn and kn >= want and not acc.get('miss') and not acc.get('pfiles'))
    cov = {'complete': complete, 'cells_known': kn, 'cells_total': int(cells_total)}
    notes = []
    if note:
        notes.append(note)
    if acc.get('miss'):
        m = acc['miss']; p = set(acc.get('pend') or [])
        notes.append('no product yet for KG ' + ','.join(m[:12]) + ('…' if len(m) > 12 else '')
                     + (f' ({len([x for x in m if x in p])} in progress)' if p else ''))
    if acc.get('pfiles'):
        notes.append('partial product: ' + ','.join(acc['pfiles'][:12]))
    if kn and kn < want and not acc.get('miss'):
        notes.append(f'{want - kn} cells outside any product')
    if notes:
        cov['note'] = '; '.join(notes)
    unit = dict(unit); unit['res'] = ca.RES; unit['cells_n'] = int(cells_total); unit['area_ha'] = fig['area_ha']
    if acc.get('kgs'):
        unit['kgs'] = acc['kgs']
    return {'schema': SCHEMA, 'service': SERVICE, 'unit': unit,
            'source': acc.get('src') or ne_h3.OBS_SOURCE, 'epoch': acc.get('epoch') or ca._epoch_pin() or '2026-10',
            'coverage': cov, 'figures': fig, 'attribution': _ATTR, 'license': 'CC-BY-4.0',
            'computed_at': _iso(ts or now), 'age_s': int(max(0.0, now - (ts or now)))}


def unit_h3(h: str, wait_s: float) -> dict | None:
    try:
        cid = int(h.strip(), 16)
        import h3
        if not h3.is_valid_cell(h3.int_to_str(cid)):
            raise ValueError
    except Exception:
        raise StatsError(400, 'bad h3 id', h3=h)
    res = ca._cell_res(cid)
    if not H3_MIN_RES <= res <= H3_MAX_RES:
        raise StatsError(400, f'h3 res must be {H3_MIN_RES}–{H3_MAX_RES}', res=res)
    r = _h3_acc(cid, res, wait_s)
    if r is None:
        return None
    acc, ts = r
    area_ha = ca._h3n().cell_area(cid, unit='m^2') / 1e4
    unit = {'kind': 'h3', 'key': f'{cid:x}', 'bbox': list(_h3_bbox(cid)), 'h3_res': res}
    return _envelope(unit, acc, ts, cells_total=7 ** (ca.RES - res), area_ha=area_ha)


def unit_kg(code: str, wait_s: float) -> dict | None:
    kg = code.strip().zfill(5)
    if not (kg.isdigit() and len(kg) == 5):
        raise StatsError(400, 'kg codes must be 5 digits', kg=code)
    bb = ca._kg_bbox(kg)
    unit = {'kind': 'kg', 'key': kg, 'bbox': [round(x, 6) for x in bb] if bb else None}
    if bb is None:
        return _envelope(unit, None, None, cells_total=0, area_ha=0.0, note='unknown KG code')
    if kg not in ca._files():
        ns = ca.ne_state(kg)
        return _envelope(unit, None, None, cells_total=0, area_ha=0.0, note=f"not processed ({ns.get('ne_status')})")
    r = _kg_acc(kg, wait_s)
    if r is None:
        return None
    acc, ts = r
    s = acc.get('summary') or {}
    cons_ha = s.get('consistency_ha') or {}
    area_ha = round(sum(v or 0 for v in cons_ha.values()), 2) if cons_ha else None
    return _envelope(unit, acc, ts, cells_total=int(acc.get('want') or acc['kn']), area_ha=area_ha,
                     ha_override={'cover_ha': s.get('cover_ha'), 'consistency_ha': cons_ha},
                     note='area_ha / cover_ha / consistency_ha weight border cells by this KG\'s share (as /kgs/summary); '
                          'counts and means are over every cell of the product')


def unit_bbox(raw: str, wait_s: float) -> dict | None:
    try:
        w, s, e, n = [float(x) for x in raw.split(',')]
    except Exception:
        raise StatsError(400, 'bbox must be w,s,e,n (WGS84)')
    if not (w < e and s < n and -180 <= w and e <= 180 and -90 <= s and n <= 90):
        raise StatsError(400, 'bad bbox order / range')
    if (e - w) > MAX_BBOX_SPAN or (n - s) > MAX_BBOX_SPAN:
        raise StatsError(400, f'bbox span > {MAX_BBOX_SPAN}°', max_span_deg=MAX_BBOX_SPAN)
    bbox = (w, s, e, n)
    fp, _on, _miss = _fingerprint_bbox(*bbox)
    key = 'bb:' + ','.join(f'{x:.6f}' for x in bbox)
    ids = ca.ids_in_bbox(w, s, e, n)        # cell CENTRE inside the box (h3 containment rule)
    r = _get_or_compute(key, fp, lambda: _acc_for_ids(ids, bbox), wait_s=wait_s, persist=ids.size > 20_000)
    if r is None:
        return None
    acc, ts = r
    hn = ca._h3n()
    c12 = int(hn.latlng_to_cell((s + n) / 2, (w + e) / 2, ca.RES))
    area_ha = ids.size * hn.cell_area(c12, unit='m^2') / 1e4
    unit = {'kind': 'bbox', 'key': [w, s, e, n], 'bbox': [w, s, e, n]}
    return _envelope(unit, acc, ts, cells_total=int(ids.size), area_ha=area_ha)


# === SECTION: cell lists (POST unit=cells, stats-envelope §6) ===

def parse_cells_body(raw: bytes, encoding: str | None) -> np.ndarray:
    """gzip'd ``NEI1`` · u32 n · n×u64 LE → sorted unique res-12 ids. Raises StatsError."""
    if not raw:
        raise StatsError(400, 'empty body (expected gzip\'d NEI1 cell list)')
    enc = (encoding or '').lower()
    max_raw = 8 + 8 * CELLS_MAX_N + 64
    try:
        if 'gzip' in enc or raw[:2] == b'\x1f\x8b':
            d = zlib.decompressobj(16 + zlib.MAX_WBITS)
            data = d.decompress(raw, max_raw + 1)
            if d.unconsumed_tail:
                raise StatsError(413, f'cell list > {CELLS_MAX_N} ids', max_n=CELLS_MAX_N)
        else:
            data = raw
    except StatsError:
        raise
    except Exception as e:  # noqa: BLE001
        raise StatsError(400, f'bad gzip body: {e}')
    if len(data) < 8 or data[:4] != CELLS_MAGIC:
        raise StatsError(400, 'bad magic: body must start with "NEI1"', magic=data[:4].decode('latin1'))
    n = struct.unpack('<I', data[4:8])[0]
    if n > CELLS_MAX_N:
        raise StatsError(413, f'cell list > {CELLS_MAX_N} ids', n=n, max_n=CELLS_MAX_N)
    if len(data) < 8 + 8 * n:
        raise StatsError(400, f'truncated body: header says {n} ids, got {(len(data) - 8) // 8}', n=n)
    ids = np.frombuffer(data, dtype='<u8', count=n, offset=8).astype(np.uint64)
    if n == 0:
        return ids
    if np.any(((ids >> np.uint64(52)) & np.uint64(0xF)) != np.uint64(ca.RES)):
        bad = int(ids[((ids >> np.uint64(52)) & np.uint64(0xF)) != np.uint64(ca.RES)][0])
        raise StatsError(400, f'all ids must be H3 res-{ca.RES}', bad_id=f'{bad:x}')
    if not np.all(ids[1:] >= ids[:-1]):
        ids = np.sort(ids)
    return np.unique(ids) if np.any(ids[1:] == ids[:-1]) else ids


def _cells_area_m2(ids: np.ndarray) -> float:
    """Σ res-12 cell areas: exact per res-10 parent (as the chunk rule in ``_acc_from_table``),
    res-8 ancestor / 7^4 for very long lists (≤ 0.5 % off, 49× fewer h3 calls)."""
    if ids.size == 0:
        return 0.0
    hn = ca._h3n()
    r10, cnt = np.unique(ne_h3.h3_parent_vec(ids, ca.CHUNK_RES), return_counts=True)
    if r10.size <= CELLS_EXACT_AREA_MAX_R10:
        par, div = r10, 49.0
    else:
        r8 = ne_h3.h3_parent_vec(r10, 8)
        par, inv = np.unique(r8, return_inverse=True)
        cnt = np.bincount(inv, weights=cnt, minlength=par.size)
        div = 49.0 * 49.0
    areas = np.array([hn.cell_area(int(x), unit='m^2') for x in par.tolist()]) / div
    return float((areas * cnt).sum())


def _scale_figures(fig: dict, scale: int) -> dict:
    """× scale on every summed figure (``_ha`` / ``_n`` keys incl. the dotted groups);
    means, ``_pct``, extremes and years untouched."""
    if scale == 1:
        return fig
    out = OrderedDict()
    for k, v in fig.items():
        if k in ('cells_n', 'area_ha'):          # already n × scale / Σ areas × scale from the caller
            out[k] = v
        elif k.endswith(('_ha', '_n')) or k.startswith(('cover_ha.', 'consistency_ha.', 'consistency_n.')):
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                out[k] = v
            else:
                out[k] = int(v) * scale if isinstance(v, int) else round(v * scale, 2)
        else:
            out[k] = v
    return out


# --- row-streaming reader (large lists) -------------------------------------
# ``ca.select`` inflates every touched res-10 chunk into a ``Chunk`` (trees,
# structures, offsets, green fix per chunk) and parks it in the chunk LRU — for
# a 1-in-49 national sample that is one full chunk per listed cell: ~0.25 ms
# and ~10 KB each, 100 k ids ≈ 25 s + >1 GB transient.  This path reads the
# obs + ctx rows of the wanted cells only, one file handle per KG, applies the
# green-ground override once over the collected rows and never touches the LRU.

class _R10Ref:
    __slots__ = ('r10', 'cells')

    def __init__(self, r10: int, cells: int):
        self.r10 = r10; self.cells = cells


def _rows_wanted(kg: str, hdr: dict, fh, rows: list[dict], want_by_r10: dict) -> list[tuple]:
    """→ [(row, obs_raw(k×41), ctx_raw(k×30), ids(k), n_chunk)] for the chunk rows of one KG;
    ``want_by_r10`` = r10 → sorted wanted ids inside that chunk."""
    rb = hdr.get('record_bytes') or {}
    r0, r1 = int(rb.get('nec1', 30)), int(rb.get('neo1', ca.REC))
    shuffled = 'shuffle' in str(hdr.get('chunk_layout') or '')
    base = int(hdr['body_offset'])
    out = []
    for row in rows:
        n = int(row['cells'])
        if n == 0:
            continue
        fh.seek(base + int(row['off']))
        comp = fh.read(int(row['len']))
        o1 = n * r0; o2 = o1 + n * r1
        try:
            raw = zlib.decompressobj().decompress(comp, o2)
        except zlib.error as ex:
            log.warning('cells: chunk %s/%x unreadable: %s', kg, int(row['r10']), ex)
            continue
        if len(raw) < o2:
            continue
        if shuffled:
            ctx = ne_h3._unshuffle(raw[:o1], r0, n); obs = ne_h3._unshuffle(raw[o1:o2], r1, n)
        else:
            ctx = raw[:o1]; obs = raw[o1:o2]
        ids = np.frombuffer(obs, ca.OBS_DT, n)['id']
        want = want_by_r10.get(int(row['r10']))
        if want is None:
            continue
        m = (ids == want[0]) if want.size == 1 else np.isin(ids, want)
        if not m.any():
            continue
        k = int(m.sum())
        if k == n:
            out.append((row, obs, ctx, ids, n))
        else:
            o2d = np.frombuffer(obs, np.uint8, n * r1).reshape(n, r1)[m]
            c2d = np.frombuffer(ctx, np.uint8, n * r0).reshape(n, r0)[m]
            out.append((row, o2d.tobytes(), c2d.tobytes(), ids[m], n))
    return out


def _acc_for_cells_fast(ids: np.ndarray) -> dict:
    """Accumulator over exactly ``ids`` (sorted unique res-12) — same figures as
    ``_acc_from_table(ca.select(ids)…)`` incl. the cross-KG de-duplication rule
    (observed copy first, then the larger chunk, then the lowest kg)."""
    par = ne_h3.h3_parent_vec(ids, ca.CHUNK_RES)
    if not np.all(par[1:] >= par[:-1]):
        o = np.argsort(par, kind='stable'); par = par[o]; ids = ids[o]
    r10s, starts = np.unique(par, return_index=True)
    bounds = np.append(starts, ids.size)
    want_by_r10 = {int(r): ids[bounds[i]:bounds[i + 1]] for i, r in enumerate(r10s.tolist())}
    rows = ca._chunk_rows(r10s)
    by_kg: dict[str, list[dict]] = {}
    for r in rows:
        by_kg.setdefault(r['kg'], []).append(r)
    hdrs: dict = {}
    obs_parts: list[bytes] = []; ctx_parts: list[bytes] = []
    chunk_refs: list[_R10Ref] = []; src_chunk_parts: list[np.ndarray] = []; kg_parts: list[np.ndarray] = []
    kgs = sorted(by_kg)
    kg_rank = {k: i for i, k in enumerate(kgs)}
    for kg in kgs:
        h = ca.header(kg)
        if h is None:
            continue
        hdrs[kg] = h
        krows = sorted(by_kg[kg], key=lambda r: int(r['off']))
        try:
            with open(ca._file_path(kg), 'rb') as fh:
                for i in range(0, len(krows), CELLS_BATCH_ROWS):
                    for row, o, c, rid, n in _rows_wanted(kg, h, fh, krows[i:i + CELLS_BATCH_ROWS], want_by_r10):
                        ci = len(chunk_refs)
                        chunk_refs.append(_R10Ref(int(row['r10']), n))
                        obs_parts.append(o); ctx_parts.append(c)
                        src_chunk_parts.append(np.full(rid.size, ci, np.int64))
                        kg_parts.append(np.full(rid.size, kg_rank[kg], np.int64))
        except OSError as ex:
            log.warning('cells: %s unreadable: %s', kg, ex)
    if not obs_parts:
        t = ca.Table(np.zeros(0, np.uint64), np.zeros(0, ca.OBS_DT), np.zeros(0, np.int64), kgs,
                     np.zeros(0, np.int64), np.zeros(0, np.int64), [])
        return _acc_from_table(t, hdrs), hdrs
    obs_raw = b''.join(obs_parts); ctx_raw = b''.join(ctx_parts)
    obs_raw, _ = ca._green_fix.fix_rows(obs_raw, ctx_raw)
    obs = np.frombuffer(obs_raw, ca.OBS_DT)
    cid = obs['id']
    src_chunk = np.concatenate(src_chunk_parts); krank = np.concatenate(kg_parts)
    observed = (obs['hm'] != 255) | (obs['cover'].sum(axis=1) > 0)
    csize = np.array([c.cells for c in chunk_refs], np.int64)[src_chunk]
    order = np.lexsort((krank, -csize, ~observed, cid))
    ids_s = cid[order]
    first = np.concatenate([[True], ids_s[1:] != ids_s[:-1]])
    sel = order[first]
    t = ca.Table(cid[sel], obs[sel], krank[sel], kgs, src_chunk[sel], np.zeros(sel.size, np.int64), chunk_refs)
    return _acc_from_table(t, hdrs), hdrs


# --- job registry: cheap polls (repeat POST / GET key=) ----------------------
# ``cj:<key>`` rows in the unit_stats table carry the list meta (n, bbox, fp,
# started_at) so any gunicorn worker can answer a poll from the cache: result
# row present → 200, job younger than CELLS_JOB_STALE_S → 202 (no second
# compute), older → the holder of the ids recomputes, key-only callers get 404.

_body_lru: OrderedDict = OrderedDict()      # sha256(raw body) → (h, n, bbox, fp)


def _job_key(h: str) -> str:
    return f'cj:{h}'


def _job_get(h: str) -> dict | None:
    with _mem_lock:
        hit = _mem.get(_job_key(h))
    if hit is not None:
        return hit[1]
    hit = _db_get(_job_key(h), 'job')
    return hit[0] if hit else None


def _job_put(h: str, meta: dict) -> None:
    _store(_job_key(h), 'job', meta, persist=True)


def _cells_meta_from_ids(ids: np.ndarray) -> tuple[str, int, tuple, str]:
    n = int(ids.size)
    h = hashlib.sha256(ids.tobytes()).hexdigest()[:16]
    bbox = ca._bbox_of_ids(ids) if n else (0.0, 0.0, 0.0, 0.0)
    fp = _fingerprint_bbox(*bbox)[0] if n else 'empty'
    return h, n, bbox, fp


def cells_meta_for_body(raw: bytes, encoding: str | None) -> tuple[str, int, tuple, str, np.ndarray | None]:
    """(h, n, bbox, fp, ids|None) — ids is None when the raw body was seen before
    on this worker (no re-parse: the list hash alone addresses cache + job)."""
    bh = hashlib.sha256(raw).hexdigest()
    with _mem_lock:
        hit = _body_lru.get(bh)
        if hit is not None:
            _body_lru.move_to_end(bh)
    if hit is not None:
        return hit + (None,)
    ids = parse_cells_body(raw, encoding)
    meta = _cells_meta_from_ids(ids)
    with _mem_lock:
        _body_lru[bh] = meta
        while len(_body_lru) > BODY_LRU_N:
            _body_lru.popitem(last=False)
    return meta + (ids,)


def _cells_compute(ids: np.ndarray, n: int, bbox) -> dict:
    if n == 0:
        a = _acc_empty(); a['want'] = 0; a['miss'] = []; a['pend'] = []; a['pfiles'] = []
        return a
    if n >= CELLS_FAST_MIN_N:
        a, hdrs = _acc_for_cells_fast(ids)
        pfiles = [k for k, h in hdrs.items() if h.get('partial_cells')]
    else:
        t, hdrs, meta = ca.select(ids, bbox)
        a = _acc_from_table(t, hdrs)
        pfiles = meta.get('partial_files') or []
    a['miss'] = []          # bbox-of-list spans KGs the list never touches — report unknown cells instead
    a['pend'] = []
    a['pfiles'] = pfiles
    a['want'] = n
    a['list_area_m2'] = round(_cells_area_m2(ids), 1)
    return a


def cells_envelope(h: str, n: int, bbox, fp: str, acc: dict, ts: float, scale: int, year: str | None) -> dict:
    unit = {'kind': 'cells', 'key': h, 'bbox': [round(x, 6) for x in bbox] if n else None, 'n': n, 'scale': scale}
    if year:
        unit['year'] = year
    area_ha = (acc.get('list_area_m2') or 0.0) / 1e4 * scale
    note = f'{n} listed cells × {scale}' if scale != 1 else None
    env = _envelope(unit, acc, ts, cells_total=n * scale, area_ha=area_ha, note=note)
    if scale != 1:
        env['figures'] = _scale_figures(env['figures'], scale)
        env['coverage']['cells_known'] = int(acc['kn']) * scale
    return env


def unit_cells_by_key(h: str, scale: int, wait_s: float, year: str | None = None) -> dict | None:
    """GET key= poll: finished envelope, None = still computing. Raises 404 when
    no job for the key is known (or it expired without a result)."""
    meta = _job_get(h)
    if meta is None:
        raise StatsError(404, 'unknown cell-list key — POST the list first (stats-envelope.md §6)', key=h)
    n, bbox, fp = int(meta['n']), tuple(meta['bbox']), meta['fp']
    key = f'cl:{h}:{n}'
    hit = _cached(key, fp)
    if hit is None:
        with _inflight_lock:
            fut = _inflight.get(key)
        if fut is not None:
            try:
                hit = fut.result(timeout=max(0.0, wait_s))
            except TimeoutError:
                return None
        else:
            if time.time() - float(meta.get('started_at') or 0) > CELLS_JOB_TTL_S:
                raise StatsError(404, 'cell-list key expired — POST the list again', key=h)
            return None                 # computing on another worker (or stale: the next POST recomputes)
    return cells_envelope(h, n, bbox, fp, hit[0], hit[1], scale, year)


class NeedIds(Exception):
    """unit_cells was called with ids=None but has to compute — caller re-parses the body."""


def unit_cells(ids: np.ndarray | None, scale: int, wait_s: float, year: str | None = None,
               meta: tuple | None = None) -> dict | None:
    """Envelope over a cell list, None = computing.  ``meta`` = (h, n, bbox, fp)
    from ``cells_meta_for_body``; ``ids`` may be None for a repeat POST whose
    body this worker already parsed (raises ``NeedIds`` if it must compute)."""
    h, n, bbox, fp = meta if meta is not None else _cells_meta_from_ids(ids)
    key = f'cl:{h}:{n}'
    r = _cached(key, fp)
    if r is None:
        with _inflight_lock:
            local = key in _inflight
        if not local:
            job = _job_get(h)
            if (job is not None and job.get('fp') == fp
                    and time.time() - float(job.get('started_at') or 0) < CELLS_JOB_STALE_S):
                return None             # running on another worker — poll again (no duplicate compute)
            if ids is None:
                raise NeedIds()
            _job_put(h, {'n': n, 'bbox': list(bbox), 'fp': fp, 'started_at': time.time()})
        r = _get_or_compute(key, fp, lambda: _cells_compute(ids, n, bbox), wait_s=wait_s, persist=True)
        if r is None:
            return None
    acc, ts = r
    return cells_envelope(h, n, bbox, fp, acc, ts, scale, year)


# === SECTION: route ===

def _filter_keys(fig: dict, keys: list[str] | None) -> dict:
    if not keys:
        return fig
    return OrderedDict((k, v) for k, v in fig.items() if any(k == p or k.startswith(p) for p in keys))


def _csv_cell(v) -> str:
    if v is None:
        return ''
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (int, float)):
        return repr(v) if isinstance(v, float) else str(v)
    s = str(v)
    return '"' + s.replace('"', '""') + '"' if any(c in s for c in ',"\n') else s


def _unit_label(env: dict) -> str:
    k = env['unit']['key']
    return ','.join(str(x) for x in k) if isinstance(k, list) else str(k)


def _etag_for(envs: list[dict], keys, fmt) -> str:
    canon = '&'.join(f'{k}={v}' for k, v in sorted(request.args.items(multi=True)))
    sig = '|'.join(f"{_unit_label(e)}:{e['computed_at']}:{e['coverage']['cells_known']}" for e in envs)
    return '"su-' + hashlib.sha256((canon + '#' + sig + ALGO).encode()).hexdigest()[:16] + '"'


def _resp(body: bytes | str, mimetype: str, etag: str, status: int = 200) -> Response:
    cc = f'public, max-age={MAX_AGE}'
    inm = request.headers.get('If-None-Match') or ''
    if etag in [x.strip() for x in inm.split(',')]:
        return Response(status=304, headers={'ETag': etag, 'Cache-Control': cc})
    r = Response(body, status=status, mimetype=mimetype)
    r.headers['ETag'] = etag; r.headers['Cache-Control'] = cc
    r.headers['X-Stats-Schema'] = SCHEMA
    return r


@stats_bp.errorhandler(StatsError)
def _on_err(e: StatsError):
    d = {'error': e.msg, 'schema': SCHEMA}; d.update(e.extra)
    r = Response(ca._dumps(d), status=e.status, mimetype='application/json')
    r.headers['Cache-Control'] = 'no-store'
    return r


@stats_bp.route('/api/v1/stats/unit', methods=['GET', 'POST'])
def stats_unit_route():
    a = request.args
    if request.method == 'POST':
        if (a.get('unit') or 'cells') != 'cells':
            raise StatsError(400, 'POST takes unit=cells with a gzip\'d NEI1 body (stats-envelope.md §6)')
        sel = ['cells']
    else:
        sel = [k for k in ('h3', 'kg', 'bbox') if a.get(k)]
        if a.get('key') and (a.get('unit') or 'cells') == 'cells':
            sel = ['cells_key']
        if len(sel) != 1:
            raise StatsError(400, 'exactly one of h3=, kg=, bbox= is required',
                             example='/api/v1/stats/unit?kg=63330')
    fmt = (a.get('format') or 'json').lower()
    if fmt not in FORMATS:
        raise StatsError(400, f'format must be one of {FORMATS}')
    keys = [k.strip() for k in (a.get('keys') or '').split(',') if k.strip()] or None
    pretty = (a.get('pretty') or '') in ('1', 'true')
    t0 = time.time()
    deadline = t0 + BUDGET_S
    envs: list[dict] = []
    pending = 0
    kind = sel[0]
    if kind == 'kg':
        codes = sorted({c.strip() for c in a['kg'].replace(';', ',').split(',') if c.strip()})
        if not codes:
            raise StatsError(400, 'kg= is empty')
        if len(codes) > MAX_KG:
            raise StatsError(400, f'more than {MAX_KG} kg codes', max=MAX_KG, n=len(codes))
        for c in codes:
            env = unit_kg(c, max(0.0, deadline - time.time()))
            if env is None:
                pending += 1
            else:
                envs.append(env)
        multi = len(codes) > 1
    elif kind == 'h3':
        env = unit_h3(a['h3'], BUDGET_S)
        multi = False
        if env is None:
            pending = 1
        else:
            envs.append(env)
    elif kind == 'bbox':
        env = unit_bbox(a['bbox'], BUDGET_S)
        multi = False
        if env is None:
            pending = 1
        else:
            envs.append(env)
    else:
        try:
            scale = int(a.get('scale') or 1)
        except ValueError:
            raise StatsError(400, 'scale must be an integer')
        if not 1 <= scale <= CELLS_MAX_SCALE:
            raise StatsError(400, f'scale must be 1–{CELLS_MAX_SCALE}')
        year = (a.get('year') or '').strip() or None
        multi = False
        if kind == 'cells_key':
            h = a['key'].strip().lower()
            if not re.fullmatch(r'[0-9a-f]{16}', h):
                raise StatsError(400, 'key must be the 16-hex unit.key of a POSTed cell list', key=h)
            env = unit_cells_by_key(h, scale, CELLS_POLL_WAIT_S, year=year)
        else:
            raw = request.get_data(cache=False)
            enc = request.headers.get('Content-Encoding')
            h, n, bbox, fp, ids = cells_meta_for_body(raw, enc)
            meta = (h, n, bbox, fp)
            try:                        # repeat POST (body seen before) → cheap poll; first POST waits BUDGET_S
                env = unit_cells(ids, scale, BUDGET_S if ids is not None else CELLS_POLL_WAIT_S, year=year, meta=meta)
            except NeedIds:
                env = unit_cells(parse_cells_body(raw, enc), scale, BUDGET_S, year=year, meta=meta)
        pending_unit = {'kind': 'cells', 'key': h, 'scale': scale}
        if env is None:
            pending = 1
        else:
            envs.append(env)
    if pending:
        body = {'schema': SCHEMA, 'service': SERVICE, 'status': 'computing', 'retry_after_s': RETRY_AFTER_S,
                'units_ready': len(envs), 'units_pending': pending}
        if kind in ('cells', 'cells_key'):
            body['unit'] = pending_unit
            body['poll'] = f'/api/v1/stats/unit?unit=cells&key={pending_unit["key"]}&scale={scale}'
        r = Response(ca._dumps(body, pretty), status=202, mimetype='application/json')
        r.headers['Retry-After'] = str(RETRY_AFTER_S); r.headers['Cache-Control'] = 'no-store'
        return r
    for e in envs:
        e['figures'] = _filter_keys(e['figures'], keys)
    etag = _etag_for(envs, keys, fmt)
    if fmt == 'csv':
        cols = list(OrderedDict((k, 1) for e in envs for k in e['figures']).keys())
        lines = [','.join(['unit', 'coverage_complete'] + cols)]
        for e in envs:
            f = e['figures']
            lines.append(','.join([_csv_cell(_unit_label(e)), _csv_cell(e['coverage']['complete'])]
                                  + [_csv_cell(f.get(c)) for c in cols]))
        return _resp('\n'.join(lines) + '\n', 'text/csv; charset=utf-8', etag)
    if fmt == 'flat':
        if multi:
            body = [OrderedDict([('unit', _unit_label(e))] + list(e['figures'].items())) for e in envs]
        else:
            body = envs[0]['figures']
        return _resp(ca._dumps(body, pretty), 'application/json', etag)
    if multi:
        now = time.time()
        body = {'schema': SCHEMA, 'service': SERVICE, 'n': len(envs), 'units': envs,
                'computed_at': _iso(now), 'age_s': 0, 'attribution': _ATTR, 'license': 'CC-BY-4.0'}
    else:
        body = envs[0]
    r = _resp(ca._dumps(body, pretty), 'application/json', etag)
    r.headers['X-Stats-Ms'] = str(int((time.time() - t0) * 1000))
    return r
