"""Tree apices and structures for ``/api/v1/trees/bbox`` — read straight from
the NE cells corpus (``data/ne/<kg>.h3b``, NET1 / NEB1 rows), no second copy
on disk.

Why: the legacy ``search_index.kg_trees`` table (sampled, h ≥ 20 m) stopped
being written with the Kataster purge (2026-10-13) and was emptied by the
next force rebuild — ``/trees/bbox`` then answered ``ready:true, count:0``
nationwide.  The NE corpus already carries EVERY apex (all heights) and
every structure, so the endpoint now queries it directly.

Tallest-first with a limit needs an accelerator: ``cells.sqlite`` ``chunk``
gets ``hmax`` / ``bmax`` columns (quarter-metres, 0–255) = tallest apex /
structure per res-10 chunk, filled by ``cells_api.index_file`` (ingest) and
``backfill_hmax`` (one-off / lazy).  A query walks the chunks of the bbox
by that max desc and stops as soon as the next chunk cannot beat the
limit-th object found.  Chunks whose max is unknown (NULL) are scanned first.

``realistic`` (default on) removes what the QA rules / flag stores call
unrealistic: data-derived height rules (see ``TREE_*`` constants), apices / structures with a high-or-
critical row in ``feedback.sqlite`` (``flags``), and anything in a res-12
cell that carries an open / confirmed public flag with a negative claim
(``public_flags``: too_tall, spurious, artifact, misplaced, wrong_height).
"""
from __future__ import annotations

import logging
import math
import threading
import time

import numpy as np

import cells_api as ca

log = logging.getLogger('ne_trees')

KINDS = ('trees', 'structures')
# Tree plausibility rules, derived from 18 M apices in 250 random KGs (2026-11):
# counts per 5 m band decay ~x0.1 above 40 m (52k/4k/405/149) then jump to 448 in
# the saturated >= 60 m bin -> that bin is ~90 % masts / wind turbines / cranes.
# A "spike" (tallest apex of its res-10 chunk with h/p90 > 1.6, or > 15 m above
# the second tallest) occurs in 0.2-0.4 % of ordinary trees but in 27 / 48 / 69 %
# of the 50-55 / 55-60 / >= 60 m bands.  Below 45 m spikes are real trees.
TREE_H_SATURATED = 60.0         # >= this: quantisation ceiling, treated as non-tree
TREE_SPIKE_H = 45.0             # spike rules apply from here
TREE_SPIKE_RATIO = 1.6          # h / p90(chunk)
TREE_SPIKE_GAP = 15.0           # h - second tallest in chunk (m)
STRUCT_H_MAX = {                # per type, metres (realistic=1)
    'roof': 250.0, 'greenhouse': 15.0, 'solar_panel': 12.0, 'mast': 350.0, 'wind_turbine': 260.0,
    'substation': 40.0, 'bridge': 200.0, 'wall': 12.0, 'fence': 6.0,
}
STRUCT_CONF_MIN = 0.2
NEGATIVE_CLAIMS = ('too_tall', 'spurious', 'artifact', 'misplaced', 'wrong_height')
FLAG_LAYERS = {'trees': ('trees', 'cells.trees'), 'structures': ('structures', 'cells.structures', 'landmarks', 'buildings')}
LEGACY_FLAG_KIND = {'trees': 'top_tree', 'structures': 'top_by_type'}
MAX_LIMIT = 20000              # hard cap; default 2000 (a 5 km² alpine tile holds >2000 apices ≥ 35 m)
DEFAULT_LIMIT = 2000
MAX_AREA_DEG2 = 0.25
CHUNK_BUDGET = 6000             # hard cap on chunk inflations per request (~1.5 s cold)
EDGE_PAD_DEG = 0.0015           # ~110 m: r10 cells whose centre is just outside still hold bbox objects
_COL = {'trees': 'hmax', 'structures': 'bmax'}
_CNT = {'trees': 'trees', 'structures': 'bld'}


# ── index column (hmax / bmax) ─────────────────────────────────────────

def ensure_schema(conn=None) -> None:
    c = conn or ca._idx_conn(write=True)
    if c is None:
        return
    cols = {r[1] for r in c.execute('PRAGMA table_info(chunk)')}
    with ca._idx_write_lock:
        for col in ('hmax', 'bmax'):
            if col not in cols:
                c.execute(f'ALTER TABLE chunk ADD COLUMN {col} INTEGER')
        c.commit()


def _have_cols(c) -> bool:
    cols = {r[1] for r in c.execute('PRAGMA table_info(chunk)')}
    return 'hmax' in cols and 'bmax' in cols


def chunk_max_from_file(kg: str, hdr: dict | None = None) -> dict[int, tuple[int, int]]:
    """{r10: (hmax_q, bmax_q)} for every chunk of ``<kg>.h3b`` (q = m*4; 0 = none)."""
    kg = str(kg).zfill(5)
    h = hdr or ca.header(kg, with_chunks=True)
    out: dict = {}
    if not h:
        return out
    for hrow in h.get('chunks') or []:
        r10 = int(hrow['r10'], 16)
        nt, nb = int(hrow.get('trees') or 0), int(hrow.get('bld') or 0)
        if not nt and not nb:
            out[r10] = (0, 0)
            continue
        row = dict(hrow, r10=r10, trees=nt, bld=nb)
        try:
            ch = ca._load_chunk(kg, row, h)
        except Exception as ex:  # noqa: BLE001
            log.warning('ne_trees: chunk %s/%x unreadable for hmax: %s', kg, r10, ex)
            continue
        tw = ch.trees.astype(np.uint32); bw = ch.bld.astype(np.uint64)
        hq = int(((tw >> 10) & 255).max()) if tw.size else 0
        bq = int(((bw >> np.uint64(10)) & np.uint64(255)).max()) if bw.size else 0
        out[r10] = (hq, bq)
    return out


def write_max(kg: str, hm: dict, conn=None) -> int:
    c = conn or ca._idx_conn(write=True)
    if c is None or not hm:
        return 0
    ensure_schema(c)
    kg = str(kg).zfill(5)
    with ca._idx_write_lock:
        c.executemany('UPDATE chunk SET hmax=?, bmax=? WHERE r10=? AND kg=?',
                      [(v[0], v[1], k, kg) for k, v in hm.items()])
        c.commit()
    return len(hm)


def index_kg(kg: str, hdr: dict | None = None) -> int:
    """Hook for ``cells_api.index_file``: fill hmax/bmax for a freshly (re)indexed file."""
    return write_max(kg, chunk_max_from_file(kg, hdr))


def backfill_hmax(limit: int | None = None, log_every: int = 200) -> dict:
    """Fill ``chunk.hmax/bmax`` for every KG that still has NULLs.  Idempotent;
    safe to run while serving (short write transactions per KG)."""
    c = ca._idx_conn(write=True)
    ensure_schema(c)
    kgs = [r[0] for r in c.execute('SELECT DISTINCT kg FROM chunk WHERE hmax IS NULL OR bmax IS NULL ORDER BY kg')]
    if limit:
        kgs = kgs[:limit]
    t0 = time.time(); n = 0
    for i, kg in enumerate(kgs, 1):
        try:
            n += write_max(kg, chunk_max_from_file(kg), c)
        except Exception as ex:  # noqa: BLE001
            log.warning('ne_trees backfill %s: %s', kg, ex)
        ca._chunk_lru.pop_if(lambda k, _kg=kg: k[0] == _kg)   # don't let the scan evict hot chunks
        if log_every and i % log_every == 0:
            log.info('ne_trees backfill: %d/%d KGs, %d chunks, %.0fs', i, len(kgs), n, time.time() - t0)
    return {'kgs': len(kgs), 'chunks': n, 'seconds': round(time.time() - t0, 1)}


_bf_lock = threading.Lock()
_bf_state = {'running': False, 'last': None}


def backfill_in_background() -> bool:
    """Start one lazy backfill thread per process if the index has NULLs."""
    st = status()
    if not st['ok'] or not st['missing']:
        return False
    with _bf_lock:
        if _bf_state['running']:
            return False
        _bf_state['running'] = True

    def run():
        try:
            _bf_state['last'] = backfill_hmax()
        except Exception as ex:  # noqa: BLE001
            log.warning('ne_trees background backfill: %s', ex)
        finally:
            _bf_state['running'] = False

    threading.Thread(target=run, name='ne_trees_backfill', daemon=True).start()
    return True


def status() -> dict:
    """Health block: is the apex path answerable at all?"""
    c = ca._idx_conn()
    if c is None:
        return {'ok': False, 'files': 0, 'chunks': 0, 'trees': 0, 'structures': 0, 'missing': 0,
                'error': 'no cells index'}
    files, chunks, trees, bld = c.execute('SELECT (SELECT count(*) FROM file), count(*), sum(trees), sum(bld) FROM chunk').fetchone()
    miss = -1
    if _have_cols(c):
        miss = int(c.execute('SELECT sum(hmax IS NULL OR bmax IS NULL) FROM chunk').fetchone()[0] or 0)
    out = {'ok': bool(files and trees), 'files': int(files or 0), 'chunks': int(chunks or 0),
           'trees': int(trees or 0), 'structures': int(bld or 0), 'missing': miss,
           'backfill_running': _bf_state['running']}
    if not out['ok']:
        out['error'] = 'cells index has no files/trees — ne_ingest / cells_api.rebuild_index()'
    return out


# ── QA: "realistic" filter ─────────────────────────────────────────────

_pf_cache: dict = {}
_pf_lock = threading.Lock()
_PF_TTL = 600.0


def flagged_cells(kg: str, kind: str) -> set[int]:
    """res-12 cell ids in ``kg`` with an open / confirmed public flag on this
    kind's layers and a negative claim (public_flags.sqlite)."""
    key = (kg, kind); now = time.time()
    with _pf_lock:
        hit = _pf_cache.get(key)
        if hit and now - hit[0] < _PF_TTL:
            return hit[1]
    cells: set[int] = set()
    try:
        import public_flags as pf
        c = pf._conn()
        pf._load_dict(c)
        lay = [pf._DICT.get(('layer', l)) for l in FLAG_LAYERS[kind]]
        clm = [pf._DICT.get(('claim', x)) for x in NEGATIVE_CLAIMS]
        lay = [x for x in lay if x is not None]; clm = [x for x in clm if x is not None]
        if lay and clm:
            q = ('SELECT cell FROM report WHERE kg=? AND cell IS NOT NULL AND status IN (0,1) '
                 'AND layer IN (%s) AND claim IN (%s)' % (','.join('?' * len(lay)), ','.join('?' * len(clm))))
            cells = {int(r[0]) for r in c.execute(q, (kg, *lay, *clm))}
    except Exception as ex:  # noqa: BLE001
        log.debug('ne_trees flagged_cells %s: %s', kg, ex)
    with _pf_lock:
        _pf_cache[key] = (now, cells)
        if len(_pf_cache) > 4000:
            _pf_cache.pop(next(iter(_pf_cache)))
    return cells


def legacy_flagged_points(kg: str, kind: str) -> set:
    try:
        import landscape_query as lq
        return lq.flagged_points(kg, LEGACY_FLAG_KIND[kind])
    except Exception:  # noqa: BLE001
        return set()


# ── query ──────────────────────────────────────────────────────────────

def _chunk_rows_bbox(w, s, e, n, kind: str) -> list[dict]:
    """chunk rows (with hmax/bmax when known) for the res-10 cells of the padded bbox."""
    r10s = ca.ids_in_bbox(w - EDGE_PAD_DEG, s - EDGE_PAD_DEG, e + EDGE_PAD_DEG, n + EDGE_PAD_DEG, res=10)
    c = ca._idx_conn()
    if r10s.size == 0 or c is None:
        return []
    col = _COL[kind] if _have_cols(c) else 'NULL'
    sel = f'c.r10, c.kg, c.off, c.len, c.raw_len, c.cells, c.trees, c.bld, f.mtime, {col}'
    ids = [int(x) for x in np.unique(r10s).tolist()]
    out = []; idx_mtime = {}
    for i in range(0, len(ids), ca.MAX_SQL_IN):
        part = ids[i:i + ca.MAX_SQL_IN]
        q = f'SELECT {sel} FROM chunk c LEFT JOIN file f ON f.kg = c.kg WHERE c.r10 IN (%s)' % ','.join('?' * len(part))
        for row in c.execute(q, part):
            d = dict(zip(('r10', 'kg', 'off', 'len', 'raw_len', 'cells', 'trees', 'bld'), row[:8]))
            d['max'] = row[9]
            out.append(d); idx_mtime[row[1]] = row[8]
    rows = ca._reconcile_stale_rows(out, idx_mtime, set(ids))
    for d in rows:
        d.setdefault('max', None)
    return rows


def _decode(kind: str, ch) -> tuple[np.ndarray, np.ndarray, dict]:
    """→ (heights, per-object cell row index, decoded columns) for one chunk."""
    if kind == 'trees':
        d = ca.decode_trees(ch.trees)
        cnt = ch.obs['tn'].astype(np.int64); n = ch.trees.size; h = d['h_m']
    else:
        d = ca.decode_structures(ch.bld)
        cnt = ch.obs['sn'].astype(np.int64); n = ch.bld.size; h = d['h_max_m']
    cell_idx = np.repeat(np.arange(ch.cells), cnt)[:n]
    return h, cell_idx, d


def points_bbox(w, s, e, n, kind: str = 'trees', min_height: float = 0.0, limit: int = DEFAULT_LIMIT,
                realistic: bool = True, types: set | None = None, max_height: float | None = None) -> dict:
    """All objects of ``kind`` in bbox, tallest first, ≤ limit.

    ``max_height`` makes a half-open window ``min_height <= h < max_height``
    so adjacent bands (20–25, 25–30 …) partition the apices without
    double-counting.  The chunk accelerator still applies: chunks are walked
    by their tallest object, chunks whose max is below ``min_height`` end
    the scan, and once ``limit`` window objects are in hand no chunk whose
    max cannot beat the limit-th one is opened."""
    if kind not in KINDS:
        raise ValueError(f'kind must be one of {KINDS}')
    t0 = time.time()
    lim = max(1, min(int(limit), MAX_LIMIT))
    min_h = max(0.0, float(min_height))
    max_h = None if max_height is None else float(max_height)
    if max_h is not None and max_h <= min_h:
        raise ValueError('max_height must be greater than min_height')
    kgs_all = ca._kgs_intersecting(w, s, e, n)
    files = ca._files()
    kgs_have = [k for k in kgs_all if k in files]
    pending, not_proc = ca.split_missing([k for k in kgs_all if k not in files])
    cnt_key = _CNT[kind]
    rows = [r for r in _chunk_rows_bbox(w, s, e, n, kind) if int(r.get(cnt_key) or 0) > 0]
    rows.sort(key=lambda r: -(255 if r['max'] is None else int(r['max'])))   # unknown first, then tallest
    tf = ca._tf('EPSG:3035', 'EPSG:4326')
    type_codes = None
    if kind == 'structures' and types:
        type_codes = {i for i, t in enumerate(ca.STRUCTURE_TYPES) if t in types}
    hdrs: dict = {}
    scanned = 0; exhausted = True; kth = -1.0
    acc: dict = {}; n_acc = 0            # accepted (inside bbox) column arrays
    bat: dict = {}; n_bat = 0            # pending batch in EPSG:3035, not yet bbox-filtered
    dropped = 0

    def flush():
        nonlocal n_bat, n_acc, kth
        if not n_bat:
            return
        cols = {k: np.concatenate(v) for k, v in bat.items()}
        lon, lat = tf.transform(cols.pop('x'), cols.pop('y'))
        lon = np.asarray(lon); lat = np.asarray(lat)
        inb = (lon >= w) & (lon <= e) & (lat >= s) & (lat <= n)
        if inb.any():
            acc.setdefault('lon', []).append(lon[inb]); acc.setdefault('lat', []).append(lat[inb])
            for k, v in cols.items():
                acc.setdefault(k, []).append(v[inb])
            n_acc += int(inb.sum())
            if n_acc >= lim:
                h_all = np.concatenate(acc['h'])
                kth = float(np.partition(h_all, h_all.size - lim)[h_all.size - lim])
        bat.clear(); n_bat = 0

    for r in rows:
        mq = r['max']
        if mq is not None:
            hm = mq / 4.0
            if hm < min_h:
                break                       # nothing left ≥ min_height
            if n_bat and (n_bat >= 4 * lim or hm <= kth):
                flush()
            if n_acc >= lim and hm <= kth:
                break                       # cannot beat the limit-th object
        if scanned >= CHUNK_BUDGET:
            exhausted = False
            break
        kg = r['kg']
        h = hdrs.get(kg)
        if h is None:
            h = ca.header(kg)
            if h is None:
                continue
            hdrs[kg] = h
        try:
            ch = ca._load_chunk(kg, r, h)
        except Exception as ex:  # noqa: BLE001
            log.warning('ne_trees: chunk %s/%x unreadable: %s', kg, int(r['r10']), ex)
            continue
        scanned += 1
        hs, cell_idx, d = _decode(kind, ch)
        if hs.size == 0:
            continue
        m = hs >= min_h
        if max_h is not None:
            m &= hs < max_h
        if realistic:
            if kind == 'trees':
                m &= hs < TREE_H_SATURATED
                if hs.size > 1 and hs.max() >= TREE_SPIKE_H:
                    srt = np.sort(hs)
                    p90 = float(np.percentile(hs, 90)); sec = float(srt[-2])
                    spike = (hs >= TREE_SPIKE_H) & ((hs / max(p90, 1.0) > TREE_SPIKE_RATIO) | (hs - sec > TREE_SPIKE_GAP))
                    m &= ~spike
            else:
                cap = np.array([STRUCT_H_MAX.get(t, 300.0) for t in ca.STRUCTURE_TYPES] + [300.0] * (16 - len(ca.STRUCTURE_TYPES)))
                m &= (hs <= cap[d['type']]) & (d['conf'] >= STRUCT_CONF_MIN)
            fc = flagged_cells(kg, kind)
            if fc:
                m &= ~np.isin(ch.ids[cell_idx], np.fromiter(fc, np.uint64))
        if type_codes is not None:
            m &= np.isin(d['type'], list(type_codes))
        if not m.any():
            continue
        sel = np.nonzero(m)[0]
        ci = cell_idx[sel]
        cen = ch.centres()
        bat.setdefault('x', []).append(cen[ci, 0] + d['dx'][sel]); bat.setdefault('y', []).append(cen[ci, 1] + d['dy'][sel])
        bat.setdefault('h', []).append(hs[sel])
        bat.setdefault('cell', []).append(ch.ids[ci].astype(np.uint64))
        for k, v in d.items():
            if k not in ('dx', 'dy', 'h_m', 'h_max_m'):
                bat.setdefault(k, []).append(v[sel])
        bat.setdefault('kg', []).append(np.full(sel.size, kg, dtype=object))
        n_bat += int(sel.size)
    flush()
    out = []
    if n_acc:
        a = {k: np.concatenate(v) for k, v in acc.items()}
        order = np.argsort(-a['h'], kind='stable')
        legacy: dict = {}
        for i in order.tolist():
            kg = a['kg'][i]; lon = float(a['lon'][i]); lat = float(a['lat'][i])
            if realistic:
                fs = legacy.get(kg)
                if fs is None:
                    fs = legacy[kg] = legacy_flagged_points(kg, kind)
                if fs and (round(lon, 5), round(lat, 5)) in fs:
                    dropped += 1
                    continue
            rec = {'lon': round(lon, 7), 'lat': round(lat, 7), 'height_m': round(float(a['h'][i]), 2)}
            if kind == 'trees':
                crown_m2 = float(a['crown_m2'][i])
                rec.update(crown_d_m=round(2.0 * math.sqrt(crown_m2 / math.pi), 1) if crown_m2 > 0 else None,
                           crown_m2=crown_m2,
                           species=ca.SPECIES[int(a['species'][i])] if int(a['species'][i]) < len(ca.SPECIES) else None,
                           vitality=ca.VITALITY[int(a['vitality'][i])] if int(a['vitality'][i]) < len(ca.VITALITY) else None,
                           ndsm_only=bool(a['ndsm_only'][i]), confidence=None)
            else:
                t = int(a['type'][i])
                rec.update(type=ca.STRUCTURE_TYPES[t] if t < len(ca.STRUCTURE_TYPES) else None,
                           height_robust_m=round(float(a['h_robust_m'][i]), 2),
                           area_m2=int(a['area_m2'][i]), confidence=round(float(a['conf'][i]), 2),
                           dh_m=round(float(a['dh_m'][i]), 2))
            rec['cell'] = f"{int(a['cell'][i]):x}"
            rec['kg_code'] = kg
            out.append(rec)
            if len(out) > lim:
                break
    truncated = len(out) > lim or not exhausted
    return {'kind': kind, 'trees' if kind == 'trees' else 'structures': out[:lim], 'kgs': kgs_have,
            'min_height': min_h, 'max_height': max_h, 'limit': lim,
            'pending_kgs': pending, 'not_processed': not_proc, 'dropped_qa': dropped,
            'realistic': bool(realistic), 'truncated': truncated, 'chunks_scanned': scanned,
            'chunks_in_bbox': len(rows), 'ms': round((time.time() - t0) * 1000, 1),
            'ready': bool(kgs_have) and not pending}


def trees_bbox(w, s, e, n, min_height: float = 0.0, limit: int = DEFAULT_LIMIT, realistic: bool = True,
               max_height: float | None = None) -> dict:
    return points_bbox(w, s, e, n, 'trees', min_height=min_height, limit=limit, realistic=realistic, max_height=max_height)


def structures_bbox(w, s, e, n, min_height: float = 0.0, limit: int = DEFAULT_LIMIT, realistic: bool = True,
                    types: set | None = None, max_height: float | None = None) -> dict:
    return points_bbox(w, s, e, n, 'structures', min_height=min_height, limit=limit, realistic=realistic, types=types,
                       max_height=max_height)
