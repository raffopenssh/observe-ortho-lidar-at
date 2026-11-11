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


# ── decoded-chunk cache ────────────────────────────────────────────────
# One fully decoded, reprojected chunk per (kg, digest, r10, kind): lon/lat,
# heights, attributes and the *height-independent* part of the realistic
# rule (saturation / spike / per-type caps / confidence).  A band request
# (3–8, 8–14 …) over an already-seen tile is then a boolean mask per chunk —
# no inflate, no decode, no pyproj — so a client's 4 parallel bands cost
# roughly one scan.  The bbox / min_h / max_h / flag filters stay per request.

def _dec_bytes(d: dict) -> int:
    return sum(int(v.nbytes) for v in d.values() if hasattr(v, 'nbytes')) + 256


import mem_lru as _mem_lru
_dec_lru = _mem_lru.ByteLRU('ne_trees_decoded', 96, sizer=_dec_bytes, env='NE_TREES_DECODED_MB')
_STRUCT_CAP = None


def _struct_cap() -> np.ndarray:
    global _STRUCT_CAP
    if _STRUCT_CAP is None:
        _STRUCT_CAP = np.array([STRUCT_H_MAX.get(t, 300.0) for t in ca.STRUCTURE_TYPES]
                               + [300.0] * (16 - len(ca.STRUCTURE_TYPES)))
    return _STRUCT_CAP


_dec_locks = [threading.Lock() for _ in range(256)]   # striped single-flight: concurrent bands of a cold
                                                        # tile decode each chunk once, not once per request


def _decoded_chunk(kg: str, r: dict, hdr: dict, kind: str) -> dict | None:
    key = (kg, hdr['neo1_digest'], int(r['r10']), kind)
    d = _dec_lru.get(key)
    if d is not None:
        return d
    with _dec_locks[hash(key) & 255]:
        d = _dec_lru.get(key)
        if d is not None:
            return d
        return _decode_chunk_uncached(key, kg, r, hdr, kind)


def _decode_chunk_uncached(key, kg: str, r: dict, hdr: dict, kind: str) -> dict:
    ch = ca._load_chunk(kg, r, hdr)
    hs, cell_idx, dec = _decode(kind, ch)
    if hs.size == 0:
        d = {'n': 0}
        _dec_lru.put(key, d)
        return d
    cen = ch.centres()
    x = cen[cell_idx, 0] + dec['dx']; y = cen[cell_idx, 1] + dec['dy']
    lon, lat = ca._tf('EPSG:3035', 'EPSG:4326').transform(x, y)
    d = {'n': int(hs.size), 'lon': np.asarray(lon, np.float64), 'lat': np.asarray(lat, np.float64),
         'x': x.astype(np.int32), 'y': y.astype(np.int32), 'h': hs.astype(np.float32),
         'cell': ch.ids[cell_idx].astype(np.uint64)}
    if kind == 'trees':
        ok = hs < TREE_H_SATURATED
        if hs.size > 1 and hs.max() >= TREE_SPIKE_H:
            srt = np.sort(hs)
            p90 = float(np.percentile(hs, 90)); sec = float(srt[-2])
            spike = (hs >= TREE_SPIKE_H) & ((hs / max(p90, 1.0) > TREE_SPIKE_RATIO) | (hs - sec > TREE_SPIKE_GAP))
            ok &= ~spike
        d.update(crown_m2=dec['crown_m2'].astype(np.float32), species=dec['species'].astype(np.uint8),
                 vitality=dec['vitality'].astype(np.uint8), ndsm_only=dec['ndsm_only'].astype(bool))
    else:
        ok = (hs <= _struct_cap()[dec['type']]) & (dec['conf'] >= STRUCT_CONF_MIN)
        d.update(type=dec['type'].astype(np.uint8), h_robust_m=dec['h_robust_m'].astype(np.float32),
                 area_m2=dec['area_m2'].astype(np.int32), conf=dec['conf'].astype(np.float32),
                 dh_m=dec['dh_m'].astype(np.float32))
    d['ok'] = ok
    _dec_lru.put(key, d)
    return d


# ── per-request state (ETag / Last-Modified) ───────────────────────────

def bbox_state(w, s, e, n) -> dict:
    """What the answer for this bbox depends on: the NE files of the
    intersecting KGs (neo1_digest changes only when a KG is reprocessed /
    re-ingested) + the newest of their mtimes.  Cheap (index only) — the
    public route hashes ``digests`` into the ETag *before* scanning so a
    matching If-None-Match costs no chunk work."""
    kgs_all = ca._kgs_intersecting(w, s, e, n)
    files = ca._files()
    have = [k for k in kgs_all if k in files]
    pending, not_proc = ca.split_missing([k for k in kgs_all if k not in files])
    digs = sorted(f'{k}:{files[k].get("neo1_digest")}' for k in have)
    mt = max([float(files[k].get('mtime') or 0) for k in have] or [0.0])
    # ready = nothing of the bbox is still on its way: a bbox that intersects
    # no KG at all (outside Austria) is a stable, cacheable empty answer.
    return {'kgs': have, 'pending': pending, 'not_processed': not_proc, 'digests': digs, 'mtime': mt,
            'n_kgs': len(kgs_all), 'ready': not pending and (bool(have) or not kgs_all)}


# ── query ──────────────────────────────────────────────────────────────

MAX_LIMIT_COLUMNS = 100000      # format=columns: one call carries a whole 0.025° tile (≈ 31k apices ≥ 3 m)
_TREE_FIELDS = ('lon', 'lat', 'h_m', 'crown_m2', 'species', 'vitality', 'ndsm_only', 'cell', 'kg_code', 'x', 'y', 'taxon', 'h_user_m', 'dbh_cm', 'tree_id')
_STRUCT_FIELDS = ('lon', 'lat', 'h_m', 'type', 'h_robust_m', 'area_m2', 'confidence', 'dh_m', 'cell', 'kg_code', 'x', 'y')
DEFAULT_COLUMNS = {'trees': ('lon', 'lat', 'h_m', 'crown_m2'), 'structures': ('lon', 'lat', 'h_m', 'type')}
COLUMN_FIELDS = {'trees': _TREE_FIELDS, 'structures': _STRUCT_FIELDS}


def collect(w, s, e, n, kind: str = 'trees', min_height: float = 0.0, limit: int = DEFAULT_LIMIT,
            realistic: bool = True, types: set | None = None, max_height: float | None = None,
            max_limit: int = MAX_LIMIT) -> dict:
    """Column arrays (numpy) of all objects of ``kind`` in bbox, tallest first,
    ≤ limit (+1 row when more exist, so the caller can set ``truncated``).

    ``max_height`` makes a half-open window ``min_height <= h < max_height``
    so adjacent bands partition the apices.  Chunk accelerator: chunks are
    walked by their tallest object (``chunk.hmax/bmax``); chunks whose max is
    below ``min_height`` end the scan; once ``limit`` window objects are in
    hand no chunk whose max cannot beat the limit-th one is opened."""
    if kind not in KINDS:
        raise ValueError(f'kind must be one of {KINDS}')
    t0 = time.time()
    lim = max(1, min(int(limit), max_limit))
    min_h = max(0.0, float(min_height))
    max_h = None if max_height is None else float(max_height)
    if max_h is not None and max_h <= min_h:
        raise ValueError('max_height must be greater than min_height')
    st = bbox_state(w, s, e, n)
    cnt_key = _CNT[kind]
    rows = [r for r in _chunk_rows_bbox(w, s, e, n, kind) if int(r.get(cnt_key) or 0) > 0]
    rows.sort(key=lambda r: -(255 if r['max'] is None else int(r['max'])))   # unknown first, then tallest
    type_codes = None
    if kind == 'structures' and types:
        type_codes = np.array(sorted(i for i, t in enumerate(ca.STRUCTURE_TYPES) if t in types), dtype=np.uint8)
    hdrs: dict = {}
    scanned = 0; exhausted = True; kth = -1.0; n_at_kth = 0
    parts: list[tuple[dict, np.ndarray, str]] = []     # (decoded chunk, selected idx, kg)
    n_acc = 0; dropped = 0
    legacy: dict = {}

    def kth_now() -> float:
        hs = np.concatenate([p[0]['h'][p[1]] for p in parts])
        return float(np.partition(hs, hs.size - lim)[hs.size - lim])

    for r in rows:
        mq = r['max']
        if mq is not None:
            hm = mq / 4.0
            if hm < min_h:
                break                       # nothing left ≥ min_height
            if n_acc >= lim:
                # a stale kth is a lower bound → still a safe cut-off; refresh when the
                # accepted set grew ≥ 10 % (np.partition over 20k+ rows per chunk is not free)
                if kth < 0 or n_acc >= n_at_kth * 1.1:
                    kth = kth_now(); n_at_kth = n_acc
                if hm <= kth:
                    break                   # cannot beat the limit-th object
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
            d = _decoded_chunk(kg, r, h, kind)
        except Exception as ex:  # noqa: BLE001
            log.warning('ne_trees: chunk %s/%x unreadable: %s', kg, int(r['r10']), ex)
            continue
        scanned += 1
        if not d or not d['n']:
            continue
        hs = d['h']
        m = (hs >= min_h) & (d['lon'] >= w) & (d['lon'] <= e) & (d['lat'] >= s) & (d['lat'] <= n)
        if max_h is not None:
            m &= hs < max_h
        if realistic:
            m &= d['ok']
            fc = flagged_cells(kg, kind)
            if fc:
                m &= ~np.isin(d['cell'], np.fromiter(fc, np.uint64))
            fs = legacy.get(kg)
            if fs is None:
                fs = legacy[kg] = legacy_flagged_points(kg, kind)
            if fs and m.any():
                idx = np.nonzero(m)[0]
                bad = [i for i in idx.tolist() if (round(float(d['lon'][i]), 5), round(float(d['lat'][i]), 5)) in fs]
                if bad:
                    m[bad] = False; dropped += len(bad)
        if type_codes is not None:
            m &= np.isin(d['type'], type_codes)
        if not m.any():
            continue
        sel = np.nonzero(m)[0]
        parts.append((d, sel, kg))
        n_acc += int(sel.size)
    out: dict = {}
    if n_acc:
        cols = [k for k in parts[0][0] if k not in ('n', 'ok')]
        for k in cols:
            out[k] = np.concatenate([p[0][k][p[1]] for p in parts])
        out['kg'] = np.concatenate([np.full(p[1].size, p[2], dtype=object) for p in parts])
        order = np.argsort(-out['h'], kind='stable')[:lim + 1]
        for k in list(out):
            out[k] = out[k][order]
    n_out = int(out['h'].size) if n_acc else 0
    truncated = n_out > lim or not exhausted
    if n_out > lim:
        for k in list(out):
            out[k] = out[k][:lim]
        n_out = lim
    return {'kind': kind, 'cols': out, 'n': n_out, 'kgs': st['kgs'], 'min_height': min_h, 'max_height': max_h,
            'limit': lim, 'pending_kgs': st['pending'], 'not_processed': st['not_processed'],
            'dropped_qa': dropped, 'realistic': bool(realistic), 'truncated': truncated,
            'chunks_scanned': scanned, 'chunks_in_bbox': len(rows), 'ms': round((time.time() - t0) * 1000, 1),
            'ready': st['ready'], 'state': st, **_user_blocks(kind, out if n_out else None, st['kgs'], w, s, e, n, min_h, max_h)}


def _user_blocks(kind, cols, kgs, w, s, e, n, min_h, max_h) -> dict:
    """What people told us about trees of this result (tree_user.py): ``user``
    = sparse {i, taxon, n, v} over the result rows, ``added`` = trees we have
    no apex for.  Empty dict (zero bytes on the wire) when the KGs carry none."""
    if kind != 'trees':
        return {}
    try:
        import tree_user as tu
        d = {}
        u = tu.annotate(cols['x'], cols['y'], cols['h'], kgs) if cols else None
        if u:
            d['user'] = u
        a = tu.added(w, s, e, n, kgs, min_h, max_h)
        if a:
            d['added'] = a
        a = tu.reported(w, s, e, n, kgs, min_h, max_h)
        if a:
            d['reported'] = a
        return d
    except Exception as ex:  # noqa: BLE001
        log.warning('ne_trees: user blocks: %s', ex)
        return {}


def _species(a: np.ndarray) -> list:
    return [ca.SPECIES[int(v)] if int(v) < len(ca.SPECIES) else None for v in a.tolist()]


def _vitality(a: np.ndarray) -> list:
    return [ca.VITALITY[int(v)] if int(v) < len(ca.VITALITY) else None for v in a.tolist()]


def _stype(a: np.ndarray) -> list:
    return [ca.STRUCTURE_TYPES[int(v)] if int(v) < len(ca.STRUCTURE_TYPES) else None for v in a.tolist()]


def columns_bbox(w, s, e, n, kind: str = 'trees', fields: tuple | None = None, **kw) -> dict:
    """``format=columns``: parallel arrays, tallest first.  Default fields
    lon, lat, h_m, crown_m2 (trees) / lon, lat, h_m, type (structures);
    ``fields=`` picks from :data:`COLUMN_FIELDS` (species, vitality, ndsm_only,
    cell, kg_code, x, y = EPSG:3035 integer metres …).  lon/lat are rounded to
    1e-6° (≈ 0.1 m, finer than the metre-snapped source), h_m to 0.25 m,
    crown_m2 to 0.1 m² — ~7 B/apex gzipped."""
    kw.setdefault('limit', MAX_LIMIT_COLUMNS)
    r = collect(w, s, e, n, kind, max_limit=MAX_LIMIT_COLUMNS, **kw)
    want = tuple(fields or DEFAULT_COLUMNS[kind])
    bad = [f for f in want if f not in COLUMN_FIELDS[kind]]
    if bad:
        raise ValueError(f'unknown fields for {kind}: {",".join(bad)} (allowed: {",".join(COLUMN_FIELDS[kind])})')
    c = r['cols']; n_out = r['n']
    cols: dict = {}
    for f in want:
        if not n_out:
            cols[f] = []; continue
        if f in ('lon', 'lat'):
            cols[f] = np.round(c[f], 6).tolist()
        elif f == 'h_m':
            cols[f] = np.round(c['h'].astype(np.float64), 2).tolist()
        elif f in ('crown_m2', 'h_robust_m', 'dh_m'):
            cols[f] = np.round(c[f].astype(np.float64), 2).tolist()
        elif f == 'confidence':
            cols[f] = np.round(c['conf'].astype(np.float64), 2).tolist()
        elif f == 'species':
            cols[f] = _species(c['species'])
        elif f == 'vitality':
            cols[f] = _vitality(c['vitality'])
        elif f == 'type':
            cols[f] = _stype(c['type'])
        elif f == 'ndsm_only':
            cols[f] = c['ndsm_only'].astype(int).tolist()
        elif f == 'cell':
            cols[f] = [f'{int(v):x}' for v in c['cell'].tolist()]
        elif f == 'kg_code':
            cols[f] = c['kg'].tolist()
        elif f in ('x', 'y', 'area_m2'):
            cols[f] = c[f].tolist()
        elif f in ('taxon', 'h_user_m', 'dbh_cm', 'tree_id'):
            col = [None] * n_out
            u = r.get('user') or {}
            for i, t in zip(u.get('i', ()), u.get(f, ())):
                col[i] = t
            cols[f] = col
    r = dict(r); r.pop('cols'); r.pop('state')
    r.update(columns=cols, fields=list(want), order='height_desc')
    return r


def points_bbox(w, s, e, n, kind: str = 'trees', min_height: float = 0.0, limit: int = DEFAULT_LIMIT,
                realistic: bool = True, types: set | None = None, max_height: float | None = None) -> dict:
    """All objects of ``kind`` in bbox as row dicts, tallest first, ≤ limit
    (see :func:`collect` for the scan semantics)."""
    r = collect(w, s, e, n, kind, min_height=min_height, limit=limit, realistic=realistic, types=types,
                max_height=max_height)
    c = r['cols']; out = []
    if r['n']:
        lon = np.round(c['lon'], 7).tolist(); lat = np.round(c['lat'], 7).tolist()
        hh = np.round(c['h'].astype(np.float64), 2).tolist(); cell = c['cell'].tolist(); kgs = c['kg'].tolist()
        if kind == 'trees':
            cr = c['crown_m2'].astype(np.float64).tolist(); sp = _species(c['species']); vi = _vitality(c['vitality'])
            nd = c['ndsm_only'].tolist()
            for i in range(r['n']):
                crown_m2 = cr[i]
                out.append({'lon': lon[i], 'lat': lat[i], 'height_m': hh[i],
                            'crown_d_m': round(2.0 * math.sqrt(crown_m2 / math.pi), 1) if crown_m2 > 0 else None,
                            'crown_m2': round(crown_m2, 1), 'species': sp[i], 'vitality': vi[i], 'ndsm_only': bool(nd[i]),
                            'confidence': None, 'cell': f'{int(cell[i]):x}', 'kg_code': kgs[i]})
        else:
            ty = _stype(c['type']); hr = np.round(c['h_robust_m'].astype(np.float64), 2).tolist()
            ar = c['area_m2'].tolist(); cf = np.round(c['conf'].astype(np.float64), 2).tolist()
            dh = np.round(c['dh_m'].astype(np.float64), 2).tolist()
            for i in range(r['n']):
                out.append({'lon': lon[i], 'lat': lat[i], 'height_m': hh[i], 'type': ty[i], 'height_robust_m': hr[i],
                            'area_m2': int(ar[i]), 'confidence': cf[i], 'dh_m': dh[i],
                            'cell': f'{int(cell[i]):x}', 'kg_code': kgs[i]})
    u = r.get('user')
    if u:
        for i, t, cnt, v in zip(u['i'], u['taxon'], u['n'], u['v']):
            out[i].update(taxon=t, taxon_n=cnt, taxon_v=v)
        for name, key_ in (('h_user_m', 'height_user_m'), ('dbh_cm', 'dbh_cm'), ('tree_id', 'tree_id')):
            for i, val in zip(u['i'], u.get(name, ())):
                if val is not None:
                    out[i][key_] = val
    a = r.get('added')
    if a:
        r['added'] = [{'lon': a['lon'][i], 'lat': a['lat'][i], 'height_m': a['h_m'][i], 'taxon': a['taxon'][i],
                       'taxon_n': a['n'][i], 'taxon_v': a['v'][i], 'added': True,
                       **({'dbh_cm': a['dbh_cm'][i]} if a.get('dbh_cm') and a['dbh_cm'][i] is not None else {}),
                       **({'client_id': a['client_id'][i]} if a.get('client_id') and a['client_id'][i] else {})}
                      for i in range(len(a['lon']))]
    a = r.get('reported')
    if a:
        r['reported'] = [{'tree_id': a['tree_id'][i], 'lon': a['lon'][i], 'lat': a['lat'][i], 'height_user_m': a['h_m'][i],
                          'taxon': a['taxon'][i], 'taxon_n': a['n'][i], 'taxon_v': a['v'][i],
                          **({'dbh_cm': a['dbh_cm'][i]} if a.get('dbh_cm') and a['dbh_cm'][i] is not None else {})}
                         for i in range(len(a['lon']))]
    r = dict(r); r.pop('cols'); r.pop('state'); r.pop('user', None)
    r['trees' if kind == 'trees' else 'structures'] = out
    return r


def trees_bbox(w, s, e, n, min_height: float = 0.0, limit: int = DEFAULT_LIMIT, realistic: bool = True,
               max_height: float | None = None) -> dict:
    return points_bbox(w, s, e, n, 'trees', min_height=min_height, limit=limit, realistic=realistic, max_height=max_height)


def structures_bbox(w, s, e, n, min_height: float = 0.0, limit: int = DEFAULT_LIMIT, realistic: bool = True,
                    types: set | None = None, max_height: float | None = None) -> dict:
    return points_bbox(w, s, e, n, 'structures', min_height=min_height, limit=limit, realistic=realistic, types=types,
                       max_height=max_height)
