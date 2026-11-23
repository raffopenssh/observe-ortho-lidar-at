"""tree_stands — national H3 res-10 tree-stand index (``GET /api/v1/trees/stands``,
``/api/v1/trees/hexes``): "where are the densest stands of trees >= 40 m" in ~10 ms.

One row per res-10 hexagon (~1.5 ha — a forestry stand) that holds >= 1 realistic
apex.  **No pass of its own**: ``StandAcc`` rides the daily ``cells_national`` build
(``Acc.add_table``), i.e. it sees exactly the apices that become ``tree_hh`` there —
exact heights, tree QA + rebin applied, border chunks deduplicated.  Totals therefore
equal ``/api/v1/trees/stats`` apex for apex, and the store carries the same ``gen``.

Store = ``data/ne/stands/`` — plain ``.npy`` columns, mmapped by the web workers
(shared page cache, no per-worker copy), ~40 B / row:

    ids   u64 [n]      res-10 H3 id, ascending
    cum   u16 [12, n]  apices with h >= EDGES[b] (reverse-cumulative 5 m bands, band-major:
                       a ">= 40 m" query touches one 7 MB row of the file)
    top   u8  [n]      tallest apex, x0.5 m          meanh u8 [n]  mean apex height, x0.5 m
    nobs  u8  [n]      observed res-12 children (<= 49)
    elev  u16 [n]      mean elevation m a.s.l. (65535 unknown)
    kg    u32 [n]      KG whose product serves the hexagon (highest code when several)
    lat/lng f32 [n]    hexagon centre           area u16 [n]  hexagon area m2

Height windows snap to the band edges (3, 5, 10 ... 55 m) — the same bands as
``tree_hh`` on ``/cells/national``; for exact heights drill down with ``/trees/bbox``.

    python3 cells_national.py --build --force    # (re)builds this store too; batch tier only
    python3 tree_stands.py --status
    python3 tree_stands.py --top 40              # ten densest >= 40 m stands
"""
from __future__ import annotations

import json
import logging
import math
import os
import shutil
import threading
import time
from pathlib import Path

import numpy as np

log = logging.getLogger('tree_stands')

# === SECTION: constants ===

ALGO = 'stands-2'
RES = 10
EDGES = (3, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)     # m; band b = [EDGES[b], EDGES[b+1])
NB = len(EDGES)
SORTS = ('density', 'count', 'tallest', 'mean_height', 'share')
MAX_LIMIT = 500
DEFAULT_LIMIT = 20
MAX_HEXES = 20_000               # /trees/hexes rows per request (~300 km2)
HEX_SPACING_M = 131.0            # res-10 centre spacing in Austria
COLS = ('ids', 'cum', 'top', 'meanh', 'nobs', 'elev', 'kg', 'lat', 'lng', 'area')
_CONCURRENCY = threading.BoundedSemaphore(2)   # full-table scans per worker


def _dir() -> Path:
    import cells_api as ca
    return ca.NE_DIR / 'stands'


def band_of(h_m: float) -> int:
    """Band index whose lower edge is the largest edge <= h (h below 3 m -> 0)."""
    b = 0
    for i, e in enumerate(EDGES):
        if h_m >= e:
            b = i
    return b


# === SECTION: build (fed by the cells_national pass; never in gunicorn) ===

class StandAcc:
    """Res-10 tree sums collected inside the ``cells_national`` build pass — the very apices
    (exact heights, QA + rebin applied, border chunks deduplicated) that become ``tree_hh`` there,
    so the store and ``/trees/stats`` agree apex for apex and share one ``gen``."""

    KEYS = ('hist', 'top', 'hsum', 'nobs', 'esum', 'en', 'kg')

    def __init__(self):
        self.parts: list[tuple] = []

    def add(self, t, observed: np.ndarray, ci: np.ndarray, h: np.ndarray) -> None:
        """t = cells_api.Table, observed = per-cell bool, (ci, h) = cell row index + height m per apex."""
        import cells_national as cn
        if not len(ci):
            return
        par, inv = np.unique(cn.parent_ids(t.ids, RES), return_inverse=True)
        m = len(par)
        pi = inv[ci]
        hist = np.bincount(pi * NB + cn.tree_hh_band(h), minlength=m * NB).reshape(m, NB)
        keep = hist.sum(1) > 0
        top = np.zeros(m); np.maximum.at(top, pi, h)
        el = t.obs['el']; ok = el != 65535
        kg = np.zeros(m, np.uint32)
        if len(t.kgs):
            lut = np.zeros(len(t.kgs), np.uint32)
            for k, name in enumerate(t.kgs):
                try:
                    lut[k] = int(str(name).split('-')[0])
                except ValueError:
                    pass
            np.maximum.at(kg, inv, lut[t.kg_idx])
        self.parts.append((par[keep], hist[keep].astype(np.uint32), top[keep].astype(np.float32),
                           np.bincount(pi, weights=h, minlength=m)[keep],
                           np.bincount(inv, weights=observed, minlength=m)[keep].astype(np.uint32),
                           np.bincount(inv[ok], weights=el[ok], minlength=m)[keep],
                           np.bincount(inv[ok], minlength=m)[keep].astype(np.uint32), kg[keep]))
        if len(self.parts) >= 1000:
            self.reduce()

    def reduce(self) -> tuple | None:
        """(ids, hist, top, hsum, nobs, esum, en, kg) with unique ascending ids."""
        if not self.parts:
            return None
        cat = [np.concatenate([p[i] for p in self.parts]) for i in range(8)]
        ids, inv = np.unique(cat[0], return_inverse=True)
        order = np.argsort(inv, kind='stable'); starts = np.searchsorted(inv[order], np.arange(len(ids)))
        out = [ids]
        for i in range(1, 8):
            f = np.maximum if i in (2, 7) else np.add
            out.append(f.reduceat(cat[i][order], starts, axis=0))
        self.parts = [tuple(out)]
        return self.parts[0]


# --- national hexagon table, one body (GET /api/v1/trees/hexes?res=9&format=bin) --------------
# NEX1 like /cells/national (``cells_national.decode`` reads it), written next to the store from the
# very sums of the build pass: h_sum is the exact Σ apex height (0.25 m units = the stored NET1 step),
# so trees / h_sum / tallest / bands equal /stats/unit over the same hexagon, apex for apex.

HEX_RESES = (9,)                 # res 10 = 55 MB gzip per download: not offered (metered host); areas → JSON form
HEX_MAGIC = b'NEX1'
HEX_COLS = (('trees', 'u32', 'count', None), ('tallest', 'u8', 'm', 0.25), ('h_sum', 'u32', 'm', 0.25),
            ('bands', 'u16', 'count', None))
BAND_NAMES = tuple(f'{a}_{b}' for a, b in zip(EDGES, EDGES[1:])) + (f'{EDGES[-1]}_up',)


def _hex_blob(res: int, ids, hist, top, hsum, gen: str, built_at: str) -> tuple[bytes, dict]:
    """gzip'd NEX1 of one resolution; res < 10 = roll-up of the res-10 sums (ids ascending)."""
    import gzip
    import struct
    import cells_api as ca
    import cells_national as cn
    q = np.rint(np.asarray(hsum, np.float64) * 4.0).astype(np.int64)      # exact: heights are k × 0.25 m
    tq = np.rint(np.asarray(top, np.float64) * 4.0).astype(np.int64)
    if res < RES:
        par, inv = np.unique(cn.parent_ids(ids, res, RES), return_inverse=True)
        o = np.argsort(inv, kind='stable'); st = np.searchsorted(inv[o], np.arange(len(par)))
        ids = par; hist = np.add.reduceat(hist[o], st, axis=0)
        q = np.add.reduceat(q[o], st); tq = np.maximum.reduceat(tq[o], st)
    n = len(ids)
    trees = hist.sum(1, dtype=np.int64)
    sat = int((hist > 65535).sum())
    cols = [np.diff(ids.astype(np.int64), prepend=0).astype('<u8'), trees.astype('<u4'),
            np.clip(tq, 0, 255).astype(np.uint8), q.astype('<u4'), np.minimum(hist, 65535).astype('<u2')]
    specs = [{'name': 'id', 'dtype': 'u64', 'bytes': cols[0].nbytes, 'unit': 'h3', 'agg': 'key'}]
    for (nm, dt, unit, scale), a in zip(HEX_COLS, cols[1:]):
        d = {'name': nm, 'dtype': dt, 'bytes': int(a.nbytes), 'unit': unit, 'agg': 'max' if nm == 'tallest' else 'sum'}
        if scale:
            d['scale'] = scale
        if a.ndim == 2:
            d['shape'] = [int(a.shape[1])]
        specs.append(d)
    hdr = {'algo': ALGO, 'kind': 'tree_hexes', 'res': res, 'n': int(n), 'gen': gen, 'built_at': built_at,
           'ids': 'delta_u64', 'columns': specs, 'fields': [c[0] for c in HEX_COLS], 'edges_m': list(EDGES),
           'bands': list(BAND_NAMES), 'trees_total': int(trees.sum()), 'h_sum_total': int(q.sum()),
           'bands_saturated': sat,
           'semantics': 'one row per hexagon holding >= 1 apex (h >= 3 m, default /trees/bbox set, counted in the '
                        'cell of its served position, border cells deduplicated across products); '
                        'mean height m = h_sum * 0.25 / trees; sum(bands) = trees',
           'attribution': ca.ATTRIBUTION, 'license': 'CC-BY-4.0', 'license_url': ca.LICENSE_URL,
           'source_service': 'srtm-lidar-at'}
    hb = json.dumps(hdr, separators=(',', ':'), ensure_ascii=False).encode()
    blob = gzip.compress(HEX_MAGIC + struct.pack('<I', len(hb)) + hb + b''.join(np.ascontiguousarray(c).tobytes() for c in cols), 6)
    return blob, {'n': int(n), 'bytes_gz': len(blob), 'trees': hdr['trees_total'], 'h_sum': hdr['h_sum_total'],
                  'tallest_q': int(tq.max()) if n else 0}


def write_hex_blobs(d: Path, ids, hist, top, hsum, gen: str, built_at: str) -> dict:
    out = {}
    for res in HEX_RESES:
        blob, info = _hex_blob(res, ids, hist, top, hsum, gen, built_at)
        tmp = d / f'hex{res}.tmp'
        tmp.write_bytes(blob); os.replace(tmp, d / f'hex{res}.nex.gz')
        out[str(res)] = info
    return out


def backfill_hex_blobs(verbose: bool = True) -> dict:
    """Write hex9.nex.gz for the store ON DISK from the ``cells_national`` unit partials (no chunk
    is read, nothing is rebuilt).  Refused unless the partials reproduce the store exactly."""
    import cells_national as cn
    t = table()
    if t is None:
        raise RuntimeError('no stand store')
    ids = np.asarray(t['ids']); n = len(ids)
    hist = np.zeros((n, NB), np.uint32); top = np.zeros(n, np.float32); hsum = np.zeros(n, np.float64)
    files = sorted(cn.PARTS_DIR.glob('*/*.npz'))
    for i, p in enumerate(files):
        _, st = cn._load_unit(p.stem)
        if st is None or not len(st[0]):
            continue
        idx = np.searchsorted(ids, st[0])
        if idx.max(initial=0) >= n or not np.array_equal(ids[idx], st[0]):
            raise RuntimeError(f'unit {p.stem} holds hexagons the store does not — run the national build')
        hist[idx] += st[1]; top[idx] = np.maximum(top[idx], st[2]); hsum[idx] += st[3]
        if verbose and i % 2000 == 0:
            print(f'  {i}/{len(files)} units', flush=True)
    tot = np.zeros(n, np.int64); same = True
    for b in range(NB - 1, -1, -1):
        tot += hist[:, b]
        same = same and np.array_equal(np.minimum(tot, 65535), t['cum'][b])
    if not (same and np.array_equal(np.clip(np.rint(top * 2.0), 1, 255).astype(np.uint8), np.asarray(t['top']))):
        raise RuntimeError('unit partials do not reproduce the stand store — run the national build')
    m = t['meta']
    info = write_hex_blobs(_dir(), ids, hist, top, hsum, m['gen'], m['built_at'])
    meta = dict(m, hex=info)
    tmp = _dir() / 'meta.tmp'
    tmp.write_text(json.dumps(meta, indent=1)); os.replace(tmp, _dir() / 'meta.json')
    return info


def hex_blob(res: int) -> tuple[bytes, str, dict] | None:
    """(gzip'd NEX1, etag, meta) of the national hexagon table; None = not built for this store."""
    m = read_meta()
    if not m or str(res) not in (m.get('hex') or {}):
        return None
    try:
        return (_dir() / f'hex{res}.nex.gz').read_bytes(), hex_etag(m, res), m
    except OSError:
        return None


def hex_etag(m: dict, res: int) -> str:
    return f'"sx-r{res}-{m.get("gen")}"'


def write_store(red: tuple, gen: str, built_at: str, extra: dict | None = None) -> dict:
    import h3.api.numpy_int as h3n
    t0 = time.time()
    ids, hist, top, hsum, nobs, esum, en, kg = red
    n = len(ids)
    tot = np.zeros(n, np.int64)                      # running reverse-cumulative count: no [n, 12] temporaries
    cum = np.empty((NB, n), '<u2')
    for b in range(NB - 1, -1, -1):
        tot += hist[:, b]
        np.minimum(tot, 65535, out=cum[b], casting='unsafe')
    lat = np.empty(n, np.float32); lng = np.empty(n, np.float32); area = np.empty(n, np.uint16)
    a = 0.0
    for i, c in enumerate(ids.tolist()):
        lat[i], lng[i] = h3n.cell_to_latlng(c)
        if i % 16 == 0:               # area varies smoothly: every 16th id of the sorted (spatially coherent) list
            a = h3n.cell_area(c, 'm^2')
        area[i] = min(65535, round(a))
    out = {
        'ids': ids.astype('<u8'), 'cum': cum, 'top': np.clip(np.rint(top * 2.0), 1, 255).astype(np.uint8),
        'meanh': np.clip(np.rint(hsum / np.maximum(tot, 1) * 2.0), 0, 255).astype(np.uint8),
        'nobs': np.minimum(nobs, 255).astype(np.uint8),
        'elev': np.where(en > 0, np.rint(esum / np.maximum(en, 1)), 65535).astype('<u2'),
        'kg': kg.astype('<u4'), 'lat': lat, 'lng': lng, 'area': area,
    }
    d = _dir(); tmp = d.with_name(f'stands.tmp-{os.getpid()}'); oldd = d.with_name('stands.old')
    shutil.rmtree(tmp, ignore_errors=True); tmp.mkdir(parents=True)
    for k, v in out.items():
        np.save(tmp / f'{k}.npy', v)
    try:
        hexinfo = write_hex_blobs(tmp, ids, hist, top, hsum, gen, built_at)
    except Exception as e:  # noqa: BLE001 — the stand store must not be lost over the export files
        log.warning('tree_stands: hex blobs not written: %s', e)
        hexinfo = {}
    meta = {
        'algo': ALGO, 'gen': gen, 'source': 'cells_national build pass', 'res': RES, 'built_at': built_at,
        'hexes': int(n), 'apices': int(tot.sum()), 'edges_m': list(EDGES),
        'at_least': [int(x) for x in hist.sum(0, dtype=np.int64)[::-1].cumsum()[::-1]],
        'cum_saturated': int((tot > 65535).sum()),
        'hex': hexinfo, 'bytes': int(sum(v.nbytes for v in out.values())), 'write_s': round(time.time() - t0, 1), **(extra or {}),
    }
    (tmp / 'meta.json').write_text(json.dumps(meta, indent=1))
    shutil.rmtree(oldd, ignore_errors=True)
    if d.exists():
        os.rename(d, oldd)
    os.rename(tmp, d)
    shutil.rmtree(oldd, ignore_errors=True)
    return meta


def read_meta() -> dict | None:
    try:
        return json.loads((_dir() / 'meta.json').read_text())
    except (OSError, ValueError):
        return None


# === SECTION: request path (mmap, read-only) ===

_lock = threading.Lock()
_tab: dict = {'key': None, 't': 0.0, 'v': None}
_kgs: dict = {'t': 0.0, 'v': None}
_res_lru: 'dict[tuple, tuple]' = {}


def table() -> dict | None:
    """{'meta', <column>: mmapped array} of the current store; re-opened when meta.json changes."""
    now = time.time()
    with _lock:
        if _tab['v'] is not None and now - _tab['t'] < 30:
            return _tab['v']
        p = _dir() / 'meta.json'
        try:
            key = p.stat().st_mtime_ns
        except OSError:
            _tab.update(t=now, v=None, key=None); return None
        if key != _tab['key']:
            try:
                v = {'meta': json.loads(p.read_text())}
                for c in COLS:
                    v[c] = np.load(_dir() / f'{c}.npy', mmap_mode='r')
            except Exception as e:  # noqa: BLE001 — mid-swap: keep the old table one more round
                log.warning('tree_stands: open: %s', e)
                _tab['t'] = now
                return _tab['v']
            _tab.update(key=key, v=v); _res_lru.clear()
        _tab['t'] = now
        return _tab['v']


def _kg_info() -> dict:
    """{code int: (kg_name, gemeinde, state_code, state_name)} + state lookup array, refreshed hourly."""
    now = time.time()
    if _kgs['v'] is not None and now - _kgs['t'] < 3600:
        return _kgs['v']
    info = {}; st = np.zeros(100_000, np.uint8)
    try:
        import landscape_query as lq
        for code, nm, gem, sc, sn in lq.idx().execute('SELECT kg_code, kg_name, gemeinde_name, state_code, state_name FROM kg'):
            try:
                c = int(code)
            except ValueError:
                continue
            info[c] = (nm, gem, sc, sn)
            try:
                st[c] = int(sc)
            except (TypeError, ValueError):
                pass
    except Exception as e:  # noqa: BLE001
        log.warning('tree_stands: kg info: %s', e)
    _kgs.update(t=now, v={'info': info, 'state': st})
    return _kgs['v']


class BadQuery(ValueError):
    pass


# --- area selection: everything is an index array into the id-sorted table --------------

MAX_AREA_CELLS = 20_000          # h3 ids per area
MAX_VERTICES = 50_000


def _digit_mask(res: int) -> int:
    return (1 << (3 * (15 - res))) - 1


def h3_ranges(ids: np.ndarray, cells) -> np.ndarray:
    """Row indices of every res-10 hexagon inside the given H3 cells (res <= 10, mixed / compacted sets fine).
    Same-resolution H3 ids sort parent-contiguously, so a parent is two binary searches; finer cells
    (res 11-15) select their res-10 parent."""
    import h3
    lo = []; hi = []
    for c in cells:
        try:
            v = int(c, 16) if isinstance(c, str) else int(c)
            r = (v >> 52) & 0xF
            if not h3.is_valid_cell(h3.int_to_str(v)):
                raise ValueError
        except (ValueError, TypeError):
            raise BadQuery(f'not an H3 cell: {c!r}')
        if r > RES:
            v = int(h3.cell_to_parent(h3.int_to_str(v), RES), 16); r = RES
        base = (v & ~(0xF << 52)) | (RES << 52)
        dm = _digit_mask(r) & ~_digit_mask(RES)          # digits r+1 .. 10
        lo.append(base & ~dm); hi.append(base | dm)      # children: digits 0..6 -> [..000.., ..777..]
    a = np.searchsorted(ids, np.asarray(lo, np.uint64), 'left')
    b = np.searchsorted(ids, np.asarray(hi, np.uint64), 'right')
    if not len(a):
        return np.empty(0, np.int64)
    return np.unique(np.concatenate([np.arange(x, y) for x, y in zip(a.tolist(), b.tolist())] or [np.empty(0, np.int64)]))


def _geom(g):
    import shapely
    from shapely.geometry import shape
    if isinstance(g, dict) and g.get('type') == 'Feature':
        g = g.get('geometry')
    if not isinstance(g, dict) or g.get('type') not in ('Polygon', 'MultiPolygon'):
        raise BadQuery('geometry must be a GeoJSON Polygon / MultiPolygon (WGS84 lon, lat)')
    try:
        s = shape(g)
    except Exception as e:  # noqa: BLE001
        raise BadQuery(f'bad geometry: {e}')
    if shapely.get_num_coordinates(s) > MAX_VERTICES:
        raise BadQuery(f'geometry has more than {MAX_VERTICES} vertices — simplify it or send H3 cells')
    if not s.is_valid:
        s = shapely.make_valid(s)
    return s


def _bbox_idx(t, w, s, e, n) -> np.ndarray:
    # lat is monotone enough to matter only as a mask; 4 compares over the table ~10 ms
    return np.flatnonzero((t['lng'] >= w) & (t['lng'] <= e) & (t['lat'] >= s) & (t['lat'] <= n))


def area_idx(t, area: dict | None) -> np.ndarray | None:
    """Row index array (ascending) for an area spec, None = all Austria.  Keys (AND-combined):
    ``h3`` (list of cells), ``geometry`` (GeoJSON polygon; hexagon centre inside), ``bbox`` [w,s,e,n],
    ``kg``, ``state``."""
    if not area:
        return None
    idx = None
    if area.get('h3'):
        cells = area['h3']
        if len(cells) > MAX_AREA_CELLS:
            raise BadQuery(f'more than {MAX_AREA_CELLS} H3 cells — send a compacted set (h3.compact_cells)')
        idx = h3_ranges(t['ids'], cells)
    if area.get('geometry') is not None:
        import shapely
        g = _geom(area['geometry'])
        if idx is None:
            idx = _bbox_idx(t, *g.bounds)
        shapely.prepare(g)
        idx = idx[shapely.contains_xy(g, np.asarray(t['lng'][idx], np.float64), np.asarray(t['lat'][idx], np.float64))]
    if area.get('bbox'):
        w, s, e, n = area['bbox']
        if idx is None:
            idx = _bbox_idx(t, w, s, e, n)
        else:
            lo, la = t['lng'][idx], t['lat'][idx]
            idx = idx[(lo >= w) & (lo <= e) & (la >= s) & (la <= n)]
    if area.get('kg') not in (None, ''):
        try:
            code = int(str(area['kg']).split('-')[0])
        except ValueError:
            raise BadQuery('kg must be a 5-digit KG code')
        idx = np.flatnonzero(t['kg'] == code) if idx is None else idx[t['kg'][idx] == code]
    if area.get('state') not in (None, ''):
        try:
            sc = int(area['state'])
        except ValueError:
            raise BadQuery('state must be a state code 1-9')
        st = _kg_info()['state']
        idx = (np.flatnonzero(st[np.minimum(t['kg'], 99_999)] == sc) if idx is None
               else idx[st[np.minimum(t['kg'][idx], 99_999)] == sc])
    return idx


def _col(a, idx):
    return np.asarray(a) if idx is None else a[idx]


def _window(t, idx, min_h, max_h):
    b0 = band_of(float(min_h or 0))
    b1 = None
    if max_h is not None:
        if float(max_h) not in EDGES or float(max_h) <= EDGES[b0]:
            raise BadQuery(f'max_height must be a band edge above min_height: {list(EDGES[b0 + 1:])}')
        b1 = EDGES.index(float(max_h))
    cnt = _col(t['cum'][b0], idx).astype(np.int32)
    if b1 is not None:
        cnt -= _col(t['cum'][b1], idx)
    return b0, b1, cnt


def _hex_row(t, i: int, cnt_i: int, bands: bool) -> dict:
    import h3
    info = _kg_info()['info']
    area_ha = float(t['area'][i]) / 1e4
    cid = h3.int_to_str(int(t['ids'][i]))
    la, lo = h3.cell_to_latlng(cid)
    code = int(t['kg'][i])
    d = {'h3': cid, 'lat': round(la, 6), 'lon': round(lo, 6), 'trees': int(cnt_i),
         'per_ha': round(cnt_i / area_ha, 1), 'trees_all': int(t['cum'][0, i]),
         'tallest_m': float(t['top'][i]) * 0.5, 'mean_height_m': float(t['meanh'][i]) * 0.5,
         'elev_m': None if t['elev'][i] == 65535 else int(t['elev'][i]), 'area_ha': round(area_ha, 3)}
    if code:
        d['kg'] = f'{code:05d}'
        k = info.get(code)
        if k:
            d['kg_name'], d['gemeinde'], d['state'] = k[0], k[1], k[3]
    if bands:
        c = t['cum'][:, i].astype(np.int64)
        d['bands'] = [int(x) for x in (c - np.append(c[1:], 0))]
    return d


def _rank(t, idx, min_height, max_height, sort, limit, sep, min_trees, elev_min, elev_max, bands, geometry):
    """(b0, b1, matched, rows) — the ranking core over a row subset (idx None = whole table)."""
    b0, b1, c = _window(t, idx, min_height, max_height)
    m = c > 0
    if elev_min is not None:
        el = _col(t['elev'], idx); m &= (el >= elev_min) & (el != 65535)
    if elev_max is not None:
        m &= _col(t['elev'], idx) < elev_max
    if min_trees:
        m &= _col(t['cum'][0], idx) >= min_trees
    sub = np.flatnonzero(m)
    c = c[sub]
    idx = sub if idx is None else idx[sub]
    if sort == 'density':
        score = c / t['area'][idx].astype(np.float32)
    elif sort == 'count':
        score = c.astype(np.float32)
    elif sort == 'tallest':
        score = t['top'][idx].astype(np.float32) + np.minimum(c, 9999) * 1e-5
    elif sort == 'mean_height':
        score = t['meanh'][idx].astype(np.float32) + np.minimum(c, 9999) * 1e-5
    else:
        score = c / np.maximum(t['cum'][0][idx], 1).astype(np.float32)
    matched = {'hexes': int(len(idx)), 'trees': int(c.sum()),
               'area_ha': round(float(t['area'][idx].sum(dtype=np.float64)) / 1e4, 1)}
    K = max(1000, limit * 50)
    if len(idx) > K:
        part = np.argpartition(score, len(idx) - K)[-K:]
        idx, c, score = idx[part], c[part], score[part]
    o = np.lexsort((idx, -score))
    idx, c = idx[o], c[o]
    picks = []
    if sep > 0 and len(idx):
        so = np.argsort(idx)                       # read the mmap in file order
        lat = np.empty(len(idx)); lng = np.empty(len(idx))
        lat[so] = t['lat'][idx[so]]; lng[so] = t['lng'][idx[so]]
        kx = 111_320.0 * math.cos(math.radians(float(lat.mean()))); ky = 110_574.0
        px = np.empty(limit); py = np.empty(limit)
        for j in range(len(idx)):
            x, y = lng[j] * kx, lat[j] * ky
            k = len(picks)
            if k and np.min(np.hypot(px[:k] - x, py[:k] - y)) < sep:
                continue
            px[k], py[k] = x, y; picks.append(j)
            if len(picks) >= limit:
                break
    else:
        picks = list(range(min(limit, len(idx))))
    import h3
    import h3.api.numpy_int as h3n
    ids = t['ids']
    out = []
    for r, j in enumerate(picks):
        i = int(idx[j])
        d = {'rank': r + 1, **_hex_row(t, i, int(c[j]), bands)}
        ring = np.asarray(h3n.grid_disk(int(ids[i]), 1), np.uint64)
        pos = np.minimum(np.searchsorted(ids, ring), len(ids) - 1)
        pos = np.sort(pos[ids[pos] == ring])
        rc = int(t['cum'][b0][pos].astype(np.int64).sum() - (t['cum'][b1][pos].astype(np.int64).sum() if b1 is not None else 0))
        d['ring'] = {'trees': rc, 'per_ha': round(rc / (7 * float(t['area'][i]) / 1e4), 1),
                     'tallest_m': float(t['top'][pos].max()) * 0.5}
        if geometry:
            d['boundary'] = [[round(p[1], 6), round(p[0], 6)] for p in h3.cell_to_boundary(d['h3'])]
        out.append(d)
    return b0, b1, matched, out


def _akey(area):
    return json.dumps(area, sort_keys=True, default=str) if area else None


def stands(min_height: float = 3, max_height: float | None = None, area: dict | None = None, elev_min=None,
           elev_max=None, sort: str = 'density', limit: int = DEFAULT_LIMIT, separation_m: float | None = None,
           min_trees: int | None = None, bands: bool = False, geometry: bool = False) -> dict | None:
    """Ranked res-10 hexagons ("stands", ~1.5 ha).  ``separation_m`` (default 2 hexagon spacings) keeps the
    list to distinct places: a hexagon closer than that to a better-ranked one is skipped."""
    t = table()
    if t is None:
        return None
    if sort not in SORTS:
        raise BadQuery(f'sort must be one of {", ".join(SORTS)}')
    limit = max(1, min(int(limit), MAX_LIMIT))
    sep = 2 * HEX_SPACING_M if separation_m is None else max(0.0, float(separation_m))
    if min_trees is None:
        min_trees = 30 if sort in ('share', 'mean_height') else 0
    small = not area or len(_akey(area)) < 400
    key = (t['meta']['gen'], min_height, max_height, _akey(area), elev_min, elev_max, sort, limit, sep, min_trees,
           bands, geometry)
    if small and key in _res_lru:
        return _res_lru[key]
    with _CONCURRENCY:
        b0, b1, matched, out = _rank(t, area_idx(t, area), min_height, max_height, sort, limit, sep, min_trees,
                                     elev_min, elev_max, bands, geometry)
    m = t['meta']
    res = {'min_height_m': EDGES[b0], 'max_height_m': EDGES[b1] if b1 is not None else None, 'sort': sort,
           'hex_res': RES, 'separation_m': round(sep), 'matched': matched, 'count': len(out), 'stands': out,
           'gen': m['gen'], 'built_at': m['built_at']}
    if small:
        if len(_res_lru) > 256:
            _res_lru.clear()
        _res_lru[key] = res
    return res


def _summary_of(t, idx, top: int = 0, min_height: float = 3, sort: str = 'density') -> dict:
    n = len(t['ids']) if idx is None else len(idx)
    if not n:
        return {'hexes': 0, 'trees': 0}
    at = [int(_col(t['cum'][b], idx).sum(dtype=np.int64)) for b in range(NB)]
    # cum saturates at 65535 per hexagon — unreachable on 1.5 ha, so the sums are exact
    area_ha = float(_col(t['area'], idx).sum(dtype=np.float64)) / 1e4
    tp = _col(t['top'], idx); call = _col(t['cum'][0], idx)
    j = int(np.argmax(tp)); i = j if idx is None else int(idx[j])
    el = _col(t['elev'], idx); elv = el[el != 65535]
    d = {'hexes': int(n), 'stocked_ha': round(area_ha, 1), 'trees': at[0], 'per_stocked_ha': round(at[0] / area_ha, 1),
         'at_least': at, 'bands': [at[b] - (at[b + 1] if b + 1 < NB else 0) for b in range(NB)],
         'mean_height_m': round(float((_col(t['meanh'], idx).astype(np.float64) * call).sum() / max(1, at[0])) * 0.5, 1),
         'tallest': {k: v for k, v in _hex_row(t, i, int(t['cum'][0, i]), False).items()
                     if k in ('h3', 'lat', 'lon', 'tallest_m', 'kg', 'kg_name')},
         'elev_m': {'min': int(elv.min()), 'mean': int(elv.mean()), 'max': int(elv.max())} if len(elv) else None}
    if top:
        d['top_stands'] = _rank(t, idx, min_height, None, sort, min(int(top), 50), 2 * HEX_SPACING_M,
                                30 if sort in ('share', 'mean_height') else 0, None, None, False, False)[3]
    return d


def summary(areas: list[dict] | None = None, top: int = 0, min_height: float = 3, sort: str = 'density') -> dict | None:
    """Tree summary per area (``areas`` = list of area specs with an optional ``id``; None / [] = Austria).
    ``top`` adds the top-N stands of each area for the ``min_height`` window."""
    t = table()
    if t is None:
        return None
    if sort not in SORTS:
        raise BadQuery(f'sort must be one of {", ".join(SORTS)}')
    out = []
    with _CONCURRENCY:
        for k, a in enumerate(areas or [None]):
            if a is not None and not isinstance(a, dict):
                raise BadQuery('areas must be objects: {id?, h3?[], geometry?, bbox?, kg?, state?}')
            spec = {x: a[x] for x in ('h3', 'geometry', 'bbox', 'kg', 'state') if a and a.get(x) not in (None, '', [])}
            if a is not None and not spec:
                raise BadQuery(f'area {k}: needs one of h3 / geometry / bbox / kg / state')
            d = _summary_of(t, area_idx(t, spec), top, min_height, sort)
            if a and 'id' in a:
                d = {'id': a['id'], **d}
            out.append(d)
    m = t['meta']
    return {'hex_res': RES, 'edges_m': list(EDGES), 'top_min_height_m': EDGES[band_of(min_height)] if top else None,
            'areas': out, 'gen': m['gen'], 'built_at': m['built_at']}


def hexes(area: dict | None, res: int = RES, min_height: float = 3, max_height: float | None = None,
          bands: bool = False) -> dict | None:
    """Every H3 hexagon (``res`` 5..10; coarser = exact roll-up of the res-10 rows) with >= 1 apex of the
    height window in the area, as columns sorted by H3 id."""
    t = table()
    if t is None:
        return None
    if not 5 <= int(res) <= RES:
        raise BadQuery('res must be 5..10')
    res = int(res)
    import h3
    import cells_national as cn
    with _CONCURRENCY:
        idx = area_idx(t, area)
        b0, b1, cnt = _window(t, idx, min_height, max_height)
        sub = np.flatnonzero(cnt > 0)
        idx = sub if idx is None else idx[sub]
        cnt = cnt[sub].astype(np.int64)
        ids = t['ids'][idx]
        par = ids if res == RES else cn.parent_ids(ids, res, RES)
        # ascending res-10 ids are parent-contiguous -> group starts without a sort
        starts = np.flatnonzero(np.r_[True, par[1:] != par[:-1]]) if len(par) else np.empty(0, np.int64)
        if len(starts) > MAX_HEXES:
            raise BadQuery(f'{len(starts)} hexagons (max {MAX_HEXES}): shrink the area, raise min_height or lower res')
        if not len(starts):
            cols = {'h3': [], 'trees': [], 'trees_all': [], 'tallest_m': [], 'mean_height_m': [], 'elev_m': []}
        else:
            red = lambda a: np.add.reduceat(a, starts, axis=0)      # noqa: E731
            call = t['cum'][0][idx].astype(np.int64); tall = red(call)
            el = t['elev'][idx]; ok = el != 65535
            en = red(ok.astype(np.int64)); es = red(np.where(ok, el, 0).astype(np.int64))
            cols = {'h3': [h3.int_to_str(int(x)) for x in par[starts]],
                    'trees': red(cnt).tolist(), 'trees_all': tall.tolist(),
                    'tallest_m': (np.maximum.reduceat(t['top'][idx], starts) * 0.5).tolist(),
                    'mean_height_m': np.round(red(t['meanh'][idx].astype(np.int64) * call) / np.maximum(tall, 1) * 0.5, 1).tolist(),
                    'elev_m': [int(s / k) if k else None for s, k in zip(es.tolist(), en.tolist())]}
            if res != RES:
                cols['stocked_ha'] = np.round(red(t['area'][idx].astype(np.int64)) / 1e4, 1).tolist()
            if bands:
                c = t['cum'][:, idx].astype(np.int64)
                hist = (c - np.vstack([c[1:], np.zeros((1, len(idx)), np.int64)])).T
                cols['bands'] = red(hist).tolist()
    m = t['meta']
    return {'min_height_m': EDGES[b0], 'max_height_m': EDGES[b1] if b1 is not None else None, 'hex_res': res,
            'edges_m': list(EDGES) if bands else None,
            'count': len(cols['h3']), 'trees': int(cnt.sum()), 'columns': cols,
            'gen': m['gen'], 'built_at': m['built_at']}


def status() -> dict:
    m = read_meta()
    if not m:
        return {'ready': False}
    return {'ready': True, **{k: m.get(k) for k in ('gen', 'built_at', 'hexes', 'apices', 'bytes', 'cum_saturated')}}


# === SECTION: CLI ===

if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--status', action='store_true')
    ap.add_argument('--hex-blobs', action='store_true',
                    help='write hex9.nex.gz for the current store from the national unit partials')
    ap.add_argument('--top', type=float, default=None, metavar='MIN_HEIGHT_M')
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if a.hex_blobs:
        print(json.dumps(backfill_hex_blobs(), indent=1))
    elif a.top is not None:
        for s in stands(min_height=a.top, limit=10)['stands']:
            print(f"{s['rank']:>2} {s['h3']} {s['lat']:.5f},{s['lon']:.5f} {s['trees']:>4} trees {s['per_ha']:>6}/ha "
                  f"tallest {s['tallest_m']} m  {s.get('kg', '')} {s.get('kg_name', '')} ({s.get('state', '')})")
    else:
        print(json.dumps(status(), indent=1))
