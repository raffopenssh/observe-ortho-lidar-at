"""landscape_query — bbox/point-keyed views of what the
segmentation learned, for the public API (``public_api.py``).

Everything answers from the primary's local stores — ``data/search_index.db``
(KG table, kg_landcover, kg_hansen, kg_trees, kg_type_top, kg_buildings),
``data/kg_v2_store.db`` (product-2.1+ ``grid25``: 25 m DTM / slope /
dominant-class / canopy fractions) and ``data/feedback.sqlite`` (QA flags).
No KG JSON, no GPKG, no Zenodo / BEV fetch in the request path.

Design rule (licensing): nothing here reads or echoes lot ids, lot areas,
footprint ids or anything else keyed by the property register.
Inputs are lon/lat, a bbox or a KG *code*; outputs are our own LiDAR /
Sentinel-derived measurements. Trees and landmarks are QA-filtered here so
clients get the realistic set without needing the flag endpoints.
"""
from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)

INDEX_DB = Path('data/search_index.db')
FLAGS_DB = Path('data/feedback.sqlite')
from kg_universe import KG_TOTAL, UNIVERSE_HASH_CONTRACT, UNIVERSE_HASH_RECIPE, universe_hash  # 7850

# Land-cover classes that are frequently the *reported* dominant type even
# when they cover a sliver (a driveway, a shed roof). ``dom_terrain`` skips
# them so a meadow with a shed is still a meadow.
IMPERVIOUS = {'roof', 'road', 'parking', 'path', 'rail', 'bridge', 'solar_panel',
              'greenhouse', 'wall', 'fence', 'mast', 'substation', 'wind_turbine'}
NATURAL = {'tree', 'shrub', 'grass', 'hedge', 'water', 'crop', 'orchard', 'vineyard',
           'garden', 'bare_soil', 'rock', 'glacier'}
WOODY = {'tree', 'shrub', 'hedge', 'orchard'}

# Realistic-tree gate (what siedler used to apply client-side, now here).
FLAG_SEVERITIES = ('high', 'critical')

# Man-made verticals worth showing as landmarks: type → (min_h, max_h).
LANDMARK_TYPES = {'roof': (18.0, 60.0), 'mast': (15.0, 120.0),
                  'wind_turbine': (40.0, 200.0), 'bridge': (20.0, 120.0)}
LANDMARK_RF_MIN = 0.6

MAX_BBOX_DEG2 = 0.03        # ~ 25 × 10 km — larger viewports should tile


# --------------------------------------------------------------------------
# connections
# --------------------------------------------------------------------------
_local = threading.local()


def _ro(path: Path, attr: str) -> sqlite3.Connection | None:
    c = getattr(_local, attr, None)
    if c is None:
        if not path.exists():
            return None
        c = sqlite3.connect(f'file:{path}?mode=ro', uri=True, check_same_thread=False,
                            timeout=10)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA busy_timeout=10000')
        setattr(_local, attr, c)
    return c


def idx():
    c = _ro(INDEX_DB, 'idx')
    if c is None:
        raise RuntimeError('search index not available')
    return c


def flags_db():
    return _ro(FLAGS_DB, 'flags')


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
def r(v, nd=2):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return round(f, nd)


def parse_bbox(args) -> tuple[float, float, float, float] | None:
    """``bbox=w,s,e,n`` or ``west/south/east/north`` (aliases minlon…) or
    ``lon/lat[&radius_m]``. None when absent; ValueError when malformed."""
    b = args.get('bbox')
    if b:
        parts = [float(x) for x in b.split(',')]
        if len(parts) != 4:
            raise ValueError('bbox needs 4 numbers: west,south,east,north')
        w, s, e, n = parts
    else:
        def g(*names):
            for nm in names:
                v = args.get(nm)
                if v not in (None, ''):
                    return float(v)
            return None
        w, s, e, n = (g('west', 'minlon'), g('south', 'minlat'),
                      g('east', 'maxlon'), g('north', 'maxlat'))
        if None in (w, s, e, n):
            lon, lat = g('lon'), g('lat')
            if lon is None or lat is None:
                return None
            rad = max(1.0, min(2000.0, g('radius_m') or 15.0))
            dlat = rad / 110540.0
            dlon = rad / (111320.0 * math.cos(math.radians(lat)) or 1e-9)
            w, s, e, n = lon - dlon, lat - dlat, lon + dlon, lat + dlat
    if not (9.0 <= w <= 17.6 and 9.0 <= e <= 17.6 and 46.0 <= s <= 49.2 and 46.0 <= n <= 49.2):
        raise ValueError('bbox must lie within Austria (lon 9–17.6, lat 46–49.2)')
    if e < w or n < s:
        raise ValueError('bbox east<west or north<south')
    if (e - w) * (n - s) > MAX_BBOX_DEG2:
        raise ValueError(f'bbox too large (max {MAX_BBOX_DEG2} deg²; tile the viewport)')
    return (w, s, e, n)


_TF = None


def _tf():
    global _TF
    if _TF is None:
        from pyproj import Transformer
        _TF = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)
    return _TF


KG_COLS = ('k.kg_code, k.kg_name, k.gemeinde_code, k.gemeinde_name, k.processed, '
           'k.product_version, k.generated_at, k.json_v2_uploaded_at, k.json_uploaded_at, '
           'k.min_lon, k.min_lat, k.max_lon, k.max_lat, k.quality_grade, k.centroid_lon, k.centroid_lat')


def kgs_in_bbox(w, s, e, n, processed_only=True) -> list[sqlite3.Row]:
    q = (f'SELECT {KG_COLS} FROM kg_rtree r JOIN kg k ON k.rowid = r.id '
         'WHERE r.min_lon <= ? AND r.max_lon >= ? AND r.min_lat <= ? AND r.max_lat >= ?')
    if processed_only:
        q += ' AND k.processed = 1'
    return idx().execute(q + ' ORDER BY k.kg_code', (e, w, n, s)).fetchall()


def kg_codes(processed_only=False) -> list[str]:
    where = 'WHERE processed = 1' if processed_only else ''
    return [r[0] for r in idx().execute(f'SELECT kg_code FROM kg {where} ORDER BY kg_code')]


def kgs_list(processed_only=True, limit=1000, offset=0) -> tuple[list, int]:
    where = 'WHERE k.processed = 1' if processed_only else ''
    c = idx()
    total = c.execute(f'SELECT COUNT(*) FROM kg k {where}').fetchone()[0]
    rows = c.execute(f'SELECT {KG_COLS} FROM kg k {where} ORDER BY k.kg_code LIMIT ? OFFSET ?',
                     (int(limit), int(offset))).fetchall()
    return rows, total


def kg_row(code: str):
    return idx().execute('SELECT * FROM kg WHERE kg_code=?', (code,)).fetchone()


def _ne_state(code: str, pv) -> dict | None:
    """Cells-layer readiness from ``cells_api`` (None when unavailable)."""
    try:
        import cells_api
        return cells_api.ne_state(code, pv)
    except Exception:  # noqa: BLE001
        return None


#: Register value shown to the public while the cells file of a v2.4 KG has
#: not been published on this host yet.  Invariant (2026-10-07, siedler bug
#: report): ``product_version == "v2.4"`` ⇒ ``/api/v1/cells`` serves the KG
#: completely.  The raw register value stays in ``product_version_registry``.
PV_PENDING = 'v2.4-pending'


def kg_public(row, with_bbox=True) -> dict:
    pv = row['product_version']
    d = {'kg_code': row['kg_code'], 'kg_name': row['kg_name'],
         'gemeinde_code': row['gemeinde_code'], 'gemeinde_name': row['gemeinde_name'],
         'processed': bool(row['processed']),
         'product_version': pv,
         'product_version_registry': pv,
         'generated_at': row['generated_at'],
         'updated_at': row['json_v2_uploaded_at'] or row['json_uploaded_at'] or row['generated_at'],
         'quality_grade': row['quality_grade']}
    ns = _ne_state(row['kg_code'], pv)
    if ns is not None:
        d['ne_ready'] = ns['ne_ready']
        d['ne_status'] = ns['ne_status']
        d['ne_cells_published_at'] = ns['ne_cells_published_at']
        if pv == 'v2.4' and not ns['ne_ready']:
            d['product_version'] = PV_PENDING
            d['retry_after_s'] = ns.get('retry_after_s') or 3600
        if ns['ne_cells_published_at'] and (d['updated_at'] or '') < ns['ne_cells_published_at']:
            d['updated_at'] = ns['ne_cells_published_at']
    if with_bbox:
        d['bbox'] = [row['min_lon'], row['min_lat'], row['max_lon'], row['max_lat']]
        d['centroid'] = [row['centroid_lon'], row['centroid_lat']]
    return d


# --------------------------------------------------------------------------
# grid25 cache (terrain + landcover + derived aspect) per parent KG
# --------------------------------------------------------------------------
# each entry = elev + slope + aspect float32 (+ landcover) per product code,
# ~0.5-1 MB for a split KG → byte-bounded (``LQ_GRID_MB``, default 128 MB)
import mem_lru as _mem_lru
_grid_cache = _mem_lru.ByteLRU('lq_grids', 128, env='LQ_GRID_MB')


def grids_for_kg(kg_code: str) -> list[dict]:
    """Decoded grid25 blocks for a parent KG (one per product code; split
    KGs have several).  [] when the KG has no 2.1+ product yet."""
    g = _grid_cache.get(kg_code)
    if g is not None:
        return g
    import kg_v2_store as kvs
    import v21_products as v21
    out = []
    for code in kvs.codes_for_parent(kg_code) or []:
        try:
            sec = kvs.get_grid25(code)
            if not sec or not sec.get('terrain'):
                continue
            t = v21.decode_terrain_grid25(sec['terrain'])
            lc = v21.decode_landcover_grid25(sec['landcover']) if sec.get('landcover') else None
            # aspect (deg clockwise from N) from the DTM gradient; NaN where undefined
            e = t['elev']
            gy, gx = np.gradient(np.where(np.isfinite(e), e, np.nan), t['cell_m'])
            asp = (np.degrees(np.arctan2(gx, -gy)) + 180.0) % 360.0   # downslope direction (rows run N→S)
            bad = ~np.isfinite(gx) | ~np.isfinite(gy)
            asp = np.where(bad, np.nan, asp)
            asp = np.where(np.hypot(np.nan_to_num(gx), np.nan_to_num(gy)) < 0.02, np.nan, asp)  # <~1° → flat
            t['aspect'] = asp.astype(np.float32)
            out.append({'code': code, 'terrain': t, 'landcover': lc,
                        'product_version': kvs.product_version(code)})
        except Exception as ex:  # noqa: BLE001
            log.warning('grid25 %s: %s', code, ex)
    _grid_cache.put(kg_code, out)
    return out


def _cells_in_envelope(grid, E0, N0, E1, N1):
    """Cells overlapping the EPSG:3035 envelope → (index tuple, weights) where
    ``weights`` is the overlap fraction of each cell (0..1], so a lot-sized
    bbox straddling four 25 m cells is averaged by area, not by cell count.
    None when nothing overlaps."""
    cell = grid['cell_m']; x0 = grid['x0']; y0 = grid['y0']
    c0 = max(int(math.floor((E0 - x0) / cell)), 0)
    c1 = min(int(math.ceil((E1 - x0) / cell)), grid['cols'])
    r0 = max(int(math.floor((y0 - N1) / cell)), 0)
    r1 = min(int(math.ceil((y0 - N0) / cell)), grid['rows'])
    if c1 <= c0 or r1 <= r0:
        return None
    cs = np.arange(c0, c1); rs = np.arange(r0, r1)
    cl = x0 + cs * cell; cr = cl + cell                   # cell left/right
    ct = y0 - rs * cell; cb = ct - cell                   # cell top/bottom
    ox = np.clip(np.minimum(cr, E1) - np.maximum(cl, E0), 0, cell) / cell
    oy = np.clip(np.minimum(ct, N1) - np.maximum(cb, N0), 0, cell) / cell
    wgt = oy[:, None] * ox[None, :]
    if not (wgt > 0).any():
        return None
    return np.ix_(rs, cs), wgt.astype(np.float32)


def _wmean(vals, wgt):
    m = np.isfinite(vals) & (wgt > 0)
    if not m.any():
        return None
    return float((vals[m] * wgt[m]).sum() / wgt[m].sum())


def _sample_nearest(grid, arr, E, N):
    cell = grid['cell_m']
    c = int(math.floor((E - grid['x0']) / cell)); rr = int(math.floor((grid['y0'] - N) / cell))
    if 0 <= c < grid['cols'] and 0 <= rr < grid['rows']:
        return arr[rr, c]
    return None


def _aspect_label(deg):
    if deg is None or not math.isfinite(deg):
        return None
    return ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'][int(((deg + 22.5) % 360) // 45)]


def _terrain_class(slope_mean):
    if slope_mean is None:
        return None
    if slope_mean < 3:
        return 'flat'
    if slope_mean < 10:
        return 'gentle'
    if slope_mean < 20:
        return 'moderate'
    if slope_mean < 35:
        return 'steep'
    return 'very_steep'


# --------------------------------------------------------------------------
# QA flags (realistic trees / landmarks)
# --------------------------------------------------------------------------
_flag_cache: "OrderedDict[tuple, tuple]" = OrderedDict()
_flag_lock = threading.Lock()
_FLAG_TTL = 3600.0
_FLAG_CACHE_MAX = 2000


def flagged_points(kg_code: str, kind: str) -> set:
    """{(round(lon,5), round(lat,5))} of objects of ``kind`` in the KG that
    carry a high/critical QA flag.  Cached 1 h per (KG, kind)."""
    key = (kg_code, kind)
    now = time.time()
    with _flag_lock:
        hit = _flag_cache.get(key)
        if hit and now - hit[0] < _FLAG_TTL:
            _flag_cache.move_to_end(key)
            return hit[1]
    pts: set = set()
    c = flags_db()
    if c is not None:
        try:
            rows = c.execute(
                'SELECT centroid_lon, centroid_lat FROM flags WHERE kg_code=? AND kind=? '
                'AND severity IN (%s)' % ','.join('?' * len(FLAG_SEVERITIES)),
                (kg_code, kind, *FLAG_SEVERITIES)).fetchall()
            pts = {(round(a, 5), round(b, 5)) for a, b in rows if a is not None and b is not None}
        except Exception as e:  # noqa: BLE001
            log.debug('flags %s/%s: %s', kg_code, kind, e)
    with _flag_lock:
        _flag_cache[key] = (now, pts)
        while len(_flag_cache) > _FLAG_CACHE_MAX:
            _flag_cache.popitem(last=False)
    return pts


def _is_flagged(pts: set, lon, lat) -> bool:
    return (round(lon, 5), round(lat, 5)) in pts


# --------------------------------------------------------------------------
# trees / landmarks / buildings in bbox
# --------------------------------------------------------------------------
def trees_bbox(w, s, e, n, min_height=0.0, limit=2000, kg_rows=None, realistic=True,
               kind='trees', types=None) -> dict:
    """Tree apices (or ``kind='structures'``) in a bbox, tallest first, read
    straight from the NE cells corpus via :mod:`ne_trees` (the legacy
    ``kg_trees`` table is empty since the Kataster purge).  ``realistic``
    applies the data-derived plausibility rules + both flag stores.  Output
    keeps the public field names (lon/lat/height_m/crown_d_m/confidence/kg_code)."""
    import ne_trees
    r = ne_trees.points_bbox(w, s, e, n, kind, min_height=float(min_height),
                             limit=limit, realistic=realistic, types=types)
    key = 'trees' if kind == 'trees' else 'structures'
    return {'trees': r[key], 'kgs': r['kgs'], 'dropped_qa': r['dropped_qa'], 'truncated': r['truncated'],
            'ready': r['ready'], 'pending_kgs': r['pending_kgs'], 'not_processed': r['not_processed'],
            'realistic': r['realistic'], 'chunks_scanned': r['chunks_scanned'], 'ms': r['ms'], 'kind': kind}


def landmarks_bbox(w, s, e, n, kg_rows=None) -> dict:
    """Tall man-made verticals (church towers, silos, masts, wind turbines,
    bridges) from the per-KG top-N-by-type table, QA-filtered."""
    kg_rows = kg_rows if kg_rows is not None else kgs_in_bbox(w, s, e, n)
    codes = [k['kg_code'] for k in kg_rows]
    if not codes:
        return {'landmarks': [], 'kgs': []}
    rows = idx().execute(
        'SELECT kg_code, object_type, lon, lat, height_max_m, area_sqm, rf_confidence, confidence '
        'FROM kg_type_top WHERE kg_code IN (%s) AND object_type IN (%s) '
        'AND lon >= ? AND lon <= ? AND lat >= ? AND lat <= ? AND height_max_m IS NOT NULL '
        'ORDER BY height_max_m DESC LIMIT 500'
        % (','.join('?' * len(codes)), ','.join('?' * len(LANDMARK_TYPES))),
        (*codes, *LANDMARK_TYPES.keys(), w, e, s, n)).fetchall()
    flagged = {c: flagged_points(c, 'top_by_type') for c in codes}
    out = []
    for kg, typ, lon, lat, h, area, rf, conf in rows:
        lo, hi = LANDMARK_TYPES[typ]
        if h is None or not (lo <= h <= hi):
            continue
        if rf is not None and rf < LANDMARK_RF_MIN:
            continue
        if _is_flagged(flagged[kg], lon, lat):
            continue
        out.append({'type': typ, 'lon': round(lon, 7), 'lat': round(lat, 7),
                    'height_m': r(h, 1), 'area_sqm': r(area, 0), 'confidence': r(rf, 2),
                    'kg_code': kg})
    return {'landmarks': out, 'kgs': codes}


def buildings_bbox(w, s, e, n, limit=3000, kg_rows=None) -> dict:
    """Measured structure heights at the centroid of each built-up object
    we segmented (LiDAR nDSM). No ids, no footprint geometry/area — match
    to your own footprints by point-in-polygon."""
    kg_rows = kg_rows if kg_rows is not None else kgs_in_bbox(w, s, e, n)
    codes = [k['kg_code'] for k in kg_rows]
    if not codes:
        return {'buildings': [], 'kgs': [], 'truncated': False}
    lim = max(1, min(int(limit), 5000))
    rows = idx().execute(
        'SELECT kg_code, centroid_lon, centroid_lat, max_height_m, mean_height_m, stories_est, '
        'roof_type_hint, centroid_dtm_m FROM kg_buildings WHERE kg_code IN (%s) '
        'AND centroid_lon >= ? AND centroid_lon <= ? AND centroid_lat >= ? AND centroid_lat <= ? '
        'AND max_height_m IS NOT NULL ORDER BY max_height_m DESC LIMIT ?'
        % ','.join('?' * len(codes)), (*codes, w, e, s, n, lim + 1)).fetchall()
    out = [{'lon': round(b[1], 7), 'lat': round(b[2], 7), 'height_max_m': r(b[3], 1),
            'height_mean_m': r(b[4], 1), 'stories_est': b[5],
            'roof': b[6] or None, 'ground_elev_m': r(b[7], 1), 'kg_code': b[0]}
           for b in rows[:lim] if b[1] is not None and b[2] is not None]
    return {'buildings': out, 'kgs': codes, 'truncated': len(rows) > lim}


# --------------------------------------------------------------------------
# the bbox primitive
# --------------------------------------------------------------------------
def landscape_bbox(w, s, e, n, *, want_trees=True, want_buildings=True,
                   want_landmarks=False, tree_limit=25) -> dict:
    kg_rows = kgs_in_bbox(w, s, e, n, processed_only=False)
    processed = [k for k in kg_rows if k['processed']]
    kgs_out = []
    grids = []
    for k in kg_rows:
        entry = kg_public(k, with_bbox=False)
        g = grids_for_kg(k['kg_code']) if k['processed'] else []
        entry['grid25'] = bool(g)
        grids.extend(g)
        kgs_out.append(entry)

    tf = _tf()
    xs, ys = tf.transform([w, e, e, w], [s, s, n, n])
    E0, E1, N0, N1 = min(xs), max(xs), min(ys), max(ys)
    cx, cy = tf.transform((w + e) / 2, (s + n) / 2)

    elev = []; slope = []; asp_x = []; asp_y = []; wsum = 0.0
    e_acc = s_acc = 0.0; e_w = s_w = 0.0
    cover: dict[str, float] = {}
    woody = []; tree_f = []; canopy = []
    n_cells = 0; n_valid = 0
    versions = set()
    for g in grids:
        t = g['terrain']; lc = g['landcover']
        if g.get('product_version'):
            versions.add(g['product_version'])
        hit = _cells_in_envelope(t, E0, N0, E1, N1)
        if hit is None:
            continue
        sel, wgt = hit
        ev = t['elev'][sel]
        ok = np.isfinite(ev) & (wgt > 0)
        n_cells += int((wgt > 0).sum())
        n_valid += int(ok.sum())
        if ok.any():
            elev.append(ev[ok])
            e_acc += float((ev[ok] * wgt[ok]).sum()); e_w += float(wgt[ok].sum())
        if t.get('slope') is not None:
            sv = t['slope'][sel]; m = np.isfinite(sv) & (wgt > 0)
            if m.any():
                slope.append(sv[m]); s_acc += float((sv[m] * wgt[m]).sum()); s_w += float(wgt[m].sum())
        av = t['aspect'][sel]; m = np.isfinite(av) & (wgt > 0)
        if m.any():
            asp_x.append(np.sin(np.radians(av[m])) * wgt[m]); asp_y.append(np.cos(np.radians(av[m])) * wgt[m])
            wsum += float(wgt[m].sum())
        if lc is not None and lc['x0'] == t['x0'] and lc['y0'] == t['y0'] and lc['cls'].shape == t['elev'].shape:
            cls = lc['cls'][sel]; frac = lc['cover_frac'][sel] * wgt
            legend = lc['legend']
            for code in np.unique(cls):
                if code == 0:
                    continue
                name = legend.get(str(int(code)))
                if not name:
                    continue
                cover[name] = cover.get(name, 0.0) + float(frac[cls == code].sum())
            for key, acc in (('woody_frac', woody), ('tree_frac', tree_f), ('canopy_frac', canopy)):
                a = lc.get(key)
                if a is not None:
                    v = a[sel]; m = np.isfinite(v) & (wgt > 0)
                    if m.any():
                        acc.append((v[m], wgt[m]))

    point_sampled = False
    if n_valid == 0 and grids:
        # bbox smaller than a cell / between centres → nearest cell at the centre
        for g in grids:
            t = g['terrain']; lc = g['landcover']
            ev = _sample_nearest(t, t['elev'], cx, cy)
            if ev is None or not np.isfinite(ev):
                continue
            point_sampled = True
            elev.append(np.array([ev])); n_cells = n_valid = 1
            e_acc, e_w = float(ev), 1.0
            if t.get('slope') is not None:
                sv = _sample_nearest(t, t['slope'], cx, cy)
                if sv is not None and np.isfinite(sv):
                    slope.append(np.array([sv])); s_acc, s_w = float(sv), 1.0
            av = _sample_nearest(t, t['aspect'], cx, cy)
            if av is not None and np.isfinite(av):
                asp_x.append(np.array([math.sin(math.radians(av))]))
                asp_y.append(np.array([math.cos(math.radians(av))])); wsum = 1.0
            if lc is not None:
                code = _sample_nearest(lc, lc['cls'], cx, cy)
                if code is not None and int(code) != 0:
                    name = lc['legend'].get(str(int(code)))
                    if name:
                        cf = _sample_nearest(lc, lc['cover_frac'], cx, cy)
                        cover[name] = float(cf) if cf is not None and cf > 0 else 1.0
                for key, acc in (('woody_frac', woody), ('tree_frac', tree_f), ('canopy_frac', canopy)):
                    a = lc.get(key)
                    if a is not None:
                        v = _sample_nearest(lc, a, cx, cy)
                        if v is not None and np.isfinite(v):
                            acc.append((np.array([v]), np.array([1.0])))
            break

    out: dict = {
        'bbox': [w, s, e, n],
        'kgs': kgs_out,
        'ready': bool(processed),
        'resolution': '25m' if grids else ('kg' if processed else None),
        'fine_grid_complete': bool(processed) and all(k['grid25'] for k in kgs_out if k['processed']),
        'coverage': {
            'kgs_total': len(kg_rows), 'kgs_processed': len(processed),
            'kgs_grid25': sum(1 for k in kgs_out if k['grid25']),
            'cells_25m': int(n_cells), 'cells_valid': int(n_valid),
            'point_sampled': point_sampled,
        },
        'product_versions': sorted(versions),
    }
    if not kg_rows:
        out['missing'] = {'all': 'no Katastralgemeinde of our index intersects this bbox'}
        return out
    if not processed:
        out['missing'] = {'all': 'no KG here is processed yet (retry later; the fleet works through all 7850)'}
        return out

    # ---- terrain -----------------------------------------------------
    if elev and sum(a.size for a in elev):
        ev = np.concatenate(elev)
        sl = np.concatenate(slope) if slope else np.array([])
        slope_mean = r(s_acc / s_w, 1) if s_w > 0 else None
        asp_deg = None
        if asp_x and wsum > 0:
            ax = float(np.concatenate(asp_x).sum() / wsum); ay = float(np.concatenate(asp_y).sum() / wsum)
            if math.hypot(ax, ay) > 0.15:          # otherwise aspect is mixed / flat
                asp_deg = (math.degrees(math.atan2(ax, ay)) + 360.0) % 360.0
        out['terrain'] = {
            'elevation_min_m': r(ev.min(), 1), 'elevation_max_m': r(ev.max(), 1),
            'elevation_mean_m': r(e_acc / e_w, 1) if e_w > 0 else r(ev.mean(), 1),
            'elevation_range_m': r(ev.max() - ev.min(), 1),
            'slope_mean_deg': slope_mean,
            'slope_max_deg': r(sl.max(), 1) if sl.size else None,
            'aspect_deg': r(asp_deg, 0), 'aspect': _aspect_label(asp_deg),
            'terrain_class': _terrain_class(slope_mean),
            'resolution_m': 25,
            'source': 'BEV ALS DTM 1 m → 25 m block means (product grid25)',
        }
    else:
        # KG processed but still a v1 product (no grid25): KG-level stats only
        kr = kg_row(processed[0]['kg_code'])
        out['terrain'] = {
            'elevation_min_m': r(kr['elevation_min_m'], 1), 'elevation_max_m': r(kr['elevation_max_m'], 1),
            'elevation_mean_m': r(kr['elevation_mean_m'], 1), 'slope_mean_deg': r(kr['slope_mean_deg'], 1),
            'aspect': kr['aspect_dominant'], 'terrain_class': kr['terrain_class'],
            'resolution_m': None, 'resolution': 'kg',
            'source': 'KG-level aggregate (no 25 m grid yet for this KG — v1 product)',
        }
        out.setdefault('missing', {})['grid25'] = 'KG not yet upgraded to product ≥2.1; values are KG averages'

    # ---- land cover --------------------------------------------------
    tot = sum(cover.values())
    if tot > 0:
        fr = {k: round(v / tot, 3) for k, v in cover.items() if v / tot >= 0.005}
        dom = max(cover, key=cover.get)
        nat = {k: v for k, v in cover.items() if k not in IMPERVIOUS}
        dom_terrain = max(nat, key=nat.get) if nat else None
        lcov = {
            'fractions': dict(sorted(fr.items(), key=lambda kv: -kv[1])),
            'dominant_type': dom, 'dom_terrain': dom_terrain,
            'natural_fraction': round(sum(v for k, v in cover.items() if k in NATURAL) / tot, 3),
            'impervious_fraction': round(sum(v for k, v in cover.items() if k in IMPERVIOUS) / tot, 3),
            'woody_fraction': round(sum(v for k, v in cover.items() if k in WOODY) / tot, 3),
            'resolution_m': 25,
            'source': 'dominant segment class per 25 m cell × covered fraction (LiDAR + ortho + Sentinel-2 segmentation)',
        }
        def _wm(acc):
            if not acc:
                return None
            v = np.concatenate([a for a, _ in acc]); wg = np.concatenate([b for _, b in acc])
            return r((v * wg).sum() / wg.sum(), 3) if wg.sum() > 0 else None
        # per-cell pixel shares (1 m segmentation inside each 25 m cell):
        # woody = tree+shrub+hedge+orchard pixels, tree = tree pixels,
        # canopy = apex-inventory crown cover. Finer than the class shares above.
        lcov['woody_pixel_share'] = _wm(woody)
        lcov['tree_pixel_share'] = _wm(tree_f)
        lcov['canopy_pixel_share'] = _wm(canopy)
        out['landcover'] = lcov
    else:
        # fall back to KG-level landcover table
        k = processed[0]['kg_code']
        rows = idx().execute('SELECT object_type, fraction FROM kg_landcover WHERE kg_code=? '
                             'ORDER BY fraction DESC', (k,)).fetchall()
        if rows:
            fr = {a: round(b, 3) for a, b in rows if b and b >= 0.005}
            nat = {a: b for a, b in rows if a not in IMPERVIOUS and b}
            out['landcover'] = {'fractions': fr, 'dominant_type': rows[0][0],
                                'dom_terrain': max(nat, key=nat.get) if nat else None,
                                'resolution': 'kg',
                                'source': 'KG-level land-cover shares (no 25 m grid for this KG yet)'}

    # ---- trees / buildings / landmarks ------------------------------
    if want_trees:
        t = trees_bbox(w, s, e, n, limit=max(1, min(int(tree_limit), 200)), kg_rows=processed)
        hs = [x['height_m'] for x in t['trees'] if x['height_m'] is not None]
        out['trees'] = {
            'count_sampled': len(t['trees']), 'truncated': t['truncated'],
            'height_max_m': max(hs) if hs else None,
            'height_mean_m': r(sum(hs) / len(hs), 1) if hs else None,
            'tallest': [{k: v for k, v in x.items() if k != 'kg_code'} for x in t['trees'][:10]],
            'dropped_qa': t['dropped_qa'],
            'note': 'tallest apices from the NE cells corpus (every apex h ≥ 3 m); '
                    'realistic set: saturated / spike-like (mast, turbine) / flagged apices removed',
        }
    if want_buildings:
        b = buildings_bbox(w, s, e, n, limit=200, kg_rows=processed)
        hs = [x['height_max_m'] for x in b['buildings'] if x['height_max_m'] is not None]
        out['buildings'] = {
            'count': len(b['buildings']), 'truncated': b['truncated'],
            'height_max_m': max(hs) if hs else None,
            'stories_max': max((x['stories_est'] or 0) for x in b['buildings']) if b['buildings'] else None,
            'roof_pitched_share': (round(sum(1 for x in b['buildings'] if x['roof'] == 'pitched')
                                         / len(b['buildings']), 2) if b['buildings'] else None),
            'note': 'LiDAR-measured heights of built-up objects; no outlines/ids — /api/v1/buildings/bbox lists the points',
        }
    if want_landmarks:
        out['landmarks'] = landmarks_bbox(w, s, e, n, kg_rows=processed)['landmarks']

    # ---- forest loss (KG-level, Hansen GFC) ---------------------------
    codes = [k['kg_code'] for k in processed]
    hrows = idx().execute(
        'SELECT loss_year, SUM(loss_pixels) FROM kg_hansen WHERE kg_code IN (%s) '
        'GROUP BY loss_year ORDER BY loss_year' % ','.join('?' * len(codes)), codes).fetchall()
    if hrows:
        out['forest_loss'] = {
            'resolution': 'kg', 'unit': 'Hansen GFC 30 m pixels per year, summed over the KGs touched',
            'by_year': {str(int(y)): int(p or 0) for y, p in hrows if y},
            'since_2020_px': int(sum((p or 0) for y, p in hrows if y and int(y) >= 2020)),
        }
    return out


# --------------------------------------------------------------------------
# heightfield for 2.5D relief
# --------------------------------------------------------------------------
def heightfield_bbox(w, s, e, n, cell=25, want_landcover=False, max_cells=250_000) -> tuple[dict, int]:
    cell = max(25, min(500, int(cell)))
    lat0 = (s + n) / 2.0
    mx = 111320.0 * math.cos(math.radians(lat0)); my = 110540.0
    cols = int(math.ceil((e - w) * mx / cell)) + 1
    rows = int(math.ceil((n - s) * my / cell)) + 1
    if cols * rows > max_cells:
        return {'error': f'grid too large ({cols}×{rows}); increase cell or shrink bbox'}, 400
    kg_rows = kgs_in_bbox(w, s, e, n)
    grids = [g for k in kg_rows for g in grids_for_kg(k['kg_code'])]
    if not grids:
        return {'error': 'no product ≥2.1 grid for this bbox yet', 'ready': False,
                'kgs': [k['kg_code'] for k in kg_rows]}, 404
    gx = w + (np.arange(cols) * cell) / mx
    gy = n - (np.arange(rows) * cell) / my
    LON, LAT = np.meshgrid(gx, gy)
    E, N = _tf().transform(LON.ravel(), LAT.ravel())
    E = np.asarray(E); N = np.asarray(N)
    Z = np.full(E.shape, np.nan, np.float32)
    LC = np.zeros(E.shape, np.int16) if want_landcover else None
    legend: dict = {}
    from grid25_sample import merge
    Z = merge([g['terrain'] for g in grids], E, N)
    for g in grids:
        lc = g['landcover']
        if want_landcover and lc is not None:
            c = _nearest(lc, lc['cls'], E, N)
            f2 = (LC == 0) & np.isfinite(c)
            LC[f2] = c[f2].astype(np.int16)
            legend.update(lc['legend'])
    Z = Z.reshape(rows, cols)
    fin = Z[np.isfinite(Z)]
    payload = {
        'bbox': [w, s, e, n], 'cell_m': cell, 'cols': cols, 'rows': rows,
        'origin': 'nw', 'rows_major_north_to_south': True,
        'elev_min': r(fin.min(), 1) if fin.size else None,
        'elev_max': r(fin.max(), 1) if fin.size else None,
        'null_cells': int(np.isnan(Z).sum()),
        'kgs': [k['kg_code'] for k in kg_rows],
        'source': 'BEV ALS DTM 1 m (CC BY 4.0, bearbeitet) → 25 m block means, bilinear',
        'z': [[None if not np.isfinite(v) else round(float(v), 1) for v in row] for row in Z],
    }
    if want_landcover:
        payload['landcover'] = [[int(v) for v in row] for row in LC.reshape(rows, cols)]
        payload['landcover_legend'] = legend
    return payload, 200


def _nearest(grid, a, E, N):
    cell = grid['cell_m']; rows, cols = a.shape
    fx = (E - grid['x0']) / cell - 0.5; fy = (grid['y0'] - N) / cell - 0.5
    inside = (fx > -0.5) & (fx < cols - 0.5) & (fy > -0.5) & (fy < rows - 0.5)
    out = np.full(E.shape, np.nan, np.float32)
    if not inside.any():
        return out
    ci = np.clip(np.round(fx).astype(int), 0, cols - 1); ri = np.clip(np.round(fy).astype(int), 0, rows - 1)
    v = a[ri, ci].astype(np.float32)
    out[inside] = v[inside]
    return out


def _bilinear(grid, a, E, N):
    cell = grid['cell_m']; rows, cols = a.shape
    fx = (E - grid['x0']) / cell - 0.5; fy = (grid['y0'] - N) / cell - 0.5
    inside = (fx > -0.5) & (fx < cols - 0.5) & (fy > -0.5) & (fy < rows - 0.5)
    out = _nearest(grid, a, E, N)
    if not inside.any():
        return out
    c0 = np.clip(np.floor(fx).astype(int), 0, cols - 1); c1 = np.clip(c0 + 1, 0, cols - 1)
    r0 = np.clip(np.floor(fy).astype(int), 0, rows - 1); r1 = np.clip(r0 + 1, 0, rows - 1)
    wx = np.clip(fx - c0, 0, 1); wy = np.clip(fy - r0, 0, 1)
    bil = (a[r0, c0] * (1 - wx) * (1 - wy) + a[r0, c1] * wx * (1 - wy)
           + a[r1, c0] * (1 - wx) * wy + a[r1, c1] * wx * wy)
    ok = inside & np.isfinite(bil)
    out[ok] = bil[ok]
    return out


# --------------------------------------------------------------------------
# per-KG summary
# --------------------------------------------------------------------------
def kg_summary(code: str) -> dict | None:
    k = kg_row(code)
    if not k:
        return None
    c = idx()
    d = kg_public(k)
    if not k['processed']:
        d['ready'] = False
        return d
    lc = c.execute('SELECT object_type, fraction, area_sqm, n_objects, height_mean, height_p90, height_max '
                   'FROM kg_landcover WHERE kg_code=? ORDER BY area_sqm DESC', (code,)).fetchall()
    cover = {x[0]: x[1] for x in lc if x[1]}
    nat = {a: b for a, b in cover.items() if a not in IMPERVIOUS}
    d.update({
        'ready': True,
        'terrain': {
            'elevation_min_m': r(k['elevation_min_m'], 1), 'elevation_max_m': r(k['elevation_max_m'], 1),
            'elevation_mean_m': r(k['elevation_mean_m'], 1), 'slope_mean_deg': r(k['slope_mean_deg'], 1),
            'steepness_max_deg': r(k['steepness_max_deg'], 1), 'aspect_dominant': k['aspect_dominant'],
            'terrain_class': k['terrain_class'], 'roughness_mean': r(k['roughness_mean'], 3),
        },
        'landcover': {
            'fractions': {a: round(b, 3) for a, b in cover.items() if b >= 0.005},
            'dominant_type': k['dominant_type'],
            'dom_terrain': max(nat, key=nat.get) if nat else None,
            'vegetated_fraction': r(k['vegetated_fraction'], 3),
            'woody_fraction': round(sum(v for a, v in cover.items() if a in WOODY), 3),
            'shannon_diversity': r(k['shannon_diversity'], 3),
            'by_type': [{'type': x[0], 'fraction': r(x[1], 4), 'n_objects': x[3],
                         'height_mean_m': r(x[4], 1), 'height_p90_m': r(x[5], 1)} for x in lc if x[1]],
        },
        'vegetation': {
            'ndvi_mean': r(k['ndvi_mean'], 3), 'ndvi_harm_amplitude': r(k['ndvi_harm_amplitude'], 3),
            'phenology_dominant': k['phenology_dominant'],
            'tree_count': k['tree_count'], 'tree_canopy_sqm': r(k['tree_canopy_sqm'], 0),
            'tree_mean_height_m': r(k['tree_mean_height_m'], 1),
            'tree_stem_volume_m3': r(k['tree_stem_volume_m3'], 0),
        },
        'built': {
            'building_count': k['building_count'],
            'building_mean_height_m': r(k['building_mean_height_m'], 1),
            'building_max_height_m': r(k['building_max_height_m'], 1),
            'building_stories_mean': r(k['building_stories_mean'], 1),
            'building_pitched_pct': r(k['building_pitched_pct'], 1),
            'new_building_count': k['new_building_count'],
            'infrastructure_count': k['infrastructure_count'],
        },
        'change': {
            'net_volume_change_m3': r(k['net_volume_change_m3'], 0),
            'n_changed_segments': k['n_changed_segments'],
            'temporal_stability': r(k['temporal_stability'], 3),
        },
        'quality': {'score': r(k['quality_score'], 3), 'grade': k['quality_grade'],
                    'rf_mean_confidence': r(k['rf_mean_confidence'], 3)},
        'grid25': bool(grids_for_kg(code)),
        'kg_area_sqm': r(k['total_area_sqm'], 0),
    })
    hrows = c.execute('SELECT loss_year, loss_pixels FROM kg_hansen WHERE kg_code=? ORDER BY loss_year',
                      (code,)).fetchall()
    if hrows:
        d['forest_loss'] = {'by_year': {str(int(y)): int(p or 0) for y, p in hrows if y},
                            'since_2020_px': int(sum((p or 0) for y, p in hrows if y and int(y) >= 2020)),
                            'unit': 'Hansen GFC 30 m pixels'}
    return d


_status_cache = {'t': 0.0, 'v': None}


def status() -> dict:
    now = time.time()
    if _status_cache['v'] is not None and now - _status_cache['t'] < 300:
        return _status_cache['v']
    v = _status_uncached()
    _status_cache.update(t=now, v=v)
    return v


def _status_uncached() -> dict:
    c = idx()
    tot, proc = c.execute('SELECT COUNT(*), SUM(processed) FROM kg').fetchone()
    pv = c.execute('SELECT product_version, COUNT(*) FROM kg WHERE processed=1 GROUP BY 1').fetchall()
    meta = dict(c.execute('SELECT key, value FROM index_meta').fetchall())
    try:
        import kg_v2_store as kvs
        g25 = kvs.stats().get('grid25_codes')
    except Exception:  # noqa: BLE001
        g25 = None
    pvd = {str(a): b for a, b in pv}
    try:
        from v21_products import MANIFEST_VERSION as _cur
    except Exception:  # noqa: BLE001
        _cur = 'v2.4'
    cur_n = int(pvd.get(str(_cur)) or 0)  # full current product only (no -partial)
    uh = universe_hash(r[0] for r in c.execute('SELECT kg_code FROM kg'))
    try:
        import cells_api
        cells_gen = cells_api.cells_generation()
        ne_served = cells_api.index_status().get('kgs_ready', 0)
    except Exception:  # noqa: BLE001
        cells_gen, ne_served = None, None
    # Coherence with /api/v1/kgs (cells publication invariant): a KG counts
    # as the full current product only when its cells file is served
    # completely; the rest of the register's vX.Y rows are "vX.Y-pending".
    cur_complete, cur_pending = cur_n, 0
    if ne_served is not None:
        ne_rows = [r[0] for r in c.execute('SELECT kg_code FROM kg WHERE processed=1 AND product_version=?', (str(_cur),))]
        try:
            ready = {k for k in ne_rows if (cells_api.ne_state(k, str(_cur)) or {}).get('ne_ready')}
        except Exception:  # noqa: BLE001
            ready = None
        if ready is not None:
            cur_complete = len(ready)
            cur_pending = len(ne_rows) - cur_complete
            if cur_pending:
                pvd[str(_cur)] = cur_complete
                pvd[f'{_cur}-pending'] = cur_pending
    return {'kgs_total': KG_TOTAL, 'cells_generation': cells_gen, 'kgs_ne_served': ne_served,
            'manifest_generation': f'{uh[:12]}.{cells_gen}.{meta.get("last_incremental_at") or meta.get("built_at")}', 'kgs_indexed': tot, 'kgs_processed': int(proc or 0),
            'universe_hash': uh, 'universe_hash_recipe': UNIVERSE_HASH_RECIPE,
            'universe_hash_contract': UNIVERSE_HASH_CONTRACT,
            'universe_agrees_contract': uh == UNIVERSE_HASH_CONTRACT,
            'product_version_current': str(_cur), 'kgs_current': cur_complete,
            'kgs_current_complete': cur_complete, 'kgs_current_pending': cur_pending,
            'kgs_current_registry': cur_n,
            'product_versions': pvd,
            'grid25_codes': g25,
            'index_updated_at': meta.get('last_incremental_at') or meta.get('built_at')}
