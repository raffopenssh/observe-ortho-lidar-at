"""grid25_patch — re-derive wrong ``terrain.grid25`` tiles from the BEV DTM.

Why: a product is stitched from ~1.5 km processing tiles.  Some tiles were
restored from rasters fetched while BEV still served elevation beyond the
national border (early 2026); that stale tile content does not match today's
BEV DTM even on the Austrian side (Nofels 92116: 440–488 m stored vs 429 m
real on the Rhine plain) → an opaque rectangle across the border and a
cliff line along the tile edge in the hillshade tiles.  A registration
offset cannot repair that; a product reprocess would.  Until then the bad
tile's cells are rebuilt here exactly as the pipeline builds them (25 m
block mean of the 1 m DTM, ≥ 20 % valid) and overlaid at read time by
``grid25_sample.register`` → every grid25 reader gets them.

Detection (``--scan``): products with cells > 300 m outside the national
outline → their processing tiles holding such cells are *suspect* → two
25 m strips of BEV DTM per suspect tile → median |stored − BEV| over the
Austrian cells > ``BAD_M`` ⇒ bad ⇒ patched.  Throttled on host RX bytes,
resumable (``data/grid25_patches/index.json``).

    python3 grid25_patch.py --edge-scan           # local: flag cliff-like internal tile edges (all products)
    python3 grid25_patch.py --scan [--budget-mb-h 150] [--codes a,b] [--max N]
    python3 grid25_patch.py --tile CODE E N       # force-check the tile at an EPSG:3035 point
    python3 grid25_patch.py --status

Docs: docs/hillshade-seams.md → Stale tiles / grid25 patches.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

import numpy as np

DIR = Path(os.environ.get('GRID25_PATCH_DIR', str(Path(__file__).resolve().parent / 'data' / 'grid25_patches')))
INDEX = DIR / 'index.json'
CELL = 25.0
FAR_M = 300.0          # "far outside": beyond what BEV still serves as border buffer in most places
MIN_FAR_CELLS = 40
BAD_M = 0.5            # median |stored − BEV| (m) over a tile's Austrian check cells (healthy tiles: < 0.1)
MIN_CHECK_CELLS = 6
_LOCK = threading.Lock()
_ST = {'mtime': None, 'doc': {}}


def _index() -> dict:
    try:
        mt = INDEX.stat().st_mtime
    except OSError:
        return {}
    if _ST['mtime'] != mt:
        with _LOCK:
            try:
                _ST.update(mtime=mt, doc=json.loads(INDEX.read_text()))
            except Exception:  # noqa: BLE001
                _ST['mtime'] = mt
    return _ST['doc']


def version() -> int:
    return int(_index().get('version') or 0)


def apply(code, grid: dict) -> bool:
    """Overlay the stored patch blocks of *code* on a decoded terrain grid
    (BEFORE registration / clipping).  Finite patch cells replace, cells the
    BEV DTM has no data for become nodata."""
    ent = (_index().get('patched') or {}).get(str(code))
    if not ent:
        return False
    a = grid.get('elev')
    if a is None or [float(grid['x0']), float(grid['y0']), a.shape[0], a.shape[1]] != ent['grid']:
        return False                      # product re-ingested with another grid → patch no longer valid
    z = np.load(DIR / f'{code}.npz')
    a = np.array(a, np.float32, copy=True)
    sl = grid.get('slope')
    if sl is not None and getattr(sl, 'shape', None) == a.shape:
        sl = np.array(sl, np.float32, copy=True)
    else:
        sl = None
    for i, (r0, c0, r1, c1) in enumerate(ent['blocks']):
        a[r0:r1, c0:c1] = z[f'e{i}'].astype(np.float32) / 10.0
        a[r0:r1, c0:c1][z[f'e{i}'] == -32768] = np.nan
        if sl is not None:
            s = z[f's{i}'].astype(np.float32)
            s[z[f's{i}'] == 255] = np.nan
            sl[r0:r1, c0:c1] = s
    grid['elev'] = a
    if sl is not None:
        grid['slope'] = sl
    grid['_patched'] = len(ent['blocks'])
    return True


def status() -> dict:
    d = _index()
    import collections
    c = collections.Counter(t['v'] for e in (d.get('checked') or {}).values() for t in e.get('tiles') or [])
    return {'version': version(), 'products_checked': len(d.get('checked') or {}), 'tiles': dict(c),
            'products_patched': len(d.get('patched') or {}),
            'tiles_patched': sum(len(e['blocks']) for e in (d.get('patched') or {}).values()),
            'edge_scan': d.get('edge_scan'), 'edges_open': edges_open(d),
            'mb': round(float(d.get('mb') or 0), 1), 'updated_at': d.get('updated_at')}


def edges_open(d=None) -> int:
    """Flagged internal tile edges whose product has not been BEV-checked yet
    (after the check an edge is either patched away or confirmed real terrain)."""
    d = d if d is not None else _index()
    ch = d.get('checked') or {}
    return sum(len(v) for c, v in (d.get('edges') or {}).items() if not (ch.get(c) or {}).get('edges_done'))


# ── build ──────────────────────────────────────────────────────────────

def _raw_grid(code):
    import kg_v2_store as kvs
    import v21_products as v21
    sec = kvs.get_grid25(code)
    if not sec or not sec.get('terrain'):
        return None
    return v21.decode_terrain_grid25(sec['terrain'])


def _tiles(code, g):
    """[(tile_index, r0, c0, r1, c1)] — processing tiles as cell ranges of the grid."""
    import kg_v2_store as kvs
    from pyproj import Transformer
    tf = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)
    d = kvs.get(code) or {}
    rows, cols = g['elev'].shape
    out = []
    for t in (d.get('data_quality') or {}).get('tiles') or []:
        bb = t.get('bbox_wgs')
        if not bb:
            continue
        w, s, e, n = bb
        ex, ny = tf.transform([w, e, w, e], [s, s, n, n])
        x0, x1, y0, y1 = (ex[0] + ex[2]) / 2, (ex[1] + ex[3]) / 2, (ny[0] + ny[1]) / 2, (ny[2] + ny[3]) / 2
        c0 = int(round((x0 - g['x0']) / CELL)); c1 = int(round((x1 - g['x0']) / CELL))
        r0 = int(round((g['y0'] - y1) / CELL)); r1 = int(round((g['y0'] - y0) / CELL))
        c0, r0, c1, r1 = max(c0, 0), max(r0, 0), min(c1, cols), min(r1, rows)
        if c1 > c0 and r1 > r0:
            out.append((int(t.get('tile_index') or 0), r0, c0, r1, c1))
    return out


def _bev_block(x0, y0, rows, cols):
    """25 m block mean (+ mean slope, deg) of the BEV 1 m DTM for a cell
    window whose upper-left corner is x0, y0 — the pipeline's own recipe."""
    import raster_io
    dtm, tf, _ = raster_io.read_window_bbox('DTM', x0, y0 - rows * CELL, x0 + cols * CELL, y0, pad=0.0)
    full = np.full((int(rows * CELL), int(cols * CELL)), np.nan, np.float32)
    co = int(round((tf.c - x0))); ro = int(round((y0 - tf.f)))
    h = min(dtm.shape[0], full.shape[0] - ro); w = min(dtm.shape[1], full.shape[1] - co)
    if h > 0 and w > 0 and ro >= 0 and co >= 0:
        full[ro:ro + h, co:co + w] = dtm[:h, :w]
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f5 = np.nanmean(full.reshape(rows * 5, 5, cols * 5, 5), axis=(1, 3))
        gy, gx = np.gradient(f5, 5.0)
        s5 = np.degrees(np.arctan(np.hypot(gx, gy)))
        blk = full.reshape(rows, 25, cols, 25)
        frac = np.isfinite(blk).mean(axis=(1, 3))
        elev = np.where(frac >= 0.2, np.nanmean(blk, axis=(1, 3)), np.nan)
        slope = np.where(frac >= 0.2, np.nanmean(s5.reshape(rows, 5, cols, 5), axis=(1, 3)), np.nan)
    return elev.astype(np.float32), slope.astype(np.float32)


def check_tile(code, g, tile, reg=(0.0, 0.0)) -> dict:
    """Two 1-cell-high strips across the tile vs. the stored cells (Austrian
    cells only, stored grid taken at its registered position)."""
    import austria_mask
    _, r0, c0, r1, c1 = tile
    a = g['elev']
    ins = austria_mask.grid_mask(g['x0'], g['y0'], a.shape[0], a.shape[1], CELL, -CELL)
    diffs = []
    cand = sorted(range(r0 + 1, r1 - 1), key=lambda r: -int((ins[r, c0:c1] & np.isfinite(a[r, c0:c1]) & (a[r, c0:c1] != 0)).sum()))
    picked = []
    for r in cand:
        if all(abs(r - p) >= max(4, (r1 - r0) // 4) for p in picked):
            picked.append(r)
        if len(picked) == 2:
            break
    for r in picked:
        k = ins[r, c0:c1] & np.isfinite(a[r, c0:c1]) & (a[r, c0:c1] != 0)
        if k.sum() < 2:
            continue
        e, _s = _bev_block(g['x0'] + c0 * CELL + reg[0], g['y0'] - r * CELL + reg[1], 1, c1 - c0)
        d = (a[r, c0:c1] - e[0])[k]
        diffs.append(d[np.isfinite(d)])
    d = np.concatenate(diffs) if diffs else np.zeros(0)
    if d.size < MIN_CHECK_CELLS:
        return {'v': 'unchecked', 'n': int(d.size)}
    med = float(np.median(np.abs(d)))
    return {'v': 'bad' if med > BAD_M else 'ok', 'n': int(d.size), 'med': round(med, 2), 'p90': round(float(np.percentile(np.abs(d), 90)), 2)}


def suspects(code, g) -> list:
    import austria_mask
    a = g['elev']
    far = austria_mask.grid_mask(g['x0'], g['y0'], a.shape[0], a.shape[1], CELL, FAR_M)
    if far is None or far.all():
        return []
    out_far = np.isfinite(a) & (a != 0) & ~far
    return [t for t in _tiles(code, g) if int(out_far[t[1]:t[3], t[2]:t[4]].sum()) >= MIN_FAR_CELLS]


EDGE_RATIO = 3.0; EDGE_FLOOR_M = 1.0; EDGE_RUN = 16      # cells (400 m)


def _longest_run(b, gap=1):
    best = cur = miss = 0
    for v in b:
        if v:
            cur += 1 + (miss if cur else 0); miss = 0; best = max(best, cur)
        elif cur and miss < gap:
            miss += 1
        else:
            cur = miss = 0
    return best


def edges(code, g, tiles=None) -> list:
    """Internal processing-tile boundaries of one product that look like a
    cliff: the 3-cell step across the boundary exceeds ``EDGE_RATIO`` × the
    median of the same step 3 and 6 cells further in on both sides (floor
    ``EDGE_FLOOR_M``) over a run of ≥ ``EDGE_RUN`` cells.  This is the seam
    rule in the DTM's own 25 m domain — a stale tile is a smooth ramp after
    interpolation, invisible to a pixel-pair rule on the finished tiles."""
    a = g['elev']
    a = np.where(a == 0, np.nan, a)
    rows, cols = a.shape
    tiles = tiles if tiles is not None else _tiles(code, g)
    seen = set(); out = []
    for ti, r0, c0, r1, c1 in tiles:
        for axis, k, lo, hi in ((1, c0, r0, r1), (1, c1, r0, r1), (0, r0, c0, c1), (0, r1, c0, c1)):
            n = cols if axis == 1 else rows
            if k < 8 or k > n - 9 or (axis, k, lo) in seen:
                continue
            seen.add((axis, k, lo))
            A = a[lo:hi, :] if axis == 1 else a[:, lo:hi].T

            def st(j):
                return np.abs(A[:, j + 1] - A[:, j - 2])
            e = st(k)
            ref = np.stack([st(k - 6), st(k - 3), st(k + 3), st(k + 6)])
            ok = np.isfinite(e) & np.isfinite(ref).all(axis=0)
            fire = ok & (e > EDGE_RATIO * np.maximum(np.median(np.where(np.isfinite(ref), ref, 0), axis=0), EDGE_FLOOR_M))
            run = _longest_run(fire)
            if run >= EDGE_RUN:
                out.append({'tile': ti, 'axis': 'col' if axis == 1 else 'row', 'k': int(k), 'lo': int(lo), 'hi': int(hi),
                            'run': int(run), 'step': round(float(np.median(e[fire])), 1)})
    return out


def edge_tiles(code, g, ed, tiles=None) -> list:
    """Tiles on either side of the flagged edges."""
    tiles = tiles if tiles is not None else _tiles(code, g)
    out = []
    for t in tiles:
        _, r0, c0, r1, c1 = t
        for e in ed:
            if e['axis'] == 'col':
                hit = (abs(c0 - e['k']) <= 6 or abs(c1 - e['k']) <= 6) and r0 < e['hi'] and r1 > e['lo']
            else:
                hit = (abs(r0 - e['k']) <= 6 or abs(r1 - e['k']) <= 6) and c0 < e['hi'] and c1 > e['lo']
            if hit and t not in out:
                out.append(t)
    return out


def edge_scan(quiet=False) -> dict:
    """All products, local data only (~10 min): flag cliff-like internal tile
    edges → ``index.json → edges`` (the BEV check + patch happens in scan())."""
    import grid25_sample as g25
    import kg_v2_store as kvs
    doc = dict(_index()) or {'version': 0, 'checked': {}, 'patched': {}}
    found = {}; n = 0; t0 = time.time()
    for code in sorted(kvs.all_codes()):
        try:
            g = _raw_grid(code)
            if g is None:
                continue
            n += 1
            apply(code, g)
            g25._drop_zero_fill(g)
            ed = edges(code, g)
            if ed:
                found[code] = ed
                if not quiet:
                    print(code, [(e['tile'], e['axis'], e['run'], e['step']) for e in ed], flush=True)
        except Exception as e:  # noqa: BLE001
            print(code, 'ERR', repr(e)[:160], file=sys.stderr)
    doc = dict(_index()) or doc              # a scan may have written meanwhile
    doc['edges'] = found
    doc['edge_scan'] = {'products': n, 'flagged_products': len(found), 'flagged_edges': sum(map(len, found.values())),
                        'secs': round(time.time() - t0), 'ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                        'rule': f'3-cell step across an internal tile edge > {EDGE_RATIO}x median of the 4 neighbour steps '
                                f'(floor {EDGE_FLOOR_M} m), run >= {EDGE_RUN} cells'}
    _save(doc)
    return doc['edge_scan']


def _save(doc):
    DIR.mkdir(parents=True, exist_ok=True)
    doc['updated_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    tmp = INDEX.with_suffix('.tmp'); tmp.write_text(json.dumps(doc, separators=(',', ':'))); os.replace(tmp, INDEX)


def process(code, doc, tiles=None, force=False) -> dict:
    """Check the suspect (or given) tiles of one product; patch the bad ones."""
    import grid25_sample as g25
    g = _raw_grid(code)
    if g is None:
        return {'tiles': []}
    g25._drop_zero_fill(g)
    reg = tuple(g25.registration()['map'].get(str(code)) or (0.0, 0.0))
    res = []; blocks = []; arrs = {}
    if tiles is None:
        tiles = suspects(code, g)
        ed = (doc.get('edges') or {}).get(code)
        if ed:
            tiles = tiles + [t for t in edge_tiles(code, g, ed) if t not in tiles]
    todo = list(tiles)
    verd = [{'v': 'bad', 'forced': True} if force else check_tile(code, g, t, reg) for t in todo]
    if any(v['v'] == 'bad' for v in verd):
        # a suspect tile with too few Austrian cells to judge, in a product with proven stale
        # tiles: rebuild it too (its cells only feed the interpolation along the border)
        verd = [dict(v, v='bad', by='sibling') if v['v'] == 'unchecked' else v for v in verd]
    for t, v in zip(todo, verd):
        v['tile'] = t[0]; v['rc'] = list(t[1:])
        if v['v'] == 'bad':
            _, r0, c0, r1, c1 = t
            e, s = _bev_block(g['x0'] + c0 * CELL, g['y0'] - r0 * CELL, r1 - r0, c1 - c0)
            i = len(blocks)
            arrs[f'e{i}'] = np.where(np.isfinite(e), np.round(e * 10), -32768).astype(np.int16)
            arrs[f's{i}'] = np.where(np.isfinite(s), np.clip(np.round(s), 0, 254), 255).astype(np.uint8)
            blocks.append([r0, c0, r1, c1])
        res.append(v)
    ent = {'tiles': res, 'ts': int(time.time()), 'edges_done': bool((doc.get('edges') or {}).get(code))}
    doc.setdefault('checked', {})[code] = ent
    if blocks:
        DIR.mkdir(parents=True, exist_ok=True)
        old = (doc.get('patched') or {}).get(code)
        if old and (DIR / f'{code}.npz').exists():       # keep earlier blocks of the same product
            z = np.load(DIR / f'{code}.npz')
            keep = [(b, z[f'e{j}'], z[f's{j}']) for j, b in enumerate(old['blocks']) if b not in blocks]
            for b, e_, s_ in keep:
                i = len(blocks); arrs[f'e{i}'] = e_; arrs[f's{i}'] = s_; blocks.append(b)
        tmp = DIR / f'.{code}.tmp.npz'
        np.savez_compressed(tmp, **arrs); os.replace(tmp, DIR / f'{code}.npz')
        doc.setdefault('patched', {})[code] = {'grid': [float(g['x0']), float(g['y0']), g['elev'].shape[0], g['elev'].shape[1]],
                                               'blocks': blocks, 'ts': int(time.time())}
        doc['dirty'] = True
    return ent


def candidates() -> list:
    """Products with ≥ MIN_FAR_CELLS cells farther than FAR_M outside Austria."""
    import austria_mask
    import kg_v2_store as kvs
    out = []
    for code in sorted(kvs.all_codes()):
        try:
            bb = kvs.get_bbox(code)
            if bb and not austria_mask.near_border(bb[0] - 0.01, bb[1] - 0.01, bb[2] + 0.01, bb[3] + 0.01):
                continue
            g = _raw_grid(code)
            if g is None:
                continue
            a = g['elev']
            far = austria_mask.grid_mask(g['x0'], g['y0'], a.shape[0], a.shape[1], CELL, FAR_M)
            if int((np.isfinite(a) & (a != 0) & ~far).sum()) >= MIN_FAR_CELLS:
                out.append(code)
        except Exception:  # noqa: BLE001
            continue
    return out


def scan(budget_mb_h=150.0, codes=None, max_n=None, quiet=False) -> dict:
    import psutil
    doc = dict(_index()) or {'version': 0, 'checked': {}, 'patched': {}}
    if codes is None:
        codes = doc.get('candidates')
        if not codes:
            codes = candidates(); doc['candidates'] = codes; _save(doc)
        codes = sorted(set(codes) | set(doc.get('edges') or {}))
    todo = [c for c in codes if c not in (doc.get('checked') or {})
            or ((doc.get('edges') or {}).get(c) and not doc['checked'][c].get('edges_done'))]
    if max_n:
        todo = todo[:max_n]
    t0 = time.time(); n0 = psutil.net_io_counters().bytes_recv
    for code in todo:
        b0 = psutil.net_io_counters().bytes_recv
        try:
            ent = process(code, doc)
        except Exception as e:  # noqa: BLE001
            print(code, 'ERR', repr(e)[:200], file=sys.stderr, flush=True)
            continue
        doc['mb'] = float(doc.get('mb') or 0) + (psutil.net_io_counters().bytes_recv - b0) / 1e6
        _save(doc)
        if not quiet:
            print(code, [(t['tile'], t['v'], t.get('med')) for t in ent['tiles']], flush=True)
        used = (psutil.net_io_counters().bytes_recv - n0) / 1e6
        wait = used / budget_mb_h * 3600 - (time.time() - t0)
        if wait > 0:
            time.sleep(min(wait, 600))
    doc = dict(_index()) or doc
    if doc.pop('dirty', None):          # publish: ONE tile-set version bump per scan that changed something
        doc['version'] = int(doc.get('version') or 0) + 1
        _save(doc)
    return status()


if __name__ == '__main__':
    def _arg(n, d, t):
        return t(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d
    if '--scan' in sys.argv:
        print(json.dumps(scan(_arg('--budget-mb-h', 150.0, float), _arg('--codes', None, lambda s: s.split(',')), _arg('--max', None, int))))
    elif '--edge-scan' in sys.argv:
        print(json.dumps(edge_scan()))
    elif '--tile' in sys.argv:
        i = sys.argv.index('--tile'); code = sys.argv[i + 1]; E = float(sys.argv[i + 2]); N = float(sys.argv[i + 3])
        doc = dict(_index()) or {'version': 0, 'checked': {}, 'patched': {}}
        g = _raw_grid(code)
        c = int((E - g['x0']) / CELL); r = int((g['y0'] - N) / CELL)
        ts = [t for t in _tiles(code, g) if t[1] <= r < t[3] and t[2] <= c < t[4]]
        print(json.dumps(process(code, doc, tiles=ts, force='--force' in sys.argv)))
        if doc.pop('dirty', None):
            doc['version'] = int(doc.get('version') or 0) + 1
        _save(doc)
    else:
        print(json.dumps(status()))
