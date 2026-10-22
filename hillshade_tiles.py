"""hillshade_tiles — XYZ hillshade / slope PNG tiles from the product-2.1
``terrain.grid25`` (25 m DTM) already held in ``data/kg_v2_store.db``
(siedler LID-4).

Rendered on the fly (numpy, 20-300 ms/tile depending on how many KGs the
tile spans), no BEV / Zenodo access.  Three layers keep this cheap and —
more importantly — keep a tile flood from taking the public host down
(Oct 2026 outage, see ``mem_lru`` / ``service_guard``):

* grid25 blocks: byte-bounded LRU (``HILLSHADE_GRID_MB``, default 96 MB)
* rendered PNGs: in-memory LRU (``TILE_PNG_MB``, 32 MB) + on-disk LRU under
  ``data/tile_cache/`` (``TILE_DISK_CACHE_MB``, 2 GB, janitor-trimmed).
  Keys carry a stamp of the intersecting KGs' latest ingest, so a product
  upgrade invalidates exactly the tiles it touches.  Strong ETag → 304.
* rendering goes through a concurrency ``Gate`` (``TILE_MAX_CONCURRENT``,
  3 per worker, ≤3 s queue wait → 503 ``Retry-After: 2``) and a per-client
  token bucket on cache *misses* (``TILE_RATE_PER_S`` 25/s, burst 150 →
  429 ``Retry-After``).  Cache hits are never limited.

``Cache-Control`` is 1 y immutable so browsers + the exe.dev proxy hold them. Resolution is the 25 m grid (header
``X-Resolution-M: 25``) — at z17 it is smooth, but every pixel is real relief.
204 when no indexed KG with grid25 intersects the tile (or all nodata).

nDSM tiles are NOT available: grid25 carries DTM + slope + canopy fraction
only; a 1 m nDSM raster per KG lives in the full GPKG (hundreds of MB) and is
out of the primary's disk/bandwidth budget → ``/tiles/ndsm/…`` answers 501.
"""
from __future__ import annotations

import hashlib
import io
import logging
import math
import os
import threading
import time
from pathlib import Path

import numpy as np
from flask import Blueprint, Response, jsonify, request

import mem_lru
import service_guard as sg

log = logging.getLogger(__name__)
bp = Blueprint('hillshade_tiles', __name__)

TILE = 256
ZMIN, ZMAX = 10, 17
# decoded grid25 blocks are ~320 KB each (elev+slope float32 on a ~200×200 grid);
# split KGs hold several per parent → budget bytes, not entries.
_grid_cache = mem_lru.ByteLRU('hillshade_grids', 96, env='HILLSHADE_GRID_MB')
_png_cache = mem_lru.ByteLRU('tile_png', 32, sizer=lambda v: len(v[0]) + 64, env='TILE_PNG_MB')
_render_gate = sg.Gate('tile_render', 3, 3.0, env='TILE_MAX_CONCURRENT')
_render_limit = sg.RateLimiter('tile_render', 25.0, 150.0, env='TILE_RATE_PER_S')
_tf = None
_inflight: dict = {}            # tile key → Event: single-flight so N clients asking the same tile render once
_inflight_lock = threading.Lock()

# --- disk cache ------------------------------------------------------------
DISK_DIR = Path(os.environ.get('TILE_DISK_CACHE_DIR', str(Path(__file__).resolve().parent / 'data' / 'tile_cache')))
DISK_MAX_BYTES = int(float(os.environ.get('TILE_DISK_CACHE_MB', 2048)) * 2 ** 20)
_disk_stats = {'hits': 0, 'misses': 0, 'writes': 0, 'bytes': 0, 'files': 0, 'trimmed': 0, 'last_trim': 0.0}
_disk_lock = threading.Lock()
_disk_ok = True


def _disk_path(key: str) -> Path:
    # key = kind/z/x/y/stamp → kind/z/x/y.stamp.png (one file per tile; stale stamps are
    # swept by the janitor, never read: the lookup is by exact name)
    kind, z, x, y, stamp = key.split('/')
    return DISK_DIR / kind / z / x / f'{y}.{stamp}.png'


def _disk_get(key: str):
    if not _disk_ok:
        return None
    p = _disk_path(key)
    try:
        b = p.read_bytes()
    except FileNotFoundError:
        _disk_stats['misses'] += 1
        return None
    except Exception as e:  # noqa: BLE001
        log.debug('tile disk read %s: %s', p, e)
        return None
    _disk_stats['hits'] += 1
    try:
        os.utime(p)          # LRU by mtime (atime is noatime on most mounts)
    except Exception:  # noqa: BLE001
        pass
    return b


def _disk_put(key: str, png: bytes) -> None:
    global _disk_ok
    if not _disk_ok:
        return
    p = _disk_path(key)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(f'.{os.getpid()}.{threading.get_ident()}.tmp')
        tmp.write_bytes(png)
        os.replace(tmp, p)
        _disk_stats['writes'] += 1
        _disk_stats['bytes'] += len(png) + 4096   # fs block granularity estimate
        _disk_stats['files'] += 1
    except OSError as e:
        log.warning('tile disk cache disabled: %s', e)
        _disk_ok = False
    if _disk_stats['bytes'] > DISK_MAX_BYTES * 1.05 and time.monotonic() - _disk_stats['last_trim'] > 60:
        threading.Thread(target=_disk_trim, name='tile-disk-trim', daemon=True).start()


def _disk_scan() -> list:
    out = []
    for root, _dirs, files in os.walk(DISK_DIR):
        for f in files:
            if not f.endswith('.png'):
                continue
            p = os.path.join(root, f)
            try:
                st = os.stat(p)
            except OSError:
                continue
            out.append((st.st_mtime, st.st_size + 4096, p))
    return out


def _disk_trim(target_frac: float = 0.85) -> int:
    """Delete oldest-touched tiles until the cache is ≤ target_frac × budget.
    Runs in a daemon thread; single-flight via _disk_lock."""
    if not _disk_lock.acquire(blocking=False):
        return 0
    try:
        _disk_stats['last_trim'] = time.monotonic()
        files = _disk_scan()
        total = sum(f[1] for f in files)
        _disk_stats['bytes'] = total
        _disk_stats['files'] = len(files)
        if total <= DISK_MAX_BYTES:
            return 0
        files.sort()
        n = 0
        for mtime, sz, p in files:
            if total <= DISK_MAX_BYTES * target_frac:
                break
            try:
                os.remove(p)
                total -= sz; n += 1
            except OSError:
                pass
        _disk_stats['bytes'] = total
        _disk_stats['files'] = len(files) - n
        _disk_stats['trimmed'] += n
        log.info('tile disk cache trimmed %d files → %.0f MB', n, total / 2 ** 20)
        return n
    finally:
        _disk_lock.release()


def _disk_init() -> None:
    """Size the disk cache once at import (background) so stats are honest."""
    def run():
        try:
            DISK_DIR.mkdir(parents=True, exist_ok=True)
            _disk_trim()
        except Exception as e:  # noqa: BLE001
            log.warning('tile disk cache init: %s', e)
    threading.Thread(target=run, name='tile-disk-init', daemon=True).start()


_disk_init()


def stats() -> dict:
    return {'grids': _grid_cache.stats(), 'png_mem': _png_cache.stats(),
            'png_disk': dict(_disk_stats, mb=round(_disk_stats['bytes'] / 2 ** 20, 1),
                             max_mb=DISK_MAX_BYTES // 2 ** 20, enabled=_disk_ok),
            'gate': _render_gate.stats(), 'rate_limit': _render_limit.stats()}


def _to3035():
    global _tf
    if _tf is None:
        from pyproj import Transformer
        _tf = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)
    return _tf


def _grids(kg_code):
    g = _grid_cache.get(kg_code)
    if g is not None:
        return g
    import kg_v2_store as kvs
    import v21_products as v21
    out = []
    for code in kvs.codes_for_parent(kg_code) or []:
        try:
            sec = kvs.get_grid25(code)
            if sec and sec.get('terrain'):
                out.append(v21.decode_terrain_grid25(sec['terrain']))
        except Exception as e:  # noqa: BLE001
            log.debug('grid25 %s: %s', code, e)
    _grid_cache.put(kg_code, out)
    return out


RENDER_VERSION = 3       # bump when render() output changes (v3: interior-cells-win grid25 merge)


def _stamp(kgs) -> str:
    """8-hex stamp of (renderer version, parent, latest ingest) over the KGs a
    tile spans → the cache key changes exactly when one of those products is
    re-ingested or the renderer changes."""
    import kg_v2_store as kvs
    try:
        rows = kvs.parents_stamp(kgs)
    except Exception as e:  # noqa: BLE001
        log.debug('stamp: %s', e)
        rows = sorted(kgs)
    return hashlib.blake2b(repr((RENDER_VERSION, rows)).encode(), digest_size=4).hexdigest()


def tile_bounds(z, x, y):
    n = 2 ** z
    lon0 = x / n * 360.0 - 180.0
    lon1 = (x + 1) / n * 360.0 - 180.0
    lat0 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    return lon0, lat0, lon1, lat1


def _sample(grid, E, N):
    """Kept for callers/tests: NaN-aware bilinear (see grid25_sample)."""
    from grid25_sample import sample
    return sample(grid, E, N)[0]


def render(z, x, y, kind='hillshade', kgs=None):
    """→ PNG bytes or None (no data).  Pure compute — no caches, no gate."""
    lon0, lat0, lon1, lat1 = tile_bounds(z, x, y)
    if kgs is None:
        import search_index as si
        kgs = si.get_index().kgs_in_bbox(lon0, lat0, lon1, lat1)
    if not kgs:
        return None
    grids = [g for kg in kgs for g in _grids(kg)]
    if not grids:
        return None
    # pixel centres with a 1-px apron for gradients
    n = TILE + 2
    lons = lon0 + (np.arange(n) - 0.5) / TILE * (lon1 - lon0)
    # mercator-linear in y
    my0 = math.log(math.tan(math.pi / 4 + math.radians(lat1) / 2))
    my1 = math.log(math.tan(math.pi / 4 + math.radians(lat0) / 2))
    mys = my0 + (np.arange(n) - 0.5) / TILE * (my1 - my0)
    lats = np.degrees(2 * np.arctan(np.exp(mys)) - np.pi / 2)
    LON, LAT = np.meshgrid(lons, lats)
    E, N = _to3035().transform(LON, LAT)
    E = np.asarray(E, np.float64); N = np.asarray(N, np.float64)
    from grid25_sample import merge
    elev = merge(grids, E, N)      # clean bilinear beats block-edge samples → no seams
    core = elev[1:-1, 1:-1]
    if not np.isfinite(core).any():
        return None
    # metres per pixel (at tile centre)
    lat_c = math.radians((lat0 + lat1) / 2)
    mpp = 156543.03392 * math.cos(lat_c) / (2 ** z)
    filled = np.where(np.isfinite(elev), elev, np.nanmean(core))
    dzdy, dzdx = np.gradient(filled, mpp)
    if kind == 'slope':
        slope = np.degrees(np.arctan(np.hypot(dzdx, dzdy)))[1:-1, 1:-1]
        val = np.clip(slope / 60.0, 0, 1)
        rgb = np.stack([255 - 200 * val, 255 - 120 * val, 255 - 60 * val], -1)
    else:
        az = math.radians(315.0); alt = math.radians(45.0)
        slope = np.arctan(np.hypot(dzdx, dzdy))
        aspect = np.arctan2(-dzdx, dzdy)
        hs = (math.sin(alt) * np.cos(slope)
              + math.cos(alt) * np.sin(slope) * np.cos(az - aspect))
        hs = np.clip(hs, 0, 1)[1:-1, 1:-1]
        g8 = (hs * 255)
        rgb = np.stack([g8, g8, g8], -1)
    alpha = np.where(np.isfinite(core), 255, 0).astype(np.uint8)
    img = np.dstack([rgb.astype(np.uint8), alpha])
    from PIL import Image
    buf = io.BytesIO()
    Image.fromarray(img, 'RGBA').save(buf, format='PNG', optimize=False, compress_level=6)
    return buf.getvalue()


_EMPTY = b''


def _lookup(key: str):
    """→ png bytes (b'' = cached no-data) or None on miss."""
    hit = _png_cache.get(key)
    if hit is not None:
        return hit[0]
    b = _disk_get(key)
    if b is not None:
        _png_cache.put(key, (b,))
    return b


def _render_cached(z, x, y, kind, kgs, key) -> bytes:
    """Single-flight render: concurrent requests for one tile wait for the
    first renderer instead of each burning a gate slot."""
    while True:
        with _inflight_lock:
            ev = _inflight.get(key)
            if ev is None:
                ev = _inflight[key] = threading.Event()
                mine = True
            else:
                mine = False
        if not mine:
            ev.wait(timeout=_render_gate.max_wait_s + 5)
            b = _lookup(key)
            if b is not None:
                return b
            continue        # the renderer failed; try to become the renderer
        try:
            with _render_gate:
                png = render(z, x, y, kind, kgs=kgs) or _EMPTY
            _png_cache.put(key, (png,))
            _disk_put(key, png)
            return png
        finally:
            with _inflight_lock:
                _inflight.pop(key, None)
            ev.set()


def _tile_resp(z, x, y, kind):
    if not (ZMIN <= z <= ZMAX):
        return jsonify({'error': f'z must be {ZMIN}..{ZMAX}'}), 400
    if not (0 <= x < 2 ** z and 0 <= y < 2 ** z):
        return jsonify({'error': 'tile out of range'}), 400
    sg.WATCHDOG.check()
    t0 = time.monotonic()
    try:
        import search_index as si
        kgs = si.get_index().kgs_in_bbox(*tile_bounds(z, x, y))
        key = f'{kind}/{z}/{x}/{y}/{_stamp(kgs) if kgs else "0"}'
        png = _lookup(key) if kgs else _EMPTY
        src = 'hit'
        if png is None:
            ok, left, retry = _render_limit.take(sg.client_key(request))
            if not ok:
                r = jsonify({'error': 'rate_limited',
                             'detail': f'tile renders limited to {_render_limit.rate:g}/s per client '
                                       f'(burst {_render_limit.burst:g}); cached tiles are not counted. '
                                       'Lower concurrency, honour Retry-After, cache tiles locally (immutable, 1 y).'})
                r.status_code = 429
                r.headers['Retry-After'] = str(int(math.ceil(retry)))
                r.headers['X-RateLimit-Limit'] = f'{_render_limit.rate:g}'
                r.headers['X-RateLimit-Remaining'] = '0'
                r.headers['Cache-Control'] = 'no-store'
                return r
            png = _render_cached(z, x, y, kind, kgs, key)
            src = 'render'
    except sg.Overloaded as e:
        r = jsonify({'error': 'overloaded', 'detail': 'tile renderer saturated; retry after the indicated delay'})
        r.status_code = 503
        r.headers['Retry-After'] = str(e.retry_after)
        r.headers['Cache-Control'] = 'no-store'
        return r
    except Exception as e:  # noqa: BLE001
        log.exception('tile %s/%d/%d/%d', kind, z, x, y)
        return jsonify({'error': str(e)}), 500
    etag = '"' + hashlib.blake2b(png, digest_size=8).hexdigest() + '"'
    common = {'Cache-Control': 'public, max-age=31536000, immutable', 'X-Resolution-M': '25',
              'Access-Control-Allow-Origin': '*', 'ETag': etag, 'X-Tile-Source': src,
              'X-Tile-Ms': f'{(time.monotonic() - t0) * 1000:.0f}'}
    if request.headers.get('If-None-Match') == etag:
        return Response(status=304, headers=common)
    if not png:
        r = Response(status=204, mimetype='image/png')
    else:
        r = Response(png, mimetype='image/png')
    for k, v in common.items():
        r.headers[k] = v
    return r


@bp.route('/tiles/hillshade/<int:z>/<int:x>/<int:y>.png')
def hillshade_tile(z, x, y):
    return _tile_resp(z, x, y, 'hillshade')


@bp.route('/tiles/slope/<int:z>/<int:x>/<int:y>.png')
def slope_tile(z, x, y):
    return _tile_resp(z, x, y, 'slope')


@bp.route('/tiles/ndsm/<int:z>/<int:x>/<int:y>.png')
def ndsm_tile(z, x, y):
    return jsonify({'error': 'ndsm tiles not available',
                    'detail': 'only the 25 m DTM grid is held on the primary; 1 m nDSM '
                              'lives in the per-KG full GPKG on Zenodo (POST /api/v1/lidar/overlay '
                              'or /api/v1/kg/<code>/heightfield for on-demand rasters)'}), 501
