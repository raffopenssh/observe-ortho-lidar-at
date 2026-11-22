"""austria_mask — the national boundary, the SAME one the NE server clips with.

Source: the NE server ``GET /api/v1/ne/glyph/admin/outline?c=AT`` (OSM relation
16239, simplified to ~5 m, © OpenStreetMap contributors, ODbL), converted
from glyph viewBox units to lon/lat with ``proj`` of ``/api/v1/ne/glyph``:

    lon = ((x - pad) / scale + minx) / kx        lat = -((y - pad) / scale + miny)

Kept in git as ``data/austria_outline.json`` (rings, 6 decimals, + sha) so no
read path ever depends on the network.  ``python3 austria_mask.py --fetch``
refreshes it (changes the sha → tile-set version, see hillshade_tiles).

    inside(E, N)            bool array, EPSG:3035 points strictly inside Austria
    grid_mask(x0, y0, rows, cols, cell, buffer_m)   mask of a north-up raster
    near_border(w, s, e, n) does a lon/lat box touch the boundary at all?

Docs: docs/hillshade-seams.md → National mask.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import threading
import time
from pathlib import Path

import numpy as np

PATH = Path(os.environ.get('AUSTRIA_OUTLINE', str(Path(__file__).resolve().parent / 'data' / 'austria_outline.json')))
NOTICE = 'Staatsgrenze © OpenStreetMap contributors (ODbL), via the NE server'
_LOCK = threading.Lock()
_ST: dict = {}


def _load() -> dict:
    """{'sha', 'poly' (EPSG:3035 shapely), 'prep', 'line', 'poly_ll'} — {} when the file is missing."""
    try:
        mt = PATH.stat().st_mtime
    except OSError:
        return {}
    if _ST.get('mtime') == mt:
        return _ST
    with _LOCK:
        if _ST.get('mtime') == mt:
            return _ST
        from pyproj import Transformer
        from shapely.geometry import Polygon
        from shapely.ops import unary_union
        from shapely.prepared import prep
        d = json.loads(PATH.read_text())
        tf = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)
        acc = acc_ll = None
        for ring in sorted(d['rings'], key=len, reverse=True):      # evenodd: xor ring by ring
            a = np.asarray(ring, np.float64)
            if len(a) < 4:
                continue
            ll = Polygon(a).buffer(0)
            x, y = tf.transform(a[:, 0], a[:, 1])
            p = Polygon(np.column_stack([x, y])).buffer(0)
            acc = p if acc is None else acc.symmetric_difference(p)
            acc_ll = ll if acc_ll is None else acc_ll.symmetric_difference(ll)
        acc = unary_union([acc])
        _ST.clear()
        _ST.update(mtime=mt, sha=d.get('sha', ''), poly=acc, prep=prep(acc), line=acc.boundary,
                   poly_ll=acc_ll, line_ll=acc_ll.boundary, buf={})
    return _ST


def available() -> bool:
    return bool(_load())


def version() -> str:
    """Short id of the outline in use ('' = no mask)."""
    return (_load().get('sha') or '')[:8]


def _buffered(st, buffer_m: float):
    b = st['buf'].get(buffer_m)
    if b is None:
        b = st['poly'].buffer(buffer_m, resolution=4) if buffer_m else st['poly']
        st['buf'][buffer_m] = b
    return b


def grid_mask(x0: float, y0: float, rows: int, cols: int, cell: float, buffer_m: float = 0.0):
    """True where the CELL CENTRE of a north-up EPSG:3035 raster (upper-left
    corner x0, y0) lies inside Austria grown by *buffer_m*.  None = no mask
    file (callers then leave the data alone)."""
    st = _load()
    if not st:
        return None
    from shapely.geometry import box
    bb = box(x0, y0 - rows * cell, x0 + cols * cell, y0)
    poly = _buffered(st, buffer_m)
    if not st['line'].intersects(bb.buffer(buffer_m + cell)):
        return np.full((rows, cols), bool(st['prep'].contains(bb.centroid)))
    part = poly.intersection(bb.buffer(cell))
    if part.is_empty:
        return np.zeros((rows, cols), bool)
    from rasterio.features import geometry_mask
    from rasterio.transform import from_origin
    return geometry_mask([part], out_shape=(rows, cols), transform=from_origin(x0, y0, cell, cell), invert=True)


def near_border(w: float, s: float, e: float, n: float) -> bool:
    st = _load()
    if not st:
        return False
    from shapely.geometry import box
    return st['line_ll'].intersects(box(w, s, e, n))


def inside(E, N):
    """Vectorised point test (EPSG:3035).  None = no mask file."""
    st = _load()
    if not st:
        return None
    import shapely
    E = np.asarray(E, np.float64); N = np.asarray(N, np.float64)
    from shapely.geometry import box
    bb = box(float(E.min()), float(N.min()), float(E.max()), float(N.max()))
    if not st['line'].intersects(bb):
        return np.full(E.shape, bool(st['prep'].contains(bb.centroid)))
    part = st['poly'].intersection(bb.buffer(1.0))
    if part.is_empty:
        return np.zeros(E.shape, bool)
    shapely.prepare(part)
    return shapely.contains_xy(part, E, N)


_ENSURE = {'t': 0.0}


def ensure(retry_s: float = 3600.0) -> bool:
    """Outline on disk?  The file is not in git: a peer gets it with one small
    fetch (2 GETs, once per VM; a failure is retried after *retry_s* at the
    earliest).  Used by the processor's read-time clip — without the file the
    clip is a silent no-op.  Never raises."""
    if _load():
        return True
    with _LOCK:
        if time.time() - _ENSURE['t'] < retry_s:
            return False
        _ENSURE['t'] = time.time()
    try:
        fetch()
    except Exception as e:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).warning('austria_mask: outline fetch failed: %s', str(e)[:160])
        return False
    return bool(_load())


# ── refresh ────────────────────────────────────────────────────────────

def fetch() -> dict:
    import requests
    import ne_h3
    base = ne_h3._ne_server_base()
    proj = requests.get(base + '/glyph', timeout=60).json()['proj']
    o = requests.get(base + '/glyph/admin/outline?c=AT', timeout=60).json()
    rings = []
    for sub in re.findall(r'M([^MZ]+)Z', o['d']):
        a = np.array([[float(v) for v in p.split()] for p in sub.split('L')], np.float64)
        lon = ((a[:, 0] - proj['pad']) / proj['scale'] + proj['minx']) / proj['kx']
        lat = -((a[:, 1] - proj['pad']) / proj['scale'] + proj['miny'])
        rings.append([[round(float(x), 6), round(float(y), 6)] for x, y in zip(lon, lat)])
    body = json.dumps(rings, separators=(',', ':'))
    doc = {'source': 'the NE server /api/v1/ne/glyph/admin/outline?c=AT (OSM relation 16239, ~5 m)',
           'notice': o.get('notice') or NOTICE, 'licence': 'ODbL', 'proj': proj,
           'sha': hashlib.sha256(body.encode()).hexdigest()[:16], 'rings': rings}
    old = {}
    try:
        old = json.loads(PATH.read_text())
    except Exception:  # noqa: BLE001
        pass
    if old.get('sha') != doc['sha']:
        tmp = PATH.with_suffix('.tmp'); tmp.write_text(json.dumps(doc, separators=(',', ':'))); os.replace(tmp, PATH)
    return {'sha': doc['sha'], 'changed': old.get('sha') != doc['sha'], 'rings': len(rings), 'points': sum(map(len, rings))}


if __name__ == '__main__':
    if '--fetch' in sys.argv:
        print(json.dumps(fetch()))
    st = _load()
    print(json.dumps({'present': bool(st), 'sha': st.get('sha'), 'area_km2': round(st['poly'].area / 1e6, 1) if st else None,
                      'bounds_ll': [round(v, 4) for v in st['poly_ll'].bounds] if st else None}))
