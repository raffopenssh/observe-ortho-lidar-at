"""Seam-free sampling of decoded grid25 terrain grids (EPSG:3035, 25 m).

Shared by ``hillshade_tiles`` (relief / slope PNG tiles), the public
``/api/v1/heightfield`` (``landscape_query``) and ``/kg/<code>/heightfield``
(``app``).

Why this exists: each grid25 is clipped to its KG / block polygon, so the
cells along its outline are partly nodata.  A plain bilinear sample that
hits a NaN corner used to fall back to *nearest-neighbour*, which paints
blocky 25 m steps along every block edge; and because grids were merged in
"first grid wins" order, that blocky fallback could beat the neighbouring
grid's perfectly good bilinear value.  That showed up as dashed seams in the
hillshade a few rows inside a tile (not at the tile edge).

Fix: NaN-aware bilinear (weights renormalised over the finite corners) and
a two-pass merge — every grid's *clean* samples (all four corners finite)
first, partial / edge samples only where nothing clean exists.
"""
from __future__ import annotations

import math

import numpy as np


def _interior(grid, a):
    """Cells that are finite and whose 4-neighbours are all finite and inside
    the array.  The outer ring of a polygon-clipped grid is a *partial* block
    mean (only the DTM pixels inside the KG/block outline) and can be metres
    off the neighbouring product's interior value — never let it win."""
    m = grid.get('_interior')
    if m is None or m.shape != a.shape:
        f = np.isfinite(a)
        m = f.copy()
        m[0, :] = m[-1, :] = m[:, 0] = m[:, -1] = False
        m[1:-1, 1:-1] &= f[:-2, 1:-1] & f[2:, 1:-1] & f[1:-1, :-2] & f[1:-1, 2:]
        grid['_interior'] = m
    return m


def sample(grid, E, N):
    """→ ``(value, clean)``.  ``value`` is NaN-aware bilinear on cell centres
    (NaN outside the grid or where all four corners are nodata), ``clean``
    marks samples whose four corners were all *interior* cells."""
    a = grid['elev'] if 'elev' in grid else grid['cls']
    cell = grid['cell_m']
    rows, cols = a.shape
    interior = _interior(grid, a)
    fx = (E - grid['x0']) / cell - 0.5
    fy = (grid['y0'] - N) / cell - 0.5
    inside = (fx > -0.5) & (fx < cols - 0.5) & (fy > -0.5) & (fy < rows - 0.5)
    out = np.full(E.shape, np.nan, np.float32)
    clean = np.zeros(E.shape, bool)
    if not inside.any():
        return out, clean
    c0 = np.clip(np.floor(fx).astype(int), 0, cols - 1); c1 = np.clip(c0 + 1, 0, cols - 1)
    r0 = np.clip(np.floor(fy).astype(int), 0, rows - 1); r1 = np.clip(r0 + 1, 0, rows - 1)
    wx = np.clip(fx - c0, 0, 1); wy = np.clip(fy - r0, 0, 1)
    num = np.zeros(E.shape, np.float64); den = np.zeros(E.shape, np.float64)
    allfin = np.ones(E.shape, bool)
    for rr, cc, w in ((r0, c0, (1 - wx) * (1 - wy)), (r0, c1, wx * (1 - wy)),
                      (r1, c0, (1 - wx) * wy), (r1, c1, wx * wy)):
        v = a[rr, cc].astype(np.float64)
        f = np.isfinite(v)
        allfin &= interior[rr, cc]
        num += np.where(f, v * w, 0.0); den += np.where(f, w, 0.0)
    ok = inside & (den > 1e-6)
    out[ok] = (num[ok] / den[ok]).astype(np.float32)
    clean[:] = ok & allfin
    return out, clean


def sample_nearest(grid, E, N):
    """Nearest cell (for categorical grids such as landcover)."""
    a = grid['elev'] if 'elev' in grid else grid['cls']
    cell = grid['cell_m']
    rows, cols = a.shape
    fx = (E - grid['x0']) / cell - 0.5
    fy = (grid['y0'] - N) / cell - 0.5
    inside = (fx > -0.5) & (fx < cols - 0.5) & (fy > -0.5) & (fy < rows - 0.5)
    out = np.full(E.shape, np.nan, np.float32)
    if not inside.any():
        return out
    ci = np.clip(np.round(fx).astype(int), 0, cols - 1)
    ri = np.clip(np.round(fy).astype(int), 0, rows - 1)
    v = a[ri, ci].astype(np.float32)
    out[inside] = v[inside]
    return out


# --------------------------------------------------------------------------
# registration (docs/hillshade-seams.md)
# --------------------------------------------------------------------------
# Products upgraded from a v1 full GPKG whose DTM had been pasted 5 m off
# (austria_processor._read_dtm_for_tile padded re-read, fixed 2026-11-21)
# carry a terrain grid25 whose cell (r, c) really describes the ground at
# declared position + (dE, dN).  ``data/grid25_registration.json`` holds the
# measured correction per product code (``grid25_registration.py``); it is
# applied to the grid ORIGIN here, once, for every reader — never resample.

_REG = {'mtime': None, 'map': {}, 'version': 0}
_REG_PATH = None


def _reg_path():
    global _REG_PATH
    if _REG_PATH is None:
        import os
        from pathlib import Path
        _REG_PATH = Path(os.environ.get('GRID25_REGISTRATION',
                         str(Path(__file__).resolve().parent / 'data' / 'grid25_registration.json')))
    return _REG_PATH


def registration() -> dict:
    """→ {'map': {code: [dE, dN]}, 'version': int} (mtime-cached)."""
    p = _reg_path()
    try:
        mt = p.stat().st_mtime
    except OSError:
        return _REG
    if mt != _REG['mtime']:
        try:
            import json
            d = json.loads(p.read_text())
            _REG.update(mtime=mt, map=d.get('offsets') or {}, version=int(d.get('version') or 0))
        except Exception:  # noqa: BLE001
            _REG['mtime'] = mt
    return _REG


def _drop_zero_fill(grid):
    """Some products carry 0.0 m where the DTM had no data (outside the
    national border, missing ALS sheets) and, next to it, cells averaged
    half over that fill.  No Austrian ground is below 100 m: zero cells and
    their 8 neighbours become nodata (also drops the dependent slope)."""
    a = grid.get('elev')
    if a is None:
        return
    z = a == 0
    if not z.any():
        return
    from scipy import ndimage
    bad = ndimage.binary_dilation(z, structure=np.ones((3, 3), bool))
    a = np.array(a, np.float32, copy=True)
    a[bad] = np.nan
    grid['elev'] = a
    sl = grid.get('slope')
    if sl is not None and getattr(sl, 'shape', None) == a.shape:
        sl = np.array(sl, np.float32, copy=True)
        sl[bad] = np.nan
        grid['slope'] = sl


def register(code, grid: dict) -> dict:
    """Shift a decoded *terrain* grid's origin by its measured registration
    offset (idempotent; landcover grids are not affected by the bug)."""
    if grid is None or grid.get('_reg') is not None:
        return grid
    _drop_zero_fill(grid)
    _patch(code, grid)
    off = registration()['map'].get(str(code))
    if off and (off[0] or off[1]):
        grid['x0'] = float(grid['x0']) + float(off[0])
        grid['y0'] = float(grid['y0']) + float(off[1])
        grid['_reg'] = (float(off[0]), float(off[1]))
    else:
        grid['_reg'] = (0.0, 0.0)
    _clip_national(grid)
    return grid


MASK_BUFFER_M = 100.0    # cells kept beyond the border: support for the cubic taps + gradient stencil only


def _clip_national(grid):
    """Products are rectangles and BEV served (partly still serves) terrain
    beyond the border — cells farther than ``MASK_BUFFER_M`` outside the
    national outline (``austria_mask``, the one the NE server clips with) become
    nodata.  Tiles cut their alpha exactly on the outline afterwards."""
    a = grid.get('elev')
    if a is None:
        return
    try:
        import austria_mask
        m = austria_mask.grid_mask(float(grid['x0']), float(grid['y0']), a.shape[0], a.shape[1],
                                   float(grid.get('cell_m') or 25.0), MASK_BUFFER_M)
    except Exception:  # noqa: BLE001 — a broken mask must never take the tiles down
        return
    if m is None or m.all():
        return
    for k in ('elev', 'slope'):
        v = grid.get(k)
        if v is not None and getattr(v, 'shape', None) == m.shape:
            v = np.array(v, np.float32, copy=True)
            v[~m] = np.nan
            grid[k] = v


def _patch(code, grid):
    """Cells re-derived from the BEV DTM for tiles whose stored terrain is
    wrong (``grid25_patch.py``; applied in the product's own, unregistered
    frame — a patched tile is by construction not shifted)."""
    try:
        import grid25_patch
        grid25_patch.apply(code, grid)
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------
# seamless mosaic: C2 cubic B-spline taps + feathered blend across products
# --------------------------------------------------------------------------
FEATHER_CELLS = 6.0      # weight ramps 0 → 1 over this many cells from a grid's data edge
_EPS_PARTIAL = 1e-9     # extrapolated half-cell rim (see sample_smooth)
_EPS_W = 1e-4            # finite-but-edge samples still fill where nothing better exists


def _feather(grid, a):
    """Per-cell blend weight: 0 on the outer (partial block-mean) ring of
    the product's data footprint, rising linearly to 1 ``FEATHER_CELLS``
    further in.  Cached on the grid dict."""
    w = grid.get('_feather')
    if w is None or w.shape != a.shape:
        from scipy.ndimage import distance_transform_edt
        f = np.zeros((a.shape[0] + 2, a.shape[1] + 2), bool)
        f[1:-1, 1:-1] = np.isfinite(a)
        d = distance_transform_edt(f)[1:-1, 1:-1]
        w = np.clip((d - 1.0) / FEATHER_CELLS, 0.0, 1.0).astype(np.float32)
        grid['_feather'] = w
    return w


def _cubic_w(t):
    """Catmull-Rom weights for taps −1, 0, +1, +2 (interpolating, C1 →
    continuous gradient, so no faceted 25 m cells in the shading)."""
    t2 = t * t; t3 = t2 * t
    return (-0.5 * t + t2 - 0.5 * t3, 1 - 2.5 * t2 + 1.5 * t3,
            0.5 * t + 2 * t2 - 1.5 * t3, -0.5 * t2 + 0.5 * t3)


def _linear_w(t):
    return (np.zeros_like(t), 1 - t, t, np.zeros_like(t))


def sample_smooth(grid, E, N, kernel='cubic'):
    """→ ``(value, weight)``: NaN-aware interpolation on cell centres
    (``cubic`` = Catmull-Rom for shading, ``linear`` = bilinear for data
    APIs) and the bilinear-interpolated feather weight.  Only the
    sub-rectangle of the sample arrays that falls on the grid is computed."""
    a = grid['elev'] if 'elev' in grid else grid['cls']
    cell = grid['cell_m']
    rows, cols = a.shape
    out = np.full(E.shape, np.nan, np.float32)
    wout = np.zeros(E.shape, np.float32)
    fx = (E - grid['x0']) / cell - 0.5
    fy = (grid['y0'] - N) / cell - 0.5
    inside = (fx > -1.0) & (fx < cols) & (fy > -1.0) & (fy < rows)
    if not inside.any():
        return out, wout
    if E.ndim == 2:
        rr = np.flatnonzero(inside.any(1)); cc = np.flatnonzero(inside.any(0))
        sl = (slice(rr[0], rr[-1] + 1), slice(cc[0], cc[-1] + 1))
    else:
        ii = np.flatnonzero(inside)
        sl = (slice(ii[0], ii[-1] + 1),)
    fx = fx[sl]; fy = fy[sl]
    feather = _feather(grid, a)
    ix = np.floor(fx).astype(np.int64); iy = np.floor(fy).astype(np.int64)
    tx = fx - ix; ty = fy - iy
    kw = _cubic_w if kernel == 'cubic' else _linear_w
    wxs = kw(tx); wys = kw(ty)
    lx = _linear_w(tx); ly = _linear_w(ty)
    num = np.zeros(fx.shape, np.float64); den = np.zeros(fx.shape, np.float64)
    lnum = np.zeros(fx.shape, np.float64); lden = np.zeros(fx.shape, np.float64)
    wacc = np.zeros(fx.shape, np.float64)
    allfin = np.ones(fx.shape, bool)
    for j in range(4):
        r = iy + (j - 1)
        rok = (r >= 0) & (r < rows)
        rc = np.clip(r, 0, rows - 1)
        for i in range(4):
            central = j in (1, 2) and i in (1, 2)
            if kernel != 'cubic' and not central:
                continue
            c = ix + (i - 1)
            ok = rok & (c >= 0) & (c < cols)
            cc_ = np.clip(c, 0, cols - 1)
            v = a[rc, cc_].astype(np.float64)
            ok &= np.isfinite(v)
            v = np.where(ok, v, 0.0)
            allfin &= ok
            k = wys[j] * wxs[i]
            num += v * k
            den += np.where(ok, k, 0.0)
            if central:
                kl = ly[j] * lx[i]
                lnum += v * kl
                lden += np.where(ok, kl, 0.0)
                wacc += np.where(ok, feather[rc, cc_] * kl, 0.0)
    # all 16 (4) taps finite → the kernel as is; otherwise fall back to the
    # NaN-aware bilinear of the central 2×2 (weights renormalised) — this
    # only happens on the outer cells of a product, where the feather weight
    # is ~0 unless no other product covers the spot.
    good = lden > 1e-6
    val = np.full(fx.shape, np.nan, np.float32)
    val[good] = (lnum[good] / lden[good]).astype(np.float32)
    val[allfin] = num[allfin].astype(np.float32)
    out[sl] = val
    # full = all four surrounding cell centres are data (true interpolation);
    # partial = extrapolated past the outermost cell centres (flat) — given a
    # vanishing weight so it only ever fills where nothing interpolates.
    full = lden > 1.0 - 1e-6
    wout[sl] = np.where(full, wacc + _EPS_W, np.where(good, _EPS_PARTIAL, 0.0))
    return out, wout


def merge(grids, E, N, kernel='linear', footprints=None, rim=True):
    """Seamless elevation mosaic of several (overlapping) product grids:
    feather-weighted mean of the per-grid samples (``kernel='cubic'`` for shading).  A product's weight
    falls to ~0 at its own data edge, so the mosaic is continuous wherever
    one product ends inside another — no hard hand-over line, whatever the
    residual disagreement between two products is.

    ``rim=False`` drops the extrapolated half-cell rim past a product's
    outermost cell centres (flat → a gradient kink): the shaded tiles end at
    the last true interpolation instead."""
    num = np.zeros(E.shape, np.float64); den = np.zeros(E.shape, np.float64)
    for g in grids:
        v, w = sample_smooth(g, E, N, kernel)
        m = w > (0 if rim else _EPS_PARTIAL * 10)
        if footprints is not None and m.any():
            footprints.append(m)
        num[m] += v[m].astype(np.float64) * w[m]
        den[m] += w[m]
    Z = np.full(E.shape, np.nan, np.float32)
    m = den > 0
    Z[m] = (num[m] / den[m]).astype(np.float32)
    return Z


def merge_mosaic(grids, E, N, kernel='cubic', footprints=None):
    """Mosaic first, resample once (the tile path).

    1. every product is put on ONE common 25 m lattice covering the sample
       points (+3 cells): products are natively on that lattice, so this is
       a plain paste — only the ones carrying a registration offset are
       bilinearly re-gridded by their 5 m;
    2. where several products cover a cell they are feather-blended;
    3. the single seamless grid is interpolated (Catmull-Rom) to E, N.

    Unlike per-product sampling (``merge``) this interpolates *across* two
    products that merely abut, and never extrapolates: samples beyond the
    outermost cell centres of the mosaic are NaN."""
    live = [g for g in grids if g is not None]
    if not live:
        return np.full(E.shape, np.nan, np.float32)
    cell = float(live[0]['cell_m'])
    pad = 3
    c0 = math.floor(float(E.min()) / cell) - pad; c1 = math.ceil(float(E.max()) / cell) + pad
    r0 = math.floor(float(N.min()) / cell) - pad; r1 = math.ceil(float(N.max()) / cell) + pad
    cols = int(c1 - c0); rows = int(r1 - r0)
    x0 = c0 * cell; y0 = r1 * cell
    Em, Nm = np.meshgrid(x0 + (np.arange(cols) + 0.5) * cell, y0 - (np.arange(rows) + 0.5) * cell)
    num = np.zeros((rows, cols), np.float64); den = np.zeros((rows, cols), np.float64)
    masks = []
    for g in live:
        v, w = sample_smooth(g, Em, Nm, 'linear')
        m = w > _EPS_PARTIAL * 10                    # true cells only, no extrapolated rim
        if not m.any():
            continue
        if footprints is not None:
            masks.append(m)
        num[m] += v[m].astype(np.float64) * w[m]
        den[m] += w[m]
    mos = np.full((rows, cols), np.nan, np.float32)
    m = den > 0
    mos[m] = (num[m] / den[m]).astype(np.float32)
    mg = {'elev': mos, 'x0': x0, 'y0': y0, 'cell_m': cell, 'rows': rows, 'cols': cols,
          '_feather': np.ones((rows, cols), np.float32)}
    Z, w = sample_smooth(mg, E, N, kernel)
    Z[w < 0.5] = np.nan
    if footprints is not None and masks:
        ci = np.clip(np.floor((E - x0) / cell).astype(np.int64), 0, cols - 1)
        ri = np.clip(np.floor((y0 - N) / cell).astype(np.int64), 0, rows - 1)
        footprints.extend(mk[ri, ci] for mk in masks)
    return Z
