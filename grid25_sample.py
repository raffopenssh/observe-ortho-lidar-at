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


def merge(grids, E, N):
    """Elevation from several (possibly overlapping) grids: samples built
    only from interior cells win over edge / partial-cell samples regardless
    of grid order; edge samples still fill where nothing better exists."""
    Z = np.full(E.shape, np.nan, np.float32)
    edge = np.full(E.shape, np.nan, np.float32)
    for g in grids:
        v, clean = sample(g, E, N)
        m = np.isnan(Z) & clean
        Z[m] = v[m]
        m2 = np.isnan(edge) & np.isfinite(v)
        edge[m2] = v[m2]
    m = np.isnan(Z) & np.isfinite(edge)
    Z[m] = edge[m]
    return Z
