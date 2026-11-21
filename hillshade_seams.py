"""hillshade_seams — detector for seam lines baked into hillshade tiles.

A seam is what the tile mosaic paints when it hands over from one product
grid to another that disagrees with it (docs/hillshade-seams.md): a thin
bright/dark line pair along the edge of a product's footprint.

Rule (as asked by the tile consumers): for every pixel pair straddling an
internal product boundary, compare the shade step ACROSS the boundary with
the steps one and two pixels further in on each side; the pair is an
outlier when the step exceeds ``RATIO`` × the median of those neighbours
(with a floor of ``FLOOR`` grey levels so quantisation noise on flat
ground cannot trigger).  A *seam* is a connected run of outlier pairs at
least ``MIN_RUN`` px long.  Expected count: 0.

``find_seams`` is pure numpy on (grey, alpha, footprints) so tests can feed
it synthetic mosaics; ``tools/hillshade_seam_check.py`` runs it over real
tiles / the whole country.
"""
from __future__ import annotations

import numpy as np

RATIO = 3.0
FLOOR = 1.5       # grey levels (0..255)
MIN_RUN = 40      # px


def _boundary_pairs(footprints, axis):
    """Bool array over adjacent pixel pairs along *axis*: some product's
    coverage differs between the two pixels."""
    out = None
    for f in footprints:
        d = (f[1:, :] != f[:-1, :]) if axis == 0 else (f[:, 1:] != f[:, :-1])
        out = d if out is None else (out | d)
    return out


def find_seams(grey, alpha, footprints, ratio=RATIO, floor=FLOOR, min_run=MIN_RUN, band_px=0):
    """→ list of ``{axis, run_px, row, col, step, ref}`` (one per seam run).

    *grey* H×W float, *alpha* H×W (0 = nodata), *footprints* list of H×W
    bool coverage masks, one per product grid (same lattice).  *band_px*
    widens every boundary to the zone in which the mosaic hands over (the
    feather width; the legacy mosaic switched one cell inside the edge)."""
    from scipy import ndimage
    seams = []
    if not footprints:
        return seams
    g = np.asarray(grey, np.float32)
    ok = np.asarray(alpha) > 0
    for axis in (0, 1):
        gg = g if axis == 0 else g.T
        kk = ok if axis == 0 else ok.T
        b = _boundary_pairs([f if axis == 0 else f.T for f in footprints], 0)
        if b is None or not b.any():
            continue
        if band_px > 0:
            b = ndimage.binary_dilation(b, iterations=int(band_px))
        d = np.abs(np.diff(gg, axis=0))                 # d[i] = |g[i+1] − g[i]|
        vd = kk[1:] & kk[:-1]
        H = d.shape[0]
        if H < 5:
            continue
        # pair i (pixels i, i+1); neighbours: pairs i−2, i−1, i+1, i+2
        c = slice(2, H - 2)
        nb = np.stack([d[0:H - 4], d[1:H - 3], d[3:H - 1], d[4:H]])
        nbv = vd[0:H - 4] & vd[1:H - 3] & vd[3:H - 1] & vd[4:H]
        ref = np.median(nb, axis=0)
        out = np.zeros_like(b)
        out[c] = b[c] & vd[c] & nbv & (d[c] > ratio * np.maximum(ref, floor))
        if not out.any():
            continue
        # a seam line tilts by a few px across a tile → 8-connected, and the
        # boundary may sit one pair up/down from column to column
        lab, n = ndimage.label(ndimage.binary_dilation(out, structure=np.ones((3, 1), bool)),
                               structure=np.ones((3, 3), bool))
        if not n:
            continue
        for sl in ndimage.find_objects(lab):
            run = sl[1].stop - sl[1].start
            if run < min_run:
                continue
            sub = out[sl]
            if sub.any(axis=0).sum() < min_run * 0.8:
                continue
            i, j = np.argwhere(sub)[0]
            r, cidx = sl[0].start + int(i), sl[1].start + int(j)
            rc = (r, cidx) if axis == 0 else (cidx, r)
            seams.append({'axis': 'row' if axis == 0 else 'col', 'run_px': int(run),
                          'row': int(rc[0]), 'col': int(rc[1]),
                          'step': round(float(d[r, cidx]), 1),
                          'ref': round(float(np.median([d[r - 2, cidx], d[r - 1, cidx], d[r + 1, cidx], d[r + 2, cidx]])), 1)})
    return seams


def merge_legacy(grids, E, N, kernel='cubic', footprints=None, **_):
    """The pre-v4 mosaic (first product with a clean bilinear sample wins,
    hard hand-over) — kept ONLY so the regression test can prove the
    detector fires on the old behaviour."""
    from grid25_sample import sample, sample_smooth
    Z = np.full(E.shape, np.nan, np.float32)
    edge = np.full(E.shape, np.nan, np.float32)
    for g in grids:
        v, clean = sample(g, E, N)
        if footprints is not None:
            w = sample_smooth(g, E, N, 'linear')[1] > 0
            if w.any():
                footprints.append(w)
        m = np.isnan(Z) & clean
        Z[m] = v[m]
        m2 = np.isnan(edge) & np.isfinite(v)
        edge[m2] = v[m2]
    m = np.isnan(Z) & np.isfinite(edge)
    Z[m] = edge[m]
    return Z
