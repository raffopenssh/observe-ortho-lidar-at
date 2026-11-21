#!/usr/bin/env python3
"""Hillshade seam regression (docs/hillshade-seams.md) — synthetic, offline.

    python3 tests/test_hillshade_seams.py
"""
import os, sys, json, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_reg = tempfile.NamedTemporaryFile('w', suffix='.json', delete=False)
json.dump({'version': 7, 'offsets': {'B': [-5.0, 5.0]}}, _reg); _reg.close()
os.environ['GRID25_REGISTRATION'] = _reg.name
os.environ['AUSTRIA_OUTLINE'] = _reg.name + '.nomask'      # national clip / patches are tested in test_grid25_patch.py
os.environ['GRID25_PATCH_DIR'] = _reg.name + '.patches'
import numpy as np
import grid25_sample as g25, hillshade_tiles as hs, hillshade_seams as sm

Z, X, Y = 15, 17257, 11494
E, N = hs.tile_lattice(Z, X, Y, 1)
EC, NC = float(E.mean()), float(N.mean())


def terrain(e, n):
    return (900 + 0.25 * (e - EC) + 0.1 * (n - NC) + 60 * np.sin((e - EC) / 180.0) * np.cos((n - NC) / 230.0)).astype(np.float32)


def grid(x0, y0, rows, cols, shift=(0.0, 0.0)):
    """Product grid; *shift* = the pipeline bug: cell at declared X holds
    the ground at X + shift."""
    e, n = np.meshgrid(x0 + (np.arange(cols) + 0.5) * 25, y0 - (np.arange(rows) + 0.5) * 25)
    return {'elev': terrain(e + shift[0], n + shift[1]), 'x0': float(x0), 'y0': float(y0),
            'cell_m': 25.0, 'rows': rows, 'cols': cols}


def snap(v):
    return round(v / 25) * 25


def scene(registered):
    # A covers the west 55 %, B (shifted product) the east 60 % → 15 % overlap
    w = float(E.max() - E.min()); x_w = snap(E.min() - 400); y_n = snap(N.max() + 400)
    rows = int((N.max() - N.min() + 800) // 25)
    A = grid(x_w, y_n, rows, int((0.55 * w + 400) // 25))
    xb = snap(E.min() + 0.40 * w)
    B = grid(xb, y_n, rows, int((0.60 * w + 400) // 25), shift=(-5.0, 5.0))
    if registered:
        g25.register('B', B)
    return [A, B]


def run(grids, merge_fn=None):
    fps = []
    r = hs.shade(Z, X, Y, grids=grids, footprints=fps, merge_fn=merge_fn)
    assert r is not None
    grey, alpha = r
    return grey, alpha, sm.find_seams(grey, alpha, [f[1:-1, 1:-1] for f in fps], band_px=70)


# 1) the old renderer on unregistered products → seams (detector fires)
_, _, s = run(scene(False), sm.merge_legacy)
assert s, 'detector must fire on the legacy hard hand-over'
assert any(q['axis'] == 'col' and q['run_px'] >= 40 for q in s), s

# 2) new mosaic, registration applied → none, and it matches the truth
grey, alpha, s = run(scene(True))
assert not s, s
assert set(np.unique(alpha)) <= {0, 255}
elev = g25.merge_mosaic(scene(True), E, N)
err = np.nanmax(np.abs(elev - terrain(E, N)))
assert err < 0.6, err

# 3) new mosaic even WITHOUT registration (unknown shifted product) → blended, no line
_, _, s = run(scene(False))
assert not s, s

# 4) abutting products without overlap → interpolated across, no gap, no seam
w = float(E.max() - E.min()); xm = snap(E.min() + 0.5 * w); y_n = snap(N.max() + 400)
rows = int((N.max() - N.min() + 800) // 25); nc = int((0.5 * w + 400) // 25)
grey, alpha, s = run([grid(xm - nc * 25, y_n, rows, nc), grid(xm, y_n, rows, nc)])
assert not s, s
assert (alpha == 255).all(), 'gap between abutting products'

# 5) contract: flat ground = 180, alpha binary, nodata never shaded against a fill
flat = {'elev': np.full((400, 400), 500, np.float32), 'x0': snap(EC - 5000), 'y0': snap(NC + 5000), 'cell_m': 25.0, 'rows': 400, 'cols': 400}
grey, alpha, _ = run([flat])
assert (alpha == 255).all() and np.all(np.round(grey).astype(int) == hs.FLAT_GREY), np.unique(np.round(grey))
half = grid(snap(EC), snap(N.max() + 400), rows, 200)       # data only in the east half
grey, alpha, _ = run([half])
assert set(np.unique(alpha)) == {0, 255}
edge = np.abs(np.diff(grey, axis=1))[(alpha[:, 1:] == 255) & (alpha[:, :-1] == 255)]
assert edge.max() < 40, edge.max()                          # no fake cliff at the data edge

# 6) registration is idempotent and versioned
g = grid(0, 0, 4, 4); g25.register('B', g); g25.register('B', g)
assert (g['x0'], g['y0']) == (-5.0, 5.0), (g['x0'], g['y0'])
assert hs.tileset_version() == f'{hs.RENDER_VERSION}.7.0-nomask', hs.tileset_version()
os.unlink(_reg.name)
print('ok')

# 7) zero-fill (0.0 m where the DTM had no data) is nodata, incl. the mixed rim
g = grid(snap(EC - 2000), snap(NC + 2000), 160, 160); g['elev'][:40] = 0.0
g25.register('zz', g)
assert np.isnan(g['elev'][:41]).all() and np.isfinite(g['elev'][41:]).all()
print('ok zero-fill')
