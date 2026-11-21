"""National mask clip, grid25 patch overlay, tile-edge rule, NE position fix.
Synthetic, no network:  python3 tests/test_grid25_patch.py"""
import json, os, sys, tempfile
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
tmp = tempfile.mkdtemp()
# a square "Austria": lon 13..14, lat 47..48
json.dump({'sha': 'testsha0', 'rings': [[[13, 47], [14, 47], [14, 48], [13, 48], [13, 47]]]}, open(tmp + '/o.json', 'w'))
os.environ['AUSTRIA_OUTLINE'] = tmp + '/o.json'
os.environ['GRID25_PATCH_DIR'] = tmp + '/patches'
json.dump({'kgs': {'00001': {'verdict': 'shift', 'off': [-5, 5]}, '00002': {'verdict': 'mixed'}}}, open(tmp + '/npf.json', 'w'))
os.environ['NE_POS_FIX'] = tmp + '/npf.json'
os.environ['GRID25_REGISTRATION'] = tmp + '/none.json'
import austria_mask as am, grid25_sample as g25, grid25_patch as gp, ne_pos_fix as pf
from pyproj import Transformer
tf = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)

# 1) mask: grid straddling the west border
x, y = tf.transform(13.0, 47.5)
g = {'elev': np.full((40, 80), 500.0, np.float32), 'slope': np.zeros((40, 80), np.float32), 'x0': x - 1000.0, 'y0': y + 500.0, 'cell_m': 25.0}
m0 = am.grid_mask(g['x0'], g['y0'], 40, 80, 25.0)
assert m0[:, :36].sum() == 0 and m0[:, 44:].all(), (m0[:, :36].sum(), m0[:, 44:].mean())
g25.register('x', g)
fin = np.isfinite(g['elev'])
assert not fin[:, :30].any() and fin[:, 40:].all(), 'cells > 100 m abroad must be dropped, the buffer kept'
assert 2 <= (fin & ~m0).sum(axis=1).min() <= 6, (fin & ~m0).sum(axis=1)      # ≈ 100 m of support cells
ins = am.inside(np.array([x - 30, x + 30]), np.array([y, y]))
assert list(ins) == [False, True]
lo, la = 13.0, 47.5
assert am.near_border(lo - .01, la - .01, lo + .01, la + .01) and not am.near_border(13.4, 47.4, 13.6, 47.6)
assert am.version() == 'testsha0'

# 2) tile-edge rule: a tile 8 m too high inside a gentle slope
a = np.fromfunction(lambda r, c: 400 + 0.2 * c + 0.1 * r, (120, 120)).astype(np.float32)
bad = a.copy(); bad[30:90, 30:90] += 8.0
tiles = [(1, 0, 0, 30, 120), (2, 30, 0, 90, 30), (3, 30, 30, 90, 90), (4, 30, 90, 90, 120), (5, 90, 0, 120, 120)]
G = {'elev': bad, 'x0': 0.0, 'y0': 0.0}
ed = gp.edges('t', G, tiles)
assert ed and all(e['run'] >= gp.EDGE_RUN for e in ed), ed
assert not gp.edges('t', {'elev': a, 'x0': 0.0, 'y0': 0.0}, tiles)
hit = {t[0] for t in gp.edge_tiles('t', G, ed, tiles)}
assert 3 in hit, hit

# 3) patch overlay: stored blocks replace, -32768 → nodata, wrong grid → ignored
os.makedirs(gp.DIR, exist_ok=True)
e = np.round(a[30:90, 30:90] * 10).astype(np.int16); e[0, 0] = -32768
np.savez_compressed(gp.DIR / 't.npz', e0=e, s0=np.zeros((60, 60), np.uint8))
json.dump({'version': 3, 'patched': {'t': {'grid': [0.0, 0.0, 120, 120], 'blocks': [[30, 30, 90, 90]]}}}, open(gp.INDEX, 'w'))
G = {'elev': bad.copy(), 'slope': np.ones((120, 120), np.float32), 'x0': 0.0, 'y0': 0.0}
assert gp.apply('t', G) and gp.version() == 3
assert np.isnan(G['elev'][30, 30]) and abs(G['elev'][60, 60] - a[60, 60]) < 0.06 and G['slope'][60, 60] == 0
assert not gp.edges('t', G, tiles)
assert not gp.apply('t', {'elev': bad.copy(), 'x0': 25.0, 'y0': 0.0})

# 4) NE position fix: NET1 / NEB1 dx,dy move by the KG offset, other bits untouched
def word(dx, dy, rest):
    return (dx & 31) | ((dy & 31) << 5) | (rest << 10)
t = np.array([word(0, 0, 12345), word(-16, 15, 7), word(3, -4, 99)], '<u4').tobytes()
b = np.array([word(1, 1, 2 ** 40 + 5)], '<u8').tobytes()
t2, b2 = pf.fix_words('00001', t, b)
w = np.frombuffer(t2, '<u4')
sx = lambda v: int(v) - 32 if v >= 16 else int(v)   # noqa: E731
assert [(sx(v & 31), sx((v >> 5) & 31), int(v >> 10)) for v in w] == [(-5, 5, 12345), (-16, 15, 7), (-2, 1, 99)]
w = np.frombuffer(b2, '<u8')
assert (sx(int(w[0]) & 31), sx((int(w[0]) >> 5) & 31), int(w[0]) >> 10) == (-4, 6, 2 ** 40 + 5)
assert pf.fix_words('00002', t, b) == (t, b) and pf.fix_words('99999', t, b) == (t, b)
assert pf.tag('00001') == '|p-5+5' and pf.tag('00002') == '' and pf.offset('1') == (-5, 5)
print('ok')
