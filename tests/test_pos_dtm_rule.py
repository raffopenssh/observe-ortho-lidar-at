"""Terrain rule of ne_pos_fix (tiles without a usable tree window): python3 tests/test_pos_dtm_rule.py"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import ne_pos_fix as P

rng = np.random.default_rng(3)
x = np.arange(60) * 25.0
ground = lambda dx: 500 + 0.2 * (x + dx) + 15 * np.sin((x + dx) / 180.0)      # noqa: E731
noise = rng.normal(0, 0.03, x.size)

# stored in the true frame
w = P.dtm_fit(ground(0) + noise, ground(0), ground(P.SHIFT[0]))
assert w['v'] == 'zero', w
# stored 5 m off
w = P.dtm_fit(ground(P.SHIFT[0]) + noise, ground(0), ground(P.SHIFT[0]))
assert w['v'] == 'shift', w
# flat ground: both frames fit -> no verdict
flat = np.full(x.size, 300.0)
assert P.dtm_fit(flat + noise, flat, flat)['v'] == 'unclear'
# stale terrain: neither fits, however large the ratio
w = P.dtm_fit(ground(0) + 40, ground(0), ground(0) + 30)
assert w['v'] == 'unclear', w
# too few cells
assert P.dtm_fit(ground(0)[:5], ground(0)[:5], ground(-5)[:5])['v'] == 'unclear'

# a terrain verdict of an earlier pass is evidence of the next one and is not measured again
import inspect
src = inspect.getsource(P.verify_tiles_kg)
assert "w.get('src') == 'dtm'" in src and 'terrain' in inspect.signature(P.verify_tiles_kg).parameters

# rule 2 cell mask: only cells the tile owns among all parent tiles, not covered by a same-product neighbour
g = {'x0': 0.0, 'y0': 2000.0}
T = np.array([[0, 0, 2000, 2000], [1500, 0, 3500, 2000], [10, 10, 2010, 2010]], float)   # 1 = same-product neighbour, 2 = twin of another block
m = P.dtm_cell_mask(g, 0, 80, 0, 80, T[:2], 0, [0, 1])
assert m[:, :58].all() and not m[:, 60:].any(), m.sum(0)            # overlap with tile 1 (x >= 1500 - one cell) is out
m2 = P.dtm_cell_mask(g, 0, 80, 0, 80, T, 2, [2])
assert m2.sum() < 0.1 * m2.size, m2.sum()                             # eclipsed twin owns next to nothing
# keep-cell share of a window
w = {'c': [125, 125]}
z = np.zeros(0)
assert P._keep_share(w, np.full(60, 100.0), np.full(60, 100.0), np.full(60, 9.0), np.full(40, 120.0), np.full(40, 120.0)) == 0.4
assert P._keep_share(w, z, z, z, z, z) == 0.0
assert 'in_keep(kp, x, y)' in src and "w.get('r') == DTM_RULE" in src
import qc_generation
assert 'verdict' in inspect.signature(qc_generation.ledger_codes).parameters
print('ok')
