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
import qc_generation
assert 'verdict' in inspect.signature(qc_generation.ledger_codes).parameters
print('ok')
