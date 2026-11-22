#!/usr/bin/env python3
"""ONE cell rule for tree apices (ne_rebin, docs/tree-cell-assignment.md).

    python3 tests/test_tree_cell_rule.py

Synthetic parts always run; the live parts need the primary corpus (data/ne/*.h3b)."""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import ne_rebin  # noqa: E402
import tree_qa  # noqa: E402
import tree_rule  # noqa: E402

# the report that started it (KG 92124, has a position frame): lon, lat, h, cell of the point
REPORT = [(9.580430, 47.215377, 42.5, '8c1f8e0ce6411ff'),
          (9.588556, 47.217010, 41.5, '8c1f8e0ca962bff'),
          (9.583366, 47.212805, 41.25, '8c1f8e0ca98e9ff')]


def test_ids_and_rounding():
    assert ne_rebin.REBIN_ID == tree_rule.CELL_RULE_ID and tree_rule.CELL_RULE_ID in tree_qa.THX_ID
    assert tree_qa._DIG_SUF.endswith(tree_rule.CELL_RULE_ID)        # store entries of an older rule are ignored
    # list rounding: half-metre steps, ties to the EVEN step = the whole metre (h is on a 0.25 m grid)
    q = tree_rule.quant_h(np.array([41.25, 41.5, 41.75, 42.0, 0.25]))
    assert q.tolist() == [82, 83, 84, 84, 1], q.tolist()


def test_cells_of_xy_matches_h3():
    import h3
    import pyproj
    tf = pyproj.Transformer.from_crs(3035, 4326, always_xy=True)
    rng = np.random.default_rng(1)
    x = 4285000 + rng.integers(0, 4000, 500).astype(float); y = 2665000 + rng.integers(0, 4000, 500).astype(float)
    lon, lat = tf.transform(x, y)
    want = [int(h3.latlng_to_cell(round(b, 6), round(a, 6), 12), 16) for a, b in zip(lon, lat)]
    assert ne_rebin.cells_of_xy(x, y).tolist() == want


def test_ring_and_live():
    import h3
    r = int(h3.latlng_to_cell(47.2, 9.58, 10), 16)
    rg = ne_rebin.ring(r)
    assert len(rg) == 6 and r not in rg
    assert ne_rebin.live_r10s([r]) == set(rg) | {r}


def test_marker_keep_identity():
    # no marker in the header → everything kept, whatever the source map says
    k = ne_rebin.marker_keep({}, 1, np.array([10.0, 20.0]), (np.array([1, 2], np.uint64), np.array([0, 3], np.int32)))
    assert k.tolist() == [True, True]


def _corpus():
    try:
        import cells_api as ca
        return ca if (ROOT / 'data' / 'ne' / '92124.h3b').exists() else None
    except Exception:  # noqa: BLE001
        return None


def test_live_no_apex_outside_its_cell():
    ca = _corpus()
    if ca is None:
        print('  (no corpus — skipped)'); return
    for kg in ('92124', '19516', '45512'):          # frame KG, two plain KGs
        st = ne_rebin.check(kg)
        assert st['ok'] and st['outside'] == 0, st
        assert st['apices'] + st['off_file'] == st['stored'], st
        assert st['off_file'] <= max(5, 0.02 * st['stored']), st
    assert ne_rebin.check('92124')['frame']


def test_live_report_and_products_agree():
    ca = _corpus()
    if ca is None:
        print('  (no corpus — skipped)'); return
    import h3
    import ne_trees as nt
    w, s, e, n = 9.575, 47.210, 9.592, 47.219
    r = nt.columns_bbox(w, s, e, n, limit=200000, fields=('lon', 'lat', 'h_m', 'cell'))   # the default (QA'd) apex set
    c = r.get('columns') or r
    lon = np.array(c['lon']); lat = np.array(c['lat']); h = np.array(c['h_m']); cell = list(c['cell'])
    # /trees: the cell field IS latLngToCell of the published position
    assert all(h3.latlng_to_cell(b, a, 12) == k for a, b, k in zip(lon.tolist(), lat.tolist(), cell))
    for lo, la, hh, want in REPORT:
        i = int(np.argmin((lon - lo) ** 2 + (lat - la) ** 2))
        assert abs(lon[i] - lo) < 2e-6 and abs(lat[i] - la) < 2e-6 and h[i] == hh, (lo, la, lon[i], lat[i], h[i])
        assert cell[i] == want, (cell[i], want)
    # cells: tree_n / tree_h_max / tree_h of every cell == the apices /trees puts into it
    per: dict = {}
    for k, q in zip(cell, tree_rule.quant_h(h).tolist()):
        per.setdefault(int(k, 16), []).append(int(q))
    ids = ca.ids_in_bbox(w, s, e, n)
    tb = ca.select(ids, (w, s, e, n))[0]
    tn = tb.obs['tn'].astype(int); thx = tb.obs['thx'].astype(int)
    inner = 0
    for cid, a, b in zip(tb.ids.tolist(), tn.tolist(), thx.tolist()):
        la, lo = h3.cell_to_latlng(f'{cid:x}')
        if not (w + 3e-4 < lo < e - 3e-4 and s + 2e-4 < la < n - 2e-4):
            continue                                 # cells cut by the bbox edge: /trees clips by point
        got = sorted(per.get(cid, []))
        if a < 255:
            assert a == len(got), (f'{cid:x}', a, len(got))
            assert b == (max(got) if got else 255), (f'{cid:x}', b, got)    # 255 = none
        inner += 1
    assert inner > 1000, inner
    print(f'  ({len(lon)} apices, {inner} cells compared)')


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn(); print('ok', name)
    print('all ok')
