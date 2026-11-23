"""tree_stands: H3 range selection, band windows, ranking separation, roll-up consistency, HTTP routes.
Runs against the live store (skips when not built).  python3 tests/test_tree_stands.py"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import h3
import numpy as np

import tree_stands as ts


def _t():
    return ts.table()


def test_band_of():
    assert [ts.band_of(h) for h in (0, 3, 4.9, 5, 39.9, 40, 99)] == [0, 0, 0, 1, 7, 8, 11]


def test_h3_ranges_match_hierarchy():
    t = _t(); ids = t['ids']
    c = h3.int_to_str(int(ids[len(ids) // 2]))
    for r in (5, 7, 9, 10):
        p = h3.cell_to_parent(c, r)
        got = ts.h3_ranges(ids, [p])
        assert len(got) and all(h3.cell_to_parent(h3.int_to_str(int(x)), r) == p for x in ids[got][:200])
        lo, hi = got[0], got[-1]
        for j in (lo - 1, hi + 1):      # neighbours in sort order are outside
            if 0 <= j < len(ids):
                assert h3.cell_to_parent(h3.int_to_str(int(ids[j])), r) != p
    kid = h3.cell_to_center_child(c, 12)
    assert ids[ts.h3_ranges(ids, [kid])].tolist() == [int(c, 16)]
    try:
        ts.h3_ranges(ids, ['zz']); raise AssertionError
    except ts.BadQuery:
        pass


def test_summary_rollup_and_meta():
    t = _t(); m = t['meta']
    a = ts.summary()['areas'][0]
    assert a['trees'] == m['apices'] and a['at_least'] == m['at_least'] and a['hexes'] == m['hexes']
    c = h3.int_to_str(int(t['ids'][len(t['ids']) // 3])); p = h3.cell_to_parent(c, 6)
    r = ts.summary([{'id': 1, 'h3': [p]}, {'id': 2, 'h3': list(h3.cell_to_children(p, 8))}])['areas']
    assert r[0]['trees'] == r[1]['trees'] > 0 and r[0]['bands'] == r[1]['bands'] and sum(r[0]['bands']) == r[0]['trees']
    for res in (10, 8, 6):
        hx = ts.hexes({'h3': [p]}, res=res, bands=True)
        assert hx['trees'] == r[0]['trees'] == sum(hx['columns']['trees'])
        assert np.sum(hx['columns']['bands'], axis=0).tolist() == r[0]['bands']
    assert ts.hexes({'h3': [p]}, res=6)['columns']['h3'] == [p]


def test_matches_tree_stats():
    """Same pass, same apices, same gen as the national table -> identical band totals."""
    import tree_stats
    st = tree_stats.national(); m = _t()['meta']
    if st.get('gen') != m['gen']:
        print('  (skip: national gen', st.get('gen'), '!= stands gen', m['gen'], ')'); return
    assert st['apices'] == m['apices'] and [a['apices'] for a in st['at_least']] == m['at_least']
    assert m.get('cum_saturated', 0) == 0


def test_stands_ranking():
    r = ts.stands(min_height=40, limit=10)
    s = r['stands']
    assert len(s) == 10 and [x['per_ha'] for x in s] == sorted((x['per_ha'] for x in s), reverse=True)
    assert all(x['tallest_m'] >= 39.5 and x['trees'] <= x['trees_all'] and x['ring']['trees'] >= x['trees'] for x in s)
    for i, a in enumerate(s):           # separation
        for b in s[:i]:
            assert h3.great_circle_distance((a['lat'], a['lon']), (b['lat'], b['lon']), unit='m') >= r['separation_m'] * 0.98
    raw = ts.stands(min_height=40, limit=10, separation_m=0)['stands']
    assert raw[0]['h3'] == s[0]['h3']
    w = ts.stands(min_height=20, max_height=30, area={'kg': s[0]['kg']}, bands=True)
    assert all(x['kg'] == s[0]['kg'] and x['trees'] == sum(x['bands'][4:6]) for x in w['stands'])
    for bad in (dict(sort='x'), dict(min_height=30, max_height=22)):
        try:
            ts.stands(**bad); raise AssertionError
        except ts.BadQuery:
            pass


def test_http():
    import public_api
    c = public_api.app.test_client()
    r = c.get('/api/v1/trees/stands?min_height=40&limit=3')
    assert r.status_code == 200 and r.headers.get('ETag') and 'max-age=3600' in r.headers.get('Cache-Control', '')
    d = r.get_json(); cell = d['stands'][0]['h3']
    assert c.get('/api/v1/trees/stands?min_height=40&limit=3', headers={'If-None-Match': r.headers['ETag']}).status_code == 304
    p = h3.cell_to_parent(cell, 7)
    g = c.get(f'/api/v1/trees/summary?h3={p}&top=1').get_json()['areas'][0]
    poly = {'type': 'Polygon', 'coordinates': [[[x[1], x[0]] for x in h3.cell_to_boundary(p)]]}
    poly['coordinates'][0].append(poly['coordinates'][0][0])
    b = c.post('/api/v1/trees/summary', json={'areas': [{'id': 'a', 'h3': [p]}, {'id': 'n2k', 'geometry': poly}]}).get_json()['areas']
    assert b[0]['trees'] == g['trees'] and b[0]['id'] == 'a' and 0.7 < b[1]['trees'] / g['trees'] < 1.3
    assert c.get(f'/api/v1/trees/hexes?h3={p}&res=9').get_json()['trees'] == g['trees']
    assert c.get('/api/v1/trees/hexes').status_code == 400          # all Austria > MAX_HEXES at res 10
    assert c.get('/api/v1/trees/stands?h3=nope').status_code == 400
    assert c.get('/api/v1/trees/summary?bbox=1,2').status_code == 400


def test_hex_blob_exact():
    """NEX1 national table: res-9 roll-up, exact h_sum / tallest (0.25 m), bands sum to trees."""
    import gzip
    import cells_national as cn
    ids = np.array([0x8a1e15b0000ffff, 0x8a1e15b0008ffff, 0x8a1e15b2000ffff], np.uint64)   # 2 share a res-9 parent?
    par = cn.parent_ids(ids, 9, 10)
    hist = np.zeros((3, ts.NB), np.uint32); hist[0, 0] = 2; hist[1, 5] = 1; hist[2, 11] = 3
    top = np.array([4.25, 27.5, 60.75], np.float32); hsum = np.array([7.75, 27.5, 180.25])
    blob, info = ts._hex_blob(9, ids, hist, top, hsum, 'g', 't')
    hdr, c = cn.decode(gzip.decompress(blob))
    assert hdr['kind'] == 'tree_hexes' and hdr['n'] == len(np.unique(par)) == info['n']
    assert c['id'].tolist() == np.unique(par).tolist()
    assert int(c['trees'].sum()) == 6 and (c['bands'].sum(1) == c['trees']).all()
    assert int(c['h_sum'].sum()) == round((7.75 + 27.5 + 180.25) * 4) and int(c['tallest'].max()) == 243


if __name__ == '__main__':
    if _t() is None:
        print('SKIP: stand index not built'); sys.exit(0)
    for k, f in list(globals().items()):
        if k.startswith('test_'):
            f(); print('ok', k)
