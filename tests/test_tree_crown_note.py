""""Same tree" (spurious + hint crown) and notes on tree reports.  Temp DB; needs the live NE corpus."""
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import public_flags as pf  # noqa: E402
import tree_user as tu  # noqa: E402
import tree_crown as tc  # noqa: E402
import ne_trees  # noqa: E402

BB = (10.075, 47.125, 10.1, 47.15)
F = ('lon', 'lat', 'h_m', 'crown_m2', 'x', 'y', 'kg_code')


def cols(**kw):
    ne_trees._pf_cache.clear()
    return ne_trees.columns_bbox(*BB, 'trees', min_height=3, fields=F, **kw)


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    pf.DB_PATH = tmp / 'f.sqlite'; pf.SALT_PATH = tmp / 'salt'
    pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear(); pf._daily.clear(); tu._state.clear()
    pf._post_limit = pf.sg.RateLimiter('t', 1000, 1000); pf._get_limit = pf.sg.RateLimiter('g', 1000, 1000)
    from flask import Flask
    app = Flask(__name__); app.register_blueprint(pf.flags_bp)
    c = app.test_client()
    assert pf.DAILY_CAP == 4500 and pf._key_cap(None) == 4500 and pf._key_cap('a~1f') >= 4500 and pf._key_cap('org') == 20000
    if not (ne_trees.status()['ok'] and ne_trees.bbox_state(*BB)['ready']):
        print('SKIP (no live corpus for the test bbox)'); return 0
    base = cols(realistic=False); plain_n = cols()['n']
    C = base['columns']; x = np.array(C['x']); y = np.array(C['y']); h = np.array(C['h_m'])
    # a triple A (tall), B, D (lower) with B, D within 4–20 m of A, all in one KG, B and D ≥ 3 m apart
    trip = None
    for a in range(len(x)):
        d = np.hypot(x - x[a], y - y[a])
        near = [int(j) for j in np.flatnonzero((d > 4) & (d < 20) & (h < h[a]) & (h[a] < 40)) if C['kg_code'][j] == C['kg_code'][a]]
        iso = lambda j: (np.hypot(x - x[j], y - y[j]) <= 2.6).sum() == 1     # noqa: E731
        near = [j for j in near if iso(j)]
        if iso(a) and len(near) >= 2 and np.hypot(x[near[0]] - x[near[1]], y[near[0]] - y[near[1]]) >= 3:
            trip = (a, near[0], near[1]); break
    assert trip, 'no apex triple in the test bbox'
    A, B, D = trip
    P = lambda i: {'lon': C['lon'][i], 'lat': C['lat'][i]}     # noqa: E731

    def flag(i, of, rep='u1', **kw):
        return c.post('/api/v1/flags', json={**P(i), 'layer': 'trees', 'claim': 'spurious', 'observed': {'h_m': C['h_m'][i]},
                                             'expected': {'absent': True, 'hint': 'crown', 'of': of}, 'reporter': rep,
                                             'source': 'client/tree', **kw})
    # --- 1b validation
    for bad in (None, {}, P(B), {'lon': C['lon'][B] + 0.01, 'lat': C['lat'][B]}, {'tree_id': 'nope'}):
        r = flag(B, bad)
        assert r.status_code == 400 and r.get_json()['reason'] == 'bad_of', (bad, r.data)
    far = int(np.argmax(np.hypot(x - x[B], y - y[B])))
    assert flag(B, P(far)).get_json()['reason'] == 'bad_of'
    assert c.get('/api/v1/flags?bbox=%s' % ','.join(map(str, BB))).get_json()['count'] == 0
    # --- 1a store / fold / tally
    r = flag(B, P(A)); j = r.get_json()
    assert r.status_code == 201 and not j['folded'] and j['tally']['claims'] == {'spurious': 1}, j
    assert j['crown']['of']['x'] == int(x[A]) and j['crown']['of']['y'] == int(y[A]) and 4 < j['crown']['dist_m'] < 20
    r = flag(B, P(A)); assert r.status_code == 201 and r.get_json()['folded'] and r.get_json()['n'] == 2
    # a second extra top (other apex, maybe same cell) is its own row; "of" = an extra top → bound to the root
    j = flag(D, P(B)).get_json()
    assert not j['folded'] and j['crown']['rebound'] and j['crown']['of']['x'] == int(x[A]), j
    assert flag(A, P(B)).get_json()['reason'] == 'bad_of'          # no cycles
    # --- 1c reads
    q = c.get(f'/api/v1/flags?lon={C["lon"][B]}&lat={C["lat"][B]}&k=1&layer=trees').get_json()
    e = [s['expected']['spurious'] for s in q['subjects'] if abs(s['lon'] - C['lon'][B]) < 1e-6]
    assert e and e[0]['hint'] == 'crown' and e[0]['of']['x'] == int(x[A]) and 'lon' in e[0]['of'], q
    # --- 1d realistic=1 drops both, merges the crowns; realistic=0 unchanged
    got = cols()
    G = got['columns']; keys = set(zip(G['x'], G['y']))
    assert (x[B], y[B]) not in keys and (x[D], y[D]) not in keys and (x[A], y[A]) in keys
    ia = list(zip(G['x'], G['y'])).index((x[A], y[A]))
    want = C['crown_m2'][A] + C['crown_m2'][B] + C['crown_m2'][D]
    assert abs(G['crown_m2'][ia] - want) < 0.25, (G['crown_m2'][ia], want)
    assert got['n'] == plain_n - 2, (got['n'], plain_n)
    raw = cols(realistic=False)
    assert raw['columns'] == base['columns']
    # --- ETag component: changes with every flag / retract, no cache clear needed
    kgB = str(C['kg_code'][B]).zfill(5); t1 = ne_trees.flag_rev(kgB); assert t1 and kgB + ':' in ne_trees.flags_token([kgB, '00000'])
    flag(B, P(A), rep='u9'); t2 = ne_trees.flag_rev(kgB); assert t2 != t1
    _r = c.post('/api/v1/flags', json={**P(B), 'layer': 'trees', 'claim': 'spurious', 'retract': True, 'reporter': 'u9'})
    assert ne_trees.flag_rev(kgB) == t1          # same rows again = same state
    c.post('/api/v1/flags', json={**P(B), 'layer': 'trees', 'claim': 'spurious', 'retract': True, 'reporter': 'u1'})
    G = ne_trees.columns_bbox(*BB, 'trees', min_height=3, fields=F)['columns']      # NOT cols(): flag cache must notice itself
    assert (x[B], y[B]) in set(zip(G['x'], G['y']))
    flag(B, P(A))
    G = ne_trees.columns_bbox(*BB, 'trees', min_height=3, fields=F)['columns']
    assert (x[B], y[B]) not in set(zip(G['x'], G['y']))
    # --- 1e labels
    ls = tc.labels()
    assert len(ls) == 2 and {(l['of_x'], l['of_y']) for l in ls} == {(int(x[A]), int(y[A]))} and ls[0]['crown_m2'] is not None
    # --- retract (no expected needed), 200 folded, 403
    b = {**P(D), 'layer': 'trees', 'claim': 'spurious', 'retract': True}
    assert c.post('/api/v1/flags', json={**b, 'reporter': 'other'}).status_code == 403
    r = c.post('/api/v1/flags', json={**b, 'reporter': 'u1'}); assert r.status_code == 201 and r.get_json()['retracted'], r.data
    r = c.post('/api/v1/flags', json={**b, 'reporter': 'u1'}); assert r.status_code == 200 and r.get_json()['folded']
    G = cols()['columns']
    assert (x[D], y[D]) in set(zip(G['x'], G['y'])) and len(tc.labels()) == 1
    # a plain spurious flag still works and is cell-keyed as before
    j = c.post('/api/v1/flags', json={**P(D), 'layer': 'trees', 'claim': 'spurious', 'reporter': 'u1'}).get_json()
    row = pf._conn().execute('SELECT skey, cell FROM report WHERE id=?', (j['id'],)).fetchone()
    assert row[0] == row[1] > 0
    assert c.post('/api/v1/flags', json={**b, 'reporter': 'u1'}).get_json()['id'] == j['id']

    # ===== 2 notes =====
    T = {**P(A), 'layer': 'trees', 'claim': 'tree', 'reporter': 'n1'}
    assert c.post('/api/v1/flags', json={**T, 'note': None}).status_code == 403
    r = c.post('/api/v1/flags', json={**T, 'note': 'Naturdenkmal · Nr: 12'}); j = r.get_json()
    assert r.status_code == 201 and j['tree']['op'] == 'note' and j['tree']['note'] == 'Naturdenkmal · Nr: 12', j
    r = c.post('/api/v1/flags', json={**T, 'note': 'Naturdenkmal · Nr: 12'}); assert r.status_code == 200 and r.get_json()['folded']
    j = c.post('/api/v1/flags', json={**T, 'species': 'Buche', 'note': 'x' * 400}).get_json()
    assert j['tree']['op'] == 'species' and len(j['tree']['note']) == 280 and 'cut' in j['tree']['warning']
    j = c.post('/api/v1/flags', json={**T, 'reporter': 'n2', 'note': 'second'}).get_json(); assert j['tree']['note'] == 'second'
    # species report with "note": null from someone without a note: no 403, nothing cleared
    r = c.post('/api/v1/flags', json={**T, 'reporter': 'n3', 'species': 'Buche', 'note': None}); assert r.status_code == 201
    assert r.get_json()['tree']['note'] == 'second'
    # undo n2's note → n1's is the newest again; unnote; folded; history
    j = c.post('/api/v1/flags', json={**T, 'reporter': 'n2', 'retract': 'last'}).get_json()
    assert j['tree']['op'] == 'undo' and j['tree']['note'] == 'x' * 280
    r = c.post('/api/v1/flags', json={**T, 'note': None}); j = r.get_json()
    assert r.status_code == 201 and j['tree']['op'] == 'unnote' and 'note' not in j['tree'] and j['tree']['taxon'] == 'Fagus sylvatica', j
    r = c.post('/api/v1/flags', json={**T, 'note': None}); assert r.status_code == 200 and r.get_json()['folded']
    hi = c.get(f'/api/v1/trees/taxa/history?lon={C["lon"][A]}&lat={C["lat"][A]}').get_json()
    ns = [v.get('note', '-') for v in hi['versions']]
    assert ns == ['Naturdenkmal · Nr: 12', 'x' * 280, 'second', '-', '-', None], ns
    assert all('note' not in v for v in c.get(f'/api/v1/trees/taxa/history?lon={C["lon"][A]}&lat={C["lat"][A]}&notes=0').get_json()['versions'])
    # bbox blocks: only where a note exists; notes=0; dense field; json rows; added tree
    assert 'note' not in cols()['user']
    c.post('/api/v1/flags', json={**T, 'note': 'alt'})
    u = cols()['user']; assert u['note'] == ['alt'] and 'note' not in cols(notes=False)['user']
    ne_trees._pf_cache.clear()
    dn = ne_trees.columns_bbox(*BB, 'trees', min_height=3, fields=('x', 'note'))
    assert dn['columns']['note'][u['i'][0]] == 'alt' and sum(v is not None for v in dn['columns']['note']) == 1
    rows = ne_trees.points_bbox(*BB, 'trees', min_height=3, limit=20000)
    assert [t['note'] for t in rows['trees'] if 'note' in t] == ['alt']
    r = c.post('/api/v1/flags', json={'lon': BB[0] + 0.01234, 'lat': BB[1] + 0.01234, 'layer': 'trees', 'claim': 'missing',
                                      'client_id': 'abcdef12-0000', 'note': 'Jungbaum', 'reporter': 'n1', 'force': True})
    assert r.status_code == 201 and r.get_json()['tree']['op'] == 'add' and r.get_json()['tree']['note'] == 'Jungbaum', r.data
    j = c.post('/api/v1/flags', json={'client_id': 'abcdef12-0000', 'note': 'gepflanzt 2024', 'reporter': 'n1'}).get_json()
    assert j['tree']['op'] == 'note' and cols()['added']['note'] == ['gepflanzt 2024']
    li = c.get('/api/v1/trees/taxa').get_json()
    assert li['fields'][-1] == 'note' and sorted(v for v in li['columns']['note'] if v) == ['alt', 'gepflanzt 2024']
    # a tree with only a note, note undone → gone from the blocks
    c.post('/api/v1/flags', json={'client_id': 'abcdef12-0000', 'retract': True, 'reporter': 'n1'})
    assert 'added' not in cols()
    print('ok')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
