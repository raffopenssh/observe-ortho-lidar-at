"""tree_user: name a measured apex, add a missing tree, versions, sparse read-back
on /trees/bbox, delta feed, vocabulary.  Temp DB; apex matching needs the live NE corpus."""
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import public_flags as pf  # noqa: E402
import tree_user as tu  # noqa: E402
import cells_api  # noqa: E402
import ne_trees  # noqa: E402

BB = (10.075, 47.125, 10.1, 47.15)


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    pf.DB_PATH = tmp / 'f.sqlite'; pf.SALT_PATH = tmp / 'salt'
    pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear(); pf._daily.clear(); tu._state.clear()
    pf._post_limit = pf.sg.RateLimiter('t', 1000, 1000); pf._get_limit = pf.sg.RateLimiter('g', 1000, 1000)
    from flask import Flask
    app = Flask(__name__); app.register_blueprint(pf.flags_bp)
    c = app.test_client()
    # normalisation
    assert tu.normalise('stieleiche') == ('Quercus robur', True) and tu.normalise('Lärche')[0] == 'Larix decidua'
    assert tu.normalise('quercus  ROBUR')[0] == 'Quercus robur' and tu.normalise('Zelkova serrata') == ('Zelkova serrata', False)
    try:
        tu.normalise('Fichtte'); raise SystemExit('expected FlagError')
    except pf.FlagError as e:
        assert 'Picea abies' in e.extra['did_you_mean'], e.extra
    d = c.get('/api/v1/trees/taxa/dict').get_json()
    assert d['fields'] == ['taxon', 'de', 'en', 'class'] and len(d['taxa']) > 80
    assert c.get('/api/v1/trees/taxa').get_json()['count'] == 0
    assert c.post('/api/v1/flags', json={'cell': '8c1e108cc922dff', 'species': 'Fichte'}).status_code == 400   # needs a point
    live = ne_trees.status()['ok'] and ne_trees.bbox_state(*BB)['ready']
    if live:
        base = ne_trees.columns_bbox(*BB, 'trees', min_height=20, limit=50, fields=('lon', 'lat', 'h_m', 'x', 'y', 'kg_code'))
        assert 'user' not in base and 'added' not in base
        lon, lat = base['columns']['lon'][3], base['columns']['lat'][3]
        r = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'species': 'Fichte', 'reporter': 'u1', 'source': 't'})
        assert r.status_code == 201, r.data
        j = r.get_json()
        assert j['claim'] == 'species' and j['tree']['taxon'] == 'Picea abies' and j['tree']['v'] == 1 and not j['folded'], j
        assert j['tree']['class'] == 'spruce' and j['tree']['matched'] == 'apex' and j['epoch']
        # same reporter, same value → no new version; change of mind → v2; second reporter → v3, plurality
        assert c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'species': 'Picea abies', 'reporter': 'u1'}).get_json()['folded']
        j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'layer': 'trees', 'claim': 'species',
                                          'expected': {'species': 'Abies alba'}, 'reporter': 'u1'}).get_json()
        assert j['tree']['v'] == 2 and j['tree']['taxon'] == 'Abies alba'
        for u in ('u2', 'u3'):
            j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'species': 'spruce', 'reporter': u}).get_json()
        assert j['tree']['v'] == 4 and j['tree']['taxon'] == 'Picea' and j['tree']['n'] == 2, j
        # read-back: sparse block on columns, per-row keys on json, nothing elsewhere
        col = ne_trees.columns_bbox(*BB, 'trees', min_height=20, limit=50, fields=('lon', 'lat', 'taxon'))
        assert col['user'] == {'i': [3], 'taxon': ['Picea'], 'n': [2], 'v': [4]}, col.get('user')
        assert col['columns']['taxon'][3] == 'Picea' and col['columns']['taxon'][2] is None
        rows = ne_trees.points_bbox(*BB, 'trees', min_height=20, limit=50)
        assert rows['trees'][3]['taxon'] == 'Picea' and rows['trees'][3]['taxon_v'] == 4 and 'taxon' not in rows['trees'][2]
        assert 'user' not in ne_trees.columns_bbox(*BB, 'structures', limit=50)
        kg = base['columns']['kg_code'][3]
        assert tu.kgs_rev([kg]) == 4
        # history + moderation (append-only)
        h = c.get(f'/api/v1/trees/taxa/history?lon={lon}&lat={lat}').get_json()
        assert [v['op'] for v in h['versions']] == ['species'] * 4 and h['versions'][0]['epoch'] and 'reporter' not in h['versions'][0]
        assert c.post('/api/v1/trees/taxa/4/moderate').status_code == 404
        t = tu.moderate(4)
        assert t['v'] == 5 and t['n'] == 1
        # retract
        j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'claim': 'species', 'layer': 'trees', 'species': None, 'reporter': 'u2'}).get_json()
        assert j['tree']['taxon'] == 'Abies alba' and j['tree']['v'] == 6, j
        # measures: dbh + own height on the same tree (no species needed), median over reporters, versioned
        j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'dbh_cm': 62, 'reporter': 'u1'}).get_json()
        assert j['claim'] == 'species' and j['tree']['dbh_cm'] == 62.0 and j['tree']['v'] == 7 and j['tree']['taxon'] == 'Abies alba', j
        assert c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'dbh_cm': 62, 'reporter': 'u1'}).get_json()['folded']
        j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'layer': 'trees', 'claim': 'tree', 'h_m': 31.5, 'dbh_cm': 70,
                                          'reporter': 'u7'}).get_json()
        assert j['tree']['h_m'] == 31.5 and j['tree']['dbh_cm'] == 70.0 and j['tree']['v'] == 8 and j['tree']['height_m'], j
        assert c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'dbh_cm': 9000}).status_code == 400
        col = ne_trees.columns_bbox(*BB, 'trees', min_height=20, limit=50, fields=('lon', 'h_m', 'dbh_cm', 'h_user_m'))
        assert col['user']['dbh_cm'] == [70.0] and col['user']['h_user_m'] == [31.5] and col['columns']['dbh_cm'][3] == 70.0
        assert col['columns']['h_m'][3] == base['columns']['h_m'][3]          # LiDAR height untouched
        row = ne_trees.points_bbox(*BB, 'trees', min_height=20, limit=50)['trees'][3]
        assert row['dbh_cm'] == 70.0 and row['height_user_m'] == 31.5 and row['height_m'] == base['columns']['h_m'][3]
        h = c.get(f'/api/v1/trees/taxa/history?lon={lon}&lat={lat}').get_json()
        assert h['versions'][-1]['dbh_cm'] == 70.0 and h['versions'][-1]['h_m'] == 31.5
        # add: refused on top of an apex, accepted in a gap
        # add next to an apex: refused (409, no flag row) on an untouched apex; allowed with force or when the
        # apex already carries a report (second stem / understorey)
        lon5, lat5 = base['columns']['lon'][5], base['columns']['lat'][5]
        n_flags = pf._conn().execute('SELECT COUNT(*) FROM report').fetchone()[0]
        r = c.post('/api/v1/flags', json={'lon': lon5, 'lat': lat5, 'layer': 'trees', 'claim': 'missing', 'reporter': 'u1'})
        assert r.status_code == 409 and r.get_json()['tree']['dist_m'] <= 2 and 'already' in r.get_json()['error'], r.data
        assert pf._conn().execute('SELECT COUNT(*) FROM report').fetchone()[0] == n_flags
        r = c.post('/api/v1/flags', json={'lon': lon5, 'lat': lat5, 'layer': 'trees', 'claim': 'missing', 'reporter': 'u1',
                                          'force': True, 'dbh_cm': 9, 'client_id': '0b9d0e86-1111-4222-8333-444455556666'})
        j = r.get_json()
        assert r.status_code == 201 and j['tree']['added'] and j['tree']['client_id'].startswith('0b9d0e86') and not j['tree']['folded'], j
        r = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'layer': 'trees', 'claim': 'missing', 'reporter': 'u1'})
        assert r.status_code == 201 and r.get_json()['tree']['added'], r.data      # apex carries measures → allowed
        # a name call next to both still means the measured apex, never the added tree
        j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'dbh_cm': 63, 'reporter': 'u1'}).get_json()
        assert j['tree']['matched'] == 'apex' and not j['tree']['added'] and j['tree']['dbh_cm'], j
        # retract my added tree by client_id alone (no point): row drops out of the delta feed, no flag row
        rev0 = tu.global_rev(); n_flags = pf._conn().execute('SELECT COUNT(*) FROM report').fetchone()[0]
        r = c.post('/api/v1/flags', json={'client_id': '0b9d0e86-1111-4222-8333-444455556666', 'layer': 'trees',
                                          'claim': 'missing', 'retract': True, 'reporter': 'u1'})
        j = r.get_json()
        assert r.status_code == 201 and j['tree']['op'] == 'retract' and not j['tree']['added'], j
        assert pf._conn().execute('SELECT COUNT(*) FROM report').fetchone()[0] == n_flags
        L = c.get(f'/api/v1/trees/taxa?since={rev0}').get_json()
        assert L['count'] == 1 and L['columns']['added'] == [0] and L['columns']['dbh_cm'] == [None], L
        # --- identity: tree_id binds exactly; a bare point must be unambiguous
        x6, y6 = base['columns']['x'][6], base['columns']['y'][6]
        tid = f't_{x6 * 10 + 5}_{y6 * 10 + 5}'                    # product apex whose metre snap is corpus row 6 (or a neighbour)
        r = c.post('/api/v1/flags', json={'tree_id': tid, 'dbh_cm': 41.5, 'reporter': 'w1', 'measured_at': '2026-11-11T08:00:00Z',
                                          'dbh_method': 'Kluppe', 'h_method': 'geschätzt', 'vitality': 'lebend',
                                          'status': 'standing', 'source': 'waldmanager/kluppe'})
        j = r.get_json()
        assert r.status_code == 201 and j['tree']['tree_id'] == tid and j['tree']['matched'] == 'apex', j
        assert j['tree']['dist_m'] == 0 and j['tree']['matched_by'] == 'tree_id' and j['tree']['dbh_cm'] == 41.5
        r = c.post('/api/v1/flags', json={'tree_id': tid, 'dbh_cm': 41.5, 'reporter': 'w1', 'measured_at': '2026-11-11T08:00:00Z',
                                          'dbh_method': 'caliper'})
        assert r.status_code == 200 and r.get_json()['folded'] and r.get_json()['tree']['v'] == 1          # idempotent
        h = c.get(f'/api/v1/trees/taxa/history?tree_id={tid}').get_json()
        v = h['versions'][0]
        assert (v['measured_at'], v['dbh_method'], v['h_method'], v['vitality'], v['status'], v['tree_id']) == \
            ('2026-11-11T08:00:00Z', 'caliper', 'estimate', 'alive', 'standing', tid), v
        j = c.post('/api/v1/flags', json={'tree_id': tid, 'dbh_cm': 42, 'dbh_method': 'schieblehre'}).get_json()
        assert 'dbh_method' in j['tree']['warning'] and j['tree']['dbh_cm'] in (41.5, 42.0), j     # unknown value → warning
        col = ne_trees.columns_bbox(*BB, 'trees', min_height=20, limit=50, fields=('lon', 'tree_id'))
        assert tid in col['user']['tree_id'] and tid in col['columns']['tree_id']
        assert c.post('/api/v1/flags', json={'tree_id': 't_1_2', 'dbh_cm': 4}).status_code == 400
        # an inventory tree the corpus has no apex for (a spot in the lake of trees: 30 m off) keeps its own record
        ghost = f't_{(x6 + 3) * 10 + 5}_{(y6 + 2) * 10 + 5}'
        near = tu._apices_near(*tu._tf('EPSG:3035', 'EPSG:4326').transform(x6 + 3.5, y6 + 2.5), 3.0)
        if near is None or not (((abs(near['x'] - (x6 + 3.5)) <= 0.5) & (abs(near['y'] - (y6 + 2.5)) <= 0.5)).any()):
            j = c.post('/api/v1/flags', json={'tree_id': ghost, 'dbh_cm': 30, 'reporter': 'w1'}).get_json()
            assert j['tree']['tree_id'] == ghost and j['tree']['matched'] == 'id' and j['tree']['v'] == 1 and 'warning' in j['tree'], j
            j = c.post('/api/v1/flags', json={'tree_id': ghost, 'dbh_cm': 31, 'reporter': 'w1'}).get_json()
            assert j['tree']['tree_id'] == ghost and j['tree']['v'] == 2 and j['tree']['dbh_cm'] == 31.0, j
            L = c.get(f'/api/v1/trees/taxa?kg={j["subject"]["kg"]}').get_json()
            assert ghost in L['columns']['tree_id'], L
            rp = ne_trees.points_bbox(*BB, 'trees', limit=5).get('reported') or []
            assert any(t['tree_id'] == ghost and t['dbh_cm'] == 31.0 for t in rp), rp
        # ambiguous point: midway between two apexes that are both in reach → 409 + candidates; far away → 404
        import numpy as np
        allc = ne_trees.collect(*BB, 'trees', realistic=False, limit=20000)['cols']
        pair = None
        for i in range(min(len(allc['x']), 3000)):
            d = np.hypot(allc['x'] - allc['x'][i], allc['y'] - allc['y'][i]); d[i] = 1e9
            k = int(np.argmin(d))
            if 1.5 < d[k] <= 2.9:
                pair = (i, k); break
        if pair:
            mx = (allc['x'][pair[0]] + allc['x'][pair[1]]) / 2.0 + 0.1; my = (allc['y'][pair[0]] + allc['y'][pair[1]]) / 2.0
            mlon, mlat = tu._tf('EPSG:3035', 'EPSG:4326').transform(mx, my)
            r = c.post('/api/v1/flags', json={'lon': mlon, 'lat': mlat, 'dbh_cm': 20})
            assert r.status_code == 409 and len(r.get_json()['candidates']) >= 2 and 'dist_m' in r.get_json()['candidates'][0], r.data
        assert c.post('/api/v1/flags', json={'lon': 10.0, 'lat': 46.5, 'species': 'Fichte'}).status_code == 404
    # add a tree where we have none (works without the corpus as long as the KG resolves)
    alon, alat = 10.0871234, 47.1371234
    if pf._kg_for_point(alon, alat):
        import numpy as np
        near = tu._apices_near(alon, alat)
        if near is not None:     # walk off any measured apex
            px, py = tu._to_3035(alon, alat)
            while near is not None and np.hypot(near['x'] - px, near['y'] - py).min() <= 12.0:
                alon += 0.0002; px, py = tu._to_3035(alon, alat); near = tu._apices_near(alon, alat)
        j = c.post('/api/v1/flags', json={'lon': alon, 'lat': alat, 'layer': 'trees', 'claim': 'missing',
                                          'expected': {'species': 'Walnuss', 'h_m': 6}, 'reporter': 'u1'}).get_json()
        assert j['id'] and j['tree']['added'] and j['tree']['taxon'] == 'Juglans regia' and j['tree']['h_m'] == 6.0, j
        v0 = j['tree']['v']
        j2 = c.post('/api/v1/flags', json={'lon': alon, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'reporter': 'u1'}).get_json()
        assert j2['tree']['folded'] and j2['tree']['v'] == v0
        # two stems 0.8 m apart with different dbh stay two trees; an identical repeat folds; client_id is the identity
        blon = alon + 0.0004; dl = 0.8 / 76000.0
        ja = c.post('/api/v1/flags', json={'lon': blon, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'dbh_cm': 20, 'reporter': 'u1'}).get_json()
        jb = c.post('/api/v1/flags', json={'lon': blon + dl, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'dbh_cm': 25, 'reporter': 'u1'}).get_json()
        assert not ja['tree']['folded'] and not jb['tree']['folded'] and jb['tree']['dbh_cm'] == 25.0 and jb['tree']['v'] == 1, (ja, jb)
        assert abs(ja['tree']['lon'] - jb['tree']['lon']) > 5e-6
        jr = c.post('/api/v1/flags', json={'lon': blon + dl, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'dbh_cm': 25, 'reporter': 'u1'}).get_json()
        assert jr['tree']['folded'] and not jr['tree']['written'] and jr['tree']['v'] == 1, jr
        jc = c.post('/api/v1/flags', json={'lon': blon + dl, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'dbh_cm': 25, 'reporter': 'u2'}).get_json()
        assert not jc['tree']['folded'] and jc['tree']['v'] == 1, jc                          # another reporter → own tree
        cid = 'a3f1c2d4-0000-4000-8000-00000000abcd'
        j1 = c.post('/api/v1/flags', json={'lon': blon + 3 * dl, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'dbh_cm': 11,
                                           'client_id': cid, 'reporter': 'u1'}).get_json()
        j2_ = c.post('/api/v1/flags', json={'lon': blon + 9 * dl, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'dbh_cm': 12,
                                            'client_id': cid, 'reporter': 'u1'}).get_json()          # same id, moved point, new dbh
        assert j1['tree']['client_id'] == cid and j2_['tree']['client_id'] == cid and j2_['tree']['v'] == 2 and j2_['tree']['folded']
        assert j2_['tree']['dbh_cm'] == 12.0 and j2_['tree']['lon'] == j1['tree']['lon']
        j3_ = c.post('/api/v1/flags', json={'client_id': cid, 'dbh_cm': 13, 'reporter': 'u1'}).get_json()   # measure by client_id
        assert j3_['claim'] == 'species' and j3_['tree']['matched_by'] == 'client_id' and j3_['tree']['dbh_cm'] == 13.0, j3_
        # species:null on a bare added tree withdraws it (no stray flag row of layer "other")
        nfl = pf._conn().execute('SELECT COUNT(*) FROM report').fetchone()[0]
        r = c.post('/api/v1/flags', json={'lon': blon + dl, 'lat': alat, 'species': None, 'reporter': 'u2'})
        assert r.get_json()['tree']['op'] == 'retract' and not r.get_json()['tree']['added'], r.data
        assert pf._conn().execute('SELECT COUNT(*) FROM report').fetchone()[0] == nfl
        # reporter keys: identified caller, shown as `by` in the history; a wrong key is 401
        pf.KEYS_PATH = tmp / 'keys.json'; pf.KEYS_PATH.write_text('{"trk_test_1": "forestry"}')
        assert c.post('/api/v1/flags', json={'client_id': cid, 'dbh_cm': 14, 'reporter_key': 'nope'}).status_code == 401
        r = c.post('/api/v1/flags', json={'reports': [{'client_id': cid, 'dbh_cm': 14, 'reporter': 'u1'},
                                                       {'client_id': 'ffffffff-0000-4000-8000-000000000000', 'dbh_cm': 1}]},
                   headers={'X-Reporter-Key': 'trk_test_1'})
        res = r.get_json()['results']
        assert [x['http'] for x in res] == [201, 404] and r.get_json()['errors'] == 1, res
        hv = c.get(f'/api/v1/trees/taxa/history?client_id={cid}').get_json()['versions']
        assert hv[-1]['by'] == 'forestry' and 'by' not in hv[0], hv
        # anyone can get a key; self-issued holders are marked
        r = c.post('/api/v1/flags/token', json={'name': 'Wald App'})
        tk = r.get_json()
        assert r.status_code == 201 and tk['reporter_key'].startswith('trk_') and tk['holder'].startswith('Wald App~'), tk
        assert c.post('/api/v1/flags/token', json={'name': '<x>'}).status_code == 400
        r = c.post('/api/v1/flags', json={'client_id': cid, 'dbh_cm': 15, 'reporter': 'u1'}, headers={'Authorization': 'Bearer ' + tk['reporter_key']})
        assert r.status_code == 201, r.data
        assert c.get(f'/api/v1/trees/taxa/history?client_id={cid}').get_json()['versions'][-1]['by'] == tk['holder']
        assert pf._key_cap(tk['holder']) == pf.DAILY_CAP_SELF and pf._key_cap('forestry') == pf.DAILY_CAP_KEYED
        # someone else names the added tree 1 m away
        j3 = c.post('/api/v1/flags', json={'lon': alon + 0.00001, 'lat': alat, 'species': 'Juglans regia', 'reporter': 'u9'}).get_json()
        assert j3['tree']['matched'] == 'added' and j3['tree']['n'] == 2 and j3['tree']['v'] == v0 + 1, j3
        j4 = c.post('/api/v1/flags', json={'lon': alon, 'lat': alat, 'h_m': 7, 'dbh_cm': 18.5, 'layer': 'trees', 'claim': 'tree',
                                           'reporter': 'u1'}).get_json()          # update my height + dbh on the added tree
        assert j4['tree']['h_m'] == 7.0 and j4['tree']['dbh_cm'] == 18.5 and j4['tree']['v'] == v0 + 2, j4
        kgs = [pf._kg_for_point(alon, alat)]
        a = tu.added(alon - 0.0001, alat - 0.0001, alon + 0.0001, alat + 0.0001, kgs, 3, 8)
        assert a and a['taxon'] == ['Juglans regia'] and a['h_m'] == [7.0] and a['dbh_cm'] == [18.5]
        assert tu.added(alon - 0.0001, alat - 0.0001, alon + 0.0001, alat + 0.0001, kgs, 8, 14) is None
        lst = c.get(f'/api/v1/trees/taxa?kg={kgs[0]}')
        L = lst.get_json()
        assert L['count'] >= 1 and 1 in L['columns']['added'] and L['rev'] == tu.global_rev()
        assert c.get(f'/api/v1/trees/taxa?kg={kgs[0]}', headers={'If-None-Match': lst.headers['ETag']}).status_code == 304
        assert c.get(f'/api/v1/trees/taxa?since={L["rev"]}').get_json()['count'] == 0
        assert c.get(f'/api/v1/trees/taxa?since={L["rev"] - 1}').get_json()['count'] == 1
        st = c.get('/api/v1/flags/stats').get_json()
        assert st['trees']['versions'] == tu.global_rev() and st['trees']['add_versions'] >= 1
    # vocabulary
    import public_docs
    src = (ROOT / 'tree_user.py').read_text()
    for w in cells_api.FORBIDDEN_WORDS:
        for text, nm in ((src, 'tree_user.py'), (public_docs.FLAGS_TXT, 'FLAGS_TXT')):
            assert not re.search(r'(?<![\w])' + re.escape(w) + r'(?![\w])', text, re.I), (w, nm)
    print(f'ok (corpus {"live" if live else "absent"})')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
