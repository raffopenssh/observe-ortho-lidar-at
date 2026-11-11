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
        # add: refused on top of an apex, accepted in a gap
        j = c.post('/api/v1/flags', json={'lon': lon, 'lat': lat, 'layer': 'trees', 'claim': 'missing', 'reporter': 'u1'}).get_json()
        assert j['id'] and 'already' in j['tree']['error'], j
        assert c.post('/api/v1/flags', json={'lon': 10.0, 'lat': 46.5, 'species': 'Fichte'}).status_code == 404
    # add a tree where we have none (works without the corpus as long as the KG resolves)
    alon, alat = 10.0871234, 47.1371234
    if pf._kg_for_point(alon, alat):
        import numpy as np
        near = tu._apices_near(alon, alat)
        if near is not None:     # walk off any measured apex
            px, py = tu._to_3035(alon, alat)
            while near is not None and np.hypot(near['x'] - px, near['y'] - py).min() <= tu.MATCH_MAX_M:
                alon += 0.0002; px, py = tu._to_3035(alon, alat); near = tu._apices_near(alon, alat)
        j = c.post('/api/v1/flags', json={'lon': alon, 'lat': alat, 'layer': 'trees', 'claim': 'missing',
                                          'expected': {'species': 'Walnuss', 'h_m': 6}, 'reporter': 'u1'}).get_json()
        assert j['id'] and j['tree']['added'] and j['tree']['taxon'] == 'Juglans regia' and j['tree']['h_m'] == 6.0, j
        v0 = j['tree']['v']
        j2 = c.post('/api/v1/flags', json={'lon': alon, 'lat': alat, 'layer': 'trees', 'claim': 'missing', 'reporter': 'u1'}).get_json()
        assert j2['tree']['folded'] and j2['tree']['v'] == v0
        # someone else names the added tree 1 m away
        j3 = c.post('/api/v1/flags', json={'lon': alon + 0.00001, 'lat': alat, 'species': 'Juglans regia', 'reporter': 'u9'}).get_json()
        assert j3['tree']['matched'] == 'added' and j3['tree']['n'] == 2 and j3['tree']['v'] == v0 + 1, j3
        kgs = [pf._kg_for_point(alon, alat)]
        a = tu.added(alon - 0.001, alat - 0.001, alon + 0.001, alat + 0.001, kgs, 3, 8)
        assert a and a['taxon'] == ['Juglans regia'] and a['h_m'] == [6.0]
        assert tu.added(alon - 0.001, alat - 0.001, alon + 0.001, alat + 0.001, kgs, 8, 14) is None
        lst = c.get(f'/api/v1/trees/taxa?kg={kgs[0]}')
        L = lst.get_json()
        assert L['count'] >= 1 and 1 in L['columns']['added'] and L['rev'] == tu.global_rev()
        assert c.get(f'/api/v1/trees/taxa?kg={kgs[0]}', headers={'If-None-Match': lst.headers['ETag']}).status_code == 304
        assert c.get(f'/api/v1/trees/taxa?since={L["rev"]}').get_json()['count'] == 0
        assert c.get(f'/api/v1/trees/taxa?since={L["rev"] - 1}').get_json()['count'] == 1
        st = c.get('/api/v1/flags/stats').get_json()
        assert st['trees']['versions'] == tu.global_rev() and st['trees']['add_versions'] == 1
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
