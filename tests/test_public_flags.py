"""public_flags: subject resolution, folding, snapshot, reads, limits, vocabulary.
Runs against a temp DB; uses a served cell from data/ne when available."""
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import public_flags as pf  # noqa: E402
import cells_api  # noqa: E402


def _app():
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(pf.flags_bp)
    app.register_blueprint(cells_api.cells_bp)
    return app.test_client()


def _served_cell():
    files = sorted(cells_api.NE_DIR.glob('*.h3b'))
    if not files or not cells_api.INDEX_DB.exists():
        return None
    import numpy as np
    h = cells_api.header(files[0].stem, with_chunks=True)
    ch = next((c for c in h['chunks'] if c.get('cells')), None)
    if not ch:
        return None
    hn = cells_api._h3n()
    kids = np.asarray(hn.cell_to_children(int(ch['r10'], 16) if isinstance(ch['r10'], str) else ch['r10'], 12), dtype=np.uint64)
    t, _, _ = cells_api.select(kids, None)
    return f'{int(t.ids[0]):x}' if len(t) else None


def test_roundtrip(tmp_path=None):
    tmp = Path(tempfile.mkdtemp())
    pf.DB_PATH = tmp / 'f.sqlite'; pf.SALT_PATH = tmp / 'salt'
    pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear(); pf._daily.clear(); pf._heat_cache.clear()
    pf._post_limit = pf.sg.RateLimiter('t', 1000, 1000)
    c = _app()
    cell = _served_cell()
    # 1. point subject + fold
    r = c.post('/api/v1/flags', json={'lon': 15.0857, 'lat': 47.0665, 'layer': 'cells.cover', 'claim': 'wrong_type',
                                      'expected': {'group': 'wald'}, 'note': 'this is forest', 'source': 'test',
                                      'reporter': 'u1', 'bogus': 1})
    assert r.status_code == 201, r.data
    d = r.get_json()
    assert d['subject']['cell'] and d['folded'] is False and d['tally']['n'] == 1 and d['warnings']
    r2 = c.post('/api/v1/flags', json={'cell': d['subject']['cell'], 'layer': 'cells.cover', 'claim': 'wrong_type', 'reporter': 'u1'})
    assert r2.get_json()['folded'] is True and r2.get_json()['n'] == 2 and r2.get_json()['id'] == d['id']
    # another reporter → new row, same subject
    r3 = c.post('/api/v1/flags', json={'cell': d['subject']['cell'], 'layer': 'cells.cover', 'claim': 'looks_right', 'reporter': 'u2'})
    assert r3.get_json()['tally'] == {'n': 3, 'reporters': 2, 'open': 2, 'claims': {'wrong_type': 2, 'looks_right': 1}}
    # 2. kg subject, bad inputs
    assert c.post('/api/v1/flags', json={'kg': '63349', 'layer': 'kg', 'claim': 'artifact'}).status_code == 201
    assert c.post('/api/v1/flags', json={'lon': 1, 'lat': 1}).status_code == 400
    assert c.post('/api/v1/flags', json={'cell': 'zz'}).status_code == 400
    assert c.post('/api/v1/flags', json={'cell': d['subject']['cell'], 'layer': 'nope'}).status_code == 400
    assert c.post('/api/v1/flags', json={'cell': d['subject']['cell'], 'observed': {'x': 'y' * 600}}).status_code == 413
    # 3. snapshot on a served cell
    if cell:
        r = c.post('/api/v1/flags', json={'cell': cell, 'layer': 'cells.trees', 'claim': 'too_tall', 'reporter': 'u3'})
        d3 = r.get_json()
        assert d3['served'] is True and 'snapshot' in d3 and d3['epoch'], d3
        one = c.get(f'/api/v1/cells/{cell}?flags=1').get_json()
        assert one['flags']['n'] == 1
        assert c.get(f'/api/v1/cells/{cell}?flags=1').headers['Cache-Control'] == 'public, max-age=60'
    # 4. reads
    g = c.get('/api/v1/flags?lon=15.0857&lat=47.0665&k=1').get_json()
    assert g['count'] == 1 and g['subjects'][0]['claims'] == {'wrong_type': 2, 'looks_right': 1}
    assert g['subjects'][0]['expected'] == {'wrong_type': {'group': 'wald'}}
    g = c.get('/api/v1/flags?bbox=15.08,47.06,15.09,47.07&group=report&claim=wrong_type').get_json()
    assert g['count'] == 1 and g['reports'][0]['note'] == 'this is forest' and 'reporter' not in g['reports'][0]
    g = c.get('/api/v1/flags?kg=63349&format=columns').get_json()
    assert g['count'] == 1 and g['cells'] == [None]
    assert c.get('/api/v1/flags?bbox=15,47,15.2,47.2').status_code == 413
    assert c.get('/api/v1/flags').status_code == 400
    h = c.get('/api/v1/flags/heat?res=7').get_json()
    assert sum(h['n']) >= 3 and h['cells']
    st = c.get('/api/v1/flags/stats').get_json()
    assert st['by']['claim']['wrong_type'] == 2 and st['subjects'] >= 2
    one = c.get(f'/api/v1/flags/{d["id"]}')
    assert one.status_code == 200 and one.headers.get('ETag')
    assert c.get(f'/api/v1/flags/{d["id"]}', headers={'If-None-Match': one.headers['ETag']}).status_code == 304
    # 5. status needs a token
    assert c.post(f'/api/v1/flags/{d["id"]}/status', json={'status': 'fixed'}).status_code == 404
    # 6. batch + rate limit
    pf._post_limit = pf.sg.RateLimiter('t2', 1.0, 1.0)
    assert c.post('/api/v1/flags', json={'kg': '63349', 'claim': 'stale'}).status_code == 201
    assert c.post('/api/v1/flags', json={'kg': '63349', 'claim': 'stale'}).status_code == 429
    pf._post_limit = pf.sg.RateLimiter('t3', 1000, 1000)
    b = c.post('/api/v1/flags', json={'reports': [{'kg': '63349', 'claim': 'stale'}, {'cell': 'zz'}]}).get_json()
    assert b['count'] == 2 and b['errors'] == 1
    # 6b. withdraw: only the reporter's own row, by cell / point / kg, single + batch
    P = {'lon': 14.301053, 'lat': 47.600903, 'layer': 'trees'}
    for cl in ('too_tall', 'too_short', 'spurious', 'misplaced', 'looks_right'):
        a1 = c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'a'}).get_json()
        assert c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'a'}).get_json()['n'] == 2
        c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'b'})
        e0 = c.get('/api/v1/flags?lon=14.301053&lat=47.600903').headers['ETag']
        r = c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'zz', 'retract': True})
        assert r.status_code == 403 and r.get_json()['reason'] == 'not_yours', r.data
        r = c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'a', 'retract': True})
        j = r.get_json()
        assert r.status_code == 201 and j['retracted'] and j['id'] == a1['id'] and j['tally']['claims'][cl] == 1, j
        assert c.get('/api/v1/flags?lon=14.301053&lat=47.600903').headers['ETag'] != e0
        assert c.get(f'/api/v1/flags/{a1["id"]}').status_code == 404
        r = c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'a', 'retract': True})
        assert r.status_code == 200 and r.get_json()['folded'] and r.get_json()['retracted'], r.data
        r = c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'b', 'retract': 'true'})
        assert r.status_code == 201 and cl not in r.get_json()['tally']['claims'], r.data
        assert c.post('/api/v1/flags', json={**P, 'claim': cl, 'reporter': 'zz', 'retract': True}).status_code == 404
    assert c.post('/api/v1/flags', json={**P, 'claim': 'looks_right', 'reporter': 'b', 'retract': True}).get_json()['tally'] == \
        {'n': 0, 'reporters': 0, 'open': 0, 'claims': {}}
    # re-flag after a withdraw starts at 1 again; withdrawing again is a real withdraw (201)
    assert c.post('/api/v1/flags', json={**P, 'claim': 'looks_right', 'reporter': 'a'}).get_json()['n'] == 1
    h1 = sum(c.get('/api/v1/flags/heat?res=7').get_json()['n'])
    b = c.post('/api/v1/flags', json={'reports': [
        {**P, 'claim': 'looks_right', 'reporter': 'a', 'retract': True},
        {**P, 'claim': 'looks_right', 'reporter': 'a', 'retract': True},
        {**P, 'claim': 'looks_right', 'reporter': 'q', 'retract': True},
        {'kg': '63349', 'claim': 'stale', 'retract': True},
        {'cell': d['subject']['cell'], 'layer': 'cells.cover', 'claim': 'looks_right', 'reporter': 'u1', 'retract': True},
        {'cell': d['subject']['cell'], 'layer': 'cells.cover', 'claim': 'looks_right', 'reporter': 'u2', 'retract': True},
    ]}).get_json()
    assert [x['http'] for x in b['results']] == [201, 200, 404, 201, 403, 201], b
    assert b['results'][5]['tally']['claims'] == {'wrong_type': 2} and b['results'][4]['reason'] == 'not_yours'
    assert sum(c.get('/api/v1/flags/heat?res=7').get_json()['n']) == h1 - 2   # kg subjects are not in the heat
    # 7. vocabulary (same rule as the cells API)
    import re
    src = (ROOT / 'public_flags.py').read_text()
    import public_docs
    for w in cells_api.FORBIDDEN_WORDS:
        for text, nm in ((src, 'public_flags.py'), (public_docs.FLAGS_TXT, 'FLAGS_TXT')):
            assert not re.search(r'(?<![\w])' + re.escape(w) + r'(?![\w])', text, re.I), (w, nm)
    print('ok')


if __name__ == '__main__':
    test_roundtrip()
