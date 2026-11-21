"""Position frames: a client that downloaded trees before a KG's position fix
(ne_pos_fix) keeps reporting with the coordinates / ids it has — the report
must land on the same tree, nothing dropped.  Temp DB + temp fix state; needs
the live NE corpus."""
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
tmp = Path(tempfile.mkdtemp())
os.environ['NE_POS_FIX'] = str(tmp / 'pos.json')

import numpy as np  # noqa: E402
import ne_pos_fix  # noqa: E402
import public_flags as pf  # noqa: E402
import tree_user as tu  # noqa: E402
import ne_trees  # noqa: E402

BB = (10.075, 47.125, 10.1, 47.15)
OFF = (-5, 5)


def main() -> int:
    pf.DB_PATH = tmp / 'f.sqlite'; pf.SALT_PATH = tmp / 'salt'
    pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear(); pf._daily.clear(); tu._state.clear()
    pf._post_limit = pf.sg.RateLimiter('t', 1000, 1000); pf._get_limit = pf.sg.RateLimiter('g', 1000, 1000)
    from flask import Flask
    app = Flask(__name__); app.register_blueprint(pf.flags_bp)
    c = app.test_client()
    if not (ne_trees.status()['ok'] and ne_trees.bbox_state(*BB)['ready']):
        print('SKIP: NE corpus not ready'); return 0
    F = ('lon', 'lat', 'h_m', 'x', 'y', 'kg_code')
    old = ne_trees.columns_bbox(*BB, 'trees', min_height=18, limit=3000, fields=F)   # the client's download
    assert 'frames' not in old
    oc = old['columns']; kg = max(set(oc['kg_code']), key=oc['kg_code'].count)
    # a report made BEFORE the fix (must follow the apex)
    i0 = next(i for i in range(old['n']) if oc['kg_code'][i] == kg)
    r = c.post('/api/v1/flags', json={'lon': oc['lon'][i0], 'lat': oc['lat'][i0], 'species': 'Fichte'}).get_json()
    assert r['tree']['taxon'] == 'Picea abies' and 'frame' not in r['tree'], r
    con = pf._conn()
    kx, ky = con.execute('SELECT x, y FROM tree_ver').fetchone()[:2]
    # feedback made before the fix: "not a tree" on one apex, a tree the map misses, a tree somebody added
    i1 = next(i for i in range(old['n']) if oc['kg_code'][i] == kg and i != i0)
    assert c.post('/api/v1/flags', json={'layer': 'trees', 'claim': 'spurious', 'lon': oc['lon'][i1], 'lat': oc['lat'][i1]}).status_code in (200, 201)
    add = c.post('/api/v1/flags', json={'layer': 'trees', 'claim': 'missing', 'lon': oc['lon'][i0] + 0.0002, 'lat': oc['lat'][i0],
                                        'species': 'Eiche', 'force': True}).get_json()['tree']
    assert add['added'], add
    # ── the fix ──
    (tmp / 'pos.json').write_text(json.dumps({'kgs': {kg: {'verdict': 'shift', 'off': list(OFF), 'ts': int(time.time()) - 100, 'windows': []},
                                                     '00001': {'verdict': 'shift', 'off': list(OFF), 'ts': 5, 'windows': []}}}))
    mg = ne_pos_fix._migrate_stores(kg, OFF, db=pf.DB_PATH)
    assert mg['tree_ver'] == 1 and mg['flags'] == 1, mg         # the added tree and its 'missing' flag stay where the user put them
    tu._state.clear(); ne_trees._pf_cache.clear(); ne_trees._fr_cache.clear()
    assert ne_pos_fix.frames(kg)['frame'] == 'p-5+5'
    new = ne_trees.columns_bbox(*BB, 'trees', min_height=10, limit=200000, fields=F + ('taxon',))
    assert new['frames'] == {kg: 'p-5+5'}, new.get('frames')
    nc = new['columns']
    cur = {(x, y): j for j, (x, y) in enumerate(zip(nc['x'], nc['y']))}
    j0 = cur[(kx + OFF[0], ky + OFF[1])]
    assert nc['taxon'][j0] == 'Picea abies'                      # nothing dropped: the old report sits on the moved apex
    occ = np.array(list(cur))
    assert (oc['x'][i1] + OFF[0], oc['y'][i1] + OFF[1]) not in cur          # the flagged apex is still hidden after the move
    raw = ne_trees.columns_bbox(*BB, 'trees', min_height=10, limit=200000, fields=F, realistic=False)['columns']
    assert (oc['x'][i1] + OFF[0], oc['y'][i1] + OFF[1]) in set(zip(raw['x'], raw['y']))
    a2 = (tu.added(*BB, new['kgs'], 0) or {}).get('lon') or []   # (no height given → 3 m band)
    assert any(abs(a - add['lon']) < 1e-6 for a in a2), (a2, add)
    # a stale client flags another tree as spurious after the fix → lands on the moved apex
    i2 = next(i for i in range(old['n']) if oc['kg_code'][i] == kg and i not in (i0, i1)
              and not ((np.abs(occ[:, 0] - oc['x'][i]) <= 2) & (np.abs(occ[:, 1] - oc['y'][i]) <= 2)).any())
    r = c.post('/api/v1/flags', json={'layer': 'trees', 'claim': 'spurious', 'lon': oc['lon'][i2], 'lat': oc['lat'][i2]}).get_json()
    sx, sy = tu._to_3035(r['subject']['lon'], r['subject']['lat'])
    assert abs(sx - (oc['x'][i2] + OFF[0])) < 1 and abs(sy - (oc['y'][i2] + OFF[1])) < 1 and r.get('frame', {}).get('moved_m') == list(OFF), r

    def lonely(x, y):                                            # no current apex near the stale coordinate
        return not ((np.abs(occ[:, 0] - x) <= 2) & (np.abs(occ[:, 1] - y) <= 2)).any()
    mine = [i for i in range(old['n']) if oc['kg_code'][i] == kg and i not in (i0, i1, i2)]
    clean = [i for i in mine if lonely(oc['x'][i], oc['y'][i])][:6]
    assert len(clean) >= 5, len(clean)
    # 1) stale lon/lat, no hint at all → evidence
    i = clean[0]
    r = c.post('/api/v1/flags', json={'lon': oc['lon'][i], 'lat': oc['lat'][i], 'species': 'Tanne', 'h_m': 21}).get_json()
    t = r['tree']; j = cur[(oc['x'][i] + OFF[0], oc['y'][i] + OFF[1])]
    assert t['frame']['by'] == 'exact' and t['frame']['moved_m'] == list(OFF) and t['frame']['sent'] == 'p0', t
    assert abs(t['lon'] - nc['lon'][j]) < 2e-6 and abs(t['lat'] - nc['lat'][j]) < 2e-6 and 'refresh' in t['warning'], t
    # 2) stale tree_id (v3 inventory id of the old copy) → same apex, id re-keyed
    i = clean[1]; tid = f"t_{oc['x'][i] * 10 + 5}_{oc['y'][i] * 10 + 5}"
    r = c.post('/api/v1/flags', json={'tree_id': tid, 'dbh_cm': 40}).get_json(); t = r['tree']
    want = f"t_{(oc['x'][i] + OFF[0]) * 10 + 5}_{(oc['y'][i] + OFF[1]) * 10 + 5}"
    assert t['matched'] == 'apex' and t['tree_id'] == want and t['frame']['tree_id'] == want and t['frame']['by'] == 'exact', t
    # … again with the stale id: bound, not a second tree; the refreshed id names the same tree
    r2 = c.post('/api/v1/flags', json={'tree_id': tid, 'species': 'Buche'}).get_json()['tree']
    r3 = c.post('/api/v1/flags', json={'tree_id': want, 'h_m': 30}).get_json()['tree']
    assert r2['tree_id'] == want and r3['tree_id'] == want and r3['taxon'] == 'Fagus sylvatica' and 'moved_m' not in r3['frame'], (r2, r3)
    # 3) a refreshed client: current coordinate → untouched
    j = next(j for j in range(new['n']) if nc['kg_code'][j] == kg and lonely(nc['x'][j] - OFF[0] + 0, nc['y'][j] - OFF[1])
             and (nc['x'][j] - OFF[0], nc['y'][j] - OFF[1]) not in cur and j != j0)
    r = c.post('/api/v1/flags', json={'lon': nc['lon'][j], 'lat': nc['lat'][j], 'species': 'Eiche'}).get_json()['tree']
    assert 'moved_m' not in r['frame'] and r['frame']['sent'] == 'p-5+5' and abs(r['lon'] - nc['lon'][j]) < 2e-6, r
    # 4) explicit echo wins over evidence; as_of = download time
    i = clean[2]
    r = c.post('/api/v1/flags', json={'lon': oc['lon'][i], 'lat': oc['lat'][i], 'species': 'Kiefer', 'frame': 'p0'}).get_json()['tree']
    assert r['frame']['by'] == 'frame' and r['frame']['moved_m'] == list(OFF)
    i = clean[3]
    r = c.post('/api/v1/flags', json={'lon': oc['lon'][i], 'lat': oc['lat'][i], 'species': 'Kiefer', 'as_of': '2026-11-01'}).get_json()['tree']
    assert r['frame']['by'] == 'as_of' and r['frame']['moved_m'] == list(OFF)
    r = c.post('/api/v1/flags', json={'lon': oc['lon'][i], 'lat': oc['lat'][i], 'species': 'Kiefer', 'frame': 'p-5+5'})
    assert r.status_code in (200, 404) and 'moved_m' not in ((r.get_json().get('tree') or {}).get('frame') or {})
    # 5) history lookup with the stale id / stale point
    h = c.get(f'/api/v1/trees/taxa/history?tree_id={tid}').get_json()
    assert h.get('tree', {}).get('tree_id') == want and len(h['versions']) >= 3, h
    h = c.get(f"/api/v1/trees/taxa/history?lon={oc['lon'][clean[0]]}&lat={oc['lat'][clean[0]]}").get_json()
    assert h['dist_m'] < 1.0, h
    # 6) whole-file vote
    pts = [[oc['lon'][i], oc['lat'][i]] for i in mine[:400]]
    v = c.post('/api/v1/trees/frame', json={'points': pts}).get_json()
    assert v['kgs'][kg]['frame'] == 'p0' and v['kgs'][kg]['confident'], v
    pts = [[nc['lon'][j], nc['lat'][j]] for j in range(new['n']) if nc['kg_code'][j] == kg][:400]
    v = c.post('/api/v1/trees/frame', json={'points': pts}).get_json()
    assert v['kgs'][kg]['frame'] == 'p-5+5' and v['kgs'][kg]['confident'], v
    g = c.get('/api/v1/trees/frame').get_json()
    assert g['kgs'][kg]['moved_m'] == list(OFF)
    # 7) audit column
    fr = dict(con.execute('SELECT fr, COUNT(*) FROM tree_ver GROUP BY fr').fetchall())
    assert fr.get(1, 0) >= 4 and fr.get(2, 0) >= 2, fr
    # 8) the public record: one KG-level machine flag per fix, nothing hidden by it
    assert ne_pos_fix.sync(db=pf.DB_PATH) == {'kgs': 2, 'unfixed': 0}
    ne_pos_fix.sync(db=pf.DB_PATH)
    assert con.execute('SELECT COUNT(*) FROM report WHERE cell IS NULL').fetchone()[0] == 2
    q = c.get('/api/v1/flags?source=auto-shift&group=report').get_json()
    rec = next(x for x in q['reports'] if x['kg'] == kg)
    assert rec['claim'] == 'misplaced' and rec['expected']['moved_m'] == list(OFF) and rec['expected']['applied'] \
        and rec['expected']['frame'] == 'p-5+5' and rec['source'] == 'auto-shift', rec
    ne_trees._pf_cache.clear()
    again = ne_trees.columns_bbox(*BB, 'trees', min_height=10, limit=200000, fields=F)
    assert again['n'] == new['n'] - 1, (again['n'], new['n'])   # (−1 = the apex flagged spurious above)
    # 9) regeneration on the true grid: shift off, same token, stale copies still resolve
    assert ne_pos_fix.retire('00001') and ne_pos_fix.offset('00001') is None
    f = ne_pos_fix.frames('00001'); assert f['frame'] == 'p-5+5' and f['delta'] == OFF, f
    # 10) overrule: a human rejects the record → positions as stored, every report moves back,
    #     a copy made WHILE the fix was on is now the stale one
    pf.set_status(rec['id'], 'rejected', 'test')
    assert ne_pos_fix.sync(db=pf.DB_PATH)['unfixed'] == 1 and ne_pos_fix.offset(kg) is None
    tu._state.clear(); ne_trees._pf_cache.clear(); ne_trees._fr_cache.clear()
    f = ne_pos_fix.frames(kg); assert f['frame'] == 'p0' and f['stale'] == 'p-5+5' and f['delta'] == (-OFF[0], -OFF[1]), f
    assert con.execute('SELECT x, y FROM tree_ver WHERE rev = 1').fetchone()[:2] == (kx, ky)
    back = ne_trees.columns_bbox(*BB, 'trees', min_height=10, limit=200000, fields=F + ('taxon',))
    bc = back['columns']; bcur = {(x, y): j for j, (x, y) in enumerate(zip(bc['x'], bc['y']))}
    assert bc['taxon'][bcur[(kx, ky)]] == 'Picea abies' and back['frames'] == {kg: 'p0'}
    assert (oc['x'][i1], oc['y'][i1]) not in bcur and (oc['x'][i2], oc['y'][i2]) not in bcur     # both flags followed
    i = clean[4]        # a report from the copy downloaded during the fix
    r = c.post('/api/v1/flags', json={'lon': nc['lon'][cur[(oc['x'][i] + OFF[0], oc['y'][i] + OFF[1])]],
                                      'lat': nc['lat'][cur[(oc['x'][i] + OFF[0], oc['y'][i] + OFF[1])]],
                                      'species': 'Ahorn', 'as_of': int(time.time()) - 50}).get_json()['tree']
    assert r['frame']['by'] == 'as_of' and r['frame']['moved_m'] == [-OFF[0], -OFF[1]] and abs(r['lon'] - oc['lon'][i]) < 2e-6, r
    rec = c.get(f'/api/v1/flags?kg={kg}&source=auto-shift&group=report&status=all').get_json()['reports'][0]
    assert rec['status'] == 'rejected' and rec['expected']['applied'] is False and rec['expected']['frame'] == 'p0', rec
    print('ok tree frames', fr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
