#!/usr/bin/env python3
"""Off-file apices → added trees (rebin_added.py, docs/tree-cell-assignment.md).

    python3 tests/test_rebin_added.py

The twin rule and the store round trip run everywhere (temp DBs); the live part needs the corpus."""
import sys
import tempfile
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import rebin_added as ra  # noqa: E402


def test_twin_rule_is_height_aware():
    t = lambda h, dx, dy, nh, **k: ra.twin(h, [dx], [dy], [nh], **k)[1]      # noqa: E731
    # identical detection: same height, offset <= 2 m — a twin at every height
    for h in (3.0, 7.5, 15.0, 25.0, 40.0):
        assert t(h, 0, 0, h) == 'dup' and t(h, 2, -2, h) == 'dup' and t(h, 3, 0, h) is None, h
    # low trees: neighbours with a similar height are common → only the exact pair counts
    assert t(3.0, 0, 0, 3.25) == 'dup' and t(3.0, 0, 0, 3.5) is None
    assert t(3.0, 1, 0, 3.25) is None and t(7.0, 1, 1, 7.25) is None and t(7.0, 0, 0, 9.0) == 'dup'
    # 10–20 m: one metre aside within 0.5 m; two metres aside only the exact height
    assert t(15.0, 1, 0, 15.5) == 'dup' and t(15.0, 1, 0, 15.75) is None and t(15.0, 2, 0, 15.25) is None
    # tall trees: no second apex can stand within 2 m — a twin unless this one is > 2 m TALLER than the other file's
    assert t(25.0, 2, 2, 27.0) == 'dup' and t(25.0, 2, 2, 23.0) == 'dup' and t(25.0, 1, 0, 22.5) is None
    assert t(25.0, 1, 0, 31.0) == 'dup' and t(7.0, 0, 0, 30.0) == 'dup' and t(7.0, 1, 0, 30.0) is None   # under a taller apex of the other file
    assert t(40.0, 0, 0, 20.0) is None                     # the other file has a much lower apex here: keep the tall one
    # frame rule: the same stored apex in two files in different frames — exact offset, height step
    assert t(18.0, 5, -5, 18.25, fdx=[-5], fdy=[5]) == 'dup_frame'
    assert t(18.0, 5, -5, 18.5, fdx=[-5], fdy=[5]) is None and t(18.0, 4, -5, 18.0, fdx=[-5], fdy=[5]) is None
    assert t(18.0, 5, -5, 18.0, fdx=[0], fdy=[0]) is None  # same frame: 7 m apart is another tree
    # nearest first
    j, k = ra.twin(25.0, [2, 0, 1], [0, 0, 1], [25.0, 26.0, 25.0])
    assert (j, k) == (1, 'dup')
    # the table is monotone: a taller class never has a tighter tolerance
    tab = np.array([r[1] for r in ra.TWIN_DH])
    assert (np.diff(tab, axis=0) >= 0).all() and (np.diff(tab, axis=1) <= 0).all()


def _tmp_stores():
    import public_flags as pf
    import tree_user as tu
    tmp = Path(tempfile.mkdtemp())
    pf.DB_PATH = tmp / 'f.sqlite'; pf.SALT_PATH = tmp / 'salt'
    pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear(); pf._daily.clear(); tu._state.clear(); tu._sys_rep.clear()
    ra.DB_PATH = tmp / 'rebin.sqlite'
    return pf, tu


def _cand(con, kg, r10, idx, x, y, h, reason='no_row', v0='add', okg=None, nb=b'', nbk='[]'):
    con.execute('INSERT OR REPLACE INTO cand(kg, r10, idx, cell, x, y, hq, word, reason, okg, v0, nb, nbk) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (kg, r10, idx, 1, x, y, int(h * 4), (int(h * 4) << 10) | (50 << 18), reason, okg, v0, nb, nbk))


def test_store_round_trip():
    pf, tu = _tmp_stores()
    ra._store_kg = lambda r: int(r['kg'])                 # no search index needed
    ra._frame = lambda kg: (0, 0)
    con = ra._conn(write=True)
    X, Y = 4285000, 2665000
    with con:
        _cand(con, 92124, 7, 0, X, Y, 22.25)
        _cand(con, 92124, 7, 1, X + 40, Y, 31.0, 'other_file', okg=92125)
        _cand(con, 92124, 7, 2, X + 80, Y, 12.0, 'other_file', 'dup', okg=92125,
              nb=bytes([1, 0, 50, 0, 2, 0, 48, 0]), nbk='[[92125,0,0]]')     # evidence: 12.5 m one metre aside, 12.0 m two aside
        _cand(con, 92124, 7, 3, X + 90, Y, 61.0, v0='qa')
        _cand(con, 92125, 9, 0, X + 1, Y, 22.5)           # the same tall tree, off-file in a second source file
        _cand(con, 92125, 9, 1, X + 200, Y, 4.0)
        con.execute('INSERT INTO kgs(kg, token, deps, stored, served, off, capped, outside) VALUES (92124, ?, "[]", 100, 96, 4, 0, 0)', (ra.RULE + '|x|',))
        con.execute('INSERT INTO kgs(kg, token, deps, stored, served, off, capped, outside) VALUES (92125, ?, "[]", 50, 48, 2, 0, 0)', (ra.RULE + '|x|',))
    r = ra.sync(verbose=False)
    assert (r['added'], r['dup'], r['dup_added'], r['qa']) == (3, 1, 1, 1), r
    assert r['invariant_ok'] == 2 and r['sync']['add'] == 3 and r['sync']['retract'] == 0, r
    assert ra.kg_counts(92124) == {'stored': 100, 'served': 96, 'off': 4, 'capped': 0, 'added': 2, 'no_row': 1,
                                   'other_file': 1, 'dup': 1, 'qa': 1, 'ok': 1}
    # read back through the added-tree path, exactly like a `missing` report
    lon, lat = tu._tf('EPSG:3035', 'EPSG:4326').transform(X, Y)
    a = tu.added(lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01, [92124, 92125])
    assert sorted(a['h_m']) == [4.0, 22.2, 31.0], a
    cid = next(c for c, h in zip(a['client_id'], a['h_m']) if h == 31.0)
    assert cid.startswith('auto-rebin:92124:rb1:other_file:7:1:') and tu.parse_client_id(cid) == cid
    note = next(n for n, h in zip(a['note'], a['h_m']) if h == 22.2)
    assert note.startswith(f'auto-rebin {ra.RULE} no_row kg=92124 h_m=22.25 crown_m2='), note
    # machine rows do not count as a human revision (cache lifetime), but they are in the ETag revision
    assert tu.kgs_rev([92124]) > 0 and tu.kgs_rev([92124], human=True) == 0
    # one KG-level flag per KG with added trees, none hides a cell
    st = pf.auto_kg_status(ra.SOURCE)
    assert set(st) == {'92124', '92125'}, st
    n0 = tu.global_rev()
    # idempotent
    r = ra.sync(verbose=False)
    assert r['sync'] == {'kgs': 0, 'add': 0, 'retract': 0} and tu.global_rev() == n0
    # a re-upload moved one apex and dropped another: retract + add, never a duplicate
    with con:
        con.execute('DELETE FROM cand WHERE kg=92124 AND idx=1')
        _cand(con, 92124, 7, 0, X + 13, Y, 22.25)
        con.execute('UPDATE kgs SET stored=99, off=3 WHERE kg=92124')
    r = ra.sync(verbose=False)
    assert r['sync']['retract'] == 2 and r['sync']['add'] == 2, r      # 92125's twin comes back: its partner moved away
    a = tu.added(lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01, [92124, 92125])
    assert sorted(a['h_m']) == [4.0, 22.2, 22.5], a
    # a person names a machine-added tree: it stays a tree, and now counts as a human revision
    assert r['invariant_ok'] == 2, r
    cid = next(c for c, h in zip(a['client_id'], a['h_m']) if h == 4.0)
    out = tu.record({'client_id': cid, 'species': 'Picea abies'}, {'lon': lon, 'lat': lat, 'kg': '92125'}, 'species',
                    pf._reporter_hash('someone', 'x'), None, None)
    assert out['matched'] == 'added' and out['taxon'] == 'Picea abies', out
    assert tu.kgs_rev([92125], human=True) > 0
    assert ra.sync(verbose=False)['sync'] == {'kgs': 0, 'add': 0, 'retract': 0}


def test_live_invariant():
    if not (ROOT / 'data' / 'ne' / '92124.h3b').exists():
        print('  (no corpus — skipped)'); return
    import importlib
    import ne_rebin
    importlib.reload(ra)                                   # the real store paths again
    c = ra.kg_counts('92124')
    if c is None:
        print('  (92124 not scanned yet — skipped)'); return
    st = ne_rebin.check('92124')
    assert st['ok'] and st['stored'] == st['apices'] + st['added'] + st['dup'] + st['qa'] + st['capped'], st
    assert st['off_file'] == st['added'] + st['dup'] + st['qa'], st


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn(); print('ok', name)
    print('all ok')
