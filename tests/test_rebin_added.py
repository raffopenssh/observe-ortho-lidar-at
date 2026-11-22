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


def _cand(con, kg, r10, idx, x, y, h, reason='no_row', v0='add', okg=None, nb=b'', nbk='[]', tb='cand'):
    con.execute(f'INSERT OR REPLACE INTO {tb}(kg, r10, idx, cell, x, y, hq, word, reason, okg, v0, nb, nbk) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
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
    kc = ra.kg_counts(92124)
    assert {k: kc[k] for k in ('stored', 'served', 'off', 'capped', 'added', 'no_row', 'other_file', 'dup', 'qa', 'ok')} == {
        'stored': 100, 'served': 96, 'off': 4, 'capped': 0, 'added': 2, 'no_row': 1, 'other_file': 1, 'dup': 1, 'qa': 1, 'ok': 1}
    assert kc['lost'] is None and kc['lost_cell'] == 0          # lost-cell part not scanned: the off-file invariant alone
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


def _nb(*rows):
    """evidence blob: (dx, dy, height m, file index)"""
    return b''.join(bytes([dx & 255, dy & 255, int(h * 4), k]) for dx, dy, h, k in rows)


def test_lost_cell_part():
    """Apices in cells another KG file serves: kept ones are added trees with reason lost_cell, the
    part has its own token and counts, and the invariant is stored == served − lost + added + dup + qa."""
    pf, tu = _tmp_stores()
    ra._store_kg = lambda r: int(r['kg'])
    ra._frame = lambda kg: (0, 0)
    # a store written before the lost-cell part existed gets the new columns / table
    import sqlite3
    old = sqlite3.connect(str(ra.DB_PATH))
    for q in ra._SCHEMA:
        old.execute(q)
    old.commit(); old.close()
    con = ra._conn(write=True)
    assert {'ltoken', 'lost', 'ldup', 'lqa', 'lost_cell'} <= {r[1] for r in con.execute('PRAGMA table_info(kgs)')}
    X, Y = 4285000, 2665000
    with con:
        _cand(con, 92124, 7, 0, X, Y, 22.25)                                    # off-file, kept
        # same (r10, idx) key as the off-file row — its own table, its own client_id
        _cand(con, 92124, 7, 0, X + 300, Y, 18.0, 'lost_cell', okg=92125, tb='lcand')
        _cand(con, 92124, 7, 5, X + 340, Y, 9.0, 'lost_cell', okg=92125, tb='lcand')
        # kept in two files' lost cells: the second one is the twin of the first
        _cand(con, 92125, 9, 3, X + 301, Y, 18.25, 'lost_cell', okg=92124, tb='lcand')
        con.execute('INSERT INTO kgs(kg, token, deps, stored, served, off, capped, outside, ltoken, ldeps, lost, ldup, lqa) '
                    'VALUES (92124, ?, "[]", 100, 99, 1, 0, 0, ?, "[]", 10, 7, 1)', (ra.RULE + '|x|', ra.LOST_ID + '.x'))
        con.execute('INSERT INTO kgs(kg, token, deps, stored, served, off, capped, outside, ltoken, ldeps, lost, ldup, lqa) '
                    'VALUES (92125, ?, "[]", 50, 50, 0, 0, 0, ?, "[]", 4, 3, 0)', (ra.RULE + '|x|', ra.LOST_ID + '.x'))
    r = ra.sync(verbose=False)
    assert (r['added'], r['lost_cell'], r['dup_added'], r['lost'], r['lost_dup'], r['lost_qa']) == (3, 2, 1, 14, 10, 1), r
    assert r['invariant_ok'] == 2 and r['invariant_bad'] == 0 and r['sync']['add'] == 3, r
    kc = ra.kg_counts(92124)
    # 100 stored == 99 served − 10 lost + 3 added + 7 dup + 1 qa
    assert (kc['lost'], kc['lost_cell'], kc['added'], kc['dup'], kc['qa'], kc['ok']) == (10, 2, 3, 7, 1, 1), kc
    assert ra.kg_counts(92125)['dup'] == 4 and ra.kg_counts(92125)['added'] == 0
    lon, lat = tu._tf('EPSG:3035', 'EPSG:4326').transform(X, Y)
    a = tu.added(lon - 0.02, lat - 0.02, lon + 0.02, lat + 0.02, [92124, 92125])
    assert sorted(a['h_m']) == [9.0, 18.0, 22.2], a
    cids = sorted(a['client_id'])
    assert [c.split(':')[3] for c in cids] == ['lost_cell', 'lost_cell', 'no_row'] and len(set(cids)) == 3, cids
    assert any(n.startswith(f'auto-rebin {ra.RULE} lost_cell kg=92124 h_m=18') for n in a['note'])
    assert ra.sync(verbose=False)['sync'] == {'kgs': 0, 'add': 0, 'retract': 0}
    # the owner file was re-uploaded and now has that tree: the lost-cell rescan drops the row → retract
    with con:
        con.execute('DELETE FROM lcand WHERE kg=92124 AND idx=5')
        con.execute('UPDATE kgs SET ldup=8 WHERE kg=92124')
    r = ra.sync(verbose=False)
    assert r['sync']['retract'] == 1 and r['sync']['add'] == 0 and r['invariant_ok'] == 2, r
    # an off-file rescan by a process that does not know the lost columns (INSERT OR REPLACE) only
    # un-scans the lost part of that KG; status is not complete until it is redone
    with con:
        con.execute('INSERT OR REPLACE INTO kgs(kg, token, deps, stored, served, off, capped, outside) VALUES (92125, ?, "[]", 50, 50, 0, 0, 0)', (ra.RULE + '|x|',))
    assert ra.totals(con)['lost_scanned'] == 1


def test_shift_twins():
    """One offset shared by the equal-height partners of a file pair = an unregistered position frame."""
    import json
    dt = np.dtype([('dx', 'i1'), ('dy', 'i1'), ('hq', 'u1'), ('k', 'u1')])
    nbk = json.dumps([[92125, 0, 0]])

    def row(i, h, nb):
        return {'tb': 'lcand', 'kg': 92124, 'r10': 1, 'idx': i, 'x': 0, 'y': 0, 'hq': int(h * 4), 'v0': 'add', 'nb': nb, 'nbk': nbk}
    rng = np.random.default_rng(3)
    # 30 apices with their equal-height partner at (−6, +6) ± 1, plus an unrelated neighbour; 10 without
    rows = [row(i, 10 + i * 0.5, _nb((-6 + int(rng.integers(-1, 2)), 6 + int(rng.integers(-1, 2)), 10 + i * 0.5, 0), (3, -7, 30, 0)))
            for i in range(30)]
    rows += [row(100 + i, 12.0, _nb((int(rng.integers(3, 8)), int(rng.integers(-8, -2)), 25.0, 0))) for i in range(10)]
    v = ['add'] * len(rows)
    sh = ra._shift_twins(rows, v, dt)
    assert v[:30] == ['dup_shift'] * 30 and v[30:] == ['add'] * 10, v
    (k, t), = sh.items()
    assert k == (92124, 92125) and abs(t[0] + 6) <= 1 and abs(t[1] - 6) <= 1 and t[2] == 30 and t[3] == 40, sh
    # no common offset (equal heights by chance, scattered): nothing is a shift twin
    rows = [row(i, 8.0, _nb((int(rng.integers(-8, 9)) or 3, int(rng.integers(3, 9)) * (1 if i % 2 else -1), 8.0, 0))) for i in range(40)]
    v = ['add'] * len(rows)
    assert ra._shift_twins(rows, v, dt) == {} and v == ['add'] * 40
    # too few apices in the pair: never
    rows = [row(i, 20.0, _nb((-6, 6, 20.0, 0))) for i in range(ra.SHIFT_MIN_N - 1)]
    v = ['add'] * len(rows)
    assert ra._shift_twins(rows, v, dt) == {} and set(v) == {'add'}
    # the off-file part follows the same rule
    rows = [dict(row(i, 20.0, _nb((-6, 6, 20.0, 0))), tb='cand') for i in range(40)]
    v = ['add'] * len(rows)
    assert len(ra._shift_twins(rows, v, dt)) == 1 and set(v) == {'dup_shift'}


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
    assert st['ok'] and st['stored'] == st['apices'] - (st['lost'] or 0) + st['added'] + st['dup'] + st['qa'] + st['capped'], st
    if st['lost'] is None:
        assert st['off_file'] == st['added'] + st['dup'] + st['qa'], st


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn(); print('ok', name)
    print('all ok')
