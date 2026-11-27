#!/usr/bin/env python3
"""Contributor groups + masks (tree_mod.py, docs/tree-user.md → Moderation by contributor).

    python3 tests/test_tree_mod.py
Temp DB, no corpus."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _tmp():
    import public_flags as pf
    import tree_user as tu
    tmp = Path(tempfile.mkdtemp())
    pf.DB_PATH = tmp / 'f.sqlite'; pf.SALT_PATH = tmp / 'salt'
    pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear(); pf._daily.clear(); tu._state.clear(); tu._sys_rep.clear()
    import ne_pos_fix
    ne_pos_fix.tag = lambda kg: ''
    return pf, tu


def _ver(tu, pf, rep, x, y, sp=None, dbh=None, kg=92124, op=0, ts=1_795_000_000):
    c = tu._conn()
    c.execute('INSERT INTO tree_ver(ts, x, y, kg, op, sp, h_dm, reporter, dbh_mm) VALUES (?,?,?,?,?,?,?,?,?)',
              (ts, x, y, kg, op, pf._dict_id(c, 'taxon', sp) if sp else None, 250, rep, dbh))
    c.commit()


def test_mask_round_trip():
    pf, tu = _tmp()
    import tree_mod as tm
    a, b, m = (pf._reporter_hash(n, 'x') for n in ('anna', 'bert', 'bert-phone'))
    X, Y = 4285000, 2665000
    _ver(tu, pf, a, X, Y, 'Picea abies')
    _ver(tu, pf, b, X, Y, 'Fagus sylvatica', dbh=420)         # newer → wins the tie
    _ver(tu, pf, b, X + 50, Y, 'Fagus sylvatica')
    _ver(tu, pf, m, X + 90, Y, 'Fagus sylvatica', ts=1_795_000_100)
    assert tu.tree_at(92124, X, Y)['taxon'] == 'Fagus sylvatica'
    rev0 = tu.global_rev(); n0 = tu._conn().execute('SELECT COUNT(*) FROM tree_ver').fetchone()[0]
    # the list groups by contributor hash; handles are the hash, nothing else is known
    L = {d['handle']: d for d in tm.contributors()}
    assert set(L) == {a.hex(), b.hex(), m.hex()} and L[b.hex()]['species'] == 2 and L[b.hex()]['conflicts'] == 1, L
    assert L[m.hex()]['near'] == [a.hex()[:8], b.hex()[:8]] or set(L[m.hex()]['near']) == {a.hex()[:8], b.hex()[:8]}
    # two hashes, one person: an internal label; mask the label = species votes of both stop counting
    tm.set_group('ch-1', [b.hex()[:8], m.hex()[:8]])
    d = tm.set_mask(['ch-1'], tu.MASK_SPECIES, 'test', dry=True)
    assert d['changed'] == 2 and d['trees'] == 3 and tu.global_rev() == rev0
    d = tm.set_mask(['ch-1'], tu.MASK_SPECIES, 'test')
    assert d['changed'] == 2 and d['trees'] == 3, d
    t = tu.tree_at(92124, X, Y)
    assert t['taxon'] == 'Picea abies' and t['n'] == 1 and t['dbh_cm'] == 42.0, t     # the measure stays, the name does not
    assert tu.tree_at(92124, X + 50, Y)['taxon'] is None
    # nothing lost: every row is still there, plus one moderation row per touched tree (new rev → delta readers follow)
    c = tu._conn()
    assert c.execute('SELECT COUNT(*) FROM tree_ver WHERE op != 3').fetchone()[0] == n0
    assert c.execute('SELECT COUNT(*) FROM tree_ver WHERE op = 3 AND ref IS NULL').fetchone()[0] == 3
    li = tu.listing(since=rev0)
    assert li['count'] == 3 and sorted(map(str, li['columns']['taxon'])) == ['None', 'None', 'Picea abies'], li
    assert tu.listing()['count'] == 1                    # since=0: only what is live
    # what the masked contributor sends later is stored and does not count either
    _ver(tu, pf, b, X + 50, Y, 'Abies alba', ts=1_795_100_000)
    assert tu.tree_at(92124, X + 50, Y)['taxon'] is None
    # idempotent
    assert tm.set_mask(['ch-1'], tu.MASK_SPECIES)['changed'] == 0
    # --all: the measure goes too
    tm.set_mask([b.hex()[:8]], tu.MASK_ALL)
    assert 'dbh_cm' not in tu.tree_at(92124, X, Y)
    # unmask: everything is back, incl. the later report
    rev1 = tu.global_rev()
    tm.set_mask(['ch-1'], 0)
    assert tu.global_rev() > rev1
    assert tu.tree_at(92124, X, Y)['taxon'] == 'Fagus sylvatica' and tu.tree_at(92124, X, Y)['dbh_cm'] == 42.0
    assert tu.tree_at(92124, X + 50, Y)['taxon'] == 'Abies alba' and tu.tree_at(92124, X + 90, Y)['taxon'] == 'Fagus sylvatica'
    s = tm.status()
    assert s['masked'] == 0 and s['groups'] == 1 and s['contributors'] == 3, s
    # an account id (the NE server key id, later) works like a group label; one row links a browser's whole history
    tm.link('sub_9f2c41aa77e0b1c2', [a_hex := pf._reporter_hash('anna', 'x').hex()[:8]])
    assert tm.set_mask(['sub_9f2c41aa77e0b1c2'], tu.MASK_SPECIES, dry=True)['contributors'] == [a_hex]
    assert {d['handle'][:8]: d['acct'] for d in tm.contributors()}[a_hex].startswith('a:') and tm.status()['accounts'] == 1
    raw = tu._conn().execute('SELECT acct FROM rmod WHERE acct IS NOT NULL').fetchone()[0]
    assert isinstance(raw, bytes) and len(raw) == 12 and b'sub_' not in raw           # only a salted hash is kept
    tm.link(None, [a_hex])                                 # erase: the row carried nothing else → gone
    assert tm.status()['accounts'] == 0 and not tu._conn().execute('SELECT 1 FROM rmod WHERE reporter=?', (pf._reporter_hash('anna', 'x'),)).fetchone()
    tm.link('sub_9f2c41aa77e0b1c2', [a_hex])
    try:
        tm.link('anna@example.org', [a_hex]); raise AssertionError('address accepted')
    except SystemExit:
        pass
    assert 'tree_mod:' in tm.text_line() and a.hex()[:6] not in tm.text_line()


def test_account_wire():
    """POST /flags "account" (verified key holder) → rmod.acct; GET / DELETE /flags/account."""
    pf, tu = _tmp()
    import json
    import tree_mod as tm
    from flask import Flask
    tm._seen.clear()
    pf.KEYS_PATH = pf.DB_PATH.parent / 'keys.json'
    pf.KEYS_PATH.write_text(json.dumps({'k-ne': 'ne-server', 'k-other': 'forestry'})); pf._keys_cache[0] = None
    app = Flask(__name__); app.register_blueprint(pf.flags_bp); cl = app.test_client()
    P = 'f3a9' * 8
    rep = {'layer': 'other', 'claim': 'other', 'kg': '92124', 'note': 'x', 'reporter': 'browser-1'}
    K = {'X-Reporter-Key': 'k-ne'}
    j = cl.post('/api/v1/flags', json=rep, headers={**K, 'X-Reporter-Account': P}).get_json()
    assert j['account'] == 'linked' and 'account_warning' not in j, j
    j = cl.post('/api/v1/flags', json={'reports': [{**rep, 'note': 'y', 'reporter': 'browser-2'}], 'account': P}, headers=K).get_json()
    assert j['account'] == 'linked', j
    c = tu._conn()
    rows = c.execute('SELECT reporter, acct FROM rmod').fetchall()
    assert len(rows) == 2 and all(bytes(r[1]) == tm.acct_hash(P, 'ne-server') for r in rows)
    assert P.encode() not in pf.DB_PATH.read_bytes()                     # the pseudonym itself is stored nowhere
    # no key / self-issued key / bad value: the report is stored, the link is not
    j = cl.post('/api/v1/flags', json={**rep, 'note': 'z', 'account': P}).get_json()
    assert 'account' not in j and 'ignored' in j['account_warning'] and j.get('id'), j
    j = cl.post('/api/v1/flags', json={**rep, 'note': 'q', 'account': 'me@example.org'}, headers=K).get_json()
    assert 'ignored' in j['account_warning'] and j.get('id'), j
    j = cl.post('/api/v1/flags', json={k: v for k, v in rep.items() if k != 'reporter'} | {'note': 'n', 'account': P}, headers=K).get_json()
    assert j['account'] == 'not_linked', j
    assert c.execute('SELECT COUNT(*) FROM rmod').fetchone()[0] == 2
    # usage: counts only; another holder sees nothing of it and cannot erase it
    u = cl.get(f'/api/v1/flags/account?account={P}', headers=K).get_json()
    assert u['linked'] and u['reporters'] == 2 and u['flags'] == 2 and u['reports'] == 2 and u['last_ts'], u
    assert not ({'handle', 'reporter', 'lon', 'kg'} & set(u))
    assert cl.get(f'/api/v1/flags/account?account={P}&since=4000000000', headers=K).get_json()['reports'] == 0
    o = {'X-Reporter-Key': 'k-other'}
    assert cl.get(f'/api/v1/flags/account?account={P}', headers=o).get_json()['linked'] is False
    assert cl.delete(f'/api/v1/flags/account?account={P}', headers=o).get_json()['unlinked'] == 0
    assert cl.get(f'/api/v1/flags/account?account={P}').status_code == 403
    assert cl.get('/api/v1/flags/account?account=short', headers=K).status_code == 400
    # moderation by pseudonym; erase keeps the reports
    assert len(tm.resolve(c, [P])) == 2
    assert cl.delete(f'/api/v1/flags/account?account={P}', headers=K).get_json()['unlinked'] == 2
    assert c.execute('SELECT COUNT(*) FROM rmod').fetchone()[0] == 0 and c.execute('SELECT COUNT(*) FROM report').fetchone()[0] >= 4
    assert cl.get(f'/api/v1/flags/account?account={P}', headers=K).get_json()['linked'] is False


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn(); print('ok', name)
    print('all ok')
