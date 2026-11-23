"""cells_national: parent arithmetic, aggregate correctness on one KG, codec, route."""
import sys, os, gzip
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import pytest
import cells_api as ca
import cells_national as cn


def _first_kg():
    files = sorted(ca._files())
    if not files:
        pytest.skip('no published cells (data/ne/*.h3b) on this machine')
    return files[0]


def _table(kg):
    h = ca.header(kg, with_chunks=True)
    assert h, f'{kg} not on disk'
    chunks = [ca._load_chunk(kg, dict(r, r10=int(r['r10'], 16)), h) for r in h['chunks']]
    return ca.gather(chunks, None)


def test_parents():
    hn = ca._h3n()
    t = _table(_first_kg())
    for res in (8, 9):
        p = cn.parent_ids(t.ids, res)
        for i in range(0, len(t), max(1, len(t) // 50)):
            assert int(p[i]) == hn.cell_to_parent(int(t.ids[i]), res)
    p9 = cn.parent_ids(t.ids, 9); p8 = cn.rollup({'parents': np.unique(p9)}, 9, 8)['parents']
    assert set(p8.tolist()) == {hn.cell_to_parent(int(x), 8) for x in np.unique(p9).tolist()}


def test_aggregate_matches_direct():
    kg = _first_kg()
    t = _table(kg)
    acc = cn.Acc(9); acc.add_table(t); red = acc.reduce(); cols = cn.finalise(red)
    p9 = cn.parent_ids(t.ids, 9)
    assert cols['n_cells'].sum() == len(t)
    for i in np.argsort(-cols['n_cells'])[:5]:
        m = p9 == cols['id'][i]
        el = t.obs['el'][m]; el = el[el != 65535]
        if el.size:
            assert abs(int(cols['elev_mean'][i]) - round(el.mean())) <= 1
        else:
            assert cols['elev_mean'][i] == cn.U16_UNK
        assert np.array_equal(cols['verdict'][i], np.bincount(np.minimum(t.obs['vd'][m], 6), minlength=7))
        assert cols['tree_n'][i] == t.obs['tn'][m].sum()
    # res-8 rollup conserves counts
    c8 = cn.finalise(cn.rollup(red, 9, 8))
    assert c8['n_cells'].sum() == cols['n_cells'].sum() and c8['verdict'].sum() == cols['verdict'].sum()
    # codec round trip (+ field slice)
    blob = cn.encode(cn.header_for(9, cols['id'].size, 'g', None), cols)
    hdr, c2 = cn.decode(blob)
    assert hdr['res'] == 9 and all(np.array_equal(cols[k], c2[k]) for k in cols)
    hdr, c3 = cn.decode(cn.encode(hdr, cols, ['verdict', 'bld_h_max']))
    assert list(c3) == ['id', 'n_cells', 'verdict', 'bld_h_max'] and hdr['fields'] == ['n_cells', 'verdict', 'bld_h_max']


def test_fields_and_route():
    assert cn.parse_fields(None) is None and cn.parse_fields('*') is None
    assert cn.parse_fields('elev_m,slope,verdict,verdict') == ['elev_mean', 'slope_mean', 'verdict']
    try:
        cn.parse_fields('nope'); assert False
    except ca.CellsError as e:
        assert e.status == 400
    import public_api
    c = public_api.app.test_client()
    r = c.get('/api/v1/cells/national?res=7'); assert r.status_code == 400
    r = c.get('/api/v1/cells/national?format=json')
    assert r.status_code in (200, 404)
    if r.status_code == 200 and (cn.NAT_DIR / 'r9.nex.gz').exists():
        r = c.get('/api/v1/cells/national?res=9&fields=elev_mean,verdict', headers={'Accept-Encoding': 'gzip'})
        assert r.status_code == 200 and r.headers['Content-Encoding'] == 'gzip' and r.headers['X-Cells-Gen']
        hdr, cols = cn.decode(gzip.decompress(r.data))
        assert hdr['res'] == 9 and list(cols) == ['id', 'n_cells', 'elev_mean', 'verdict'] and hdr['n'] == cols['id'].size
        r2 = c.get('/api/v1/cells/national?res=9&fields=elev_mean,verdict', headers={'If-None-Match': r.headers['ETag']})
        assert r2.status_code == 304
        r3 = c.get('/api/v1/cells/national?res=8', headers={'Accept-Encoding': 'gzip'})
        assert r3.status_code == 200 and cn.decode(gzip.decompress(r3.data))[0]['res'] == 8


def test_tree_qa_fold():
    """current_gen follows the corpus tree-QA token; small churn of it is minor drift."""
    import time
    real = ca._tree_qa.corpus_sig
    try:
        sig = {'id': 'x+thx1', 'kgs': 3700, 'hidden': 300000, 'tok': 'aaa'}
        ca._tree_qa.corpus_sig = lambda: dict(sig)
        g0 = cn.current_gen()
        files = ca._files()
        if not files:
            return                                                 # no corpus on this host (public fork)
        meta = {'algo': cn.ALGO, 'green_fix': cn._green_fix.FIX_ID, 'kgs': len(files), 'corpus': cn._corpus_sig(files),
                'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'build_started': time.time(),
                'tree_qa': {'id': 'x+thx1', 'kgs': 3700, 'hidden': 300000}}
        assert cn.minor_drift(meta, files) is not None
        sig.update(tok='bbb', hidden=300400, kgs=3705)             # an hourly linear-noise run
        assert cn.current_gen() != g0
        d = cn.minor_drift(meta, files); assert d and d['tree_qa_hidden_delta'] == 400
        sig.update(hidden=330000)                                  # a real change
        assert cn.minor_drift(meta, files) is None
        sig.update(hidden=300000, id='y+thx1')                     # rule bump
        assert cn.minor_drift(meta, files) is None
        assert cn.minor_drift({k: v for k, v in meta.items() if k != 'tree_qa'}, files) is None   # built before the fold
    finally:
        ca._tree_qa.corpus_sig = real


def test_units_match_single_pass():
    """Incremental build (units on disk → scatter merge) == the one-pass reduction, bit for bit:
    per-product units for 3 neighbouring KGs + every duplicate group touching the first one."""
    import tempfile, shutil
    from pathlib import Path
    import tree_stands
    files = ca._files()
    if not files:
        return
    ca._tree_qa.sync()
    units = cn.plan(files)
    groups = [u for u in units if u['kind'] == 'g']
    g0 = min(groups, key=lambda u: u['chunks'] if u['chunks'] > 30 else 10 ** 9)
    kgs = sorted(g0['kgsigs'])[:3]
    sel = [u for u in units if u['kind'] == 'k' and u['kg'] in kgs] + \
          sorted([u for u in groups if kgs[0] in u['kgsigs']], key=lambda u: u['chunks'])[:3]
    if g0['id'] not in {u['id'] for u in sel}:
        sel.append(g0)
    # reference: the pre-incremental algorithm
    acc = cn.Acc(9); acc.stands = tree_stands.StandAcc()
    for u in sel:
        if u['kind'] == 'k':
            h = ca.header(u['kg'], with_chunks=True); dup = set(u['dup'].tolist())
            ch = [ca._load_chunk(u['kg'], dict(r, r10=int(r['r10'], 16)), h) for r in h['chunks'] if int(r['r10'], 16) not in dup]
        else:
            ch, _ = ca.chunks_for_r10(u['r10s'])
        if ch:
            acc.add_table(ca.gather(ch, None))
    ref = acc.reduce(); sref = acc.stands.reduce()
    d = Path(tempfile.mkdtemp(prefix='nat_parts_'))
    try:
        st = cn.run_units(sel, jobs=1, d=d)
        assert st['done'] == len(sel) and st['stale'] == 0 and st['cells'] == acc.cells
        assert all(cn._unit_sig(u['id'], d) == u['sig'] for u in sel)           # resumable: nothing left to do
        out = dict(cn.merge_units(sel, d=d))
        red = out['red9']
        assert set(red) == set(ref) and red['parents'].size > 0
        for k in ref:
            assert np.array_equal(red[k], ref[k]), k
        assert sref is not None and all(np.array_equal(a, b) for a, b in zip(out['stands'], sref))
        assert int(out['stands'][1].sum()) == int(red['tree_hh'].sum())         # stands == tree_hh, apex for apex
        # a second, parallel run over the same units reproduces the stored partials
        d2 = d / 'par'
        cn.run_units(sel, jobs=2, d=d2)
        red2 = dict(cn.merge_units(sel, d=d2))['red9']
        assert all(np.array_equal(red2[k], ref[k]) for k in ref)
        # a signature that moved is rebuilt; a unit that fell out of the plan is pruned
        assert cn._prune_parts({u['id'] for u in sel[1:]}, d) == 1 and cn._unit_sig(sel[0]['id'], d) is None
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_pack_lossless():
    for a in (np.array([0., 3., 65536.]), np.array([0.25, 1.5]), np.array([-3, 200], np.int64), np.zeros((2, 3), np.int64),
              np.array([2 ** 63 + 5], np.uint64), np.zeros(0)):
        b = cn._pack(a)
        assert np.array_equal(b.astype(a.dtype), a) and b.nbytes <= a.nbytes
    assert cn._pack(np.array([0., 3.])).dtype == np.uint8 and cn._pack(np.array([0.25])).dtype == np.float64



def test_tree_hh_band():
    import cells_national as cn
    h = np.array([3.0, 4.75, 5.0, 9.9, 10.0, 34.9, 35.0, 54.9, 55.0, 63.75])
    assert cn.tree_hh_band(h).tolist() == [0, 0, 1, 1, 2, 6, 7, 10, 11, 11]
    assert cn.TREE_HH_BANDS == 12 and cn.FIELDS['tree_hh']['shape'] == [12]


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'):
            f(); print('ok', n)
