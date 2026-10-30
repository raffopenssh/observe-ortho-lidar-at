"""cells_national: parent arithmetic, aggregate correctness on one KG, codec, route."""
import sys, os, gzip
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import cells_api as ca
import cells_national as cn


def _table(kg):
    h = ca.header(kg, with_chunks=True)
    assert h, f'{kg} not on disk'
    chunks = [ca._load_chunk(kg, dict(r, r10=int(r['r10'], 16)), h) for r in h['chunks']]
    return ca.gather(chunks, None)


def test_parents():
    hn = ca._h3n()
    t = _table(sorted(ca._files())[0])
    for res in (8, 9):
        p = cn.parent_ids(t.ids, res)
        for i in range(0, len(t), max(1, len(t) // 50)):
            assert int(p[i]) == hn.cell_to_parent(int(t.ids[i]), res)
    p9 = cn.parent_ids(t.ids, 9); p8 = cn.rollup({'parents': np.unique(p9)}, 9, 8)['parents']
    assert set(p8.tolist()) == {hn.cell_to_parent(int(x), 8) for x in np.unique(p9).tolist()}


def test_aggregate_matches_direct():
    kg = sorted(ca._files())[0]
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


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'):
            f(); print('ok', n)
