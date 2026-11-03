"""cells_blocks: centre-child rule, NEXB v2 codec + enumeration checksum, routes (block=1 default / ids=1, /cells/blocks, /cells/sample)."""
import sys, os, gzip, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import h3
import cells_api as ca
import cells_bins as cb
import cells_blocks as cbl


def _stored_block():
    m = cb.manifest(); b = m['bins']
    st = {(i, j) for i, j, s in zip(b['i'], b['j'], b['stored']) if s}
    per = {}
    for i, j in st:
        per[(i // 4, j // 4)] = per.get((i // 4, j // 4), 0) + 1
    full = sorted(k for k, v in per.items() if v == 16)
    assert full, 'no fully stored block on disk'
    return full[len(full) // 2]


def test_centre_child_rule():
    ids = np.sort(ca.ids_in_bbox(*cb.bin_bbox(400, 1880)))
    for res in (9, 10, 11):
        cc = ids[cbl._sample_mask(ids, res)]
        assert cc.size
        for c in cc[:100]:
            s = h3.int_to_str(int(c))
            assert h3.cell_to_center_child(h3.cell_to_parent(s, res), 12) == s
        # every parent appears exactly once and the order is ascending
        par = np.array([int(h3.cell_to_parent(h3.int_to_str(int(c)), res), 16) for c in cc])
        assert np.unique(par).size == par.size and np.all(np.diff(par.astype(np.int64)) > 0)


def test_block_v2_codec_and_checksum():
    bi, bj = _stored_block()
    hdr, raw, inps = cbl.build_block(bi, bj, cbl.GLYPH_FIELDS)
    h2, present, cols = cbl.decode_block(raw)
    assert h2['v'] == 2 and h2['ids'] == 'bitmap' and list(cols) == list(cbl.GLYPH_FIELDS)
    assert present.size == h2['n'] and present.all() and all(c.shape[0] == h2['n'] for c in cols.values())
    assert len(h2['bins']) == 16 and sum(e['n'] for e in h2['bins']) == h2['n']
    # the client-side enumeration (polygonToCells centre rule) reproduces n + xor per bin
    for e in h2['bins'][::5]:
        ids = np.sort(ca.ids_in_bbox(*cb.bin_bbox(e['i'], e['j'])))
        assert ids.size == e['n'] and f'{int(np.bitwise_xor.reduce(ids)):016x}' == e['ids_xor']
    # rows equal the v1 frames (same bins, same fields)
    body1, _, _, _ = cb.serve_block(bi, bj, list(cbl.GLYPH_FIELDS), 'delta_u64')
    _, frames = cb.decode_block(body1)
    k = 3; e = h2['bins'][k]; _, c1 = cb.decode(frames[k])
    assert np.array_equal(c1['elev_mean'], cols['elev_mean'][e['row0']:e['row0'] + e['n']])
    assert cbl.zdecompress(cbl.zcompress(raw)) == raw


def test_routes():
    import public_api
    c = public_api.app.test_client()
    bi, bj = _stored_block()
    w, s, e, n = cb.block_bbox(bi, bj)
    q = f'/api/v1/cells?bbox={w},{s},{e},{n}&format=bin&block=1&fields=' + ','.join(cbl.GLYPH_FIELDS)
    r = c.get(q, headers={'Accept-Encoding': 'zstd, gzip'})
    assert r.status_code == 200 and r.headers['Content-Encoding'] == 'zstd' and r.headers['X-Cells-Ids'] == 'bitmap'
    assert r.headers['ETag'].startswith('"B-') and r.headers.get('X-Cells-Epoch')
    hdr, present, cols = cbl.decode_block(cbl.zdecompress(r.data))
    assert hdr['ij'] == [bi, bj] and len(r.data) <= 1_200_000
    r2 = c.get(q, headers={'If-None-Match': r.headers['ETag']}); assert r2.status_code == 304
    r3 = c.get(q, headers={'Accept-Encoding': 'gzip'})
    assert r3.status_code == 200 and r3.headers['Content-Encoding'] == 'gzip'
    assert gzip.decompress(r3.data)[:4] == b'NEXB'
    r4 = c.get(q + '&ids=1', headers={'Accept-Encoding': 'gzip'})
    assert r4.status_code == 200 and r4.headers['ETag'] != r.headers['ETag']
    h4, frames = cb.decode_block(gzip.decompress(r4.data)); assert len(frames) == 16 and h4.get('v') is None
    r5 = c.get('/api/v1/cells/blocks?fields=' + ','.join(cbl.GLYPH_FIELDS))
    assert r5.status_code == 200
    m = r5.get_json(); row = list(zip(m['blocks']['bi'], m['blocks']['bj'])).index((bi, bj))
    assert m['blocks']['etag'][row] == r.headers['ETag'] and m['blocks']['bytes'][row] == len(r.data)
    assert c.get('/api/v1/cells/blocks?epoch=nope').status_code == 409
    assert c.get('/api/v1/cells/blocks?epoch=' + r.headers['X-Cells-Epoch']).status_code == 200
    r6 = c.get('/api/v1/cells/sample?format=json')
    assert r6.status_code in (200, 404)
    if r6.status_code == 200:
        r7 = c.get('/api/v1/cells/sample?res=10&format=bin&fields=glyph', headers={'Accept-Encoding': 'zstd'})
        assert r7.status_code == 200 and r7.headers['Content-Encoding'] == 'zstd' and r7.headers['ETag'].startswith('"S-r10-')
        hdr, cols = cb.decode(cbl.zdecompress(r7.data))
        assert hdr['sample_res'] == 10 and list(cols) == ['id'] + list(cbl.GLYPH_FIELDS) and hdr['n'] == cols['id'].size
        ids = cols['id']; assert np.all(np.diff(ids.astype(np.int64)) > 0) and cbl._sample_mask(ids, 10).all()
        assert c.get('/api/v1/cells/sample?res=10&format=bin&fields=glyph', headers={'If-None-Match': r7.headers['ETag']}).status_code == 304
    assert c.get('/api/v1/cells/sample?res=7').status_code == 400
    d = c.get('/api/v1/cells/dict').get_json(); assert 'sample' in d and 'blocks_v2' in d


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'):
            f(); print('ok', n)
