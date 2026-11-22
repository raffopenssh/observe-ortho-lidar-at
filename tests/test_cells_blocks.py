"""cells_blocks: centre-child rule, NEXB v2 codec + enumeration checksum, routes (block=1 default / ids=1, /cells/blocks, /cells/sample)."""
import sys, os, gzip, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import h3
import pytest
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
    if not full:
        pytest.skip('needs a fully stored 0.1° block on disk (python3 cells_bins.py --prewarm)')
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


def test_block_ragged_tree_heights():
    bi, bj = _stored_block()
    fields = [*cbl.GLYPH_FIELDS, 'tree_h']
    _, raw, _ = cbl.build_block(bi, bj, fields)
    hdr, present, cols = cbl.decode_block(raw)
    assert hdr['fields'] == [*cbl.GLYPH_FIELDS, 'tree_h_n', 'tree_h']
    counts, heights = cols['tree_h_n'], cols['tree_h']
    assert counts.size == hdr['n'] == present.size
    assert int(counts.sum()) == heights.size
    offsets = cb.ragged_offsets(counts)
    starts = np.zeros(heights.size, dtype=bool)
    starts[offsets[:-1][counts > 0]] = True
    # Ignore comparisons across row boundaries (including empty rows).
    assert np.all(heights[1:][~starts[1:]] >= heights[:-1][~starts[1:]])
    spec = next(c for c in hdr['columns'] if c['name'] == 'tree_h')
    assert spec['shape'] == 'ragged' and spec['counts'] == 'tree_h_n'


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


def test_slice_is_build():
    """The base set sliced from the tree_h build is byte-identical to its own build (same ETag)."""
    bi, bj = _stored_block()
    _, sup, _ = cbl.build_block(bi, bj, cbl.GLYPH_TREE_FIELDS)
    _, direct, _ = cbl.build_block(bi, bj, cbl.GLYPH_FIELDS)
    assert cbl.slice_block(sup, cbl.GLYPH_FIELDS)[1] == direct
    assert cbl.slice_block(sup, ['elev_mean', 'tree_h_n', 'tree_h'])[1] == cbl.build_block(bi, bj, ['elev_mean', 'tree_h'])[1]


def test_request_never_builds(monkeypatch, tmp_path):
    """Stale bins: stored previous generation → 200 + X-Cells-Stale (304 on its ETag), nothing
    stored → 503 + Retry-After; the block is queued; build_bin is never called; rebuild=1 ignored."""
    import public_api
    c = public_api.app.test_client()
    bi, bj = _stored_block()
    w, s, e, n = cb.block_bbox(bi, bj)
    q = f'/api/v1/cells?bbox={w},{s},{e},{n}&format=bin&block=1&fields='
    r0 = c.get(q + ','.join(cbl.GLYPH_TREE_FIELDS)); assert r0.status_code == 200
    monkeypatch.setattr(cb, 'WANT_DIR', tmp_path / 'want')
    monkeypatch.setattr(cb, 'build_bin', lambda *a, **k: (_ for _ in ()).throw(AssertionError('built in a request')))
    real = cb._gen
    monkeypatch.setattr(cb, '_gen', lambda g: real(g + '|test-bump'))      # every bin is stale now
    monkeypatch.setattr(cb, '_blob_lru', type(cb._blob_lru)('t', 8)); cbl._manifest_cache.clear()
    try:
        r = c.get(q + ','.join(cbl.GLYPH_TREE_FIELDS) + '&rebuild=1')
        if r0.headers.get('X-Cells-Stale'):
            pytest.skip('block already stale on disk')
        assert r.status_code == 200 and r.headers['X-Cells-Stale'] == '1' and r.headers['Retry-After']
        assert r.headers['ETag'] == r0.headers['ETag'] and 'max-age=%d' % cb.STALE_MAX_AGE in r.headers['Cache-Control']
        assert r.headers['X-Cells-Gen-Current'] != r.headers['X-Cells-Gen'] and cb.wanted_blocks() == [(bi, bj)]
        r2 = c.get(q + ','.join(cbl.GLYPH_TREE_FIELDS), headers={'If-None-Match': r.headers['ETag']})
        assert r2.status_code == 304 and r2.headers['X-Cells-Stale'] == '1'
        monkeypatch.setattr(cbl, '_superset', lambda *a, **k: None); monkeypatch.setattr(cbl, 'remember_fieldset', lambda f: None)
        r3 = c.get(q + 'tpi,tri')
        if r3.status_code == 200:
            assert r3.headers.get('X-Cells-Stale') == '1'      # someone stored exactly this set before
        else:
            assert r3.status_code == 503 and r3.headers['Retry-After'] == str(cb.BUILD_RETRY_S) and r3.get_json()['building']
        r4 = c.get(q + 'elev_mean&ids=1')                       # v1 frames: stored stale bins as they are
        assert r4.status_code == 200 and r4.headers['X-Cells-Stale'] == '1'
    finally:
        cbl._manifest_cache.clear()


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'):
            try:
                f(); print('ok', n)
            except pytest.skip.Exception as e:
                print('skip', n, e)
