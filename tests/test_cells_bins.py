"""NEX1 bin export (cells_bins.py): codec round-trip, bin alignment, per-cell
stats, and — when a product is on disk — a live build with its invariants."""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import cells_api  # noqa: E402
import cells_bins as cb  # noqa: E402


def test_bin_of():
    assert cb.bin_of(16.35, 48.225, 16.375, 48.25) == (654, 1929)
    assert cb.bin_of(16.351, 48.225, 16.375, 48.25) is None
    assert cb.bin_of(16.35, 48.225, 16.4, 48.25) is None
    assert cb.bin_bbox(654, 1929) == (16.35, 48.225, 16.375, 48.25)


def test_codec_roundtrip():
    n = 5
    ids = np.array([10, 11, 13, 20, 21], '<u8')
    cols = {'id': ids, 'obs': np.array([1, 0, 1, 1, 0], np.uint8),
            'elev_mean': np.array([1, 2, 3, 65535, 5], '<u2'),
            'cover': np.arange(n * 9, dtype=np.uint8).reshape(n, 9)}
    hdr = {'n': n, 'gen': 'g', 'epoch': '2026-10'}
    for enc in ('delta_u64', 'u64'):
        blob = cb.encode(hdr, cols, ['elev_mean', 'cover'], enc)
        h, c = cb.decode(blob)
        assert h['ids'] == enc and h['fields'] == ['elev_mean', 'cover']
        assert list(c) == ['id', 'elev_mean', 'cover']
        assert (c['id'] == ids).all() and (c['elev_mean'] == cols['elev_mean']).all()
        assert c['cover'].shape == (n, 9) and (c['cover'] == cols['cover']).all()


def test_per_cell_stats():
    ci = np.array([0, 0, 0, 2, 2])
    v = np.array([1.0, 5.0, 3.0, 2.0, 4.0])
    mx, p50, p90, cnt = cb._per_cell_stats(ci, v, 4)
    assert cnt.tolist() == [3, 0, 2, 0]
    assert mx[0] == 5 and p50[0] == 3 and np.isnan(mx[1]) and mx[2] == 4 and p50[2] == 2


def test_u8h():
    v = np.array([0.1, 3.0, 200.0, np.nan])
    present = np.array([True, True, True, False]); unknown = np.array([False, False, False, True])
    assert cb._u8h(v, present, unknown).tolist() == [1, 6, 254, 255]


def test_fields_declared():
    assert 'id' in cb.FIELDS
    for f in ('bld_h_max', 'bld_h_p50', 'stories_max', 'roof_dom', 'canopy_h_p90', 'aspect_deg',
              'slope_mean', 'slope_p90', 'elev_mean'):
        assert f in cb.FIELDS, f
    for f in cells_api.OBS_PUBLIC:
        assert f in cb.FIELDS, f
    assert all(f in cb.FIELDS for f in cb.DEFAULT_FIELDS)


def test_live_bin():
    if not cells_api.INDEX_DB.exists() or not list(cells_api.NE_DIR.glob('*.h3b')):
        return
    m = cb.manifest()
    if not m['n']:
        return
    b = m['bins']
    k = max(range(m['n']), key=lambda i: b['kgs_n'][i])        # the densest bin
    i, j = b['i'][k], b['j'][k]
    hdr, cols, inp = cb.load_bin(i, j)
    assert hdr['gen'] == b['gen'][k] == inp['gen']
    ids = cols['id']
    assert (np.diff(ids.astype(np.int64)) > 0).all()            # sorted, unique
    assert hdr['n'] == ids.size <= cb.MAX_BIN_CELLS
    obs = cols['obs'].astype(bool)
    # unknown (255) exactly where unobserved, for the half-metre building columns
    for f in ('bld_h_max', 'bld_h_p50', 'stories_max', 'canopy_h_p90', 'struct_h_max'):
        assert ((cols[f] == 255) == ~obs).all(), f
    # terrain covers at least every observed cell
    assert (cols['terrain_src'][obs] > 0).all()
    assert (cols['elev_mean'][cols['terrain_src'] > 0] != 65535).all()
    a = cols['aspect_deg']
    assert ((a <= 179) | (a >= 254)).all()
    assert set(np.unique(cols['roof_dom']).tolist()) <= {0, 1, 2}
    # the request-path etag is derived without touching the blob
    etag, _ = cb.quick_etag(i, j, ['elev_mean'], 'delta_u64')
    assert etag == cb.bin_etag(hdr['gen'], ['elev_mean'], 'delta_u64')


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'):
            f(); print('ok', n)


def test_block_geometry():
    assert cb.block_of(16.3, 48.2, 16.4, 48.3) == (163, 482)
    assert cb.block_of(16.3, 48.2, 16.4, 48.325) is None
    assert cb.block_of(16.325, 48.2, 16.425, 48.3) is None
    bins = cb.block_bins(163, 482)
    assert len(bins) == 16 and bins[0] == (652, 1928) and bins[1] == (653, 1928) and bins[4] == (652, 1929)
    assert bins[15] == (655, 1931)
    assert cb.bin_bbox(*bins[0])[:2] == cb.block_bbox(163, 482)[:2]
    assert cb.bin_bbox(*bins[15])[2:] == cb.block_bbox(163, 482)[2:]


def test_block_container_roundtrip(monkeypatch):
    """serve_block = NEXB header + 16 frames, each byte-identical to encode() of the bin."""
    def fake_load(i, j, rebuild=False, need=None):
        n = 3
        ids = np.array([i * 100 + j * 10 + k for k in range(n)], '<u8')
        cols = {'id': ids, 'obs': np.ones(n, np.uint8), 'elev_mean': np.full(n, i, '<u2')}
        hdr = {'n': n, 'gen': f'g{i}_{j}', 'epoch': '2026-10', 'kgs': [f'{i}'], 'complete': True}
        return hdr, cols, {'missing': [], 'partial_files': []}
    monkeypatch.setattr(cb, 'load_bin', fake_load)
    monkeypatch.setattr(cb.ca, '_with_epoch', lambda hx, v=None: dict(hx, **({'X-Cells-Epoch': v} if v else {})))
    body, hx, etag, entries = cb.serve_block(163, 482, ['elev_mean'], 'delta_u64')
    assert body[:4] == b'NEXB' and etag.startswith('"B-') and hx['X-Cells-Block'] == '163,482'
    hdr, frames = cb.decode_block(body)
    assert len(frames) == 16 and [(e['i'], e['j']) for e in hdr['bins']] == cb.block_bins(163, 482)
    for e, fr in zip(hdr['bins'], frames):
        h, c, _ = fake_load(e['i'], e['j'])
        assert fr == cb.encode(h, c, ['elev_mean'], 'delta_u64')
        h2, c2 = cb.decode(fr)
        assert h2['gen'] == e['gen'] and c2['elev_mean'].tolist() == [e['i']] * 3
    assert hdr['gen'] == cb.block_gen([e['gen'] for e in hdr['bins']])
    assert 'X-Cells-Partial' not in hx


def test_stored_gen_header_peek(tmp_path, monkeypatch):
    import zlib
    monkeypatch.setattr(cb, 'BIN_DIR', tmp_path)
    monkeypatch.setattr(cb, '_path', lambda i, j: tmp_path / f'{i}_{j}.nex')
    cb._stored_gen_cache.clear()
    n = 20000
    cols = {'id': np.arange(n, dtype='<u8'), 'obs': np.ones(n, np.uint8), 'cover': np.random.randint(0, 255, (n, 9)).astype(np.uint8)}
    hdr = {'n': n, 'gen': 'abc123', 'algo': cb.ALGO, 'kgs': ['1'] * 50}
    (tmp_path / '1_2.nex').write_bytes(zlib.compress(cb.encode(hdr, cols, None, 'delta_u64'), 6))
    assert cb.stored_gen(1, 2) == 'abc123'
    assert cb.stored_gen(1, 3) is None
    hdr['gen'] = 'zzz'
    (tmp_path / '1_2.nex').write_bytes(zlib.compress(cb.encode(hdr, cols, None, 'delta_u64'), 6))
    assert cb.stored_gen(1, 2) == 'zzz'


def test_ragged_heights_and_take():
    cell = np.array([2, 0, 2, 2, 0])
    h = np.array([10.0, 3.0, 4.0, 30.0, 12.25])
    n, blob = cb.ragged_heights(cell, h, 4)
    assert n.tolist() == [2, 0, 3, 0] and n.dtype == np.dtype('<u2')
    assert blob.tolist() == [6, 24, 8, 20, 60]            # ×0.5 m, ascending within cell
    off = cb.ragged_offsets(n)
    assert off.tolist() == [0, 2, 2, 5, 5]
    # mask subset
    n2, b2 = cb.ragged_take(n, blob, np.array([True, False, True, False]))
    assert n2.tolist() == [2, 3] and b2.tolist() == [6, 24, 8, 20, 60]
    # index permutation
    n3, b3 = cb.ragged_take(n, blob, np.array([2, 3, 0]))
    assert n3.tolist() == [3, 0, 2] and b3.tolist() == [8, 20, 60, 6, 24]
    assert cb.ragged_take(n, blob, np.array([1, 3]))[1].size == 0


def test_ragged_codec_roundtrip():
    ids = np.array([10, 11, 13], '<u8')
    cols = {'id': ids, 'obs': np.array([1, 0, 1], np.uint8), 'tree_h_n': np.array([2, 0, 1], '<u2'),
            'tree_h': np.array([6, 24, 60], np.uint8)}
    hdr = {'n': 3, 'gen': 'g', 'epoch': '2026-10'}
    assert cb.with_ragged_counts(['obs', 'tree_h']) == ['obs', 'tree_h_n', 'tree_h']
    assert cb.parse_fields('tree_h,obs') == ['tree_h_n', 'tree_h', 'obs']
    blob = cb.encode(hdr, cols, ['obs', 'tree_h'])
    h, c = cb.decode(blob)
    assert h['fields'] == ['obs', 'tree_h_n', 'tree_h']
    spec = [x for x in h['columns'] if x['name'] == 'tree_h'][0]
    assert spec['shape'] == 'ragged' and spec['counts'] == 'tree_h_n' and spec['bytes'] == 3
    assert c['tree_h'].tolist() == [6, 24, 60] and c['tree_h_n'].tolist() == [2, 0, 1]
    # without tree_h nothing ragged is emitted
    h, c = cb.decode(cb.encode(hdr, cols, ['obs']))
    assert list(c) == ['id', 'obs']
