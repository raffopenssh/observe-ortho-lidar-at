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
