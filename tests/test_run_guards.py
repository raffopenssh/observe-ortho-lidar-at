"""Guards added after the 2026 run (docs/qc-lessons.md → What the next run should do differently):
national clip at read time, BEV source identity, raster sidecars in checkpoint tars."""
import os, sys, json, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import numpy as np
from rasterio.transform import from_origin


def test_clip_national():
    import raster_io, austria_mask
    st = austria_mask._load()
    assert st, 'outline file missing'
    # a point on the outline: a 600 m tile around it has pixels on both sides
    x, y = (float(v) for v in st['line'].interpolate(0.37, normalized=True).coords[0])
    n = 600
    tf = from_origin(round(x) - n // 2, round(y) + n // 2, 1.0, 1.0)
    d = {'dtm': np.ones((n, n), np.float32), 'dsm': np.ones((n, n), np.float32) * 2,
         'ndsm': np.ones((n, n), np.float32), 'mask': np.ones((n, n), bool), 'transform': tf}
    ro = np.load  # arrays may be read-only mmaps: the clip must copy
    d['dtm'].setflags(write=False)
    k = raster_io.clip_national(d, 100.0)
    assert 0 < k < n * n, k
    assert int(np.isnan(d['dtm']).sum()) == k == int((~d['mask']).sum()) == d['outline_clip_px']
    assert np.isnan(d['ndsm'][~d['mask']]).all() and np.isfinite(d['dsm'][d['mask']]).all()
    # kept = inside OR within 100 m of the outline
    keep = austria_mask.grid_mask(tf.c, tf.f, n, n, 1.0, 100.0)
    assert (keep == d['mask']).all()
    k0 = raster_io.clip_national(dict(d, dtm=np.ones((n, n), np.float32), mask=np.ones((n, n), bool)), 0.0)
    assert k0 > k
    # inland tile: untouched, same objects
    c = st['poly'].representative_point()
    tf2 = from_origin(round(c.x), round(c.y), 1.0, 1.0)
    a = np.ones((50, 50), np.float32)
    d2 = {'dtm': a, 'dsm': a, 'ndsm': a, 'mask': np.ones((50, 50), bool), 'transform': tf2}
    assert raster_io.clip_national(d2, 100.0) == 0 and d2['dtm'] is a
    # off by default: the interactive tier and the detectors read unclipped
    assert raster_io.OUTLINE_CLIP_M is None and raster_io.RECORD_SOURCES is False
    print('ok clip_national', k, k0)


def test_source_identity():
    import raster_io
    a = [{'layer': 'DTM', 'file': 'a.tif', 'etag': '1'}, {'layer': 'DSM', 'file': 'b.tif', 'etag': '1'}]
    b = [{'layer': 'DTM', 'file': 'a.tif', 'etag': '1'}, {'layer': 'DSM', 'file': 'b.tif', 'etag': '2'}]
    assert raster_io.sources_changed(a, a) == [] and raster_io.sources_changed(a, b) == ['b.tif']
    assert raster_io.sources_changed(a, [{'layer': 'DSM', 'file': 'b.tif'}]) == []      # unknown ≠ changed
    assert raster_io.sources_changed(None, b) == []
    import austria_processor as ap
    assert ap._compact_sources([{'layer': 'DTM', 'file': 'a.tif', 'etag': '1', 'size': 3, 'dataset': 'x'}]) == \
        [{'file': 'a.tif', 'etag': '1'}]
    assert raster_io.sources_changed(ap._compact_sources(a), b) == ['b.tif']   # compact vs full form
    print('ok source_identity')


def test_chkpt_rasters():
    import tile_checkpoint_registry as reg, tile_raster_sidecar as trs
    with tempfile.TemporaryDirectory() as td:
        src, dst = Path(td) / 'src', Path(td) / 'dst'
        rng = np.random.default_rng(1)
        ident = [{'layer': 'DTM', 'file': 'a.tif', 'etag': 'e1'}]
        want = {}
        for t in (0, 1, 2):
            (src / '00001').mkdir(parents=True, exist_ok=True)
            (src / '00001' / f'tile_{t}.pkl').write_bytes(b'pickle%d' % t)
            if t == 2:
                continue                       # tile without a sidecar
            h, w = 300 + t, 257
            dtm = (500 + np.cumsum(rng.normal(0, .05, (h, w)), 1)).astype(np.float32)
            dsm = dtm + np.abs(rng.normal(0, 3, (h, w))).astype(np.float32)
            dtm[:5] = np.nan
            valid = np.isfinite(dtm); valid[:, :7] = False
            ndsm = np.where(valid, dsm - dtm, np.nan).astype(np.float32)
            if t == 1:
                ndsm[50, 50] = 99.0            # not derivable → shipped in full
            trs.persist_dtm_dsm(src, '00001', t, {'dtm': dtm, 'dsm': dsm, 'ndsm': ndsm, 'sources': ident,
                                                  'transform': from_origin(4e6, 2.7e6, 1, 1), 'shape': (h, w)})
            want[t] = (dtm, dsm, ndsm)
        reg.CKPT_ROOT = src
        blob = reg._bundle_tile_pickles('00001')
        assert blob and len(blob) < 3 * 300 * 257 * 4 * 2
        reg.CKPT_ROOT = dst
        assert reg._unpack_bundle(blob, '00001') == 3
        assert reg.LAST_UNPACK == {'rasters': 2, 'identity': 0}, reg.LAST_UNPACK
        for t, (dtm, dsm, ndsm) in want.items():
            got = trs.load_dtm_dsm(dst, '00001', t)
            for k, ref in (('dtm', dtm), ('dsm', dsm), ('ndsm', ndsm)):
                assert got[k].dtype == np.float32 and np.array_equal(got[k], ref, equal_nan=True), (t, k)
            assert trs.read_meta(dst, '00001', t)['bev_source'] == ident
        assert trs.load_dtm_dsm(dst, '00001', 2) is None
        # no room for rasters: pickles + identity only, a later re-read keeps the first identity
        reg.CKPT_ROOT = src
        cap, reg.MAX_BUNDLE_BYTES = reg.MAX_BUNDLE_BYTES, len(reg._tar_gz(
            [(p.name, p) for p in sorted((src / '00001').glob('tile_*.pkl'))])) + reg.RASTER_MARGIN_BYTES + 2000
        try:
            small = reg._bundle_tile_pickles('00001')
        finally:
            reg.MAX_BUNDLE_BYTES = cap
        dst2 = Path(td) / 'dst2'; reg.CKPT_ROOT = dst2
        assert reg._unpack_bundle(small, '00001') == 3 and reg.LAST_UNPACK == {'rasters': 0, 'identity': 2}
        assert trs.load_dtm_dsm(dst2, '00001', 0) is None
        dtm, dsm, ndsm = want[0]
        trs.persist_dtm_dsm(dst2, '00001', 0, {'dtm': dtm, 'dsm': dsm, 'ndsm': ndsm,
                                               'sources': [{'layer': 'DTM', 'file': 'a.tif', 'etag': 'e2'}],
                                               'transform': from_origin(4e6, 2.7e6, 1, 1), 'shape': dtm.shape})
        m = trs.read_meta(dst2, '00001', 0)
        assert m['bev_source'] == ident and m['bev_source_reread'][0]['etag'] == 'e2'
        # an old reader (pickles by basename) still works on a new bundle: covered by the member layout
        import tarfile, gzip, io
        names = tarfile.open(fileobj=gzip.GzipFile(fileobj=io.BytesIO(blob))).getnames()
        assert names[:3] == ['tile_0.pkl', 'tile_1.pkl', 'tile_2.pkl'] and 'tile_0/raster/dtm.f32z' in names
    print('ok chkpt_rasters', len(blob), len(small))


if __name__ == '__main__':
    test_clip_national(); test_source_identity(); test_chkpt_rasters()
    print('ALL OK')


def test_neighbour_registration():
    import neighbour_registration as nr
    rng = np.random.default_rng(3)
    # smooth synthetic terrain sampled at 5 m, block-averaged to 25 m at a chosen offset
    yy, xx = np.mgrid[0:3000:5, 0:4000:5].astype(np.float64)
    z5 = (400 + 60 * np.sin(xx / 310.0) * np.cos(yy / 270.0) + 25 * np.sin((xx + 2 * yy) / 90.0)).astype(np.float32)

    def grid(x0, y0, cols, rows, dx=0.0, dy=0.0):
        """25 m cells of the terrain whose rasters sit (dx, dy) m off: cell (r, c) declared at
        x0 + c·25 holds terrain from x0 + c·25 + dx (dy: north positive)."""
        c5 = int((x0 + dx) / 5); r5 = int((3000 - (y0 + dy)) / 5)
        blk = z5[r5:r5 + rows * 5, c5:c5 + cols * 5].reshape(rows, 5, cols, 5).mean((1, 3))
        return {'elev': blk.astype(np.float32), 'x0': float(x0), 'y0': float(y0)}

    A = grid(500, 2500, 60, 60)
    B = grid(1500, 2700, 70, 80)                       # overlaps A's east part
    p = nr.compare(A, B)
    assert p['whole'][1] == 'aligned' and p['whole'][2] == (0.0, 0.0), p['whole']
    A5 = grid(500, 2500, 60, 60, dx=5.0, dy=-5.0)      # ours holds terrain from 5 m SE = rasters (−5, +5) off
    p = nr.compare(A5, B)
    assert p['whole'][1] == 'shifted' and p['whole'][2] == (5.0, -5.0), p['whole']
    # one tile of ours shifted, the rest fine → tiles-shifted, tile named
    Am = grid(500, 2500, 60, 60)
    Am['elev'][0:30, 40:60] = A5['elev'][0:30, 40:60]
    tiles = [(1, 0, 40, 30, 60), (2, 30, 40, 60, 60), (3, 0, 0, 60, 40)]
    res = nr.summarise({'B': nr.compare(Am, B, tiles)}, len(tiles))
    assert res['verdict'] == 'tiles-shifted' and list(res['tiles']) == [1], res
    assert nr.summarise({'B': nr.compare(A, B, tiles)}, 3)['verdict'] == 'aligned'
    # flat ground: no signal, never a finding
    F = {'elev': np.full((60, 60), 200, np.float32), 'x0': 500.0, 'y0': 2500.0}
    F2 = {'elev': np.full((80, 70), 200, np.float32), 'x0': 1500.0, 'y0': 2700.0}
    assert nr.summarise({'B': nr.compare(F, F2)})['verdict'] == 'no-signal'
    # other terrain altogether → mismatch
    C = dict(B, elev=B['elev'] + rng.normal(0, 30, B['elev'].shape).astype(np.float32))
    assert nr.summarise({'C': nr.compare(A, C)}).get('mismatch') == ['C']
    # end to end through v2_verify with injected neighbours; network-free
    import v21_products as v21, v2_verify
    def doc(code, g, **kw):
        return dict({'kg_code': code, 'terrain': {'grid25': v21.encode_terrain_grid25(g['elev'], None, g['x0'], g['y0'])
                                                   if hasattr(v21, 'encode_terrain_grid25') else None}}, **kw)
    if hasattr(v21, 'encode_terrain_grid25'):
        rep = v2_verify.Report()
        v2_verify.check_neighbour_registration('A', doc('A', A5), rep, fetch=lambda c: doc('B', B), cand=['B'])
        assert rep['ok'] and rep['warnings'] and 'shifted' in rep['warnings'][0], rep
        rep = v2_verify.Report()
        v2_verify.check_neighbour_registration('A', doc('A', A), rep, fetch=lambda c: doc('B', B), cand=['B'])
        assert not rep['warnings'], rep
        # a neighbour with a measured offset is compared on its corrected origin
        Bs = grid(1500, 2700, 70, 80, dx=5.0, dy=-5.0)
        nr._offsets = lambda: {'offsets': {'B': [-5.0, 5.0]}}
        rep = v2_verify.Report()
        v2_verify.check_neighbour_registration('A', doc('A', A), rep,
                                               fetch=lambda c: doc('B', Bs, upgraded_from={'x': 1}), cand=['B'])
        assert not rep['warnings'], rep
    print('ok neighbour_registration')


if __name__ == '__main__':
    test_neighbour_registration()
    print('ALL OK')
