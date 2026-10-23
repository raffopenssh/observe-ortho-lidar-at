"""python3 tests/test_safe_load.py — whitelist unpickler + numpy hook."""
import io, os, pickle, sys, tempfile, glob
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import safe_load


class Evil:
    def __reduce__(self):
        return (os.system, ('echo PWNED',))


def main():
    # 1) benign payloads round-trip
    import affine
    from object_segmentation import SegmentedObject
    obj = {'a': np.arange(4, dtype=np.float32), 's': np.float64(1.5), 't': affine.Affine(1, 0, 0, 0, -1, 0),
           'set': {1, 2}, 'd': {'k': [1, (2, 3)]}}
    assert safe_load.loads(pickle.dumps(obj))['a'].sum() == 6
    so = SegmentedObject.__new__(SegmentedObject)
    safe_load.loads(pickle.dumps(so))
    # 2) malicious payloads refused
    for bad in (Evil(), ):
        try:
            safe_load.loads(pickle.dumps(bad)); raise SystemExit('FAIL: evil unpickled')
        except safe_load.UnsafePickle as e:
            assert 'posix.system' in str(e) or 'nt.system' in str(e), e
    # 3) numpy hook: np.load(allow_pickle=True) refuses evil object arrays, allows dict payloads
    assert safe_load.install()
    with tempfile.TemporaryDirectory() as d:
        good = os.path.join(d, 'g.npz'); np.savez(good, data=np.array({'map': np.zeros((2, 2))}, dtype=object))
        assert np.load(good, allow_pickle=True)['data'].item()['map'].shape == (2, 2)
        badp = os.path.join(d, 'b.npz'); np.savez(badp, data=np.array(Evil(), dtype=object))
        try:
            np.load(badp, allow_pickle=True)['data']; raise SystemExit('FAIL: evil npz loaded')
        except safe_load.UnsafePickle:
            pass
    # 4) real checkpoint pickles if present
    n = 0
    for f in glob.glob('data/austria_processor/tile_checkpoints/*/tile_*.pkl')[:5] + glob.glob('/tmp/ck/tile_*.pkl'):
        with open(f, 'rb') as fh:
            safe_load.load(fh); n += 1
    print(f'OK (checked {n} real checkpoint pickles)')


if __name__ == '__main__':
    main()
