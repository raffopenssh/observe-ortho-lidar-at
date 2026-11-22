#!/usr/bin/env python3
"""tree_qa: the chunk override (hidden apex rows dropped, tree_n / tree_h_max follow) and the
bin-generation fold.  Synthetic rows only — no corpus needed.

    python3 tests/test_tree_qa.py
"""
import os
import sys
import tempfile
from collections import OrderedDict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import cells_api as ca        # noqa: E402
import cells_bins as cb       # noqa: E402
import tree_qa                # noqa: E402


def _word(h_m):
    return np.uint32(int(round(h_m * 4)) << 10)


def test_patch():
    obs = np.zeros(4, ca.OBS_DT)
    obs['id'] = [10, 11, 12, 13]
    obs['tn'] = [2, 0, 3, 1]
    obs['thx'] = [168, 255, 60, 127]          # raw: 84 m, none, 30 m, 63.5 m
    trees = np.array([_word(63.75), _word(20), _word(30), _word(12.25), _word(25), _word(63.75)], '<u4')
    hide = np.array([0, 5], '<u2').tobytes()   # the 63.75 m apex of cell 10 and the only apex of cell 13
    o2, t2 = tree_qa.patch(obs.tobytes(), trees.tobytes(), hide)
    o2 = np.frombuffer(o2, ca.OBS_DT); t2 = np.frombuffer(t2, '<u4')
    assert t2.tolist() == trees[[1, 2, 3, 4]].tolist()
    assert o2['tn'].tolist() == [1, 0, 3, 0], o2['tn']
    assert o2['thx'].tolist() == [40, 255, 60, 255], o2['thx']       # 20 m; untouched; untouched raw; none left
    assert int(o2['tn'].sum()) == t2.size
    other = [f for f in ca.OBS_DT.names if f not in ('tn', 'thx')]
    assert all(np.array_equal(o2[f], obs[f]) for f in other)
    # thx1: no hidden rows → tree_h_max still derived from the NET1 rows (list scale), tn / rows untouched
    o3, t3 = tree_qa.patch(obs.tobytes(), trees.tobytes(), None)
    o3 = np.frombuffer(o3, ca.OBS_DT)
    assert t3 == trees.tobytes() and o3['tn'].tolist() == [2, 0, 3, 1]
    assert o3['thx'].tolist() == [128, 255, 60, 128], o3['thx']
    assert all(np.array_equal(o3[f], obs[f]) for f in other)
    # tree_n sum does not address the rows (saturated cell) → stored thx kept
    sat = obs.copy(); sat['tn'] = [2, 0, 255, 1]
    assert tree_qa.patch(sat.tobytes(), trees.tobytes(), None)[0] == sat.tobytes()
    # a stored bin's own lists give the same column (cells_bins.thx_from_lists = the in-place upgrade)
    cols = {'tree_h_n': np.array([2, 0, 3, 1], '<u2'), 'tree_h': np.array([40, 128, 24, 50, 60, 128], np.uint8),
            'tree_n': o3['tn'], 'tree_h_max': obs['thx']}
    assert cb.thx_from_lists(cols).tolist() == o3['thx'].tolist()
    g0 = cb._gen_prev(1, 2, [], []); assert cb._gen(g0) != g0 and tree_qa.THX_ID.startswith('thx1')
    # out-of-range index → trees returned unchanged
    assert tree_qa.patch(obs.tobytes(), trees.tobytes(), np.array([99], '<u2').tobytes())[1] == trees.tobytes()
    # quantiser = the tree_h list's
    h = np.array([3.0, 12.25, 59.5, 63.75])
    assert np.array_equal(tree_qa.quant_h(h), np.clip(np.round(h * 2), 1, 254).astype(np.uint8))


def test_gen_fold_and_snapshot():
    d = tempfile.mkdtemp()
    old = tree_qa.DB_PATH
    tree_qa.DB_PATH = Path(d) / 'tree_qa.sqlite'
    try:
        tree_qa._snap.update(t=0.0, ver='x')
        base = cb._base_gen(1, 2, [('00001', 'd', 0)], [])
        assert cb._gen_prev(1, 2, [('00001', 'd', 0)], []) == base          # no store → generation unchanged
        con = tree_qa._conn()
        idx = np.array([3], '<u2').tobytes()
        tree_qa._commit_kg(con, '00001', 'sig', {77: idx}, {(1, 2): ('abc', np.array([5], np.uint64))}, 'd')
        con.close()
        assert cb._gen_prev(1, 2, [('00001', 'd', 0)], []) != base
        assert cb._gen_prev(1, 3, [('00001', 'd', 0)], []) == cb._base_gen(1, 3, [('00001', 'd', 0)], [])
        assert tree_qa.chunk_idx('00001', 77, 'd') == idx
        assert tree_qa.chunk_idx('00001', 77, 'other-file-version') is None
        assert tree_qa.chunk_idx('00001', 78) is None and tree_qa.kg_tok('00002') == ''
        tok = tree_qa.kg_tok('00001')
        tree_qa._snap.update(t=0.0, ver='x')                           # a second process: reload from disk
        assert tree_qa.kg_tok('00001') == tok and tree_qa.bin_tok(1, 2)
        con = tree_qa._conn(); tree_qa._commit_kg(con, '00001', None, {}, {}); con.close()
        assert cb._gen_prev(1, 2, [('00001', 'd', 0)], []) == base and tree_qa.chunk_idx('00001', 77) is None
    finally:
        tree_qa.DB_PATH = old
        tree_qa._snap.update(t=0.0, ver='x')


def test_snapshot_race():
    """A worker with a stale (< 30 s old) snapshot meets a bin / block the refresher of ANOTHER
    process just patched to the new generation: no rebuild, no overwrite, new generation served."""
    import hashlib
    import json
    import time
    import cells_blocks
    d = Path(tempfile.mkdtemp())
    saved = (tree_qa.DB_PATH, cb.BIN_DIR, cb.bin_inputs, cb.build_bin, cells_blocks.BLOCK_DIR, cells_blocks.store_block, tree_qa.sync)
    saved_fs = cells_blocks.FIELDSETS_PATH; cells_blocks.FIELDSETS_PATH = d / 'fieldsets.json'   # never the live registry
    tree_qa.DB_PATH = d / 'tree_qa.sqlite'; cb.BIN_DIR = d / 'bins'; cells_blocks.BLOCK_DIR = d / 'nexb'
    I, J = 4, 8                                   # block (1, 2)
    parts = [('00001', 'd', 0)]
    builds = []; stores = []
    try:
        cb.bin_inputs = lambda i, j: {'i': i, 'j': j, 'bbox': cb.bin_bbox(i, j), 'kgs': ['00001'], 'on_disk': ['00001'],
                                       'missing': [], 'partial_files': [], 'gen': cb._gen(cb._gen_prev(i, j, parts, [])), 'gen_prev': cb._gen_prev(i, j, parts, []), 'epoch': None}

        def fake_build(i, j, inp=None):
            builds.append((i, j, inp['gen']))
            return {'hdr': {'gen': inp['gen'], 'algo': cb.ALGO, 'n': 1, 'built_by': 'stale worker'},
                    'cols': OrderedDict(id=np.array([5], '<u8'))}
        cb.build_bin = fake_build
        cells_blocks.store_block = lambda *a, **k: stores.append(a) or (_ for _ in ()).throw(AssertionError('block rebuilt'))
        tree_qa._conn().close()                                        # empty store
        tree_qa._snap.update(t=0.0, ver='x')
        old_gen = cb.bin_inputs(I, J)['gen']
        assert old_gen == cb._gen(cb._base_gen(I, J, parts, [])) and tree_qa._snap['ver'] == tree_qa.version()
        # --- the refresher (another process): commit the KG, then patch the stored bin + block
        con = tree_qa._conn()
        con.execute("INSERT INTO hid VALUES ('00001', 77, ?)", (np.array([3], '<u2').tobytes(),))
        con.execute("INSERT INTO binkg VALUES (?, ?, '00001', 'abc')", (I, J))
        con.execute("INSERT INTO kg VALUES ('00001', 'sig', 'tok', 1, 1, 0, ?)", ('d' + tree_qa._DIG_SUF,))
        con.commit(); con.close()
        new_gen = cb._gen(hashlib.sha256(f"{cb._base_gen(I, J, parts, [])}|tq|{tree_qa._bin_tok({'00001': 'abc'})}".encode()).hexdigest()[:24])
        cb._store_blob(I, J, {'gen': new_gen, 'algo': cb.ALGO, 'n': 1, 'built_by': 'refresher'}, OrderedDict(id=np.array([5], '<u8')))
        blob = cb._path(I, J).read_bytes()
        fields = list(cb.DEFAULT_FIELDS)
        gens = [new_gen if ij == (I, J) else cb._gen(cb._base_gen(*ij, parts, [])) for ij in cb.block_bins(1, 2)]
        zp, mp = cells_blocks._paths(1, 2, fields)
        zp.parent.mkdir(parents=True, exist_ok=True); zp.write_bytes(b'patched-block')
        meta = {'gen': cb.block_gen(gens), 'algo': cells_blocks.ALGO, 'zstd_level': cells_blocks.ZSTD_LEVEL, 'bytes': 13,
                'etag': cells_blocks.block_etag(cb.block_gen(gens), fields)}
        mp.write_text(json.dumps(meta))
        # --- the worker: snapshot still inside its 30 s TTL → expects the OLD generation
        assert time.time() - tree_qa._snap['t'] < tree_qa._STAT_S and cb.bin_inputs(I, J)['gen'] == old_gen
        # without the guard the race is real: old filter rebuilt + the patched blob overwritten
        tree_qa.sync = lambda: False
        hdr, _, _ = cb.load_bin(I, J)
        assert builds == [(I, J, old_gen)] and hdr['built_by'] == 'stale worker' and cb._path(I, J).read_bytes() != blob
        tree_qa.sync = saved[6]; builds.clear(); cb._blob_lru.clear()
        cb._store_blob(I, J, {'gen': new_gen, 'algo': cb.ALGO, 'n': 1, 'built_by': 'refresher'}, OrderedDict(id=np.array([5], '<u8')))
        assert cb.bin_inputs(I, J)['gen'] == old_gen                  # still stale
        # with the guard
        hdr, _, inp = cb.load_bin(I, J)
        assert builds == [], builds
        assert hdr['gen'] == new_gen == inp['gen'] and hdr['built_by'] == 'refresher'
        assert cb._path(I, J).read_bytes() == blob
        assert tree_qa.chunk_idx('00001', 77, 'd') is not None        # chunk LRU key follows the reloaded snapshot
        # block: stale snapshot again (store rolled forward by "the refresher" once more)
        tree_qa._snap.update(t=time.time(), ver='stale', bin={}, binkg={})
        assert cells_blocks.current_gen(1, 2)[0] != meta['gen']
        body, _, etag, m = cells_blocks.serve_block(1, 2, fields)
        assert body == b'patched-block' and etag == meta['etag'] and not stores
        # a snapshot that is current triggers no reload and a real mismatch still builds
        assert tree_qa.sync() is False
        cb._blob_lru.clear(); cb._path(I, J).unlink()
        cb.load_bin(I, J)
        assert builds == [(I, J, new_gen)]
    finally:
        (tree_qa.DB_PATH, cb.BIN_DIR, cb.bin_inputs, cb.build_bin, cells_blocks.BLOCK_DIR, cells_blocks.store_block, tree_qa.sync) = saved
        cells_blocks.FIELDSETS_PATH = saved_fs
        tree_qa._snap.update(t=0.0, ver='x'); cb._blob_lru.clear()
        with cb._stored_gen_guard:
            cb._stored_gen_cache.clear()


def test_tree_cols_shared():
    """build_bin and patch_tree_cols derive the apex columns through one function."""
    import inspect
    assert 'tree_cols_for(' in inspect.getsource(cb.build_bin)
    assert 'tree_cols_for(' in inspect.getsource(cb.patch_tree_cols)


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_') and callable(f):
            f(); print('ok', n)
    print('all ok')
