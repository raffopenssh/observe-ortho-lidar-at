#!/usr/bin/env python3
"""tree_qa: the chunk override (hidden apex rows dropped, tree_n / tree_h_max follow) and the
bin-generation fold.  Synthetic rows only — no corpus needed.

    python3 tests/test_tree_qa.py
"""
import os
import sys
import tempfile
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
    # nothing to hide / out-of-range index → input returned unchanged
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
        assert cb._gen(1, 2, [('00001', 'd', 0)], []) == base          # no store → generation unchanged
        con = tree_qa._conn()
        idx = np.array([3], '<u2').tobytes()
        tree_qa._commit_kg(con, '00001', 'sig', {77: idx}, {(1, 2): ('abc', np.array([5], np.uint64))}, 'd')
        con.close()
        assert cb._gen(1, 2, [('00001', 'd', 0)], []) != base
        assert cb._gen(1, 3, [('00001', 'd', 0)], []) == cb._base_gen(1, 3, [('00001', 'd', 0)], [])
        assert tree_qa.chunk_idx('00001', 77, 'd') == idx
        assert tree_qa.chunk_idx('00001', 77, 'other-file-version') is None
        assert tree_qa.chunk_idx('00001', 78) is None and tree_qa.kg_tok('00002') == ''
        tok = tree_qa.kg_tok('00001')
        tree_qa._snap.update(t=0.0, ver='x')                           # a second process: reload from disk
        assert tree_qa.kg_tok('00001') == tok and tree_qa.bin_tok(1, 2)
        con = tree_qa._conn(); tree_qa._commit_kg(con, '00001', None, {}, {}); con.close()
        assert cb._gen(1, 2, [('00001', 'd', 0)], []) == base and tree_qa.chunk_idx('00001', 77) is None
    finally:
        tree_qa.DB_PATH = old
        tree_qa._snap.update(t=0.0, ver='x')


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
