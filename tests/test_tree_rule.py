#!/usr/bin/env python3
"""tree_rule: one height-rule function for /trees/bbox (ne_trees) and the NE builder (ne_h3).

* grouped evaluation == per-chunk evaluation;
* a file built WITH the rule == the same file built without it + ``tree_qa.patch`` of the apices
  the rule hides (NEO1 / NET1 bytes) — so both file generations give the same cell products;
* header marker (``net1.tree_rule``), ``built_with``.

    python3 tests/test_tree_rule.py        (the builder part needs one data/ne/*.h3b; skipped without)
"""
import inspect
import os
import sys
import tempfile
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import ne_h3                  # noqa: E402
import json                    # noqa: E402
import tree_rule              # noqa: E402


def test_one_function():
    rng = np.random.default_rng(3)
    h = np.round(rng.uniform(3, 40, 4000) * 4) / 4
    g = rng.integers(0, 60, 4000).astype(np.uint64)
    h[rng.integers(0, 4000, 25)] = rng.choice([45.0, 50.25, 59.75, 60.0, 63.75], 25)
    m = tree_rule.keep_mask(h, g)
    ref = np.ones(h.size, bool)
    for k in np.unique(g):
        ref[g == k] = tree_rule.keep_mask(h[g == k])
    assert np.array_equal(m, ref) and not m.all()
    assert tree_rule.keep_mask(np.array([63.75]))[0] == False and tree_rule.keep_mask(np.array([50.0]))[0]  # noqa: E712
    assert tree_rule.keep_mask(np.zeros(0), np.zeros(0, np.uint64)).size == 0
    import ne_trees, tree_qa
    assert 'tree_rule.chunk_keep(' in inspect.getsource(ne_trees._decode_chunk_uncached)
    import tree_qa
    assert tree_qa.RULE_ID == tree_rule.RULE_ID and tree_qa.quant_h is tree_rule.quant_h
    mk = tree_rule.marker({'8a1f8e333717fff': [2, 5]})
    assert json.loads(json.dumps(mk)) == mk and mk['hidden'] == 2
    h = {'net1': {'digest': 'd1', 'tree_rule': mk}}
    assert tree_rule.built_with(h) and tree_rule.marked_chunks(h) == {0x8a1f8e333717fff}
    assert tree_rule.hidden_rows(h, 0x8a1f8e333717fff).tolist() == [2, 5] and tree_rule.hidden_rows(h, 7).size == 0
    # header verdict wins over a re-evaluation (63.75 m at row 0 is NOT listed → kept; rows 2, 5 hidden)
    assert tree_rule.chunk_keep(h, 0x8a1f8e333717fff, np.array([63.75, 5, 5, 5, 5, 5.])).tolist() == [True, True, False, True, True, False]
    assert json.loads(json.dumps(h)) == h                       # parsing left the header plain JSON
    for hh in ({'net1': {'rows_n': 1}}, None, {'net1': {'tree_rule': dict(mk, id='older')}}):
        assert not tree_rule.built_with(hh) and tree_rule.hidden_rows(hh, 1) is None and tree_rule.marked_chunks(hh) is None
        assert tree_rule.chunk_keep(hh, 1, np.array([63.75, 5.0])).tolist() == [False, True]
    # the drop-at-build marker of fdff4ff (no rows): nothing left to hide, not evaluated again
    lg = {'net1': {'digest': 'd2', 'tree_rule': {'id': tree_rule.RULE_ID, 'dropped': 3}}}
    assert tree_rule.chunk_keep(lg, 1, np.array([63.75, 5.0])).all()


def _build(declared, apex, tf, n=400):
    seg = np.zeros((n, n), np.uint8); nd = np.full((n, n), 20.0, np.float32); lab = np.zeros((n, n), np.int32)
    return ne_h3.build_observed(declared, seg, nd, lab, [], tf, {}, apex_rows=apex, kg=declared.header.get('kg', ''))


def test_writer_marks_rows_file_stays_raw():
    files = sorted(ne_h3.NE_DIR.glob('*.h3b'), key=lambda p: p.stat().st_size)
    if not files:
        print('  (no data/ne/*.h3b — builder part skipped)'); return
    from affine import Affine
    import tree_qa
    declared = ne_h3.unpack_sections(ne_h3.nec1_section_from_h3b(files[min(20, len(files) - 1)]))[0]
    cids = declared.cell_ids()
    ce, cn = ne_h3.cell_centres_3035(cids)
    x0 = float(np.median(ce)) - 200; y1 = float(np.median(cn)) + 200
    tf = Affine(1.0, 0, x0, 0, -1.0, y1)
    rng = np.random.default_rng(5)
    apex = [{'e': x0 + float(x), 'n': y1 - float(y), 'h_m': float(h), 'crown_area_m2': 12.0}
            for x, y, h in zip(rng.uniform(5, 395, 900), rng.uniform(5, 395, 900), rng.uniform(3, 32, 900))]
    for k, h in enumerate([62.0, 70.0, 51.3, 47.0, 58.9, 45.2, 49.0, 64.1]):      # masts / spikes
        apex.append({'e': x0 + 30 + 45 * k, 'n': y1 - 40 - 40 * k, 'h_m': h, 'crown_area_m2': 5.0})
    obs = _build(declared, apex, tf)
    # the builder is non-destructive: every apex is in NET1 (realistic=0 = the raw inventory)
    w_all = np.frombuffer(obs['net1_raw'], '<u4')
    assert 0 < w_all.size <= len(apex) and w_all.size == int(obs['tree_n'].sum()) == obs['summary']['observed']['trees_n']
    assert ((w_all >> 10) & 255).max() == 255 and 'tree_rule' not in obs
    d = tempfile.mkdtemp(); p = os.path.join(d, 'x.h3b')
    hdr = ne_h3.write_h3b(p, declared, obs, declared.header.get('kg', '0'), '2.4')
    assert ne_h3.verify_h3b(p, expected_declared_digest=declared.digest)['ok']
    hdr = ne_h3.read_h3b_header(p)
    mk = hdr['net1']['tree_rule']
    assert tree_rule.built_with(hdr) and mk['mode'] == 'mark' and hdr['net1']['rows_n'] == w_all.size
    # header verdict == the rule evaluated on each stored chunk (what the primary did for old files),
    # and tree_qa.patch with the header rows == patch with the read-time rule
    rec = ne_h3.OBS_REC.size
    par = ne_h3.h3_parent_vec(obs['cell_ids'], ne_h3.CHUNK_RES)
    cut = np.flatnonzero(np.concatenate([[True], par[1:] != par[:-1], [True]]))
    toff = np.concatenate([[0], np.cumsum(obs['tree_n'].astype(np.int64))])
    hidden = 0
    assert len(hdr['chunks']) == len(cut) - 1
    for c, a, b in zip(hdr['chunks'], cut[:-1].tolist(), cut[1:].tolist()):
        o = obs['neo1_raw'][a * rec:b * rec]; t = obs['net1_raw'][toff[a] * 4:toff[b] * 4]
        w = np.frombuffer(t, '<u4'); r10 = int(c['r10'], 16)
        h = ((w >> 10) & 255).astype(np.float64) / 4.0
        keep = tree_rule.keep_mask(h) if w.size else np.ones(0, bool)
        assert np.array_equal(tree_rule.chunk_keep(hdr, r10, h), keep), c['r10']
        assert (r10 in tree_rule.marked_chunks(hdr)) == (not keep.all())
        if not keep.all():
            hidden += int((~keep).sum())
            hr = tree_rule.hidden_rows(hdr, r10)
            assert tree_qa.patch(o, t, hr.astype('<u2').tobytes()) == tree_qa.patch(o, t, np.flatnonzero(~keep).astype('<u2').tobytes())
            assert len(tree_qa.patch(o, t, hr.astype('<u2').tobytes())[1]) // 4 == int(keep.sum())
    assert hidden == mk['hidden'] > 0, (hidden, mk)
    print(f'  ({hidden} of {w_all.size} apices marked in the header; NET1 keeps all of them)')


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_') and callable(f):
            f(); print('ok', n)
    print('all ok')
