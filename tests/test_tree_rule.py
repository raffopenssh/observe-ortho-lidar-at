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
    assert 'tree_rule.keep_mask(' in inspect.getsource(ne_trees._decode_chunk_uncached)
    assert '.keep_mask(' in inspect.getsource(ne_h3.build_observed)
    assert tree_qa.RULE_ID == tree_rule.RULE_ID and tree_qa.quant_h is tree_rule.quant_h
    assert tree_rule.built_with({'net1': {'tree_rule': tree_rule.marker(3)}})
    assert not tree_rule.built_with({'net1': {'rows_n': 1}}) and not tree_rule.built_with(None)
    assert not tree_rule.built_with({'net1': {'tree_rule': {'id': 'older', 'dropped': 0}}})


def _build(declared, apex, tf, n=400):
    seg = np.zeros((n, n), np.uint8); nd = np.full((n, n), 20.0, np.float32); lab = np.zeros((n, n), np.int32)
    return ne_h3.build_observed(declared, seg, nd, lab, [], tf, {}, apex_rows=apex, kg=declared.header.get('kg', ''))


def test_builder_equals_old_file_plus_tree_qa():
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
    new = _build(declared, apex, tf)
    real = tree_rule.keep_mask
    tree_rule.keep_mask = lambda h, chunk=None: np.ones(np.asarray(h).size, bool)
    try:
        old = _build(declared, apex, tf)
    finally:
        tree_rule.keep_mask = real
    assert old['tree_rule']['dropped'] == 0 and new['tree_rule']['dropped'] > 0, new['tree_rule']
    assert new['tree_rule']['id'] == tree_rule.RULE_ID
    # old file → what the primary serves: per res-10 chunk, hide what the rule hides (tree_qa.patch)
    rec = ne_h3.OBS_REC.size
    par = ne_h3.h3_parent_vec(old['cell_ids'], ne_h3.CHUNK_RES)
    cut = np.flatnonzero(np.concatenate([[True], par[1:] != par[:-1], [True]]))
    toff = np.concatenate([[0], np.cumsum(old['tree_n'].astype(np.int64))])
    exp_o = bytearray(); exp_t = bytearray(); hidden = 0
    for a, b in zip(cut[:-1].tolist(), cut[1:].tolist()):
        o = old['neo1_raw'][a * rec:b * rec]; t = old['net1_raw'][toff[a] * 4:toff[b] * 4]
        w = np.frombuffer(t, '<u4')
        keep = tree_rule.keep_mask(((w >> 10) & 255).astype(np.float64) / 4.0) if w.size else np.ones(0, bool)
        if not keep.all():
            hidden += int((~keep).sum())
            o, t = tree_qa.patch(o, t, np.flatnonzero(~keep).astype('<u2').tobytes())
        exp_o += o; exp_t += t
    assert hidden == new['tree_rule']['dropped'], (hidden, new['tree_rule'])
    assert bytes(exp_t) == new['net1_raw'], 'NET1 rows differ'
    assert bytes(exp_o) == new['neo1_raw'], 'NEO1 rows differ'
    assert new['summary']['observed']['trees_n'] == len(new['net1_raw']) // 4 == int(new['tree_n'].sum())
    # raw surface stats untouched: every NEO1 byte outside tree_n / tree_h_max equals the unfiltered build
    import cells_api as ca
    A = np.frombuffer(old['neo1_raw'], ca.OBS_DT); B = np.frombuffer(new['neo1_raw'], ca.OBS_DT)
    assert all(np.array_equal(A[f], B[f]) for f in ca.OBS_DT.names if f not in ('tn', 'thx'))
    d = tempfile.mkdtemp()
    for tag, obs in (('new', new), ('old', dict(old, tree_rule=None))):
        p = os.path.join(d, tag + '.h3b')
        hdr = ne_h3.write_h3b(p, declared, obs, declared.header.get('kg', '0'), '2.4')
        assert ne_h3.verify_h3b(p, expected_declared_digest=declared.digest)['ok']
        assert tree_rule.built_with(ne_h3.read_h3b_header(p)) == (tag == 'new'), hdr['net1']
    print(f'  ({hidden} apices dropped by the builder; bytes == unfiltered build + tree_qa.patch)')


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_') and callable(f):
            f(); print('ok', n)
    print('all ok')
