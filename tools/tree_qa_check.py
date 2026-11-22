#!/usr/bin/env python3
"""Acceptance check for tree_qa (docs/ne-public-api.md → 12e): per bin, the multiset of
tree_h per cell must equal the default /api/v1/trees/bbox apices (0.5 m quantised) of the
cell's owner KG, and sum(tree_n) == count.

    python3 tools/tree_qa_check.py --random 20 [--seed 1]     # random stored bins
    python3 tools/tree_qa_check.py 391,1895 ...               # given bins (i,j)
    python3 tools/tree_qa_check.py --repro                    # the KG 92001 report cells
"""
import json
import random
import sys
import urllib.request
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import cells_bins as cb  # noqa: E402

PAD = 0.0006
BASE = 'http://localhost:8100/api/v1/trees/bbox'


def check(i, j):
    hdr, c, _ = cb.load_bin(i, j, need=('tree_h', 'tree_h_max'))
    ids = c['id']; w, s, e, n = cb.bin_bbox(i, j)
    u = (f'{BASE}?bbox={w - PAD:.6f},{s - PAD:.6f},{e + PAD:.6f},{n + PAD:.6f}&format=columns&limit=100000'
         '&min_height=3&fields=h_m,cell,kg_code')
    r = json.load(urllib.request.urlopen(u, timeout=120))
    if r.get('truncated'):
        return {'bin': (i, j), 'skipped': 'truncated'}
    col = r['columns']
    cell = np.array([int(x, 16) for x in col['cell']], np.uint64); h = np.array(col['h_m'], float)
    k = np.searchsorted(ids, cell); kc = np.minimum(k, max(ids.size - 1, 0))
    inb = (k < ids.size) & (ids[kc] == cell) if ids.size else np.zeros(cell.size, bool)
    raw_n = int(inb.sum()); kgs = hdr['kgs']
    own = np.array([kgs[x] if x < len(kgs) else '' for x in c['kg'][kc]], object) if ids.size else np.zeros(0, object)
    inb &= (own == np.array(col['kg_code'], object))      # /trees/bbox repeats a cell per KG file storing it
    q = np.clip(np.round(h[inb] * 2), 1, 254).astype(np.uint8)
    a = np.lexsort((q, k[inb])); api = np.stack([k[inb][a], q[a]])
    side = np.stack([np.repeat(np.arange(ids.size), c['tree_h_n']), c['tree_h']])
    same = api.shape == side.shape and np.array_equal(api, side)
    tn_ok = bool(np.array_equal(np.minimum(c['tree_h_n'], 255), c['tree_n']))
    lt = c['tree_n'] < 255                                 # thx1: tree_h_max == max(tree_h) (255 = no apex)
    thx_bad = int((cb.thx_from_lists(c)[lt] != c['tree_h_max'][lt]).sum()) if 'tree_h_max' in c else -1
    return {'bin': (i, j), 'ok': bool(same and tn_ok and thx_bad == 0), 'thx_ne_max': thx_bad, 'api_n': int(inb.sum()), 'list_n': int(c['tree_h'].size),
            'tree_n_eq_len': tn_ok, 'api_dup_other_kg': raw_n - int(inb.sum()),
            'max_h': float(c['tree_h'].max() / 2) if c['tree_h'].size else None}


def repro():
    hdr, c, _ = cb.load_bin(391, 1895, need=('tree_h',))
    ids = c['id']; off = cb.ragged_offsets(c['tree_h_n']); out = {}
    for s in ('8c1f8e3337141ff', '8c1f8e3334493ff', '8c1f8e33344b7ff', '8c1f8e3337131ff'):
        k = int(np.searchsorted(ids, int(s, 16)))
        out[s] = {'tree_h': (c['tree_h'][off[k]:off[k + 1]] / 2).tolist(), 'tree_n': int(c['tree_n'][k]),
                  'tree_h_max': None if c['tree_h_max'][k] == 255 else c['tree_h_max'][k] / 2}
    out['bin_max_tree_h'] = float(c['tree_h'].max() / 2)
    return out


if __name__ == '__main__':
    a = sys.argv[1:]
    if '--repro' in a:
        print(json.dumps(repro(), indent=1)); sys.exit(0)
    if '--random' in a:
        n = int(a[a.index('--random') + 1]); seed = int(a[a.index('--seed') + 1]) if '--seed' in a else None
        allb = [tuple(map(int, p.stem.split('_'))) for p in cb.BIN_DIR.glob('*.nex')]
        bins = random.Random(seed).sample(allb, min(n, len(allb)))
    else:
        bins = [tuple(map(int, x.split(','))) for x in a]
    bad = 0
    for ij in bins:
        r = check(*ij); bad += not r.get('ok', True)
        print('OK ' if r.get('ok', True) else 'BAD', r)
    print('bins', len(bins), 'bad', bad)
    sys.exit(1 if bad else 0)
