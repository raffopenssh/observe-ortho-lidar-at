"""stats-envelope/1 contract checks for /api/v1/stats/unit (runs against data/ne on disk)."""
import csv
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from flask import Flask  # noqa: E402

import cells_api  # noqa: E402
import stats_unit  # noqa: E402

app = Flask(__name__)
app.register_blueprint(stats_unit.stats_bp)
C = app.test_client()
ENV_KEYS = {'schema', 'service', 'unit', 'source', 'epoch', 'coverage', 'figures', 'attribution', 'license',
            'computed_at', 'age_s'}


def _first_kg():
    fs = cells_api._files()
    return sorted(k for k, f in fs.items() if not f.get('partial'))[0] if fs else None


def test_unknown_kg_is_200_incomplete():
    d = C.get('/api/v1/stats/unit?kg=00000').get_json()
    assert set(d) == ENV_KEYS and d['schema'] == 'stats-envelope/1'
    assert d['coverage']['complete'] is False and d['figures']['cells_n'] == 0
    assert all(not isinstance(v, (dict, list)) for v in d['figures'].values())


def test_errors():
    assert C.get('/api/v1/stats/unit').status_code == 400
    assert C.get('/api/v1/stats/unit?h3=841e109ffffffff').status_code == 400     # res 4
    assert C.get('/api/v1/stats/unit?bbox=15,47,15.2,47.1').status_code == 400   # span
    assert C.get('/api/v1/stats/unit?kg=' + ','.join(str(10000 + i) for i in range(51))).status_code == 400


def test_kg_envelope_and_formats():
    kg = _first_kg()
    if kg is None:
        return
    r = C.get(f'/api/v1/stats/unit?kg={kg}')
    assert r.status_code == 200 and r.headers.get('ETag') and 'max-age=600' in r.headers['Cache-Control']
    d = r.get_json()
    f = d['figures']
    assert d['unit']['kind'] == 'kg' and d['unit']['key'] == kg and d['coverage']['cells_known'] > 0
    for k in ('cells_n', 'area_ha', 'cover_ha.wald', 'consistency_n.consistent', 'als.newest_year', 'height.p90_mean_m'):
        assert k in f, k
    s = cells_api.kg_summary(kg)
    assert f['cover_ha.wald'] == s['cover_ha']['wald']
    assert C.get(f'/api/v1/stats/unit?kg={kg}', headers={'If-None-Match': r.headers['ETag']}).status_code == 304
    flat = C.get(f'/api/v1/stats/unit?kg={kg}&format=flat&keys=cover_ha').get_json()
    assert set(flat) == {k for k in f if k.startswith('cover_ha')}
    r = C.get(f'/api/v1/stats/unit?kg={kg},00000&format=csv')
    assert r.mimetype == 'text/csv' and not r.data.startswith(b'\xef\xbb\xbf')
    rows = list(csv.reader(io.StringIO(r.get_data(as_text=True))))
    assert rows[0][:2] == ['unit', 'coverage_complete'] and len(rows) == 3 and all(len(x) == len(rows[0]) for x in rows)
    multi = C.get(f'/api/v1/stats/unit?kg={kg},00000').get_json()
    assert multi['n'] == 2 and [u['unit']['key'] for u in multi['units']] == sorted([kg, '00000'])


def test_h3_unit_exact_children():
    kg = _first_kg()
    if kg is None:
        return
    hdr = cells_api.header(kg, with_chunks=True)
    r10 = hdr['chunks'][len(hdr['chunks']) // 2]['r10']
    d = C.get(f'/api/v1/stats/unit?h3={r10}').get_json()
    assert d['unit']['kind'] == 'h3' and d['unit']['cells_n'] == 49 and d['coverage']['cells_known'] <= 49
    assert 1.4 < d['figures']['area_ha'] < 1.6          # 49 × ~305 m²
    # the same unit as a bbox contains at least those cells
    w, s, e, n = d['unit']['bbox']
    b = C.get(f'/api/v1/stats/unit?bbox={w},{s},{e},{n}').get_json()
    assert b['unit']['kind'] == 'bbox' and b['figures']['cells_n'] >= 49


def test_tree_figures_exact():
    """trees.* / trees_height_n.* come from the exact apex heights of the cells behind trees_n; the fast
    cell-list reader, the chunk path and a height window all agree apex for apex."""
    import numpy as np
    import stats_unit as su
    kg = _first_kg()
    if kg is None:
        return
    f = C.get(f'/api/v1/stats/unit?kg={kg}').get_json()['figures']
    bands = [f[f'trees_height_n.{k}'] for k in su.TREE_BAND_KEYS]
    assert len(bands) == 12 and sum(bands) == f['trees_n']
    h = cells_api.header(kg, with_chunks=True)
    t = cells_api.gather([cells_api._load_chunk(kg, dict(r, r10=int(str(r['r10']), 16)), h) for r in h['chunks']], None)
    hm = cells_api.decode_trees(t.gather_trees()[1])['h_m']
    assert len(hm) == f['trees_n']
    if len(hm):
        assert f['trees.tallest_m'] == hm.max() and abs(f['trees.height_mean_m'] - hm.mean()) <= 0.05 + 1e-9
        assert bands[0] == int((hm < 5).sum()) and bands[4] == int(((hm >= 20) & (hm < 25)).sum())
    w = C.get(f'/api/v1/stats/unit?kg={kg}&min_height=20&max_height=30').get_json()
    assert w['unit']['tree_height_m'] == [20.0, 30.0]
    wf = w['figures']
    assert wf['trees_n'] == int(((hm >= 20) & (hm <= 30)).sum()) == sum(wf[f'trees_height_n.{k}'] for k in su.TREE_BAND_KEYS)
    assert wf['cells_n'] == f['cells_n'] and wf['structures_n'] == f['structures_n']
    assert C.get(f'/api/v1/stats/unit?kg={kg}&min_height=30&max_height=20').status_code == 400
    ids = np.sort(t.ids)
    fast, _ = su._acc_for_cells_fast(ids)
    t2, hd, _ = cells_api.select(ids, None)
    slow = su._acc_from_table(t2, hd)
    assert fast['trees'] == slow['trees'] == sum(fast['th']) and fast['th'] == slow['th']
    sc = su._scale_figures(su._figures(slow, cells_total=len(ids), area_ha_total=None), 49)
    assert sum(sc[f'trees_height_n.{k}'] for k in su.TREE_BAND_KEYS) == sc['trees_n'] == slow['trees'] * 49
    assert su.tree_figures([0] * 12 + [1, 0, 1])['trees.height_mean_m'] == 3.3     # 3.25 → half up


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn(); print('ok', name)
    print(json.dumps(C.get('/api/v1/stats/unit?kg=' + (_first_kg() or '00000')).get_json()['figures'])[:200])
