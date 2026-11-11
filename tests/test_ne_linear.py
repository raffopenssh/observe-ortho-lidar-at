"""ne_linear: the detector on synthetic apex sets (a hanging line is found; tree
rows, orchards, hedges, forest aisles are left alone) + the machine-report sync."""
import sys
import tempfile
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import ne_linear as nl  # noqa: E402

rng = np.random.default_rng(7)


def line(x0, y0, ang, length, spacing=4.5, span=45.0, h_sup=6.0, sag=1.6, noise=0.2, crown=(2, 7), jitter=0.5):
    t = np.arange(0, length, spacing) + rng.uniform(-0.8, 0.8, int(np.ceil(length / spacing)))
    u = (t % span) / span
    h = h_sup - sag * 4 * u * (1 - u) + rng.normal(0, noise, t.size)
    lat = rng.uniform(-jitter, jitter, t.size)
    x = x0 + t * np.cos(ang) - lat * np.sin(ang); y = y0 + t * np.sin(ang) + lat * np.cos(ang)
    return np.round(x), np.round(y), np.round(h * 4) / 4, rng.uniform(*crown, t.size)


def trees(x0, y0, w, hgt, n, h=(12, 28), crown=(25, 90)):
    return (np.round(x0 + rng.uniform(0, w, n)), np.round(y0 + rng.uniform(0, hgt, n)),
            np.round(rng.uniform(*h, n) * 4) / 4, rng.uniform(*crown, n))


def cat(*parts):
    return [np.concatenate([p[i] for p in parts]) for i in range(4)]


def run(*parts):
    x, y, h, cr = cat(*parts)
    return nl.detect(x, y, h, cr)


def main():
    base = (4_700_000, 2_650_000)
    far_forest = trees(base[0] - 400, base[1] - 400, 200, 200, 600)

    # 1. a hanging line across open ground, forest far away -> found, only its apices
    ln = line(base[0], base[1], 0.3, 300)
    m, runs = run(ln, far_forest)
    n = ln[0].size
    assert len(runs) == 1 and m[:n].sum() >= 0.9 * n and not m[n:].any(), (len(runs), m[:n].sum(), n)
    assert runs[0]['sags'] >= 1 and 25 <= runs[0]['span_m'] <= 120, runs[0]

    # 2. a straight row of real trees (random heights, real crowns) -> untouched
    row = line(base[0], base[1], 0.3, 300, spacing=6, sag=0, noise=2.5, h_sup=14, crown=(20, 60))
    m, runs = run(row, far_forest)
    assert not m.any() and not runs

    # 3. young planted row: small crowns, heights jumping from tree to tree -> untouched
    row = line(base[0], base[1], 1.1, 300, spacing=5, sag=0, noise=1.0, h_sup=5, crown=(3, 9))
    m, runs = run(row)
    assert not m.any(), runs

    # 4. orchard: parallel rows 5 m apart -> untouched
    rows = [line(base[0] - 5 * k * np.sin(0.3), base[1] + 5 * k * np.cos(0.3), 0.3, 200, spacing=4, sag=0,
                 noise=0.25, h_sup=4, crown=(3, 8)) for k in range(8)]
    m, runs = run(*rows)
    assert not m.any(), runs

    # 5. the same line as 1, but in a forest aisle (trees 10-40 m either side) -> untouched
    ln = line(base[0], base[1], 0.0, 300)
    m, runs = run(ln, trees(base[0], base[1] + 10, 300, 30, 250), trees(base[0], base[1] - 40, 300, 30, 250))
    assert not m.any(), runs

    # 6. hedge / fence line: an apex every 1.7 m at constant height -> untouched
    hd = line(base[0], base[1], 0.7, 200, spacing=1.7, sag=0, noise=0.1, h_sup=3.8, crown=(1, 5))
    m, runs = run(hd)
    assert not m.any(), runs

    # 7. a real tree standing in the line corridor keeps its place
    ln = line(base[0], base[1], 0.3, 300)
    tr = (np.array([round(base[0] + 150 * np.cos(0.3))]), np.array([round(base[1] + 150 * np.sin(0.3))]),
          np.array([17.0]), np.array([60.0]))
    m, runs = run(ln, tr)
    assert len(runs) == 1 and not m[-1]

    # machine reports: idempotent, a human verdict survives a re-run, vanished runs go stale
    import public_flags as pf
    with tempfile.TemporaryDirectory() as td:
        pf.DB_PATH = Path(td) / 'f.sqlite'; pf.SALT_PATH = Path(td) / 'salt'
        pf._TLS.c = None; pf._DICT.clear(); pf._DICT_REV.clear()
        cells = [0x8c1e101a9a031ff, 0x8c1e101a9af41ff]
        items = [{'cell': c, 'lon': 14.93, 'lat': 46.88, 'expected': '{"absent":true,"hint":"linear","n":20,"len_m":90}',
                  'observed': None} for c in cells]
        assert pf.auto_sync('77214', nl.SOURCE, 'trees', 'spurious', items) == {'upserted': 2, 'stale': 0}
        pf.auto_sync('77214', nl.SOURCE, 'trees', 'spurious', items)
        c = pf._conn()
        assert tuple(c.execute('SELECT COUNT(*), SUM(n) FROM report').fetchone()) == (2, 2)
        rid = c.execute('SELECT id FROM report WHERE skey=?', (cells[0],)).fetchone()[0]
        pf.set_status(rid, 'rejected', 'real trees')
        pf.auto_sync('77214', nl.SOURCE, 'trees', 'spurious', items)
        assert pf.auto_cells('77214', nl.SOURCE, 'rejected') == {cells[0]}
        assert pf.auto_sync('77214', nl.SOURCE, 'trees', 'spurious', [], [cells[1]])['stale'] == 1
        assert pf.auto_sync('77214', nl.SOURCE, 'trees', 'spurious', [], [cells[0]])['stale'] == 0   # verdict kept
        # a human "linear" hint that no run covers = tuning set
        pf.record({'lon': 14.9, 'lat': 46.9, 'layer': 'trees', 'claim': 'spurious',
                   'expected': {'absent': True, 'hint': 'linear'}}, 'test')
        hr = pf.hinted_reports('linear', exclude_source=nl.SOURCE)
        assert len(hr) == 1 and abs(hr[0]['lon'] - 14.9) < 1e-6, hr
    print('ok')


if __name__ == '__main__':
    main()
