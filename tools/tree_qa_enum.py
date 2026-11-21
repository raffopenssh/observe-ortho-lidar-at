"""Read-only: cells / KGs / bins / blocks holding >= 1 apex that /trees/bbox hides by default
(realistic=1).  Causes: static height rule (saturation / spike), linear-noise runs, reader flags."""
import sqlite3, sys, time
import numpy as np
sys.path.insert(0, '.')
import cells_api as ca, ne_trees as nt, ne_linear as nl
t0 = time.time()
tf = ca._tf('EPSG:3035', 'EPSG:4326')
hn = ca._h3n()
acc = {}   # cause -> dict(apices, cells set, kgs set, bins set)
def add(cause, kg, n, cells, lon, lat):
    a = acc.setdefault(cause, {'n': 0, 'cells': set(), 'kgs': set(), 'bins': set()})
    a['n'] += int(n); a['kgs'].add(kg); a['cells'].update(int(c) for c in cells)
    a['bins'].update(zip((np.floor(np.asarray(lon) / 0.025)).astype(int).tolist(), (np.floor(np.asarray(lat) / 0.025)).astype(int).tolist()))
# 1) static rule: only chunks whose tallest apex >= 45 m can fail
c = sqlite3.connect('file:data/ne/cells.sqlite?mode=ro', uri=True); c.row_factory = sqlite3.Row
rows = c.execute('SELECT * FROM chunk WHERE hmax >= ? AND trees > 0 ORDER BY kg', (int(nt.TREE_SPIKE_H * 4),)).fetchall()
hdr = None; cur = None; stale = set()
for r in rows:
    kg = r['kg']
    if kg != cur:
        cur = kg; hdr = ca.header(kg); nt._dec_lru.clear() if hasattr(nt._dec_lru, 'clear') else None
    if hdr is None: continue
    try:
        d = nt._decode_chunk_uncached(('enum',), kg, dict(r), hdr, 'trees')
    except Exception:
        stale.add(kg); continue
    if not d.get('n'): continue
    bad = ~d['ok']
    if bad.any():
        add('static', kg, bad.sum(), d['cell'][bad], d['lon'][bad], d['lat'][bad])
print(f'static scan: {len(rows)} chunks, stale-index kgs skipped={len(stale)}, {time.time()-t0:.0f}s', flush=True)
# 2) linear-noise apices
sn = nl._snapshot()
for kg, keys in sn['keys'].items():
    x = (keys >> np.uint64(32)).astype(np.int64); y = (keys & np.uint64(0xFFFFFFFF)).astype(np.int64)
    lon, lat = tf.transform(x, y)
    cells = [hn.latlng_to_cell(float(b), float(a), 12) for a, b in zip(lon, lat)]
    add('linear', kg, keys.size, cells, lon, lat)
# 3) reader flags (cell-keyed + single-tree points)
import public_flags as pf
tfi = ca._tf('EPSG:3035', 'EPSG:4326')
for (kg,) in pf._conn().execute('SELECT DISTINCT kg FROM report WHERE status IN (0,1) AND cell IS NOT NULL').fetchall():
    fc, fp, _, _ = nt._flags(str(kg), 'trees')
    if fc:
        ll = [hn.cell_to_latlng(int(x)) for x in fc]
        add('flag_cell', kg, 0, fc, [p[1] for p in ll], [p[0] for p in ll])
    if len(fp):
        lon, lat = tfi.transform(fp[:, 0], fp[:, 1])
        add('flag_point', kg, len(fp), [hn.latlng_to_cell(float(b), float(a), 12) for a, b in zip(lon, lat)], lon, lat)
fb = sqlite3.connect('file:data/feedback.sqlite?mode=ro', uri=True)
for kg, lo, la in fb.execute("SELECT kg_code, centroid_lon, centroid_lat FROM flags WHERE kind='top_tree' AND severity IN ('high','critical') AND centroid_lon IS NOT NULL"):
    add('legacy_qa', str(kg), 1, [hn.latlng_to_cell(la, lo, 12)], [lo], [la])
tot_kg = 7850
allc, allk, allb = set(), set(), set()
for k, a in acc.items():
    blk = {(i // 4, j // 4) for i, j in a['bins']}
    print(f"{k:11s} apices={a['n']:>8d} cells={len(a['cells']):>7d} kgs={len(a['kgs']):>5d} ({100*len(a['kgs'])/tot_kg:.1f}%) bins={len(a['bins']):>6d} blocks={len(blk)}")
    allc |= a['cells']; allk |= a['kgs']; allb |= a['bins']
print(f"UNION       cells={len(allc)} kgs={len(allk)} ({100*len(allk)/tot_kg:.1f}%) bins={len(allb)} blocks={len({(i//4,j//4) for i,j in allb})}  [{time.time()-t0:.0f}s]")
