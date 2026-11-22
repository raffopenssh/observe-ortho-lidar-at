"""grid25_registration — measure which products' terrain grid25 is 5 m off.

Root cause (docs/hillshade-seams.md): until 2026-11-21
``austria_processor._read_dtm_for_tile`` re-read a tile's DTM with the
default 5 m pad and pasted the array at the tile's own offset, so every
tile without a local raster sidecar landed in the v1 full GPKG shifted by
(−5 m E, +5 m N).  v2.x products *upgraded from* such a GPKG inherited the
shift in ``terrain.grid25``; fresh v2 products never had it.  Two
overlapping products that disagree by 5 m paint a 0.3–1 m step where the
tile mosaic hands over from one to the other → the hillshade seam lines.

This tool finds the affected products without touching BEV:

1. every pair of products with overlapping interiors is compared under the
   three hypotheses "relative shift −S / 0 / +S" (S = (−5, +5) m);
2. fresh products (no ``upgraded_from``) are anchors (label 0 — verified:
   48 299 fresh↔fresh pairs, none shifted); upgraded products start from
   the date prior (v1 GPKG generated before the raster sidecars rolled out
   → shifted) and are relabelled by their neighbours' weighted votes.

Output ``data/grid25_registration.json``: ``{version, offsets: {code:
[dE, dN]}, stats}`` — the origin correction ``grid25_sample.register``
applies for every reader (tiles, heightfield, landscape queries).

    python3 grid25_registration.py --build      # ~5 min, niced, local only
    python3 grid25_registration.py --status
"""
from __future__ import annotations

import collections
import functools
import json
import os
import sys
import time
from pathlib import Path

import numpy as np

SHIFT = (-5.0, 5.0)              # ground position = declared + SHIFT for an affected product
SIDECAR_CUTOVER = '2026-05-29'   # dd08d74 (raster sidecars) reached the fleet
OUT = Path(os.environ.get('GRID25_REGISTRATION',
           str(Path(__file__).resolve().parent / 'data' / 'grid25_registration.json')))
CELL = 25.0


def _load_corpus(progress=None):
    """→ {code: {x0, y0, rows, cols, v1}} + a loader for (elev, interior)."""
    import kg_v2_store as kvs
    import v21_products as v21
    idx = {}
    raw = {}
    codes = sorted(kvs.all_codes())
    for i, c in enumerate(codes):
        try:
            sec = kvs.get_grid25(c)
            if not sec or not sec.get('terrain'):
                continue
            g = v21.decode_terrain_grid25(sec['terrain'])
            e = g['elev']
            raw[c] = np.where(np.isfinite(e), np.round(e * 10), -32768).astype(np.int16)
            uf = (kvs.get(c) or {}).get('upgraded_from') or {}
            idx[c] = {'x0': g['x0'], 'y0': g['y0'], 'rows': g['rows'], 'cols': g['cols'],
                      'v1': uf.get('v1_generated_at')}
        except Exception as ex:  # noqa: BLE001
            print('skip', c, ex, file=sys.stderr)
        if progress and i % 2000 == 0:
            progress(f'decode {i}/{len(codes)}')

    @functools.lru_cache(maxsize=400)
    def load(c):
        a = raw[c].astype(np.float32)
        a[raw[c] == -32768] = np.nan
        a /= 10
        f = np.isfinite(a)
        m = f.copy()
        m[0, :] = m[-1, :] = m[:, 0] = m[:, -1] = False
        m[1:-1, 1:-1] &= f[:-2, 1:-1] & f[2:, 1:-1] & f[1:-1, :-2] & f[1:-1, 2:]
        return a, m
    return idx, load


def _bil(a, fx, fy):
    c0 = np.floor(fx).astype(int); r0 = np.floor(fy).astype(int)
    ok = (c0 >= 0) & (r0 >= 0) & (c0 < a.shape[1] - 1) & (r0 < a.shape[0] - 1)
    c0 = np.clip(c0, 0, a.shape[1] - 2); r0 = np.clip(r0, 0, a.shape[0] - 2)
    wx = fx - c0; wy = fy - r0
    v = (a[r0, c0] * (1 - wx) * (1 - wy) + a[r0, c0 + 1] * wx * (1 - wy)
         + a[r0 + 1, c0] * (1 - wx) * wy + a[r0 + 1, c0 + 1] * wx * wy)
    v[~ok] = np.nan
    return v


def pair_rms(ga, A, ia, gb, B, ib):
    """RMS of A − B over the common interior for relative shift k·SHIFT,
    k = −1, 0, +1 (A(X) = B(X + k·S) ⇔ label_A − label_B = k).
    → (n, [rms−, rms0, rms+]) or None."""
    x0 = max(ga['x0'], gb['x0']); x1 = min(ga['x0'] + ga['cols'] * CELL, gb['x0'] + gb['cols'] * CELL)
    y1 = min(ga['y0'], gb['y0']); y0 = max(ga['y0'] - ga['rows'] * CELL, gb['y0'] - gb['rows'] * CELL)
    if x1 - x0 < 100 or y1 - y0 < 100:
        return None
    ca = int((x0 - ga['x0']) / CELL); ra = int((ga['y0'] - y1) / CELL)
    nr = int((y1 - y0) / CELL); nc = int((x1 - x0) / CELL)
    Aw = A[ra:ra + nr, ca:ca + nc]; iaw = ia[ra:ra + nr, ca:ca + nc]
    if iaw.sum() < 20:
        return None
    XX, YY = np.meshgrid(x0 + (np.arange(nc) + 0.5) * CELL, y1 - (np.arange(nr) + 0.5) * CELL)
    Bn = np.where(ib, B, np.nan)
    res = [_bil(Bn, (XX + k * SHIFT[0] - gb['x0']) / CELL - 0.5,
                (gb['y0'] - (YY + k * SHIFT[1])) / CELL - 0.5) for k in (-1, 0, 1)]
    m = iaw & np.isfinite(res[0]) & np.isfinite(res[1]) & np.isfinite(res[2])
    if m.sum() < 20:
        return None
    return int(m.sum()), [float(np.sqrt(np.mean((Aw[m] - v[m]) ** 2))) for v in res]


def decide(n, r):
    """→ k ∈ {−1, 0, +1} when one hypothesis clearly wins, else None
    (flat ground / tiny overlap carries no registration signal)."""
    o = np.argsort(r)
    if n >= 40 and r[o[1]] > 0.15 and r[o[0]] < 0.6 * r[o[1]]:
        return int(o[0]) - 1
    return None


def solve(idx, decisive):
    """Labels (0 = registered, 1 = shifted by SHIFT) from pair votes."""
    lab = {c: (0 if not g['v1'] else int(g['v1'] < SIDECAR_CUTOVER)) for c, g in idx.items()}
    fixed = {c for c, g in idx.items() if not g['v1']}
    nb = collections.defaultdict(list)
    for a, b, k, n in decisive:
        nb[a].append((b, k, n)); nb[b].append((a, -k, n))

    def votes(c):
        v = collections.Counter()
        for o, k, n in nb.get(c, ()):
            v[lab[o] + k] += min(n, 2000)
        return v
    for _ in range(30):
        ch = 0
        for c in idx:
            if c in fixed or c not in nb:
                continue
            v = votes(c)
            best = max((0, 1), key=lambda s: v[s])
            if v[0] + v[1] > 0 and best != lab[c]:
                lab[c] = best; ch += 1
        if not ch:
            break
    mixed = []
    for c in idx:
        if c in fixed:
            continue
        v = votes(c)
        tot = sum(v.values())
        if tot and v[lab[c]] < 0.9 * tot:
            mixed.append(c)
    return lab, mixed


def build(write=True, progress=print):
    t0 = time.time()
    idx, load = _load_corpus(progress)
    H = collections.defaultdict(list)
    for c, g in idx.items():
        x1 = g['x0'] + g['cols'] * CELL; y1 = g['y0'] - g['rows'] * CELL
        for i in range(int(g['x0'] // 5000), int(x1 // 5000) + 1):
            for j in range(int(y1 // 5000), int(g['y0'] // 5000) + 1):
                H[(i, j)].append(c)
    pairs = set()
    for L in H.values():
        for i in range(len(L)):
            for j in range(i + 1, len(L)):
                pairs.add((L[i], L[j]) if L[i] < L[j] else (L[j], L[i]))
    decisive = []; n_pairs = 0
    for n_, (a, b) in enumerate(sorted(pairs)):
        r = pair_rms(idx[a], *load(a), idx[b], *load(b))
        if r is None:
            continue
        n_pairs += 1
        k = decide(*r)
        if k is not None:
            decisive.append((a, b, k, r[0]))
        if progress and n_ % 50000 == 0:
            progress(f'pairs {n_}/{len(pairs)}')
    lab, mixed = solve(idx, decisive)
    offsets = {c: list(SHIFT) for c, s in sorted(lab.items()) if s}
    doc = {'version': int(time.time()), 'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
           'shift_m': list(SHIFT), 'offsets': offsets,
           'stats': {'codes': len(idx), 'upgraded': sum(1 for g in idx.values() if g['v1']),
                     'pairs': n_pairs, 'decisive': len(decisive), 'shifted': len(offsets),
                     'mixed': len(mixed), 'secs': round(time.time() - t0)},
           'mixed': sorted(mixed)}
    try:    # offsets unchanged → keep the version (it keys every client's tile cache)
        prev = json.loads(OUT.read_text())
        if prev.get('retired'):     # record of offsets dropped by a fresh generation (retire)
            doc['retired'] = prev['retired']
        if prev.get('offsets') == offsets:
            doc['version'] = prev['version']
    except Exception:  # noqa: BLE001
        pass
    if write:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        tmp = OUT.with_suffix('.tmp')
        tmp.write_text(json.dumps(doc))
        os.replace(tmp, OUT)
    return doc


def retire(code, generated_at: str = '') -> list | None:
    """The product was REGENERATED on the true grid (fresh run, no ``upgraded_from`` — the anchor
    class of rule 2): its measured offset belonged to the old rasters.  Drop it now instead of at
    the next weekly build; ``version`` stays (it keys every client's tile cache — the product's own
    ingest stamp re-keys its tiles).  Called by ``qc_generation.on_ingest``."""
    code = str(code)
    try:
        doc = json.loads(OUT.read_text())
    except Exception:  # noqa: BLE001
        return None
    off = (doc.get('offsets') or {}).pop(code, None)
    mixed = code in (doc.get('mixed') or [])
    if off is None and not mixed:
        return None
    if mixed:
        doc['mixed'] = [c for c in doc['mixed'] if c != code]
    doc['retired'] = dict(doc.get('retired') or {}, **{code: {'ts': int(time.time()), 'generated_at': generated_at,
                                                               'off': off, 'mixed': mixed}})
    tmp = OUT.with_suffix('.tmp')
    tmp.write_text(json.dumps(doc))
    os.replace(tmp, OUT)
    return off or [0, 0]


def status():
    try:
        d = json.loads(OUT.read_text())
    except Exception as e:  # noqa: BLE001
        return {'present': False, 'error': str(e)}
    return {'present': True, 'version': d.get('version'), 'built_at': d.get('built_at'), **(d.get('stats') or {})}


if __name__ == '__main__':
    if '--build' in sys.argv:
        try:
            os.nice(15)
        except OSError:
            pass
        d = build()
        print(json.dumps({'version': d['version'], **d['stats']}))
    else:
        print(json.dumps(status()))
