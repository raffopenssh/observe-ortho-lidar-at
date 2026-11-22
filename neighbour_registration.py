"""neighbour_registration — is a product's 25 m terrain registered like its neighbours'?

Pre-upload guard for the 2026 ``pos_shift`` / ``grid25_shift`` fault class
(docs/qc-lessons.md): a product whose rasters sit a few metres off shows up
only where it overlaps another product.  ``grid25_registration`` measures that
for the whole corpus on the primary, after the fact; this module asks the same
question for ONE product document before it is uploaded:

    for every neighbour product whose terrain grid overlaps ours
        RMS(ours − neighbour sampled at +(dx, dy))   for dx, dy ∈ {−10 … +10 m}
        → the offset that explains the overlap best, per processing tile

``(0, 0)`` clearly best = aligned; another offset clearly best = that tile (or
the whole product) is displaced against the neighbour; flat ground carries no
signal.  Neighbours with a measured offset (``data/grid25_registration.json``,
in git) are compared on their corrected origin, ``mixed`` ones are skipped,
fresh ones (no ``upgraded_from``) are anchors.

Neighbour documents (``<code>_json_v2``, 60–900 KB each) come straight from
Zenodo — no traffic through the primary — at most ``MAX_FETCH`` per product,
inside ``DEADLINE_S``.  Everything is fail-soft: no manifest / token / network
→ ``{'verdict': 'unchecked'}``.

    python3 neighbour_registration.py <code>        # primary: check a stored product against the store
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
CELL = 25.0
STEPS = (-10.0, -5.0, 0.0, 5.0, 10.0)
HYP = [(dx, dy) for dy in STEPS for dx in STEPS]
ZERO = HYP.index((0.0, 0.0))
MIN_CELLS = 40            # as grid25_registration.decide
MIN_RMS = 0.15            # m: below this the overlap is too flat to tell
MISMATCH_RMS = 3.0        # m: best-offset RMS above this = different terrain, not an offset
WIN_RATIO = 0.6           # best must beat (0, 0) / the runner-up by this factor
MAX_FETCH = int(os.environ.get('NEIGHBOUR_REG_MAX_FETCH', '6'))
DEADLINE_S = float(os.environ.get('NEIGHBOUR_REG_DEADLINE_S', '90'))
MANIFEST = HERE / 'data' / 'austria_processor' / 'zenodo_manifest.json'


def _interior(a):
    f = np.isfinite(a)
    m = f.copy()
    m[0, :] = m[-1, :] = m[:, 0] = m[:, -1] = False
    m[1:-1, 1:-1] &= f[:-2, 1:-1] & f[2:, 1:-1] & f[1:-1, :-2] & f[1:-1, 2:]
    return m


def _bil(a, fx, fy):
    c0 = np.floor(fx).astype(int); r0 = np.floor(fy).astype(int)
    ok = (c0 >= 0) & (r0 >= 0) & (c0 < a.shape[1] - 1) & (r0 < a.shape[0] - 1)
    c0 = np.clip(c0, 0, a.shape[1] - 2); r0 = np.clip(r0, 0, a.shape[0] - 2)
    wx = fx - c0; wy = fy - r0
    v = (a[r0, c0] * (1 - wx) * (1 - wy) + a[r0, c0 + 1] * wx * (1 - wy)
         + a[r0 + 1, c0] * (1 - wx) * wy + a[r0 + 1, c0 + 1] * wx * wy)
    v[~ok] = np.nan
    return v


def residuals(ga, gb):
    """Squared difference ours(X) − neighbour(X + h) for every hypothesis h
    over the common window.  → (r0, c0, sq[len(HYP), nr, nc] with NaN where
    not comparable) in OUR grid's cell indices, or None."""
    A = np.asarray(ga['elev'], np.float32); B = np.asarray(gb['elev'], np.float32)
    ar, ac = A.shape; br, bc = B.shape
    x0 = max(ga['x0'], gb['x0']); x1 = min(ga['x0'] + ac * CELL, gb['x0'] + bc * CELL)
    y1 = min(ga['y0'], gb['y0']); y0 = max(ga['y0'] - ar * CELL, gb['y0'] - br * CELL)
    if x1 - x0 < 100 or y1 - y0 < 100:
        return None
    ca = int(np.ceil((x0 - ga['x0']) / CELL - 1e-6)); ra = int(np.ceil((ga['y0'] - y1) / CELL - 1e-6))
    nc = int((ga['x0'] + ac * CELL - max(x0, ga['x0'] + ca * CELL)) // CELL)
    nc = min(nc, int((x1 - (ga['x0'] + ca * CELL)) // CELL))
    nr = min(ar - ra, int(((ga['y0'] - ra * CELL) - y0) // CELL))
    if nr < 3 or nc < 3:
        return None
    Aw = A[ra:ra + nr, ca:ca + nc]
    ia = _interior(A)[ra:ra + nr, ca:ca + nc]
    if ia.sum() < MIN_CELLS // 2:
        return None
    XX, YY = np.meshgrid(ga['x0'] + (ca + np.arange(nc) + 0.5) * CELL,
                         ga['y0'] - (ra + np.arange(nr) + 0.5) * CELL)
    Bn = np.where(_interior(B), B, np.nan)
    sq = np.empty((len(HYP), nr, nc), np.float32)
    for i, (dx, dy) in enumerate(HYP):
        v = _bil(Bn, (XX + dx - gb['x0']) / CELL - 0.5, (gb['y0'] - (YY + dy)) / CELL - 0.5)
        sq[i] = (Aw - v) ** 2
    ok = ia & np.isfinite(sq).all(0)
    sq[:, ~ok] = np.nan
    return ra, ca, sq


def decide(sq):
    """→ (n, verdict, best (dx, dy), rms0, rms_best) for one window of residuals.
    verdict: 'aligned' | 'shifted' | 'flat' (no signal) | 'unclear'."""
    ok = np.isfinite(sq[0])
    n = int(ok.sum())
    if n < MIN_CELLS:
        return n, 'flat', None, None, None
    r = np.sqrt(np.nanmean(sq[:, ok], axis=1).astype(np.float64))
    o = np.argsort(r)
    b = int(o[0])
    out = (n, 'unclear', HYP[b], round(float(r[ZERO]), 3), round(float(r[b]), 3))
    if r[o[1]] <= MIN_RMS:
        return n, 'flat', None, out[3], out[4]
    if b == ZERO:
        return (n, 'aligned' if r[b] < WIN_RATIO * r[o[1]] or r[o[1]] - r[b] > 0.25 else 'unclear') + out[2:]
    if r[b] < WIN_RATIO * r[ZERO] and r[ZERO] > MIN_RMS:
        return (n, 'shifted') + out[2:]
    return out


def compare(ga, gb, tiles=None):
    """One neighbour.  → {'whole': decide(), 'tiles': {tile_no: decide()}} or None.
    *tiles*: [(tile_no, r0, c0, r1, c1)] in our grid (grid25_patch.tiles_of_doc)."""
    res = residuals(ga, gb)
    if res is None:
        return None
    ra, ca, sq = res
    out = {'whole': decide(sq), 'tiles': {}}
    for tno, r0, c0, r1, c1 in tiles or []:
        a0, a1 = max(r0 - ra, 0), min(r1 - ra, sq.shape[1])
        b0, b1 = max(c0 - ca, 0), min(c1 - ca, sq.shape[2])
        if a1 - a0 >= 3 and b1 - b0 >= 3:
            d = decide(sq[:, a0:a1, b0:b1])
            if d[1] != 'flat':
                out['tiles'][int(tno)] = d
    return out


def grid_of_doc(doc, offsets=None):
    """Terrain grid of a product document on its TRUE origin: a measured
    registration offset is applied unless the document is a fresh generation.
    → grid dict or None ('mixed' products are not usable as a reference)."""
    import v21_products as v21
    sec = ((doc.get('terrain') or {}).get('grid25'))
    if not isinstance(sec, dict) or not sec.get('elev'):
        return None
    g = v21.decode_terrain_grid25(sec)
    g = {'elev': g['elev'], 'x0': float(g['x0']), 'y0': float(g['y0'])}
    code = str(doc.get('kg_code') or '')
    if offsets and doc.get('upgraded_from'):
        if code in (offsets.get('mixed') or ()):
            return None
        off = (offsets.get('offsets') or {}).get(code)
        if off:
            g['x0'] += float(off[0]); g['y0'] += float(off[1])
    return g


def _offsets():
    try:
        return json.loads((HERE / 'data' / 'grid25_registration.json').read_text())
    except Exception:  # noqa: BLE001
        return {}


def _bbox(doc):
    bb = doc.get('bbox')
    try:
        if isinstance(bb, dict):
            bb = [bb['min_lon'], bb['min_lat'], bb['max_lon'], bb['max_lat']]
        return [float(x) for x in bb] if bb and len(bb) == 4 else None
    except Exception:  # noqa: BLE001
        return None


def summarise(pairs: dict, n_tiles: int = 0) -> dict:
    """pairs: {neighbour code: compare()} → verdict over all neighbours."""
    whole = {c: p['whole'] for c, p in pairs.items() if p}
    dec = {c: w for c, w in whole.items() if w[1] in ('aligned', 'shifted')}
    shifted = {c: w for c, w in dec.items() if w[1] == 'shifted'}
    tiles_bad, tiles_ok = {}, set()
    for c, p in pairs.items():
        for tno, d in (p or {}).get('tiles', {}).items():
            if d[1] == 'shifted':
                tiles_bad.setdefault(tno, []).append((c, d[2]))
            elif d[1] == 'aligned':
                tiles_ok.add(tno)
    # a tile one neighbour calls shifted and another aligned is that neighbour's problem, not ours
    tiles_bad = {t: v for t, v in tiles_bad.items() if t not in tiles_ok}
    out = {'neighbours': len(pairs), 'decisive': len(dec), 'aligned': len(dec) - len(shifted),
           'shifted': len(shifted), 'pairs': {c: {'n': w[0], 'verdict': w[1], 'best': w[2], 'rms0': w[3],
                                                  'rms_best': w[4]} for c, w in whole.items()}}
    # terrain that disagrees by metres under EVERY offset is not a registration
    # question: one of the two holds other terrain (stale / foreign tile raster)
    mism = sorted(c for c, w in whole.items() if w[0] >= MIN_CELLS and w[4] is not None and w[4] > MISMATCH_RMS)
    if mism:
        out['mismatch'] = mism
    if not dec and not tiles_bad:
        out['verdict'] = 'no-signal' if pairs else 'no-neighbours'
    elif shifted and len(shifted) > len(dec) - len(shifted):
        vecs = sorted({tuple(w[2]) for w in shifted.values()})
        out['verdict'] = 'shifted'
        out['offset'] = list(vecs[0]) if len(vecs) == 1 else [list(v) for v in vecs]
    elif tiles_bad and len(tiles_bad) < max(n_tiles, len(tiles_bad) + 1):
        out['verdict'] = 'tiles-shifted'
    else:
        out['verdict'] = 'aligned'
    if tiles_bad:
        out['tiles'] = {int(t): [[c, list(v)] for c, v in sorted(set((c, tuple(v)) for c, v in lst))][:3]
                        for t, lst in sorted(tiles_bad.items())}
    return out


# ── neighbour discovery + fetch (peer side) ──────────────────────────

def candidates(code: str, bbox_wgs, manifest_keys) -> list:
    """Product codes with a ``_json_v2`` whose parent KG bbox meets *bbox_wgs*
    (largest overlap first; own code and sweep units excluded)."""
    import kg_splitter
    bb = json.loads((HERE / 'kg_bbox_r1.json').read_text())['bbox']
    w, s, e, n = bbox_wgs
    par = []
    for k, (w2, s2, e2, n2) in bb.items():
        ox = min(e, e2) - max(w, w2); oy = min(n, n2) - max(s, s2)
        if ox > 0 and oy > 0:
            par.append((ox * oy, k))
    par.sort(reverse=True)
    by_parent = {}
    for key in manifest_keys:
        if key.endswith('_json_v2'):
            c = key[:-8]
            by_parent.setdefault(c.split('-')[0], []).append(c)
    out = []
    for _, p in par:
        for c in sorted(by_parent.get(p, ())):
            if c != code and not kg_splitter.is_sweep_code(c):
                out.append(c)
    return out


def check_before_upload(code: str, doc: dict, *, fetch=None, cand=None, deadline_s: float | None = None) -> dict:
    """The guard.  *fetch(code) -> document | None* and *cand* (codes) are
    injectable (tests, primary); default = Zenodo via the local manifest."""
    t0 = time.time()
    deadline_s = DEADLINE_S if deadline_s is None else deadline_s
    try:
        import grid25_patch as gp
        ga = grid_of_doc(doc)            # ours as built: no stored offset applies to a new generation
        if ga is None:
            return {'verdict': 'unchecked', 'why': 'no terrain grid'}
        tiles = gp.tiles_of_doc(doc, ga)
        if fetch is None or cand is None:
            ents = json.loads(MANIFEST.read_text()).get('entries', {})
            live = {k: v for k, v in ents.items() if k.endswith('_json_v2') and v.get('status') != 'removed'
                    and int(v.get('size') or 0) > 0}
            bb = _bbox(doc)
            if not bb:
                return {'verdict': 'unchecked', 'why': 'no bbox'}
            cand = candidates(code, bb, live) if cand is None else cand
            if fetch is None:
                import requests
                import kg_json_v2
                import zenodo_client
                tok = zenodo_client.DEFAULT_TOKEN

                def fetch(c):
                    e = live[f'{c}_json_v2']
                    r = requests.get(f"{e['bucket_url']}/{e['filename']}",
                                     headers={'Authorization': f'Bearer {tok}'}, timeout=30)
                    return kg_json_v2.decode(r.content) if r.status_code == 200 else None
        offs = _offsets()
        pairs, fetched, nbytes = {}, 0, 0
        for c in cand:
            if fetched >= MAX_FETCH or time.time() - t0 > deadline_s:
                break
            if sum(1 for p in pairs.values() if p and p['whole'][1] in ('aligned', 'shifted')) >= 3:
                break
            try:
                nd = fetch(c)
            except Exception:  # noqa: BLE001
                nd = None
            fetched += 1
            if not nd:
                continue
            gb = grid_of_doc(nd, offs)
            if gb is None:
                continue
            p = compare(ga, gb, tiles)
            if p:
                pairs[c] = p
        out = summarise(pairs, len(tiles))
        out['fetched'] = fetched
        out['s'] = round(time.time() - t0, 1)
        return out
    except Exception as e:  # noqa: BLE001 — a guard must never fail a KG
        return {'verdict': 'unchecked', 'why': f'{type(e).__name__}: {str(e)[:120]}'}


def line(res: dict) -> str:
    v = res.get('verdict')
    if v in ('unchecked', 'no-neighbours', 'no-signal'):
        return (f"{v}{(' (' + res['why'] + ')') if res.get('why') else ''}"
                + (f", terrain mismatch vs {','.join(res['mismatch'][:4])}" if res.get('mismatch') else ''))
    s = f"{v}: {res.get('aligned', 0)} aligned / {res.get('shifted', 0)} shifted of {res.get('neighbours', 0)} neighbour(s)"
    if res.get('offset'):
        s += f", offset {res['offset']} m"
    if res.get('mismatch'):
        s += f", terrain mismatch >{MISMATCH_RMS:.0f} m vs {','.join(res['mismatch'][:4])}"
    if res.get('tiles'):
        s += ", tiles " + ",".join(f"{t}{v_[0][1]}" for t, v_ in list(res['tiles'].items())[:10])
    return s


def _main(argv):
    import kg_v2_store as kvs
    code = argv[0]
    doc = kvs.get(code)
    sec = kvs.get_grid25(code)
    doc = dict(doc, terrain=dict(doc.get('terrain') or {}, grid25=sec['terrain']))
    codes = set(kvs.all_codes())

    def fetch(c):
        d = kvs.get(c); s = kvs.get_grid25(c)
        return dict(d, terrain=dict(d.get('terrain') or {}, grid25=s['terrain'])) if d and s and s.get('terrain') else None
    cand = candidates(code, _bbox(doc), {c + '_json_v2' for c in codes})
    res = check_before_upload(code, doc, fetch=fetch, cand=cand)
    print(code, line(res)); print(json.dumps(res, indent=1, default=str)[:3000])


if __name__ == '__main__':
    _main(sys.argv[1:])
