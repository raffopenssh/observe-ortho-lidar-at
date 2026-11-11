"""Linear noise in the NE tree apices (``ne_trees``): detector, store, flags.

Thin elevated linear objects end up in the nDSM as a 1-2 m wide ridge and
the apex finder turns the ridge into a chain of "trees".  The signature in
the apex columns alone (x / y in EPSG:3035 metres, height, crown area):

* dead straight over >= 80 m (apex positions are 1 m quantised);
* apices every 2.5-7.5 m, tiny crowns (ridge width x spacing);
* a smooth height profile hanging in U-shaped sags between supports - where
  it sags below the apex finder's 3 m floor the chain has gaps;
* open ground: nothing beside it, no real trees within 40 m (forest aisles
  with regrowth and orchard rows look alike and are NOT touched).

Precision first: a missed chain is fine, a row of real trees must never
pass (``accept``).  Gates were tuned on hand-checked chains.

Steps (``detect``, one KG at a time, no raster read)
1. candidates = apices with a small crown for their height;
2. direction vote: per candidate, the candidates 9-40 m away are binned by
   bearing (mod 180 deg); a chain member has most of them in one narrow bin;
3. chains = connected "linear" candidates with the same bearing lying on
   each other's line;
4. per chain a robust line fit, split into runs at long gaps, the height
   profile (``sag_profile``) and the gates.

Store: ``data/linear_noise.sqlite`` (primary only; the mapped runs are not
served by any route) - one row per run with its endpoints, metrics and the
packed keys of its apices (8 B each), one row per KG with the file digest
the result was made from.  Nothing is recomputed while a KG file is unchanged.

Flags: ONE machine report per run in the public flag store (layer
``trees``, claim ``spurious``, source ``auto-linear``, subject = the run's
middle apex, ``expected: {"absent": true, "hint": "linear", "n", "len_m"}``),
written in one transaction per KG.  A human overrules a run by setting that
report to ``rejected`` - its apices are served again.  ``ne_trees`` hides
exactly the apices of the runs (never a whole cell) under ``realistic=1``;
the same key set applies to every KG file that carries the apex (KG files
overlap), so neighbouring KGs answer alike.

    python3 ne_linear.py --backfill [--budget-s 3000]   # KGs whose file changed (hourly timer)
    python3 ne_linear.py --kg 77214 [--force]
    python3 ne_linear.py --status
"""
from __future__ import annotations

import numpy as np

VERSION = 1
CROWN_MAX = 36.0        # m2: candidate crown area cap ...
CROWN_PER_H = 3.5       # ... and crown <= CROWN_PER_H * h + 4 (a real 4 m tree is not 1-7 m2 wide for long)
H_MAX = 45.0            # anything above this is handled by the spike / saturation rules
R_IN, R_OUT = 9.0, 40.0  # vote annulus (m)
BIN_DEG = 4.0
VOTE_MIN = 6            # neighbours in the peak bearing (+-1 bin)
VOTE_FRAC = 0.45        # ... and that share of all annulus neighbours
LINK_M = 30.0           # chain link distance
LINK_LAT = 2.0          # lateral offset of a linked neighbour from the point's own line
LINK_DEG = 8.0
RUN_GAP = 45.0          # a run ends at an along-line gap longer than this
RUN_LEN = 60.0          # min run length (m)
RUN_N = 10              # min apices on the run
RUN_SPACING = 9.0       # pre-gate: max mean spacing (m)
LAT_RMS = 1.3           # pre-gate: straightness (m)
CORRIDOR = 2.0          # flagged half-width (m)
FLANK = (3.0, 9.0)      # lateral band that must be (nearly) empty: orchard / vineyard rows, forest
BIN_M = 6.0             # sag profile: along-line bin (m)
H_FLOOR = 3.0           # apex finder's min height: a gap inside a run = the line sagging below it
GAP_FILL = 12.0         # along-line gaps longer than this are filled with the floor
CHORD = (6.0, 25.0)     # chord test: neighbours this far to either side (m)
CHORD_MED = 0.25        # median |apex - chord| (m); heights are quantised to 0.25 m
CHORD_OUT = 0.75        # ... and at most CHORD_OUT_FRAC of the apices further than this from their chord
CHORD_OUT_FRAC = 0.12
RES_OUT = 1.5           # an apex further than this from the profile is not part of the chain (kept as a tree)
SAG_MIN = 1.0           # a span = valley at least this deep below BOTH of its supports (m)
SPAN_MIN = 25.0         # ... whose supports stand at least this far apart (m).  Overhead networks here:
#   local distribution / telecom on wooden masts: spans 30-60 m, 5-8 m up, sag <= ~1.5 m
#   regional distribution: spans 80-150 m, sag 1-3 m
#   transmission: spans 250-450 m, sag 8-15 m
# i.e. sag stays within ~4 % of the span; terrain under the span adds a little.
SAG_PER_SPAN = 0.05
SAG_SLACK = 1.5         # m
ACC_LEN = 80.0          # accepted run: min length (m) ...
ACC_N = 15              # ... apices ...
ACC_LAT = 0.8           # ... lateral rms (m; apex positions are 1 m quantised)
SP_MIN, SP_MAX = 2.5, 7.5   # mean apex spacing (m): denser = hedge / fence line, wider = planted row
NEAR_FRAC = 0.15        # apices 3-9 m beside the run, per on-run apex
TALL = (8.0, 40.0)      # "real tree" = apex >= 8 m within 40 m of the run ...
TALL_PER_100M = 6.0     # ... at most this many per 100 m: open ground only (forest aisles look alike)
TALL_PER_100M_STRONG = 25.0
STRONG_LEN, STRONG_N, STRONG_CROWN = 200.0, 40, 18.0
THIN_CROWN = 12.0       # m2 median crown of a thin chain
WIDE_CROWN = 22.0
_TILE = 1500.0          # work tile (m); keeps the pair arrays small


def _pairs(tree, r):
    p = tree.query_pairs(r, output_type='ndarray')
    return p[:, 0], p[:, 1]


def _detect_tile(x, y, h, cr):
    from scipy.spatial import cKDTree
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    n = x.size
    out = np.zeros(n, bool); runs = []
    is_cand = (cr <= np.minimum(CROWN_MAX, CROWN_PER_H * h + 4.0)) & (h <= H_MAX)
    cand = np.nonzero(is_cand)[0]
    if cand.size < RUN_N:
        return out, runs
    cx, cy = x[cand], y[cand]
    tree = cKDTree(np.c_[cx, cy])
    i, j = _pairs(tree, R_OUT)
    dx = cx[j] - cx[i]; dy = cy[j] - cy[i]; dist = np.hypot(dx, dy)
    far = dist >= R_IN
    nb = int(round(180.0 / BIN_DEG))
    ang = np.degrees(np.arctan2(dy, dx)) % 180.0
    b = np.minimum((ang / BIN_DEG).astype(np.int64), nb - 1)
    hist = np.zeros((cand.size, nb), np.int32)
    np.add.at(hist, (i[far], b[far]), 1); np.add.at(hist, (j[far], b[far]), 1)
    tot = hist.sum(1)
    sm = hist + np.roll(hist, 1, 1) + np.roll(hist, -1, 1)
    pk = sm.argmax(1); votes = sm[np.arange(cand.size), pk]
    lin = (votes >= VOTE_MIN) & (votes >= VOTE_FRAC * np.maximum(tot, 1))
    if lin.sum() < RUN_N:
        return out, runs
    th = np.radians((pk + 0.5) * BIN_DEG)
    ux, uy = np.cos(th), np.sin(th)
    k = lin[i] & lin[j] & (dist <= LINK_M)
    i2, j2 = i[k], j[k]
    da = np.abs(((pk[i2] - pk[j2]) * BIN_DEG + 90.0) % 180.0 - 90.0)
    lat = np.abs(-(cx[j2] - cx[i2]) * uy[i2] + (cy[j2] - cy[i2]) * ux[i2])
    k2 = (da <= LINK_DEG) & ((lat <= LINK_LAT) | (dist[k] < 6.0))
    g = coo_matrix((np.ones(k2.sum(), np.int8), (i2[k2], j2[k2])), shape=(cand.size, cand.size))
    nc, lab = connected_components(g, directed=False)
    lab[~lin] = -1
    sizes = np.bincount(lab[lab >= 0], minlength=nc)
    all_tree = cKDTree(np.c_[x, y])
    for c in np.nonzero(sizes >= RUN_N)[0]:
        m = np.nonzero(lab == c)[0]
        px, py = cx[m], cy[m]
        # robust line: PCA, then refit on the inliers
        for _ in range(2):
            mx, my = px.mean(), py.mean()
            cov = np.cov(np.c_[px - mx, py - my].T)
            w, v = np.linalg.eigh(cov); u = v[:, 1]
            s = -(cx[m] - mx) * u[1] + (cy[m] - my) * u[0]
            keep = np.abs(s) <= max(CORRIDOR, 2.5 * np.median(np.abs(s)))
            px, py = cx[m][keep], cy[m][keep]
            if px.size < RUN_N:
                break
        if px.size < RUN_N:
            continue
        # every apex (any crown) in the bbox of the chain, in line coordinates
        t_c = (cx[m] - mx) * u[0] + (cy[m] - my) * u[1]
        t0, t1 = t_c.min(), t_c.max()
        ctr = np.array([mx + u[0] * (t0 + t1) / 2, my + u[1] * (t0 + t1) / 2])
        near = np.asarray(all_tree.query_ball_point(ctr, (t1 - t0) / 2 + TALL[1] + 1.0), dtype=np.int64)
        t = (x[near] - mx) * u[0] + (y[near] - my) * u[1]
        s = -(x[near] - mx) * u[1] + (y[near] - my) * u[0]
        on = (np.abs(s) <= CORRIDOR) & is_cand[near]     # a real tree standing in the corridor is never a member
        o = near[on][np.argsort(t[on])]; to = np.sort(t[on])
        if o.size < RUN_N:
            continue
        brk = np.nonzero(np.diff(to) > RUN_GAP)[0] + 1
        for seg_o, seg_t in zip(np.split(o, brk), np.split(to, brk)):
            if seg_o.size < RUN_N:
                continue
            ln = float(seg_t[-1] - seg_t[0])
            if ln < RUN_LEN or ln / (seg_o.size - 1) > RUN_SPACING:
                continue
            ss = -(x[seg_o] - mx) * u[1] + (y[seg_o] - my) * u[0]
            a, bb = np.polyfit(seg_t, ss, 1)
            rms = float(np.sqrt(np.mean((ss - (a * seg_t + bb)) ** 2)))
            if rms > LAT_RMS:
                continue
            hh = h[seg_o]
            dh = float(np.median(np.abs(np.diff(hh))))
            inr = (t >= seg_t[0]) & (t <= seg_t[-1])
            fl = int((inr & (np.abs(s) >= FLANK[0]) & (np.abs(s) <= FLANK[1])).sum())
            pr = sag_profile(seg_t, hh)
            if pr is None:
                continue
            pr['crown_med'] = float(np.median(cr[seg_o])); pr['crown_p90'] = float(np.percentile(cr[seg_o], 90))
            tall = int((inr & ((np.abs(s) >= FLANK[0]) | ~is_cand[near]) & (np.abs(s) <= TALL[1])
                        & (h[near] >= TALL[0])).sum())
            pr['tall_per_100m'] = round(tall / ln * 100, 1)
            if not DEBUG_ALL and not accept(ln, seg_o.size, rms, fl, pr):
                continue
            seg_o = seg_o[pr['on_line'] & (cr[seg_o] <= min(CROWN_MAX, 3 * pr['crown_med'] + 4))]
            if seg_o.size < ACC_N:
                continue
            out[seg_o] = True
            runs.append({'x0': float(mx + u[0] * seg_t[0]), 'y0': float(my + u[1] * seg_t[0]),
                         'x1': float(mx + u[0] * seg_t[-1]), 'y1': float(my + u[1] * seg_t[-1]),
                         'n': int(seg_o.size), 'len_m': round(ln, 1), 'h_med': float(np.median(hh)),
                         'lat_rms': round(rms, 2), 'dh_med': dh, 'flank': fl, 'idx': seg_o,
                         **{k: v for k, v in pr.items() if k != 'on_line'}})
    return out, runs


DEBUG_ALL = False


def accept(ln, n, rms, fl, pr) -> bool:
    """The gates.  Tuned for precision on hand-checked runs: a miss is fine,
    a row of real trees must never pass."""
    sp = ln / max(n - 1, 1)
    if ln < ACC_LEN or n < ACC_N or not (SP_MIN <= sp <= SP_MAX) or rms > ACC_LAT:
        return False
    if fl > NEAR_FRAC * n or pr['far'] > 0.05 or pr['chord_out'] > 0.5 or pr['odd_dips'] > pr['sags']:
        return False
    # long, clean and sagging: one height quantum of scatter over >= 200 m is no row of trees,
    # so such a run may pass along a forest edge
    strong = (ln >= STRONG_LEN and n >= STRONG_N and rms <= 0.7 and pr['res_m'] <= 0.25 and pr['chord_out'] <= 0.12
              and pr['far'] <= 0.02 and pr['sags'] >= 1 and pr['odd_dips'] == 0 and pr['crown_med'] <= STRONG_CROWN)
    pr['tier'] = 'strong' if strong else 'open'
    if pr['tall_per_100m'] > (TALL_PER_100M_STRONG if strong else TALL_PER_100M):   # forest aisle / hedge with trees
        return False
    if strong:
        return True
    if pr['crown_med'] <= THIN_CROWN and pr['crown_p90'] <= 2 * THIN_CROWN:
        # thin chain: sag between supports, or else very clean
        return pr['res_m'] <= 0.7 and (pr['sags'] >= 1 or (pr['res_m'] <= 0.45 and n >= 20))
    # wider crowns (a broader ridge, 8-12 m up): only with a clean U
    return pr['crown_med'] <= WIDE_CROWN and pr['res_m'] <= 0.3 and pr['sags'] >= 1 and pr['chord_out'] <= 0.15


def sag_profile(t, h):
    """The U-shape gate.  ``t`` = sorted along-line positions (m), ``h`` =
    apex heights.  A hanging line: the height profile is smooth and made of
    valleys (sag) between supports.  Where it sags below the apex finder's floor there are no
    apices at all, so long gaps count as profile at the floor.  Returns None
    (too short to judge) or the profile metrics + 'on_line' = mask of the apices following the profile."""
    t = np.asarray(t, np.float64) - float(t[0]); h = np.asarray(h, np.float64)
    nb = int(t[-1] // BIN_M) + 1
    if nb < 6:
        return None
    b = np.minimum((t / BIN_M).astype(np.int64), nb - 1)
    p = np.full(nb, np.nan)
    for k in np.unique(b):
        p[k] = np.median(h[b == k])
    have = ~np.isnan(p)
    # empty bins: floor when part of a long gap, else interpolated
    tc = (np.arange(nb) + 0.5) * BIN_M
    gap_after = np.diff(t) > GAP_FILL
    floor = np.zeros(nb, bool)
    for g in np.nonzero(gap_after)[0]:
        floor |= (tc > t[g] + BIN_M / 2) & (tc < t[g + 1] - BIN_M / 2)
    floor &= ~have
    # a floor fill only makes sense next to a low line
    lo = float(np.min(p[have]))
    if floor.any() and lo > H_FLOOR + 2.5:
        floor[:] = False
    p[floor] = H_FLOOR - 0.25
    known = ~np.isnan(p)
    p = np.interp(tc, tc[known], p[known])
    q = np.convolve(np.pad(p, 1, mode='edge'), [0.25, 0.5, 0.25], mode='valid')
    res = h - np.interp(t, tc, q)
    far = np.abs(res) > RES_OUT
    # chord test: every apex against the straight line between its neighbours 6-25 m
    # to the left and right.  A hanging line is smooth and convex: the apex sits on
    # (or just under) the chord, except at the few supports.  Tree tops in a row
    # jump by metres from one to the next.
    li = np.searchsorted(t, t - CHORD[0], 'right') - 1
    ri = np.searchsorted(t, t + CHORD[0], 'left')
    ok = (li >= 0) & (ri < t.size)
    li = np.clip(li, 0, t.size - 1); ri = np.clip(ri, 0, t.size - 1)
    ok &= (t - t[li] <= CHORD[1]) & (t[ri] - t <= CHORD[1])
    if ok.sum() < 6:
        return None
    w = (t - t[li]) / np.maximum(t[ri] - t[li], 1e-6)
    r = (h - (h[li] * (1 - w) + h[ri] * w))[ok]
    mad = float(np.median(np.abs(r)))
    cout = float(np.mean(np.abs(r) > CHORD_OUT))
    from scipy.signal import find_peaks
    # valleys of the padded profile; prominence = depth below the lower support
    val, pv = find_peaks(-q, prominence=SAG_MIN)
    sags = []; spans = []; sup = []; odd = 0
    for k, lb, rb in zip(val, pv['left_bases'], pv['right_bases']):
        # supports = the profile maxima on either side of the valley
        span = (rb - lb) * BIN_M; sag = float(min(q[lb], q[rb]) - q[k])
        if sag < SAG_MIN:
            continue
        if span < SPAN_MIN or sag > SAG_PER_SPAN * span + SAG_SLACK:
            odd += 1                        # a dip no hanging line makes: too short a span or too deep for it
            continue
        sags.append(sag); spans.append(span); sup += [float(q[lb]), float(q[rb])]
    turns = len(find_peaks(q, prominence=0.75)[0]) + len(find_peaks(-q, prominence=0.75)[0])
    return {'on_line': ~far, 'far': float(far.mean()), 'sags': len(sags), 'sag_m': round(max(sags or [0.0]), 2),
            'res_m': round(mad, 3), 'chord_out': round(cout, 3), 'turns_per_100m': round(turns / max(t[-1], 1.0) * 100, 2),
            'floor_gaps': int(floor.any()), 'odd_dips': odd,
            'span_m': round(float(np.median(spans)), 1) if spans else 0.0,
            'sup_m': round(float(np.median(sup)), 2) if sup else 0.0,
            'sup_spread': round(float(np.percentile(sup, 90) - np.percentile(sup, 10)), 2) if sup else 0.0}


def detect(x, y, h, crown_m2):
    """-> (bool mask over the input apices, list of accepted runs)."""
    x = np.asarray(x, np.float64); y = np.asarray(y, np.float64)
    h = np.asarray(h, np.float32); cr = np.asarray(crown_m2, np.float32)
    out = np.zeros(x.size, bool); runs = []
    if x.size < RUN_N:
        return out, runs
    pad = 250.0
    for tx in np.arange(x.min(), x.max() + 1, _TILE):
        mxs = (x >= tx - pad) & (x < tx + _TILE + pad)
        if not mxs.any():
            continue
        for ty in np.arange(y.min(), y.max() + 1, _TILE):
            idx = np.nonzero(mxs & (y >= ty - pad) & (y < ty + _TILE + pad))[0]
            if idx.size < RUN_N:
                continue
            m, r = _detect_tile(x[idx], y[idx], h[idx], cr[idx])
            for q in r:
                q['idx'] = idx[q['idx']]
            out[idx[m]] = True; runs += r
    seen = set(); uniq = []
    for r in runs:                       # tiles overlap: the same run may be found twice
        k = tuple(sorted(((round(r['x0'] / 10), round(r['y0'] / 10)), (round(r['x1'] / 10), round(r['y1'] / 10)))))
        if k not in seen:
            seen.add(k); uniq.append(r)
    out[:] = False
    for r in uniq:
        out[r['idx']] = True
    return out, uniq


# === SECTION: store (data/linear_noise.sqlite) ===
#   kg(kg PK, digest, ver, n_apex, n_flag, n_runs, ts)
#   run(kg, cell, x0, y0, x1, y1, n, len_m, h_med, sags, sag_m, chord, crown_med, lat_rms, keys BLOB)
#     cell = res-12 id of the run's middle apex = subject of its flag report
#     keys = sorted uint64 (x << 32 | y) of the run's apices

SOURCE = 'auto-linear'
HINT = 'linear'
_KEY_TTL = 60.0


def _db_path():
    import cells_api as ca
    return ca.NE_DIR.parent / 'linear_noise.sqlite'


def _conn(write: bool = False):
    import sqlite3
    p = _db_path()
    if not write:
        if not p.exists():
            return None
        return sqlite3.connect(f'file:{p}?mode=ro', uri=True, timeout=5)
    c = sqlite3.connect(str(p), timeout=30)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('CREATE TABLE IF NOT EXISTS kg (kg TEXT PRIMARY KEY, digest TEXT, ver INTEGER, n_apex INTEGER,'
              ' n_flag INTEGER, n_runs INTEGER, ts INTEGER, sig INTEGER) WITHOUT ROWID')
    if 'sig' not in {r[1] for r in c.execute('PRAGMA table_info(kg)')}:
        c.execute('ALTER TABLE kg ADD COLUMN sig INTEGER')
    c.execute('PRAGMA synchronous=NORMAL')
    c.execute('CREATE TABLE IF NOT EXISTS run (kg TEXT, cell INTEGER, x0 INTEGER, y0 INTEGER, x1 INTEGER, y1 INTEGER,'
              ' n INTEGER, len_m REAL, h_med REAL, sags INTEGER, sag_m REAL, chord REAL, crown_med REAL,'
              ' lat_rms REAL, keys BLOB)')
    c.execute('CREATE INDEX IF NOT EXISTS run_kg ON run(kg)')
    c.execute('CREATE INDEX IF NOT EXISTS run_cell ON run(cell)')
    return c


def apex_keys(x, y) -> np.ndarray:
    return (np.asarray(x).astype(np.uint64) << np.uint64(32)) | np.asarray(y).astype(np.uint64)


def load_kg(kg: str) -> dict | None:
    """All apices of one KG file -> x / y (3035 m), h, cr, cell (res-12 id) + the file digest."""
    import cells_api as ca
    kg = str(kg).zfill(5)
    hdr = ca.header(kg, with_chunks=True)
    if not hdr:
        return None
    xs, ys, hs, cs, ids = [], [], [], [], []
    for hrow in hdr.get('chunks') or []:
        nt = int(hrow.get('trees') or 0)
        if not nt:
            continue
        row = dict(hrow, r10=int(hrow['r10'], 16), trees=nt, bld=int(hrow.get('bld') or 0))
        try:
            ch = ca._load_chunk(kg, row, hdr)
        except Exception:  # noqa: BLE001
            continue
        d = ca.decode_trees(ch.trees)
        if not d['h_m'].size:
            continue
        ci = np.repeat(np.arange(ch.cells), ch.obs['tn'].astype(np.int64))[:ch.trees.size]
        cen = ch.centres()
        xs.append((cen[ci, 0] + d['dx']).astype(np.int32)); ys.append((cen[ci, 1] + d['dy']).astype(np.int32))
        hs.append(d['h_m'].astype(np.float32)); cs.append(d['crown_m2'].astype(np.float32))
        ids.append(ch.ids[ci].astype(np.uint64))
    cat = lambda a, t: np.concatenate(a).astype(t) if a else np.zeros(0, t)   # noqa: E731
    return {'x': cat(xs, np.int32), 'y': cat(ys, np.int32), 'h': cat(hs, np.float32), 'cr': cat(cs, np.float32),
            'cell': cat(ids, np.uint64), 'digest': hdr.get('neo1_digest')}


def process_kg(kg: str, conn=None, force: bool = False, flags: bool = True) -> dict | None:
    """Detect + store + sync the flag reports of one KG.  No-op while the file digest is unchanged."""
    import time
    import cells_api as ca
    kg = str(kg).zfill(5)
    c = conn or _conn(write=True)
    hdr = ca.header(kg)
    if not hdr:
        return None
    row = c.execute('SELECT digest, ver FROM kg WHERE kg=?', (kg,)).fetchone()
    if row and not force and row[0] == hdr.get('neo1_digest') and row[1] == VERSION:
        return {'kg': kg, 'skipped': True}
    d = load_kg(kg)
    if d is None:
        return None
    m, runs = detect(d['x'], d['y'], d['h'], d['cr'])
    # runs other KG files already hold in the overlap: share their report (one flag per physical run)
    other = []
    if runs and d['x'].size:
        bb = (int(d['x'].min()), int(d['x'].max()), int(d['y'].min()), int(d['y'].max()))
        other = [(int(cl), np.frombuffer(k, np.uint64)) for cl, k in c.execute(
            'SELECT cell, keys FROM run WHERE kg != ? AND ((x0 BETWEEN ? AND ? AND y0 BETWEEN ? AND ?) '
            'OR (x1 BETWEEN ? AND ? AND y1 BETWEEN ? AND ?))', (kg, *bb, *bb))]
    rows = []; items = {}
    for r in runs:
        idx = r['idx']; mid = idx[idx.size // 2]
        cell = int(d['cell'][mid])
        keys = np.unique(apex_keys(d['x'][idx], d['y'][idx]))
        for ocell, okeys in other:
            if np.isin(keys, okeys, assume_unique=True).sum() >= 0.5 * min(keys.size, okeys.size):
                cell = ocell
                break
        rows.append((kg, cell, round(r['x0']), round(r['y0']), round(r['x1']), round(r['y1']), r['n'], r['len_m'],
                     r['h_med'], r['sags'], r['sag_m'], r['res_m'], r['crown_med'], r['lat_rms'], keys.tobytes()))
        items.setdefault(cell, {'cell': cell, 'x': int(d['x'][mid]), 'y': int(d['y'][mid]), 'n': int(r['n']),
                                'len_m': int(round(r['len_m'])), 'h_m': float(d['h'][mid])})
    import zlib
    rows.sort(key=lambda r: (r[2], r[3], r[4], r[5]))
    sig = 0
    for r in rows:
        sig = zlib.crc32(r[14], zlib.crc32(str(r[1]).encode(), sig))
    prev = c.execute('SELECT sig, ts FROM kg WHERE kg=?', (kg,)).fetchone()
    same = bool(prev and prev[0] == sig and not force)
    old = {int(r[0]) for r in c.execute('SELECT DISTINCT cell FROM run WHERE kg=?', (kg,))}
    with c:
        if not same:
            c.execute('DELETE FROM run WHERE kg=?', (kg,))
            c.executemany('INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
        c.execute('INSERT OR REPLACE INTO kg VALUES (?,?,?,?,?,?,?,?)',
                  (kg, d['digest'], VERSION, int(d['x'].size), int(m.sum()), len(runs),
                   prev[1] if same else int(time.time()), sig))
    out = {'kg': kg, 'n_apex': int(d['x'].size), 'n_flag': int(m.sum()), 'runs': len(runs)}
    gone = [cl for cl in old - set(items)
            if not c.execute('SELECT 1 FROM run WHERE cell=? LIMIT 1', (cl,)).fetchone()]
    if same:
        out['unchanged'] = True
        return out
    if flags and (items or gone):
        try:
            out['flags'] = sync_flags(kg, list(items.values()), gone)
        except Exception as ex:  # noqa: BLE001
            out['flags_error'] = str(ex)
    return out


def sync_flags(kg: str, items: list, gone: list) -> dict:
    """One ``auto-linear`` report per run of ``kg`` (single transaction); reports of
    runs that no longer exist in any KG (``gone``) go ``stale``.  Human verdicts stay."""
    import json
    import cells_api as ca
    import public_flags as pf
    if items:
        lon, lat = ca._tf('EPSG:3035', 'EPSG:4326').transform([i['x'] for i in items], [i['y'] for i in items])
        for i, lo, la in zip(items, lon, lat):
            i['lon'] = float(lo); i['lat'] = float(la)
            i['expected'] = json.dumps({'absent': True, 'hint': HINT, 'n': i['n'], 'len_m': i['len_m']},
                                       separators=(',', ':'))
            i['observed'] = json.dumps({'h_m': i['h_m']}, separators=(',', ':'))
    return pf.auto_sync(kg, SOURCE, 'trees', 'spurious', items, gone)


# === SECTION: read side (ne_trees) ===
# The whole store is a few MB (one uint64 per hidden apex), so a serving process keeps ONE
# in-memory snapshot {kg: sorted keys}.  A request costs dict lookups + one searchsorted per
# chunk that can contain a hit; the disk is touched only by a stat() every _STAT_S seconds
# and re-read only when the store file changed (or a report verdict may have: _KEY_TTL).

_STAT_S = 10.0
_snap = {'t': 0.0, 'stamp': None, 'loaded': 0.0, 'keys': {}, 'tok': {}, 'grid': {}}
_snap_lock = __import__('threading').Lock()
_EMPTY = np.zeros(0, np.uint64)


def _stamp():
    out = []
    p = _db_path()
    for q in (p, p.with_name(p.name + '-wal')):
        try:
            st = q.stat(); out.append((st.st_mtime_ns, st.st_size))
        except OSError:
            out.append(None)
    return tuple(out)


def _rejected() -> set:
    try:
        import public_flags as pf
        return pf.auto_cells(None, SOURCE, status='rejected')
    except Exception:  # noqa: BLE001
        return set()


def _snapshot() -> dict:
    import time
    now = time.time()
    sn = _snap
    if now - sn['t'] < _STAT_S:
        return sn
    with _snap_lock:
        if now - _snap['t'] < _STAT_S:
            return _snap
        stamp = _stamp()
        if stamp == _snap['stamp'] and now - _snap['loaded'] < _KEY_TTL:
            _snap['t'] = now
            return _snap
        rej = _rejected()
        if stamp == _snap['stamp'] and rej == _snap.get('rej'):
            _snap.update(t=now, loaded=now)
            return _snap
        keys, tok, grid = {}, {}, {}
        c = _conn() if stamp[0] else None
        if c is not None:
            try:
                parts = {}
                for kg, cell, k in c.execute('SELECT kg, cell, keys FROM run'):
                    if int(cell) not in rej:
                        parts.setdefault(kg, []).append(np.frombuffer(k, np.uint64))
                for kg, p in parts.items():
                    v = np.unique(np.concatenate(p)); keys[kg] = v; grid[kg] = _key_grid(v)
                rk = {}
                for kg, cell in c.execute('SELECT kg, cell FROM run'):
                    if int(cell) in rej:
                        rk.setdefault(kg, []).append(int(cell))
                for kg, sig in c.execute('SELECT kg, sig FROM kg WHERE n_runs > 0'):
                    r = rk.get(kg)
                    tok[kg] = f'{(sig or 0):x}' + (f'-{len(r)}' if r else '')
            except Exception:  # noqa: BLE001
                c.close()
                _snap['t'] = now            # keep serving the previous snapshot
                return _snap
            c.close()
        _snap.update(t=now, loaded=now, stamp=stamp, rej=rej, keys=keys, tok=tok, grid=grid)
        return _snap


def _kg_keys(kg: str) -> np.ndarray:
    return _snapshot()['keys'].get(kg, _EMPTY)


def hidden_keys(kgs) -> np.ndarray:
    """Sorted apex keys to hide for a request touching ``kgs`` (union: KG files overlap)."""
    k = _snapshot()['keys']
    parts = [k[g] for g in (str(g).zfill(5) for g in kgs) if g in k]
    if not parts:
        return _EMPTY
    return parts[0] if len(parts) == 1 else np.unique(np.concatenate(parts))


GRID_SHIFT = 8          # 256 m coarse grid: "can this chunk hold a hidden apex?" is a set lookup


def grid_ids(x, y) -> frozenset:
    """Coarse grid cells touched by these points (bounding box, as ints)."""
    x0, x1 = int(np.min(x)) >> GRID_SHIFT, int(np.max(x)) >> GRID_SHIFT
    y0, y1 = int(np.min(y)) >> GRID_SHIFT, int(np.max(y)) >> GRID_SHIFT
    return frozenset((gx << 20) | gy for gx in range(x0, x1 + 1) for gy in range(y0, y1 + 1))


def _key_grid(keys: np.ndarray) -> frozenset:
    g = ((keys >> np.uint64(32 + GRID_SHIFT)) << np.uint64(20)) | ((keys & np.uint64(0xFFFFFFFF)) >> np.uint64(GRID_SHIFT))
    return frozenset(np.unique(g).tolist())


def hidden(kgs) -> tuple:
    """(sorted keys, coarse grid set) to hide for a request touching ``kgs``."""
    sn = _snapshot()
    k = sn['keys']
    ks = [g for g in (str(g).zfill(5) for g in kgs) if g in k]
    if not ks:
        return _EMPTY, frozenset()
    if len(ks) == 1:
        return k[ks[0]], sn['grid'][ks[0]]
    return np.unique(np.concatenate([k[g] for g in ks])), frozenset().union(*(sn['grid'][g] for g in ks))


def is_hidden(x, y, keys: np.ndarray, sel=None) -> np.ndarray:
    """Bool per apex; with ``sel`` (bool mask) only those are tested."""
    if sel is None:
        k = apex_keys(x, y)
        i = np.searchsorted(keys, k)
        return keys[np.minimum(i, keys.size - 1)] == k
    out = np.zeros(sel.shape, bool)
    j = np.flatnonzero(sel)
    if j.size and keys.size:
        k = apex_keys(x[j], y[j])
        i = np.searchsorted(keys, k)
        out[j] = keys[np.minimum(i, keys.size - 1)] == k
    return out


def state_token(kgs) -> str:
    """Changes when the served result of any of ``kgs`` changes (part of the /trees/bbox ETag):
    content signature of the KG's runs, not a timestamp - a recompute with the same result keeps it."""
    v = _snapshot()['tok']
    return ','.join(f'{k}:{v[k]}' for k in sorted(str(g).zfill(5) for g in kgs) if k in v)


# === SECTION: ops ===

def status() -> dict:
    c = _conn()
    if c is None:
        return {'kgs': 0}
    try:
        a = c.execute('SELECT COUNT(*), SUM(n_runs > 0), SUM(n_apex), SUM(n_flag), SUM(n_runs), MAX(ts) FROM kg '
                      'WHERE ver=?', (VERSION,)).fetchone()
        km = c.execute('SELECT SUM(len_m) FROM run').fetchone()[0] or 0
    finally:
        c.close()
    return {'ver': VERSION, 'kgs': a[0], 'kgs_with_runs': a[1] or 0, 'apices': a[2] or 0, 'hidden': a[3] or 0,
            'runs': a[4] or 0, 'km': round(km / 1000, 1), 'last': a[5], 'db_bytes': _db_path().stat().st_size}


def backfill(budget_s: float = 3000.0, flags: bool = True, quiet: bool = False) -> dict:
    import time
    import cells_api as ca
    t0 = time.time()
    c = _conn(write=True)
    done = {k: (d, v) for k, d, v in c.execute('SELECT kg, digest, ver FROM kg')}
    files = ca._files()
    todo = [k for k in sorted(files) if done.get(k) != (files[k].get('neo1_digest'), VERSION)]
    n = runs = hid = 0
    for kg in todo:
        if time.time() - t0 > budget_s:
            break
        try:
            r = process_kg(kg, c, flags=flags)
        except Exception as ex:  # noqa: BLE001
            print(f'ne_linear {kg}: {ex}', flush=True)
            continue
        ca._chunk_lru.pop_if(lambda k, kg=kg: k[0] == kg)
        if r and not r.get('skipped'):
            n += 1; runs += r['runs']; hid += r['n_flag']
            if not quiet and n % 200 == 0:
                print(f'ne_linear: {n}/{len(todo)} kgs, {runs} runs, {hid} apices, {time.time() - t0:.0f}s', flush=True)
    out = {'todo': len(todo), 'done': n, 'runs': runs, 'hidden': hid, 's': round(time.time() - t0, 1)}
    print('ne_linear backfill:', out, flush=True)
    return out


def misses(limit: int = 200) -> list:
    """Human reports that carry ``hint: linear`` but are on no stored run - the tuning set for the gates."""
    import public_flags as pf
    out = []
    for r in pf.hinted_reports(HINT, exclude_source=SOURCE, limit=limit):
        x, y = _tf_fwd(r['lon'], r['lat'])
        keys = _kg_keys(r['kg']) if r.get('kg') else np.zeros(0, np.uint64)
        near = False
        if keys.size:
            kx = (keys >> np.uint64(32)).astype(np.int64); ky = (keys & np.uint64(0xFFFFFFFF)).astype(np.int64)
            near = bool((np.hypot(kx - x, ky - y) <= 15).any())
        if not near:
            out.append({**r, 'x': round(x), 'y': round(y)})
    return out


def _tf_fwd(lon, lat):
    import cells_api as ca
    return ca._tf('EPSG:4326', 'EPSG:3035').transform(lon, lat)


if __name__ == '__main__':
    import argparse
    import json
    import logging
    logging.basicConfig(level=logging.WARNING)
    ap = argparse.ArgumentParser()
    ap.add_argument('--backfill', action='store_true'); ap.add_argument('--budget-s', type=float, default=3000.0)
    ap.add_argument('--kg'); ap.add_argument('--force', action='store_true'); ap.add_argument('--no-flags', action='store_true')
    ap.add_argument('--status', action='store_true'); ap.add_argument('--misses', action='store_true')
    ap.add_argument('--quiet', action='store_true')
    a = ap.parse_args()
    if a.kg:
        print(json.dumps(process_kg(a.kg, force=a.force, flags=not a.no_flags)))
    if a.backfill:
        backfill(a.budget_s, flags=not a.no_flags, quiet=a.quiet)
    if a.misses:
        for r in misses():
            print(json.dumps(r))
    if a.status:
        print(json.dumps(status()))
