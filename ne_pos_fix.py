"""ne_pos_fix — read-time position correction for tree apices / structures
of KGs whose NE file was built on 5 m-shifted rasters (docs/hillshade-seams.md).

Same idea as ``ne_green_fix``: no ``.h3b`` file, Zenodo product or JSON is
rewritten.  The correction is applied where chunks are inflated
(``cells_api._load_chunk``): the 5-bit dx / dy of every NET1 / NEB1 word is
moved by the KG's offset, so EVERY reader (``/trees/bbox``, cells API,
exports, linear-noise detector, tree_user matching) sees one consistent,
corrected frame.  A tree stays attached to the res-12 cell row it was
recorded in (its position may now lie just inside the neighbouring hexagon).
NEO1 cell statistics are NOT moved — that needs a reprocess.

Which KGs: NOT derived from the grid25 registration (a product can be
shifted in some tiles only).  Each candidate KG is measured directly:
apices vs. the BEV 1 m nDSM in a few small windows   A KG is fixed only when every
window says "shifted" (an apex of height h must sit on nDSM ≈ h: median
|nDSM − h| ≈ 0.5 m in the right frame, ≈ 4 m in the wrong one) and at
least ``MIN_SHIFT_WINDOWS`` do; any window or > 10 % of the 125 m sub-cells
at zero shift → ``mixed`` → left alone.

    python3 ne_pos_fix.py --verify [--budget-mb-h 150] [--max-kgs N]   # resumable, throttled
    python3 ne_pos_fix.py --status

State: ``data/ne_pos_fix.json`` ``{kgs: {kg: {verdict, off, windows, ts}}}``
(in git — it is measured data, cheap to keep, expensive to redo).
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

import numpy as np

DATA = Path(__file__).resolve().parent / 'data'
PATH = Path(os.environ.get('NE_POS_FIX', str(DATA / 'ne_pos_fix.json')))
SHIFT = (-5, 5)                 # true position = stored position + SHIFT
WINDOW_M = 250
WINDOWS_PER_KG = 4
MIN_SHIFT_WINDOWS = 3
MIN_TREES = 40                  # per window
RATIO = 0.5                     # winning frame: median |nDSM − h| below this × the other frame's …
ERR_OK = 1.5                    # … and below this many metres
SUB_M = 125; SUB_MIN_TREES = 12 # per-sub-cell verdicts inside a window
MIXED_SUB_FRAC = 0.1            # minority sub-cells above this share → the KG is mixed
_LOCK = threading.Lock()
_ST = {'mtime': None, 'off': {}, 'n': 0}


def _load() -> dict:
    try:
        mt = PATH.stat().st_mtime
    except OSError:
        return _ST
    if _ST['mtime'] != mt:
        with _LOCK:
            try:
                d = json.loads(PATH.read_text())
                off = {k: (int(v['off'][0]), int(v['off'][1])) for k, v in (d.get('kgs') or {}).items()
                       if v.get('verdict') == 'shift' and v.get('off')}
                _ST.update(mtime=mt, off=off, n=len(off))
            except Exception:  # noqa: BLE001 — a half-written file never breaks a read path
                _ST['mtime'] = mt
    return _ST


def offset(kg) -> tuple[int, int] | None:
    return _load()['off'].get(str(kg).zfill(5))


def tag(kg) -> str:
    """Cache-key / ETag suffix: '' for an untouched KG (keys stay as they were)."""
    o = offset(kg)
    return f'|p{o[0]:+d}{o[1]:+d}' if o else ''


def _shift5(v, d):
    v = v.astype(np.int64)
    v[v >= 16] -= 32
    return (np.clip(v + d, -16, 15) & 31)


def fix_words(kg, trees: bytes, bld: bytes) -> tuple[bytes, bytes]:
    """NET1 (u32) / NEB1 (u64) rows with dx / dy moved by the KG's offset."""
    o = offset(kg)
    if not o:
        return trees, bld
    if trees:
        w = np.frombuffer(trees, '<u4').copy()
        dx = _shift5(w & 31, o[0]); dy = _shift5((w >> 5) & 31, o[1])
        w = (w & np.uint32(0xFFFFFC00)) | dx.astype(np.uint32) | (dy.astype(np.uint32) << 5)
        trees = w.astype('<u4').tobytes()
    if bld:
        w = np.frombuffer(bld, '<u8').copy()
        dx = _shift5(w & np.uint64(31), o[0]); dy = _shift5((w >> np.uint64(5)) & np.uint64(31), o[1])
        w = (w & ~np.uint64(1023)) | dx.astype(np.uint64) | (dy.astype(np.uint64) << np.uint64(5))
        bld = w.astype('<u8').tobytes()
    return trees, bld


def shift_lonlat_points(kg, pts: set) -> set:
    """{(round(lon,5), round(lat,5))} recorded in the stored frame → corrected frame."""
    o = offset(kg)
    if not o or not pts:
        return pts
    import cells_api as ca
    a = np.array(sorted(pts), np.float64)
    x, y = ca._tf('EPSG:4326', 'EPSG:3035').transform(a[:, 0], a[:, 1])
    lon, lat = ca._tf('EPSG:3035', 'EPSG:4326').transform(np.asarray(x) + o[0], np.asarray(y) + o[1])
    return {(round(float(a_), 5), round(float(b_), 5)) for a_, b_ in zip(lon, lat)}


def status() -> dict:
    try:
        d = json.loads(PATH.read_text())
    except Exception as e:  # noqa: BLE001
        return {'present': False, 'error': str(e)}
    import collections
    c = collections.Counter(v.get('verdict') for v in (d.get('kgs') or {}).values())
    return {'present': True, 'kgs': len(d.get('kgs') or {}), 'verdicts': dict(c),
            'candidates_left': len(candidates()) - len(d.get('kgs') or {}), 'updated_at': d.get('updated_at'),
            'mb': round(float(d.get('mb') or 0), 1)}


# ── measurement ────────────────────────────────────────────────────────

def candidates() -> list[str]:
    """Parents with any shifted / mixed product in the grid25 registration."""
    try:
        d = json.loads((DATA / 'grid25_registration.json').read_text())
    except Exception:  # noqa: BLE001
        return []
    return sorted({c.split('-')[0] for c in list(d.get('offsets') or {}) + list(d.get('mixed') or [])})


def _raw_apices(kg):
    """x, y (stored frame, EPSG:3035 m), h of every apex + the chunk each is in."""
    import cells_api as ca
    hdr = ca.header(kg, with_chunks=True)
    xs = []; ys = []; hs = []
    if not hdr:
        return None
    for hrow in hdr.get('chunks') or []:
        nt = int(hrow.get('trees') or 0)
        if not nt:
            continue
        row = dict(hrow, r10=int(hrow['r10'], 16), trees=nt, bld=int(hrow.get('bld') or 0))
        ch = ca._load_chunk(kg, row, hdr, raw=True)
        d = ca.decode_trees(ch.trees)
        ci = np.repeat(np.arange(ch.cells), ch.obs['tn'].astype(np.int64))[:ch.trees.size]
        cen = ch.centres()
        xs.append(cen[ci, 0] + d['dx']); ys.append(cen[ci, 1] + d['dy']); hs.append(d['h_m'])
    if not xs:
        return None
    return np.concatenate(xs), np.concatenate(ys), np.concatenate(hs)


def _pick_windows(x, y, h, k=WINDOWS_PER_KG):
    """k window centres spread over the KG, each on a dense patch of real
    trees (≥ 8 m — hedges and shrubs carry little signal)."""
    m = h >= 8.0
    if m.sum() < MIN_TREES:
        m = h >= 3.0
    x = x[m]; y = y[m]
    if x.size < MIN_TREES:
        return []
    gx = np.floor(x / WINDOW_M).astype(np.int64); gy = np.floor(y / WINDOW_M).astype(np.int64)
    key, cnt = np.unique(np.stack([gx, gy], 1), axis=0, return_counts=True)
    ok = cnt >= MIN_TREES
    key = key[ok]; cnt = cnt[ok]
    out = []
    while len(out) < k and key.shape[0]:
        if not out:
            i = int(np.argmax(cnt))
        else:   # farthest-point sampling, weighted towards dense cells
            dmin = np.min([np.hypot(key[:, 0] - a, key[:, 1] - b) for a, b in out], axis=0)
            if dmin.max() < 2:
                break
            i = int(np.argmax(dmin * np.sqrt(cnt)))
        out.append((int(key[i, 0]), int(key[i, 1])))
        keep = np.ones(key.shape[0], bool); keep[i] = False
        key = key[keep]; cnt = cnt[keep]
    return [((a + 0.5) * WINDOW_M, (b + 0.5) * WINDOW_M) for a, b in out]


def _mixed_parents() -> set:
    try:
        d = json.loads((DATA / 'grid25_registration.json').read_text())
    except Exception:  # noqa: BLE001
        return set()
    return {c.split('-')[0] for c in d.get('mixed') or []}


def _tile_windows(kg, x, y, h):
    """One window per processing tile (densest 250 m patch inside it) — for
    parents whose terrain grid is only partly shifted, every tile has to be
    measured.  None when the tile list is unknown."""
    import cells_api as ca
    import kg_v2_store as kvs
    tiles = []
    for code in [kg] + [c for c in kvs.codes_for_parent(kg) if c != kg]:
        try:
            d = kvs.get(code) or {}
        except Exception:  # noqa: BLE001
            d = {}
        tiles += [t['bbox_wgs'] for t in (d.get('data_quality') or {}).get('tiles') or [] if t.get('bbox_wgs')]
    if not tiles:
        return None
    tf = ca._tf('EPSG:4326', 'EPSG:3035')
    m = h >= 3.0
    gx = np.floor(x[m] / WINDOW_M).astype(np.int64); gy = np.floor(y[m] / WINDOW_M).astype(np.int64)
    key, cnt = np.unique(np.stack([gx, gy], 1), axis=0, return_counts=True)
    out = set()
    for w, s_, e, n in tiles:
        ex, ny = tf.transform([w, e, w, e], [s_, s_, n, n])
        x0, x1, y0, y1 = max(ex[0], ex[2]), min(ex[1], ex[3]), max(ny[0], ny[1]), min(ny[2], ny[3])
        k = ((key[:, 0] * WINDOW_M >= x0) & ((key[:, 0] + 1) * WINDOW_M <= x1)
             & (key[:, 1] * WINDOW_M >= y0) & ((key[:, 1] + 1) * WINDOW_M <= y1) & (cnt >= MIN_TREES))
        if k.any():
            j = np.flatnonzero(k)[int(np.argmax(cnt[k]))]
            out.add((int(key[j, 0]), int(key[j, 1])))
    return [((a + 0.5) * WINDOW_M, (b + 0.5) * WINDOW_M) for a, b in sorted(out)]


def window_verdict(x, y, h, cx, cy) -> dict:
    """Read one BEV nDSM window and score the two hypotheses per apex: an
    apex of height h sits on nDSM ≈ h at its TRUE position, so
    ``|nDSM(pos) − h|`` is small for the right frame (median ≈ 0.5 m) and
    several metres for the wrong one.  Also judged per ``SUB_M`` sub-cell so
    a window straddling shifted and unshifted ground is seen as such."""
    import raster_io
    from shapely.geometry import box
    half = WINDOW_M / 2 + 10
    d = raster_io.read_dtm_dsm(box(cx - half, cy - half, cx + half, cy + half), pad=0.0)
    nd = d['ndsm']; tf = d['transform']
    m = (np.abs(x - cx) <= WINDOW_M / 2) & (np.abs(y - cy) <= WINDOW_M / 2) & (h >= 3.0)
    xx = x[m]; yy = y[m]; hh = h[m]

    def err(dx, dy):
        c = np.clip(((xx + dx - tf.c) / tf.a).astype(np.int64), 0, nd.shape[1] - 1)
        r = np.clip(((yy + dy - tf.f) / tf.e).astype(np.int64), 0, nd.shape[0] - 1)
        return np.abs(nd[r, c] - hh)
    ez = err(0, 0); es = err(*SHIFT)
    ok = np.isfinite(ez) & np.isfinite(es)
    if ok.sum() < MIN_TREES:
        return {'v': 'nodata', 'n': int(ok.sum()), 'c': [round(cx), round(cy)]}

    def judge(k):
        a = float(np.median(ez[k])); b = float(np.median(es[k]))
        return ('shift' if b < RATIO * a and b < ERR_OK else 'zero' if a < RATIO * b and a < ERR_OK else 'unclear'), a, b
    v, a, b = judge(ok)
    sub = {'shift': 0, 'zero': 0, 'unclear': 0}
    gx = np.floor(xx / SUB_M).astype(np.int64); gy = np.floor(yy / SUB_M).astype(np.int64)
    for u in set(zip(gx[ok].tolist(), gy[ok].tolist())):
        k = ok & (gx == u[0]) & (gy == u[1])
        if k.sum() >= SUB_MIN_TREES:
            sub[judge(k)[0]] += 1
    return {'v': v, 'n': int(ok.sum()), 'zero': round(a, 2), 'shift': round(b, 2), 'sub': [sub['shift'], sub['zero'], sub['unclear']],
            'c': [round(cx), round(cy)]}


def verify_kg(kg) -> dict:
    a = _raw_apices(kg)
    if a is None:
        return {'verdict': 'no_trees', 'windows': [], 'ts': int(time.time())}
    x, y, h = a
    per_tile = kg in _mixed_parents()
    cen = _tile_windows(kg, x, y, h) if per_tile else _pick_windows(x, y, h)
    if cen is None:
        return {'verdict': 'unclear', 'why': 'mixed grid, tile list unknown', 'windows': [], 'ts': int(time.time())}
    wins = [window_verdict(x, y, h, cx, cy) for cx, cy in cen]
    ns = sum(w['v'] == 'shift' for w in wins); nz = sum(w['v'] == 'zero' for w in wins)
    ss = sum(w.get('sub', [0, 0, 0])[0] for w in wins); sz = sum(w.get('sub', [0, 0, 0])[1] for w in wins)
    if ns and nz or (ss and sz and min(ss, sz) > MIXED_SUB_FRAC * (ss + sz)):
        verdict = 'mixed'
    elif ns >= MIN_SHIFT_WINDOWS and all(w['v'] in ('shift', 'nodata') or w.get('shift', 9) < w.get('zero', 0)
                                         for w in wins) and (not per_tile or ns >= 0.8 * len(wins)):
        verdict = 'shift'
    elif nz and not ns:
        verdict = 'zero'
    else:
        verdict = 'unclear'
    out = {'verdict': verdict, 'windows': wins, 'ts': int(time.time()), 'per_tile': per_tile}
    if verdict == 'shift':
        out['off'] = list(SHIFT)
    return out


def _migrate_stores(kg, off) -> dict:
    """User tree reports are keyed on the apex position (stored frame) —
    move them with the apices.  Idempotent per KG via the caller's state."""
    import sqlite3
    n = 0
    if PATH.parent != DATA:      # test / dry-run state file → never touch the live store
        return {'tree_ver': 0, 'dry': True}
    p = DATA / 'public_flags.sqlite'
    if p.exists():
        c = sqlite3.connect(str(p), timeout=30)
        try:
            cols = {r[1] for r in c.execute('PRAGMA table_info(tree_ver)')}
            if 'x' in cols:
                extra = ', e_dm = e_dm + ?, n_dm = n_dm + ?' if {'e_dm', 'n_dm'} <= cols else ''
                args = [off[0], off[1]] + ([off[0] * 10, off[1] * 10] if extra else []) + [int(kg)]
                n = c.execute(f'UPDATE tree_ver SET x = x + ?, y = y + ?{extra} WHERE kg = ?', args).rowcount
                c.commit()
        finally:
            c.close()
    return {'tree_ver': n}


def verify(budget_mb_h: float = 150.0, max_kgs: int | None = None, only=None, quiet=False) -> dict:
    import psutil
    try:
        doc = json.loads(PATH.read_text())
    except Exception:  # noqa: BLE001
        doc = {'kgs': {}}
    kgs = doc.setdefault('kgs', {})
    todo = [k for k in (only or candidates()) if k not in kgs]
    if max_kgs:
        todo = todo[:max_kgs]
    t0 = time.time(); n0 = psutil.net_io_counters().bytes_recv; done = 0
    for kg in todo:
        b0 = psutil.net_io_counters().bytes_recv
        try:
            r = verify_kg(kg)
        except Exception as e:  # noqa: BLE001
            r = {'verdict': 'error', 'error': repr(e)[:200], 'windows': [], 'ts': int(time.time())}
        if r['verdict'] == 'shift':
            try:
                r['migrated'] = _migrate_stores(kg, r['off'])
            except Exception as e:  # noqa: BLE001 — do not serve a KG whose reports could not follow
                r = dict(r, verdict='error', error='migrate: ' + repr(e)[:160]); r.pop('off', None)
        if r['verdict'] != 'error':
            kgs[kg] = r
        doc['mb'] = float(doc.get('mb') or 0) + (psutil.net_io_counters().bytes_recv - b0) / 1e6
        doc['updated_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        tmp = PATH.with_suffix('.tmp'); tmp.write_text(json.dumps(doc)); os.replace(tmp, PATH)
        done += 1
        if not quiet:
            print(kg, r['verdict'], [w.get('v') for w in r['windows']], r.get('error', ''), flush=True)
        if r['verdict'] == 'shift':
            try:    # linear-noise runs are keyed on apex positions → recompute this KG now
                import ne_linear
                ne_linear.process_kg(kg)
            except Exception as e:  # noqa: BLE001
                print('ne_linear', kg, e, file=sys.stderr)
        # throttle on host-wide received bytes (BEV range reads dominate)
        used = (psutil.net_io_counters().bytes_recv - n0) / 1e6
        wait = used / budget_mb_h * 3600 - (time.time() - t0)
        if wait > 0:
            time.sleep(min(wait, 600))
    return {'done': done, 'left': len(candidates()) - len(kgs), **status()}


if __name__ == '__main__':
    if '--verify' in sys.argv:
        def _arg(n, d, t):
            return t(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d
        only = _arg('--kgs', None, lambda s: s.split(','))
        print(json.dumps(verify(_arg('--budget-mb-h', 150.0, float), _arg('--max-kgs', None, int), only)))
    else:
        print(json.dumps(status()))
