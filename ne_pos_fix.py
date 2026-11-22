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
    python3 ne_pos_fix.py --verify-tiles [--budget-mb-h 150] [--max-kgs N] [--kgs a,b] [--dry]
    python3 ne_pos_fix.py --status

PARTLY shifted KGs (``--verify-tiles``, verdict ``partial``): the shift is a property of a processing
tile, so ``mixed`` / ``unclear`` KGs and the KGs of a ``rebin_added`` shift pair get one verdict per
tile (a window in the tile's core); the offset is then applied per word at its stored position
(:func:`moved`, tile ownership = the mosaic's feather rule).  Unmeasured tiles are not moved.

State: ``data/ne_pos_fix.json`` ``{kgs: {kg: {verdict, off, windows, ts}}}``
(in git — it is measured data, cheap to keep, expensive to redo).
"""
from __future__ import annotations

import json
import re
import os
import sys
import threading
import zlib
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
FLAG_APEX_M = 2.5               # = ne_trees.FLAG_APEX_M: a point flag names the apex within this
MIXED_SUB_FRAC = 0.1            # minority sub-cells above this share → the KG is mixed
_LOCK = threading.Lock()
FRAME_STALE = 'p0'              # token of the stored (pre-fix) frame; anything else = today's positions
TILE_MARGIN = 100.0            # feather width of the product mosaic (austria_processor._feather_weight)
CORE_M = 100.0                  # a tile is measured where its feather weight is 1 (no blend with a neighbour)
PAIR_MIN = 20                   # same-height partners in a neighbour file that decide an unmeasured tile
TILE_RULE = 2                   # entry field `tv`: measured per processing tile with this rule
_ST = {'mtime': None, 'off': {}, 'n': 0, 'past': {}, 'part': {}}


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
                # partly shifted KGs: the offset applies inside the processing tiles measured as shifted
                part = {}
                for k, v in (d.get('kgs') or {}).items():
                    if v.get('verdict') == 'partial' and v.get('off') and v.get('tiles'):
                        t = np.asarray(v['tiles'], np.float64).reshape(-1, 5)
                        part[k] = ((int(v['off'][0]), int(v['off'][1])), t[:, :4].copy(), t[:, 4] > 0,
                                   '%08x' % (zlib.crc32(json.dumps(v['tiles'], separators=(',', ':')).encode()) & 0xFFFFFFFF))
                # frame history (docs/tree-user.md → Position frames): a copy downloaded before the
                # fix (or before a regeneration that retired it) is `delta` off today's positions
                past = {}
                for k, v in (d.get('kgs') or {}).items():
                    f = _frames_of(v)
                    if f:
                        past[k] = f
                _ST.update(mtime=mt, off=off, n=len(off), past=past, part=part)
            except Exception:  # noqa: BLE001 — a half-written file never breaks a read path
                _ST['mtime'] = mt
    return _ST


def offset(kg) -> tuple[int, int] | None:
    """Offset of a KG moved AS A WHOLE (None for an untouched and for a partly moved KG — see
    :func:`partial` / :func:`moved`)."""
    return _load()['off'].get(str(kg).zfill(5))


def partial(kg):
    """Partly shifted KG → (off, tiles (n, 4) EPSG:3035 x0 y0 x1 y1, moved (n,) bool, digest), else None."""
    return _load()['part'].get(str(kg).zfill(5))


def any_offset(kg) -> tuple[int, int] | None:
    """The offset some or all positions of the KG are moved by."""
    st = _load(); kg = str(kg).zfill(5)
    return st['off'].get(kg) or (st['part'][kg][0] if kg in st['part'] else None)


def tile_owner(tiles: np.ndarray, x, y) -> np.ndarray:
    """Index of the processing tile a mosaic position belongs to (-1 = none): the tile with the
    highest feather weight there, first tile on ties — the pipeline's own ownership rule
    (``austria_processor``: weight = product of two 100 m edge ramps, ``fw > best``)."""
    x = np.asarray(x, np.float64).reshape(-1, 1); y = np.asarray(y, np.float64).reshape(-1, 1)
    if not len(tiles):
        return np.full(x.shape[0], -1, np.int64)
    dx = np.minimum(x - tiles[:, 0], tiles[:, 2] - x); dy = np.minimum(y - tiles[:, 1], tiles[:, 3] - y)
    w = np.clip(dx / TILE_MARGIN, 0.0, 1.0) * np.clip(dy / TILE_MARGIN, 0.0, 1.0)
    # a point outside every ramp but inside an envelope still belongs to that tile
    w = np.where((dx >= 0) & (dy >= 0), np.maximum(w, 1e-9), 0.0)
    o = np.argmax(w, axis=1)
    o[w[np.arange(w.shape[0]), o] <= 0] = -1
    return o


def moved(kg, x, y) -> np.ndarray:
    """Per STORED position (EPSG:3035 m): is it served ``any_offset(kg)`` away?  All True for a KG
    moved as a whole, all False for an untouched one, per owning tile for a partly shifted one."""
    st = _load(); kg = str(kg).zfill(5)
    n = np.asarray(x).reshape(-1).shape[0]
    if kg in st['off']:
        return np.ones(n, bool)
    p = st['part'].get(kg)
    if p is None:
        return np.zeros(n, bool)
    o = tile_owner(p[1], x, y)
    return (o >= 0) & p[2][np.clip(o, 0, None)]


def moves_point(kgs, x, y) -> bool:
    """Does a position of a pre-fix copy (= stored frame) move for any of *kgs*?"""
    st = _load()
    for k in kgs:
        k = str(k).zfill(5)
        if k not in st['part'] or bool(moved(k, [x], [y])[0]):      # whole-KG frames (and their history) always move
            return True
    return False


def tag(kg) -> str:
    """Cache-key / ETag suffix: '' for an untouched KG (keys stay as they were)."""
    o = offset(kg)
    if o:
        return f'|p{o[0]:+d}{o[1]:+d}'
    p = partial(kg)
    return f'|p{p[0][0]:+d}{p[0][1]:+d}~{p[3]}' if p else ''


def _tok(o) -> str:
    return f'p{int(o[0]):+d}{int(o[1]):+d}' if o and (o[0] or o[1]) else FRAME_STALE


def _frames_of(v: dict) -> dict | None:
    """Frame history of one state entry.  ``shift``: served = stored + off since ``ts``; a copy made
    before is frame p0, ``delta`` = off away.  ``retired`` (product regenerated on the true grid): the
    same.  ``rejected`` (fix overruled, positions stored again): a copy made WHILE the fix was on
    (``ts`` ≤ t < ``rejected_ts``) is ``-off`` away."""
    vd = v.get('verdict')
    o = v.get('off') if vd in ('shift', 'partial') else v.get('was') if vd in ('retired', 'rejected') else None
    if not o:
        return None
    o = (int(o[0]), int(o[1])); ts = int(v.get('ts') or 0)
    pt = {'partial': True} if vd == 'partial' or v.get('was_tiles') else {}
    if vd == 'rejected':
        return {'frame': FRAME_STALE, 'stale': _tok(o), 'delta': (-o[0], -o[1]), 'from': ts, 'until': int(v.get('rejected_ts') or ts), **pt}
    return {'frame': _tok(o), 'stale': FRAME_STALE, 'delta': o, 'from': 0, 'until': ts, **pt}


def frames(kg) -> dict | None:
    """None for a KG whose tree positions never changed, else ``{frame, stale, delta, from, until}``:
    ``frame`` = token of the positions served today; a copy made in [from, until) is frame ``stale``
    and its coordinates need ``delta`` (dx, dy) m added — for a ``partial`` KG only where
    :func:`moved` says so (the token is the same: "moved where measured as shifted")."""
    return _load()['past'].get(str(kg).zfill(5))


def all_frames() -> dict:
    return _load()['past']


def _update(fn):
    """Read-modify-write of the state file under a lock (verify, sync, unfix, retire may overlap)."""
    import fcntl
    PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(str(PATH) + '.lock', 'w') as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        try:
            doc = json.loads(PATH.read_text())
        except Exception:  # noqa: BLE001
            doc = {'kgs': {}}
        doc.setdefault('kgs', {})
        out = fn(doc)
        doc['updated_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        tmp = PATH.with_suffix('.tmp'); tmp.write_text(json.dumps(doc)); os.replace(tmp, PATH)
        return out


def retire(kg) -> bool:
    """Call when a shifted KG's product is REGENERATED on the true grid: the read-time shift stops,
    the frame history stays (stale copies still resolve), the KG is never re-measured."""
    kg = str(kg).zfill(5)

    def f(doc):
        v = doc['kgs'].get(kg)
        if not v or v.get('verdict') not in ('shift', 'partial'):
            return False
        doc['kgs'][kg] = {'verdict': 'retired', 'was': v['off'], 'ts': v.get('ts'), 'retired_ts': int(time.time()), 'windows': [],
                          **({'was_tiles': v['tiles']} if v.get('tiles') else {})}
        return True
    return _update(f)


def unfix(kg, db=None) -> bool:
    """Overrule (the KG's ``auto-shift`` report was rejected): serve stored positions again and move
    the user reports back.  Copies made while the fix was on stay resolvable (frame history)."""
    kg = str(kg).zfill(5)
    v = (json.loads(PATH.read_text()).get('kgs') or {}).get(kg) if PATH.exists() else None
    if not v or v.get('verdict') not in ('shift', 'partial'):
        return False
    o = v['off']
    _migrate_stores(kg, (-int(o[0]), -int(o[1])), db=db, stored=False, sel=_tiles_sel(v.get('tiles')))

    def f(doc):
        doc['kgs'][kg] = {'verdict': 'rejected', 'was': o, 'ts': v.get('ts'), 'rejected_ts': int(time.time()), 'windows': v.get('windows') or [],
                          **({'was_tiles': v['tiles']} if v.get('tiles') else {})}
        return True
    return _update(f)


# ── public record: one machine flag per KG (like ne_linear's auto-linear runs) ──

SOURCE = 'auto-shift'


def _flag(kg, v: dict) -> None:
    """KG-level report, layer ``trees``, claim ``misplaced``, source ``auto-shift`` — what we did to
    the 2.4 product of this KG (or know and did not do: ``applied: false``)."""
    import public_flags as pf
    vd = v.get('verdict')
    if vd not in ('shift', 'partial', 'mixed', 'retired', 'rejected'):
        return
    o = v.get('off') or v.get('was') or list(SHIFT)
    w = [x for x in v.get('windows') or [] if x.get('v') in ('shift', 'zero', 'unclear')]
    exp = {'hint': 'shift', 'applied': vd in ('shift', 'partial'), 'moved_m': [int(o[0]), int(o[1])], 'crs': 'EPSG:3035',
           'layers': ['trees', 'structures'], 'frame': _tok(o) if vd in ('shift', 'partial', 'retired') else FRAME_STALE, 'verdict': vd}
    tl = v.get('tiles') or v.get('was_tiles')
    if tl:      # partly shifted product: the processing tiles (EPSG:3035 x0 y0 x1 y1) whose content moved / stayed
        exp['partial'] = True
        exp['tiles'] = {'moved': sum(t[4] > 0 for t in tl), 'kept': sum(t[4] == 0 for t in tl), 'unmeasured': sum(t[4] < 0 for t in tl)}
        exp['moved_tiles'] = [[int(c) for c in t[:4]] for t in tl if t[4] > 0]
    if vd in ('shift', 'partial', 'retired', 'rejected'):
        exp['since'] = int(v.get('ts') or 0)
    if vd == 'rejected':
        exp['until'] = int(v.get('rejected_ts') or 0)
    obs = {'windows': len(w), 'shifted': sum(x['v'] == 'shift' for x in w),
           'err_m': {'moved': round(float(np.median([x['shift'] for x in w])), 2), 'stored': round(float(np.median([x['zero'] for x in w])), 2)}} if w else None
    pf.auto_kg(kg, SOURCE, 'trees', 'misplaced', json.dumps(exp, separators=(',', ':')),
               json.dumps(obs, separators=(',', ':')) if obs else None)


def sync(db=None) -> dict:
    """State ↔ flags: every decided KG has its report; a report a human REJECTED turns the fix off."""
    import public_flags as pf
    try:
        doc = json.loads(PATH.read_text())
    except Exception:  # noqa: BLE001
        return {'kgs': 0}
    st = pf.auto_kg_status(SOURCE)
    n = un = 0
    for kg, v in sorted((doc.get('kgs') or {}).items()):
        if v.get('verdict') in ('shift', 'partial') and st.get(kg) == 'rejected':
            un += bool(unfix(kg, db=db))
            v = json.loads(PATH.read_text())['kgs'][kg]
        _flag(kg, v); n += 1
    return {'kgs': n, 'unfixed': un}


def _shift5(v, d):
    v = v.astype(np.int64)
    v[v >= 16] -= 32
    return (np.clip(v + d, -16, 15) & 31)


def _s5(v):
    v = v.astype(np.int64)
    v[v >= 16] -= 32
    return v


def fix_words(kg, trees: bytes, bld: bytes, cells=None) -> tuple[bytes, bytes]:
    """NET1 (u32) / NEB1 (u64) rows with dx / dy moved by the KG's offset.  A partly shifted KG needs
    *cells* = callable → (centres (n, 2) EPSG:3035, trees per row, structures per row) of the chunk's
    NEO1 rows: every word is tested at its own stored position (:func:`moved`)."""
    o = offset(kg)
    p = None if o else partial(kg)
    if not o and (p is None or cells is None or not (trees or bld)):
        return trees, bld
    if p is not None:
        o = p[0]
        cen, tn, bn = cells()
        cen = np.asarray(cen, np.float64).reshape(-1, 2)
    if trees:
        w = np.frombuffer(trees, '<u4').copy()
        m = None
        if p is not None:
            ci = np.repeat(np.arange(cen.shape[0]), np.asarray(tn, np.int64))[:w.size]
            m = moved(kg, cen[ci, 0] + _s5(w[:ci.size] & 31), cen[ci, 1] + _s5((w[:ci.size] >> 5) & 31))
            m = np.concatenate([m, np.zeros(w.size - m.size, bool)])
        dx = _shift5(w & 31, o[0]); dy = _shift5((w >> 5) & 31, o[1])
        w2 = (w & np.uint32(0xFFFFFC00)) | dx.astype(np.uint32) | (dy.astype(np.uint32) << 5)
        trees = (w2 if m is None else np.where(m, w2, w)).astype('<u4').tobytes()
    if bld:
        w = np.frombuffer(bld, '<u8').copy()
        m = None
        if p is not None:
            ci = np.repeat(np.arange(cen.shape[0]), np.asarray(bn, np.int64))[:w.size]
            m = moved(kg, cen[ci, 0] + _s5(w[:ci.size] & np.uint64(31)), cen[ci, 1] + _s5((w[:ci.size] >> np.uint64(5)) & np.uint64(31)))
            m = np.concatenate([m, np.zeros(w.size - m.size, bool)])
        dx = _shift5(w & np.uint64(31), o[0]); dy = _shift5((w >> np.uint64(5)) & np.uint64(31), o[1])
        w2 = (w & ~np.uint64(1023)) | dx.astype(np.uint64) | (dy.astype(np.uint64) << np.uint64(5))
        bld = (w2 if m is None else np.where(m, w2, w)).astype('<u8').tobytes()
    return trees, bld


def shift_lonlat_points(kg, pts: set) -> set:
    """{(round(lon,5), round(lat,5))} recorded in the stored frame → corrected frame."""
    o = any_offset(kg)
    if not o or not pts:
        return pts
    import cells_api as ca
    a = np.array(sorted(pts), np.float64)
    x, y = ca._tf('EPSG:4326', 'EPSG:3035').transform(a[:, 0], a[:, 1])
    x = np.asarray(x, np.float64); y = np.asarray(y, np.float64)
    m = moved(kg, x, y)
    lon, lat = ca._tf('EPSG:3035', 'EPSG:4326').transform(x + o[0] * m, y + o[1] * m)
    return {(round(float(a_), 5), round(float(b_), 5)) if k else (float(p[0]), float(p[1]))
            for a_, b_, k, p in zip(lon, lat, m, a)}


def status() -> dict:
    try:
        d = json.loads(PATH.read_text())
    except Exception as e:  # noqa: BLE001
        return {'present': False, 'error': str(e)}
    import collections
    c = collections.Counter(v.get('verdict') for v in (d.get('kgs') or {}).values())
    tc = tile_candidates(d)
    tl = [t[4] for v in (d.get('kgs') or {}).values() if v.get('verdict') == 'partial' for t in v.get('tiles') or []]
    return {'present': True, 'kgs': len(d.get('kgs') or {}), 'verdicts': dict(c),
            'candidates_left': len(set(candidates()) - set(d.get('kgs') or {})), 'updated_at': d.get('updated_at'),
            'mb': round(float(d.get('mb') or 0), 1),
            'per_tile': {'measured': sum(v.get('tv') == TILE_RULE for v in (d.get('kgs') or {}).values()), 'todo': len(tc['todo']),
                         'partial_tiles': {'moved': sum(t > 0 for t in tl), 'kept': sum(t == 0 for t in tl), 'unmeasured': sum(t < 0 for t in tl)},
                         'rebin_pairs': tc['pairs'], 'review': tc['review'][:40]}}


# ── measurement ────────────────────────────────────────────────────────

def candidates() -> list[str]:
    """Parents with any shifted / mixed product in the grid25 registration."""
    try:
        d = json.loads((DATA / 'grid25_registration.json').read_text())
    except Exception:  # noqa: BLE001
        return []
    return sorted({c.split('-')[0] for c in list(d.get('offsets') or {}) + list(d.get('mixed') or [])})


def _raw_apices(kg, raw: bool = True):
    """x, y (stored frame, EPSG:3035 m; *raw* False: as served, QA not applied), h of every apex."""
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
        ch = ca._load_chunk(kg, row, hdr, raw=True) if raw else ca._load_chunk(kg, row, hdr, qa=False)
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


def _tiles_sel(tiles):
    """State ``tiles`` rows → selector(x, y stored) → bool (None = the whole KG moves)."""
    if not tiles:
        return None
    t = np.asarray(tiles, np.float64).reshape(-1, 5)

    def sel(x, y):
        o = tile_owner(t[:, :4], x, y)
        return (o >= 0) & (t[:, 4] > 0)[np.clip(o, 0, None)]
    return sel


# ── per-tile measurement (partly shifted KGs) ──────────────────────────────────
# Root cause is per PROCESSING TILE (docs/hillshade-seams.md): a tile restored without its raster
# sidecar was pasted 5 m off, its neighbour not.  The product mosaic feather-blends the tiles, so a
# verdict is measured where exactly one tile contributes (its core) and applied to everything that
# tile owns (highest feather weight).

def kg_tiles(kg) -> np.ndarray:
    """(n, 4) integer EPSG:3035 envelopes of the processing tiles of the parent's products (sweep
    units excluded: fresh, tiny, and only fill cells the main product does not have)."""
    import cells_api as ca
    import kg_splitter
    import kg_v2_store as kvs
    kg = str(kg).zfill(5)
    tf = ca._tf('EPSG:4326', 'EPSG:3035')
    out = []
    for code in [kg] + sorted(c for c in kvs.codes_for_parent(kg) if c != kg):
        if kg_splitter.is_sweep_code(code):
            continue
        try:
            d = kvs.get(code) or {}
        except Exception:  # noqa: BLE001
            d = {}
        for t in (d.get('data_quality') or {}).get('tiles') or []:
            if t.get('bbox_wgs'):
                w, s_, e, n = t['bbox_wgs']
                x, y = tf.transform([w, e, e, w], [s_, s_, n, n])
                out.append([round(min(x)), round(min(y)), round(max(x)), round(max(y))])
    return np.asarray(out, np.float64).reshape(-1, 4)


def _core(tiles: np.ndarray, x, y) -> np.ndarray:
    """Positions inside exactly ONE tile envelope (the mosaic there is that tile alone)."""
    x = np.asarray(x, np.float64).reshape(-1, 1); y = np.asarray(y, np.float64).reshape(-1, 1)
    return ((x >= tiles[:, 0]) & (x <= tiles[:, 2]) & (y >= tiles[:, 1]) & (y <= tiles[:, 3])).sum(1) == 1


def _clean(w: dict) -> str | None:
    """'shift' / 'zero' when a window says so without a dissenting sub-cell, else None."""
    sub = w.get('sub') or [0, 0, 0]
    if w.get('v') == 'shift' and not sub[1]:
        return 'shift'
    if w.get('v') == 'zero' and not sub[0]:
        return 'zero'
    return None


def _tile_cells(x, y, h):
    """Candidate 250 m window cells of one tile's core apices, densest first."""
    for hmin in (8.0, 3.0):
        m = h >= hmin
        if m.sum() < MIN_TREES:
            continue
        gx = np.floor(x[m] / WINDOW_M).astype(np.int64); gy = np.floor(y[m] / WINDOW_M).astype(np.int64)
        key, cnt = np.unique(np.stack([gx, gy], 1), axis=0, return_counts=True)
        k = cnt >= MIN_TREES
        if k.any():
            o = np.argsort(-cnt[k], kind='stable')
            return [(int(a), int(b)) for a, b in key[k][o].tolist()]
    return []


def verify_tiles_kg(kg, prev: dict | None = None, tries: int = 2, read: bool = True) -> dict:
    """One verdict per processing tile → ``shift`` (every measured tile shifted), ``zero``,
    ``partial`` (both kinds: ``tiles`` rows [x0, y0, x1, y1, 1 moved | 0 kept | -1 unmeasured]) or
    ``unclear``.  Windows of an earlier run that lie in one tile's core are reused (no BEV read);
    *read* = False: decide from those alone (dry run)."""
    now = int(time.time())
    a = _raw_apices(kg)
    if a is None:
        return {'verdict': 'no_trees', 'windows': [], 'ts': now, 'tv': TILE_RULE}
    T = kg_tiles(kg)
    if not len(T):
        return {'verdict': 'unclear', 'why': 'tile list unknown', 'windows': (prev or {}).get('windows') or [], 'ts': now, 'tv': TILE_RULE}
    x, y, h = a
    own = tile_owner(T, x, y); core = _core(T, x, y) & (h >= 3.0)
    wins = []; tv = {}
    for w in (prev or {}).get('windows') or []:              # free evidence first
        v = _clean(w)
        if v is None or not w.get('c'):
            continue
        m = (np.abs(x - w['c'][0]) <= WINDOW_M / 2) & (np.abs(y - w['c'][1]) <= WINDOW_M / 2) & (h >= 3.0)
        if m.sum() < MIN_TREES:
            continue
        ts_, cn = np.unique(own[m & core], return_counts=True)
        if ts_.size and cn.max() >= 0.8 * m.sum() and ts_[int(np.argmax(cn))] >= 0:
            t = int(ts_[int(np.argmax(cn))])
            wins.append(dict(w, t=t, reused=1))
            tv.setdefault(t, set()).add(v)
    for t in range(len(T)):
        if t in tv or not read:
            continue
        k = core & (own == t)
        cells = _tile_cells(x[k], y[k], h[k])
        if not cells:           # edge sliver without a core: what the tile owns (weight >= its neighbours')
            k = (own == t) & (h >= 3.0)
            cells = _tile_cells(x[k], y[k], h[k])
        done = []
        for c in cells:
            if len(done) >= tries:
                break
            if any(max(abs(c[0] - d[0]), abs(c[1] - d[1])) < 2 for d in done):
                continue
            done.append(c)
            w = window_verdict(x[k], y[k], h[k], (c[0] + 0.5) * WINDOW_M, (c[1] + 0.5) * WINDOW_M)
            w['t'] = t
            wins.append(w)
            v = _clean(w)
            if v:
                tv.setdefault(t, set()).add(v)
                break
    # tiles without a usable window: the overlap with a neighbour FILE is a measurement too — the same
    # trees, same height, at offset 0 or at SHIFT (what rebin_added reports as a `dup_shift` pair)
    miss = [t for t in range(len(T)) if t not in tv]
    others = sorted({str(q[1] if str(q[0]).zfill(5) == str(kg).zfill(5) else q[0]).zfill(5) for q in _rebin_pairs()
                     if str(kg).zfill(5) in (str(q[0]).zfill(5), str(q[1]).zfill(5))}) if miss else []
    for ok in others:
        try:
            b = _raw_apices(ok, raw=False)
        except Exception:  # noqa: BLE001
            b = None
        if b is None:
            continue
        from scipy.spatial import cKDTree
        tr = cKDTree(np.c_[b[0], b[1]])
        hit = []
        for off in ((0, 0), SHIFT):
            d, i = tr.query(np.c_[x + off[0], y + off[1]], distance_upper_bound=1.5)
            f = np.isfinite(d)
            f[f] = np.abs(b[2][i[f]] - h[f]) <= 0.25
            hit.append(f & (h >= 3.0))
        for t in miss:
            nz_, ns_ = int((hit[0] & (own == t)).sum()), int((hit[1] & (own == t)).sum())
            v = 'shift' if ns_ >= PAIR_MIN and ns_ >= 5 * max(nz_, 1) else 'zero' if nz_ >= PAIR_MIN and nz_ >= 5 * max(ns_, 1) else None
            if ns_ + nz_:
                wins.append({'t': t, 'v': v or 'unclear', 'src': 'pair', 'other': ok, 'n': ns_ + nz_, 'pair': [ns_, nz_]})
            if v:
                tv.setdefault(t, set()).add(v)
    flag = [(1 if tv[t] == {'shift'} else 0 if tv[t] == {'zero'} else -1) if t in tv else -1 for t in range(len(T))]
    ns = flag.count(1); nz = flag.count(0)
    if ns and nz:
        verdict = 'partial'
    elif ns:
        verdict = 'shift' if ns >= min(MIN_SHIFT_WINDOWS, len(T)) else 'partial'
    elif nz:
        verdict = 'zero'
    else:
        verdict = 'unclear'
    out = {'verdict': verdict, 'windows': wins, 'ts': now, 'tv': TILE_RULE, 'tiles_n': [ns, nz, len(T) - ns - nz]}
    if prev and prev.get('verdict'):
        out['was_verdict'] = prev['verdict']
    if verdict in ('shift', 'partial'):
        out['off'] = list(SHIFT)
    if verdict == 'partial':
        out['tiles'] = [[int(c) for c in T[t]] + [flag[t]] for t in range(len(T))]
    return out


def _rebin_pairs() -> list:
    """File pairs ``rebin_added`` found at one fixed offset beyond the twin reach — a position frame
    this module has not registered ([kg, other, dx, dy, matched, with_neighbour])."""
    try:
        import rebin_added
        con = rebin_added._conn()
        if con is None:
            return []
        r = con.execute("SELECT v FROM meta WHERE k='shifts'").fetchone()
        return json.loads(r[0]) if r and r[0] else []
    except Exception:  # noqa: BLE001
        return []


def tile_candidates(doc: dict | None = None) -> dict:
    """What the per-tile pass has to measure: ``todo`` = KGs named by a rebin shift pair first, then the
    ``mixed`` / ``unclear`` ones of the whole-KG pass; ``review`` = KGs of a pair that are served moved as
    a whole already (re-measuring those needs a second frame epoch — by hand)."""
    if doc is None:
        try:
            doc = json.loads(PATH.read_text())
        except Exception:  # noqa: BLE001
            doc = {}
    kgs = doc.get('kgs') or {}
    todo = []; review = []
    pairs = _rebin_pairs()
    for q in sorted(pairs, key=lambda q: -int(q[4])):
        for k in (str(q[0]).zfill(5), str(q[1]).zfill(5)):
            v = kgs.get(k) or {}
            if v.get('tv') == TILE_RULE:
                continue
            (review if v.get('verdict') in ('shift', 'retired', 'rejected') else todo).append(k)
    todo += [k for vd in ('mixed', 'unclear') for k, v in sorted(kgs.items()) if v.get('verdict') == vd and v.get('tv') != TILE_RULE]
    return {'todo': list(dict.fromkeys(todo)), 'review': sorted(set(review)), 'pairs': len(pairs)}


def verify_tiles(budget_mb_h: float = 150.0, max_kgs: int | None = None, only=None, quiet=False, dry=False) -> dict:
    """The per-tile pass over :func:`tile_candidates` — throttled on host RX like :func:`verify`,
    resumable (a measured KG carries ``tv``).  A KG that becomes ``shift`` / ``partial`` is served
    moved from the next read on; its user reports follow in the same step."""
    import psutil
    try:
        doc = json.loads(PATH.read_text())
    except Exception:  # noqa: BLE001
        doc = {}
    kgs = doc.get('kgs') or {}
    todo = [str(k).zfill(5) for k in only] if only else tile_candidates(doc)['todo']
    if max_kgs:
        todo = todo[:max_kgs]
    t0 = time.time(); done = 0; own = 0.0
    for kg in todo:
        prev = kgs.get(kg)
        if prev and prev.get('verdict') in ('shift', 'partial', 'retired', 'rejected'):
            if not quiet:
                print(kg, 'skipped: positions already moved once', prev.get('verdict'), flush=True)
            continue
        b0 = psutil.net_io_counters().bytes_recv
        try:
            r = verify_tiles_kg(kg, prev, read=not dry)
        except Exception as e:  # noqa: BLE001
            r = {'verdict': 'error', 'error': repr(e)[:200], 'windows': [], 'ts': int(time.time())}
        if dry:
            print(kg, r['verdict'], r.get('tiles_n'), flush=True)
            continue
        if r['verdict'] in ('shift', 'partial'):
            try:
                r['migrated'] = _migrate_stores(kg, r['off'], sel=_tiles_sel(r.get('tiles')))
            except Exception as e:  # noqa: BLE001 — do not serve a KG whose reports could not follow
                r = dict(r, verdict='error', error='migrate: ' + repr(e)[:160]); r.pop('off', None); r.pop('tiles', None)
        mb = (psutil.net_io_counters().bytes_recv - b0) / 1e6

        def put(doc, kg=kg, r=r, mb=mb):
            if r['verdict'] != 'error':
                doc['kgs'][kg] = r
            doc['mb'] = float(doc.get('mb') or 0) + mb
        _update(put)
        if PATH.parent == DATA:
            try:
                _flag(kg, r)
            except Exception as e:  # noqa: BLE001
                print('auto-shift flag', kg, e, file=sys.stderr)
        done += 1
        if not quiet:
            print(kg, r['verdict'], r.get('tiles_n'), f'{mb:.1f} MB', r.get('error', ''), flush=True)
        if r['verdict'] in ('shift', 'partial') and PATH.parent == DATA:
            try:    # linear-noise runs are keyed on apex positions → recompute this KG now
                import ne_linear
                ne_linear.process_kg(kg)
            except Exception as e:  # noqa: BLE001
                print('ne_linear', kg, e, file=sys.stderr)
        own += mb               # RX during our own reads only: idle host traffic must not stall the pass
        wait = own / budget_mb_h * 3600 - (time.time() - t0)
        if wait > 0 and kg != todo[-1]:
            time.sleep(min(wait, 600))
    return {'done': done, **status()}


def _migrate_stores(kg, off, db=None, stored: bool = True, sel=None) -> dict:
    """Everything users told us about a tree is keyed on the apex position
    (stored frame) — move it with the apices so nothing is dropped:
    ``tree_ver`` (species / measures / state / notes; NOT trees a user added —
    those are their own real-world points), tree-layer flags that name an apex
    (spurious, wrong height …; NOT ``missing``) and "same tree" crown flags
    (apex-keyed ``skey`` + ``expected.of``).  Idempotent per KG via the
    caller's state.  Reports arriving later from copies made before the fix
    are handled at write time (tree_user.reframe).  *sel* (partly shifted KG):
    only rows whose STORED position it selects move."""
    import sqlite3
    out = {'tree_ver': 0, 'flags': 0, 'crown': 0}
    if db is None and PATH.parent != DATA:      # test / dry-run state file → never touch the live store
        return {'tree_ver': 0, 'dry': True}
    p = Path(db) if db else DATA / 'public_flags.sqlite'
    if not p.exists():
        return out
    dx, dy = int(off[0]), int(off[1])
    c = sqlite3.connect(str(p), timeout=30, isolation_level=None)
    try:
        c.execute('BEGIN IMMEDIATE')
        tabs = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'tree_ver' in tabs:
            cols = {r[1] for r in c.execute('PRAGMA table_info(tree_ver)')}
            extra = ', e_dm = e_dm + ?, n_dm = n_dm + ?' if {'e_dm', 'n_dm'} <= cols else ''
            args = [dx, dy] + ([dx * 10, dy * 10] if extra else [])
            cond = ('kg = ? AND NOT EXISTS (SELECT 1 FROM tree_ver a WHERE a.kg = tree_ver.kg AND a.x = tree_ver.x '
                    'AND a.y = tree_ver.y AND a.op = 1)')
            if sel is None:
                out['tree_ver'] = c.execute(f'UPDATE tree_ver SET x = x + ?, y = y + ?{extra} WHERE {cond}', args + [int(kg)]).rowcount
            else:
                rs = c.execute(f'SELECT rowid, x, y FROM tree_ver WHERE {cond}', (int(kg),)).fetchall()
                if rs:
                    a = np.asarray(rs, np.int64)
                    k = sel(a[:, 1] + (0 if stored else dx), a[:, 2] + (0 if stored else dy))
                    c.executemany(f'UPDATE tree_ver SET x = x + ?, y = y + ?{extra} WHERE rowid = ?',
                                  [args + [int(r)] for r in a[k, 0].tolist()])
                    out['tree_ver'] = int(k.sum())
        if 'report' in tabs:
            import cells_api as ca
            import tree_crown as tc
            fwd = ca._tf('EPSG:4326', 'EPSG:3035'); inv = ca._tf('EPSG:3035', 'EPSG:4326')

            def mv(lo, la):
                x, y = fwd.transform(lo, la)
                lo2, la2 = inv.transform(x + dx, y + dy)
                return float(lo2), float(la2)
            did = {(k, v): i for i, k, v in c.execute("SELECT id, kind, val FROM dict WHERE kind IN ('layer', 'claim', 'source')")}
            lay = [i for (k, v), i in did.items() if k == 'layer' and v == 'trees']
            skip_claim = did.get(('claim', 'missing'), -1); auto = did.get(('source', 'auto-linear'), -1)
            # a flag belongs to this KG's move when it names one of ITS stored apices — never by the row's
            # `kg` (= KG of the cell; KG rectangles overlap, the apex may be served by the neighbour's file)
            ap = _raw_apices(kg) if lay else None
            if ap is not None and sel is not None:
                k = sel(ap[0], ap[1])
                ap = (ap[0][k], ap[1][k], ap[2][k]) if k.any() else None
            rows = []
            if ap is not None:
                # stored=False (unfix): the rows sit on the SHIFTED apices
                ax = np.round(ap[0]).astype(np.int64) - (0 if stored else dx)
                ay = np.round(ap[1]).astype(np.int64) - (0 if stored else dy)
                have = set(zip(ax.tolist(), ay.tolist()))
                w, s_ = inv.transform(float(ax.min()) - 30, float(ay.min()) - 30); e_, n_ = inv.transform(float(ax.max()) + 30, float(ay.max()) + 30)
                rows = c.execute('SELECT id, skey, lon_e6, lat_e6, expected, layer, claim FROM report WHERE lat_e6 BETWEEN ? AND ? '
                                 'AND lon_e6 BETWEEN ? AND ? AND (source IS NULL OR source != ?)',
                                 (int(s_ * 1e6) - 500, int(n_ * 1e6) + 500, int(w * 1e6) - 500, int(e_ * 1e6) + 500, auto)).fetchall()
            for rid, skey, lo, la, exp, layer, claim in rows:
                if layer not in lay or claim == skip_claim:
                    continue
                lo2, la2 = mv(lo / 1e6, la / 1e6)
                if not tc.is_apex_skey(skey):
                    px, py = fwd.transform(lo / 1e6, la / 1e6)
                    if float(np.min(np.hypot(ax - px, ay - py))) > FLAG_APEX_M:
                        continue
                if tc.is_apex_skey(skey):
                    x, y = tc.skey_xy(skey)
                    if (x, y) not in have:
                        continue
                    try:
                        e = json.loads(exp) if exp else None
                        for o in (e.get('of'), (e.get('spurious') or {}).get('of')) if isinstance(e, dict) else ():
                            if isinstance(o, dict) and 'x' in o and 'y' in o and (int(o['x']), int(o['y'])) in have:
                                o['x'] = int(o['x']) + dx; o['y'] = int(o['y']) + dy
                                if o.get('lon') is not None and o.get('lat') is not None:
                                    a, b = mv(float(o['lon']), float(o['lat'])); o['lon'], o['lat'] = round(a, 7), round(b, 7)
                                m = re.match(r'^t_(\d+)_(\d+)$', str(o.get('tree_id') or ''))
                                if m:
                                    o['tree_id'] = f't_{int(m.group(1)) + dx * 10}_{int(m.group(2)) + dy * 10}'
                        exp = json.dumps(e, separators=(',', ':'), ensure_ascii=False) if e is not None else exp
                    except Exception:  # noqa: BLE001 — unreadable hint: the key still follows
                        pass
                    c.execute('UPDATE OR IGNORE report SET skey = ?, lon_e6 = ?, lat_e6 = ?, expected = ? WHERE id = ?',
                              (tc.apex_skey(x + dx, y + dy), int(round(lo2 * 1e6)), int(round(la2 * 1e6)), exp, rid))
                    out['crown'] += 1
                else:
                    c.execute('UPDATE report SET lon_e6 = ?, lat_e6 = ? WHERE id = ?', (int(round(lo2 * 1e6)), int(round(la2 * 1e6)), rid))
                    out['flags'] += 1
        c.execute('COMMIT')
    except Exception:
        c.execute('ROLLBACK')
        raise
    finally:
        c.close()
    return out


def verify(budget_mb_h: float = 150.0, max_kgs: int | None = None, only=None, quiet=False) -> dict:
    import psutil
    try:
        kgs = json.loads(PATH.read_text()).get('kgs') or {}
    except Exception:  # noqa: BLE001
        kgs = {}
    todo = [k for k in (only or candidates()) if k not in kgs]
    if max_kgs:
        todo = todo[:max_kgs]
    t0 = time.time(); done = 0; own = 0.0
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
        mb = (psutil.net_io_counters().bytes_recv - b0) / 1e6

        def put(doc, kg=kg, r=r, mb=mb):
            if r['verdict'] != 'error':
                doc['kgs'][kg] = r
            doc['mb'] = float(doc.get('mb') or 0) + mb
        _update(put)
        if PATH.parent == DATA:
            try:
                _flag(kg, r)
            except Exception as e:  # noqa: BLE001
                print('auto-shift flag', kg, e, file=sys.stderr)
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
        own += mb               # RX during our own reads only: idle host traffic must not stall the pass
        wait = own / budget_mb_h * 3600 - (time.time() - t0)
        if wait > 0 and kg != todo[-1]:
            time.sleep(min(wait, 600))
    return {'done': done, **status()}


if __name__ == '__main__':
    if '--sync' in sys.argv:
        print(json.dumps(sync()))
    elif '--unfix' in sys.argv or '--retire' in sys.argv:
        f = unfix if '--unfix' in sys.argv else retire
        kg = sys.argv[sys.argv.index('--unfix' if f is unfix else '--retire') + 1]
        ok = f(kg)
        if ok:
            _flag(kg, json.loads(PATH.read_text())['kgs'][str(kg).zfill(5)])
        print(json.dumps({'kg': kg, 'ok': ok}))
    elif '--verify-tiles' in sys.argv:
        def _arg(n, d, t):
            return t(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d
        print(json.dumps(verify_tiles(_arg('--budget-mb-h', 150.0, float), _arg('--max-kgs', None, int),
                                      _arg('--kgs', None, lambda s: s.split(',')), dry='--dry' in sys.argv)))
    elif '--verify' in sys.argv:
        def _arg(n, d, t):
            return t(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d
        only = _arg('--kgs', None, lambda s: s.split(','))
        print(json.dumps(verify(_arg('--budget-mb-h', 150.0, float), _arg('--max-kgs', None, int), only)))
    else:
        print(json.dumps(status()))
