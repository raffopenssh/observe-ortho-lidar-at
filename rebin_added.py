"""rebin_added — no tree disappears because of the cell rule: apices ``ne_rebin`` stops serving from
their KG file ("off-file") come back as system-added trees, unless they are a TWIN of an apex the
served inventory already has.  Full story + the measured model: docs/tree-cell-assignment.md →
"Off-file apices → added trees".

Decision per off-file apex A (height h, served position p, source file S):

    qa         the static apex rule hides A in its own file (``tree_rule`` header marker / saturated)
    dup        TWIN rule ``tw1`` against every apex another KG file serves (one file per cell):
               Chebyshev offset D = max(|dx|, |dy|) m of the served positions, dh = |h - h_other|, and
               dh <= TWIN_DH[class of h][D]  (D <= 2)                       — height-aware, see TWIN_DH
               (where that tolerance is the full 2 m, a TALLER other apex is a twin at any dh)
    dup_frame  the two files sit in different position frames and the other apex is at exactly
               p - (frame_S - frame_other) with dh <= FRAME_DH: the same stored apex in two files
    dup_shift  the kept apices of a file pair have their equal-height partners at ONE
               offset beyond the twin reach: an unregistered position frame — :func:`_shift_twins`
    dup_added  A is the twin (same rule) of an already kept off-file apex of ANOTHER source file
               (kept = the first in (kg, r10, idx) order)
    add        everything else → a row in the added-tree store (``tree_user``, op add, system
               reporter, source ``auto-rebin``), reason ``no_row`` (target cell in no file) or
               ``other_file`` in the ``client_id``:  auto-rebin:<kg>:<rule>:<reason>:<r10 hex>:<idx>:<v>

LOST-CELL apices (rule ``lc1``): an apex whose served cell IS a row of its file, but the one-file-per-cell
rule (``cells_api.gather``) serves that cell from another KG file, is not on /trees/bbox either.  It is
decided by the very same twin rule against what the other files serve; reason ``lost_cell``.  Roughly 1.5 %
of all stored apices stand in such cells and ~94 % of them are twins, so only the kept ones are stored
(table ``lcand``); twins / QA-hidden ones are counted per KG (``kgs.ldup`` / ``lqa``).  That part has its
own token (``kgs.ltoken``), so it is scanned and refreshed independently of the off-file part.

Invariant per KG file:  stored == served − lost + added + dup + qa (+ capped)   (served = apices in rows
of the file, lost = those in cells another file owns; added / dup / qa over both parts).

The twin rule is a pure function of (h, offsets, heights) — :func:`twin`.  Every candidate keeps
its evidence (all apices of other files within NB_R m) in ``data/rebin_added.sqlite``, so a rule
change is re-decided (:func:`resolve`) without re-reading a product file.

    python3 rebin_added.py --scan [--budget-s N] [--kg K …] [--part off|lost]   # resumable; stale / unscanned KGs only
    python3 rebin_added.py --sync                              # resolve + write tree_user / flags (idempotent)
    python3 rebin_added.py --status
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path

import numpy as np

import ne_rebin
import tree_rule

log = logging.getLogger('rebin_added')

SOURCE = 'auto-rebin'
TWIN_ID = 'tw1'
# max |dh| (m) for a twin, per Chebyshev offset D = 0, 1, 2 m; rows = height class of the off-file apex
# (lower bound, m).  Derived by tools/rebin_twin_model.py: every (class, D, dh band) whose estimated
# share of real twins is >= 0.5 — chance pairs are frequent among low trees (dense, similar heights) and
# practically impossible next to a tall one (the detector never puts two apices that close).
TWIN_DH = ((0.0, (0.25, 0.0, 0.0)),
           (5.0, (2.0, 0.0, 0.0)),
           (10.0, (2.0, 0.5, 0.0)),
           (20.0, (2.0, 2.0, 2.0)))
TWIN_D = 2
TWIN_COVER = 2.0                # the largest tolerance: beyond it two inventories disagree about the spot — the
                                # apex is kept when it is the TALLER one (> 2 m), covered when the other is
FRAME_DH = 0.25                 # frame rule: identical stored position, height within one quantisation step
NB_R = 8                        # evidence kept per candidate: other files' apices within this Chebyshev distance (m)
RULE = f'{ne_rebin.REBIN_ID}.{TWIN_ID}'
LOST_ID = 'lc1'                 # lost-cell part (scan_lost_kg); its token = LOST_ID + RULE + file digests
DB_PATH = Path(__file__).resolve().parent / 'data' / 'rebin_added.sqlite'

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS cand (
        kg INTEGER NOT NULL, r10 INTEGER NOT NULL, idx INTEGER NOT NULL,     -- stored NET1 row of the source file
        cell INTEGER NOT NULL, x INTEGER NOT NULL, y INTEGER NOT NULL,       -- target res-12 cell, served EPSG:3035 m
        hq INTEGER NOT NULL, word INTEGER NOT NULL,                          -- height / 0.25 m, re-encoded NET1 word
        reason TEXT NOT NULL,                                                -- no_row | other_file
        okg INTEGER,                                                         -- file that serves the target cell
        v0 TEXT NOT NULL,                                                    -- vs served inventory: add | dup | dup_frame | qa
        tw_d INTEGER, tw_dh REAL, tw_kg INTEGER,                             -- the twin found (offset, |dh|, its file)
        verdict TEXT,                                                        -- final (resolve): v0 or dup_added
        nb BLOB, nbk TEXT,                                                   -- evidence: (dx i1, dy i1, hq u1, k u1) × n; k → nbk[k] = [kg, fdx, fdy]
        PRIMARY KEY (kg, r10, idx)) WITHOUT ROWID""",
    """CREATE TABLE IF NOT EXISTS kgs (
        kg INTEGER PRIMARY KEY, token TEXT, deps TEXT, stored INTEGER, served INTEGER, off INTEGER, capped INTEGER,
        outside INTEGER, added INTEGER, no_row INTEGER, other_file INTEGER, dup INTEGER, qa INTEGER,
        ok INTEGER, ts INTEGER, err TEXT, synced TEXT)""",
    'CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)',
)
# lost-cell part: kept apices only, same columns as cand (r10 / idx = NET1 row of the chunk AS SERVED)
_KGS_ADD = (('ltoken', 'TEXT'), ('ldeps', 'TEXT'), ('lost', 'INTEGER'), ('ldup', 'INTEGER'), ('lqa', 'INTEGER'), ('lost_cell', 'INTEGER'))
TABLES = ('cand', 'lcand')


def _conn(write: bool = False):
    if not write:
        if not DB_PATH.exists():
            return None
        c = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True, timeout=10)
        if 'ltoken' not in {r[1] for r in c.execute('PRAGMA table_info(kgs)')}:      # store from before the lost-cell part
            c.close()
            _conn(write=True).close()
            c = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True, timeout=10)
    else:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        c = sqlite3.connect(str(DB_PATH), timeout=60)
        c.execute('PRAGMA journal_mode=WAL')
        for s in _SCHEMA:
            c.execute(s)
        c.execute(_SCHEMA[0].replace('EXISTS cand', 'EXISTS lcand'))
        have = {r[1] for r in c.execute('PRAGMA table_info(kgs)')}
        for k, t in _KGS_ADD:
            if k not in have:
                c.execute(f'ALTER TABLE kgs ADD COLUMN {k} {t}')
    c.row_factory = sqlite3.Row
    return c


# === SECTION: the twin rule (pure) ===

def _tol(h) -> np.ndarray:
    """(n, 3) max |dh| per offset D = 0..2 for apex heights *h*."""
    lo = np.array([r[0] for r in TWIN_DH]); tab = np.array([r[1] for r in TWIN_DH])
    return tab[np.clip(np.searchsorted(lo, np.asarray(h, np.float64), side='right') - 1, 0, len(lo) - 1)]


def twin(h: float, dx, dy, nh, fdx=None, fdy=None):
    """Is an apex of height *h* a twin of one of the apices at offsets (*dx*, *dy*) m with heights
    *nh*?  *fdx* / *fdy* = (frame of the apex's file) − (frame of the neighbour's file), 0 when equal.
    → (index of the twin | -1, 'dup' | 'dup_frame' | None).  Nearest offset first, then smallest dh."""
    dx = np.asarray(dx, np.int64); dy = np.asarray(dy, np.int64); nh = np.asarray(nh, np.float64)
    if not dx.size:
        return -1, None
    d = np.maximum(np.abs(dx), np.abs(dy)); dh = np.abs(nh - float(h))
    t = _tol(h)[np.minimum(d, TWIN_D)]
    # at an offset where the tolerance is the full TWIN_COVER the detector resolves no second apex at all:
    # an at-least-as-tall apex of the other file there covers this one whatever the height difference
    m = (d <= TWIN_D) & ((dh <= t + 1e-9) | ((t >= TWIN_COVER) & (nh >= float(h))))
    if m.any():
        j = np.flatnonzero(m)
        return int(j[np.lexsort((dh[j], d[j]))[0]]), 'dup'
    if fdx is not None:
        fdx = np.asarray(fdx, np.int64); fdy = np.asarray(fdy, np.int64)
        m = ((fdx != 0) | (fdy != 0)) & (dx == -fdx) & (dy == -fdy) & (dh <= FRAME_DH + 1e-9)
        if m.any():
            j = np.flatnonzero(m)
            return int(j[np.argmin(dh[j])]), 'dup_frame'
    return -1, None


# === SECTION: scan (one KG file → candidates + evidence) ===

def _frame(kg) -> tuple[int, int]:
    import ne_pos_fix
    o = ne_pos_fix.offset(str(kg).zfill(5))
    return (int(o[0]), int(o[1])) if o else (0, 0)


def _file_tok(kg) -> str:
    import cells_api as ca
    import ne_pos_fix
    f = ca._files().get(str(kg).zfill(5))
    return (str(f.get('neo1_digest')) + ne_pos_fix.tag(kg)) if f else '-'


def _token(kg, deps) -> str:
    return RULE + '|' + _file_tok(kg) + '|' + ','.join(f'{k}:{_file_tok(k)}' for k in sorted(deps))


def _ltoken(kg, deps) -> str:
    import cells_api as ca
    return f'{LOST_ID}.{ca._green_fix.FIX_ID}.' + _token(kg, deps)


def evidence(kg: str, off: dict, tight: bool = False) -> dict:
    """What the other KG files hold around the off-file apices of *kg*: per apex the file that serves
    its target cell (``okg``, -1 = the cell is in no file) and every apex another file serves within
    ``NB_R`` m (``ni`` = apex index, ``ndx`` / ``ndy`` / ``nh`` / ``nk`` = offset, height, file)."""
    import cells_api as ca
    import ne_trees
    h3 = ne_rebin._h3n()
    n = int(off['idx'].size)
    out = {'okg': np.full(n, -1, np.int64), 'ni': np.zeros(0, np.int64), 'ndx': np.zeros(0, np.int64),
           'ndy': np.zeros(0, np.int64), 'nh': np.zeros(0, np.float64), 'nk': np.zeros(0, np.int64), 'deps': set()}
    if not n:
        return out
    need: set = set()
    if tight:                                       # res-12 disk 2 reaches > NB_R m beyond the cell: only those chunks
        for c in np.unique(off['cell']).tolist():
            need.update(int(h3.cell_to_parent(int(q), 10)) for q in h3.grid_disk(int(c), 2))
    else:
        par = np.fromiter((h3.cell_to_parent(int(c), 10) for c in off['cell']), np.uint64, count=n)
        for p in np.unique(par).tolist():
            need.update(int(c) for c in h3.grid_disk(int(p), 1))
    by: dict = {}
    for r in ca._chunk_rows(np.array(sorted(need), np.uint64)):
        by.setdefault(int(r['r10']), []).append(r)
    X = []; Y = []; H = []; K = []; hdrs: dict = {}
    cells = off['cell'].astype(np.uint64)
    for r10 in sorted(by):
        rs = by[r10]
        for r in sorted(rs, key=lambda q: q['kg']):
            if r['kg'] == kg:
                continue
            hd = hdrs.get(r['kg']) or hdrs.setdefault(r['kg'], ca.header(r['kg']))
            if hd is None:
                continue
            out['deps'].add(r['kg'])
            ch = ca._load_chunk(r['kg'], r, hd, qa=False)
            ids = ch.ids.astype(np.uint64)
            if len(rs) > 1:
                lost = ca.lost_cells(r['kg'], rs)
                if lost is not None:
                    ids = ids[~np.isin(ids, lost)]
            hit = np.isin(cells, ids) & (out['okg'] < 0)
            out['okg'][hit] = int(r['kg'])
            d = ne_trees._decoded_chunk(r['kg'], r, hd, 'trees')
            if not d or not d['n']:
                continue
            m = ne_trees._own_mask(d, r['kg'], r10, rs) if len(rs) > 1 else None
            sel = slice(None) if m is None else m
            X.append(d['x'][sel]); Y.append(d['y'][sel]); H.append(d['h'][sel]); K.append(np.full(len(d['x'][sel]), int(r['kg'])))
    if X:
        from scipy.spatial import cKDTree
        X = np.concatenate(X).astype(np.int64); Y = np.concatenate(Y).astype(np.int64)
        H = np.concatenate(H).astype(np.float64); K = np.concatenate(K)
        o = np.lexsort((H, Y, X, K))                     # deterministic neighbour order
        X, Y, H, K = X[o], Y[o], H[o], K[o]
        t = cKDTree(np.c_[X, Y].astype(np.float64))
        P = np.c_[off['x'], off['y']].astype(np.float64)
        nb = t.query_ball_point(P, NB_R + 0.5, p=np.inf)
        ii = np.repeat(np.arange(n), [len(v) for v in nb]); jj = np.fromiter((j for v in nb for j in sorted(v)), np.int64)
        out.update(ni=ii, ndx=X[jj] - off['x'][ii], ndy=Y[jj] - off['y'][ii], nh=H[jj], nk=K[jj])
    return out


def _qa_hidden(kg: str, off: dict) -> np.ndarray:
    """Off-file apices the static apex rule hides in their own file (never added back)."""
    import cells_api as ca
    hdr = ca.header(kg, with_chunks=True)
    bad = off['h_m'] >= tree_rule.H_SATURATED
    for r in np.unique(off['r10']).tolist():
        hr = tree_rule.hidden_rows(hdr, int(r))
        if hr is not None and len(hr):
            m = off['r10'] == r
            bad[m] |= np.isin(off['idx'][m], hr)
    return bad


def decide(kg: str, off: dict, ev: dict) -> list[dict]:
    """Verdict of every off-file apex against the served inventory (``v0``)."""
    n = int(off['idx'].size)
    fs = _frame(kg)
    fr = {int(k): _frame(k) for k in np.unique(ev['nk']).tolist()}
    qa = (off['qa'] if 'qa' in off else _qa_hidden(kg, off)) if n else np.zeros(0, bool)
    reason = off.get('reason')
    cut = np.searchsorted(ev['ni'], np.arange(n + 1))
    rows = []
    for i in range(n):
        a, b = int(cut[i]), int(cut[i + 1])
        dx = ev['ndx'][a:b]; dy = ev['ndy'][a:b]; nh = ev['nh'][a:b]; nk = ev['nk'][a:b]
        ks = sorted(set(nk.tolist()))
        fdx = np.array([fs[0] - fr[int(k)][0] for k in nk], np.int64); fdy = np.array([fs[1] - fr[int(k)][1] for k in nk], np.int64)
        h = float(off['h_m'][i])
        j, kind = twin(h, dx, dy, nh, fdx, fdy)
        v0 = 'qa' if qa[i] else (kind or 'add')
        blob = np.zeros(b - a, np.dtype([('dx', 'i1'), ('dy', 'i1'), ('hq', 'u1'), ('k', 'u1')]))
        blob['dx'] = dx; blob['dy'] = dy; blob['hq'] = np.round(nh * 4); blob['k'] = [ks.index(int(k)) for k in nk]
        rows.append(dict(kg=int(kg), r10=int(off['r10'][i]), idx=int(off['idx'][i]), cell=int(off['cell'][i]),
                         x=int(off['x'][i]), y=int(off['y'][i]), hq=int(round(h * 4)), word=int(off['word'][i]),
                         reason=reason or ('other_file' if ev['okg'][i] >= 0 else 'no_row'),
                         okg=int(ev['okg'][i]) if ev['okg'][i] >= 0 else None, v0=v0,
                         tw_d=int(max(abs(dx[j]), abs(dy[j]))) if j >= 0 else None,
                         tw_dh=float(abs(nh[j] - h)) if j >= 0 else None, tw_kg=int(nk[j]) if j >= 0 else None,
                         nb=blob.tobytes(), nbk=json.dumps([[k, fs[0] - fr[k][0], fs[1] - fr[k][1]] for k in ks], separators=(',', ':'))))
    return rows


def _i64(v: int) -> int:
    v = int(v)
    return v - (1 << 64) if v >= (1 << 63) else v


def scan_kg(kg: str, con=None) -> dict:
    """One KG file: ``ne_rebin.scan`` (the check + every off-file apex, ONE pass) → evidence →
    verdicts → store.  Replaces the KG's rows (a re-upload / rule bump recomputes, never duplicates)."""
    kg = str(kg).zfill(5)
    con = con or _conn(write=True)
    st, off = ne_rebin.scan(kg)
    ev = evidence(kg, off)
    rows = decide(kg, off, ev)
    tok = _token(kg, ev['deps'])
    with con:
        con.execute('DELETE FROM cand WHERE kg=?', (int(kg),))
        con.executemany('INSERT INTO cand(kg, r10, idx, cell, x, y, hq, word, reason, okg, v0, tw_d, tw_dh, tw_kg, nb, nbk) '
                        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        [(r['kg'], _i64(r['r10']), r['idx'], _i64(r['cell']), r['x'], r['y'], r['hq'], r['word'], r['reason'], r['okg'],
                          r['v0'], r['tw_d'], r['tw_dh'], r['tw_kg'], r['nb'], r['nbk']) for r in rows])
        con.execute('INSERT INTO kgs(kg, token, deps, stored, served, off, capped, outside, ts, err) VALUES (?,?,?,?,?,?,?,?,?,NULL) '
                    'ON CONFLICT(kg) DO UPDATE SET token=excluded.token, deps=excluded.deps, stored=excluded.stored, served=excluded.served, '
                    'off=excluded.off, capped=excluded.capped, outside=excluded.outside, ts=excluded.ts, err=NULL',
                    (int(kg), tok, json.dumps(sorted(ev['deps'])), st['stored'], st['apices'], st['off_file'], st['capped'],
                     st['outside'], int(time.time())))
        con.execute("INSERT OR REPLACE INTO meta VALUES ('resolved', '0')")
    return dict(st, candidates=len(rows))


# === SECTION: lost-cell part (apices in cells another KG file serves) ===

def lost_apices(kg: str) -> tuple[dict, dict]:
    """Apices of *kg* as served by its file (after rb1) whose cell the one-file-per-cell rule gives to
    another KG file → ``(stats, off)``; *off* has the columns of ``ne_rebin.scan`` (``r10`` / ``idx`` =
    NET1 row of the chunk as served) + ``qa`` (static apex rule) and ``reason`` = ``lost_cell``.
    Only chunks stored by more than one file are read."""
    import cells_api as ca
    import ne_trees
    kg = str(kg).zfill(5)
    hdr = ca.header(kg, with_chunks=True)
    mine = {int(c['r10'], 16): c for c in hdr['chunks'] if int(c.get('trees') or 0)}
    by: dict = {}
    if mine:
        for r in ca._chunk_rows(np.array(sorted(mine), np.uint64)):
            by.setdefault(int(r['r10']), []).append(r)
    cols = {k: [] for k in ('r10', 'idx', 'cell', 'word', 'x', 'y', 'h_m', 'qa')}
    st = {'kg': kg, 'chunks': 0, 'lost_cells': 0}
    for r10 in sorted(by):
        rs = by[r10]
        if len(rs) < 2:
            continue
        row = next((r for r in rs if r['kg'] == kg), None)
        if row is None:
            continue
        st['chunks'] += 1
        lost = ca.lost_cells(kg, rs)
        if lost is None or not len(lost):
            continue
        st['lost_cells'] += int(len(lost))
        d = ne_trees._decoded_chunk(kg, row, hdr, 'trees')
        if not d or not d['n']:
            continue
        m = np.isin(d['cell'], lost)
        if not m.any():
            continue
        ch = ca._load_chunk(kg, row, hdr, qa=False)
        i = np.flatnonzero(m)
        cols['r10'].append(np.full(i.size, r10, np.uint64)); cols['idx'].append(i.astype(np.int64))
        cols['cell'].append(d['cell'][i]); cols['word'].append(np.asarray(ch.trees)[i].astype(np.uint32))
        cols['x'].append(d['x'][i].astype(np.int64)); cols['y'].append(d['y'][i].astype(np.int64))
        cols['h_m'].append(d['h'][i].astype(np.float64)); cols['qa'].append(~np.asarray(d['ok'], bool)[i])
    dt = {'r10': np.uint64, 'idx': np.int64, 'cell': np.uint64, 'word': np.uint32, 'x': np.int64, 'y': np.int64, 'h_m': np.float64, 'qa': bool}
    off = {k: (np.concatenate(v) if v else np.zeros(0, dt[k])) for k, v in cols.items()}
    off['reason'] = 'lost_cell'
    st['lost'] = int(off['idx'].size)
    return st, off


def scan_lost_kg(kg: str, con=None) -> dict:
    """Lost-cell part of one KG: verdicts by the same twin rule; kept apices → ``lcand`` (with their
    evidence), twins / QA-hidden ones → counts in ``kgs``.  Replaces the KG's rows."""
    kg = str(kg).zfill(5)
    con = con or _conn(write=True)
    st, off = lost_apices(kg)
    ev = evidence(kg, off, tight=True)
    rows = decide(kg, off, ev)
    keep = [r for r in rows if r['v0'] == 'add']
    ndup = sum(1 for r in rows if r['v0'].startswith('dup')); nqa = sum(1 for r in rows if r['v0'] == 'qa')
    tok = _ltoken(kg, ev['deps'])
    with con:
        con.execute('DELETE FROM lcand WHERE kg=?', (int(kg),))
        con.executemany('INSERT INTO lcand(kg, r10, idx, cell, x, y, hq, word, reason, okg, v0, tw_d, tw_dh, tw_kg, nb, nbk) '
                        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        [(r['kg'], _i64(r['r10']), r['idx'], _i64(r['cell']), r['x'], r['y'], r['hq'], r['word'], r['reason'], r['okg'],
                          r['v0'], r['tw_d'], r['tw_dh'], r['tw_kg'], r['nb'], r['nbk']) for r in keep])
        n = con.execute('UPDATE kgs SET ltoken=?, ldeps=?, lost=?, ldup=?, lqa=? WHERE kg=? AND token IS NOT NULL',
                        (tok, json.dumps(sorted(ev['deps'])), st['lost'], ndup, nqa, int(kg))).rowcount
        if not n:
            raise RuntimeError(f'{kg}: off-file part not scanned')
        con.execute("INSERT OR REPLACE INTO meta VALUES ('resolved', '0')")
    return dict(st, kept=len(keep), dup=ndup, qa=nqa)


def stale_lost(con=None) -> list[str]:
    """KG files whose lost-cell part is missing or out of date (own file / rule / a compared file changed)."""
    con = con or _conn(write=True)
    return [f'{r["kg"]:05d}' for r in con.execute('SELECT kg, ltoken, ldeps FROM kgs WHERE token IS NOT NULL ORDER BY kg').fetchall()
            if r['ltoken'] is None or r['ltoken'] != _ltoken(f'{r["kg"]:05d}', json.loads(r['ldeps'] or '[]'))]


def stale_kgs(con=None) -> list[str]:
    """KG files to (re)scan: never scanned, own file / frame / rule changed, a file they were
    compared with changed, or a changed / new file whose envelope they touch."""
    import cells_api as ca
    con = con or _conn(write=True)
    files = sorted(k for k in ca._files() if len(k) == 5 and k.isdigit())
    have = {f'{r["kg"]:05d}': r for r in con.execute('SELECT kg, token, deps, err FROM kgs')}
    out = []; changed = []
    for kg in files:
        r = have.get(kg)
        if r is None or r['token'] is None:
            out.append(kg); changed.append(kg); continue
        deps = json.loads(r['deps'] or '[]')
        if r['token'] != _token(kg, deps):
            out.append(kg)
            if r['token'].split('|')[:2] != [RULE, _file_tok(kg)]:
                changed.append(kg)
    if have and len(changed) < 500:                 # not the initial run: neighbours of changed files
        seen = set(out)
        for kg in changed:
            bb = ca._kg_bbox(kg)
            for k in (ca._kgs_intersecting(*bb) if bb else ()):
                if k in have and k not in seen:
                    seen.add(k); out.append(k)
    gone = [k for k in have if k not in set(files)]
    if gone:
        with con:
            for k in gone:
                con.execute('DELETE FROM cand WHERE kg=?', (int(k),)); con.execute('DELETE FROM lcand WHERE kg=?', (int(k),))
                con.execute('DELETE FROM kgs WHERE kg=?', (int(k),))
            con.execute("INSERT OR REPLACE INTO meta VALUES ('resolved', '0')")
    return out


def scan(budget_s: float | None = None, kgs=None, verbose: bool = True, part: str = 'all') -> dict:
    """Both parts, resumable: the off-file part of every stale KG first, then the lost-cell part
    (*part* = ``off`` | ``lost`` | ``all``)."""
    con = _conn(write=True)
    ks = [str(k).zfill(5) for k in kgs] if kgs else None
    todo = [] if part == 'lost' else [(k, 'off') for k in (ks or stale_kgs(con))]
    if part != 'off':
        todo += [(k, 'lost') for k in (ks or sorted(set(stale_lost(con)) | {k for k, _ in todo}))]
    t0 = time.time(); done = err = 0
    for kg, what in todo:
        if budget_s and time.time() - t0 > budget_s:
            break
        try:
            (scan_kg if what == 'off' else scan_lost_kg)(kg, con); done += 1
        except Exception as e:  # noqa: BLE001
            err += 1
            log.warning('rebin_added: scan %s %s: %s', what, kg, e)
            with con:
                if what == 'lost':
                    con.execute('UPDATE kgs SET ltoken=NULL, err=?, ts=? WHERE kg=?', (('lost: ' + str(e))[:200], int(time.time()), int(kg)))
                    continue
                con.execute('INSERT INTO kgs(kg, err, ts) VALUES (?,?,?) ON CONFLICT(kg) DO UPDATE SET err=excluded.err, ts=excluded.ts, token=NULL',
                            (int(kg), str(e)[:200], int(time.time())))
        if verbose and (done + err) % 100 == 0:
            print(f'  {done + err}/{len(todo)} KGs, {err} errors, {time.time() - t0:.0f}s', flush=True)
    return {'todo': len(todo), 'scanned': done, 'errors': err, 'left': len(todo) - done - err, 'seconds': round(time.time() - t0, 1)}


# === SECTION: resolve (national, pure function of the candidate table) ===

def resolve(con=None) -> dict:
    """Final verdicts: ``v0`` re-decided from the stored evidence under the current rule constants,
    then twins among the kept apices of DIFFERENT source files (the same tree off-file in two
    files) — the first in (kg, r10, idx) order stays, the others are ``dup_added``.  Deterministic."""
    con = con or _conn(write=True)
    rows = con.execute("SELECT 'cand' AS tb, kg, r10, idx, x, y, hq, v0, nb, nbk FROM cand UNION ALL "
                       "SELECT 'lcand', kg, r10, idx, x, y, hq, v0, nb, nbk FROM lcand ORDER BY kg, tb, r10, idx").fetchall()
    n = len(rows)
    dt = np.dtype([('dx', 'i1'), ('dy', 'i1'), ('hq', 'u1'), ('k', 'u1')])
    v = []; upd0 = []
    for r in rows:
        v0 = r['v0']
        if v0 != 'qa':                                   # re-decide from the evidence (rule constants may have changed)
            b = np.frombuffer(r['nb'] or b'', dt); ks = json.loads(r['nbk'] or '[]')
            if b.size:
                fd = np.array([[ks[k][1], ks[k][2]] for k in b['k']], np.int64).reshape(-1, 2)
                j, kind = twin(r['hq'] / 4.0, b['dx'], b['dy'], b['hq'] / 4.0, fd[:, 0], fd[:, 1])
            else:
                j, kind = -1, None
            nv = kind or 'add'
            if nv != v0:
                upd0.append((nv, int(max(abs(int(b['dx'][j])), abs(int(b['dy'][j])))) if j >= 0 else None,
                             abs(b['hq'][j] / 4.0 - r['hq'] / 4.0) if j >= 0 else None, ks[b['k'][j]][0] if j >= 0 else None,
                             r['kg'], r['r10'], r['idx'], r['tb']))
                v0 = nv
        v.append(v0)
    shifts = _shift_twins(rows, v, dt)
    kept = np.array([x == 'add' for x in v], bool)
    if kept.any():
        from scipy.spatial import cKDTree
        ki = np.flatnonzero(kept)
        X = np.array([rows[i]['x'] for i in ki], np.int64); Y = np.array([rows[i]['y'] for i in ki], np.int64)
        H = np.array([rows[i]['hq'] for i in ki], np.float64) / 4.0; K = np.array([rows[i]['kg'] for i in ki], np.int64)
        fr = {int(k): _frame(k) for k in np.unique(K).tolist()}
        pairs = cKDTree(np.c_[X, Y].astype(np.float64)).query_pairs(NB_R + 0.5, p=np.inf, output_type='ndarray')
        part: dict = {}
        for a, b in pairs.tolist():                      # a < b = (kg, r10, idx) order
            if K[a] == K[b]:
                continue
            fa, fb = fr[int(K[a])], fr[int(K[b])]
            t1 = twin(H[b], [X[a] - X[b]], [Y[a] - Y[b]], [H[a]], [fb[0] - fa[0]], [fb[1] - fa[1]])[0]
            t2 = twin(H[a], [X[b] - X[a]], [Y[b] - Y[a]], [H[b]], [fa[0] - fb[0]], [fa[1] - fb[1]])[0]
            if t1 >= 0 or t2 >= 0:
                part.setdefault(b, []).append(a)
        alive = np.ones(ki.size, bool)
        for b in sorted(part):
            if any(alive[a] for a in part[b]):
                alive[b] = False; v[ki[b]] = 'dup_added'
    with con:
        for tb in TABLES:
            con.executemany(f'UPDATE {tb} SET v0=?, tw_d=?, tw_dh=?, tw_kg=? WHERE kg=? AND r10=? AND idx=?', [u[:7] for u in upd0 if u[7] == tb])
            con.executemany(f'UPDATE {tb} SET verdict=? WHERE kg=? AND r10=? AND idx=? AND verdict IS NOT ?',
                            [(x, r['kg'], r['r10'], r['idx'], x) for x, r in zip(v, rows) if r['tb'] == tb])
        # per KG over both parts; the lost-cell part stores kept apices only — its twins / QA are kgs.ldup / lqa
        con.execute("""UPDATE kgs SET
            lost_cell = (SELECT COUNT(*) FROM lcand c WHERE c.kg = kgs.kg AND c.verdict = 'add'),
            no_row = (SELECT COUNT(*) FROM cand c WHERE c.kg = kgs.kg AND c.verdict = 'add' AND c.reason = 'no_row'),
            other_file = (SELECT COUNT(*) FROM cand c WHERE c.kg = kgs.kg AND c.verdict = 'add' AND c.reason = 'other_file'),
            dup = (SELECT COUNT(*) FROM cand c WHERE c.kg = kgs.kg AND c.verdict LIKE 'dup%')
                  + (SELECT COUNT(*) FROM lcand c WHERE c.kg = kgs.kg AND c.verdict LIKE 'dup%') + COALESCE(ldup, 0),
            qa = (SELECT COUNT(*) FROM cand c WHERE c.kg = kgs.kg AND c.verdict = 'qa') + COALESCE(lqa, 0) WHERE token IS NOT NULL""")
        con.execute('UPDATE kgs SET added = no_row + other_file + lost_cell WHERE token IS NOT NULL')
        con.execute('UPDATE kgs SET ok = (outside = 0 AND stored = served - COALESCE(lost, 0) + added + dup + qa + capped) WHERE token IS NOT NULL')
        con.execute("INSERT OR REPLACE INTO meta VALUES ('shifts', ?)",
                    (json.dumps([[f'{a:05d}', f'{b:05d}', *t] for (a, b), t in sorted(shifts.items())], separators=(',', ':')),))
        con.execute("INSERT OR REPLACE INTO meta VALUES ('resolved', ?)", (RULE,))
        con.execute("INSERT OR REPLACE INTO meta VALUES ('resolved_ts', ?)", (str(int(time.time())),))
    return {'candidates': n, 'redecided': len(upd0), **totals(con)}


SHIFT_MIN_D = 3                 # a file-pair offset is looked for beyond the twin rule's reach …
SHIFT_JIT = 2                   # … apices agree with it within this (m, per axis; frame-KG jitter) and FRAME_DH in height
SHIFT_MIN_N = 20                # … and it must explain at least this many kept apices of the pair
SHIFT_MIN_SHARE = 0.3           # … and this share of the kept apices that have an apex of the other file nearby


def _shift_twins(rows, v: list, dt) -> dict:
    """Apices kept so far (both parts), per (source file, other file): when the equal-height neighbours
    pile up at ONE offset beyond the twin reach, the two files sit in different position frames that
    ``ne_pos_fix`` has not registered or only to the nearest raster step (a "5 m" KG) — the apices that have their equal-height partner at
    that offset are the same trees → ``dup_shift``.  Pure function of the stored evidence; mutates *v*.
    → {(kg, other kg): (dx, dy, matched, with_neighbour)} for the status line (ne_pos_fix to-do list)."""
    from collections import Counter, defaultdict
    grp = defaultdict(list)
    for i, r in enumerate(rows):
        if v[i] == 'add' and r['nb']:
            grp[r['kg']].append(i)
    out = {}
    for kg, ii in grp.items():
        if len(ii) < SHIFT_MIN_N:
            continue
        per = defaultdict(list)                          # other kg → [(row, dx[], dy[])] equal-height neighbours
        for i in ii:
            r = rows[i]; b = np.frombuffer(r['nb'], dt); ks = json.loads(r['nbk'] or '[]')
            m = (np.abs(b['hq'].astype(np.int64) - r['hq']) <= round(FRAME_DH * 4)) & (np.maximum(np.abs(b['dx']), np.abs(b['dy'])) >= SHIFT_MIN_D)
            for k in set(b['k'].tolist()):
                mk = m & (b['k'] == k)
                per[ks[k][0]].append((i, b['dx'][mk].astype(np.int64), b['dy'][mk].astype(np.int64)))
        for ok, lst in sorted(per.items()):
            if len(lst) < SHIFT_MIN_N:
                continue
            cnt = Counter((int(a), int(b_)) for _, dx, dy in lst for a, b_ in zip(dx, dy))
            if not cnt:
                continue
            (mx, my), _ = min(cnt.items(), key=lambda kv: (-kv[1], kv[0]))
            hit = [i for i, dx, dy in lst if ((np.abs(dx - mx) <= SHIFT_JIT) & (np.abs(dy - my) <= SHIFT_JIT)).any()]
            if len(hit) >= SHIFT_MIN_N and len(hit) >= SHIFT_MIN_SHARE * len(lst):
                for i in hit:
                    v[i] = 'dup_shift'
                out[(int(kg), int(ok))] = (mx, my, len(hit), len(lst))
    return out


def totals(con=None) -> dict:
    con = con or _conn()
    if con is None:
        return {}
    t: dict = {}; rs: dict = {}
    for tb in TABLES:
        for r in con.execute(f'SELECT verdict, COUNT(*) FROM {tb} GROUP BY 1'):
            t[r[0] or 'pending'] = t.get(r[0] or 'pending', 0) + r[1]
        for r in con.execute(f"SELECT reason, COUNT(*) FROM {tb} WHERE verdict='add' GROUP BY 1"):
            rs[r[0]] = rs.get(r[0], 0) + r[1]
    lk = con.execute('SELECT SUM(ltoken IS NOT NULL), SUM(lost), SUM(ldup), SUM(lqa), SUM(err IS NOT NULL AND token IS NOT NULL AND ltoken IS NULL) '
                     'FROM kgs WHERE token IS NOT NULL').fetchone()
    k = con.execute('SELECT COUNT(*), SUM(token IS NOT NULL), SUM(ok = 1), SUM(err IS NOT NULL AND token IS NULL), SUM(stored), SUM(served), '
                    'SUM(off), SUM(capped), SUM(added > 0), SUM(ok = 0) FROM kgs').fetchone()
    return {'rule': RULE, 'kgs': k[0], 'scanned': k[1] or 0, 'invariant_ok': k[2] or 0, 'invariant_bad': k[9] or 0, 'errors': k[3] or 0,
            'stored': k[4] or 0, 'served': k[5] or 0, 'off_file': k[6] or 0, 'capped': k[7] or 0, 'kgs_with_added': k[8] or 0,
            'added': t.get('add', 0), 'no_row': rs.get('no_row', 0), 'other_file': rs.get('other_file', 0),
            'dup': t.get('dup', 0), 'dup_frame': t.get('dup_frame', 0), 'dup_added': t.get('dup_added', 0), 'qa': t.get('qa', 0),
            'pending': t.get('pending', 0), 'dup_shift': t.get('dup_shift', 0),
            # lost-cell part: 'dup' / 'qa' above are stored rows; its twins / QA-hidden vs the served inventory are counts only
            'lost_scanned': lk[0] or 0, 'lost': lk[1] or 0, 'lost_cell': rs.get('lost_cell', 0), 'lost_dup': lk[2] or 0, 'lost_qa': lk[3] or 0,
            'lost_errors': lk[4] or 0}


def kg_counts(kg) -> dict | None:
    """Store view of one KG for ``ne_rebin.check`` (None = not scanned / stale rule)."""
    con = _conn()
    if con is None:
        return None
    r = con.execute('SELECT * FROM kgs WHERE kg=?', (int(kg),)).fetchone()
    if r is None or r['token'] is None or r['added'] is None or not r['token'].startswith(RULE + '|'):
        return None
    out = {k: r[k] for k in ('stored', 'served', 'off', 'capped', 'added', 'no_row', 'other_file', 'dup', 'qa', 'ok')}
    lost_ok = r['ltoken'] is not None and r['ltoken'].startswith(f'{LOST_ID}.')
    # the lost-cell part is only folded into added / dup / qa by resolve(): between a (re)scan and the next
    # resolve the per-KG counts are the off-file part alone, so `lost` must not enter the invariant yet
    m = con.execute("SELECT v FROM meta WHERE k='resolved'").fetchone()
    lost_ok = lost_ok and m is not None and m[0] == RULE
    out.update(lost=r['lost'] if lost_ok else None, lost_cell=r['lost_cell'] or 0, lost_dup=r['ldup'] or 0, lost_qa=r['lqa'] or 0)
    return out


# === SECTION: sync (kept apices → tree_user added trees + one flag per KG) ===

def _species(i) -> str | None:
    import cells_api as ca
    return ca.SPECIES[int(i)] if int(i) < len(ca.SPECIES) else None


def _vitality(i) -> str | None:
    import cells_api as ca
    return ca.VITALITY[int(i)] if int(i) < len(ca.VITALITY) else None


def cid_of(r) -> str:
    v = hashlib.sha1(f"{r['x']}|{r['y']}|{r['hq']}".encode()).hexdigest()[:4]
    return f"{SOURCE}:{int(r['kg']):05d}:{ne_rebin.REBIN_ID}:{r['reason']}:{int(r['r10']) & ((1 << 64) - 1):x}:{int(r['idx'])}:{v}"


def note_of(r) -> str:
    """Machine-readable record of the measured apex (the added-tree store has no columns for these)."""
    import cells_api as ca
    d = ca.decode_trees(np.array([r['word']], np.uint32))
    p = [f"{SOURCE} {RULE} {r['reason']} kg={int(r['kg']):05d}", f"h_m={r['hq'] / 4.0:g}", f"crown_m2={float(d['crown_m2'][0]):g}"]
    sp = _species(d['species'][0]); vi = _vitality(d['vitality'][0])
    if sp:
        p.append(f'class={sp}')
    if vi:
        p.append(f'vitality={vi}')
    if bool(d['ndsm_only'][0]):
        p.append('ndsm_only=1')
    return ' '.join(p)


def _store_kg(r) -> int:
    """KG the added tree is filed under = a file whose envelope holds the point, so every /trees/bbox
    that contains the point looks at it: the source file, else the file serving the cell, else any."""
    import cells_api as ca
    lon, lat = ne_rebin._to_wgs(float(r['x']), float(r['y']))
    near = set(ca._kgs_intersecting(lon, lat, lon, lat))
    src = f"{int(r['kg']):05d}"
    if src in near or not near:
        return int(r['kg'])
    if r['okg'] is not None and f"{int(r['okg']):05d}" in near:
        return int(r['okg'])
    return int(sorted(near)[0])


def _free_key(c, kg: int, x: int, y: int, taken: set) -> tuple[int, int]:
    for rr in range(0, 6):
        for dx in range(-rr, rr + 1):
            for dy in range(-rr, rr + 1):
                if max(abs(dx), abs(dy)) != rr or (dx, dy) in taken:
                    continue
                if not c.execute('SELECT 1 FROM tree_ver WHERE kg=? AND x=? AND y=? LIMIT 1', (kg, x + dx, y + dy)).fetchone():
                    return x + dx, y + dy
    raise RuntimeError(f'no free tree key near {x},{y} in {kg}')


def sync_kg(kg, con=None, dry: bool = False) -> dict:
    """Bring the added-tree store in line with the verdicts of one source KG: missing → op add,
    no longer wanted (rule bump, re-upload, verdict change) → op retract.  Append-only, idempotent."""
    import public_flags as pf
    import tree_user as tu
    con = con or _conn(write=True)
    kg = int(kg)
    want = {cid_of(r): r for tb in TABLES for r in con.execute(f"SELECT * FROM {tb} WHERE kg=? AND verdict='add' ORDER BY r10, idx", (kg,))}
    c = tu._conn()
    rep = tu.system_reporter(SOURCE)
    live: dict = {}                                     # cid → (kg, x, y) of the system's live adds
    for r in c.execute('SELECT cid, op, kg, x, y FROM tree_ver WHERE cid GLOB ? AND reporter=? ORDER BY rev', (f'{SOURCE}:{kg:05d}:*', rep)):
        if r['op'] == tu.OP_ADD:
            live[r['cid']] = (r['kg'], r['x'], r['y'])
        elif r['op'] == tu.OP_RETRACT:
            live.pop(r['cid'], None)
    add = [k for k in want if k not in live]; drop = [k for k in live if k not in want]
    out = {'kg': f'{kg:05d}', 'want': len(want), 'add': len(add), 'retract': len(drop)}
    if dry or not (add or drop):
        if not dry:
            _flag(con, kg)
        return out
    now = int(time.time())
    sid = pf._dict_id(c, 'source', SOURCE)
    ep = pf._epoch_now(f'{kg:05d}'); ep = pf._dict_id(c, 'epoch', ep) if ep else None
    dt = np.dtype([('dx', 'i1'), ('dy', 'i1'), ('hq', 'u1'), ('k', 'u1')])
    with pf._LOCK:
        c.execute('BEGIN IMMEDIATE')
        try:
            for k in drop:
                skg, x, y = live[k]
                c.execute('INSERT INTO tree_ver(ts, x, y, kg, op, reporter, source, cid) VALUES (?,?,?,?,?,?,?,?)',
                          (now, x, y, skg, tu.OP_RETRACT, rep, sid, k))
            for k in add:
                r = want[k]
                t = tu._tree_by_cid(c, k)
                if t is not None:
                    skg, x, y = t
                else:
                    skg = _store_kg(r)
                    b = np.frombuffer(r['nb'] or b'', dt)
                    x, y = _free_key(c, skg, r['x'], r['y'], {(int(a), int(b_)) for a, b_ in zip(b['dx'], b['dy']) if max(abs(int(a)), abs(int(b_))) <= 5})
                c.execute('INSERT INTO tree_ver(ts, x, y, kg, op, reporter, source, epoch, uh_dm, e_dm, n_dm, cid, note) '
                          'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                          (now, x, y, skg, tu.OP_ADD, rep, sid, ep, int(round(r['hq'] * 2.5)), r['x'] * 10, r['y'] * 10, k, note_of(r)))
            c.execute('COMMIT')
        except Exception:
            c.execute('ROLLBACK')
            raise
    _flag(con, kg)
    return out


def _flag(con, kg: int) -> None:
    """One public KG-level record (layer trees, claim missing, source auto-rebin) of what the cell
    rule took out of this file and what came back — written once a KG has an added tree."""
    import public_flags as pf
    r = con.execute('SELECT * FROM kgs WHERE kg=?', (kg,)).fetchone()
    if r is None or r['added'] is None:
        return
    code = f'{kg:05d}'
    if not r['added'] and code not in pf.auto_kg_status(SOURCE):
        return
    exp = {'hint': 'rebin', 'rule': RULE, 'added': r['added'], 'no_row': r['no_row'], 'other_file': r['other_file'],
           'dup': r['dup'], 'qa': r['qa']}
    obs = {'stored': r['stored'], 'served': r['served'], 'off_file': r['off'], 'ok': bool(r['ok'])}
    if r['ltoken'] is not None:                         # lost-cell part scanned: apices in cells another file serves
        exp['lost_cell'] = r['lost_cell']; obs['lost'] = r['lost']; obs['served'] = r['served'] - (r['lost'] or 0)
    pf.auto_kg(code, SOURCE, 'trees', 'missing', json.dumps(exp, separators=(',', ':')), json.dumps(obs, separators=(',', ':')))


def sync(verbose: bool = True, dry: bool = False) -> dict:
    """resolve() + sync_kg() for every KG whose verdicts changed since its last sync.  Refuses while
    KGs are unscanned (twins between two source files need both) unless the store was synced before."""
    con = _conn(write=True)
    res = resolve(con)
    if res['scanned'] < res['kgs'] or res['errors']:
        log.warning('rebin_added: %d/%d KGs scanned, %d errors — syncing what is decided', res['scanned'], res['kgs'], res['errors'])
    out = {'kgs': 0, 'add': 0, 'retract': 0}
    for r in con.execute('SELECT kg, synced FROM kgs WHERE token IS NOT NULL ORDER BY kg').fetchall():
        sig = hashlib.sha1('\n'.join(cid_of(q) for tb in TABLES for q in con.execute(
            f"SELECT kg, r10, idx, x, y, hq, reason FROM {tb} WHERE kg=? AND verdict='add' ORDER BY r10, idx", (r['kg'],))).encode()).hexdigest()[:16]
        cnt = con.execute('SELECT added, dup, qa, ok FROM kgs WHERE kg=?', (r['kg'],)).fetchone()
        sig += f".{cnt['added']}.{cnt['dup']}.{cnt['qa']}.{cnt['ok']}"
        if r['synced'] == sig:
            continue
        s = sync_kg(r['kg'], con, dry=dry)
        out['kgs'] += 1; out['add'] += s['add']; out['retract'] += s['retract']
        if not dry:
            with con:
                con.execute('UPDATE kgs SET synced=? WHERE kg=?', (sig, r['kg']))
    if not dry:
        with con:
            con.execute("INSERT OR REPLACE INTO meta VALUES ('synced_ts', ?)", (str(int(time.time())),))
    return {**res, 'sync': out}


# === SECTION: status ===

def status() -> dict:
    con = _conn()
    if con is None:
        return {'rule': RULE, 'kgs': 0}
    s = totals(con)
    m = dict(con.execute('SELECT k, v FROM meta').fetchall())
    try:                                             # denominator = KG files of the corpus, not rows scanned so far
        import cells_api as ca
        s['files'] = sum(1 for k in ca._files() if len(k) == 5 and k.isdigit())
    except Exception:  # noqa: BLE001
        s['files'] = s['kgs']
    s['complete'] = s['scanned'] >= s['files'] and not s['errors'] and s['lost_scanned'] >= s['files'] and not s['lost_errors']
    sh = json.loads(m.get('shifts') or '[]')
    s['shift_pairs'] = len(sh); s['shifts'] = sorted(sh, key=lambda q: -q[4])[:20]
    s.update(resolved=m.get('resolved') == RULE, synced_ts=int(m.get('synced_ts') or 0),
             unsynced=con.execute("SELECT COUNT(*) FROM kgs WHERE token IS NOT NULL AND synced IS NULL").fetchone()[0])
    return s


def text_line() -> str:
    s = status()
    if not s.get('kgs'):
        return f'rebin: {RULE} — no scan yet (python3 rebin_added.py --scan)'
    ts = time.strftime('%Y-%m-%dT%H:%MZ', time.gmtime(s['synced_ts'])) if s.get('synced_ts') else '-'
    return (f"rebin: {RULE} scanned {s['scanned']}/{s['files']} KGs ({s['errors']} errors) · off_file {s['off_file']} of {s['stored']} stored"
            f" · lost_cell {LOST_ID} {s['lost_scanned']}/{s['files']} KGs: {s['lost']} apices in cells of another file"
            + (f" ({s['lost_errors']} errors)" if s['lost_errors'] else '')
            + f" · added {s['added']} (no_row {s['no_row']} / other_file {s['other_file']} / lost_cell {s['lost_cell']})"
            f" + dup {s['dup'] + s['dup_frame'] + s['dup_shift'] + s['dup_added'] + s['lost_dup']} (twin {s['dup'] + s['lost_dup']} / frame {s['dup_frame']}"
            f" / shift {s['dup_shift']} in {s['shift_pairs']} file pairs / added-twin {s['dup_added']}) + qa {s['qa'] + s['lost_qa']}"
            + (f" + undecided {s['pending']}" if s['pending'] else '')
            + f" · capped {s['capped']} · invariant ok {s['invariant_ok']}/{s['scanned']}"
            + (f" BAD {s['invariant_bad']}" if s['invariant_bad'] else '')
            + f" · {s['kgs_with_added']} KGs with added trees · synced {ts}"
            + ('' if s['resolved'] else ' · RESOLVE PENDING') + (f" · {s['unsynced']} KGs unsynced" if s['unsynced'] else ''))


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--scan', action='store_true'); ap.add_argument('--sync', action='store_true')
    ap.add_argument('--resolve', action='store_true'); ap.add_argument('--dry', action='store_true')
    ap.add_argument('--status', action='store_true'); ap.add_argument('--kg', action='append')
    ap.add_argument('--budget-s', type=float); ap.add_argument('--part', default='all', choices=('all', 'off', 'lost'))
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if a.scan:
        print(json.dumps(scan(a.budget_s, a.kg, part=a.part)))
    if a.resolve:
        print(json.dumps(resolve()))
    if a.sync:
        print(json.dumps(sync(dry=a.dry)))
    if a.status or not (a.scan or a.sync or a.resolve):
        print(json.dumps(status())); print(text_line())
