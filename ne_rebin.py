"""ne_rebin — ONE rule for "which res-12 cell does this apex belong to".

    cell(apex) = latLngToCell(round(lon, 6), round(lat, 6), 12) of the position we SERVE
                 for it (stored 5-bit dx / dy from the metre-snapped cell centre, after every
                 read-time position change: ``ne_pos_fix`` frames today)

Applied at read time where a chunk is inflated (``cells_api._load_chunk`` and the posted-rows
reader ``ne_h3.neo1_rows_from_h3b``) — the ONE place every reader goes through — so
``/trees/bbox`` (``cell`` + ownership), ``tree_n`` / ``tree_h_max`` (obs rows, cells API, posted
NEO1), the ``tree_h`` / ``tree_h_n`` lists, ``canopy_h_p*`` and the res-9 ``tree_hh`` histogram all
count the SAME apices per cell.  No file is rewritten, no position changes, nothing is added
to a payload.  Full story, invariants and the checklist for new position sources:
``docs/tree-cell-assignment.md``.

Why it was needed: the builder binned the float apex position and then stored it rounded to
the metre (≈ 5.5 % of all apices sit in the neighbouring hexagon of their served position), and
a KG with a position frame (``ne_pos_fix``, 7 m) kept every apex in its pre-frame cell (≈ 47 %).

How (stateless, per chunk, neighbours cached):

* :func:`_assign` — per stored chunk: target cell of every apex.  H3 is only asked for
  apices farther than ``SAFE_R`` from their row's centre (a res-12 hexagon's inradius is
  > 8 m everywhere in Austria).  A move is accepted when the re-encoded dx / dy fit 5 bits.
* :func:`rebin` — the chunk as served: apices leave to / arrive from the 6 neighbouring
  res-10 chunks of the SAME KG file.  A mover whose target cell is no row of that file (edge
  of the KG's cell set, ≈ 0.05 %; ≈ 0.7 % in a KG with a frame) is NOT served from this file:
  an apex without a cell row has no cell to be counted in, and where another KG file stores
  that cell, that file's own inventory covers the spot (one file per cell, ``lost_cells``).
  ``--check`` reports them as ``off_file``.  Row order is deterministic: by cell row, own
  apices first (stored order), then arrivals by (source chunk, source row).
* identity: ``Chunk.t_src = (src_r10[], src_idx[])`` names the stored NET1 row of every served
  apex — per-stored-row verdicts (``tree_rule`` header marker) are translated through it
  (:func:`marker_keep`).  Position-keyed stores (linear noise, user reports, flags) need nothing.

``REBIN_ID`` (= ``tree_rule.CELL_RULE_ID``) is folded into ``tree_qa.THX_ID`` and therefore into
every bin / block / national generation, per-KG ETag, posted-rows token and the tree-QA store.

    python3 ne_rebin.py --check 92124 [--check …]     # `outside` must be 0; `off_file` = not served
    python3 ne_rebin.py --bound-hmax                  # index-only: chunk.hmax := ring max (upper bound)
"""
from __future__ import annotations

import logging

import numpy as np

import tree_rule

log = logging.getLogger(__name__)

REBIN_ID = tree_rule.CELL_RULE_ID
RES = 12
SAFE_R = 6.0                    # m — closer to the row centre than this: inside the hexagon, no H3 call
LL_DECIMALS = 6                 # = the lon / lat precision /trees/bbox publishes


def _a_bytes(a: dict) -> int:
    n = 256 + int(a['ids'].nbytes)
    if a.get('ci') is not None:
        n += int(a['ci'].nbytes)
    if a.get('mv') is not None:
        n += sum(int(v.nbytes) for v in a['mv'])
    return n


import mem_lru as _mem_lru
_lru = _mem_lru.ByteLRU('rebin_assign', 12, sizer=_a_bytes, env='NE_REBIN_MB')   # stored-chunk assignments
_h3 = None
_tf = None


def _h3n():
    global _h3
    if _h3 is None:
        import h3.api.numpy_int as h
        _h3 = h
    return _h3


def _to_wgs(x, y):
    global _tf
    if _tf is None:
        from pyproj import Transformer
        _tf = Transformer.from_crs('EPSG:3035', 'EPSG:4326', always_xy=True)
    return _tf.transform(x, y)


_tf_inv = None


def _centres(ids: np.ndarray) -> np.ndarray:
    """(n, 2) metre-snapped EPSG:3035 centres — the dx / dy origin (same rounding as
    ``ne_h3.cell_centres_3035``; own cached transformer, that one builds a new one per call)."""
    global _tf_inv
    if not len(ids):
        return np.zeros((0, 2), np.int64)
    if _tf_inv is None:
        from pyproj import Transformer
        _tf_inv = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)
    f = _h3n().cell_to_latlng
    ll = np.array([f(int(c)) for c in ids])
    e, n = _tf_inv.transform(ll[:, 1], ll[:, 0])
    return np.stack([np.round(e), np.round(n)], 1).astype(np.int64)


def _s5(v: np.ndarray) -> np.ndarray:
    v = v.astype(np.int64)
    v[v >= 16] -= 32
    return v


def cells_of_xy(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """THE rule: EPSG:3035 served positions → res-12 cell of the published lon / lat."""
    if not len(x):
        return np.zeros(0, np.uint64)
    lon, lat = _to_wgs(np.asarray(x, np.float64), np.asarray(y, np.float64))
    f = _h3n().latlng_to_cell
    return np.fromiter((f(a, b, RES) for a, b in zip(np.round(lat, LL_DECIMALS).tolist(), np.round(lon, LL_DECIMALS).tolist())),
                       np.uint64, count=len(lon))


def ring(r10: int) -> list[int]:
    """The 6 (5 at a pentagon) res-10 neighbours of a chunk."""
    return [int(c) for c in _h3n().grid_disk(int(r10), 1) if int(c) != int(r10)]


def live_r10s(tree_r10s) -> set:
    """Chunks that can carry an apex as served: every chunk with stored trees + its ring."""
    out: set = set()
    gd = _h3n().grid_disk
    for r in tree_r10s:
        out.update(int(c) for c in gd(int(r), 1))
    return out


class Source:
    """One KG file as the rebin sees it.  ``key`` identifies file version + position frame;
    ``rows(r10s)`` → {r10: handle} of the chunks present with stored trees or cells;
    ``stored(r10, handle)`` → (obs bytes, NET1 bytes AFTER position overrides)."""
    key: tuple = ()

    def rows(self, r10s) -> dict:            # pragma: no cover - interface
        raise NotImplementedError

    def stored(self, r10: int, handle) -> tuple[bytes, bytes]:   # pragma: no cover - interface
        raise NotImplementedError

    def trees_n(self, handle) -> int:        # stored NET1 rows of that chunk (index / header)
        return int(handle.get('trees') or 0)


_ID_DT = np.dtype('<u8')


def _assign(src: Source, r10: int, obs: bytes, trees: bytes, rec: int, o_tn: int) -> dict:
    """Target cell of every stored apex of one chunk (cached per file version)."""
    key = (src.key, int(r10))
    a = _lru.get(key)
    if a is not None:
        return a
    rows = np.frombuffer(obs, np.uint8).reshape(-1, rec)
    ids = np.ascontiguousarray(rows[:, :8]).view(_ID_DT).ravel()
    tn = rows[:, o_tn].astype(np.int64)
    w = np.frombuffer(trees, '<u4')
    a = {'ids': ids, 'n': int(w.size), 'exact': int(tn.sum()) == w.size and bool(np.all(ids[1:] > ids[:-1])), 'mv': None}
    if a['exact'] and w.size:
        ci = np.repeat(np.arange(ids.size), tn)
        dx = _s5(w & 31); dy = _s5((w >> 5) & 31)
        far = np.flatnonzero(dx * dx + dy * dy > SAFE_R * SAFE_R)
        if far.size:
            cen = _centres(ids)
            x = cen[ci[far], 0] + dx[far]; y = cen[ci[far], 1] + dy[far]
            c = cells_of_xy(x, y)
            m = c != ids[ci[far]]
            if m.any():
                idx = far[m]; tgt = c[m]
                ut, inv = np.unique(tgt, return_inverse=True)
                tc = _centres(ut)[inv]
                ndx = x[m] - tc[:, 0]; ndy = y[m] - tc[:, 1]
                fit = (ndx >= -16) & (ndx <= 15) & (ndy >= -16) & (ndy <= 15)
                if fit.any():
                    idx = idx[fit]; tgt = tgt[fit]
                    nw = (w[idx] & np.uint32(0xFFFFFC00)) | (ndx[fit] & 31).astype(np.uint32) | ((ndy[fit] & 31).astype(np.uint32) << 5)
                    a['mv'] = (idx, tgt, nw.astype('<u4'))      # stored row idx, target cell, re-encoded word
        a['ci'] = ci
    _lru.put(key, a)
    return a


def rebin(src: Source, r10: int, obs: bytes, trees: bytes, rec: int, o_tn: int, o_thx: int, stats: dict | None = None):
    """One chunk as served → ``(obs, NET1, src_r10[], src_idx[])`` or None when nothing moves.
    *obs* / *trees* = this chunk's stored rows (trees after position overrides); neighbours are
    read through *src*.  tree_n = apices per row (≤ 255), tree_h_max = their tallest on the
    0.5 m scale (``tree_rule.quant_h``; 255 = none)."""
    r10 = int(r10)
    a = _assign(src, r10, obs, trees, rec, o_tn)
    if not a['exact']:
        return None
    ids = a['ids']; w = np.frombuffer(trees, '<u4')
    nb_rows = src.rows(ring(r10))
    nb: dict = {}

    def neighbour(r):
        if r not in nb:
            h = nb_rows.get(r)
            b = None
            if h is not None:
                b = _lru.get((src.key, r))
                if b is None:
                    o, t = src.stored(r, h)
                    b = _assign(src, r, o, t, rec, o_tn)
                b = b if b['exact'] else None
            nb[r] = b
        return nb[r]

    row = a['ci'].copy() if w.size else np.zeros(0, np.int64)
    words = w.copy()
    keep = np.ones(w.size, bool)
    n_moved = n_drop = 0
    if a['mv'] is not None:
        idx, tgt, nw = a['mv']
        pos = np.searchsorted(ids, tgt); inside = (pos < ids.size) & (ids[np.minimum(pos, ids.size - 1)] == tgt)
        row[idx[inside]] = pos[inside]; words[idx[inside]] = nw[inside]
        n_moved = int(inside.sum())
        out = np.flatnonzero(~inside)
        if out.size:
            par = np.fromiter((_h3n().cell_to_parent(int(c), 10) for c in tgt[out]), np.uint64, count=out.size)
            for p in np.unique(par).tolist():
                b = neighbour(int(p))
                sel = out[par == p]
                if b is None:
                    has = np.zeros(sel.size, bool)
                else:
                    q = np.searchsorted(b['ids'], tgt[sel]); has = (q < b['ids'].size) & (b['ids'][np.minimum(q, b['ids'].size - 1)] == tgt[sel])
                # in the neighbour chunk of this file → served there; in no row of this file (edge
                # of the KG's cell set, ~0.05 %) → not served from this file: where another KG
                # file has that cell, its own inventory covers it (same spirit as the
                # one-file-per-cell rule of cells_api.lost_cells)
                keep[idx[sel]] = False
                n_drop += int((~has).sum())
                if stats is not None and 'off' in stats and not has.all():
                    g = sel[~has]               # the caller's list: (source r10, stored row idx, target cell, re-encoded word)
                    stats['off'].append((r10, idx[g].copy(), tgt[g].copy(), nw[g].copy()))
    in_row = []; in_w = []; in_r = []; in_i = []
    for r in sorted(nb_rows):
        if not src.trees_n(nb_rows[r]):
            continue
        b = neighbour(r)
        if b is None or b['mv'] is None:
            continue
        idx, tgt, nw = b['mv']
        pos = np.searchsorted(ids, tgt); m = (pos < ids.size) & (ids[np.minimum(pos, ids.size - 1)] == tgt)
        if m.any():
            in_row.append(pos[m]); in_w.append(nw[m]); in_r.append(np.full(int(m.sum()), r, np.uint64)); in_i.append(idx[m])
    if stats is not None:
        stats['dropped'] = stats.get('dropped', 0) + n_drop
    if a['mv'] is None and not in_row:
        return None
    own = np.flatnonzero(keep)
    rows_all = np.concatenate([row[own]] + in_row)
    w_all = np.concatenate([words[own]] + in_w)
    s_r = np.concatenate([np.full(own.size, r10, np.uint64)] + in_r)
    s_i = np.concatenate([own] + in_i).astype(np.int64)
    arrived = np.concatenate([np.zeros(own.size, np.int8), np.ones(rows_all.size - own.size, np.int8)])
    o = np.lexsort((s_i, s_r, arrived, rows_all))
    rows_all, w_all, s_r, s_i = rows_all[o], w_all[o], s_r[o], s_i[o]
    cnt = np.bincount(rows_all, minlength=ids.size)
    if cnt.max(initial=0) > 255:                     # tree_n is one byte: the builder's cap, kept
        rank = np.arange(rows_all.size) - np.repeat(np.concatenate([[0], np.cumsum(cnt)[:-1]]), cnt)
        k = rank < 255
        log.warning('ne_rebin: %s/%x: %d apices over the 255-per-cell cap dropped', src.key[0], r10, int((~k).sum()))
        if stats is not None:
            stats['capped'] = stats.get('capped', 0) + int((~k).sum())
        rows_all, w_all, s_r, s_i = rows_all[k], w_all[k], s_r[k], s_i[k]
        cnt = np.minimum(cnt, 255)
    ob = np.frombuffer(obs, np.uint8).reshape(-1, rec).copy()
    mx = np.zeros(ids.size, np.uint8)
    np.maximum.at(mx, rows_all, tree_rule.quant_h(((w_all >> 10) & 0xFF) * 0.25))
    old_tn = ob[:, o_tn].copy()
    ob[:, o_tn] = cnt
    sel = (cnt < 255) & (old_tn < 255)
    ob[sel, o_thx] = np.where(cnt[sel] > 0, mx[sel], 255)
    if stats is not None:
        stats['moved'] = stats.get('moved', 0) + n_moved + int((~keep).sum()) - n_drop
    return ob.tobytes(), np.ascontiguousarray(w_all, '<u4').tobytes(), s_r, s_i.astype(np.int32)


def marker_keep(hdr: dict | None, r10: int, h_m, t_src) -> np.ndarray:
    """``tree_rule.chunk_keep`` for a served chunk: the header marker names STORED NET1 rows, so
    every served apex is looked up under its source chunk / row.  Files without a current
    marker: the rule on the chunk as served."""
    if t_src is None or tree_rule.hidden_rows(hdr, r10) is None:
        return tree_rule.chunk_keep(hdr, r10, h_m)
    s_r, s_i = t_src
    ok = np.ones(len(s_i), bool)
    for r in np.unique(s_r).tolist():
        hr = tree_rule.hidden_rows(hdr, int(r))
        if hr is not None and len(hr):
            m = s_r == r
            ok[m] = ~np.isin(s_i[m], hr)
    return ok


# === SECTION: sources ===

class FileSource(Source):
    """A product / corpus file read with ``ne_h3`` (posted-rows path)."""

    def __init__(self, path, hdr: dict, kg: str | None = None):
        import ne_pos_fix
        self.path = path; self.hdr = hdr; self.kg = kg or hdr.get('kg')
        self._pf = ne_pos_fix
        self.key = (str(self.kg), str((hdr.get('neo1') or {}).get('digest')) + ne_pos_fix.tag(self.kg), 'f')
        self._rows = {int(c['r10'], 16): c for c in hdr['chunks']}

    def rows(self, r10s) -> dict:
        return {r: self._rows[r] for r in r10s if r in self._rows}

    def stored(self, r10, handle):
        import ne_h3
        p = ne_h3.read_h3b_chunk(self.path, self.hdr, handle)
        return p['obs'], self.fix(p['trees'])

    def fix(self, trees: bytes) -> bytes:
        """Stored NET1 rows → with the position overrides every reader applies."""
        return self._pf.fix_words(self.kg, trees, b'')[0]


# === SECTION: index accelerator + check CLI ===

def bound_hmax(verbose: bool = True) -> dict:
    """``chunk.hmax`` := max over the chunk and its ring in the same file — an upper bound of
    the tallest apex as served, from the index alone (minutes).  Run BEFORE code that rebins
    goes live: /trees/bbox's early exit stays correct with a bound;
    ``ne_trees.backfill_hmax(force=True)`` then replaces it by the exact value file by file.
    Once per ``REBIN_ID`` (meta key ``hmax_rebin``) — a second run would only loosen the bound."""
    import time
    import cells_api as ca
    c = ca._idx_conn(write=True)
    if (c.execute("SELECT v FROM meta WHERE k='hmax_rebin'").fetchone() or [None])[0] == REBIN_ID:
        return {'skipped': 'already bounded for ' + REBIN_ID}
    t0 = time.time(); n = 0
    by_kg: dict = {}                    # ONE scan (the primary key is (r10, kg): a per-KG query scans the table)
    for r, kg, h in c.execute('SELECT r10, kg, hmax FROM chunk WHERE hmax IS NOT NULL'):
        by_kg.setdefault(kg, {})[int(r)] = int(h)
    kgs = sorted(by_kg)
    for k, kg in enumerate(kgs, 1):
        hm = by_kg[kg]
        new: dict = {}
        for r, h in hm.items():
            if h:
                for q in ring(r):
                    if q in hm and hm[q] < h and new.get(q, 0) < h:
                        new[q] = h
        if new:
            with ca._idx_write_lock:
                c.executemany('UPDATE chunk SET hmax=? WHERE r10=? AND kg=?', [(h, r, kg) for r, h in new.items()]); c.commit()
            n += len(new)
        if verbose and k % 1000 == 0:
            print(f'  {k}/{len(kgs)} files, {n} chunks raised, {time.time() - t0:.0f}s', flush=True)
    with ca._idx_write_lock:
        c.execute("INSERT OR REPLACE INTO meta VALUES ('hmax_rebin', ?)", (REBIN_ID,)); c.commit()
    return {'files': len(kgs), 'chunks_raised': n, 'seconds': round(time.time() - t0, 1)}


def scan(kg: str) -> tuple[dict, dict]:
    """ONE pass over a KG file as served → ``(check dict, off)``; *off* = every apex the cell rule
    takes out of this file (target cell is no row of it), as columns: ``r10`` / ``idx`` (stored
    NET1 row), ``cell`` (target res-12), ``x`` / ``y`` (served EPSG:3035 m), ``lon`` / ``lat``
    (6 dp), ``h_m``, ``crown_m2``, ``species``, ``vitality``, ``ndsm_only``, ``word``."""
    import cells_api as ca
    kg = str(kg).zfill(5)
    hdr = ca.header(kg, with_chunks=True)
    st = {'kg': kg, 'apices': 0, 'outside': 0, 'stored': 0, 'chunks': 0, 'frame': bool(ca._pos_fix.offset(kg))}
    rs: dict = {'off': []}
    for c in hdr['chunks']:
        row = dict(c, r10=int(c['r10'], 16), trees=int(c.get('trees') or 0), bld=int(c.get('bld') or 0))
        st['stored'] += row['trees']
        ch = ca._load_chunk(kg, row, hdr, qa=False, stats=rs)
        st['chunks'] += 1
        if not ch.trees.size:
            continue
        d = ca.decode_trees(ch.trees)
        ci = np.repeat(np.arange(ch.cells), ch.obs['tn'].astype(np.int64))
        cen = ch.centres()
        st['apices'] += int(ch.trees.size)
        st['outside'] += int((cells_of_xy(cen[ci, 0] + d['dx'], cen[ci, 1] + d['dy']) != ch.ids[ci]).sum())
    parts = rs['off']
    off = {'r10': np.concatenate([np.full(p[1].size, p[0], np.uint64) for p in parts]) if parts else np.zeros(0, np.uint64),
           'idx': np.concatenate([p[1] for p in parts]).astype(np.int64) if parts else np.zeros(0, np.int64),
           'cell': np.concatenate([p[2] for p in parts]).astype(np.uint64) if parts else np.zeros(0, np.uint64),
           'word': np.concatenate([p[3] for p in parts]).astype(np.uint32) if parts else np.zeros(0, np.uint32)}
    d = ca.decode_trees(off['word'])
    ut, inv = np.unique(off['cell'], return_inverse=True)
    cen = _centres(ut)[inv] if ut.size else np.zeros((0, 2), np.int64)
    off['x'] = cen[:, 0] + d['dx']; off['y'] = cen[:, 1] + d['dy']
    lon, lat = _to_wgs(off['x'].astype(np.float64), off['y'].astype(np.float64)) if ut.size else (np.zeros(0), np.zeros(0))
    off['lon'] = np.round(lon, LL_DECIMALS); off['lat'] = np.round(lat, LL_DECIMALS)
    for k in ('h_m', 'crown_m2', 'species', 'vitality', 'ndsm_only'):
        off[k] = d[k]
    st.update(moved=rs.get('moved', 0), off_file=rs.get('dropped', 0), capped=rs.get('capped', 0),
              ok=st['outside'] == 0 and st['apices'] == st['stored'] - rs.get('dropped', 0) - rs.get('capped', 0)
              and off['idx'].size == rs.get('dropped', 0))
    return st, off


def check(kg: str) -> dict:
    """Every apex of a KG as served: is it inside the cell row that lists it?  With the off-file
    store (``rebin_added``) scanned for this KG also the invariant
    stored == served + added + dup + qa (+ capped): no apex is lost by the rule."""
    st = scan(kg)[0]
    try:
        import rebin_added
        c = rebin_added.kg_counts(kg)
    except Exception:  # noqa: BLE001
        c = None
    if c is not None:
        st.update(added=c['added'], dup=c['dup'], qa=c['qa'], no_row=c['no_row'], other_file=c['other_file'])
        st['invariant'] = (c['stored'] == st['stored'] and c['off'] == st['off_file']
                           and st['stored'] == st['apices'] + c['added'] + c['dup'] + c['qa'] + st['capped'])
        st['ok'] = bool(st['ok'] and st['invariant'])
    return st


if __name__ == '__main__':
    import argparse
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument('--check', action='append', default=[])
    ap.add_argument('--bound-hmax', action='store_true')
    a = ap.parse_args()
    for k in a.check:
        print(json.dumps(check(k)))
    if a.bound_hmax:
        print(json.dumps(bound_hmax()))
