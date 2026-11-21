"""tree_qa — keeps the per-cell tree fields of the cells products (``cells_bins``: tree_h /
tree_h_n / tree_n / tree_h_max / canopy_h_p50|p90) equal to what ``/api/v1/trees/bbox``
returns by default (realistic=1, min_height=3).

The predicate itself lives in ``ne_trees`` (``_qa_mask`` / ``chunk_keep``) and is applied by
``cells_bins.build_bin``.  This module only answers "which bins does it change?": a tiny
store ``data/ne/tree_qa.sqlite`` with one row per candidate 0.025° bin —

    in_sig  hash of everything the hidden set of the bin can depend on
    tok     hash of the hidden (cell, apex word) rows, '' when nothing is hidden

``tok`` (when non-empty) is folded into the bin generation (``cells_bins._gen``), so only
bins whose bytes change get a new gen / ETag.  ``refresh()`` (hourly, in front of the
``cells_bins --prewarm`` pass, same niced unit) re-evaluates the bins whose ``in_sig``
changed, block by block, and rebuilds a bin + its prebuilt blocks right after flipping its
token — the public workers never have to build one on demand.  Resumable (one commit per
bin), bounded memory (one bin's candidate chunks at a time), budgeted.

    python3 tree_qa.py --status | --refresh [--budget-s N] [--limit N] [--dry]
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time

import numpy as np

import cells_api as ca
import ne_h3

log = logging.getLogger('tree_qa')

DB_PATH = ca.NE_DIR / 'tree_qa.sqlite'
STATE_PATH = ca.NE_DIR / 'tree_qa.json'
BIN_DEG = 0.025
R10_PAD_DEG = 0.0013           # res-10 cell circumradius (~76 m) in degrees, generous
_STAT_S = 30.0                  # token reload (and bin-manifest recompute) at most this often per worker

_cache = {'t': 0.0, 'ver': None, 'tok': {}}
_lock = threading.Lock()


# === SECTION: store (read side: request path) ===

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), timeout=30)
    c.execute('CREATE TABLE IF NOT EXISTS bin (i INTEGER, j INTEGER, in_sig TEXT, tok TEXT, n INTEGER, cells INTEGER, '
              'ts INTEGER, PRIMARY KEY (i, j)) WITHOUT ROWID')
    return c


def version():
    """Changes whenever the store is written (part of the bin-manifest cache key)."""
    try:
        st = os.stat(DB_PATH)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def _tokens() -> dict:
    now = time.time()
    if now - _cache['t'] < _STAT_S:
        return _cache['tok']
    with _lock:
        ver = version()
        if ver != _cache['ver']:
            tok = {}
            if ver is not None:
                try:
                    c = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True, timeout=10)
                    try:
                        tok = {(i, j): t for i, j, t in c.execute("SELECT i, j, tok FROM bin WHERE tok != ''")}
                    finally:
                        c.close()
                except sqlite3.Error as e:
                    log.warning('tree_qa: store unreadable (%s) — keeping previous tokens', e)
                    tok = _cache['tok']; ver = _cache['ver']
            _cache['tok'] = tok; _cache['ver'] = ver
        _cache['t'] = now
        return _cache['tok']


def tok(i: int, j: int) -> str:
    """'' unless the default tree filter hides ≥ 1 apex of this bin."""
    return _tokens().get((i, j), '')


# === SECTION: candidates (cheap: index + flag stores only) ===

def _bins_of_r10(hn, r10: int) -> list:
    la, lo = hn.cell_to_latlng(r10)
    return list({(int(np.floor((lo + a) / BIN_DEG)), int(np.floor((la + b) / BIN_DEG)))
                 for a in (-R10_PAD_DEG, R10_PAD_DEG) for b in (-R10_PAD_DEG, R10_PAD_DEG)})


def candidates() -> dict:
    """bin (i, j) → {'r10': set, 'items': set} — every res-10 chunk that can hold an apex the
    default filter hides: tallest apex ≥ TREE_SPIKE_H (height rule), a linear-noise key, a
    reader flag (cell / point) or a legacy QA point (each ± one res-10 ring)."""
    import ne_trees as nt
    hn = ca._h3n()
    out: dict = {}
    bins_memo: dict = {}

    def add(r10: int, item) -> None:
        b = bins_memo.get(r10)
        if b is None:
            b = bins_memo[r10] = _bins_of_r10(hn, r10)
        for ij in b:
            e = out.get(ij)
            if e is None:
                e = out[ij] = {'r10': set(), 'items': set()}
            e['r10'].add(r10)
            if item is not None:
                e['items'].add(item)

    def add_points(lon, lat, item) -> None:
        seen = set()
        for lo, la in zip(lon, lat):
            c = int(hn.latlng_to_cell(float(la), float(lo), 10))
            if c not in seen:
                seen.add(c)
                for r in hn.grid_disk(c, 1):
                    add(int(r), item)

    files = ca._files()
    c = ca._idx_conn()
    for r10, kg in c.execute('SELECT r10, kg FROM chunk WHERE hmax >= ? AND trees > 0', (int(nt.TREE_SPIKE_H * 4),)):
        if kg in files:
            add(int(r10), None)
    tf = ca._tf('EPSG:3035', 'EPSG:4326')
    try:
        import ne_linear
        sn = ne_linear._snapshot()
        for kg, keys in sn['keys'].items():
            if kg not in files or not keys.size:
                continue
            x = (keys >> np.uint64(32)).astype(np.int64); y = (keys & np.uint64(0xFFFFFFFF)).astype(np.int64)
            lon, lat = tf.transform(x, y)
            add_points(lon, lat, f'l:{kg}:{sn["tok"].get(kg)}')
    except Exception as e:  # noqa: BLE001
        log.warning('tree_qa: linear keys: %s', e)
    try:
        import public_flags as pf
        for (kg,) in pf._conn().execute('SELECT DISTINCT kg FROM report WHERE status IN (0,1) AND cell IS NOT NULL').fetchall():
            kg = str(kg)
            if kg not in files:
                continue
            fc, fp, _, _ = nt._flags(kg, 'trees')
            item = f'f:{kg}:{nt.flag_rev(kg)}'
            for cell in fc:
                add(int(hn.cell_to_parent(int(cell), 10)), item)
            if len(fp):
                lon, lat = tf.transform(fp[:, 0], fp[:, 1])
                add_points(lon, lat, item)
    except Exception as e:  # noqa: BLE001
        log.warning('tree_qa: reader flags: %s', e)
    try:
        import landscape_query as lq
        fb = lq.flags_db()
        per: dict = {}
        if fb is not None:
            for kg, lo, la in fb.execute(
                    'SELECT kg_code, centroid_lon, centroid_lat FROM flags WHERE kind=? AND severity IN (%s) AND centroid_lon IS NOT NULL'
                    % ','.join('?' * len(lq.FLAG_SEVERITIES)), (nt.LEGACY_FLAG_KIND['trees'], *lq.FLAG_SEVERITIES)):
                per.setdefault(str(kg), []).append((lo, la))
        for kg, pts in per.items():
            if kg in files:
                a = np.array(sorted(pts))
                add_points(a[:, 0], a[:, 1], 'q:%s:%s' % (kg, hashlib.sha1(a.tobytes()).hexdigest()[:10]))
    except Exception as e:  # noqa: BLE001
        log.warning('tree_qa: legacy flags: %s', e)
    return out


def _in_sig(i: int, j: int, cand: dict) -> str:
    import cells_bins as cb
    h = hashlib.sha1()
    h.update(cb.bin_inputs(i, j)['base_gen'].encode())
    h.update(json.dumps(sorted(cand['r10'])).encode()); h.update(json.dumps(sorted(cand['items'])).encode())
    return h.hexdigest()[:20]


# === SECTION: evaluation (the shared predicate on the bin's candidate chunks) ===

def evaluate(i: int, j: int, r10s) -> tuple[str, int, int]:
    """(tok, hidden apices, cells touched) of one bin — same cell selection and the same
    keep-mask ``cells_bins.build_bin`` uses, restricted to the candidate chunks."""
    import cells_bins as cb
    inp = cb.bin_inputs(i, j)
    if not inp['on_disk'] or not r10s:
        return '', 0, 0
    ids = np.sort(ca.ids_in_bbox(*inp['bbox']))
    sub = ids[np.isin(ne_h3.h3_parent_vec(ids, ca.CHUNK_RES), np.fromiter(r10s, np.uint64, len(r10s)))]
    if not sub.size:
        return '', 0, 0
    tb, _, _ = ca.select(sub, inp['bbox'])
    if not len(tb):
        return '', 0, 0
    ci, words = tb.gather_trees()
    keep = cb.tree_keep(tb, inp['on_disk']) if words.size else None
    if keep is None:
        return '', 0, 0
    hid = ~keep
    cells = tb.ids[ci[hid]].astype(np.uint64); w = words[hid].astype(np.uint64)
    o = np.lexsort((w, cells))
    h = hashlib.sha1(); h.update(cells[o].tobytes()); h.update(w[o].tobytes())
    return h.hexdigest()[:16], int(hid.sum()), int(np.unique(cells).size)


# === SECTION: refresh pass ===

def refresh(budget_s: float | None = None, limit: int | None = None, dry: bool = False, verbose: bool = True) -> dict:
    import cells_bins as cb
    import cells_blocks
    t0 = time.time()
    cand = candidates()
    con = _conn()
    stored = {(i, j): (s, t) for i, j, s, t in con.execute('SELECT i, j, in_sig, tok FROM bin')}
    todo = []
    for ij in set(cand) | set(stored):
        sig = _in_sig(*ij, cand[ij]) if ij in cand else None
        if sig is None or stored.get(ij, (None,))[0] != sig:
            todo.append((ij, sig))
    todo.sort(key=lambda x: (x[0][0] // cb.BLOCK_N, x[0][1] // cb.BLOCK_N, x[0]))
    t_cand = time.time() - t0
    if verbose:
        print(f'tree_qa: {len(cand)} candidate bins, {len(stored)} stored, {len(todo)} to evaluate ({t_cand:.0f}s)', flush=True)
    st = {'candidates': len(cand), 'todo': len(todo), 'evaluated': 0, 'changed_bins': 0, 'rebuilt_bins': 0,
          'rebuilt_blocks': 0, 'hidden': 0, 'cells': 0, 'errors': 0, 'stopped_on_budget': False}
    dirty: set = set(); cur_block = None

    def flush_block(blk) -> None:
        if blk is None or blk not in dirty or dry:
            return
        dirty.discard(blk)
        for fs in cells_blocks.prewarm_fieldsets():
            try:
                if cells_blocks.stored_meta(blk[0], blk[1], fs) is not None:      # only blobs that exist get refreshed here
                    cells_blocks.store_block(blk[0], blk[1], fs); st['rebuilt_blocks'] += 1
            except Exception as e:  # noqa: BLE001
                log.warning('tree_qa: block %s: %s', blk, e)

    for (i, j), sig in todo:
        blk = (i // cb.BLOCK_N, j // cb.BLOCK_N)
        if blk != cur_block:
            flush_block(cur_block); cur_block = blk
            if (budget_s is not None and time.time() - t0 > budget_s) or (limit and st['evaluated'] >= limit):
                st['stopped_on_budget'] = True
                break
        try:
            t, n, k = evaluate(i, j, cand[(i, j)]['r10']) if sig is not None else ('', 0, 0)
        except Exception as e:  # noqa: BLE001
            st['errors'] += 1
            log.warning('tree_qa: evaluate %d_%d: %s', i, j, e)
            continue
        st['evaluated'] += 1; st['hidden'] += n; st['cells'] += k
        old = stored.get((i, j), (None, ''))[1] or ''
        if dry:
            st['changed_bins'] += int(t != old)
            continue
        if sig is None:
            con.execute('DELETE FROM bin WHERE i=? AND j=?', (i, j))
        else:
            con.execute('INSERT OR REPLACE INTO bin VALUES (?,?,?,?,?,?,?)', (i, j, sig, t, n, k, int(time.time())))
        con.commit()
        if t != old:
            st['changed_bins'] += 1
            with _lock:                      # this process sees its own write at once
                _cache['t'] = 0.0; _cache['ver'] = None
            try:
                cb.load_bin(i, j, rebuild=True); st['rebuilt_bins'] += 1; dirty.add(blk)
            except Exception as e:  # noqa: BLE001
                st['errors'] += 1
                log.warning('tree_qa: rebuild %d_%d: %s', i, j, e)
        if verbose and st['evaluated'] % 200 == 0:
            print(f"  {st['evaluated']}/{len(todo)} evaluated, {st['changed_bins']} changed … {time.time() - t0:.0f}s", flush=True)
    else:
        flush_block(cur_block)
    flush_block(cur_block)
    con.close()
    st.update(seconds=round(time.time() - t0, 1), candidates_s=round(t_cand, 1),
              finished_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    if not dry:
        try:
            STATE_PATH.write_text(json.dumps(st))
        except OSError:
            pass
    return st


def status() -> dict:
    out = {'db': str(DB_PATH), 'exists': DB_PATH.exists()}
    if out['exists']:
        c = _conn()
        a = c.execute("SELECT COUNT(*), SUM(tok != ''), SUM(n), SUM(cells), MAX(ts) FROM bin").fetchone()
        c.close()
        out.update(bins_tracked=a[0], bins_filtered=a[1] or 0, apices_hidden=a[2] or 0, cells_touched=a[3] or 0, last_ts=a[4])
    try:
        out['last_run'] = json.loads(STATE_PATH.read_text())
    except (OSError, ValueError):
        pass
    return out


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--status', action='store_true'); ap.add_argument('--refresh', action='store_true')
    ap.add_argument('--dry', action='store_true'); ap.add_argument('--budget-s', type=float); ap.add_argument('--limit', type=int)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.refresh:
        print(json.dumps(refresh(a.budget_s, a.limit, dry=a.dry)))
    else:
        print(json.dumps(status(), indent=1))
