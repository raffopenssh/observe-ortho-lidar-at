"""tree_qa — the cell products show exactly the tree apices ``/api/v1/trees/bbox`` returns by
default (realistic=1, min_height=3).

One predicate: ``ne_trees._qa_mask`` (height rule, linear-noise runs, reader flags, legacy QA
flags).  It is evaluated here, per KG file, on the few res-10 chunks that can be affected, and
the result — the NET1 row indices it hides — is kept in ``data/ne/tree_qa.sqlite``.  Readers
never evaluate anything: ``cells_api._load_chunk`` looks the chunk up in an in-memory snapshot
(O(1), nothing for an unaffected chunk) and, on a hit, drops those apex rows and rewrites
``tree_n`` / ``tree_h_max`` of the cells that lost one (:func:`patch`).  Everything built from
chunks follows: /cells (all formats), bins / blocks / sample, national + stats aggregates, the
public KG container, and the NEO1 rows posted upstream (``ne_h3.neo1_rows_from_h3b``).
The KG files stay the raw inventory (``/trees/bbox?realistic=0`` is unchanged).

Cache validity: ``chunk_rev`` (chunk LRU key), ``kg_tok`` (per-KG ETags, pub container, posted
digest cache), ``bin_tok`` (folded into the bin generation only when non-empty — untouched bins
keep bytes, generation and ETag).

``refresh()`` — hourly in front of ``cells_bins --prewarm`` (same niced unit), single-flight,
resumable per KG (one transaction each), bounded memory, budgeted: re-evaluates KGs whose
inputs changed (file digest, position tag, linear-noise token, flag revisions), then patches
the tree columns of the affected cells inside the stored bins (no bin rebuild) and re-stores
their blocks, so the public workers do not build on demand.

    python3 tree_qa.py --status | --refresh [--budget-s N] [--limit N] | --kg 92001
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
import zlib

import numpy as np

import ne_h3
import tree_rule

log = logging.getLogger('tree_qa')

DB_PATH = ne_h3.NE_DIR / 'tree_qa.sqlite'
BIN_DEG = 0.025
_STAT_S = 30.0                  # snapshot reload at most this often per process
RULE_ID = tree_rule.RULE_ID     # bump THERE (one place) to force a re-evaluation of every KG

_snap = {'t': 0.0, 'ver': None, 'hid': {}, 'kg': {}, 'dig': {}, 'bin': {}, 'binkg': {}}
_lock = threading.Lock()


def _db_path():
    return DB_PATH


# === SECTION: store + snapshot (request path) ===

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(_db_path()), timeout=60)
    c.executescript(
        'CREATE TABLE IF NOT EXISTS hid (kg TEXT, r10 INTEGER, idx BLOB, PRIMARY KEY (kg, r10)) WITHOUT ROWID;'
        'CREATE TABLE IF NOT EXISTS kg (kg TEXT PRIMARY KEY, in_sig TEXT, tok TEXT, n INTEGER, cells INTEGER, ts INTEGER, dig TEXT) WITHOUT ROWID;'
        'CREATE TABLE IF NOT EXISTS binkg (i INTEGER, j INTEGER, kg TEXT, sig TEXT, PRIMARY KEY (i, j, kg)) WITHOUT ROWID;')
    return c


def version():
    try:
        st = os.stat(_db_path())
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def _bin_tok(per_kg: dict) -> str:
    return hashlib.sha1(json.dumps(sorted(per_kg.items())).encode()).hexdigest()[:12] if per_kg else ''


def _snapshot() -> dict:
    now = time.time()
    s = _snap
    if now - s['t'] < _STAT_S:
        return s
    with _lock:
        ver = version()
        if ver != _snap['ver']:
            hid: dict = {}; kgt: dict = {}; binkg: dict = {}; dig: dict = {}
            if ver is not None:
                try:
                    c = sqlite3.connect(f'file:{_db_path()}?mode=ro', uri=True, timeout=10)
                    try:
                        c.execute('BEGIN')
                        for kg, r10, idx in c.execute('SELECT kg, r10, idx FROM hid'):
                            hid[(kg, r10)] = idx
                        for k, t, d in c.execute("SELECT kg, tok, dig FROM kg WHERE tok != ''"):
                            kgt[k] = t; dig[k] = d
                        for i, j, kg, sig in c.execute('SELECT i, j, kg, sig FROM binkg'):
                            binkg.setdefault((i, j), {})[kg] = sig
                    finally:
                        c.close()
                except sqlite3.Error as e:
                    log.warning('tree_qa: store unreadable (%s) — keeping the previous snapshot', e)
                    _snap['t'] = now
                    return _snap
            _snap.update(ver=ver, hid=hid, kg=kgt, dig=dig, binkg=binkg, bin={ij: _bin_tok(v) for ij, v in binkg.items()})
        _snap['t'] = now
        return _snap


def sync() -> bool:
    """Bring this process's snapshot up to the store on disk NOW (one stat; reload only when the
    store changed) → True when it changed.  Every reader that is about to BUILD or REPLACE a
    stored product (``cells_bins.load_bin``, ``cells_blocks.serve_block`` / ``store_block``)
    calls this on a generation mismatch and re-checks first: a worker whose ≤ 30 s old snapshot
    predates a refresh would otherwise take the refresher's freshly patched blob for stale,
    rebuild it with the old filter and overwrite it."""
    if version() == _snap['ver']:
        return False
    _snap['t'] = 0.0
    _snapshot()
    return True


def chunk_idx(kg: str, r10: int, digest: str | None = None):
    """Hidden NET1 row indices (u16 LE bytes) of one stored chunk, or None.  With *digest*
    (the file's NEO1 digest) a store entry evaluated on another file version is ignored."""
    s = _snapshot()
    b = s['hid'].get((kg, int(r10)))
    if b is not None and digest is not None and s['dig'].get(kg) != digest:
        return None
    return b


def kg_tok(kg: str, digest: str | None = None) -> str:
    s = _snapshot()
    if digest is not None and s['dig'].get(str(kg)) != digest:
        return ''
    return s['kg'].get(str(kg), '')


def kgs_tok(kgs) -> str:
    s = _snapshot()['kg']
    return '|'.join(f'{k}:{s[k]}' for k in sorted(map(str, kgs)) if k in s)


def bin_tok(i: int, j: int) -> str:
    return _snapshot()['bin'].get((i, j), '')


# === SECTION: the override (one implementation for chunks and posted rows) ===

_TN_OFF = None


def _offsets():
    global _TN_OFF
    if _TN_OFF is None:
        import cells_api as ca
        _TN_OFF = (ca.OBS_DT.fields['tn'][1], ca.OBS_DT.fields['thx'][1], ca.OBS_DT.itemsize)
    return _TN_OFF


quant_h = tree_rule.quant_h     # apex height → u8 ×0.5 m (shared with the ne_h3 builder)


def patch(obs: bytes, trees: bytes, idx: bytes) -> tuple[bytes, bytes]:
    """NEO1 rows + NET1 rows of one chunk without the hidden apices: rows dropped, and for
    the cells that lost one ``tree_n`` = kept count, ``tree_h_max`` = tallest kept (255 = none)."""
    o_tn, o_thx, rec = _offsets()
    rows = np.frombuffer(obs, np.uint8).reshape(-1, rec).copy()
    w = np.frombuffer(trees, '<u4')
    hide = np.frombuffer(idx, '<u2').astype(np.int64)
    hide = hide[hide < w.size]
    if not hide.size:
        return obs, trees
    n = rows.shape[0]
    cell = np.repeat(np.arange(n), rows[:, o_tn].astype(np.int64))[:w.size]
    keep = np.ones(w.size, bool); keep[hide] = False
    touched = np.unique(cell[hide])
    cnt = np.bincount(cell[keep], minlength=n)
    mx = np.zeros(n, np.uint8)
    np.maximum.at(mx, cell[keep], quant_h(((w[keep] >> 10) & 0xFF) * 0.25))
    rows[touched, o_tn] = np.minimum(cnt[touched], 255)
    rows[touched, o_thx] = np.where(cnt[touched] > 0, mx[touched], 255)
    return rows.tobytes(), np.ascontiguousarray(w[keep]).tobytes()


# === SECTION: evaluation per KG (refresh side) ===

def _static_candidates() -> dict:
    """kg → [r10] of the chunks whose tallest apex can trip the height rule (index only)."""
    import cells_api as ca
    import ne_trees as nt
    out: dict = {}
    c = ca._idx_conn()
    if c is not None:
        for r10, kg in c.execute('SELECT r10, kg FROM chunk WHERE hmax >= ? AND trees > 0', (int(nt.TREE_SPIKE_H * 4),)):
            out.setdefault(kg, []).append(int(r10))
    return out


def _legacy_points() -> dict:
    import landscape_query as lq
    import ne_trees as nt
    per: dict = {}
    fb = lq.flags_db()
    if fb is not None:
        for kg, lo, la in fb.execute(
                'SELECT kg_code, centroid_lon, centroid_lat FROM flags WHERE kind=? AND severity IN (%s) AND centroid_lon IS NOT NULL'
                % ','.join('?' * len(lq.FLAG_SEVERITIES)), (nt.LEGACY_FLAG_KIND['trees'], *lq.FLAG_SEVERITIES)):
            per.setdefault(str(kg), []).append((lo, la))
    return per


class _Ctx:
    """Inputs of one pass, read once: height-rule candidates, linear keys, flag / legacy KGs."""

    def __init__(self):
        import cells_api as ca
        import ne_trees as nt
        import ne_linear
        self.ca = ca; self.nt = nt
        self.files = ca._files()
        self.static = _static_candidates()
        self.sn = ne_linear._snapshot()
        self.lin_all = ne_linear.hidden(list(self.sn['keys']))
        try:
            import public_flags as pf
            self.flag_kgs = {str(k) for (k,) in pf._conn().execute(
                'SELECT DISTINCT kg FROM report WHERE status IN (0,1) AND cell IS NOT NULL')}
        except Exception as e:  # noqa: BLE001
            log.warning('tree_qa: reader flags: %s', e); self.flag_kgs = set()
        try:
            self.legacy = _legacy_points()
        except Exception as e:  # noqa: BLE001
            log.warning('tree_qa: legacy flags: %s', e); self.legacy = {}
        self._nb: dict = {}

    def neighbours(self, kg: str) -> list:
        v = self._nb.get(kg)
        if v is None:
            bb = self.ca._kg_bbox(kg)
            v = self._nb[kg] = sorted(set(self.ca._kgs_intersecting(*bb)) | {kg}) if bb else [kg]
        return v

    def lin_kgs(self, kg: str) -> list:
        return [k for k in self.neighbours(kg) if k in self.sn['keys']]

    def has_inputs(self, kg: str) -> bool:
        return bool(kg in self.static or kg in self.flag_kgs or kg in self.legacy or self.lin_kgs(kg))

    def in_sig(self, kg: str) -> str:
        f = self.files[kg]
        parts = [RULE_ID, str(f.get('neo1_digest')), self.ca._pos_fix.tag(kg),
                 ','.join(f'{k}:{self.sn["tok"].get(k)}' for k in self.lin_kgs(kg)),
                 self.nt.flag_rev(kg) if kg in self.flag_kgs else '',
                 hashlib.sha1(repr(sorted(self.legacy[kg])).encode()).hexdigest()[:10] if kg in self.legacy else '']
        return hashlib.sha1('|'.join(parts).encode()).hexdigest()[:20]


def _ring_r10(hn, lon, lat) -> set:
    out: set = set(); seen: set = set()
    for lo, la in zip(lon, lat):
        c = int(hn.latlng_to_cell(float(la), float(lo), 10))
        if c not in seen:
            seen.add(c); out.update(int(r) for r in hn.grid_disk(c, 1))
    return out


def evaluate_kg(kg: str, ctx: _Ctx) -> tuple[dict, dict]:
    """→ ({r10: hidden row idx u16 bytes}, {(i, j): sig of the hidden apices whose cell is in that bin})."""
    ca, nt = ctx.ca, ctx.nt
    hn = ca._h3n()
    hdr = ca.header(kg, with_chunks=True)
    if hdr is None:
        return {}, {}
    rows = {int(c['r10'], 16): c for c in hdr['chunks'] if int(c.get('trees') or 0) > 0}
    # height rule: only files built without it (new files carry net1.tree_rule — the builder
    # already dropped those apices; linear-noise / flag candidates below apply to both)
    cand = set() if tree_rule.built_with(hdr) else set(ctx.static.get(kg, ()))
    tf = ca._tf('EPSG:3035', 'EPSG:4326')
    for k in ctx.lin_kgs(kg):
        keys = ctx.sn['keys'][k]
        x = (keys >> np.uint64(32)).astype(np.int64); y = (keys & np.uint64(0xFFFFFFFF)).astype(np.int64)
        lon, lat = tf.transform(x, y)
        cand |= _ring_r10(hn, lon, lat)
    if kg in ctx.flag_kgs:
        fc, fp, _, _ = nt._flags(kg, 'trees')
        cand |= {int(hn.cell_to_parent(int(c), 10)) for c in fc}
        if len(fp):
            lon, lat = tf.transform(fp[:, 0], fp[:, 1])
            cand |= _ring_r10(hn, lon, lat)
    if kg in ctx.legacy:
        a = np.array(ctx.legacy[kg])
        cand |= _ring_r10(hn, a[:, 0], a[:, 1])
    q = nt._qa_state((), 'trees')
    q['lin'], q['lin_grid'] = ctx.lin_all
    hid: dict = {}; cells = []; words = []
    for r10 in sorted(cand & set(rows)):
        c = rows[r10]
        row = dict(c, r10=r10, trees=int(c.get('trees') or 0), bld=int(c.get('bld') or 0))
        ch = ca._load_chunk(kg, row, hdr, qa=False)
        m = nt.chunk_keep(ch, q, hdr)
        if m is None:
            continue
        bad = np.flatnonzero(~m)
        hid[r10] = bad.astype('<u2').tobytes()
        cell_idx = np.repeat(np.arange(ch.cells), ch.obs['tn'].astype(np.int64))[:ch.trees.size]
        cells.append(ch.ids[cell_idx[bad]].astype(np.uint64)); words.append(((ch.trees[bad] >> 10) & 0xFF).astype(np.uint64))
    bins: dict = {}
    if cells:
        cells = np.concatenate(cells); words = np.concatenate(words)
        uc, inv = np.unique(cells, return_inverse=True)
        ll = np.array([hn.cell_to_latlng(int(c)) for c in uc])
        bi = np.floor(ll[:, 1] / BIN_DEG).astype(np.int64)[inv]; bj = np.floor(ll[:, 0] / BIN_DEG).astype(np.int64)[inv]
        o = np.lexsort((words, cells, bj, bi))
        bi, bj, cells, words = bi[o], bj[o], cells[o], words[o]
        cut = np.flatnonzero(np.diff(bi) | np.diff(bj)) + 1
        for a, b in zip(np.concatenate([[0], cut]), np.concatenate([cut, [cells.size]])):
            h = hashlib.sha1(); h.update(cells[a:b].tobytes()); h.update(words[a:b].tobytes())
            bins[(int(bi[a]), int(bj[a]))] = (h.hexdigest()[:12], cells[a:b])
    return hid, bins


def _kg_tok(hid: dict) -> str:
    if not hid:
        return ''
    h = hashlib.sha1()
    for r10 in sorted(hid):
        h.update(str(r10).encode()); h.update(hid[r10])
    return h.hexdigest()[:12]


def _hidden_cells(con, kg: str, ca) -> dict:
    """bin → cell ids currently hidden-from for one KG (what a change must re-patch)."""
    out: dict = {}
    hdr = None
    hn = ca._h3n()
    for r10, idx in con.execute('SELECT r10, idx FROM hid WHERE kg=?', (kg,)).fetchall():
        if hdr is None:
            hdr = ca.header(kg, with_chunks=True)
            rows = {int(c['r10'], 16): c for c in (hdr or {}).get('chunks') or []}
        c = rows.get(int(r10)) if hdr else None
        if c is None:
            continue
        try:
            ch = ca._load_chunk(kg, dict(c, r10=int(r10), trees=int(c.get('trees') or 0), bld=int(c.get('bld') or 0)), hdr, qa=False)
        except Exception:  # noqa: BLE001
            continue
        cell_idx = np.repeat(np.arange(ch.cells), ch.obs['tn'].astype(np.int64))[:ch.trees.size]
        bad = np.frombuffer(idx, '<u2').astype(np.int64); bad = bad[bad < cell_idx.size]
        for cid in np.unique(ch.ids[cell_idx[bad]]).tolist():
            la, lo = hn.cell_to_latlng(int(cid))
            out.setdefault((int(np.floor(lo / BIN_DEG)), int(np.floor(la / BIN_DEG))), set()).add(int(cid))
    return out


def _commit_kg(con, kg: str, sig: str | None, hid: dict, bins: dict, dig: str | None = None) -> None:
    con.execute('BEGIN IMMEDIATE')
    con.execute('DELETE FROM hid WHERE kg=?', (kg,)); con.execute('DELETE FROM binkg WHERE kg=?', (kg,))
    if sig is None:
        con.execute('DELETE FROM kg WHERE kg=?', (kg,))
    else:
        con.executemany('INSERT INTO hid VALUES (?,?,?)', [(kg, r, b) for r, b in hid.items()])
        con.executemany('INSERT INTO binkg VALUES (?,?,?,?)', [(i, j, kg, s) for (i, j), (s, _) in bins.items()])
        con.execute('INSERT OR REPLACE INTO kg VALUES (?,?,?,?,?,?,?)',
                    (kg, sig, _kg_tok(hid), sum(len(b) // 2 for b in hid.values()), sum(int(np.unique(c).size) for _, c in bins.values()), int(time.time()), dig))
    con.commit()
    with _lock:                              # this process sees its own write at once (no full reload)
        s = _snap
        for k in [k for k in s['hid'] if k[0] == kg]:
            del s['hid'][k]
        for ij in [ij for ij, v in s['binkg'].items() if kg in v]:
            s['binkg'][ij].pop(kg, None)
            if not s['binkg'][ij]:
                del s['binkg'][ij]; s['bin'].pop(ij, None)
            else:
                s['bin'][ij] = _bin_tok(s['binkg'][ij])
        s['kg'].pop(kg, None); s['dig'].pop(kg, None)
        if sig is not None:
            for r, b in hid.items():
                s['hid'][(kg, r)] = b
            for ij, (sg, _) in bins.items():
                s['binkg'].setdefault(ij, {})[kg] = sg; s['bin'][ij] = _bin_tok(s['binkg'][ij])
            t = _kg_tok(hid)
            if t:
                s['kg'][kg] = t; s['dig'][kg] = dig
        s['ver'] = version(); s['t'] = time.time()


def refresh_kg(kg: str) -> dict:
    """Evaluate one KG now (ingest hook): store only — its bins change generation with the new file anyway."""
    kg = str(kg).zfill(5)
    _snapshot()
    ctx = _Ctx()
    if kg not in ctx.files:
        return {'kg': kg, 'skipped': 'no file'}
    con = _conn()
    try:
        if not ctx.has_inputs(kg):
            if con.execute('SELECT 1 FROM kg WHERE kg=?', (kg,)).fetchone():
                _commit_kg(con, kg, None, {}, {})
            return {'kg': kg, 'hidden': 0}
        hid, bins = evaluate_kg(kg, ctx)
        _commit_kg(con, kg, ctx.in_sig(kg), hid, bins, ctx.files[kg].get('neo1_digest'))
        return {'kg': kg, 'hidden': sum(len(b) // 2 for b in hid.values()), 'bins': len(bins)}
    finally:
        con.close()


def refresh(budget_s: float | None = None, limit: int | None = None, verbose: bool = True, bins: bool = True) -> dict:
    """Single-flight across processes (flock): a second caller returns at once."""
    import fcntl
    with open(_db_path().with_suffix('.lock'), 'w') as lk:
        try:
            fcntl.flock(lk, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return {'skipped': 'another refresh is running'}
        return _refresh(budget_s, limit, verbose, bins)


def _refresh(budget_s, limit, verbose, do_bins) -> dict:
    import cells_api as ca
    import cells_bins as cb
    import cells_blocks
    t0 = time.time()
    _snap['t'] = 0.0; _snapshot()
    ctx = _Ctx()
    con = _conn()
    stored = {k: s for k, s in con.execute('SELECT kg, in_sig FROM kg')}
    todo = []
    for kg in sorted(set(ctx.files) | set(stored)):
        if kg not in ctx.files or not ctx.has_inputs(kg):
            if kg in stored:
                todo.append((kg, None))
        elif stored.get(kg) != ctx.in_sig(kg):
            todo.append((kg, ctx.in_sig(kg)))
    st = {'kgs_with_inputs': sum(1 for k in ctx.files if ctx.has_inputs(k)), 'todo': len(todo), 'kgs_evaluated': 0,
          'kgs_changed': 0, 'bins_patched': 0, 'bins_rebuilt': 0, 'bins_left_to_prewarm': 0, 'blocks_stored': 0,
          'cells_patched': 0, 'errors': 0, 'stopped_on_budget': False, 'plan_s': round(time.time() - t0, 1)}
    if verbose:
        print(f"tree_qa: {st['kgs_with_inputs']} KGs with inputs, {len(stored)} stored, {len(todo)} to evaluate ({st['plan_s']}s)", flush=True)
    dirty: set = set(); t_flush = time.time()

    def flush() -> None:
        nonlocal t_flush
        for blk in sorted(dirty):
            for fs in cells_blocks.prewarm_fieldsets():
                try:
                    if cells_blocks.stored_meta(blk[0], blk[1], fs) is not None:
                        cells_blocks.store_block(blk[0], blk[1], fs); st['blocks_stored'] += 1
                except Exception as e:  # noqa: BLE001
                    log.warning('tree_qa: block %s: %s', blk, e)
        dirty.clear(); t_flush = time.time()

    for kg, sig in todo:
        if (budget_s is not None and time.time() - t0 > budget_s) or (limit and st['kgs_evaluated'] >= limit):
            st['stopped_on_budget'] = True
            break
        try:
            old_tok = dict(_snap['bin'])
            old_gen = {}
            before = _hidden_cells(con, kg, ca) if kg in stored else {}
            hid, kbins = evaluate_kg(kg, ctx) if sig is not None else ({}, {})
            touched = set(before) | set(kbins)
            if do_bins:
                for ij in touched:
                    old_gen[ij] = cb.bin_inputs(*ij)['gen']
            _commit_kg(con, kg, sig, hid, kbins, (ctx.files.get(kg) or {}).get('neo1_digest'))
            st['kgs_evaluated'] += 1
            for ij in sorted(touched):
                if _snap['bin'].get(ij, '') == old_tok.get(ij, ''):
                    continue
                if not do_bins:
                    continue
                cells = set(before.get(ij, ())) | set(kbins[ij][1].tolist() if ij in kbins else ())
                r = cb.patch_tree_cols(ij[0], ij[1], np.array(sorted(cells), np.uint64), old_gen[ij])
                st['bins_' + r] += 1; st['cells_patched'] += len(cells) if r == 'patched' else 0
                dirty.add((ij[0] // cb.BLOCK_N, ij[1] // cb.BLOCK_N))
            st['kgs_changed'] += int(bool(touched))
        except Exception as e:  # noqa: BLE001
            st['errors'] += 1
            log.warning('tree_qa: %s: %s', kg, e)
        if time.time() - t_flush > 20:
            flush()
        if verbose and st['kgs_evaluated'] and st['kgs_evaluated'] % 250 == 0:
            print(f"  {st['kgs_evaluated']}/{len(todo)} KGs, {st['bins_patched']} bins patched … {time.time() - t0:.0f}s", flush=True)
    flush()
    con.close()
    st.update(seconds=round(time.time() - t0, 1), finished_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    try:
        last = _db_path().with_suffix('.json'); runs = _db_path().with_name('tree_qa_runs.jsonl')
        with open(runs, 'a') as fh:                       # run history (one line per refresh that did work)
            if fh.tell() == 0 and last.exists():
                fh.write(last.read_text().strip() + '\n')
            if st['kgs_evaluated'] or st['errors']:
                fh.write(json.dumps(st) + '\n')
        last.write_text(json.dumps(st))
    except OSError:
        pass
    return st


def status() -> dict:
    out = {'db': str(_db_path()), 'exists': _db_path().exists(), 'rule': RULE_ID}
    if out['exists']:
        c = _conn()
        a = c.execute("SELECT COUNT(*), SUM(tok != ''), SUM(n), SUM(cells) , MAX(ts) FROM kg").fetchone()
        out.update(kgs_tracked=a[0], kgs_filtered=a[1] or 0, apices_hidden=a[2] or 0, cells_touched=a[3] or 0, last_ts=a[4],
                   chunks=c.execute('SELECT COUNT(*) FROM hid').fetchone()[0],
                   bins=c.execute('SELECT COUNT(*) FROM (SELECT DISTINCT i, j FROM binkg)').fetchone()[0],
                   blocks=c.execute('SELECT COUNT(*) FROM (SELECT DISTINCT i/4, j/4 FROM binkg)').fetchone()[0])
        c.close()
    try:
        out['last_run'] = json.loads(_db_path().with_suffix('.json').read_text())
    except (OSError, ValueError):
        pass
    return out


def text_line() -> str:
    s = status()
    lr = s.get('last_run') or {}
    return (f"tree_qa: {s.get('kgs_filtered', 0)} KGs / {s.get('bins', 0)} bins / {s.get('cells_touched', 0)} cells / "
            f"{s.get('apices_hidden', 0)} apices hidden from cell products (= /trees/bbox default); last refresh "
            f"{lr.get('finished_at', '-')}: {lr.get('kgs_evaluated', 0)}/{lr.get('todo', 0)} KGs, {lr.get('bins_patched', 0)} bins patched, "
            f"{lr.get('errors', 0)} errors{' (budget stop)' if lr.get('stopped_on_budget') else ''}")


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--status', action='store_true'); ap.add_argument('--refresh', action='store_true')
    ap.add_argument('--no-bins', action='store_true'); ap.add_argument('--kg')
    ap.add_argument('--budget-s', type=float); ap.add_argument('--limit', type=int)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.refresh:
        print(json.dumps(refresh(a.budget_s, a.limit, bins=not a.no_bins)))
    elif a.kg:
        print(json.dumps(refresh_kg(a.kg)))
    else:
        print(json.dumps(status(), indent=1)); print(text_line())
