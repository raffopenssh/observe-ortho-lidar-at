"""tree_user — what people tell us about single trees: which taxon a measured
apex is, and trees we do not have at all.  Fed through ``POST /api/v1/flags``
(``claim: species`` / ``claim: missing`` on layer ``trees``), served back
sparsely on ``/api/v1/trees/bbox`` and as a delta feed ``/api/v1/trees/taxa``.

Design (docs/tree-user.md):
  * a tree is keyed by its position only: EPSG:3035 integer metres of the
    measured apex it was matched to at write time (the corpus is metre-snapped),
    or the reporter's point for a tree we do not have.  No ids of any kind.
  * storage = ONE append-only table ``tree_ver`` in ``public_flags.sqlite``
    (~65 B / row incl. index: ints + dictionary ids).  Nothing is ever updated or deleted:
    every change of mind, retraction and moderation is a new row, ``rev`` (the
    rowid) is the global revision clients sync against, the row count of a tree
    is its version ``v``.  The product epoch a version was made against is kept
    (dictionary id) so a version can be judged after a KG rebuild.
  * the *current* answer per tree is derived: each reporter's latest live
    version votes, plurality wins (tie → newest).  Cached per KG in process,
    invalidated across gunicorn workers by one ``MAX(rev)`` lookup.
  * read side is sparse: ``/trees/bbox`` only grows by a ``user`` block (row
    indices of trees with a taxon) and an ``added`` block when a KG of the bbox
    has any — zero bytes otherwise; existing fields never change.
"""
from __future__ import annotations

import logging
import math
import re
import threading
import time

import numpy as np
from flask import request

import public_flags as pf
import tree_taxa as tt

log = logging.getLogger('tree_user')

OP_SPECIES, OP_ADD, OP_RETRACT, OP_MODERATE = 0, 1, 2, 3
OP_NAMES = ('species', 'add', 'retract', 'moderated')
MATCH_MIN_M = 1.5            # a point this close to an apex means that tree …
MATCH_CROWN_FRAC = 0.5       # … or within this share of its crown radius (whichever is larger)
MATCH_SURE_M = 0.75          # the apex coordinate itself (metre-snapped corpus) — never ambiguous
MATCH_AMBIG_RATIO = 2.0      # a second tree within reach must be this many times farther, else 409
ADDED_NAME_M = 1.5           # a name / measure call reaches a user-added tree within this
ADDED_FOLD_M = 0.5           # an add this close to the reporter's own added tree is the same tree
EXISTS_M = 2.0               # "missing" this close to a measured apex needs force / a measured apex
ID_SNAP_M = 0.5              # corpus apex = product apex rounded to the metre (per axis)
ID_SHIFT_M = 1.5             # … or one pixel aside, when it is the only apex there
SEARCH_M = 12.0
ORPHAN_M = 1.5               # read side: apex moved after a rebuild → re-attach within this …
ORPHAN_DH_M = 3.0            # … if the height still agrees
ADDED_DEFAULT_H = 3.0        # window height of an added tree without a reported height (inventory floor)
MAX_VERSIONS_PER_TREE = 200
MAX_ROWS = 5_000_000         # ~250 MB worst case; writes are refused past it
MAX_FREE_TAXA = 3000         # distinct taxa outside the built-in list
MAX_TAXON_CHARS = 60
LIST_LIMIT = 5000
LIST_MAX_AREA_DEG2 = 0.25
TAXA_MAX_AGE = 30

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS tree_ver (
        rev INTEGER PRIMARY KEY,            -- global revision (monotonic; rows are never deleted)
        ts INTEGER NOT NULL,
        x INTEGER NOT NULL, y INTEGER NOT NULL,   -- EPSG:3035 metres: matched apex, or the reporter's point (add)
        kg INTEGER NOT NULL,
        op INTEGER NOT NULL,                -- 0 species | 1 add | 2 retract | 3 moderated (ref = hidden rev)
        sp INTEGER,                         -- dict id (kind 'taxon')
        h_dm INTEGER,                       -- apex height when matched (dm; NULL for added trees)
        reporter BLOB NOT NULL,             -- 8-byte salted hash, never returned
        source INTEGER,                     -- dict id
        epoch INTEGER,                      -- dict id (kind 'epoch'): product epoch the version was made against
        ref INTEGER
    )""",
    'CREATE INDEX IF NOT EXISTS tv_kg ON tree_ver(kg, x, y)',
]
_ADD_COLS = (('uh_dm', 'INTEGER'),      # reporter's own height measurement / estimate (dm)
             ('dbh_mm', 'INTEGER'),     # reporter's diameter at breast height (mm)
             ('e_dm', 'INTEGER'),       # exact EPSG:3035 position in 0.1 m: the inventory tree id (op 0/2 rows)
             ('n_dm', 'INTEGER'),       #   or the reporter's point of an added tree (op 1 rows)
             ('cid', 'TEXT'),           # client_id of an added tree (idempotency key + identity)
             ('mts', 'INTEGER'),        # measured_at (epoch s); ts stays the receive time
             ('meta', 'INTEGER'),       # packed training metadata, 4 bits each (META_FIELDS order; 0 = not given)
             ('org', 'INTEGER'))        # dict id (kind 'org'): holder of the reporter key the version came with
# training metadata vocabulary (standard English terms; value index + 1 is stored).  German field terms
# and common synonyms are accepted and normalised.
META_FIELDS = (
    ('dbh_method', ('caliper', 'tape', 'estimate')),
    ('h_method', ('hypsometer', 'laser', 'estimate')),
    ('vitality', ('alive', 'dead')),
    ('status', ('standing', 'felled')),
)
_META_ALIAS = {
    'dbh_method': {'kluppe': 'caliper', 'calliper': 'caliper', 'calipers': 'caliper', 'umfang': 'tape',
                   'girth': 'tape', 'girth_tape': 'tape', 'circumference': 'tape', 'massband': 'tape',
                   'diameter_tape': 'tape', 'schaetzung': 'estimate', 'geschaetzt': 'estimate',
                   'estimated': 'estimate', 'visual': 'estimate', 'ocular': 'estimate'},
    'h_method': {'vertex': 'hypsometer', 'clinometer': 'hypsometer', 'hoehenmesser': 'hypsometer',
                 'rangefinder': 'laser', 'laser_rangefinder': 'laser', 'lidar': 'laser',
                 'schaetzung': 'estimate', 'geschaetzt': 'estimate', 'estimated': 'estimate',
                 'visual': 'estimate', 'ocular': 'estimate'},
    'vitality': {'lebend': 'alive', 'living': 'alive', 'live': 'alive', 'vital': 'alive', 'tot': 'dead',
                 'abgestorben': 'dead', 'snag': 'dead'},
    'status': {'stehend': 'standing', 'gefaellt': 'felled', 'geschlaegert': 'felled', 'cut': 'felled',
               'harvested': 'felled', 'removed': 'felled'},
}
_TREE_ID = re.compile(r'^t_(\d{7,9})_(\d{7,9})$')
_CLIENT_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{7,63}$')
DBH_CM = (1.0, 600.0)
H_M = (1.0, 70.0)

# the dictionary + resolver live in tree_taxa.py (rank, codes, synonyms, abbreviations)
TAXA = tt.TAXA
_CLASS = tt.CLASS


def normalise(raw) -> tuple[str, bool]:
    """→ (scientific name, known).  Accepts every name, code and synonym of
    the dictionary, rule-based Latin abbreviations ("P. abies", "PiAb"), or
    any well-formed binomial (known False)."""
    if not isinstance(raw, str) or not raw.strip():
        raise pf.FlagError(400, 'species must be a non-empty string', dict='/api/v1/trees/taxa/dict')
    try:
        return tt.resolve(raw, MAX_TAXON_CHARS)
    except tt.Unknown as e:
        x = {'did_you_mean': e.did_you_mean, 'dict': '/api/v1/trees/taxa/dict'}
        if e.ambiguous:
            x['reason'] = 'ambiguous'
        raise pf.FlagError(400, str(e), **x)


taxon_class = tt.taxon_class


def _fold(s: str) -> str:
    s = s.strip().lower().replace('×', 'x')
    for a, b in (('ä', 'ae'), ('ö', 'oe'), ('ü', 'ue'), ('ß', 'ss')):
        s = s.replace(a, b)
    return re.sub(r'[\s_]+', ' ', s)


def _conn():
    c = pf._conn()
    if getattr(pf._TLS, 'tu_c', None) is not c:
        for s in _SCHEMA:
            c.execute(s)
        have = {r[1] for r in c.execute('PRAGMA table_info(tree_ver)')}
        for col, typ in _ADD_COLS:
            if col not in have:
                try:
                    c.execute(f'ALTER TABLE tree_ver ADD COLUMN {col} {typ}')
                except Exception:  # noqa: BLE001 — another worker won the race
                    pass
        c.execute('CREATE INDEX IF NOT EXISTS tv_cid ON tree_ver(cid) WHERE cid IS NOT NULL')
        pf._TLS.tu_c = c
    return c


def global_rev() -> int:
    try:
        if not pf.DB_PATH.exists():
            return 0
        return int(_conn().execute('SELECT COALESCE(MAX(rev), 0) FROM tree_ver').fetchone()[0])
    except Exception as e:  # noqa: BLE001
        log.debug('tree_user rev: %s', e)
        return 0


def _tf(a, b):
    import cells_api as ca
    return ca._tf(a, b)


def _to_3035(lon, lat):
    return _tf('EPSG:4326', 'EPSG:3035').transform(lon, lat)


# === SECTION: current state (derived, cached per KG) ===

_state: dict = {}            # kg(int) -> state dict
_state_lock = threading.Lock()
_EMPTY = {'n': 0, 'rev': 0, 'path': None}


def _derive(rows) -> dict:
    """rows of ONE tree in rev order → its current answer."""
    hidden = {r['ref'] for r in rows if r['op'] == OP_MODERATE}
    votes: dict = {}         # reporter -> (rev, sp)
    adds: set = set()        # reporters who added the tree
    uh: dict = {}            # reporter -> own height (dm)
    dbh: dict = {}           # reporter -> dbh (mm)
    apex_h = None
    tid = pos = cid = None
    for r in rows:
        if r['e_dm'] is not None:                 # identity survives retraction / moderation
            if r['op'] == OP_ADD:
                pos = pos or (r['e_dm'], r['n_dm'])
            elif r['op'] in (OP_SPECIES, OP_RETRACT):
                tid = (r['e_dm'], r['n_dm'])
        if r['cid'] and cid is None:
            cid = r['cid']
        if r['op'] == OP_MODERATE or r['rev'] in hidden:
            continue
        rep = bytes(r['reporter'])
        if r['op'] == OP_RETRACT:
            votes.pop(rep, None); adds.discard(rep); uh.pop(rep, None); dbh.pop(rep, None)
            continue
        if r['op'] == OP_ADD:
            adds.add(rep)
        elif r['h_dm'] is not None:
            apex_h = r['h_dm']
        if r['uh_dm'] is not None:
            uh[rep] = r['uh_dm']
        if r['dbh_mm'] is not None:
            dbh[rep] = r['dbh_mm']
        if r['sp'] is not None:
            votes[rep] = (r['rev'], r['sp'])
    sp = None; n = 0
    if votes:
        cnt: dict = {}
        for rev, s in votes.values():
            a = cnt.setdefault(s, [0, 0]); a[0] += 1; a[1] = max(a[1], rev)
        sp, (n, _) = max(cnt.items(), key=lambda kv: (kv[1][0], kv[1][1]))
    hs = sorted(uh.values()); ds = sorted(dbh.values())          # several reporters → median
    return {'sp': sp, 'n': n, 'added': bool(adds), 'h_dm': hs[len(hs) // 2] if hs else None,
            'dbh_mm': ds[len(ds) // 2] if ds else None, 'apex_h': apex_h, 'tid': tid, 'pos': pos, 'cid': cid,
            'v': len(rows), 'rev': rows[-1]['rev'], 'reporters': len(set(votes) | adds | set(uh) | set(dbh))}


def _load(c, kg: int) -> dict:
    rows = c.execute('SELECT rev, x, y, op, sp, h_dm, uh_dm, dbh_mm, reporter, ref, e_dm, n_dm, cid FROM tree_ver '
                     'WHERE kg=? ORDER BY rev', (kg,)).fetchall()
    if not rows:
        return dict(_EMPTY, path=str(pf.DB_PATH))
    by: dict = {}
    for r in rows:
        by.setdefault((r['x'], r['y']), []).append(r)
    xy = np.array(list(by), dtype=np.int64).reshape(-1, 2)
    cur = [_derive(v) for v in by.values()]
    # exact position where we know it: the inventory id of an apex, the reporter's point of an added tree
    ex = [(d['tid'] if not d['added'] else None) or d['pos'] or d['tid'] for d in cur]
    e = np.array([p[0] / 10.0 if p else float(k[0]) for p, k in zip(ex, by)], np.float64)
    n = np.array([p[1] / 10.0 if p else float(k[1]) for p, k in zip(ex, by)], np.float64)
    lon, lat = _tf('EPSG:3035', 'EPSG:4326').transform(e, n)
    st = {'n': len(cur), 'rev': rows[-1]['rev'], 'path': str(pf.DB_PATH), 'x': xy[:, 0], 'y': xy[:, 1], 'e': e, 'n_': n,
          'tid': [tree_id_str(d['tid']) for d in cur], 'cid': [d['cid'] for d in cur],
          'key': (xy[:, 0] << 32) | xy[:, 1], 'lon': np.asarray(lon), 'lat': np.asarray(lat),
          'sp': np.array([d['sp'] or 0 for d in cur], np.int64), 'cnt': np.array([d['n'] for d in cur], np.int32),
          'added': np.array([d['added'] for d in cur], bool), 'v': np.array([d['v'] for d in cur], np.int32),
          'trev': np.array([d['rev'] for d in cur], np.int64),
          'h': np.array([np.nan if d['h_dm'] is None else d['h_dm'] / 10.0 for d in cur]),
          'dbh': np.array([np.nan if d['dbh_mm'] is None else d['dbh_mm'] / 10.0 for d in cur]),
          'apex_h': np.array([np.nan if d['apex_h'] is None else d['apex_h'] / 10.0 for d in cur])}
    st['live'] = (st['sp'] > 0) | st['added'] | np.isfinite(st['h']) | np.isfinite(st['dbh'])
    st['has_id'] = np.array([t is not None for t in st['tid']], bool)
    # inventory trees reported by id that the corpus had no apex for (apex-bound versions store the apex height)
    st['idonly'] = st['has_id'] & ~st['added'] & ~np.isfinite(st['apex_h'])
    return st


def tree_id_str(t) -> str | None:
    return f't_{t[0]}_{t[1]}' if t else None


def parse_tree_id(v):
    """'t_<E>_<N>' (EPSG:3035 in 0.1 m — the id of the product apex inventory) → (e_dm, n_dm) | None."""
    if v in (None, ''):
        return None
    m = _TREE_ID.match(str(v).strip())
    if not m:
        raise pf.FlagError(400, 'tree_id must look like "t_47092875_26831275" (EPSG:3035 easting_northing in 0.1 m); '
                                'ids of the pre-2.4 live inventory (tree_id_v2) depend on the query area and cannot be '
                                'resolved here')
    e, n = int(m.group(1)), int(m.group(2))
    if not (40_000_000 <= e <= 50_000_000 and 25_000_000 <= n <= 30_000_000):
        raise pf.FlagError(400, 'tree_id lies outside Austria')
    return e, n


def prefill(body: dict) -> None:
    """A report that names its tree by ``tree_id`` / ``client_id`` needs no point:
    fill lon / lat from the id (tree_id always wins over a sent point)."""
    if not isinstance(body, dict):
        return
    tid = parse_tree_id(body.get('tree_id'))
    if tid is not None:
        lon, lat = _tf('EPSG:3035', 'EPSG:4326').transform(tid[0] / 10.0, tid[1] / 10.0)
        body['lon'], body['lat'] = float(lon), float(lat)
    elif body.get('client_id') not in (None, '') and (body.get('lon') is None or body.get('lat') is None):
        t = _tree_by_cid(_conn(), parse_client_id(body['client_id']))
        if t is None:
            raise pf.FlagError(404, 'unknown client_id — send lon, lat with the first report of an added tree')
        st = state(t[0]); j = _state_idx(st, t[1], t[2])
        if j is not None:
            body['lon'], body['lat'] = float(st['lon'][j]), float(st['lat'][j])


def parse_client_id(v) -> str | None:
    if v in (None, ''):
        return None
    v = str(v).strip().lower()
    if not _CLIENT_ID.match(v):
        raise pf.FlagError(400, 'client_id must be a UUID (8–64 chars of A-Z a-z 0-9 . _ : -)')
    return v


def _tree_by_cid(c, cid):
    if not cid:
        return None
    r = c.execute('SELECT kg, x, y FROM tree_ver WHERE cid=? ORDER BY rev LIMIT 1', (cid,)).fetchone()
    return (int(r['kg']), int(r['x']), int(r['y'])) if r else None


def _state_idx(st, x, y):
    if not st['n']:
        return None
    j = np.nonzero(st['key'] == ((int(x) << 32) | int(y)))[0]
    return int(j[0]) if j.size else None


def state(kg, grev: int | None = None) -> dict:
    """Current per-tree answers of one KG.  ``grev`` = global revision the
    caller already looked up (one query per request, not per KG)."""
    try:
        kg = int(kg)
    except (TypeError, ValueError):
        return _EMPTY
    if grev is None:
        grev = global_rev()
    if not grev:
        return _EMPTY
    st = _state.get(kg)
    if st is not None and st.get('grev') == grev and st.get('path') == str(pf.DB_PATH):
        return st
    try:
        c = _conn()
        if st is not None and st.get('path') == str(pf.DB_PATH):
            kr = c.execute('SELECT COALESCE(MAX(rev), 0) FROM tree_ver WHERE kg=?', (kg,)).fetchone()[0]
            if kr == st['rev']:
                st['grev'] = grev
                return st
        st = _load(c, kg)
    except Exception as e:  # noqa: BLE001
        log.warning('tree_user state %s: %s', kg, e)
        return _EMPTY
    st['grev'] = grev
    with _state_lock:
        if len(_state) > 8000:
            _state.clear()
        _state[kg] = st
    return st


def kgs_rev(kgs) -> int:
    """Newest revision touching any of ``kgs`` (0 = none) — ETag component."""
    g = global_rev()
    if not g:
        return 0
    return max([state(k, g)['rev'] for k in kgs] or [0])


def _taxon(i) -> str | None:
    return pf._dict_val(int(i)) if i else None


# === SECTION: read side for /trees/bbox ===

def annotate(x: np.ndarray, y: np.ndarray, h: np.ndarray, kgs) -> dict | None:
    """Sparse user block for an apex result set (EPSG:3035 metre columns):
    ``{i, taxon, n, v}`` — row indices of apices people named, the winning
    taxon, how many reporters agree, the tree's version count.  None when
    nothing in these KGs has been named."""
    g = global_rev()
    if not g or x is None or not len(x):
        return None
    sts = [s for s in (state(k, g) for k in kgs) if s['n']]
    if not sts:
        return None
    key = (x.astype(np.int64) << 32) | y.astype(np.int64)
    out_i: list = []; out_sp: list = []; out_n: list = []; out_v: list = []; out_h: list = []; out_d: list = []
    out_t: list = []

    def take(k, j):
        out_i.append(k); out_sp.append(int(ssp[j])); out_n.append(int(scn[j])); out_v.append(int(sv[j]))
        out_h.append(_num(suh[j])); out_d.append(_num(sdb[j])); out_t.append(stid[j])

    for s in sts:
        m = s['live'] & ~s['added']
        if not m.any():
            continue
        sk = s['key'][m]; ssp = s['sp'][m]; scn = s['cnt'][m]; sv = s['v'][m]; suh = s['h'][m]; sdb = s['dbh'][m]
        stid = [t for t, k_ in zip(s['tid'], m.tolist()) if k_]
        order = np.argsort(key, kind='stable')
        pos = np.searchsorted(key[order], sk)
        pos[pos >= key.size] = 0
        hit = key[order][pos] == sk
        for j in np.nonzero(hit)[0].tolist():
            take(int(order[pos[j]]), j)
        miss = np.nonzero(~hit)[0]
        if miss.size and miss.size <= 500:      # apex moved by a rebuild: same spot ± 1.5 m and same height
            sx = s['x'][m]; sy = s['y'][m]; sh = s['apex_h'][m]
            for j in miss.tolist():
                if not np.isfinite(sh[j]):
                    continue
                d2 = (x - sx[j]).astype(np.float64) ** 2 + (y - sy[j]).astype(np.float64) ** 2
                k = int(np.argmin(d2))
                if d2[k] <= ORPHAN_M ** 2 and abs(float(h[k]) - float(sh[j])) <= ORPHAN_DH_M:
                    take(k, j)
    if not out_i:
        return None
    o = sorted(range(len(out_i)), key=out_i.__getitem__)
    seen = set(); keep = [j for j in o if not (out_i[j] in seen or seen.add(out_i[j]))]
    u = {'i': [out_i[j] for j in keep], 'taxon': [_taxon(out_sp[j]) for j in keep],
         'n': [out_n[j] for j in keep], 'v': [out_v[j] for j in keep]}
    for name, col in (('h_user_m', out_h), ('dbh_cm', out_d), ('tree_id', out_t)):   # only when any is known
        if any(col[j] is not None for j in keep):
            u[name] = [col[j] for j in keep]
    return u


def _num(v) -> float | None:
    return round(float(v), 1) if np.isfinite(v) else None


def reported(w, s, e, n, kgs, min_h: float = 0.0, max_h: float | None = None) -> dict | None:
    """Inventory trees somebody reported by ``tree_id`` that the corpus has no
    apex for (trees only the live fused inventory of ``/api/v3/trees`` finds):
    ``{tree_id, lon, lat, h_m, taxon, n, v[, dbh_cm]}``, same height window rule
    as :func:`added`."""
    return added(w, s, e, n, kgs, min_h, max_h, which='idonly')


def added(w, s, e, n, kgs, min_h: float = 0.0, max_h: float | None = None, which: str = 'added') -> dict | None:
    """User-added trees (ones the corpus has no apex for) in a bbox as columns
    ``{lon, lat, h_m, taxon, n, v}``.  Height window: the reported height, or
    3 m (the inventory floor) when none was given — so exactly one band of a
    partition carries each tree."""
    g = global_rev()
    if not g:
        return None
    out = {'lon': [], 'lat': [], 'h_m': [], 'taxon': [], 'n': [], 'v': [], 'dbh_cm': [], 'client_id': [], 'tree_id': []}
    seen = set()
    for k in kgs:
        st = state(k, g)
        if not st['n']:
            continue
        hw = np.where(np.isfinite(st['h']), st['h'], ADDED_DEFAULT_H)
        m = st[which] & st['live'] & (st['lon'] >= w) & (st['lon'] <= e) & (st['lat'] >= s) & (st['lat'] <= n) & (hw >= min_h)
        if max_h is not None:
            m &= hw < max_h
        for j in np.nonzero(m)[0].tolist():
            kk = int(st['key'][j])
            if kk in seen:
                continue
            seen.add(kk)
            out['lon'].append(round(float(st['lon'][j]), 6)); out['lat'].append(round(float(st['lat'][j]), 6))
            out['h_m'].append(_num(st['h'][j])); out['dbh_cm'].append(_num(st['dbh'][j]))
            out['taxon'].append(_taxon(st['sp'][j])); out['n'].append(int(st['cnt'][j])); out['v'].append(int(st['v'][j]))
            out['client_id'].append(st['cid'][j]); out['tree_id'].append(st['tid'][j] if which == 'idonly' else None)
    if not out['lon']:
        return None
    for k in ('dbh_cm', 'client_id', 'tree_id'):
        if all(v is None for v in out[k]):
            del out[k]
    return out


# === SECTION: write (called from public_flags.record) ===

def _near_kgs(lon, lat, r_m=SEARCH_M) -> list:
    dlat = r_m / 110_540.0; dlon = r_m / (111_320.0 * math.cos(math.radians(lat)))
    try:
        import cells_api as ca
        return list(ca._kgs_intersecting(lon - dlon, lat - dlat, lon + dlon, lat + dlat))
    except Exception:  # noqa: BLE001
        return []


def _apices_near(lon, lat, r_m=SEARCH_M) -> dict | None:
    dlat = r_m / 110_540.0; dlon = r_m / (111_320.0 * math.cos(math.radians(lat)))
    try:
        import ne_trees
        r = ne_trees.collect(lon - dlon, lat - dlat, lon + dlon, lat + dlat, 'trees', realistic=False, limit=2000)
        return r['cols'] if r['n'] else None
    except Exception as e:  # noqa: BLE001
        log.debug('tree_user apices near %.5f,%.5f: %s', lon, lat, e)
        return None


def _reach(crown_m2) -> np.ndarray:
    return np.maximum(MATCH_MIN_M, MATCH_CROWN_FRAC * np.sqrt(np.maximum(crown_m2, 0) / math.pi))


def _candidates(lon, lat, px, py) -> list:
    """Every tree within SEARCH_M of the point, nearest first:
    ``{kind: apex | id | added, x, y, kg, d, h, reach, tid, live}``.
    apex = measured apex of the corpus; id = an inventory tree somebody
    reported by tree_id that the corpus has no apex for; added = user-added."""
    out = []
    seen = set()
    g = global_rev()
    sts = {}
    for k in (_near_kgs(lon, lat) if g else ()):
        st = state(k, g)
        if st['n']:
            sts[int(k)] = st
    c = _apices_near(lon, lat)
    if c is not None:
        d = np.hypot(c['x'] - px, c['y'] - py)
        rad = _reach(c['crown_m2'])
        for j in np.argsort(d).tolist():
            kg = int(c['kg'][j]); x = int(c['x'][j]); y = int(c['y'][j])
            st = sts.get(kg); i = _state_idx(st, x, y) if st else None
            tid = st['tid'][i] if i is not None else None
            dj = float(d[j])
            if tid is not None:                       # exact position known → measure against it
                dj = float(math.hypot(st['e'][i] - px, st['n_'][i] - py))
            seen.add((x, y))
            out.append({'kind': 'apex', 'x': x, 'y': y, 'kg': kg, 'd': dj, 'h': float(c['h'][j]),
                        'reach': float(rad[j]), 'tid': tid, 'live': bool(i is not None and st['live'][i]),
                        'lon': float(c['lon'][j]) if tid is None else float(st['lon'][i]),
                        'lat': float(c['lat'][j]) if tid is None else float(st['lat'][i])})
    for kg, st in sts.items():
        d = np.hypot(st['e'] - px, st['n_'] - py)
        for i in np.nonzero(d <= SEARCH_M)[0].tolist():
            x, y = int(st['x'][i]), int(st['y'][i])
            if st['added'][i]:
                kind, reach = 'added', ADDED_NAME_M
            elif st['idonly'][i] and (x, y) not in seen:
                kind, reach = 'id', MATCH_MIN_M
            else:
                continue
            out.append({'kind': kind, 'x': x, 'y': y, 'kg': kg, 'd': float(d[i]), 'h': None, 'reach': reach,
                        'tid': st['tid'][i] if kind == 'id' else None, 'cid': st['cid'][i],
                        'live': bool(st['live'][i]), 'lon': float(st['lon'][i]), 'lat': float(st['lat'][i])})
    out.sort(key=lambda t: t['d'])
    return out


def _cand_public(cs, n=5) -> list:
    out = []
    for t in cs[:n]:
        d = {'tree_id': t.get('tid'), 'lon': round(t['lon'], 6), 'lat': round(t['lat'], 6),
             'height_m': None if t['h'] is None else round(t['h'], 2), 'dist_m': round(t['d'], 1),
             'kind': t['kind']}
        if t.get('cid'):
            d['client_id'] = t['cid']
        out.append(d)
    return out


def _pick(cs: list):
    """The tree a point means: a measured tree within reach wins over an added
    one; the nearest must be clearly nearer than the next (409 otherwise)."""
    for kinds in (('apex', 'id'), ('added',)):
        hit = [t for t in cs if t['kind'] in kinds and t['d'] <= t['reach']]
        if not hit:
            continue
        a = hit[0]
        if len(hit) > 1 and a['d'] > MATCH_SURE_M and hit[1]['d'] < MATCH_AMBIG_RATIO * a['d']:
            raise pf.FlagError(409, 'several trees are within reach of this point — send tree_id (or the exact '
                                    'coordinate of one candidate)', candidates=_cand_public(hit))
        return a
    return None


def _bind_id(c, tid, lon, lat, subj_kg):
    """tree_id → the tree it names, never an added one.  Order: a tree already
    carrying this id; the corpus apex this inventory apex was snapped to
    (≤ 0.5 m per axis, not owned by another id); else a tree of its own
    (the corpus has no apex for it) — keyed in the transaction."""
    e, n = tid[0] / 10.0, tid[1] / 10.0
    fx, fy = int(math.floor(e)), int(math.floor(n))
    kgs = [int(k) for k in _near_kgs(lon, lat, 3.0)]
    for k in kgs:
        r = c.execute('SELECT x, y, h_dm FROM tree_ver WHERE kg=? AND x BETWEEN ? AND ? AND y BETWEEN ? AND ? '
                      'AND e_dm=? AND n_dm=? AND op IN (0, 2) ORDER BY rev DESC LIMIT 1',
                      (k, fx - 3, fx + 4, fy - 3, fy + 4, tid[0], tid[1])).fetchone()
        if r is not None:
            return {'kind': 'apex' if r['h_dm'] is not None else 'id', 'x': int(r['x']), 'y': int(r['y']), 'kg': k,
                    'd': 0.0, 'h': None if r['h_dm'] is None else r['h_dm'] / 10.0}
    ap = _apices_near(lon, lat, 3.0)
    if ap is not None:
        want = tree_id_str(tid)
        # 1) the metre snap of this very pixel; 2) one pixel off (a product stitched a tile a pixel
        #    aside — 63350-north, 2026-11) when exactly one apex qualifies
        for tol, single in ((ID_SNAP_M, False), (ID_SHIFT_M, True)):
            ok = (np.abs(ap['x'] - e) <= tol + 1e-6) & (np.abs(ap['y'] - n) <= tol + 1e-6)
            free = []
            for j in sorted(np.nonzero(ok)[0].tolist(), key=lambda j: (int(ap['x'][j]), int(ap['y'][j]))):
                kg = int(ap['kg'][j]); x = int(ap['x'][j]); y = int(ap['y'][j])
                st = state(kg); i = _state_idx(st, x, y)
                if i is not None and (st['added'][i] or st['tid'][i] not in (None, want)):
                    continue                              # that apex already is another inventory tree
                free.append({'kind': 'apex', 'x': x, 'y': y, 'kg': kg, 'd': 0.0, 'h': float(ap['h'][j])})
            if free and (len(free) == 1 or not single):
                return free[0]
    if not subj_kg:
        raise pf.FlagError(400, 'tree_id lies in no KG we know')
    return {'kind': 'id', 'x': fx, 'y': fy, 'kg': int(subj_kg), 'd': 0.0, 'h': None, 'new': True}


def _free_key(c, kg, x, y):
    """Nearest unused integer-metre key (two trees closer than a metre must
    not share one; the exact position lives in e_dm / n_dm)."""
    for r in range(0, 4):
        for dx in range(-r, r + 1):
            for dy in range(-r, r + 1):
                if max(abs(dx), abs(dy)) != r:
                    continue
                if not c.execute('SELECT 1 FROM tree_ver WHERE kg=? AND x=? AND y=? LIMIT 1', (kg, x + dx, y + dy)).fetchone():
                    return x + dx, y + dy
    raise pf.FlagError(409, 'too many trees reported on this spot')


def _free_taxa_ok(c, latin: str) -> bool:
    if ('taxon', latin) in pf._DICT:
        return True
    n = c.execute("SELECT COUNT(*) FROM dict WHERE kind='taxon'").fetchone()[0]
    return n < MAX_FREE_TAXA + len(TAXA)


def _truthy(v) -> bool:
    return v is True or str(v).strip().lower() in ('1', 'true', 'yes')


def _metadata(body, exp, warnings: list):
    """→ (packed meta | None, measured_at epoch | None).  Unknown values are a
    warning, never an error (the measure itself must not be lost)."""
    meta = 0
    for i, (name, vals) in enumerate(META_FIELDS):
        v = next((d[name] for d in (body, exp) if d.get(name) not in (None, '')), None)
        if v is None:
            continue
        s = _fold(str(v)).replace(' ', '_').replace('-', '_')
        s = _META_ALIAS[name].get(s, s)
        if s in vals:
            meta |= (vals.index(s) + 1) << (4 * i)
        else:
            warnings.append(f'{name} {str(v)[:30]!r} not understood (use {" | ".join(vals)}) — not stored')
    mts = None
    v = next((d['measured_at'] for d in (body, exp) if d.get('measured_at') not in (None, '')), None)
    if v is not None:
        try:
            import datetime as dt
            if isinstance(v, (int, float)):
                t = float(v)
            else:
                d = dt.datetime.fromisoformat(str(v).strip().replace('Z', '+00:00'))
                if d.tzinfo is None:
                    d = d.replace(tzinfo=dt.timezone.utc)
                t = d.timestamp()
            if not (946684800 <= t <= time.time() + 86400):
                raise ValueError
            mts = int(t)
        except (ValueError, OverflowError):
            warnings.append('measured_at not understood (ISO 8601, e.g. "2026-11-11T09:30:00Z", 2000 … now) — not stored')
    return (meta or None), mts


def _meta_public(meta, mts, v: dict) -> None:
    if mts is not None:
        v['measured_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(mts))
    if meta:
        for i, (name, vals) in enumerate(META_FIELDS):
            k = (meta >> (4 * i)) & 15
            if 0 < k <= len(vals):
                v[name] = vals[k - 1]


def record(body: dict, subj: dict, claim: str, rep: bytes, source_id, epoch: str | None, org_id=None) -> dict:
    """One tree report → appended version (or a no-op when the reporter repeats
    themself).  Returns the tree's current public answer.  Raises FlagError
    (404 no tree, 409 ambiguous / exists) BEFORE anything is written."""
    exp = body.get('expected') if isinstance(body.get('expected'), dict) else {}
    has_sp = any(k in d for d in (body, exp) for k in ('species', 'taxon'))
    raw = next((d[k] for d in (body, exp) for k in ('species', 'taxon') if d.get(k) not in (None, '')), None)
    uh_dm = _measure(body, exp, ('h_m', 'height_m'), H_M, 10, 'h_m', 'm')
    dbh_mm = _measure(body, exp, ('dbh_cm', 'dbh'), DBH_CM, 10, 'dbh_cm', 'cm')
    warnings: list = []
    meta, mts = _metadata(body, exp, warnings)
    tid = parse_tree_id(body.get('tree_id'))
    cid = parse_client_id(body.get('client_id'))
    if body.get('tree_id_v2') not in (None, '') and tid is None:
        warnings.append('tree_id_v2 ignored: pre-2.4 ids depend on the query area — matched by position instead; '
                        'send tree_id (POST /api/v3/trees, legacy_ids=1 maps old → new)')
    lon, lat = subj['lon'], subj['lat']
    px, py = _to_3035(lon, lat)
    latin = known = None
    if raw is not None:
        latin, known = normalise(raw)
    empty = latin is None and uh_dm is None and dbh_mm is None
    retract = _truthy(body.get('retract')) or (empty and has_sp)
    c = _conn()
    e_dm = n_dm = None
    new_tree = False
    if claim == 'species':
        if tid is not None:
            m = _bind_id(c, tid, lon, lat, subj.get('kg'))
            e_dm, n_dm = tid
            new_tree = bool(m.get('new'))
        elif cid is not None:
            t = _tree_by_cid(c, cid)
            if t is None:
                raise pf.FlagError(404, 'unknown client_id — add the tree first (claim "missing")',
                                   hint={'layer': 'trees', 'claim': 'missing'})
            m = {'kind': 'added', 'kg': t[0], 'x': t[1], 'y': t[2], 'd': 0.0, 'h': None}
        else:
            cs = _candidates(lon, lat, px, py)
            m = None
            if retract:                             # several added trees on one spot: withdraw my own
                top = next((t for t in cs if t['kind'] in ('apex', 'id') and t['d'] <= t['reach']), None)
                if top is None:
                    m = next((t for t in cs if t['kind'] == 'added' and t['d'] <= ADDED_NAME_M
                              and _mine_ok(c, t, rep, None)), None)
            m = m or _pick(cs)
            if m is None:
                raise pf.FlagError(404, f'no tree within reach of this point (max({MATCH_MIN_M:g} m, '
                                        f'{MATCH_CROWN_FRAC:g} × crown radius) of an apex, {ADDED_NAME_M:g} m of an added '
                                        'tree). Send tree_id, or add the tree: claim "missing" with the same point.',
                                   hint={'layer': 'trees', 'claim': 'missing'}, candidates=_cand_public(cs))
        if empty and not retract and meta is None and mts is None:
            raise pf.FlagError(400, 'say something about the tree: species, dbh_cm and / or h_m '
                                    '("species": null or "retract": true withdraws your report)', dict='/api/v1/trees/taxa/dict')
        if retract:
            op, h_dm = OP_RETRACT, None
        else:
            op, h_dm = OP_SPECIES, (None if m['h'] is None else int(round(m['h'] * 10)))
    else:                                           # missing → add (or withdraw my add)
        cs = _candidates(lon, lat, px, py)
        m = None
        t = _tree_by_cid(c, cid)
        if t is not None:
            m = {'kind': 'added', 'kg': t[0], 'x': t[1], 'y': t[2], 'd': 0.0, 'h': None}
        elif retract or cid is None:
            # my own added tree at this spot (never somebody else's, never one with other measures)
            lim = ADDED_NAME_M if retract else ADDED_FOLD_M
            m = next((t for t in cs if t['kind'] == 'added' and t['d'] <= lim
                      and _mine_ok(c, t, rep, None if retract else (uh_dm, dbh_mm))), None)
        if retract:
            if m is None:
                raise pf.FlagError(404, 'no tree added by you at this point (send client_id, or the point you added it at)')
            op, h_dm = OP_RETRACT, None
        else:
            if m is None:
                near = next((t for t in cs if t['kind'] in ('apex', 'id') and t['d'] <= EXISTS_M), None)
                if near is not None and not (_truthy(body.get('force')) or near['live']):
                    raise pf.FlagError(409, f'we already have a tree {near["d"]:.1f} m from here — describe it with claim '
                                            '"tree" (tree_id / its coordinate), or send "force": true for a second '
                                            'stem / understorey tree',
                                       tree=_cand_public([near])[0], hint={'layer': 'trees', 'claim': 'tree', 'force': True})
                if not subj.get('kg'):
                    raise pf.FlagError(400, 'point lies in no KG we know')
                m = {'kind': 'added', 'x': int(round(px)), 'y': int(round(py)), 'kg': int(subj['kg']), 'd': 0.0, 'h': None}
                new_tree = True
                e_dm, n_dm = int(round(px * 10)), int(round(py * 10))
            op, h_dm = OP_ADD, None
    kind, x, y, kg, dist, apex_h = m['kind'], m['x'], m['y'], m['kg'], m['d'], m['h']
    if latin is not None and not known and not _free_taxa_ok(c, latin):
        raise pf.FlagError(400, 'too many free-form species names; use one from the dictionary', dict='/api/v1/trees/taxa/dict')
    sp = pf._dict_id(c, 'taxon', latin) if latin else None
    ep = pf._dict_id(c, 'epoch', epoch) if epoch else None
    now = int(time.time())
    written = False
    with pf._LOCK:
        c.execute('BEGIN IMMEDIATE')
        try:
            if new_tree:
                if cid is not None and _tree_by_cid(c, cid) is not None:      # lost a race on the same client_id
                    kg, x, y = _tree_by_cid(c, cid); new_tree = False
                else:
                    x, y = _free_key(c, kg, x, y)
            rows = c.execute('SELECT rev, op, sp, uh_dm, dbh_mm, mts, meta, reporter, ref FROM tree_ver '
                             'WHERE kg=? AND x=? AND y=? ORDER BY rev', (kg, x, y)).fetchall()
            hidden = {r['ref'] for r in rows if r['op'] == OP_MODERATE}
            mine = [r for r in rows if bytes(r['reporter']) == rep and r['op'] != OP_MODERATE and r['rev'] not in hidden]
            since = 0                               # my versions after my last retraction
            for i, r in enumerate(mine):
                if r['op'] == OP_RETRACT:
                    since = i + 1
            cur = mine[since:]

            def latest(col):
                return next((r[col] for r in reversed(cur) if r[col] is not None), None)

            def meta_same():
                if meta is None:
                    return True
                for i in range(len(META_FIELDS)):
                    k = (meta >> (4 * i)) & 15
                    if k and k != next(((r['meta'] >> (4 * i)) & 15 for r in reversed(cur)
                                        if r['meta'] and (r['meta'] >> (4 * i)) & 15), 0):
                        return False
                return True

            if op == OP_RETRACT:
                same = not cur
                if same and not mine:
                    # never reported on this tree: say so (403) instead of a silent no-op.  The reporter hash
                    # is reporter id + key (or network), so a different / absent "reporter" lands here too.
                    raise pf.FlagError(403, 'nothing of yours on this tree — only your own reports can be withdrawn '
                                            '(same "reporter" and reporter key as the report)',
                                       reason='not_yours', tree=tree_at(kg, x, y))
                if same:
                    warnings.append('already withdrawn — nothing of yours left on this tree')
            else:
                same = bool(cur) and meta_same() and all(
                    v is None or v == latest(col)
                    for col, v in (('sp', sp), ('uh_dm', uh_dm), ('dbh_mm', dbh_mm), ('mts', mts)))
                if op == OP_ADD and not any(r['op'] == OP_ADD for r in cur):
                    same = False
            if not same:
                if len(rows) >= MAX_VERSIONS_PER_TREE:
                    raise pf.FlagError(429, f'this tree already has {MAX_VERSIONS_PER_TREE} versions')
                if c.execute('SELECT COUNT(*) FROM tree_ver').fetchone()[0] >= MAX_ROWS:
                    raise pf.FlagError(507, 'tree store full')
                c.execute('INSERT INTO tree_ver(ts, x, y, kg, op, sp, h_dm, reporter, source, epoch, uh_dm, dbh_mm, '
                          'e_dm, n_dm, cid, mts, meta, org) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                          (now, x, y, kg, op, sp, h_dm, rep, source_id, ep, uh_dm, dbh_mm, e_dm, n_dm,
                           cid if op == OP_ADD else None, mts, meta, org_id))
                written = True
            c.execute('COMMIT')
        except Exception:
            c.execute('ROLLBACK')
            raise
    out = tree_at(kg, x, y) or {}
    # folded: nothing new was created — a repeat (no version written) or, for an add, the report landed on
    # a tree that already existed.  written: a version was appended.
    out.update(folded=(not written) or (op == OP_ADD and not new_tree), written=written,
               op=OP_NAMES[op], matched=kind, dist_m=round(dist, 1))
    if tid is not None:
        out['matched_by'] = 'tree_id'
        if kind == 'id':
            warnings.append('this tree_id has no apex on /api/v1/trees/bbox (cell outside the published set); the report '
                            'is kept under the id and served by /api/v1/trees/taxa')
    elif cid is not None and kind == 'added':
        out['matched_by'] = 'client_id'
    if apex_h is not None:
        out['height_m'] = round(apex_h, 2)
    if latin is not None and not known:
        warnings.append('species not in our dictionary — stored as written; check /api/v1/trees/taxa/dict')
    if warnings:
        out['warning'] = '; '.join(warnings)
    return out


def _mine_ok(c, t, rep: bytes, measures) -> bool:
    """Did this reporter add tree ``t`` (and, for a fold, without measures that
    contradict the new ones)?"""
    rows = c.execute('SELECT rev, op, uh_dm, dbh_mm, reporter, ref FROM tree_ver WHERE kg=? AND x=? AND y=? ORDER BY rev',
                     (t['kg'], t['x'], t['y'])).fetchall()
    hidden = {r['ref'] for r in rows if r['op'] == OP_MODERATE}
    live = [r for r in rows if r['op'] != OP_MODERATE and r['rev'] not in hidden]
    mine = False
    for r in live:
        if bytes(r['reporter']) == rep:
            if r['op'] == OP_ADD:
                mine = True
            elif r['op'] == OP_RETRACT:
                mine = False
    if not mine:
        return False
    if measures is None:
        return True
    for col, v in zip(('uh_dm', 'dbh_mm'), measures):
        if v is not None:
            have = {r[col] for r in live if r[col] is not None}
            if have and have != {v}:
                return False
    return True


def _measure(body, exp, keys, rng, scale, name, unit) -> int | None:
    v = next((d[k] for d in (body, exp) for k in keys if d.get(k) not in (None, '')), None)
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        raise pf.FlagError(400, f'{name} must be a number ({unit})')
    if not (rng[0] <= v <= rng[1]):
        raise pf.FlagError(400, f'{name} must be {rng[0]:g}–{rng[1]:g} {unit}')
    return int(round(v * scale))


def tree_at(kg: int, x: int, y: int) -> dict | None:
    st = state(kg)
    if not st['n']:
        return None
    j = np.nonzero(st['key'] == ((int(x) << 32) | int(y)))[0]
    return _public(st, int(j[0])) if j.size else None


def _public(st: dict, j: int) -> dict:
    t = _taxon(st['sp'][j])
    d = {'lon': round(float(st['lon'][j]), 6), 'lat': round(float(st['lat'][j]), 6), 'taxon': t,
         'class': taxon_class(t), 'n': int(st['cnt'][j]), 'v': int(st['v'][j]), 'rev': int(st['trev'][j]),
         'added': bool(st['added'][j])}
    if np.isfinite(st['h'][j]):
        d['h_m'] = round(float(st['h'][j]), 1)          # reporters' own height (median), not the LiDAR one
    if np.isfinite(st['dbh'][j]):
        d['dbh_cm'] = round(float(st['dbh'][j]), 1)
    if st['tid'][j] and not st['added'][j]:
        d['tree_id'] = st['tid'][j]
    if st['cid'][j]:
        d['client_id'] = st['cid'][j]
    return d


def moderate(rev: int) -> dict | None:
    """Hide one version (append-only: a moderation row referencing it)."""
    c = _conn()
    with pf._LOCK:
        r = c.execute('SELECT x, y, kg, op FROM tree_ver WHERE rev=?', (rev,)).fetchone()
        if r is None or r['op'] == OP_MODERATE:
            return None
        if not c.execute('SELECT 1 FROM tree_ver WHERE op=3 AND ref=? AND kg=? AND x=? AND y=?',
                         (rev, r['kg'], r['x'], r['y'])).fetchone():
            c.execute('INSERT INTO tree_ver(ts, x, y, kg, op, reporter, ref) VALUES (?,?,?,?,3,?,?)',
                      (int(time.time()), r['x'], r['y'], r['kg'], b'', rev))
    return tree_at(r['kg'], r['x'], r['y'])


# === SECTION: listing / history / stats ===

LIST_FIELDS = ('lon', 'lat', 'taxon', 'n', 'v', 'rev', 'added', 'h_m', 'dbh_cm', 'tree_id', 'client_id')


def listing(bbox=None, kg=None, since: int = 0, limit: int = LIST_LIMIT) -> dict:
    """Current answers as columns, oldest change first.  ``since`` = only trees
    changed after that revision (a tree that lost its last live version comes
    back once with taxon null, added 0 → drop it)."""
    g = global_rev()
    cols: dict = {f: [] for f in LIST_FIELDS}
    out = {'rev': g, 'since': since, 'fields': list(LIST_FIELDS), 'order': 'rev_asc', 'columns': cols,
           'count': 0, 'truncated': False}
    if not g or since >= g:
        return out
    if kg is not None:
        kgs = [int(kg)]
    elif bbox is not None:
        import cells_api as ca
        kgs = [int(k) for k in ca._kgs_intersecting(*bbox)]
    else:
        kgs = [r[0] for r in _conn().execute('SELECT DISTINCT kg FROM tree_ver WHERE rev > ?', (since,))]
    rows = []
    for k in kgs:
        st = state(k, g)
        if not st['n']:
            continue
        m = st['trev'] > since
        if not since:
            m &= st['live']
        if bbox is not None:
            w, s, e, n = bbox
            m &= (st['lon'] >= w) & (st['lon'] <= e) & (st['lat'] >= s) & (st['lat'] <= n)
        rows.extend((int(st['trev'][j]), st, j) for j in np.nonzero(m)[0].tolist())
    rows.sort(key=lambda t: t[0])
    if len(rows) > limit:
        rows = rows[:limit]; out['truncated'] = True
        out['next_since'] = rows[-1][0]
    for _, st, j in rows:
        cols['lon'].append(round(float(st['lon'][j]), 6)); cols['lat'].append(round(float(st['lat'][j]), 6))
        cols['taxon'].append(_taxon(st['sp'][j])); cols['n'].append(int(st['cnt'][j])); cols['v'].append(int(st['v'][j]))
        cols['rev'].append(int(st['trev'][j])); cols['added'].append(int(st['added'][j]))
        cols['h_m'].append(_num(st['h'][j])); cols['dbh_cm'].append(_num(st['dbh'][j]))
        cols['tree_id'].append(None if st['added'][j] else st['tid'][j]); cols['client_id'].append(st['cid'][j])
    out['count'] = len(rows)
    return out


def history(lon: float | None = None, lat: float | None = None, tree_id=None, client_id=None) -> dict | None:
    """Every version of one reported tree: by tree_id, by client_id, or the
    nearest to a point (≤ 5 m)."""
    g = global_rev()
    if not g:
        return None
    best = None
    tid = parse_tree_id(tree_id)
    if tid is not None:
        lon, lat = _tf('EPSG:3035', 'EPSG:4326').transform(tid[0] / 10.0, tid[1] / 10.0)
        want = tree_id_str(tid)
        for k in _near_kgs(lon, lat, 3.0):
            st = state(k, g)
            if st['n'] and want in st['tid']:
                j = st['tid'].index(want)
                best = (0.0, st, j, int(k))
                break
    elif client_id not in (None, ''):
        t = _tree_by_cid(_conn(), parse_client_id(client_id))
        if t is not None:
            st = state(t[0], g); j = _state_idx(st, t[1], t[2])
            if j is not None:
                best = (0.0, st, j, t[0])
    else:
        px, py = _to_3035(lon, lat)
        for k in _near_kgs(lon, lat):
            st = state(k, g)
            if not st['n']:
                continue
            d = np.hypot(st['e'] - px, st['n_'] - py)
            j = int(np.argmin(d))
            if d[j] <= 5.0 and (best is None or d[j] < best[0]):
                best = (float(d[j]), st, j, int(k))
    if best is None:
        return None
    d, st, j, kg = best
    rows = _conn().execute('SELECT rev, ts, op, sp, uh_dm, dbh_mm, source, epoch, ref, e_dm, n_dm, mts, meta, org FROM tree_ver '
                           'WHERE kg=? AND x=? AND y=? ORDER BY rev', (kg, int(st['x'][j]), int(st['y'][j]))).fetchall()
    hidden = {r['ref'] for r in rows if r['op'] == OP_MODERATE}
    vs = []
    for i, r in enumerate(rows, 1):
        v = {'v': i, 'rev': r['rev'], 'ts': r['ts'], 'op': OP_NAMES[r['op']]}
        if r['sp'] is not None:
            v['taxon'] = _taxon(r['sp'])
        if r['uh_dm'] is not None:
            v['h_m'] = r['uh_dm'] / 10.0
        if r['dbh_mm'] is not None:
            v['dbh_cm'] = r['dbh_mm'] / 10.0
        _meta_public(r['meta'], r['mts'], v)
        if r['e_dm'] is not None and r['op'] in (OP_SPECIES, OP_RETRACT):
            v['tree_id'] = tree_id_str((r['e_dm'], r['n_dm']))
        if r['source'] is not None:
            v['source'] = pf._dict_val(r['source'])
        if r['org'] is not None:
            v['by'] = pf._dict_val(r['org'])
        if r['epoch'] is not None:
            v['epoch'] = pf._dict_val(r['epoch'])
        if r['op'] == OP_MODERATE:
            v['hides_rev'] = r['ref']
        elif r['rev'] in hidden:
            v['hidden'] = True
        vs.append(v)
    return {'tree': _public(st, j), 'kg': f'{kg:05d}', 'dist_m': round(d, 1), 'versions': vs}


def taxa_dict() -> dict:
    return {**tt.as_dict(),
            'accepts': 'in this order: any taxon / de / en / codes / alt value (case, diacritics, ß/ss, ä/ae, ×/x, '
                       'hyphens, dots and spaces do not matter); a Latin abbreviation matching exactly one species '
                       '("P. abies", "Pic. abi.", "PiAb", "PicAbi", "PiceAbie"); a well-formed scientific binomial '
                       'not listed (stored as written, flagged with a warning); else 400 + did_you_mean (≤ 5 taxa)',
            'rank': '1 = most frequent tree in Austria (forest: ÖWI shares; street / park: Vienna Baumkataster '
                    'counts; orchards: Streuobst order); a genus follows its most frequent species, groups last',
            'measures': {'dbh_cm': list(DBH_CM), 'h_m': list(H_M),
                         'note': 'optional on any tree report; several reporters → median; h_m is the reporter\'s '
                                 'height and never replaces the LiDAR height_m'},
            'limits': {'taxon_chars': MAX_TAXON_CHARS, 'versions_per_tree': MAX_VERSIONS_PER_TREE,
                       'match_m': MATCH_MIN_M, 'match_crown_frac': MATCH_CROWN_FRAC, 'match_sure_m': MATCH_SURE_M,
                       'match_ambiguous_ratio': MATCH_AMBIG_RATIO, 'added_name_m': ADDED_NAME_M,
                       'added_fold_m': ADDED_FOLD_M, 'exists_m': EXISTS_M},
            'match': 'tree_id (t_<E>_<N>, EPSG:3035 in 0.1 m) binds exactly; client_id binds an added tree; a bare '
                     'point means the nearest apex within max(match_m, match_crown_frac × crown radius) when the next '
                     'tree in reach is ≥ match_ambiguous_ratio × farther (or the point is ≤ match_sure_m from the '
                     'apex) — else 409 + candidates; measured trees win over added ones',
            'metadata': {name: list(vals) for name, vals in META_FIELDS} | {'measured_at': 'ISO 8601'}}


# === SECTION: what is used where (/api/v1/trees/taxa/top) ===

TOP_MAX_AGE = 60
TOP_MAX_AREA_DEG2 = 0.01      # ~85 km²: larger views get the national class tally (scope "national")
TOP_APEX_CAP = 20000          # tallest apexes counted per bbox (approximate beyond that)
TOP_SAMPLE_CHUNKS = 600       # national class tally = a random sample of corpus chunks, scaled
TOP_NATIONAL_TTL = 7 * 86400
_top_nat: dict = {}
_top_lock = threading.Lock()


def top_named(bbox=None, limit: int = 12) -> list:
    """[[taxon, n trees]] of the current reported answers, most named first (ties by dictionary rank)."""
    g = global_rev()
    if not g:
        return []
    if bbox is not None:
        import cells_api as ca
        kgs = [int(k) for k in ca._kgs_intersecting(*bbox)]
    else:
        kgs = [r[0] for r in _conn().execute('SELECT DISTINCT kg FROM tree_ver')]
    tally: dict = {}
    for k in kgs:
        st = state(k, g)
        if not st['n']:
            continue
        m = st['live'] & (st['sp'] > 0)
        if bbox is not None:
            w, s, e, n = bbox
            m &= (st['lon'] >= w) & (st['lon'] <= e) & (st['lat'] >= s) & (st['lat'] <= n)
        ids, cnt = np.unique(st['sp'][m], return_counts=True)
        for i, c in zip(ids.tolist(), cnt.tolist()):
            tally[i] = tally.get(i, 0) + c
    rows = [(t, c) for t, c in ((_taxon(i), c) for i, c in tally.items()) if t]
    rows.sort(key=lambda r: (-r[1], tt.RANK.get(r[0], 10 ** 6), r[0]))
    return [[t, c] for t, c in rows[:limit]]


def _class_rows(counts) -> list:
    import cells_api as ca
    rows = [(ca.SPECIES[i], int(c)) for i, c in enumerate(counts) if c and i < len(ca.SPECIES) and ca.SPECIES[i] != 'unknown']
    rows.sort(key=lambda r: -r[1])
    return [[k, c] for k, c in rows]


def _national_classes() -> dict:
    """Model-class tally of the whole corpus, estimated from a random sample of
    chunks (ratio estimator).  Kept in process and on disk for a week."""
    import json
    import cells_api as ca
    import ne_trees
    now = time.time()
    if _top_nat.get('ts', 0) > now - TOP_NATIONAL_TTL:
        return _top_nat
    path = pf.DB_PATH.parent / 'tree_class_tally.json'
    with _top_lock:
        try:
            d = json.loads(path.read_text())
            if d.get('ts', 0) > now - TOP_NATIONAL_TTL:
                _top_nat.update(d)
                return _top_nat
        except Exception:  # noqa: BLE001
            pass
        c = ca._idx_conn()
        if c is None:
            return {'classes': [], 'apexes': 0}
        total = c.execute('SELECT COALESCE(SUM(trees), 0) FROM chunk').fetchone()[0]
        rows = c.execute('SELECT r10, kg, off, len, raw_len, cells, trees, bld FROM chunk WHERE trees > 0 '
                         'ORDER BY RANDOM() LIMIT ?', (TOP_SAMPLE_CHUNKS,)).fetchall()
        counts = np.zeros(len(ca.SPECIES), dtype=np.int64); seen = 0; hdrs: dict = {}
        for row in rows:
            r = dict(zip(('r10', 'kg', 'off', 'len', 'raw_len', 'cells', 'trees', 'bld'), row))
            try:
                h = hdrs.get(r['kg']) or hdrs.setdefault(r['kg'], ca.header(r['kg']))
                d = ne_trees._decoded_chunk(r['kg'], r, h, 'trees') if h else None
            except Exception:  # noqa: BLE001
                continue
            if not d or not d['n']:
                continue
            sp = d['species'][d['ok']]
            counts[:] += np.bincount(sp, minlength=len(ca.SPECIES))[:len(ca.SPECIES)]
            seen += int(d['n'])
        scale = (total / seen) if seen else 0.0
        d = {'ts': now, 'apexes': int(total), 'sampled': seen,
             'classes': _class_rows(np.round(counts * scale).astype(np.int64))}
        if seen:
            try:
                tmp = path.with_suffix('.tmp'); tmp.write_text(json.dumps(d)); tmp.replace(path)
            except OSError as e:
                log.warning('tree class tally not saved: %s', e)
            _top_nat.clear(); _top_nat.update(d)
        return d


def top(bbox=None, limit: int = 12) -> dict:
    """What is used here: reader-named taxa and our model classes inside the
    bbox (national without one, or when the bbox is too large to scan)."""
    out = {'rev': tt.REV, 'tree_rev': global_rev(), 'named': top_named(bbox, limit)}
    scope = 'national'
    if bbox is not None and (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) <= TOP_MAX_AREA_DEG2:
        import cells_api as ca
        import ne_trees
        try:
            r = ne_trees.collect(*bbox, kind='trees', min_height=3.0, limit=TOP_APEX_CAP, max_limit=TOP_APEX_CAP)
            sp = r['cols'].get('species') if r['n'] else None
            counts = np.bincount(sp, minlength=len(ca.SPECIES)) if sp is not None else []
            out.update(classes=_class_rows(counts), apexes=int(r['n']), approximate=bool(r['truncated']))
            scope = 'bbox'
        except Exception as e:  # noqa: BLE001
            log.warning('taxa/top classes %s: %s', bbox, e)
    if scope == 'national':
        try:
            d = _national_classes()
        except Exception as e:  # noqa: BLE001
            log.warning('taxa/top national classes: %s', e)
            d = {'classes': [], 'apexes': 0}
        out.update(classes=d['classes'], apexes=d['apexes'], approximate=True)
    out['scope'] = {'named': 'bbox' if bbox is not None else 'national', 'classes': scope}
    return out


def stats() -> dict:
    try:
        c = _conn()
        r = c.execute('SELECT COUNT(*), COUNT(DISTINCT kg), COUNT(DISTINCT reporter), COALESCE(MAX(rev), 0), '
                      'COUNT(DISTINCT x * 4294967296 + y), SUM(op = 1), COUNT(DISTINCT sp), MAX(ts) FROM tree_ver').fetchone()
        return {'versions': r[0], 'kgs': r[1], 'reporters': r[2], 'rev': r[3], 'trees': r[4], 'add_versions': r[5] or 0,
                'taxa': r[6], 'last': r[7]}
    except Exception as e:  # noqa: BLE001
        return {'error': str(e)}


# === SECTION: HTTP (mounted on the flags blueprint) ===

def _meta():
    return {'service': 'srtm-lidar-at', 'docs': '/api/v1/docs/llm.txt#tree-species', 'dict': '/api/v1/trees/taxa/dict'}


@pf.flags_bp.route('/api/v1/trees/taxa')
def taxa_get():
    lim = pf._limited(pf._get_limit)
    if lim is not None:
        return lim
    a = request.args
    try:
        since = int(a.get('since') or 0)
        limit = max(1, min(int(a.get('limit') or LIST_LIMIT), LIST_LIMIT))
    except ValueError:
        raise pf.FlagError(400, 'since / limit must be integers')
    bbox = kg = None
    if a.get('bbox'):
        try:
            bbox = tuple(float(v) for v in a['bbox'].split(','))
            assert len(bbox) == 4 and bbox[0] < bbox[2] and bbox[1] < bbox[3]
        except (ValueError, AssertionError):
            raise pf.FlagError(400, 'bbox must be w,s,e,n')
        if (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) > LIST_MAX_AREA_DEG2:
            raise pf.FlagError(413, f'bbox larger than {LIST_MAX_AREA_DEG2} deg² — omit bbox and page with since=')
    elif a.get('kg'):
        kg = a['kg'].strip().zfill(5)
        if not (kg.isdigit() and len(kg) == 5):
            raise pf.FlagError(400, 'kg must be a 5-digit code')
    out = listing(bbox, kg, since, limit)
    out['meta'] = _meta()
    return pf._resp(out, max_age=TAXA_MAX_AGE)


@pf.flags_bp.route('/api/v1/trees/taxa/history')
def taxa_history():
    lim = pf._limited(pf._get_limit)
    if lim is not None:
        return lim
    a = request.args
    if a.get('tree_id') or a.get('client_id'):
        d = history(tree_id=a.get('tree_id'), client_id=a.get('client_id'))
    else:
        try:
            lon, lat = float(a['lon']), float(a['lat'])
        except (KeyError, ValueError):
            raise pf.FlagError(400, 'tree_id=, client_id= or lon=&lat= required')
        d = history(lon, lat)
    if d is None:
        raise pf.FlagError(404, 'no reported tree with this id / within 5 m')
    d['meta'] = _meta()
    return pf._resp(d, max_age=TAXA_MAX_AGE)


@pf.flags_bp.route('/api/v1/trees/taxa/top')
def taxa_top_get():
    lim = pf._limited(pf._get_limit)
    if lim is not None:
        return lim
    a = request.args
    bbox = None
    if a.get('bbox'):
        try:
            bbox = tuple(float(v) for v in a['bbox'].split(','))
            assert len(bbox) == 4 and bbox[0] < bbox[2] and bbox[1] < bbox[3]
        except (ValueError, AssertionError):
            raise pf.FlagError(400, 'bbox must be w,s,e,n')
    try:
        limit = max(1, min(int(a.get('limit') or 12), 50))
    except ValueError:
        raise pf.FlagError(400, 'limit must be an integer')
    d = top(bbox, limit); d['meta'] = _meta()
    return pf._resp(d, max_age=TOP_MAX_AGE)


@pf.flags_bp.route('/api/v1/trees/taxa/dict')
def taxa_dict_get():
    d = taxa_dict(); d['meta'] = _meta()
    return pf._resp(d, max_age=86400)


@pf.flags_bp.route('/api/v1/trees/taxa/<int:rev>/moderate', methods=['POST'])
def taxa_moderate(rev: int):
    import api_tiers as tiers
    if not tiers.token_valid(tiers.request_token(request)):
        raise pf.FlagError(404, 'not_found')
    d = moderate(rev)
    if d is None and not _conn().execute('SELECT 1 FROM tree_ver WHERE rev=? AND op != 3', (rev,)).fetchone():
        raise pf.FlagError(404, 'no such version')
    return pf._resp({'rev': rev, 'hidden': True, 'tree': d, 'meta': _meta()})
