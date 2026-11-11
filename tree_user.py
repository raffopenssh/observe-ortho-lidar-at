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

import difflib
import logging
import math
import re
import threading
import time

import numpy as np
from flask import request

import public_flags as pf

log = logging.getLogger('tree_user')

OP_SPECIES, OP_ADD, OP_RETRACT, OP_MODERATE = 0, 1, 2, 3
OP_NAMES = ('species', 'add', 'retract', 'moderated')
MATCH_MIN_M = 4.0            # a point this close to an apex always means that tree
MATCH_MAX_M = 10.0           # … or inside its crown radius, capped here
ADDED_MATCH_M = 3.0          # reports this close to a user-added tree fold onto it
EXISTS_M = 2.0               # "missing" this close to a measured apex is refused (it exists)
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
             ('dbh_mm', 'INTEGER'))     # reporter's diameter at breast height (mm)
DBH_CM = (1.0, 600.0)
H_M = (1.0, 70.0)

# (scientific name, German, English, model class of /trees/bbox `species`)
_T = """Picea abies|Fichte|Norway spruce|spruce
Picea|Fichte (Gattung)|spruce|spruce
Picea pungens|Stechfichte|blue spruce|spruce
Picea omorika|Serbische Fichte|Serbian spruce|spruce
Abies alba|Weißtanne|silver fir|fir
Abies|Tanne (Gattung)|fir|fir
Abies nordmanniana|Nordmanntanne|Nordmann fir|fir
Pinus sylvestris|Rotföhre|Scots pine|pine
Pinus nigra|Schwarzföhre|black pine|pine
Pinus cembra|Zirbe|Swiss stone pine|pine
Pinus mugo|Latsche|mountain pine|pine
Pinus strobus|Weymouthskiefer|white pine|pine
Pinus|Föhre (Gattung)|pine|pine
Larix decidua|Lärche|European larch|larch
Larix|Lärche (Gattung)|larch|larch
Pseudotsuga menziesii|Douglasie|Douglas fir|conifer
Taxus baccata|Eibe|yew|conifer
Thuja|Thuje|arborvitae|conifer
Juniperus communis|Wacholder|juniper|conifer
Sequoiadendron giganteum|Mammutbaum|giant sequoia|conifer
Metasequoia glyptostroboides|Urweltmammutbaum|dawn redwood|conifer
Ginkgo biloba|Ginkgo|ginkgo|broadleaf
Fagus sylvatica|Rotbuche|beech|beech
Quercus robur|Stieleiche|pedunculate oak|oak
Quercus petraea|Traubeneiche|sessile oak|oak
Quercus cerris|Zerreiche|Turkey oak|oak
Quercus pubescens|Flaumeiche|downy oak|oak
Quercus rubra|Roteiche|red oak|oak
Quercus|Eiche (Gattung)|oak|oak
Acer pseudoplatanus|Bergahorn|sycamore maple|maple
Acer platanoides|Spitzahorn|Norway maple|maple
Acer campestre|Feldahorn|field maple|maple
Acer|Ahorn (Gattung)|maple|maple
Fraxinus excelsior|Esche|ash|ash
Fraxinus ornus|Blumenesche|manna ash|ash
Betula pendula|Hängebirke|silver birch|birch
Betula pubescens|Moorbirke|downy birch|birch
Betula|Birke (Gattung)|birch|birch
Alnus glutinosa|Schwarzerle|black alder|alder
Alnus incana|Grauerle|grey alder|alder
Alnus alnobetula|Grünerle|green alder|alder
Alnus|Erle (Gattung)|alder|alder
Populus nigra|Schwarzpappel|black poplar|poplar
Populus alba|Silberpappel|white poplar|poplar
Populus tremula|Zitterpappel|aspen|poplar
Populus x canadensis|Hybridpappel|hybrid poplar|poplar
Populus|Pappel (Gattung)|poplar|poplar
Salix alba|Silberweide|white willow|willow
Salix caprea|Salweide|goat willow|willow
Salix babylonica|Trauerweide|weeping willow|willow
Salix|Weide (Gattung)|willow|willow
Malus domestica|Apfel|apple|fruit
Pyrus communis|Birne|pear|fruit
Prunus avium|Vogelkirsche|cherry|fruit
Prunus domestica|Zwetschke|plum|fruit
Prunus armeniaca|Marille|apricot|fruit
Prunus persica|Pfirsich|peach|fruit
Prunus padus|Traubenkirsche|bird cherry|broadleaf
Prunus|Steinobst (Gattung)|stone fruit|fruit
Juglans regia|Walnuss|walnut|fruit
Cydonia oblonga|Quitte|quince|fruit
Morus|Maulbeere|mulberry|fruit
Castanea sativa|Edelkastanie|sweet chestnut|broadleaf
Aesculus hippocastanum|Rosskastanie|horse chestnut|broadleaf
Tilia cordata|Winterlinde|small-leaved lime|broadleaf
Tilia platyphyllos|Sommerlinde|large-leaved lime|broadleaf
Tilia|Linde (Gattung)|lime|broadleaf
Carpinus betulus|Hainbuche|hornbeam|broadleaf
Ostrya carpinifolia|Hopfenbuche|hop hornbeam|broadleaf
Ulmus glabra|Bergulme|wych elm|broadleaf
Ulmus minor|Feldulme|field elm|broadleaf
Ulmus laevis|Flatterulme|white elm|broadleaf
Ulmus|Ulme (Gattung)|elm|broadleaf
Robinia pseudoacacia|Robinie|black locust|broadleaf
Ailanthus altissima|Götterbaum|tree of heaven|broadleaf
Platanus x hispanica|Platane|plane|broadleaf
Sorbus aucuparia|Eberesche|rowan|broadleaf
Sorbus aria|Mehlbeere|whitebeam|broadleaf
Sorbus torminalis|Elsbeere|wild service tree|broadleaf
Sorbus domestica|Speierling|service tree|broadleaf
Corylus colurna|Baumhasel|Turkish hazel|broadleaf
Corylus avellana|Hasel|hazel|broadleaf
Sambucus nigra|Holunder|elder|broadleaf
Crataegus|Weißdorn|hawthorn|broadleaf
Liriodendron tulipifera|Tulpenbaum|tulip tree|broadleaf
Catalpa bignonioides|Trompetenbaum|catalpa|broadleaf
Paulownia tomentosa|Blauglockenbaum|paulownia|broadleaf
Gleditsia triacanthos|Gleditschie|honey locust|broadleaf
Celtis australis|Zürgelbaum|hackberry|broadleaf
Magnolia|Magnolie|magnolia|broadleaf
Coniferae|Nadelbaum|conifer|conifer
Magnoliopsida|Laubbaum|broadleaf|broadleaf"""
TAXA = tuple(tuple(l.split('|')) for l in _T.splitlines())
_CLASS = {t[0]: t[3] for t in TAXA}
_GENUS_CLASS: dict = {}
for _t in TAXA:
    _GENUS_CLASS.setdefault(_t[0].split()[0], _t[3])


def _fold(s: str) -> str:
    s = s.strip().lower().replace('×', 'x')
    for a, b in (('ä', 'ae'), ('ö', 'oe'), ('ü', 'ue'), ('ß', 'ss')):
        s = s.replace(a, b)
    return re.sub(r'[\s_]+', ' ', s)


_ALIAS: dict = {}
for _t in TAXA:
    for _n in (_t[0], _t[1].replace(' (Gattung)', ''), _t[2]):
        _ALIAS.setdefault(_fold(_n), _t[0])       # first entry wins (species before genus for shared names)
for _a, _l in (('buche', 'Fagus sylvatica'), ('eiche', 'Quercus'), ('ahorn', 'Acer'), ('birke', 'Betula'),
               ('erle', 'Alnus'), ('pappel', 'Populus'), ('weide', 'Salix'), ('linde', 'Tilia'), ('ulme', 'Ulmus'),
               ('tanne', 'Abies alba'), ('kiefer', 'Pinus sylvestris'), ('foehre', 'Pinus sylvestris'),
               ('waldkiefer', 'Pinus sylvestris'), ('schwarzkiefer', 'Pinus nigra'), ('zirbelkiefer', 'Pinus cembra'),
               ('arve', 'Pinus cembra'), ('kirsche', 'Prunus avium'), ('apfelbaum', 'Malus domestica'),
               ('birnbaum', 'Pyrus communis'), ('nussbaum', 'Juglans regia'), ('kastanie', 'Aesculus hippocastanum'),
               ('akazie', 'Robinia pseudoacacia'), ('vogelbeere', 'Sorbus aucuparia'), ('zwetschge', 'Prunus domestica'),
               ('pflaume', 'Prunus domestica'), ('aprikose', 'Prunus armeniaca'), ('obstbaum', 'Prunus'),
               ('fruit', 'Prunus'), ('laerche', 'Larix decidua'), ('fir', 'Abies'), ('spruce', 'Picea'),
               ('pine', 'Pinus'), ('larch', 'Larix'), ('oak', 'Quercus'), ('maple', 'Acer'), ('birch', 'Betula'),
               ('alder', 'Alnus'), ('willow', 'Salix'), ('elm', 'Ulmus'), ('lime', 'Tilia'), ('linden', 'Tilia')):
    _ALIAS[_a] = _l
_LATIN = re.compile(r'^[a-z]{3,24}( (x )?[a-z][a-z-]{2,30})?$')


def normalise(raw) -> tuple[str, bool]:
    """→ (scientific name, known).  Accepts the built-in list by scientific,
    German or English name, or any well-formed binomial / genus."""
    if not isinstance(raw, str) or not raw.strip():
        raise pf.FlagError(400, 'species must be a non-empty string', dict='/api/v1/trees/taxa/dict')
    s = _fold(raw)[:MAX_TAXON_CHARS]
    s = re.sub(r' (sp|spp|spec)\.?$', '', s)
    hit = _ALIAS.get(s)
    if hit:
        return hit, True
    if _LATIN.match(s) and ' ' in s:              # free binomial; a lone unknown word is more likely a typo
        return s[0].upper() + s[1:], False
    near = difflib.get_close_matches(s, list(_ALIAS), n=4, cutoff=0.72)
    raise pf.FlagError(400, f'unknown species {raw!r}: send a scientific name ("Quercus robur", genus alone is fine '
                            'for listed genera) or a German / English name from the dictionary',
                       did_you_mean=sorted({_ALIAS[n] for n in near}), dict='/api/v1/trees/taxa/dict')


def taxon_class(latin: str | None) -> str | None:
    if not latin:
        return None
    return _CLASS.get(latin) or _GENUS_CLASS.get(latin.split()[0])


# === SECTION: storage ===

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
    for r in rows:
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
            'dbh_mm': ds[len(ds) // 2] if ds else None, 'apex_h': apex_h,
            'v': len(rows), 'rev': rows[-1]['rev'], 'reporters': len(set(votes) | adds | set(uh) | set(dbh))}


def _load(c, kg: int) -> dict:
    rows = c.execute('SELECT rev, x, y, op, sp, h_dm, uh_dm, dbh_mm, reporter, ref FROM tree_ver WHERE kg=? ORDER BY rev', (kg,)).fetchall()
    if not rows:
        return dict(_EMPTY, path=str(pf.DB_PATH))
    by: dict = {}
    for r in rows:
        by.setdefault((r['x'], r['y']), []).append(r)
    xy = np.array(list(by), dtype=np.int64).reshape(-1, 2)
    cur = [_derive(v) for v in by.values()]
    lon, lat = _tf('EPSG:3035', 'EPSG:4326').transform(xy[:, 0].astype(float), xy[:, 1].astype(float))
    st = {'n': len(cur), 'rev': rows[-1]['rev'], 'path': str(pf.DB_PATH), 'x': xy[:, 0], 'y': xy[:, 1],
          'key': (xy[:, 0] << 32) | xy[:, 1], 'lon': np.asarray(lon), 'lat': np.asarray(lat),
          'sp': np.array([d['sp'] or 0 for d in cur], np.int64), 'cnt': np.array([d['n'] for d in cur], np.int32),
          'added': np.array([d['added'] for d in cur], bool), 'v': np.array([d['v'] for d in cur], np.int32),
          'trev': np.array([d['rev'] for d in cur], np.int64),
          'h': np.array([np.nan if d['h_dm'] is None else d['h_dm'] / 10.0 for d in cur]),
          'dbh': np.array([np.nan if d['dbh_mm'] is None else d['dbh_mm'] / 10.0 for d in cur]),
          'apex_h': np.array([np.nan if d['apex_h'] is None else d['apex_h'] / 10.0 for d in cur])}
    st['live'] = (st['sp'] > 0) | st['added'] | np.isfinite(st['h']) | np.isfinite(st['dbh'])
    return st


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

    def take(k, j):
        out_i.append(k); out_sp.append(int(ssp[j])); out_n.append(int(scn[j])); out_v.append(int(sv[j]))
        out_h.append(_num(suh[j])); out_d.append(_num(sdb[j]))

    for s in sts:
        m = s['live'] & ~s['added']
        if not m.any():
            continue
        sk = s['key'][m]; ssp = s['sp'][m]; scn = s['cnt'][m]; sv = s['v'][m]; suh = s['h'][m]; sdb = s['dbh'][m]
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
    for name, col in (('h_user_m', out_h), ('dbh_cm', out_d)):       # only when someone measured one
        if any(col[j] is not None for j in keep):
            u[name] = [col[j] for j in keep]
    return u


def _num(v) -> float | None:
    return round(float(v), 1) if np.isfinite(v) else None


def added(w, s, e, n, kgs, min_h: float = 0.0, max_h: float | None = None) -> dict | None:
    """User-added trees (ones the corpus has no apex for) in a bbox as columns
    ``{lon, lat, h_m, taxon, n, v}``.  Height window: the reported height, or
    3 m (the inventory floor) when none was given — so exactly one band of a
    partition carries each tree."""
    g = global_rev()
    if not g:
        return None
    out = {'lon': [], 'lat': [], 'h_m': [], 'taxon': [], 'n': [], 'v': [], 'dbh_cm': []}
    seen = set()
    for k in kgs:
        st = state(k, g)
        if not st['n']:
            continue
        hw = np.where(np.isfinite(st['h']), st['h'], ADDED_DEFAULT_H)
        m = st['added'] & (st['lon'] >= w) & (st['lon'] <= e) & (st['lat'] >= s) & (st['lat'] <= n) & (hw >= min_h)
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
    if not out['lon']:
        return None
    if all(v is None for v in out['dbh_cm']):
        del out['dbh_cm']
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


def _match(lon, lat, px, py, want_measured=True):
    """Nearest tree to the point → (kind, x, y, kg, dist_m, apex_h_m) with kind
    'apex' | 'added', or None."""
    best = None
    c = _apices_near(lon, lat) if want_measured else None
    if c is not None:
        d = np.hypot(c['x'] - px, c['y'] - py)
        rad = np.clip(np.sqrt(np.maximum(c['crown_m2'], 0) / math.pi), MATCH_MIN_M, MATCH_MAX_M)
        ok = d <= rad
        if ok.any():
            j = int(np.argmin(np.where(ok, d, 1e9)))
            best = ('apex', int(c['x'][j]), int(c['y'][j]), int(c['kg'][j]), float(d[j]), float(c['h'][j]))
    g = global_rev()
    for k in (_near_kgs(lon, lat) if g else ()):
        st = state(k, g)
        if not st['n'] or not st['added'].any():
            continue
        d = np.where(st['added'], np.hypot(st['x'] - px, st['y'] - py), 1e9)
        j = int(np.argmin(d))
        if d[j] <= ADDED_MATCH_M and (best is None or d[j] < best[4]):
            best = ('added', int(st['x'][j]), int(st['y'][j]), int(k), float(d[j]), None)
    return best


def _free_taxa_ok(c, latin: str) -> bool:
    if ('taxon', latin) in pf._DICT:
        return True
    n = c.execute("SELECT COUNT(*) FROM dict WHERE kind='taxon'").fetchone()[0]
    return n < MAX_FREE_TAXA + len(TAXA)


def record(body: dict, subj: dict, claim: str, rep: bytes, source_id, epoch: str | None) -> dict:
    """One species / add report → appended version (or a no-op when the
    reporter repeats themself).  Returns the tree's current public answer."""
    exp = body.get('expected') if isinstance(body.get('expected'), dict) else {}
    has_sp = any(k in d for d in (body, exp) for k in ('species', 'taxon'))
    raw = next((d[k] for d in (body, exp) for k in ('species', 'taxon') if d.get(k) not in (None, '')), None)
    uh_dm = _measure(body, exp, ('h_m', 'height_m'), H_M, 10, 'h_m', 'm')
    dbh_mm = _measure(body, exp, ('dbh_cm', 'dbh'), DBH_CM, 10, 'dbh_cm', 'cm')
    lon, lat = subj['lon'], subj['lat']
    px, py = _to_3035(lon, lat)
    latin = known = None
    if raw is not None:
        latin, known = normalise(raw)
    c = _conn()
    if claim == 'species':
        m = _match(lon, lat, px, py)
        if m is None:
            raise pf.FlagError(404, f'no tree within reach of this point (crown radius, {MATCH_MIN_M:g}–{MATCH_MAX_M:g} m). '
                                    'To add a tree we do not have: claim "missing" with the same point.',
                               hint={'layer': 'trees', 'claim': 'missing'})
        kind, x, y, kg, dist, apex_h = m
        if latin is None and uh_dm is None and dbh_mm is None:
            if not has_sp:
                raise pf.FlagError(400, 'say something about the tree: species, dbh_cm and / or h_m '
                                        '("species": null withdraws your report)', dict='/api/v1/trees/taxa/dict')
            op, sp, h_dm = OP_RETRACT, None, None
        else:
            op, h_dm = OP_SPECIES, (None if apex_h is None else int(round(apex_h * 10)))
    else:                                           # missing → add
        m = _match(lon, lat, px, py)
        if m is not None and m[0] == 'apex' and m[4] <= EXISTS_M:
            raise pf.FlagError(409, 'we already have a tree here — name it with claim "species"',
                               tree={'height_m': m[5], 'dist_m': round(m[4], 1)}, hint={'layer': 'trees', 'claim': 'species'})
        m = m if (m is not None and m[0] == 'added') else None
        if m is not None:
            kind, x, y, kg, dist, apex_h = m
        else:
            if not subj.get('kg'):
                raise pf.FlagError(400, 'point lies in no KG we know')
            kind, x, y, kg, dist, apex_h = 'added', int(round(px)), int(round(py)), int(subj['kg']), 0.0, None
        op, h_dm = OP_ADD, None
    if latin is not None and not known and not _free_taxa_ok(c, latin):
        raise pf.FlagError(400, 'too many free-form species names; use one from the dictionary', dict='/api/v1/trees/taxa/dict')
    sp = pf._dict_id(c, 'taxon', latin) if latin else None
    ep = pf._dict_id(c, 'epoch', epoch) if epoch else None
    now = int(time.time())
    folded = False
    with pf._LOCK:
        c.execute('BEGIN IMMEDIATE')
        try:
            rows = c.execute('SELECT rev, op, sp, uh_dm, dbh_mm, reporter, ref FROM tree_ver WHERE kg=? AND x=? AND y=? ORDER BY rev',
                             (kg, x, y)).fetchall()
            hidden = {r['ref'] for r in rows if r['op'] == OP_MODERATE}
            mine = [r for r in rows if bytes(r['reporter']) == rep and r['op'] != OP_MODERATE and r['rev'] not in hidden]
            since = 0                               # my versions after my last retraction
            for i, r in enumerate(mine):
                if r['op'] == OP_RETRACT:
                    since = i + 1
            cur = mine[since:]

            def latest(col):
                return next((r[col] for r in reversed(cur) if r[col] is not None), None)

            if op == OP_RETRACT:
                folded = not cur
            else:
                folded = bool(cur) and all(v is None or v == latest(col)
                                           for col, v in (('sp', sp), ('uh_dm', uh_dm), ('dbh_mm', dbh_mm)))
                if op == OP_ADD and not any(r['op'] == OP_ADD for r in cur):
                    folded = False
            if not folded:
                if len(rows) >= MAX_VERSIONS_PER_TREE:
                    raise pf.FlagError(429, f'this tree already has {MAX_VERSIONS_PER_TREE} versions')
                if c.execute('SELECT COUNT(*) FROM tree_ver').fetchone()[0] >= MAX_ROWS:
                    raise pf.FlagError(507, 'tree store full')
                c.execute('INSERT INTO tree_ver(ts, x, y, kg, op, sp, h_dm, reporter, source, epoch, uh_dm, dbh_mm) '
                          'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (now, x, y, kg, op, sp, h_dm, rep, source_id, ep, uh_dm, dbh_mm))
            c.execute('COMMIT')
        except Exception:
            c.execute('ROLLBACK')
            raise
    out = tree_at(kg, x, y) or {}
    out.update(folded=folded, matched=kind, dist_m=round(dist, 1))
    if apex_h is not None:
        out['height_m'] = round(apex_h, 2)
    if latin is not None and not known:
        out['warning'] = 'species not in our dictionary — stored as written; check /api/v1/trees/taxa/dict'
    return out


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

LIST_FIELDS = ('lon', 'lat', 'taxon', 'n', 'v', 'rev', 'added', 'h_m', 'dbh_cm')


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
    out['count'] = len(rows)
    return out


def history(lon: float, lat: float) -> dict | None:
    """Every version of the reported tree nearest to the point (≤ 5 m)."""
    px, py = _to_3035(lon, lat)
    g = global_rev()
    best = None
    for k in (_near_kgs(lon, lat) if g else ()):
        st = state(k, g)
        if not st['n']:
            continue
        d = np.hypot(st['x'] - px, st['y'] - py)
        j = int(np.argmin(d))
        if d[j] <= 5.0 and (best is None or d[j] < best[0]):
            best = (float(d[j]), st, j, int(k))
    if best is None:
        return None
    d, st, j, kg = best
    rows = _conn().execute('SELECT rev, ts, op, sp, uh_dm, dbh_mm, source, epoch, ref FROM tree_ver WHERE kg=? AND x=? AND y=? '
                           'ORDER BY rev', (kg, int(st['x'][j]), int(st['y'][j]))).fetchall()
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
        if r['source'] is not None:
            v['source'] = pf._dict_val(r['source'])
        if r['epoch'] is not None:
            v['epoch'] = pf._dict_val(r['epoch'])
        if r['op'] == OP_MODERATE:
            v['hides_rev'] = r['ref']
        elif r['rev'] in hidden:
            v['hidden'] = True
        vs.append(v)
    return {'tree': _public(st, j), 'kg': f'{kg:05d}', 'dist_m': round(d, 1), 'versions': vs}


def taxa_dict() -> dict:
    return {'fields': ['taxon', 'de', 'en', 'class'], 'taxa': [list(t) for t in TAXA],
            'classes': sorted(set(_CLASS.values())),
            'accepts': 'any taxon / de / en value above (case- and umlaut-insensitive), or a well-formed scientific '
                       'binomial not listed (stored as written, flagged with a warning)',
            'measures': {'dbh_cm': list(DBH_CM), 'h_m': list(H_M),
                         'note': 'optional on any tree report; several reporters → median; h_m is the reporter\'s '
                                 'height and never replaces the LiDAR height_m'},
            'limits': {'taxon_chars': MAX_TAXON_CHARS, 'versions_per_tree': MAX_VERSIONS_PER_TREE,
                       'match_m': [MATCH_MIN_M, MATCH_MAX_M], 'added_fold_m': ADDED_MATCH_M, 'exists_m': EXISTS_M}}


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
    try:
        lon, lat = float(request.args['lon']), float(request.args['lat'])
    except (KeyError, ValueError):
        raise pf.FlagError(400, 'lon, lat required')
    d = history(lon, lat)
    if d is None:
        raise pf.FlagError(404, 'no reported tree within 5 m')
    d['meta'] = _meta()
    return pf._resp(d, max_age=TAXA_MAX_AGE)


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
