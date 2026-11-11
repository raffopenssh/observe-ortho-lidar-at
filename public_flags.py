"""public_flags — public, tokenless "this looks wrong" reports for everything the
public tier serves (cells / trees / structures / landscape / tiles / KG dossiers).

Design (docs/public-flags.md):
  * subject = H3 res-12 cell (from cell id or lon/lat) | KG code; nothing else is
    ever keyed (no object ids, no geometry, no identifiers of any kind).
  * one SQLite file ``data/public_flags.sqlite`` (WAL, synchronous=NORMAL), one
    ``report`` row per (subject, layer, claim, reporter) — repeats FOLD into ``n``
    instead of new rows; strings go through a tiny dictionary table; coords are
    int32 micro-degrees; notes ≤ 280 chars; observed/expected ≤ 512/256 B JSON.
  * when the reporter omits ``observed`` we SNAPSHOT what we serve for that cell
    at report time (+ product epoch), so a flag can be judged after a rebuild.
  * reads are grouped tallies by default (one row per subject), ETag'd, gzip'd,
    bbox / cell / ids / kg / point keyed exactly like /api/v1/cells.
  * standalone Blueprint: imports cells_api (read-only helpers), service_guard,
    api_tiers (admin token for status changes). Never app.py.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import os
import sqlite3
import threading
import time
from pathlib import Path

from flask import Blueprint, Response, request

import service_guard as sg

log = logging.getLogger('public_flags')
flags_bp = Blueprint('public_flags', __name__)

DATA_DIR = Path('data')
DB_PATH = DATA_DIR / 'public_flags.sqlite'
SALT_PATH = DATA_DIR / 'public_flags.salt'

RES = 12
MAX_BATCH = 50
MAX_NOTE = 280
MAX_OBSERVED_B = 512
MAX_EXPECTED_B = 256
MAX_SOURCE = 40
MAX_BODY_B = 32 * 1024
MAX_BBOX_SPAN_DEG = 0.05
MAX_IDS = 512
MAX_ROWS_OUT = 2000
MAX_K = 3
DAILY_CAP = 500                      # reports per client key per UTC day
DAILY_CAP_KEYED = 20_000             # … per reporter key issued by us (organisations)
DAILY_CAP_SELF = 2_000               # … per self-issued reporter key (POST /api/v1/flags/token)
TOKENS_PER_CLIENT_DAY = 20           # self-issued keys per client per UTC day
MAX_SELF_KEYS = 200_000
SELF_MARK = '~'                      # holder "name~1a2b" = self-issued, unverified
KEYS_PATH = DATA_DIR / 'reporter_keys.json'        # {"<key>": "<holder>"} — gitignored, 0600
KEY_REQUIRED_PATH = DATA_DIR / 'tree_key_required'  # flag file: tree reports without a key → 401
MAX_ROWS_DB = 2_000_000              # hard cap; oldest non-open rows pruned past it
CLOSED_RETENTION_S = 180 * 86400     # rejected / fixed / stale rows older than this are pruned
_PRUNE_EVERY = 500
CACHE_MAX_AGE = 30
HEAT_MAX_AGE = 300

LAYERS = (
    'cells.cover',        # the observed land-cover groups of a cell (what a map glyph paints under v=obs)
    'cells.height',       # height.mean / p90 / max of a cell
    'cells.trees',        # trees.n / h_max / canopy + the apex list of a cell
    'cells.structures',   # structures_n / cover + the structure list of a cell
    'cells.change',       # dh_m / dtm_m / forest_loss_year / als_years
    'cells.consistency',  # our consistency verdict vs the statistical layer
    'cells.terrain',      # elev / slope / aspect / roughness
    'cells.ndvi',         # ndvi.mean / amp / phenology
    'trees',              # /api/v1/trees/bbox apex (giant tree) at lon/lat
    'landmarks',          # /api/v1/landmarks/bbox vertical at lon/lat
    'buildings',          # /api/v1/buildings/bbox height / storeys at lon/lat
    'landscape',          # /api/v1/landscape roll-up for a bbox / point
    'heightfield',        # /api/v1/heightfield grid value
    'tiles',              # hillshade / slope tile artefact
    'kg',                 # whole KG dossier / product
    'docs',               # llm.txt / dict wording
    'other',
)
CLAIMS = (
    'wrong_type',    # the class / cover group is wrong (say what in expected.group / expected.type)
    'wrong_height',  # height off by more than the quantisation (expected.h_m)
    'too_tall',      # implausibly tall (mast, crane, artefact)
    'too_short',
    'missing',       # something real is not in our data
    'spurious',      # we show something that is not there
    'misplaced',     # right thing, wrong position (> 1 cell / > 5 m)
    'stale',         # changed since our observation date
    'artifact',      # processing artefact (stripe, seam, tile edge, NaN hole)
    'looks_right',   # positive confirmation — counts as QA evidence
    'species',       # layer trees + a point: describe the tree there — species, dbh_cm, h_m (versioned, tree_user.py)
    'tree',          # same as species (clearer name when only dbh_cm / h_m are sent)
    'other',
)
STATUS = ('open', 'confirmed', 'rejected', 'fixed', 'stale')
_STATUS_ID = {s: i for i, s in enumerate(STATUS)}

_SCHEMA = [
    'PRAGMA journal_mode=WAL', 'PRAGMA synchronous=NORMAL',
    """CREATE TABLE IF NOT EXISTS dict (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, val TEXT NOT NULL,
                                        UNIQUE(kind, val))""",
    """CREATE TABLE IF NOT EXISTS report (
        id INTEGER PRIMARY KEY,
        ts INTEGER NOT NULL,                -- first seen (unix s)
        ts_last INTEGER NOT NULL,           -- last fold
        n INTEGER NOT NULL DEFAULT 1,       -- folded repeats
        skey INTEGER NOT NULL,              -- subject key: cell id (res 12) or -kg for KG subjects
        cell INTEGER,                       -- res-12 id (NULL for KG subjects)
        kg TEXT,                            -- 5-digit code when resolved
        lon_e6 INTEGER, lat_e6 INTEGER,     -- subject point (cell centre unless the reporter gave a point)
        layer INTEGER NOT NULL,             -- dict id
        claim INTEGER NOT NULL,             -- dict id
        source INTEGER,                     -- dict id (client app label)
        reporter BLOB NOT NULL,             -- 8-byte salted hash (client id or IP) — never the raw value
        epoch TEXT,                         -- product epoch of the subject when flagged
        observed TEXT,                      -- compact JSON: what we served (client copy or our snapshot)
        expected TEXT,                      -- compact JSON: what the reporter thinks
        note TEXT,
        status INTEGER NOT NULL DEFAULT 0,
        status_ts INTEGER, status_note TEXT,
        UNIQUE(skey, layer, claim, reporter)
    )""",
    'CREATE INDEX IF NOT EXISTS r_cell ON report(cell)',
    'CREATE INDEX IF NOT EXISTS r_kg ON report(kg)',
    'CREATE INDEX IF NOT EXISTS r_ll ON report(lat_e6, lon_e6)',
    'CREATE INDEX IF NOT EXISTS r_ts ON report(ts_last)',
    'CREATE INDEX IF NOT EXISTS r_status ON report(status)',
    "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)",
    # self-issued reporter keys (POST /api/v1/flags/token): only the sha256 of a key is kept
    "CREATE TABLE IF NOT EXISTS rkey (h BLOB PRIMARY KEY, holder TEXT NOT NULL, ts INTEGER NOT NULL, client TEXT)",
]

_LOCK = threading.Lock()
_TLS = threading.local()
_DICT: dict = {}          # (kind, val) -> id
_DICT_REV: dict = {}      # id -> val
_WRITES = 0
_post_limit = sg.RateLimiter('flags_post', 1.0, 10.0, env='FLAGS_POST_RATE_PER_S')
_get_limit = sg.RateLimiter('flags_get', 5.0, 20.0, env='FLAGS_GET_RATE_PER_S')
_daily: dict = {}         # client key -> [day, count]
_heat_cache: dict = {}    # key -> (ts, body, etag)


class FlagError(Exception):
    def __init__(self, status: int, msg: str, **extra):
        super().__init__(msg)
        self.status, self.msg, self.extra = status, msg, extra


# === SECTION: storage ===

def _salt() -> bytes:
    s = getattr(_salt, '_v', None)
    if s is None:
        try:
            s = SALT_PATH.read_bytes()
        except FileNotFoundError:
            s = os.urandom(32)
            SALT_PATH.parent.mkdir(parents=True, exist_ok=True)
            SALT_PATH.write_bytes(s)
            try:
                os.chmod(SALT_PATH, 0o600)
            except OSError:
                pass
        _salt._v = s
    return s


def _conn() -> sqlite3.Connection:
    c = getattr(_TLS, 'c', None)
    if c is None or getattr(_TLS, 'path', None) != str(DB_PATH):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        c = sqlite3.connect(str(DB_PATH), timeout=10, isolation_level=None, check_same_thread=False)
        c.row_factory = sqlite3.Row
        for s in _SCHEMA:
            c.execute(s)
        _TLS.c, _TLS.path = c, str(DB_PATH)
        _load_dict(c)
    return c


def _load_dict(c):
    with _LOCK:
        for r in c.execute('SELECT id, kind, val FROM dict'):
            _DICT[(r['kind'], r['val'])] = r['id']
            _DICT_REV[r['id']] = r['val']


def _dict_id(c, kind: str, val: str) -> int:
    k = (kind, val)
    i = _DICT.get(k)
    if i is None:
        with _LOCK:
            i = _DICT.get(k)
            if i is None:
                c.execute('INSERT OR IGNORE INTO dict(kind, val) VALUES (?, ?)', k)
                i = c.execute('SELECT id FROM dict WHERE kind=? AND val=?', k).fetchone()[0]
                _DICT[k] = i; _DICT_REV[i] = val
    return i


def _dict_val(i) -> str | None:
    if i is None:
        return None
    v = _DICT_REV.get(i)
    if v is None:
        _load_dict(_conn())
        v = _DICT_REV.get(i)
    return v


def _reporter_hash(reporter: str | None, client: str, org: str | None = None) -> bytes:
    # a keyed caller's reporters live in their own namespace and do not depend on where the caller connects from
    v = (f'k:{org}:{reporter or ""}') if org else (('r:' + reporter) if reporter else ('c:' + client))
    return hmac.new(_salt(), v.encode(), hashlib.blake2s).digest()[:8]


def _prune(c):
    now = int(time.time())
    c.execute('DELETE FROM report WHERE status IN (2,3,4) AND status_ts < ?', (now - CLOSED_RETENTION_S,))
    n = c.execute('SELECT COUNT(*) FROM report').fetchone()[0]
    if n > MAX_ROWS_DB:
        c.execute('DELETE FROM report WHERE id IN (SELECT id FROM report WHERE status != 0 ORDER BY ts_last LIMIT ?)',
                  (n - MAX_ROWS_DB,))
    c.execute("INSERT OR REPLACE INTO meta(k, v) VALUES ('pruned_at', ?)", (str(now),))


# === SECTION: subjects ===

def _h3():
    import h3
    return h3


def _parse_hex(s: str) -> int:
    s = str(s).strip().lower()
    if not s or len(s) > 16 or any(ch not in '0123456789abcdef' for ch in s):
        raise FlagError(400, f'bad cell id {s!r}')
    return int(s, 16)


def _cell_res(cid: int) -> int:
    return (cid >> 52) & 0xF


def _in_at(lon, lat) -> bool:
    return 9.0 <= lon <= 17.6 and 46.0 <= lat <= 49.2


def _kg_for_point(lon: float, lat: float) -> str | None:
    try:
        import cells_api
        ks = cells_api._kgs_intersecting(lon, lat, lon, lat)
    except Exception:  # noqa: BLE001
        return None
    return ks[0] if ks else None


def _subject(body: dict) -> dict:
    """→ {skey, cell, kg, lon, lat}. Exactly one of cell / lon+lat / kg (kg may
    accompany the others as a hint)."""
    h3 = _h3()
    cell = body.get('cell')
    lon, lat = body.get('lon'), body.get('lat')
    kg = body.get('kg') or body.get('kg_code')
    if kg is not None:
        kg = str(kg).strip().zfill(5)
        if not (kg.isdigit() and len(kg) == 5):
            raise FlagError(400, 'kg must be a 5-digit code')
    if cell is not None:
        cid = _parse_hex(cell)
        r = _cell_res(cid)
        if r != RES:
            if r < 8 or r > RES:
                raise FlagError(400, f'cell must be res 8..{RES}')
            # coarser id → flag its centre cell at res 12, remember the res the reporter used
            la, lo = h3.cell_to_latlng(h3.int_to_str(cid))
            cid = h3.str_to_int(h3.latlng_to_cell(la, lo, RES))
        la, lo = h3.cell_to_latlng(h3.int_to_str(cid))
        if lon is None or lat is None:
            lon, lat = lo, la
        else:
            lon, lat = float(lon), float(lat)
            if abs(lon - lo) > 0.002 or abs(lat - la) > 0.002:
                raise FlagError(400, 'lon/lat does not lie in the given cell')
        return {'skey': cid, 'cell': cid, 'kg': kg or _kg_for_point(lon, lat), 'lon': lon, 'lat': lat}
    if lon is not None and lat is not None:
        try:
            lon, lat = float(lon), float(lat)
        except (TypeError, ValueError):
            raise FlagError(400, 'lon, lat must be numbers')
        if not _in_at(lon, lat):
            raise FlagError(400, 'point must lie within Austria (lon 9–17.6, lat 46–49.2)')
        cid = h3.str_to_int(h3.latlng_to_cell(lat, lon, RES))
        return {'skey': cid, 'cell': cid, 'kg': kg or _kg_for_point(lon, lat), 'lon': lon, 'lat': lat}
    if kg:
        return {'skey': -int(kg), 'cell': None, 'kg': kg, 'lon': None, 'lat': None}
    raise FlagError(400, 'subject required: cell=<h3 res 12> | lon=&lat= | kg=<code>')


# === SECTION: snapshot (what we served at report time) ===

def snapshot_cell(cid: int, lon: float | None = None, lat: float | None = None, layer: str = '') -> tuple[dict | None, str | None]:
    """Compact picture of the cell as the public tier serves it now (+ epoch).
    (None, None) when the cell is not on disk."""
    try:
        import numpy as np
        import cells_api as ca
        import ne_h3
        want = np.array([cid], np.uint64)
        t, hdrs, meta = ca.select(want, None)
        if len(t) == 0:
            return None, None
        layers = ('trees',) if layer in ('cells.trees', 'trees') else ('structures',) if layer in ('cells.structures', 'buildings', 'landmarks') else ()
        row = ca.table_json(t, ca.decode_obs(t.obs), layers=layers, centres=False)[0]
    except Exception as e:  # noqa: BLE001
        log.debug('snapshot %x: %s', cid, e)
        return None, None
    cov = row.get('cover') or {}
    dom = max(cov, key=cov.get) if cov else None
    snap = {'group': dom, 'share': cov.get(dom) if dom else None,
            'h_max': (row.get('height') or {}).get('max'),
            'trees_n': (row.get('trees') or {}).get('n') if isinstance(row.get('trees'), dict) else None,
            'structures_n': row.get('structures_n'), 'consistency': row.get('consistency')}
    pts = row.get('trees') if layers == ('trees',) else row.get('structures') if layers else None
    if isinstance(pts, list) and pts and lon is not None:
        best, bd = None, 1e9
        for p in pts:
            d = _dist_m(lon, lat, p.get('lon'), p.get('lat'))
            if d < bd:
                best, bd = p, d
        if best is not None and bd <= 30:
            snap['nearest'] = {k: best.get(k) for k in ('h_m', 'h_max_m', 'type', 'species', 'crown_m2', 'area_m2') if best.get(k) is not None}
            snap['nearest']['dist_m'] = round(bd, 1)
    snap = {k: v for k, v in snap.items() if v is not None}
    return snap, meta.get('epoch')


def _dist_m(lon1, lat1, lon2, lat2) -> float:
    if None in (lon1, lat1, lon2, lat2):
        return 1e9
    kx = 111_320.0 * math.cos(math.radians(lat1))
    return math.hypot((lon2 - lon1) * kx, (lat2 - lat1) * 110_540.0)


def _epoch_now(kg: str | None) -> str | None:
    if not kg:
        return None
    try:
        import cells_api as ca
        h = ca.header(kg)
        return h.get('epoch') if h else None
    except Exception:  # noqa: BLE001
        return None


# === SECTION: write ===

def _compact_json(v, cap: int, name: str) -> str | None:
    if v in (None, '', {}, []):
        return None
    if not isinstance(v, (dict, list)):
        raise FlagError(400, f'{name} must be an object')
    s = json.dumps(v, separators=(',', ':'), ensure_ascii=False, sort_keys=True)
    if len(s.encode()) > cap:
        raise FlagError(413, f'{name} larger than {cap} bytes', max_bytes=cap)
    return s


def _clean_note(s) -> str | None:
    if s is None:
        return None
    s = ''.join(ch for ch in str(s) if ch == '\n' or ord(ch) >= 32).strip()
    return s[:MAX_NOTE] or None


_KNOWN = {'cell', 'lon', 'lat', 'kg', 'kg_code', 'layer', 'claim', 'observed', 'expected', 'note', 'source',
          'reporter', 'epoch', 'species', 'taxon', 'h_m', 'dbh_cm', 'tree_id', 'tree_id_v2', 'client_id', 'force',
          'retract', 'measured_at', 'dbh_method', 'h_method', 'vitality', 'status', 'reporter_key'}
TREE_LAYERS = ('trees', 'cells.trees')


def _names_tree(body: dict) -> bool:
    """Anything only a tree report carries makes it one — also the two withdraw
    spellings without layer / claim: ``"species": null`` and ``"retract": true``
    (retract exists for trees only; an explicit non-tree layer keeps it a flag)."""
    if any(body.get(k) not in (None, '') for k in ('species', 'taxon', 'dbh_cm', 'tree_id', 'client_id')):
        return True
    if any(k in body for k in ('species', 'taxon')):
        return True
    r = body.get('retract')
    return ((r is True or str(r).strip().lower() in ('1', 'true', 'yes'))
            and str(body.get('layer') or 'trees').strip().lower() in TREE_LAYERS
            and body.get('lon') is not None and body.get('lat') is not None)


def record(body: dict, client: str, org: str | None = None) -> dict:
    if not isinstance(body, dict):
        raise FlagError(400, 'report must be a JSON object')
    # anything only a tree report carries makes it one (also "species": null = withdraw)
    names_tree = _names_tree(body)
    if names_tree or str(body.get('layer') or '').strip().lower() in TREE_LAYERS:
        import tree_user as tu
        tu.prefill(body)                # tree_id / client_id → lon, lat
    layer = str(body.get('layer') or ('trees' if names_tree else 'other')).strip().lower()
    if layer not in LAYERS:
        raise FlagError(400, f'layer must be one of {", ".join(LAYERS)}', layers=LAYERS)
    claim = str(body.get('claim') or ('species' if names_tree and layer in TREE_LAYERS else 'other')).strip().lower()
    if claim not in CLAIMS:
        raise FlagError(400, f'claim must be one of {", ".join(CLAIMS)}', claims=CLAIMS)
    subj = _subject(body)
    has_point = body.get('lon') is not None and body.get('lat') is not None
    if claim == 'tree':
        claim = 'species'
    if claim == 'species':
        # per-tree, versioned: lives in tree_ver only (no folded report row)
        if layer not in TREE_LAYERS or not has_point:
            raise FlagError(400, 'claim "species" / "tree" needs layer "trees" and a point: {lon, lat, species | dbh_cm | h_m}')
        import tree_user as tu
        c = _conn()
        src = _clean_note(body.get('source'))
        rp = body.get('reporter')
        epoch = str(body['epoch'])[:40] if body.get('epoch') else _epoch_now(subj['kg'])
        tree = tu.record(body, subj, claim, _reporter_hash(str(rp)[:120] if rp not in (None, '') else None, client, org),
                         _dict_id(c, 'source', src[:MAX_SOURCE]) if src else None, epoch,
                         _dict_id(c, 'org', org) if org else None)
        out = {'rev': tree.get('rev'), 'folded': tree['folded'], 'status': 'open', 'layer': layer, 'claim': claim,
               'subject': _subject_public(subj), 'epoch': epoch, 'tree': tree}
        warn = [k for k in body if k not in _KNOWN]
        if warn:
            out['warnings'] = [f'ignored unknown field(s): {", ".join(sorted(warn))}']
        return out
    observed = _compact_json(body.get('observed'), MAX_OBSERVED_B, 'observed')
    expected = _compact_json(body.get('expected'), MAX_EXPECTED_B, 'expected')
    note = _clean_note(body.get('note') or body.get('notes'))
    source = _clean_note(body.get('source'))
    source = source[:MAX_SOURCE] if source else None
    reporter = body.get('reporter')
    reporter = str(reporter)[:120] if reporter not in (None, '') else None
    epoch = body.get('epoch')
    epoch = str(epoch)[:40] if epoch else None
    served = None
    snap = None
    if subj['cell'] is not None:
        snap, ep = snapshot_cell(subj['cell'], subj['lon'], subj['lat'], layer)
        served = snap is not None
        if observed is None and snap:
            observed = json.dumps(snap, separators=(',', ':'), ensure_ascii=False)
        epoch = epoch or ep
    else:
        epoch = epoch or _epoch_now(subj['kg'])
    c = _conn()
    now = int(time.time())
    lid, cid_ = _dict_id(c, 'layer', layer), _dict_id(c, 'claim', claim)
    sid = _dict_id(c, 'source', source) if source else None
    rep = _reporter_hash(reporter, client, org)
    tree = None
    if claim == 'missing' and layer in TREE_LAYERS and has_point:
        # a tree we do not have: kept as a versioned user-added tree (tree_user.py).  Done BEFORE the flag
        # row so a refused add (409: a tree exists there) or a withdrawal leaves no stray row behind.
        import tree_user as tu
        try:
            tree = tu.record(body, subj, claim, rep, _dict_id(c, 'source', source) if source else None, epoch,
                             _dict_id(c, 'org', org) if org else None)
        except FlagError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning('tree add failed: %s', e)
        if tree is not None and tree.get('op') == 'retract':
            return {'rev': tree.get('rev'), 'folded': tree['folded'], 'status': 'open', 'layer': layer,
                    'claim': claim, 'subject': _subject_public(subj), 'epoch': epoch, 'tree': tree}
    lon_e6 = int(round(subj['lon'] * 1e6)) if subj['lon'] is not None else None
    lat_e6 = int(round(subj['lat'] * 1e6)) if subj['lat'] is not None else None
    global _WRITES
    with _LOCK:
        c.execute('BEGIN IMMEDIATE')
        try:
            cur = c.execute(
                'INSERT INTO report(ts, ts_last, n, skey, cell, kg, lon_e6, lat_e6, layer, claim, source, reporter, '
                'epoch, observed, expected, note, status) VALUES (?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,0) '
                'ON CONFLICT(skey, layer, claim, reporter) DO UPDATE SET n = n + 1, ts_last = excluded.ts_last, '
                'expected = COALESCE(excluded.expected, expected), note = COALESCE(excluded.note, note), '
                'observed = COALESCE(observed, excluded.observed), epoch = COALESCE(epoch, excluded.epoch), '
                'source = COALESCE(excluded.source, source), '
                'status = CASE WHEN status IN (3, 4) THEN 0 ELSE status END '
                'RETURNING id, n, status',
                (now, now, subj['skey'], subj['cell'], subj['kg'], lon_e6, lat_e6, lid, cid_, sid, rep,
                 epoch, observed, expected, note))
            rid, n, st = cur.fetchone()
            _WRITES += 1
            if _WRITES % _PRUNE_EVERY == 0:
                _prune(c)
            c.execute('COMMIT')
        except Exception:
            c.execute('ROLLBACK')
            raise
    _heat_cache.clear()
    out = {'id': rid, 'folded': n > 1, 'n': n, 'status': STATUS[st], 'layer': layer, 'claim': claim,
           'subject': _subject_public(subj), 'epoch': epoch}
    if served is not None:
        out['served'] = served
        if snap and body.get('observed') in (None, '', {}):
            out['snapshot'] = snap
    out['tally'] = tally(subj['skey'])
    if tree is not None:
        out['tree'] = tree
    warn = [k for k in body if k not in _KNOWN]
    if warn:
        out['warnings'] = [f'ignored unknown field(s): {", ".join(sorted(warn))}']
    return out


def _subject_public(subj: dict) -> dict:
    d = {'kg': subj['kg']}
    if subj['cell'] is not None:
        d['cell'] = f"{subj['cell']:x}"
        d['lon'] = round(subj['lon'], 6); d['lat'] = round(subj['lat'], 6)
    return d


def tally(skey: int) -> dict:
    c = _conn()
    rows = c.execute('SELECT claim, status, SUM(n) AS n, COUNT(*) AS reporters FROM report WHERE skey=? '
                     'GROUP BY claim, status', (skey,)).fetchall()
    claims: dict = {}
    n = rep = open_ = 0
    for r in rows:
        cl = _dict_val(r['claim'])
        claims[cl] = claims.get(cl, 0) + r['n']
        n += r['n']; rep += r['reporters']
        if r['status'] == 0:
            open_ += r['reporters']
    return {'n': n, 'reporters': rep, 'open': open_, 'claims': claims}


def set_status(rid: int, status: str, note: str | None) -> dict | None:
    if status not in STATUS:
        raise FlagError(400, f'status must be one of {", ".join(STATUS)}')
    c = _conn()
    with _LOCK:
        r = c.execute('UPDATE report SET status=?, status_ts=?, status_note=? WHERE id=? RETURNING id',
                      (_STATUS_ID[status], int(time.time()), _clean_note(note), rid)).fetchone()
    _heat_cache.clear()
    return get_one(rid) if r else None


# === SECTION: read ===

def _row_public(r, notes: bool = True, epoch_now: dict | None = None) -> dict:
    d = {'id': r['id'], 'layer': _dict_val(r['layer']), 'claim': _dict_val(r['claim']), 'n': r['n'],
         'status': STATUS[r['status']], 'ts': r['ts'], 'ts_last': r['ts_last'], 'kg': r['kg']}
    if r['cell'] is not None:
        d['cell'] = f"{r['cell']:x}"
        d['lon'] = r['lon_e6'] / 1e6; d['lat'] = r['lat_e6'] / 1e6
    if r['epoch']:
        d['epoch'] = r['epoch']
        if epoch_now is not None and r['kg']:
            en = epoch_now.get(r['kg'], ...)
            if en is ...:
                en = epoch_now[r['kg']] = _epoch_now(r['kg'])
            if en and en != r['epoch']:
                d['superseded'] = True       # product rebuilt since the flag: re-check before acting
    for k in ('observed', 'expected'):
        if r[k]:
            try:
                d[k] = json.loads(r[k])
            except ValueError:
                pass
    if notes and r['note']:
        d['note'] = r['note']
    if r['source'] is not None:
        d['source'] = _dict_val(r['source'])
    if r['status'] and r['status_note']:
        d['status_note'] = r['status_note']
    return d


def get_one(rid: int) -> dict | None:
    r = _conn().execute('SELECT * FROM report WHERE id=?', (rid,)).fetchone()
    return _row_public(r, epoch_now={}) if r else None


def _where(a: dict) -> tuple[str, list]:
    """Subject selector + filters from query args → SQL where + params."""
    h3 = _h3()
    w: list[str] = []; p: list = []
    if a.get('cell'):
        cid = _parse_hex(a['cell'])
        if _cell_res(cid) == RES:
            w.append('cell = ?'); p.append(cid)
        else:
            if _cell_res(cid) < 6:
                raise FlagError(400, 'cell must be res 6..12')
            la, lo = h3.cell_to_latlng(h3.int_to_str(cid))
            # coarse cell → bbox of its boundary (cheap, slightly generous)
            bnd = h3.cell_to_boundary(h3.int_to_str(cid))
            lats = [b[0] for b in bnd]; lons = [b[1] for b in bnd]
            w.append('lat_e6 BETWEEN ? AND ? AND lon_e6 BETWEEN ? AND ?')
            p += [int(min(lats) * 1e6), int(max(lats) * 1e6), int(min(lons) * 1e6), int(max(lons) * 1e6)]
    elif a.get('ids'):
        ids = [_parse_hex(x) for x in a['ids'].split(',') if x.strip()]
        if len(ids) > MAX_IDS:
            raise FlagError(413, f'more than {MAX_IDS} ids', max_ids=MAX_IDS)
        if any(_cell_res(i) != RES for i in ids):
            raise FlagError(400, 'ids must be res-12 cells')
        w.append('cell IN (' + ','.join('?' * len(ids)) + ')'); p += ids
    elif a.get('lon') and a.get('lat'):
        try:
            lon, lat, k = float(a['lon']), float(a['lat']), int(a.get('k', 0))
        except ValueError:
            raise FlagError(400, 'lon, lat must be numbers; k an integer 0..3')
        if not (0 <= k <= MAX_K):
            raise FlagError(400, f'k must be 0..{MAX_K}')
        c0 = h3.latlng_to_cell(lat, lon, RES)
        ids = [h3.str_to_int(x) for x in (h3.grid_disk(c0, k) if k else [c0])]
        w.append('cell IN (' + ','.join('?' * len(ids)) + ')'); p += ids
    elif a.get('bbox'):
        try:
            bw, bs, be, bn = [float(x) for x in a['bbox'].split(',')]
        except ValueError:
            raise FlagError(400, 'bbox=west,south,east,north')
        if be < bw or bn < bs:
            raise FlagError(400, 'bbox east<west or north<south')
        if (be - bw) > MAX_BBOX_SPAN_DEG or (bn - bs) > MAX_BBOX_SPAN_DEG:
            raise FlagError(413, f'bbox span must be ≤ {MAX_BBOX_SPAN_DEG}°; use /api/v1/flags/heat for overviews',
                            max_span_deg=MAX_BBOX_SPAN_DEG)
        w.append('lat_e6 BETWEEN ? AND ? AND lon_e6 BETWEEN ? AND ?')
        p += [int(bs * 1e6), int(bn * 1e6), int(bw * 1e6), int(be * 1e6)]
    elif a.get('kg'):
        kg = str(a['kg']).strip().zfill(5)
        if not (kg.isdigit() and len(kg) == 5):
            raise FlagError(400, 'kg must be a 5-digit code')
        w.append('kg = ?'); p.append(kg)
    else:
        raise FlagError(400, 'selector required: bbox= | cell= | ids= | lon=&lat=[&k=] | kg=')
    for kind in ('layer', 'claim'):
        if a.get(kind):
            vals = [v.strip().lower() for v in a[kind].split(',') if v.strip()]
            ids_ = [_DICT.get((kind, v)) for v in vals]
            ids_ = [i for i in ids_ if i is not None]
            if not ids_:
                w.append('0'); continue
            w.append(f'{kind} IN (' + ','.join('?' * len(ids_)) + ')'); p += ids_
    st = (a.get('status') or 'open').lower()
    if st != 'all':
        sts = [_STATUS_ID[s] for s in st.split(',') if s in _STATUS_ID]
        if not sts:
            raise FlagError(400, f'status must be all or a list of {", ".join(STATUS)}')
        w.append('status IN (' + ','.join('?' * len(sts)) + ')'); p += sts
    if a.get('since'):
        w.append('ts_last >= ?'); p.append(_parse_since(a['since']))
    return ' AND '.join(w), p


def _parse_since(s: str) -> int:
    s = s.strip()
    if s.isdigit():
        return int(s)
    import datetime as dt
    try:
        return int(dt.datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp())
    except ValueError:
        raise FlagError(400, 'since must be unix seconds or ISO-8601')


def query(a: dict) -> dict:
    _conn()
    where, p = _where(a)
    limit = max(1, min(int(a.get('limit', 500) or 500), MAX_ROWS_OUT))
    notes = (a.get('notes') or '1') not in ('0', 'false', 'no')
    group = (a.get('group') or 'subject').lower()
    c = _conn()
    if group == 'report':
        rows = c.execute(f'SELECT * FROM report WHERE {where} ORDER BY ts_last DESC LIMIT ?', p + [limit + 1]).fetchall()
        trunc = len(rows) > limit
        epoch_now: dict = {}
        return {'reports': [_row_public(r, notes, epoch_now) for r in rows[:limit]], 'count': min(len(rows), limit),
                'truncated': trunc, 'group': 'report'}
    # grouped: one row per (subject, layer) with claim histogram
    rows = c.execute(
        f'SELECT skey, cell, kg, MIN(lon_e6) AS lon_e6, MIN(lat_e6) AS lat_e6, layer, claim, '
        f'SUM(n) AS n, COUNT(*) AS reporters, SUM(status = 0) AS open, MAX(ts_last) AS last, '
        f'MIN(epoch) AS epoch, MAX(expected) AS expected '
        f'FROM report WHERE {where} GROUP BY skey, layer, claim ORDER BY last DESC LIMIT ?', p + [limit * 4]).fetchall()
    subj: dict = {}
    for r in rows:
        k = (r['skey'], r['layer'])
        d = subj.get(k)
        if d is None:
            d = subj[k] = {'kg': r['kg'], 'layer': _dict_val(r['layer']), 'n': 0, 'reporters': 0, 'open': 0,
                           'claims': {}, 'last': 0}
            if r['cell'] is not None:
                d['cell'] = f"{r['cell']:x}"; d['lon'] = r['lon_e6'] / 1e6; d['lat'] = r['lat_e6'] / 1e6
            if r['epoch']:
                d['epoch'] = r['epoch']
        cl = _dict_val(r['claim'])
        d['claims'][cl] = d['claims'].get(cl, 0) + r['n']
        d['n'] += r['n']; d['reporters'] += r['reporters']; d['open'] += r['open']
        d['last'] = max(d['last'], r['last'])
        if r['expected'] and cl != 'looks_right':
            try:
                d.setdefault('expected', {})[cl] = json.loads(r['expected'])
            except ValueError:
                pass
    out = sorted(subj.values(), key=lambda d: -d['last'])
    trunc = len(out) > limit
    out = out[:limit]
    epoch_now: dict = {}
    for d in out:
        if d.get('epoch') and d.get('kg'):
            en = epoch_now.get(d['kg'], ...)
            if en is ...:
                en = epoch_now[d['kg']] = _epoch_now(d['kg'])
            if en and en != d['epoch']:
                d['superseded'] = True
    return {'subjects': out, 'count': len(out), 'truncated': trunc, 'group': 'subject'}


def columns(res_: dict) -> dict:
    """Grouped result → column arrays (map heat layers)."""
    s = res_['subjects']
    return {'cells': [d.get('cell') for d in s], 'kg': [d['kg'] for d in s], 'layer': [d['layer'] for d in s],
            'n': [d['n'] for d in s], 'open': [d['open'] for d in s], 'last': [d['last'] for d in s],
            'top_claim': [max(d['claims'], key=d['claims'].get) for d in s],
            'count': res_['count'], 'truncated': res_['truncated']}


def heat(res: int, status: str, kg: str | None, layer: str | None) -> dict:
    """National overview: flagged cells rolled up to H3 res 6..10 → {cells, n, open}."""
    h3 = _h3()
    c = _conn()
    w = ['cell IS NOT NULL']; p: list = []
    if status != 'all':
        w.append('status = 0')
    if kg:
        w.append('kg = ?'); p.append(kg)
    if layer:
        i = _DICT.get(('layer', layer))
        if i is None:
            return {'res': res, 'cells': [], 'n': [], 'open': [], 'reporters': []}
        w.append('layer = ?'); p.append(i)
    rows = c.execute(f'SELECT cell, SUM(n) AS n, SUM(status = 0) AS open, COUNT(*) AS rep FROM report '
                     f'WHERE {" AND ".join(w)} GROUP BY cell', p).fetchall()
    agg: dict = {}
    for r in rows:
        par = h3.cell_to_parent(h3.int_to_str(r['cell']), res) if res < RES else f"{r['cell']:x}"
        a = agg.setdefault(par, [0, 0, 0])
        a[0] += r['n']; a[1] += r['open']; a[2] += r['rep']
    keys = sorted(agg)
    return {'res': res, 'cells': keys, 'n': [agg[k][0] for k in keys], 'open': [agg[k][1] for k in keys],
            'reporters': [agg[k][2] for k in keys], 'subjects_total': len(rows)}


def stats() -> dict:
    c = _conn()
    now = int(time.time())
    tot = c.execute('SELECT COUNT(*) AS rows_, COALESCE(SUM(n),0) AS n, COUNT(DISTINCT skey) AS subjects, '
                    'COUNT(DISTINCT kg) AS kgs, MIN(ts) AS first, MAX(ts_last) AS last FROM report').fetchone()
    by = {}
    for kind in ('layer', 'claim', 'source'):
        by[kind] = {_dict_val(r[0]) or 'null': r[1] for r in
                    c.execute(f'SELECT {kind}, SUM(n) FROM report GROUP BY {kind} ORDER BY 2 DESC')}
    by['status'] = {STATUS[r[0]]: r[1] for r in c.execute('SELECT status, COUNT(*) FROM report GROUP BY status')}
    recent = {f'{h}h': c.execute('SELECT COUNT(*) FROM report WHERE ts_last >= ?', (now - h * 3600,)).fetchone()[0]
              for h in (24, 168)}
    top = [{'kg': r[0], 'open': r[1], 'n': r[2]} for r in
           c.execute('SELECT kg, SUM(status = 0), SUM(n) FROM report WHERE kg IS NOT NULL GROUP BY kg '
                     'ORDER BY 2 DESC, 3 DESC LIMIT 20')]
    try:
        db_b = DB_PATH.stat().st_size
    except OSError:
        db_b = 0
    return {'reports': tot['rows_'], 'n': tot['n'], 'subjects': tot['subjects'], 'kgs': tot['kgs'],
            'first': tot['first'], 'last': tot['last'], 'by': by, 'recent': recent, 'top_kgs': top,
            'trees': __import__('tree_user').stats(), 'db_bytes': db_b, 'limits': _limits(),
            'rate_limits': {'post': _post_limit.stats(), 'get': _get_limit.stats()}}


def _limits() -> dict:
    return {'batch': MAX_BATCH, 'note_chars': MAX_NOTE, 'observed_bytes': MAX_OBSERVED_B,
            'expected_bytes': MAX_EXPECTED_B, 'bbox_span_deg': MAX_BBOX_SPAN_DEG, 'ids': MAX_IDS,
            'rows_out': MAX_ROWS_OUT, 'k': MAX_K, 'daily_per_client': DAILY_CAP,
            'daily_per_reporter_key': DAILY_CAP_KEYED, 'daily_per_self_issued_key': DAILY_CAP_SELF,
            'keys_per_client_per_day': TOKENS_PER_CLIENT_DAY,
            'rate_hint': 'POST ≤ 1 r/s sustained (burst 10); GET ≤ 5 r/s (burst 20)'}


def dict_block() -> dict:
    return {'layers': list(LAYERS), 'claims': list(CLAIMS), 'status': list(STATUS), 'limits': _limits(),
            'subject': 'cell (H3 res 12; res 8–11 ids are snapped to their centre res-12 cell) | lon,lat | kg',
            'snapshot': 'when observed is omitted and the cell is served, we store what we serve now: '
                        '{group, share, h_max, trees_n, structures_n, consistency[, nearest{…}]} + epoch',
            'fold': 'same subject + layer + claim + reporter → one row, n += 1 (reporter = your opaque id, '
                    'else a salted hash of the client IP; neither is stored or returned)',
            'superseded': 'read-side flag: the KG product was rebuilt since the report (epoch differs)',
            'trees': 'layer trees + lon,lat: claim species (= tree) {species, dbh_cm, h_m} describes the tree at the point, '
                     'claim missing [{species, dbh_cm, h_m}] adds one we do not have — versioned per tree; /api/v1/trees/taxa[/dict|/history]'}


# === SECTION: HTTP ===

def _dumps(o) -> bytes:
    return json.dumps(o, separators=(',', ':'), ensure_ascii=False,
                      indent=1 if request.args.get('pretty') else None).encode()


def _resp(body, status: int = 200, max_age: int = CACHE_MAX_AGE) -> Response:
    data = _dumps(body)
    r = Response(data, status=status, mimetype='application/json')
    if status == 200 and request.method == 'GET':
        et = '"f-' + hashlib.sha256(data).hexdigest()[:16] + '"'
        r.headers['ETag'] = et
        r.headers['Cache-Control'] = f'public, max-age={max_age}'
        inm = request.headers.get('If-None-Match') or ''
        if et in [x.strip() for x in inm.split(',')]:
            return Response(status=304, headers={'ETag': et, 'Cache-Control': r.headers['Cache-Control']})
    else:
        r.headers['Cache-Control'] = 'no-store'
    return r


def _meta(**extra) -> dict:
    m = {'service': 'srtm-lidar-at', 'docs': '/api/v1/docs/llm.txt#flags', 'dict': '/api/v1/flags/dict'}
    m.update(extra)
    return m


@flags_bp.errorhandler(FlagError)
def _on_err(e: FlagError):
    d = {'error': e.msg}
    d.update(e.extra)
    return _resp(d, e.status)


def _limited(lim: sg.RateLimiter, cost: float = 1.0):
    ok, _left, ra = lim.take(sg.client_key(request), cost)
    if ok:
        return None
    r = _resp({'error': 'rate_limited', 'retry_after_s': round(ra, 1), 'limits': _limits()['rate_hint']}, 429)
    r.headers['Retry-After'] = str(max(1, int(math.ceil(ra))))
    return r


_keys_cache: list = [None, {}]


def reporter_keys() -> dict:
    """{key: holder}.  Re-read when the file changes."""
    try:
        mt = KEYS_PATH.stat().st_mtime_ns
    except OSError:
        return {}
    if _keys_cache[0] != mt:
        try:
            d = json.loads(KEYS_PATH.read_text())
            _keys_cache[1] = {str(k): str(v)[:MAX_SOURCE] for k, v in d.items()} if isinstance(d, dict) else {}
        except Exception as e:  # noqa: BLE001
            log.warning('reporter_keys unreadable: %s', e)
            _keys_cache[1] = {}
        _keys_cache[0] = mt
    return _keys_cache[1]


def _org_for(key) -> str | None:
    """Holder of a reporter key; None without a key; 401 for a wrong one."""
    if key in (None, ''):
        return None
    key = str(key).strip()
    for k, v in reporter_keys().items():
        if hmac.compare_digest(k.encode(), key.encode()):
            return v
    if key.startswith('trk_') and len(key) <= 80:
        r = _conn().execute('SELECT holder FROM rkey WHERE h=?', (hashlib.sha256(key.encode()).digest(),)).fetchone()
        if r is not None:
            return r[0]
    raise FlagError(401, 'unknown reporter_key (get one: POST /api/v1/flags/token {"name": "…"})')


def _key_cap(org) -> int:
    return DAILY_CAP if not org else DAILY_CAP_SELF if SELF_MARK in org else DAILY_CAP_KEYED


def issue_key(name, client: str) -> dict:
    """A self-issued reporter key: anyone may ask.  The holder is the chosen
    name + a 4-hex suffix (``waldapp~3f9c``) — unique, and visibly not one of
    the names we assign ourselves.  The key is returned once; we keep its hash."""
    import re
    import secrets
    name = re.sub(r'\s+', ' ', str(name or '')).strip()
    if not (2 <= len(name) <= 40) or not re.fullmatch(r'[\w .@/+-]+', name):
        raise FlagError(400, 'name: 2–40 characters (letters, digits, space, . @ / + -) — your app, organisation or handle')
    if not _daily_ok('t:' + client, 1, TOKENS_PER_CLIENT_DAY):
        raise FlagError(429, f'{TOKENS_PER_CLIENT_DAY} keys per client per day')
    c = _conn()
    key = 'trk_' + secrets.token_urlsafe(24)
    h = hashlib.sha256(key.encode()).digest()
    holder = f'{name}{SELF_MARK}{h.hex()[:4]}'
    with _LOCK:
        if c.execute('SELECT COUNT(*) FROM rkey').fetchone()[0] >= MAX_SELF_KEYS:
            raise FlagError(507, 'key store full')
        c.execute('INSERT INTO rkey(h, holder, ts, client) VALUES (?,?,?,?)',
                  (h, holder, int(time.time()), hashlib.sha256((client or '').encode()).hexdigest()[:16]))
        c.commit()
    return {'reporter_key': key, 'holder': holder, 'self_issued': True, 'daily_cap': DAILY_CAP_SELF,
            'send_as': 'header X-Reporter-Key (or Authorization: Bearer …, or body "reporter_key")',
            'note': 'shown once — store it; we keep only its hash. One key per app, or one per user / device.'}


@flags_bp.route('/api/v1/flags/token', methods=['POST'])
def flags_token():
    lim = _limited(_post_limit, cost=3.0)
    if lim is not None:
        return lim
    body = request.get_json(silent=True) or {}
    out = issue_key(body.get('name') if isinstance(body, dict) else None, sg.client_key(request))
    out['meta'] = _meta()
    r = _resp(out, 201)
    r.headers['Cache-Control'] = 'no-store'
    return r


def _request_key(body):
    k = body.get('reporter_key') if isinstance(body, dict) else None
    if k in (None, ''):
        k = request.headers.get('X-Reporter-Key')
    if k in (None, ''):
        a = request.headers.get('Authorization') or ''
        if a.lower().startswith('bearer trk_'):
            k = a[7:]
    return k


def _is_tree_report(it) -> bool:
    return isinstance(it, dict) and (str(it.get('layer') or '').lower() in TREE_LAYERS or _names_tree(it))


def _daily_ok(client: str, n: int, cap: int = DAILY_CAP) -> bool:
    day = int(time.time() // 86400)
    with _LOCK:
        e = _daily.get(client)
        if e is None or e[0] != day:
            if len(_daily) > 20_000:
                _daily.clear()
            e = _daily[client] = [day, 0]
        if e[1] + n > cap:
            return False
        e[1] += n
    return True


@flags_bp.route('/api/v1/flags', methods=['POST'])
def flags_post():
    if request.content_length and request.content_length > MAX_BODY_B:
        raise FlagError(413, f'body larger than {MAX_BODY_B} bytes')
    body = request.get_json(silent=True)
    if body is None:
        raise FlagError(400, 'JSON body required (one report or {"reports": […]})')
    batch = body.get('reports') if isinstance(body, dict) and 'reports' in body else None
    items = batch if batch is not None else [body]
    if not isinstance(items, list) or not items:
        raise FlagError(400, 'reports must be a non-empty list')
    if len(items) > MAX_BATCH:
        raise FlagError(413, f'more than {MAX_BATCH} reports in one call', max_batch=MAX_BATCH)
    lim = _limited(_post_limit, cost=float(len(items)) ** 0.5)
    if lim is not None:
        return lim
    client = sg.client_key(request)
    org = _org_for(_request_key(body))            # one key per call (body, X-Reporter-Key or Bearer)
    cap = _key_cap(org)
    if not _daily_ok(('k:' + org) if org else client, len(items), cap):
        r = _resp({'error': 'daily_cap', 'detail': f'{cap} reports per {"key" if org else "client"} per day'}, 429)
        r.headers['Retry-After'] = str(int(86400 - time.time() % 86400))
        return r
    need_key = not org and KEY_REQUIRED_PATH.exists()
    results = []
    for it in items:
        try:
            if isinstance(it, dict) and it.get('reporter_key') not in (None, '') and batch is not None:
                it_org = _org_for(it['reporter_key'])
            else:
                it_org = org
            if need_key and not it_org and _is_tree_report(it):
                raise FlagError(401, 'tree reports need a reporter_key: POST /api/v1/flags/token {"name": "…"}')
            r = record(it, client, it_org)
            r['http'] = 201 if r.get('tree', {}).get('written', True) else 200
            results.append(r)
        except FlagError as e:
            if batch is None:
                raise
            results.append({'error': e.msg, 'http': e.status, **e.extra})
    if batch is None:
        out = results[0]
        out['meta'] = _meta()
        return _resp(out, out.pop('http'))
    return _resp({'results': results, 'count': len(results),
                  'errors': sum(1 for r in results if 'error' in r), 'meta': _meta()}, 201)


@flags_bp.route('/api/v1/flags', methods=['GET'])
def flags_get():
    lim = _limited(_get_limit)
    if lim is not None:
        return lim
    a = request.args
    res_ = query(a)
    fmt = (a.get('format') or 'json').lower()
    if fmt == 'columns':
        if res_['group'] != 'subject':
            raise FlagError(400, 'format=columns needs group=subject')
        out = columns(res_)
    elif fmt == 'json':
        out = res_
    else:
        raise FlagError(400, 'format must be json|columns')
    out['meta'] = _meta()
    return _resp(out)


@flags_bp.route('/api/v1/flags/dict')
def flags_dict():
    d = dict_block(); d['meta'] = _meta()
    return _resp(d, max_age=3600)


@flags_bp.route('/api/v1/flags/stats')
def flags_stats():
    d = stats(); d['meta'] = _meta()
    return _resp(d, max_age=HEAT_MAX_AGE)


@flags_bp.route('/api/v1/flags/heat')
def flags_heat():
    a = request.args
    try:
        res = int(a.get('res', 7))
    except ValueError:
        raise FlagError(400, 'res must be an integer 6..12')
    if not (6 <= res <= RES):
        raise FlagError(400, 'res must be 6..12')
    status = (a.get('status') or 'open').lower()
    kg = (a.get('kg') or '').strip().zfill(5) if a.get('kg') else None
    layer = (a.get('layer') or '').strip().lower() or None
    key = (res, status, kg, layer)
    hit = _heat_cache.get(key)
    now = time.time()
    if hit and now - hit[0] < HEAT_MAX_AGE:
        d = hit[1]
    else:
        d = heat(res, status, kg, layer)
        d['meta'] = _meta(generated_at=int(now))
        if len(_heat_cache) > 64:
            _heat_cache.clear()
        _heat_cache[key] = (now, d)
    return _resp(d, max_age=HEAT_MAX_AGE)


@flags_bp.route('/api/v1/flags/<int:rid>')
def flags_one(rid: int):
    d = get_one(rid)
    if d is None:
        raise FlagError(404, 'no such report')
    d['tally'] = tally(_conn().execute('SELECT skey FROM report WHERE id=?', (rid,)).fetchone()[0])
    d['meta'] = _meta()
    return _resp(d)


@flags_bp.route('/api/v1/flags/<int:rid>/status', methods=['POST'])
def flags_status(rid: int):
    # Operator-only moderation: needs the configured operator token
    # (``config.operator_token()``); unconfigured ⇒ the route does not exist.
    import hmac
    import config
    tok = config.operator_token()
    sent = request.headers.get('X-Operator-Token') or ''
    if not tok or not hmac.compare_digest(sent, tok):
        raise FlagError(404, 'not_found')
    body = request.get_json(silent=True) or {}
    d = set_status(rid, str(body.get('status') or '').lower(), body.get('note'))
    if d is None:
        raise FlagError(404, 'no such report')
    d['meta'] = _meta()
    return _resp(d)


def tally_for_cell(cid: int) -> dict | None:
    """Cheap per-cell hook for /api/v1/cells/<h3>?flags=1 (one indexed lookup)."""
    try:
        if not DB_PATH.exists():
            return None
        t = tally(cid)
        return t if t['n'] else None
    except Exception:  # noqa: BLE001
        return None


import tree_user  # noqa: E402,F401  — mounts /api/v1/trees/taxa* on flags_bp
