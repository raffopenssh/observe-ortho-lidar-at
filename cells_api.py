"""cells_api — public H3 res-12 cells API (what we measured per ~300 m² cell).

Spec: ``docs/ne-public-api.md``.  Standalone: one Flask ``Blueprint``
(``cells_bp``) + pure functions; imports only ``ne_h3``, ``h3``, ``numpy``,
``sqlite3``, ``pyproj``.  Mounted by ``public_api.py``.  Never imports
``app`` / ``peer_director`` / ``search_index``.

Request path reads only ``data/ne/*.h3b`` + ``data/ne/cells.sqlite`` (+ the
``kg`` table of ``data/search_index.db`` for code→bbox).  Every emitted byte
is rebuilt from the NEO1 / NET1 / NEB1 slices of a chunk; the first slice of
each product chunk (third-party statistics) never leaves this host here.

Vocabulary (public keys): cell, cover, structures, trees, terrain, change,
phenology, consistency.  ``tests/test_cells_vocab.py`` greps this file.
"""
from __future__ import annotations

import csv
import gc
import hashlib
import io
import json
import logging
import math
import os
import sqlite3
import ne_green_fix as _green_fix
import struct
import threading
import time
import zlib
from collections import OrderedDict
from pathlib import Path

import numpy as np
from flask import Blueprint, Response, request, send_file

try:
    import orjson as _orjson
except Exception:  # noqa: BLE001
    _orjson = None

import ne_h3

log = logging.getLogger('cells_api')

# === SECTION: constants ===

DATA_DIR = Path('data')
NE_DIR = DATA_DIR / 'ne'
INDEX_DB = NE_DIR / 'cells.sqlite'
PUB_DIR = NE_DIR / 'pub'
SEARCH_DB = DATA_DIR / 'search_index.db'

RES = ne_h3.H3_RES              # 12
CHUNK_RES = ne_h3.CHUNK_RES     # 10
REC = ne_h3.OBS_REC.size        # 41

MAX_BBOX_SPAN_DEG = 0.05
MAX_CELLS = 16_384
MAX_IDS_GET = 512
MAX_IDS_POST = 16_384
MAX_GEOJSON_CELLS = 4_096
MAX_K = 3
MAX_SQL_IN = 2_000
CHUNK_LRU_N = 4_096
HEADER_LRU_N = 512
PUB_LRU_FILES = 1_000
RETRY_AFTER_S = 3600
CACHE_MAX_AGE = 86_400
LAYERS = ('obs', 'trees', 'structures')
FORMATS = ('json', 'columns', 'bin', 'h3b', 'csv', 'geojson')

import attributions as _attr
ATTRIBUTION = _attr.attribution_short() + ' · Derived landscape segmentation (c) srtm-lidar-at, CC BY 4.0.'
LICENSE_URL = _attr.OUTPUT_LICENSE_URL

# --- forbidden-words-begin (vocabulary rule, docs/ne-public-api.md §0.1; the
#     unit test strips this block before grepping the file)
FORBIDDEN_WORDS = (
    'cadastre', 'cadastral', 'kataster', 'parcel', 'parcels', 'grundstück', 'grundstueck', 'gst',
    'footprint', 'footprints', 'outline', 'outlines', 'gnr', 'ez', 'grundbuch', 'owner', 'owners',
    'address', 'addresses', 'declared', 'register', 'lu', 'kg_share', 'n_parc', 'bld_cover',
)
# --- forbidden-words-end

GROUP_LABELS = {
    'geb': 'buildings', 'bau': 'construction & parking', 'acker': 'cropland',
    'gruen': 'grass / garden / orchard / vineyard / shrub / hedge', 'wald': 'trees',
    'wasser': 'water', 'verkehr': 'roads & paths', 'alpen': 'rock / bare / glacier', 'sonst': 'other',
}
# verdict code → public name (index-aligned with ne_h3.VERDICTS)
_CONSISTENCY_NAMES = ('consistent', 'forest_loss', 'forest_gain', 'sealed_new', 'structure_new',
                      'green_new', 'unknown')
assert len(_CONSISTENCY_NAMES) == len(ne_h3.VERDICTS)
CONSISTENCY_PUBLIC = dict(zip(ne_h3.VERDICTS, _CONSISTENCY_NAMES))
ASPECTS = ('N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'flat')
PHENOLOGY = ne_h3.PHENOLOGY_CODES
SPECIES = ne_h3.SPECIES_CODES
VITALITY = ne_h3.VITALITY_CODES
STRUCTURE_TYPES = ne_h3.BLD_TYPES

# NEO1 row as a numpy structured dtype (41 B, unaligned, little-endian)
OBS_DT = np.dtype([
    ('id', '<u8'), ('cover', 'u1', (9,)), ('sc', 'u1'), ('sn', 'u1'), ('hm', 'u1'), ('h90', 'u1'),
    ('hx', 'u1'), ('tn', 'u1'), ('thx', 'u1'), ('cf', 'u1'), ('nm', 'u1'), ('na', 'u1'), ('dh', 'i1'),
    ('fy', 'u1'), ('vd', 'u1'), ('el', '<u2'), ('sl', 'u1'), ('s90', 'u1'), ('asp', 'u1'), ('ro', 'u1'),
    ('tri', 'u1'), ('tpi', 'i1'), ('dtm', 'i1'), ('hl', 'u1'), ('ph', 'u1')])
assert OBS_DT.itemsize == REC

# public flat field names (``fields=``) → (struct field, decode kind)
#   kinds: u8h = /2 m (255 nodata) · u8f254 = /254 · ndvi = v/254*2-1 · i8q = /4 (-128 nodata)
#          u16 = m (65535 nodata) · u8 = int (255 nodata) · u8d10 = /10 · cat = code table
OBS_PUBLIC = OrderedDict([
    ('cover', ('cover', 'cover')), ('structures_cover', ('sc', 'u8f255')), ('structures_n', ('sn', 'int')),
    ('h_mean', ('hm', 'u8h')), ('h_p90', ('h90', 'u8h')), ('h_max', ('hx', 'u8h')),
    ('tree_n', ('tn', 'int')), ('tree_h_max', ('thx', 'u8h')), ('canopy', ('cf', 'u8f254')),
    ('ndvi_mean', ('nm', 'ndvi')), ('ndvi_amp', ('na', 'u8f254')), ('dh_m', ('dh', 'i8q')),
    ('als_years', ('fy', 'years')), ('consistency', ('vd', 'consistency')),
    ('elev_m', ('el', 'u16')), ('slope', ('sl', 'u8')), ('slope_p90', ('s90', 'u8')),
    ('aspect', ('asp', 'aspect')), ('roughness', ('ro', 'u8d10')), ('tri', ('tri', 'u8d10')),
    ('tpi', ('tpi', 'i8q')), ('dtm_m', ('dtm', 'i8q')), ('forest_loss_year', ('hl', 'lossyear')),
    ('phenology', ('ph', 'phenology')),
])
# nested JSON layout: public field → (group, key)
JSON_GROUPS = {
    'h_mean': ('height', 'mean'), 'h_p90': ('height', 'p90'), 'h_max': ('height', 'max'),
    'tree_n': ('trees', 'n'), 'tree_h_max': ('trees', 'h_max'), 'canopy': ('trees', 'canopy'),
    'ndvi_mean': ('ndvi', 'mean'), 'ndvi_amp': ('ndvi', 'amp'),
    'dh_m': ('change', 'dh_m'), 'dtm_m': ('change', 'dtm_m'), 'forest_loss_year': ('change', 'forest_loss_year'),
    'als_years': ('change', 'als_years'),
    'elev_m': ('terrain', 'elev_m'), 'slope': ('terrain', 'slope'), 'slope_p90': ('terrain', 'slope_p90'),
    'aspect': ('terrain', 'aspect'), 'roughness': ('terrain', 'roughness'), 'tri': ('terrain', 'tri'),
    'tpi': ('terrain', 'tpi'),
}
UNITS = {
    'cover': 'fraction of cell area per group, groups[] order (columns: u8 /255)',
    'structures_cover': 'fraction of cell area under segmented structures (columns: u8 /255)',
    'structures_n': 'count (saturates at 255)',
    'h_mean/h_p90/h_max/tree_h_max': 'm above ground (columns: u8 ×0.5 m, 255 = null)',
    'tree_n': 'count of tree apices h ≥ 3 m (saturates at 255)',
    'canopy': 'crown-area fraction of the cell (columns: u8 /254, 255 = null)',
    'ndvi_mean': 'Sentinel-2 NDVI mean (columns: u8 → v/254·2−1, 255 = null)',
    'ndvi_amp': 'seasonal NDVI amplitude (columns: u8 /254, 255 = null)',
    'dh_m': 'nDSM change newest−oldest ALS flight, m (columns: i8 ×0.25, −128 = null)',
    'dtm_m': 'terrain change newest−oldest, m (columns: i8 ×0.25, −128 = null)',
    'als_years': 'ALS flight years seen (columns: bitmask, bit i = 2020+i)',
    'elev_m': 'm a.s.l. (columns: u16, 65535 = null)',
    'slope/slope_p90': 'degrees (columns: u8, 255 = null)',
    'aspect': 'dominant aspect N NE E SE S SW W NW flat (columns: 0..8, 255 = null)',
    'roughness': 'DSM local std, m (columns: u8 /10, 255 = null)',
    'tri': 'terrain ruggedness index, m (columns: u8 /10, 255 = null)',
    'tpi': 'topographic position index r=5, m (columns: i8 ×0.25, −128 = null)',
    'forest_loss_year': 'Hansen GFC loss year or null (columns: 0 none, else year−2000, 255 = null)',
    'phenology': 'index into phenology[] (columns: u8, 255 = null)',
    'consistency': 'index into consistency[] (columns: u8)',
    'trees[]': 'dx/dy m from the metre-snapped EPSG:3035 cell centre; h_m ×0.25; crown_m2 ≈ 2^(q/5)',
    'structures[]': 'dx/dy m; h_max_m / h_robust_m ×0.25; area_m2; conf /15; dh_m ×0.25',
}

_NEO1_REC_FIELDS = list(OBS_PUBLIC.keys())

# === SECTION: sqlite chunk index ===

SCHEMA = """
CREATE TABLE IF NOT EXISTS chunk (r10 INTEGER NOT NULL, kg TEXT NOT NULL, off INTEGER, len INTEGER,
                    raw_len INTEGER, cells INTEGER, trees INTEGER, bld INTEGER,
                    PRIMARY KEY (r10, kg)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS file (kg TEXT PRIMARY KEY, body_offset INTEGER, cells_n INTEGER, epoch TEXT,
                    product_version TEXT, neo1_digest TEXT, partial INTEGER, mtime REAL,
                    w REAL, s REAL, e REAL, n REAL);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""
_local = threading.local()
_idx_write_lock = threading.Lock()


def _idx_conn(write: bool = False) -> sqlite3.Connection | None:
    c = getattr(_local, 'cells_idx', None)
    if c is not None:
        return c
    if not INDEX_DB.exists() and not write:
        return None
    INDEX_DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(INDEX_DB), check_same_thread=False, timeout=15)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA synchronous=NORMAL')
    c.execute('PRAGMA busy_timeout=15000')
    c.executescript(SCHEMA)
    _local.cells_idx = c
    return c


def _search_conn() -> sqlite3.Connection | None:
    c = getattr(_local, 'cells_search', None)
    if c is None:
        if not SEARCH_DB.exists():
            return None
        c = sqlite3.connect(f'file:{SEARCH_DB}?mode=ro', uri=True, check_same_thread=False, timeout=10)
        c.execute('PRAGMA busy_timeout=10000')
        _local.cells_search = c
    return c


def _r10_bbox(r10_hexes: list[str]) -> tuple[float, float, float, float] | None:
    import h3.api.numpy_int as hn
    if not r10_hexes:
        return None
    w = s = math.inf
    e = n = -math.inf
    for h in r10_hexes:
        for lat, lon in hn.cell_to_boundary(int(h, 16)):
            w = min(w, lon); e = max(e, lon); s = min(s, lat); n = max(n, lat)
    return (w, s, e, n)


def index_file(path: str | Path, *, conn: sqlite3.Connection | None = None) -> dict:
    """(Re)insert one ``data/ne/<kg>.h3b`` into ``cells.sqlite``.  Called by
    ``ne_ingest`` after every successful write / merge."""
    p = Path(path)
    from kg_splitter import is_sweep_code
    if is_sweep_code(p.stem) or p.parent.name == 'sweeps' or p.name.startswith('.'):
        # sliver-sweep unit files (data/ne/sweeps/<parent>-sw<k>.h3b) and
        # merge temps are never indexed — only the merged data/ne/<kg>.h3b.
        raise ValueError(f'not a corpus file: {p.name}')
    hdr = ne_h3.read_h3b_header(p)
    kg = str(hdr['kg']).zfill(5)
    chunks = hdr.get('chunks') or []
    bb = _r10_bbox([c['r10'] for c in chunks]) or (None, None, None, None)
    rows = [(int(c['r10'], 16), kg, int(c['off']), int(c['len']), int(c.get('raw_len') or 0),
             int(c['cells']), int(c.get('trees') or 0), int(c.get('bld') or 0)) for c in chunks]
    c = conn or _idx_conn(write=True)
    with _idx_write_lock:
        c.execute('BEGIN IMMEDIATE')
        try:
            c.execute('DELETE FROM chunk WHERE kg=?', (kg,))
            c.executemany('INSERT INTO chunk VALUES (?,?,?,?,?,?,?,?)', rows)
            c.execute('INSERT OR REPLACE INTO file VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                      (kg, int(hdr['body_offset']), int(hdr['cells_n']), (hdr.get('neo1') or {}).get('epoch'),
                       hdr.get('product_version'), (hdr.get('neo1') or {}).get('digest'),
                       1 if hdr.get('partial_cells') else 0, p.stat().st_mtime, *bb))
            c.execute("INSERT OR REPLACE INTO meta VALUES ('schema','1')")
            c.execute('COMMIT')
        except Exception:
            c.execute('ROLLBACK')
            raise
    _header_cache_drop(kg)
    _files_cache['t'] = 0.0
    return {'kg': kg, 'chunks': len(rows), 'cells_n': hdr['cells_n']}


def index_remove(kg: str) -> None:
    c = _idx_conn(write=True)
    with _idx_write_lock:
        c.execute('DELETE FROM chunk WHERE kg=?', (kg,))
        c.execute('DELETE FROM file WHERE kg=?', (kg,))
        c.commit()
    _header_cache_drop(kg)
    _files_cache['t'] = 0.0


def rebuild_index() -> dict:
    """Rescan ``data/ne/*.h3b`` headers into a fresh index."""
    t0 = time.time()
    c = _idx_conn(write=True)
    with _idx_write_lock:
        c.execute('DELETE FROM chunk'); c.execute('DELETE FROM file'); c.commit()
    n = 0
    for p in sorted(NE_DIR.glob('*.h3b')):
        if p.name.startswith('.'):
            continue
        try:
            index_file(p, conn=c)
            n += 1
        except Exception as ex:  # noqa: BLE001
            log.warning('cells index: %s skipped: %s', p.name, ex)
    with _idx_write_lock:
        c.execute("INSERT OR REPLACE INTO meta VALUES ('rebuilt_at',?)", (time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),))
        c.execute("INSERT OR REPLACE INTO meta VALUES ('schema','1')")
        c.commit()
    _chunk_lru.clear(); _header_lru.clear()
    _files_cache['t'] = 0.0
    return {'files': n, 'seconds': round(time.time() - t0, 2)}


_files_cache: dict = {'t': 0.0, 'v': {}}


def _files() -> dict:
    """kg → file row (dict), cached 30 s."""
    now = time.time()
    if now - _files_cache['t'] < 30:
        return _files_cache['v']
    c = _idx_conn()
    v = {}
    if c is not None:
        for row in c.execute('SELECT kg, body_offset, cells_n, epoch, product_version, neo1_digest, partial, mtime, w, s, e, n FROM file'):
            v[row[0]] = dict(zip(('kg', 'body_offset', 'cells_n', 'epoch', 'product_version', 'neo1_digest',
                                  'partial', 'mtime', 'w', 's', 'e', 'n'), row))
    _files_cache.update(t=now, v=v)
    return v


def has_kg(code: str) -> bool:
    return str(code).zfill(5) in _files()


def _registry_versions(codes) -> dict:
    """kg → product_version from the KG list (3-line sqlite read)."""
    codes = [str(c).zfill(5) for c in codes]
    if not codes:
        return {}
    c = _search_conn()
    if c is None:
        return {}
    out = {}
    for i in range(0, len(codes), 500):
        part = codes[i:i + 500]
        q = f'SELECT kg_code, product_version FROM kg WHERE kg_code IN ({",".join("?" * len(part))})'
        out.update(dict(c.execute(q, part).fetchall()))
    return out


def _registry_has_cells_product(pv) -> bool:
    """Registry says a cells product (v2.4+) was generated for this KG."""
    return bool(pv) and str(pv).startswith('v2.4')


def _iso(ts) -> str | None:
    if not ts:
        return None
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(float(ts)))


def ne_state(code: str, product_version=None) -> dict:
    """Readiness of the cells layer for ONE KG — the oracle behind the
    public invariant ``product_version == "v2.4" ⇒ /api/v1/cells serves it``.

    ``ne_status``: ``served`` (complete file on disk, indexed),
    ``partial`` (split parent still merging — covered cells are served,
    ``meta.partial:true``), ``pending`` (the KG list has a v2.4 product but
    the cells file has not been ingested on this host yet — poll),
    ``not_processed`` (no cells product generated).  ``ne_ready`` is true
    only for ``served``.
    """
    code = str(code).zfill(5)
    f = _files().get(code)
    if f is not None:
        st = 'partial' if f.get('partial') else 'served'
        return {'ne_ready': st == 'served', 'ne_status': st, 'ne_cells_n': int(f.get('cells_n') or 0),
                'ne_cells_published_at': _iso(f.get('mtime')), 'ne_epoch': f.get('epoch'),
                'retry_after_s': RETRY_AFTER_S if st != 'served' else None}
    if product_version is None:
        product_version = _registry_versions([code]).get(code)
    st = 'pending' if _registry_has_cells_product(product_version) else 'not_processed'
    return {'ne_ready': False, 'ne_status': st, 'ne_cells_n': 0, 'ne_cells_published_at': None, 'ne_epoch': None,
            'retry_after_s': RETRY_AFTER_S if st == 'pending' else None}


def split_missing(missing: list[str]) -> tuple[list[str], list[str]]:
    """Intersecting KGs without a cells file → (pending, not_processed)."""
    if not missing:
        return [], []
    pv = _registry_versions(missing)
    pend = [k for k in missing if _registry_has_cells_product(pv.get(k))]
    return pend, [k for k in missing if k not in set(pend)]


def cells_generation() -> str:
    """Opaque token that changes whenever a KG's cells go live (or are
    re-ingested): count + newest mtime + sum of cells over the index.
    Folded into the KG-list ETags (``/api/v1/kgs``, ``/api/v1/status``)
    so ETag revalidation picks up cells publication, not only KG rows."""
    fs = _files()
    if not fs:
        return 'c0'
    newest = max((f.get('mtime') or 0) for f in fs.values())
    tot = sum(int(f.get('cells_n') or 0) for f in fs.values())
    npart = sum(1 for f in fs.values() if f.get('partial'))
    return f'c{len(fs)}-{npart}-{int(newest)}-{tot}'


def index_status() -> dict:
    c = _idx_conn()
    if c is None:
        return {'kgs_ready': 0, 'cells_total': 0, 'index_rebuilt_at': None, 'ready': False}
    kgs, cells = c.execute('SELECT COUNT(*), COALESCE(SUM(cells_n),0) FROM file').fetchone()
    meta = dict(c.execute('SELECT k, v FROM meta').fetchall())
    npart = c.execute('SELECT COUNT(*) FROM file WHERE partial=1').fetchone()[0]
    return {'kgs_ready': int(kgs) - int(npart), 'kgs_partial': int(npart), 'cells_total': int(cells),
            'index_rebuilt_at': meta.get('rebuilt_at'), 'ready': bool(kgs), 'epoch': _epoch_pin(),
            'generation': cells_generation()}


def _epoch_pin() -> str | None:
    eps = sorted({f['epoch'] for f in _files().values() if f.get('epoch')})
    return eps[-1] if eps else None


def _kgs_intersecting(w, s, e, n) -> list[str]:
    c = _search_conn()
    if c is None:
        return []
    try:
        rows = c.execute('SELECT k.kg_code FROM kg_rtree r JOIN kg k ON k.rowid = r.id '
                         'WHERE r.min_lon <= ? AND r.max_lon >= ? AND r.min_lat <= ? AND r.max_lat >= ?',
                         (e, w, n, s)).fetchall()
    except sqlite3.Error:
        rows = c.execute('SELECT kg_code FROM kg WHERE min_lon <= ? AND max_lon >= ? AND min_lat <= ? AND max_lat >= ?',
                         (e, w, n, s)).fetchall()
    return sorted(r[0] for r in rows)


def _kg_bbox(code: str):
    c = _search_conn()
    if c is None:
        return None
    r = c.execute('SELECT min_lon, min_lat, max_lon, max_lat FROM kg WHERE kg_code=?', (code,)).fetchone()
    return tuple(r) if r and r[0] is not None else None


# === SECTION: caches (header LRU, chunk LRU) ===

import mem_lru as _mem_lru


def _chunk_bytes(ch) -> int:
    # obs/trees/bld raw + offsets + centres/pub_z once materialised (~3× raw worst case)
    o, t, b = ch.raw_pub
    return 3 * (len(o) + len(t) + len(b)) + 512


_header_lru = _mem_lru.ByteLRU('cells_headers', 24, env='CELLS_HEADER_MB')
_chunk_lru = _mem_lru.ByteLRU('cells_chunks', 64, sizer=_chunk_bytes, env='CELLS_CHUNK_MB')


def _header_cache_drop(kg: str) -> None:
    _header_lru.pop(kg)
    _chunk_lru.pop_if(lambda k: k[0] == kg)


def _lru_get(d, key):
    return d.get(key)


def _lru_put(d, key, val, cap=None):
    d.put(key, val)


def _file_path(kg: str) -> Path:
    return NE_DIR / f'{kg}.h3b'


def header(kg: str, with_chunks: bool = False) -> dict | None:
    """Slim header of a product file (no ``chunks`` unless asked), validated
    against the file mtime."""
    kg = str(kg).zfill(5)
    p = _file_path(kg)
    try:
        mt = p.stat().st_mtime
    except FileNotFoundError:
        return None
    h = _lru_get(_header_lru, kg)
    if h is not None and h['_mtime'] == mt and (not with_chunks or 'chunks' in h):
        return h
    full = ne_h3.read_h3b_header(p)
    h = {k: full.get(k) for k in ('kg', 'body_offset', 'cells_n', 'product_version', 'partial_cells',
                                   'record_bytes', 'chunk_layout', 'neo1', 'net1', 'neb1', 'h3_res',
                                   'chunk_res', 'created_at', 'merged_from')}
    h['epoch'] = (full.get('neo1') or {}).get('epoch')
    h['neo1_digest'] = (full.get('neo1') or {}).get('digest')
    h['summary'] = public_summary(full.get('summary') or {})
    h['_mtime'] = mt
    if with_chunks:
        h['chunks'] = full.get('chunks') or []
    _lru_put(_header_lru, kg, h, HEADER_LRU_N)
    return h


def public_summary(s: dict) -> dict:
    """Allow-listed, renamed view of the product header ``summary``."""
    obs = s.get('observed') or {}
    out = {
        'epoch': obs.get('epoch'), 'source': obs.get('source'), 'cells_observed': obs.get('cells_observed'),
        'trees_n': obs.get('trees_n'), 'structures_n': obs.get('manmade_n'),
        'cover_ha': s.get('observed_ha'), 'terrain': s.get('terrain'),
        'h3_res': s.get('h3_res'), 'chunk_res': s.get('chunk_res'), 'groups': s.get('groups'),
    }
    vn = s.get('verdict_n') or {}
    vh = s.get('verdict_ha') or {}
    if vn:
        out['consistency_n'] = {CONSISTENCY_PUBLIC.get(k, k): v for k, v in vn.items()}
    if vh:
        out['consistency_ha'] = {CONSISTENCY_PUBLIC.get(k, k): v for k, v in vh.items()}
    return out


class Chunk:
    """One inflated res-10 chunk: NEO1 rows + NET1 / NEB1 rows + offsets."""
    __slots__ = ('kg', 'r10', 'ids', 'obs', 'trees', 'tree_off', 'bld', 'bld_off', 'cells', 'pub_z',
                 'raw_pub', 'row', '_cen')

    def __init__(self, kg: str, row: dict, parts: dict):
        self.kg = kg
        self.row = row
        self.r10 = int(row['r10'])
        n = int(row['cells'])
        self.cells = n
        self.obs = np.frombuffer(parts['obs'], OBS_DT, n)
        self.ids = self.obs['id']
        self.trees = np.frombuffer(parts['trees'], '<u4')
        self.bld = np.frombuffer(parts['bld'], '<u8')
        tn = self.obs['tn'].astype(np.int64); bn = self.obs['sn'].astype(np.int64)
        self.tree_off = np.concatenate([[0], np.cumsum(tn)])
        self.bld_off = np.concatenate([[0], np.cumsum(bn)])
        self.raw_pub = (parts['obs'], parts['trees'], parts['bld'])
        self.pub_z = None
        self._cen = None

    def centres(self) -> np.ndarray:
        """(n, 2) metre-snapped EPSG:3035 centres, computed once per chunk."""
        if self._cen is None:
            self._cen = _centres_3035(self.ids)
        return self._cen

    def public_deflated(self) -> bytes:
        """zlib(shuffle(NEO1)|shuffle(NET1)|shuffle(NEB1)) — the chunk minus the first slice."""
        if self.pub_z is None:
            o, t, b = self.raw_pub
            self.pub_z = zlib.compress(ne_h3._shuffle(o, REC) + ne_h3._shuffle(t, 4) + ne_h3._shuffle(b, 8), 6)
        return self.pub_z


def _load_chunk(kg: str, row: dict, hdr: dict) -> Chunk:
    key = (kg, hdr['neo1_digest'], int(row['r10']), _green_fix.FIX_ID)
    ch = _lru_get(_chunk_lru, key)
    if ch is not None:
        return ch
    p = _file_path(kg)
    with open(p, 'rb') as fh:
        fh.seek(int(hdr['body_offset']) + int(row['off']))
        raw = zlib.decompress(fh.read(int(row['len'])))
    n, t, b = int(row['cells']), int(row['trees']), int(row['bld'])
    rb = hdr.get('record_bytes') or {}
    r0, r1, r2, r3 = int(rb.get('nec1', 30)), int(rb.get('neo1', REC)), int(rb.get('net1', 4)), int(rb.get('neb1', 8))
    o1 = n * r0; o2 = o1 + n * r1; o3 = o2 + t * r2
    shuffled = 'shuffle' in str(hdr.get('chunk_layout') or '')
    if shuffled:
        parts = {'obs': ne_h3._unshuffle(raw[o1:o2], r1, n), 'trees': ne_h3._unshuffle(raw[o2:o3], r2, t),
                 'bld': ne_h3._unshuffle(raw[o3:o3 + b * r3], r3, b)}
        ctx_rows = ne_h3._unshuffle(raw[:o1], r0, n)
    else:
        parts = {'obs': raw[o1:o2], 'trees': raw[o2:o3], 'bld': raw[o3:o3 + b * r3]}
        ctx_rows = raw[:o1]
    # read-time green-ground override (bau/alpen on S2-green cells → gruen);
    # the file is untouched — see ne_green_fix.py
    parts['obs'], _ = _green_fix.fix_rows(parts["obs"], ctx_rows)
    ch = Chunk(kg, row, parts)
    _lru_put(_chunk_lru, key, ch, CHUNK_LRU_N)
    return ch


def _chunk_rows(r10s: np.ndarray) -> list[dict]:
    c = _idx_conn()
    if c is None or r10s.size == 0:
        return []
    out = []
    ids = [int(x) for x in np.unique(r10s).tolist()]
    for i in range(0, len(ids), MAX_SQL_IN):
        part = ids[i:i + MAX_SQL_IN]
        q = ('SELECT r10, kg, off, len, raw_len, cells, trees, bld FROM chunk WHERE r10 IN (%s)'
             % ','.join('?' * len(part)))
        for row in c.execute(q, part):
            out.append(dict(zip(('r10', 'kg', 'off', 'len', 'raw_len', 'cells', 'trees', 'bld'), row)))
    return out


def chunks_for_r10(r10s: np.ndarray) -> tuple[list[Chunk], dict]:
    """All chunks (across KGs) for the given res-10 ids + the file headers touched."""
    rows = _chunk_rows(r10s)
    hdrs: dict = {}
    chunks = []
    for row in rows:
        kg = row['kg']
        h = hdrs.get(kg)
        if h is None:
            h = header(kg)
            if h is None:
                continue
            hdrs[kg] = h
        try:
            chunks.append(_load_chunk(kg, row, h))
        except Exception as ex:  # noqa: BLE001
            log.warning('cells: chunk %s/%x unreadable: %s', kg, int(row['r10']), ex)
    return chunks, hdrs


# === SECTION: gather + dedupe ===

class Table:
    """Selected res-12 cells: ids (sorted), structured obs rows, kg per row,
    and the owning chunk/row for layer gathers."""
    __slots__ = ('ids', 'obs', 'kg_idx', 'kgs', 'src_chunk', 'src_row', 'chunks', 'trees_truncated')

    def __init__(self, ids, obs, kg_idx, kgs, src_chunk, src_row, chunks):
        self.ids = ids; self.obs = obs; self.kg_idx = kg_idx; self.kgs = kgs
        self.src_chunk = src_chunk; self.src_row = src_row; self.chunks = chunks
        self.trees_truncated = int((obs['tn'] == 255).sum()) if len(obs) else 0

    def __len__(self):
        return int(self.ids.size)

    def kg_col(self) -> np.ndarray:
        return np.asarray(self.kgs, dtype=object)[self.kg_idx] if len(self.kgs) else np.zeros(0, object)

    def gather_trees(self):
        """→ (cell_row_index, u32 words) for all trees of the selected cells."""
        return _gather_rows(self, 'tree_off', 'trees')

    def gather_structures(self):
        return _gather_rows(self, 'bld_off', 'bld')


def _gather_rows(t: Table, off_attr: str, arr_attr: str):
    n = len(t)
    if n == 0:
        return np.zeros(0, np.int64), np.zeros(0, np.uint64)
    base = np.zeros(len(t.chunks) + 1, np.int64)
    for i, ch in enumerate(t.chunks):
        base[i + 1] = base[i] + len(getattr(ch, arr_attr))
    allw = np.concatenate([getattr(ch, arr_attr) for ch in t.chunks]) if t.chunks else np.zeros(0)
    starts = np.empty(n, np.int64); cnt = np.empty(n, np.int64)
    for i, ch in enumerate(t.chunks):
        m = t.src_chunk == i
        if not m.any():
            continue
        off = getattr(ch, off_attr)
        r = t.src_row[m]
        starts[m] = base[i] + off[r]
        cnt[m] = off[r + 1] - off[r]
    total = int(cnt.sum())
    if total == 0:
        return np.zeros(0, np.int64), allw[:0]
    cell_idx = np.repeat(np.arange(n), cnt)
    idx = np.arange(total) - np.repeat(np.cumsum(cnt) - cnt, cnt) + np.repeat(starts, cnt)
    return cell_idx, allw[idx]


def gather(chunks: list[Chunk], want_ids: np.ndarray | None) -> Table:
    """Concatenate chunk rows, keep ``want_ids`` (None = all), dedupe cells
    that appear in several KGs (observed copy first, then the larger chunk,
    then the lowest kg)."""
    if not chunks:
        return Table(np.zeros(0, np.uint64), np.zeros(0, OBS_DT), np.zeros(0, np.int64), [], np.zeros(0, np.int64),
                     np.zeros(0, np.int64), [])
    kgs = sorted({c.kg for c in chunks})
    kg_rank = {k: i for i, k in enumerate(kgs)}
    ids = np.concatenate([c.ids for c in chunks])
    obs = np.concatenate([c.obs for c in chunks])
    src_chunk = np.concatenate([np.full(c.cells, i, np.int64) for i, c in enumerate(chunks)])
    src_row = np.concatenate([np.arange(c.cells, dtype=np.int64) for c in chunks])
    if want_ids is not None:
        m = np.isin(ids, want_ids)
        ids, obs, src_chunk, src_row = ids[m], obs[m], src_chunk[m], src_row[m]
    if ids.size == 0:
        return Table(ids, obs, np.zeros(0, np.int64), kgs, src_chunk, src_row, chunks)
    observed = (obs['hm'] != 255) | (obs['cover'].sum(axis=1) > 0)
    csize = np.array([c.cells for c in chunks], np.int64)[src_chunk]
    krank = np.array([kg_rank[c.kg] for c in chunks], np.int64)[src_chunk]
    order = np.lexsort((krank, -csize, ~observed, ids))        # last key = primary
    ids_s = ids[order]
    first = np.concatenate([[True], ids_s[1:] != ids_s[:-1]])
    sel = order[first]
    return Table(ids[sel], obs[sel], krank[sel], kgs, src_chunk[sel], src_row[sel], chunks)


# === SECTION: decode (quantised → public values) ===

def _nan_where(v: np.ndarray, bad: np.ndarray) -> np.ndarray:
    out = v.astype(np.float64)
    out[bad] = np.nan
    return out


def decode_obs(obs: np.ndarray, fields: list[str] | None = None) -> OrderedDict:
    """Structured NEO1 rows → dict of public columns (float64 with NaN for
    null, int for counts, (n,9) float for cover, str codes for categories)."""
    out = OrderedDict()
    for name, (f, kind) in OBS_PUBLIC.items():
        if fields and name not in fields:
            continue
        v = obs[f]
        if kind == 'cover':
            out[name] = v.astype(np.float64) / 255.0
        elif kind == 'u8f255':
            out[name] = v.astype(np.float64) / 255.0
        elif kind == 'int':
            out[name] = v.astype(np.int64)
        elif kind == 'u8h':
            out[name] = _nan_where(v, v == 255) / 2.0
        elif kind == 'u8f254':
            out[name] = _nan_where(v, v == 255) / 254.0
        elif kind == 'ndvi':
            out[name] = _nan_where(v, v == 255) / 254.0 * 2.0 - 1.0
        elif kind == 'i8q':
            out[name] = _nan_where(v, v == -128) / 4.0
        elif kind == 'u16':
            out[name] = _nan_where(v, v == 65535)
        elif kind == 'u8':
            out[name] = _nan_where(v, v == 255)
        elif kind == 'u8d10':
            out[name] = _nan_where(v, v == 255) / 10.0
        elif kind == 'lossyear':
            x = _nan_where(v, (v == 255) | (v == 0))
            out[name] = x + 2000.0
        elif kind == 'years':
            out[name] = v.astype(np.int64)                  # bitmask; rendered per format
        elif kind in ('consistency', 'phenology', 'aspect'):
            out[name] = v.astype(np.int64)                  # code; rendered per format
    return out


def _years_list(mask: int) -> list[int]:
    return [ne_h3.FLIGHT_YEAR0 + i for i in range(8) if mask & (1 << i)]


def _cat(codes: np.ndarray, table) -> list:
    tab = list(table)
    n = len(tab)
    return [tab[c] if 0 <= c < n else None for c in codes.tolist()]


def _col_list(v: np.ndarray, nd: int = 2) -> list:
    """numpy column → JSON-safe Python list (NaN → None, rounded floats)."""
    if v.dtype.kind == 'f':
        r = np.round(v, nd)
        o = r.astype(object)
        o[np.isnan(v)] = None
        return o.tolist()
    return v.tolist()


_ROUND = {'cover': 3, 'structures_cover': 3, 'canopy': 3, 'ndvi_mean': 3, 'ndvi_amp': 3, 'dh_m': 2, 'dtm_m': 2,
          'elev_m': 0, 'slope': 0, 'slope_p90': 0, 'roughness': 1, 'tri': 1, 'tpi': 2, 'h_mean': 1, 'h_p90': 1,
          'h_max': 1, 'tree_h_max': 1}


def _public_lists(dec: OrderedDict, *, cover_mean: bool = False) -> OrderedDict:
    """Decoded columns → per-field Python lists with codes rendered as strings."""
    out = OrderedDict()
    for name, v in dec.items():
        if name == 'cover':
            r = np.round(v, 3)
            out[name] = [dict(zip(ne_h3.GROUPS, row)) for row in r.tolist()]
        elif name == 'als_years':
            out[name] = [_years_list(m) for m in v.tolist()]
        elif name == 'consistency':
            out[name] = _cat(v, _CONSISTENCY_NAMES)
        elif name == 'phenology':
            out[name] = _cat(v, PHENOLOGY)
        elif name == 'aspect':
            out[name] = _cat(v, ASPECTS)
        elif name == 'forest_loss_year':
            o = v.astype(object); o[np.isnan(v)] = None
            out[name] = [int(x) if x is not None else None for x in o.tolist()]
        else:
            out[name] = _col_list(v, _ROUND.get(name, 2))
    return out


def decode_trees(words: np.ndarray) -> dict:
    w = words.astype(np.uint32)
    dx = (w & 31).astype(np.int64); dx[dx >= 16] -= 32
    dy = ((w >> 5) & 31).astype(np.int64); dy[dy >= 16] -= 32
    return {'dx': dx, 'dy': dy, 'h_m': ((w >> 10) & 255).astype(np.float64) / 4.0,
            'crown_m2': np.round(np.power(2.0, ((w >> 18) & 63).astype(np.float64) / 5.0), 1),
            'species': ((w >> 24) & 15).astype(np.int64), 'vitality': ((w >> 28) & 7).astype(np.int64),
            'ndsm_only': (w >> 31).astype(bool)}


def decode_structures(words: np.ndarray) -> dict:
    w = words.astype(np.uint64)
    dx = (w & np.uint64(31)).astype(np.int64); dx[dx >= 16] -= 32
    dy = ((w >> np.uint64(5)) & np.uint64(31)).astype(np.int64); dy[dy >= 16] -= 32
    dh = ((w >> np.uint64(50)) & np.uint64(255)).astype(np.int64); dh[dh >= 128] -= 256
    return {'dx': dx, 'dy': dy,
            'h_max_m': ((w >> np.uint64(10)) & np.uint64(255)).astype(np.float64) / 4.0,
            'h_robust_m': ((w >> np.uint64(18)) & np.uint64(255)).astype(np.float64) / 4.0,
            'area_m2': ((w >> np.uint64(26)) & np.uint64(0xFFFF)).astype(np.int64),
            'type': ((w >> np.uint64(42)) & np.uint64(15)).astype(np.int64),
            'conf': np.round(((w >> np.uint64(46)) & np.uint64(15)).astype(np.float64) / 15.0, 2),
            'dh_m': dh.astype(np.float64) / 4.0}


_TF = {}


def _tf(src: str, dst: str):
    k = (src, dst)
    if k not in _TF:
        from pyproj import Transformer
        _TF[k] = Transformer.from_crs(src, dst, always_xy=True)
    return _TF[k]


def _centres_3035(ids: np.ndarray) -> np.ndarray:
    """(n, 2) metre-snapped EPSG:3035 centres (same rounding as ne_h3.cell_centres_3035)."""
    if ids.size == 0:
        return np.zeros((0, 2))
    hn = _h3n()
    ll = np.array([hn.cell_to_latlng(c) for c in ids.tolist()])
    e, n = _tf('EPSG:4326', 'EPSG:3035').transform(ll[:, 1], ll[:, 0])
    return np.stack([np.round(e), np.round(n)], axis=1)


def table_centres(t: 'Table') -> np.ndarray:
    """(n, 2) EPSG:3035 centres of the selected cells, served from the per-chunk cache."""
    out = np.empty((len(t), 2))
    for i, ch in enumerate(t.chunks):
        m = t.src_chunk == i
        if m.any():
            out[m] = ch.centres()[t.src_row[m]]
    return out


def cell_centres(ids: np.ndarray, t: 'Table | None' = None) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """→ (e, n) metre-snapped EPSG:3035 + (lon, lat) WGS84 of the snapped centre."""
    if t is not None and ids is t.ids:
        en = table_centres(t)
    else:
        en = _centres_3035(ids)
    if ids.size == 0:
        return en[:, 0], en[:, 1], en[:, 0], en[:, 1]
    lon, lat = _tf('EPSG:3035', 'EPSG:4326').transform(en[:, 0], en[:, 1])
    return en[:, 0], en[:, 1], np.asarray(lon), np.asarray(lat)


def _hex(ids: np.ndarray) -> list[str]:
    return [format(x, 'x') for x in ids.tolist()]


# === SECTION: roll-up (res 11 / 10) ===

def _reduce_mean(v: np.ndarray, bounds: np.ndarray) -> np.ndarray:
    valid = ~np.isnan(v)
    s = np.add.reduceat(np.where(valid, v, 0.0), bounds)
    c = np.add.reduceat(valid.astype(np.int64), bounds)
    with np.errstate(invalid='ignore', divide='ignore'):
        return np.where(c > 0, s / np.maximum(c, 1), np.nan)


def _reduce_mode(codes: np.ndarray, bounds: np.ndarray, k: int):
    hist = np.stack([np.add.reduceat((codes == i).astype(np.int64), bounds) for i in range(k)], axis=1)
    return hist.argmax(axis=1), hist


def rollup(t: Table, res: int) -> tuple[np.ndarray, OrderedDict, dict]:
    """res-12 table → parent ids at ``res`` + aggregated public columns + extras
    (``n_children``, ``consistency_hist``).  One ``np.add.reduceat`` pass per field."""
    dec = decode_obs(t.obs)
    if len(t) == 0:
        return np.zeros(0, np.uint64), OrderedDict((k, v[:0]) for k, v in dec.items()), {'n_children': [], 'consistency_hist': []}
    parents = ne_h3.h3_parent_vec(t.ids, res)
    order = np.argsort(parents, kind='stable')
    ps = parents[order]
    bounds = np.flatnonzero(np.concatenate([[True], ps[1:] != ps[:-1]]))
    n_children = np.diff(np.append(bounds, ps.size))
    out = OrderedDict()
    for name, v in dec.items():
        v = v[order]
        if name == 'cover':
            out[name] = np.stack([np.add.reduceat(v[:, i], bounds) / n_children for i in range(9)], axis=1)
        elif name in ('structures_cover', 'canopy', 'ndvi_mean', 'ndvi_amp', 'h_mean', 'h_p90', 'dh_m', 'dtm_m',
                      'elev_m', 'slope', 'slope_p90', 'roughness', 'tri', 'tpi'):
            out[name] = _reduce_mean(v, bounds)
        elif name in ('h_max', 'tree_h_max', 'forest_loss_year'):
            out[name] = np.maximum.reduceat(np.where(np.isnan(v), -np.inf, v), bounds)
            out[name][~np.isfinite(out[name])] = np.nan
        elif name in ('tree_n', 'structures_n'):
            out[name] = np.add.reduceat(v, bounds)
        elif name == 'als_years':
            out[name] = np.bitwise_or.reduceat(v, bounds)
        elif name == 'aspect':
            vv = np.where(v == 255, 9, v)
            mode, _ = _reduce_mode(vv, bounds, 10)
            out[name] = np.where(mode == 9, 255, mode)
        elif name == 'phenology':
            vv = np.where(v == 255, len(PHENOLOGY), v)
            mode, _ = _reduce_mode(vv, bounds, len(PHENOLOGY) + 1)
            out[name] = np.where(mode == len(PHENOLOGY), 255, mode)
        elif name == 'consistency':
            mode, hist = _reduce_mode(np.minimum(v, 6), bounds, 7)
            out[name] = mode
            extras_hist = hist
    extras = {'n_children': n_children.tolist(), 'consistency_hist': extras_hist.tolist()}
    kg_idx = t.kg_idx[order][bounds]
    extras['kg'] = np.asarray(t.kgs, dtype=object)[kg_idx].tolist() if t.kgs else []
    return ps[bounds], out, extras


def aggregate_all(t: Table) -> dict:
    """Whole-selection roll-up for ``/landscape?include=cells`` (§5)."""
    n = len(t)
    if n == 0:
        return {'n': 0}
    dec = decode_obs(t.obs)
    bounds = np.array([0])
    cover = {g: round(float(dec['cover'][:, i].mean()), 3) for i, g in enumerate(ne_h3.GROUPS)}

    def mean(name, nd=2):
        v = _reduce_mean(dec[name], bounds)[0]
        return None if np.isnan(v) else round(float(v), nd)

    def mx(name, nd=1):
        v = dec[name]; v = v[~np.isnan(v)]
        return round(float(v.max()), nd) if v.size else None
    ch = {}
    _, chist = _reduce_mode(np.minimum(dec['consistency'], 6), bounds, 7)
    ch_hist = {_CONSISTENCY_NAMES[i]: int(c) for i, c in enumerate(chist[0]) if c}
    _, phist = _reduce_mode(np.where(dec['phenology'] == 255, len(PHENOLOGY), dec['phenology']), bounds, len(PHENOLOGY) + 1)
    ph_hist = {(PHENOLOGY[i] if i < len(PHENOLOGY) else 'null'): int(c) for i, c in enumerate(phist[0]) if c}
    fl = dec['forest_loss_year']
    return {
        'n': n, 'res': RES, 'cover': cover,
        'structures_n': int(dec['structures_n'].sum()), 'structures_cover': mean('structures_cover', 3),
        'trees_n': int(dec['tree_n'].sum()), 'canopy': mean('canopy', 3),
        'height': {'mean': mean('h_mean', 1), 'p90': mean('h_p90', 1), 'max': mx('h_max')},
        'ndvi': {'mean': mean('ndvi_mean', 3), 'amp': mean('ndvi_amp', 3)},
        'terrain': {'elev_m': mean('elev_m', 0), 'slope': mean('slope', 1), 'slope_p90': mean('slope_p90', 1),
                    'roughness': mean('roughness', 1), 'tri': mean('tri', 1), 'tpi': mean('tpi', 2)},
        'consistency_hist': ch_hist, 'phenology_hist': ph_hist,
        'change': {'dh_m': mean('dh_m', 2), 'dtm_m': mean('dtm_m', 2),
                   'forest_loss_cells': int((~np.isnan(fl)).sum()),
                   'als_years': _years_list(int(np.bitwise_or.reduce(dec['als_years'])))},
        'kgs': list(t.kgs), 'trees_truncated_cells': t.trees_truncated,
    }


# === SECTION: query resolution ===

class CellsError(Exception):
    def __init__(self, status: int, msg: str, **extra):
        super().__init__(msg)
        self.status = status; self.msg = msg; self.extra = extra


def _h3n():
    import h3.api.numpy_int as hn
    return hn


def _parse_hex(s: str) -> int:
    s = s.strip().lower()
    if not s or len(s) > 16 or any(c not in '0123456789abcdef' for c in s):
        raise CellsError(400, f'bad cell id {s!r}')
    return int(s, 16)


def _cell_res(cid: int) -> int:
    return (cid >> 52) & 0xF


def ids_in_bbox(w, s, e, n, res: int = RES) -> np.ndarray:
    import h3
    poly = h3.LatLngPoly([(s, w), (s, e), (n, e), (n, w)])
    return np.asarray(_h3n().polygon_to_cells(poly, res), dtype=np.uint64)


def ids_for_point(lon: float, lat: float, k: int) -> np.ndarray:
    hn = _h3n()
    c = int(hn.latlng_to_cell(lat, lon, RES))
    return np.asarray(hn.grid_disk(c, k) if k > 0 else [c], dtype=np.uint64)


def expand_ids(cids: list[int], cap: int) -> np.ndarray:
    """Coarser ids → their res-12 children; res-12 ids as is; finer → 400."""
    hn = _h3n()
    out = []
    total = 0
    for c in cids:
        r = _cell_res(c)
        if r > RES:
            raise CellsError(400, f'cell {c:x} finer than res {RES}')
        if r == RES:
            out.append(np.array([c], np.uint64)); total += 1
        else:
            if r < 8:
                raise CellsError(400, f'cell {c:x} too coarse (res {r}); use res ≥ 8 ids')
            ch = np.asarray(hn.cell_to_children(c, RES), dtype=np.uint64)
            out.append(ch); total += ch.size
        if total > cap:
            raise CellsError(413, f'more than {cap} res-{RES} cells requested', max_cells=cap)
    return np.unique(np.concatenate(out)) if out else np.zeros(0, np.uint64)


def _bbox_of_ids(ids: np.ndarray) -> tuple[float, float, float, float]:
    hn = _h3n()
    pts = [hn.cell_to_latlng(int(x)) for x in (ids if ids.size <= 64 else ids[:: max(1, ids.size // 64)]).tolist()]
    lats = [p[0] for p in pts]; lons = [p[1] for p in pts]
    return (min(lons) - 0.001, min(lats) - 0.001, max(lons) + 0.001, max(lats) + 0.001)


def select(want_ids: np.ndarray, bbox=None) -> tuple[Table, dict, dict]:
    """res-12 ids → Table + headers touched + readiness meta."""
    r10s = ne_h3.h3_parent_vec(want_ids, CHUNK_RES)
    chunks, hdrs = chunks_for_r10(r10s)
    t = gather(chunks, want_ids)
    if bbox is None and want_ids.size:
        bbox = _bbox_of_ids(want_ids)
    inter = _kgs_intersecting(*bbox) if bbox else []
    on_disk = _files()
    missing = [k for k in inter if k not in on_disk]
    inter_on_disk = [k for k in inter if k in on_disk]
    partial_files = [k for k, h in hdrs.items() if h.get('partial_cells')]
    cells_missing = int(want_ids.size) - len(t)
    pending, not_proc = split_missing(missing)
    meta = {'kgs': sorted(t.kgs), 'kgs_missing': missing, 'kgs_pending': pending, 'kgs_not_processed': not_proc,
            'kgs_on_disk': inter_on_disk,
            'partial': bool(cells_missing > 0 or partial_files), 'cells_missing': cells_missing,
            'partial_files': partial_files, 'epoch': max((h.get('epoch') or '' for h in hdrs.values()), default=None) or None}
    return t, hdrs, meta


# === SECTION: formats ===

def table_json(t: Table, dec: OrderedDict, *, ids=None, res: int = RES, layers=(), centres=False,
               extras: dict | None = None, kg_col=None) -> list[dict]:
    """Per-cell nested dicts (§3)."""
    ids = t.ids if ids is None else ids
    n = int(ids.size)
    if n == 0:
        return []
    cols = _public_lists(dec)
    hexes = _hex(ids)
    kgc = (kg_col if kg_col is not None else t.kg_col().tolist())
    top = [(nm, cols[nm]) for nm in cols if nm not in JSON_GROUPS]
    groups: 'OrderedDict[str, list]' = OrderedDict()
    for nm in cols:
        g = JSON_GROUPS.get(nm)
        if g:
            groups.setdefault(g[0], []).append((g[1], cols[nm]))
    top_keys = ['cell', 'res', 'kg'] + [nm for nm, _ in top]
    top_cols = [hexes, [res] * n, kgc] + [c for _, c in top]
    rows = [dict(zip(top_keys, vals)) for vals in zip(*top_cols)]
    for g, items in groups.items():
        keys = [k for k, _ in items]
        subs = [dict(zip(keys, vals)) for vals in zip(*[c for _, c in items])]
        for d, sub in zip(rows, subs):
            d[g] = sub
    if extras:
        if 'n_children' in extras:
            for i, d in enumerate(rows):
                d['n_children'] = extras['n_children'][i]
                d['consistency_hist'] = dict(zip(_CONSISTENCY_NAMES, extras['consistency_hist'][i]))
    if centres:
        _, _, lon, lat = cell_centres(ids, t)
        for i, d in enumerate(rows):
            d['lon'] = round(float(lon[i]), 6); d['lat'] = round(float(lat[i]), 6)
    if 'trees' in layers and res == RES:
        cell_idx, words = t.gather_trees()
        _attach_points(rows, t, cell_idx, decode_trees(words), 'trees', species=SPECIES, vitality=VITALITY)
    if 'structures' in layers and res == RES:
        cell_idx, words = t.gather_structures()
        _attach_points(rows, t, cell_idx, decode_structures(words), 'structures', type=STRUCTURE_TYPES)
    return rows


def _points_lonlat(t: Table, cell_idx: np.ndarray, dx: np.ndarray, dy: np.ndarray):
    en = table_centres(t)
    e, n = en[:, 0], en[:, 1]
    lon, lat = _tf('EPSG:3035', 'EPSG:4326').transform(e[cell_idx] + dx, n[cell_idx] + dy)
    return np.asarray(lon), np.asarray(lat)


def _attach_points(rows: list[dict], t: Table, cell_idx: np.ndarray, dec: dict, key: str, **codes) -> None:
    for d in rows:
        d[key] = []
    if cell_idx.size == 0:
        return
    lon, lat = _points_lonlat(t, cell_idx, dec['dx'], dec['dy'])
    cols = {}
    for k, v in dec.items():
        if k in codes:
            cols[k] = _cat(v, codes[k])
        elif v.dtype.kind == 'b':
            cols[k] = v.tolist()
        else:
            cols[k] = _col_list(v, 2)
    cols['lon'] = np.round(lon, 6).tolist(); cols['lat'] = np.round(lat, 6).tolist()
    keys = list(cols.keys())
    pts = [dict(zip(keys, vals)) for vals in zip(*cols.values())]
    lists = [d[key] for d in rows]
    for o, pt in zip(cell_idx.tolist(), pts):
        lists[o].append(pt)


def table_columns(t: Table, *, res: int = RES, dec: OrderedDict | None = None, ids=None,
                  fields=None, centres=False, layers=(), extras=None, kg_col=None) -> dict:
    """``format=columns``: raw quantised integers at res 12 (~40 B/cell before
    gzip); aggregated (rounded floats) at res 11 / 10."""
    ids = t.ids if ids is None else ids
    out = {'cells': _hex(ids), 'res': res, 'kg': kg_col if kg_col is not None else t.kg_col().tolist()}
    if res == RES:
        obs = t.obs
        for name, (f, kind) in OBS_PUBLIC.items():
            if fields and name not in fields:
                continue
            out[name] = obs[f].tolist()
    else:
        for name, v in (dec or {}).items():
            if fields and name not in fields:
                continue
            if name == 'cover':
                out[name] = np.round(v, 3).tolist()
            elif name in ('als_years', 'consistency', 'phenology', 'aspect', 'tree_n', 'structures_n'):
                out[name] = v.tolist()
            else:
                out[name] = _col_list(v, _ROUND.get(name, 2))
        if extras:
            out['n_children'] = extras['n_children']; out['consistency_hist'] = extras['consistency_hist']
    if centres:
        _, _, lon, lat = cell_centres(ids, t)
        out['lon'] = np.round(lon, 6).tolist(); out['lat'] = np.round(lat, 6).tolist()
    if 'trees' in layers and res == RES:
        cell_idx, words = t.gather_trees()
        d = decode_trees(words)
        lon, lat = _points_lonlat(t, cell_idx, d['dx'], d['dy']) if cell_idx.size else (np.zeros(0), np.zeros(0))
        out['trees'] = {'cell_index': cell_idx.tolist(), 'dx': d['dx'].tolist(), 'dy': d['dy'].tolist(),
                        'h_m': d['h_m'].tolist(), 'crown_m2': d['crown_m2'].tolist(), 'species': d['species'].tolist(),
                        'vitality': d['vitality'].tolist(), 'ndsm_only': d['ndsm_only'].astype(np.uint8).tolist(),
                        'lon': np.round(lon, 6).tolist(), 'lat': np.round(lat, 6).tolist()}
    if 'structures' in layers and res == RES:
        cell_idx, words = t.gather_structures()
        d = decode_structures(words)
        lon, lat = _points_lonlat(t, cell_idx, d['dx'], d['dy']) if cell_idx.size else (np.zeros(0), np.zeros(0))
        out['structures'] = {'cell_index': cell_idx.tolist(), 'dx': d['dx'].tolist(), 'dy': d['dy'].tolist(),
                             'h_max_m': d['h_max_m'].tolist(), 'h_robust_m': d['h_robust_m'].tolist(),
                             'area_m2': d['area_m2'].tolist(), 'type': d['type'].tolist(), 'conf': d['conf'].tolist(),
                             'dh_m': d['dh_m'].tolist(), 'lon': np.round(lon, 6).tolist(), 'lat': np.round(lat, 6).tolist()}
    out['units'] = UNITS
    out['codes'] = {'groups': list(ne_h3.GROUPS), 'consistency': list(_CONSISTENCY_NAMES), 'phenology': list(PHENOLOGY),
                    'aspect': list(ASPECTS), 'species': list(SPECIES), 'vitality': list(VITALITY),
                    'structure_types': list(STRUCTURE_TYPES)}
    return out


def table_bin(t: Table, layers=()) -> tuple[bytes, dict]:
    """``format=bin``: NEO1 rows (41 B LE, sorted by id) [+ NET1 u32 rows + NEB1 u64 rows]."""
    parts = [t.obs.tobytes()]
    hdrs = {'X-Cells-Record': f'neo1/{REC}'}
    layout = [f'neo1={len(t)}']
    if 'trees' in layers:
        _, w = t.gather_trees(); parts.append(np.ascontiguousarray(w, dtype='<u4').tobytes()); layout.append(f'net1={w.size}')
    if 'structures' in layers:
        _, w = t.gather_structures(); parts.append(np.ascontiguousarray(w, dtype='<u8').tobytes()); layout.append(f'neb1={w.size}')
    hdrs['X-Cells-Layout'] = ','.join(layout)
    hdrs['X-Cells-Fields'] = ','.join(_NEO1_REC_FIELDS)
    return b''.join(parts), hdrs


def table_csv(t: Table, dec: OrderedDict, *, ids=None, res=RES, centres=False, extras=None, kg_col=None) -> str:
    ids = t.ids if ids is None else ids
    cols = _public_lists(dec)
    buf = io.StringIO()
    wri = csv.writer(buf)
    head = ['cell', 'res', 'kg']
    flat = []
    for name in cols:
        if name == 'cover':
            flat += [('cover_' + g, name, g) for g in ne_h3.GROUPS]
        else:
            flat.append((name, name, None))
    head += [f[0] for f in flat]
    if centres:
        head += ['lon', 'lat']
    if extras:
        head += ['n_children']
    wri.writerow(head)
    hexes = _hex(ids)
    kgc = kg_col if kg_col is not None else t.kg_col().tolist()
    lon = lat = None
    if centres:
        _, _, lon, lat = cell_centres(ids, t)
        lon = np.round(lon, 6).tolist(); lat = np.round(lat, 6).tolist()
    for i in range(len(hexes)):
        row = [hexes[i], res, kgc[i]]
        for _, name, g in flat:
            v = cols[name][i]
            if g is not None:
                v = v[g]
            elif isinstance(v, list):
                v = ' '.join(str(x) for x in v)
            row.append('' if v is None else v)
        if centres:
            row += [lon[i], lat[i]]
        if extras:
            row.append(extras['n_children'][i])
        wri.writerow(row)
    return buf.getvalue()


def table_geojson(rows: list[dict], ids: np.ndarray) -> dict:
    hn = _h3n()
    feats = []
    for d, cid in zip(rows, ids.tolist()):
        ring = [[round(lon, 6), round(lat, 6)] for lat, lon in hn.cell_to_boundary(cid)]
        ring.append(ring[0])
        feats.append({'type': 'Feature', 'id': d['cell'], 'geometry': {'type': 'Polygon', 'coordinates': [ring]},
                      'properties': d})
    return {'type': 'FeatureCollection', 'features': feats}


# === SECTION: public h3b container (chunks without the first slice) ===

PUB_CHUNK_LAYOUT = ('zlib(shuffle(NEO1 rows) | shuffle(NET1 rows) | shuffle(NEB1 rows)); '
                    'shuffle = byte-column-major transpose of the fixed-size records')


def _pub_header_base(h: dict, kg: str) -> dict:
    return {
        'magic': ne_h3.H3B_MAGIC.decode(), 'fmt_ver': ne_h3.FMT_VER, 'kg': kg, 'product_version': h.get('product_version'),
        'epoch': h.get('epoch'), 'h3_res': RES, 'chunk_res': CHUNK_RES, 'groups': list(ne_h3.GROUPS),
        'chunk_layout': PUB_CHUNK_LAYOUT,
        'record_bytes': {'nec1': 0, 'neo1': REC, 'net1': 4, 'neb1': 8},
        'neo1': {'algo': ne_h3.ALGO_OBS, 'digest': h.get('neo1_digest'), 'record': ne_h3.OBS_REC.format,
                 'epoch': h.get('epoch'), 'source': ne_h3.OBS_SOURCE_FIXED, 'fields': _NEO1_REC_FIELDS,
                 'units': UNITS},
        'net1': dict(h.get('net1') or {}, record='u32: dx:5 dy:5 h:8(0.25m) crown:6(log2*5) species:4 vitality:3 flag:1',
                     species=list(SPECIES), vitality=list(VITALITY)),
        'neb1': dict(h.get('neb1') or {}, record='u64: dx:5 dy:5 h_max:8(0.25m) h_robust:8(0.25m) area:16(m2) type:4 conf:4 dh:8(i8 0.25m)',
                     types=list(STRUCTURE_TYPES)),
        'codes': {'consistency': list(_CONSISTENCY_NAMES), 'phenology': list(PHENOLOGY), 'aspect': list(ASPECTS)},
        'neo1_digest': h.get('neo1_digest'), 'partial_cells': bool(h.get('partial_cells')),
        'green_fix': _green_fix.FIX_ID,
        'green_fix_rule': _green_fix.header_note(0, 0)['green_fix_rule'] + ' (applied at read time; summary.observed_ha is pre-fix)',
        'attribution': ATTRIBUTION, 'license': 'CC-BY-4.0', 'license_url': LICENSE_URL,
        'docs': '/api/v1/docs/llm.txt#cells', 'source_service': 'srtm-lidar-at',
    }


def _finish_h3b(hdr: dict, body: bytes) -> bytes:
    hdr['body_offset'] = 0
    for _ in range(6):
        hj = ne_h3.canonical_json(hdr)
        hb = ne_h3.H3B_MAGIC + bytes([ne_h3.FMT_VER]) + struct.pack('<I', len(hj)) + hj
        if len(hb) == hdr['body_offset']:
            break
        hdr['body_offset'] = len(hb)
    return hb + body


def chunks_h3b(chunks: list[Chunk], hdrs: dict) -> bytes:
    """h3b container for a set of chunks (``/cells/chunk/<h3>``)."""
    body = bytearray(); entries = []
    for ch in sorted(chunks, key=lambda c: (c.r10, c.kg)):
        z = ch.public_deflated()
        o, t, b = ch.raw_pub
        entries.append({'r10': format(ch.r10, 'x'), 'kg': ch.kg, 'off': len(body), 'len': len(z),
                        'raw_len': len(o) + len(t) + len(b), 'cells': ch.cells, 'trees': int(ch.trees.size),
                        'bld': int(ch.bld.size), 'first': format(int(ch.ids[0]), 'x') if ch.cells else None})
        body += z
    kgs = sorted(hdrs)
    h0 = hdrs[kgs[0]] if kgs else {}
    hdr = _pub_header_base(h0, kgs[0] if len(kgs) == 1 else ','.join(kgs))
    hdr['cells_n'] = int(sum(e['cells'] for e in entries))
    hdr['chunks'] = entries
    hdr['files'] = {k: {'neo1_digest': h.get('neo1_digest'), 'epoch': h.get('epoch')} for k, h in hdrs.items()}
    hdr.pop('summary', None)
    return _finish_h3b(hdr, bytes(body))


_pub_lock = threading.Lock()


def pub_file(kg: str) -> Path | None:
    """Whole-KG public container, built once per (kg, neo1_digest) under
    ``data/ne/pub/``; LRU-pruned to ``PUB_LRU_FILES``."""
    kg = str(kg).zfill(5)
    h = header(kg)
    if h is None:
        return None
    out = PUB_DIR / f'{kg}-{h["neo1_digest"]}-{_green_fix.FIX_ID}.h3b'
    if out.exists():
        return out
    with _pub_lock:
        if out.exists():
            return out
        t0 = time.time()
        full = header(kg, with_chunks=True)
        body = bytearray(); entries = []
        for hrow in full['chunks']:
            row = dict(hrow, r10=int(hrow['r10'], 16), trees=int(hrow.get('trees') or 0), bld=int(hrow.get('bld') or 0))
            ch = _load_chunk(kg, row, full)
            z = ch.public_deflated()
            o, t, b = ch.raw_pub
            entries.append({'r10': hrow['r10'], 'off': len(body), 'len': len(z), 'raw_len': len(o) + len(t) + len(b),
                            'cells': ch.cells, 'trees': int(ch.trees.size), 'bld': int(ch.bld.size), 'first': hrow.get('first')})
            body += z
        hdr = _pub_header_base(full, kg)
        hdr['cells_n'] = int(full['cells_n']); hdr['chunks'] = entries
        hdr['summary'] = full['summary']; hdr['created_at'] = full.get('created_at')
        if full.get('merged_from'):
            hdr['merged_from'] = full['merged_from']
        blob = _finish_h3b(hdr, bytes(body))
        PUB_DIR.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix('.tmp')
        tmp.write_bytes(blob); tmp.replace(out)
        # drop stale digests of this kg + LRU prune
        for p in PUB_DIR.glob(f'{kg}-*.h3b'):
            if p != out:
                p.unlink(missing_ok=True)
        files = sorted(PUB_DIR.glob('*.h3b'), key=lambda p: p.stat().st_mtime)
        for p in files[:-PUB_LRU_FILES] if len(files) > PUB_LRU_FILES else []:
            p.unlink(missing_ok=True)
        log.info('cells: built public container %s (%d B, %d chunks, %.0f ms)', out.name, len(blob), len(entries),
                 (time.time() - t0) * 1000)
    return out


# === SECTION: Flask blueprint ===

cells_bp = Blueprint('cells', __name__)


def _args():
    return request.args


def _flag(name: str) -> bool:
    return (_args().get(name) or '').lower() in ('1', 'true', 'yes')


def _layers() -> tuple:
    raw = _args().get('layers') or 'obs'
    ls = tuple(x.strip() for x in raw.split(',') if x.strip())
    bad = [x for x in ls if x not in LAYERS]
    if bad:
        raise CellsError(400, f'unknown layers {bad}; use {",".join(LAYERS)}')
    return ls


def _fields() -> list[str] | None:
    raw = _args().get('fields')
    if not raw:
        return None
    fs = [x.strip() for x in raw.split(',') if x.strip()]
    bad = [x for x in fs if x not in OBS_PUBLIC]
    if bad:
        raise CellsError(400, f'unknown fields {bad}', fields=list(OBS_PUBLIC))
    return fs


def _res() -> int:
    r = _args().get('res')
    if not r:
        return RES
    try:
        r = int(r)
    except ValueError:
        raise CellsError(400, 'res must be 10, 11 or 12')
    if r not in (10, 11, 12):
        raise CellsError(400, 'res must be 10, 11 or 12')
    return r


def _format(allowed=FORMATS, default: str = 'json') -> str:
    f = (_args().get('format') or default).lower()
    if f not in allowed:
        raise CellsError(400, f'format must be one of {",".join(allowed)}')
    return f


def _etag(hdrs: dict, extra: str = '') -> str:
    digs = sorted(str(h.get('neo1_digest')) for h in hdrs.values())
    canon = request.method + ' ' + request.path + '?' + '&'.join(f'{k}={v}' for k, v in sorted(request.args.items(multi=True)))
    if request.method == 'POST':
        canon += '#' + hashlib.sha256(request.get_data() or b'').hexdigest()[:16]
    return '"c-' + hashlib.sha256(('|'.join(digs) + '\n' + canon + extra + _green_fix.FIX_ID).encode()).hexdigest()[:16] + '"'


def _dumps(obj, pretty: bool = False) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(obj, option=(_orjson.OPT_INDENT_2 if pretty else 0) | _orjson.OPT_SERIALIZE_NUMPY)
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False, indent=1 if pretty else None,
                      allow_nan=False).encode()


def _resp(body, *, status=200, mimetype='application/json', headers: dict | None = None, etag: str | None = None,
          partial: bool = False) -> Response:
    if isinstance(body, (dict, list)):
        body = _dumps(body, _flag('pretty'))
    r = Response(body, status=status, mimetype=mimetype)
    r.headers['X-Cells-Source'] = 'disk'
    if partial:
        r.headers['X-Cells-Partial'] = '1'
    if headers:
        for k, v in headers.items():
            r.headers[k] = v
    if status == 200 and etag:
        r.headers['ETag'] = etag
        r.headers['Cache-Control'] = f'public, max-age={CACHE_MAX_AGE}'
        inm = request.headers.get('If-None-Match') or ''
        if etag in [x.strip() for x in inm.split(',')] or inm.strip() == '*':
            return Response(status=304, headers={'ETag': etag, 'Cache-Control': r.headers['Cache-Control']})
    elif status >= 400:
        r.headers['Cache-Control'] = 'no-store'
    return r


def _error(e: CellsError) -> Response:
    d = {'error': e.msg}
    d.update(e.extra)
    return _resp(d, status=e.status)


def _not_ready(meta: dict) -> Response:
    st = 'partial' if meta.get('kgs_on_disk') else 'not_processed'
    body = {'ready': False, 'status': st, 'retry_after_s': RETRY_AFTER_S, 'kgs_missing': meta.get('kgs_missing') or []}
    if 'kgs_pending' not in meta and body['kgs_missing']:
        meta['kgs_pending'], meta['kgs_not_processed'] = split_missing(body['kgs_missing'])
    body['kgs_pending'] = meta.get('kgs_pending') or []
    body['kgs_not_processed'] = meta.get('kgs_not_processed') or []
    if body['kgs_pending'] and st == 'not_processed':
        body['status'] = 'pending'
    return _resp(body, status=404, headers={'Retry-After': str(RETRY_AFTER_S)})


@cells_bp.errorhandler(CellsError)
def _on_cells_error(e):
    return _error(e)


def _meta(t_meta: dict, n: int, res: int, layers, t: Table | None = None, **extra) -> dict:
    m = {'service': 'srtm-lidar-at', 'license': 'CC-BY-4.0', 'license_url': LICENSE_URL, 'attribution': ATTRIBUTION,
         'docs': '/api/v1/docs/llm.txt#cells', 'n': n, 'res': res, 'layers': list(layers), 'kgs': t_meta.get('kgs', []),
         'epoch': t_meta.get('epoch'), 'partial': bool(t_meta.get('partial')), 'source': 'disk'}
    if t_meta.get('kgs_missing'):
        m['kgs_missing'] = t_meta['kgs_missing']
        m['kgs_pending'] = t_meta.get('kgs_pending') or []
        m['kgs_not_processed'] = t_meta.get('kgs_not_processed') or []
        if m['kgs_pending'] or t_meta.get('partial_files'):
            m['retry_after_s'] = RETRY_AFTER_S
    if t_meta.get('cells_missing'):
        m['cells_missing'] = t_meta['cells_missing']
    if t_meta.get('partial_files'):
        m['kgs_partial'] = t_meta['partial_files']
    if t is not None and 'trees' in layers:
        m['trees_truncated_cells'] = t.trees_truncated
    m.update(extra)
    return m


def _want_ids_from_request() -> tuple[np.ndarray, tuple | None, str]:
    a = _args()
    if request.method == 'POST':
        body = request.get_json(silent=True) or {}
        ids = body.get('ids') or []
        if not isinstance(ids, list) or not ids:
            raise CellsError(400, 'POST body must be {"ids": [h3, …]}')
        if len(ids) > MAX_IDS_POST:
            raise CellsError(413, f'more than {MAX_IDS_POST} ids', max_ids=MAX_IDS_POST)
        return expand_ids([_parse_hex(str(x)) for x in ids], MAX_CELLS), None, 'ids'
    if a.get('ids'):
        ids = [x for x in a['ids'].split(',') if x.strip()]
        if len(ids) > MAX_IDS_GET:
            raise CellsError(413, f'more than {MAX_IDS_GET} ids on GET; POST {{"ids": […]}} for up to {MAX_IDS_POST}',
                             max_ids=MAX_IDS_GET)
        return expand_ids([_parse_hex(x) for x in ids], MAX_CELLS), None, 'ids'
    if a.get('lon') and a.get('lat'):
        try:
            lon, lat = float(a['lon']), float(a['lat'])
            k = int(a.get('k', 0))
        except ValueError:
            raise CellsError(400, 'lon, lat must be numbers; k an integer 0..3')
        if not (0 <= k <= MAX_K):
            raise CellsError(400, f'k must be 0..{MAX_K}')
        if not (9.0 <= lon <= 17.6 and 46.0 <= lat <= 49.2):
            raise CellsError(400, 'point must lie within Austria (lon 9–17.6, lat 46–49.2)')
        ids = ids_for_point(lon, lat, k)
        d = 0.0005 * (k + 1)
        return ids, (lon - d, lat - d, lon + d, lat + d), 'point'
    if a.get('bbox'):
        try:
            w, s, e, n = [float(x) for x in a['bbox'].split(',')]
        except ValueError:
            raise CellsError(400, 'bbox=west,south,east,north')
        if not (9.0 <= w <= 17.6 and 9.0 <= e <= 17.6 and 46.0 <= s <= 49.2 and 46.0 <= n <= 49.2):
            raise CellsError(400, 'bbox must lie within Austria (lon 9–17.6, lat 46–49.2)')
        if e < w or n < s:
            raise CellsError(400, 'bbox east<west or north<south')
        if (e - w) > MAX_BBOX_SPAN_DEG or (n - s) > MAX_BBOX_SPAN_DEG:
            raise CellsError(413, f'bbox span must be ≤ {MAX_BBOX_SPAN_DEG}°; tile the viewport',
                             max_span_deg=MAX_BBOX_SPAN_DEG)
        ids = ids_in_bbox(w, s, e, n)
        if ids.size > MAX_CELLS:
            raise CellsError(413, f'{ids.size} cells in bbox > {MAX_CELLS}; tile the viewport or use res=11|10 on a smaller box',
                             cells=int(ids.size), max_cells=MAX_CELLS)
        return ids, (w, s, e, n), 'bbox'
    raise CellsError(400, 'give bbox=w,s,e,n | lon=&lat=[&k=] | ids=h3,… (or POST {"ids": […]})')


_gc_lock = threading.Lock()
_gc_users = 0


class _NoGC:
    """Pause the cyclic GC while a large per-cell JSON structure is built —
    tens of thousands of short-lived dicts otherwise trigger generation-0/1
    collections every few hundred allocations (measured 3× slower).
    Reference-counted across threads so concurrent renders don't re-enable
    early."""

    def __init__(self, n: int):
        self.on = n >= 1000

    def __enter__(self):
        global _gc_users
        if self.on:
            with _gc_lock:
                _gc_users += 1
                gc.disable()
        return self

    def __exit__(self, *exc):
        global _gc_users
        if self.on:
            with _gc_lock:
                _gc_users -= 1
                if _gc_users <= 0:
                    _gc_users = 0
                    gc.enable()
        return False


def _render(t: Table, hdrs: dict, meta: dict, *, fmt: str, res: int, layers: tuple, fields, centres: bool) -> Response:
    with _NoGC(len(t)):
        return _render_inner(t, hdrs, meta, fmt=fmt, res=res, layers=layers, fields=fields, centres=centres)


def _render_inner(t: Table, hdrs: dict, meta: dict, *, fmt: str, res: int, layers: tuple, fields, centres: bool) -> Response:
    n = len(t)
    etag = _etag(hdrs)
    partial = bool(meta.get('partial'))
    if fmt == 'bin':
        if res != RES:
            raise CellsError(400, 'format=bin only at res 12')
        body, hx = table_bin(t, layers)
        hx['X-Cells-Epoch'] = str(meta.get('epoch'))
        hx['X-Cells-Kgs'] = ','.join(meta.get('kgs') or [])
        return _resp(body, mimetype='application/octet-stream', headers=hx, etag=etag, partial=partial)
    if res == RES:
        ids = t.ids; dec = decode_obs(t.obs, fields); extras = None; kg_col = None
    else:
        ids, dec, extras = rollup(t, res)
        kg_col = extras.pop('kg')
        if fields:
            dec = OrderedDict((k, v) for k, v in dec.items() if k in fields)
    n_out = int(ids.size)
    if fmt == 'columns':
        out = table_columns(t, res=res, dec=dec, ids=ids, fields=fields, centres=centres, layers=layers, extras=extras, kg_col=kg_col)
        out['meta'] = _meta(meta, n_out, res, layers, t)
        return _resp(out, etag=etag, partial=partial)
    if fmt == 'csv':
        return _resp(table_csv(t, dec, ids=ids, res=res, centres=centres, extras=extras, kg_col=kg_col).encode(),
                     mimetype='text/csv', etag=etag, partial=partial,
                     headers={'X-Cells-Kgs': ','.join(meta.get('kgs') or []), 'X-Cells-Epoch': str(meta.get('epoch'))})
    rows = table_json(t, dec, ids=ids, res=res, layers=layers, centres=centres, extras=extras, kg_col=kg_col)
    if fmt == 'geojson':
        if n_out > MAX_GEOJSON_CELLS:
            raise CellsError(413, f'geojson limited to {MAX_GEOJSON_CELLS} cells', max_cells=MAX_GEOJSON_CELLS)
        fc = table_geojson(rows, ids)
        fc['meta'] = _meta(meta, n_out, res, layers, t)
        return _resp(fc, mimetype='application/geo+json', etag=etag, partial=partial)
    return _resp({'cells': rows, 'meta': _meta(meta, n_out, res, layers, t)}, etag=etag, partial=partial)


@cells_bp.route('/api/v1/cells', methods=['GET', 'POST'])
def cells():
    want, bbox, kind = _want_ids_from_request()
    layers = _layers(); fmt = _format(('json', 'columns', 'bin', 'csv', 'geojson')); res = _res()
    fields = _fields(); centres = _flag('centres')
    if res != RES and any(l in layers for l in ('trees', 'structures')):
        raise CellsError(400, 'layers trees/structures only at res 12')
    t, hdrs, meta = select(want, bbox)
    if len(t) == 0:
        return _not_ready(meta)
    return _render(t, hdrs, meta, fmt=fmt, res=res, layers=layers, fields=fields, centres=centres)


@cells_bp.route('/api/v1/cells/dict')
def cells_dict():
    d = {
        'res': RES, 'chunk_res': CHUNK_RES, 'cell_area_m2': '~307 (res 12)',
        'layers': {'obs': 'one 41 B record per cell (fields below)', 'trees': 'tree apices h ≥ 3 m, 4 B each (res 12 only)',
                   'structures': 'segmented man-made objects with LiDAR heights, 8 B each (res 12 only)'},
        'fields': _NEO1_REC_FIELDS, 'units': UNITS,
        'groups': {g: GROUP_LABELS[g] for g in ne_h3.GROUPS},
        'consistency': {
            'codes': list(_CONSISTENCY_NAMES),
            'note': 'our observation vs. the statistical land-use layer of umfeld-at for the same cell '
                    '(frozen epoch), computed when the product was built. sealed_new / structure_new / '
                    'green_new = we see it, the statistical layer does not (yet).'},
        'phenology': list(PHENOLOGY), 'aspect': list(ASPECTS), 'species': list(SPECIES), 'vitality': list(VITALITY),
        'structure_types': list(STRUCTURE_TYPES),
        'record_bytes': {'neo1': REC, 'net1': 4, 'neb1': 8},
        'neo1_record': ne_h3.OBS_REC.format,
        'net1_record': 'u32 LE: dx:5 dy:5 h:8(×0.25 m) crown:6(log2·5) species:4 vitality:3 ndsm_only:1',
        'neb1_record': 'u64 LE: dx:5 dy:5 h_max:8(×0.25 m) h_robust:8(×0.25 m) area:16(m²) type:4 conf:4 dh:8(i8 ×0.25 m)',
        'origin': 'dx/dy in metres from the metre-snapped EPSG:3035 centre of the cell',
        'limits': {'bbox_span_deg': MAX_BBOX_SPAN_DEG, 'cells': MAX_CELLS, 'ids_get': MAX_IDS_GET, 'ids_post': MAX_IDS_POST,
                   'geojson_cells': MAX_GEOJSON_CELLS, 'k': MAX_K, 'rate_hint': '≤ 5 r/s sustained, burst 20'},
        'formats': list(FORMATS), 'epoch': _epoch_pin(),
        'dedupe': 'a border cell present in several KG products is served once: observed copy first, then the '
                  'larger chunk, then the lowest kg; meta.kgs lists the products touched',
        'rollup': {'res': [11, 10], 'mean': ['cover', 'structures_cover', 'canopy', 'ndvi.*', 'terrain.* (aspect → mode)',
                                            'height.mean', 'height.p90 (mean of p90s)', 'change.dh_m', 'change.dtm_m'],
                   'max': ['height.max', 'trees.h_max', 'change.forest_loss_year'], 'sum': ['trees.n', 'structures_n'],
                   'mode': ['phenology', 'consistency (+ consistency_hist)'], 'union': ['change.als_years'],
                   'returns': 'n_children'},
    }
    return _resp(d, etag='"c-dict-' + hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()[:12] + '"')


@cells_bp.route('/api/v1/cells/chunk/<h>')
def cells_chunk(h):
    cid = _parse_hex(h)
    r = _cell_res(cid)
    if r not in (9, 10):
        raise CellsError(400, 'chunk id must be res 9 or 10')
    r10s = np.asarray(_h3n().cell_to_children(cid, CHUNK_RES) if r == 9 else [cid], dtype=np.uint64)
    fmt = _format(('h3b', 'bin', 'json'), default='h3b')
    chunks, hdrs = chunks_for_r10(r10s)
    if not chunks:
        bb = _bbox_of_ids(r10s)
        inter = _kgs_intersecting(*bb)
        return _not_ready({'kgs': [], 'kgs_missing': [k for k in inter if k not in _files()]})
    etag = _etag(hdrs)
    partial = any(h_.get('partial_cells') for h_ in hdrs.values())
    if fmt == 'h3b':
        blob = chunks_h3b(chunks, hdrs)
        return _resp(blob, mimetype='application/x-h3b', etag=etag, partial=partial,
                     headers={'Content-Disposition': f'inline; filename="chunk-{h}.h3b"', 'X-Cells-Kgs': ','.join(sorted(hdrs))})
    t = gather(chunks, None)
    meta = {'kgs': sorted(t.kgs), 'kgs_missing': [], 'partial': partial,
            'partial_files': [k for k, h_ in hdrs.items() if h_.get('partial_cells')],
            'epoch': max((h_.get('epoch') or '' for h_ in hdrs.values()), default=None) or None}
    layers = _layers() if _args().get('layers') else LAYERS
    return _render(t, hdrs, meta, fmt=fmt, res=RES, layers=layers, fields=_fields(), centres=_flag('centres'))


@cells_bp.route('/api/v1/cells/<h>')
def cell_one(h):
    cid = _parse_hex(h)
    if _cell_res(cid) != RES:
        raise CellsError(400, f'single-cell lookup needs a res-{RES} id; use /api/v1/cells?ids= for coarser ids')
    want = np.array([cid], np.uint64)
    t, hdrs, meta = select(want, None)
    if len(t) == 0:
        return _not_ready(meta)
    rows = table_json(t, decode_obs(t.obs), layers=LAYERS, centres=True)
    d = rows[0]
    d['meta'] = _meta(meta, 1, RES, LAYERS, t)
    if not _flag('flags'):
        return _resp(d, etag=_etag(hdrs), partial=bool(meta.get('partial')))
    # public "looks wrong" reports on this cell (public_flags.py); one indexed lookup.
    # Short max-age + tally in the ETag so the card refreshes after a new report.
    try:
        import public_flags
        d['flags'] = public_flags.tally_for_cell(cid)
    except Exception:  # noqa: BLE001
        d['flags'] = None
    r = _resp(d, etag=_etag(hdrs, json.dumps(d['flags'], sort_keys=True)), partial=bool(meta.get('partial')))
    if r.status_code == 200:
        r.headers['Cache-Control'] = 'public, max-age=60'
    return r


@cells_bp.route('/api/v1/kg/<code>/cells')
def kg_cells(code):
    code = code.strip()
    if not (code.isdigit() and len(code) == 5):
        raise CellsError(400, 'kg code must be 5 digits')
    fmt = _format(('h3b', 'json'), default='h3b')
    h = header(code)
    if h is None:
        bb = _kg_bbox(code)
        if bb is None:
            raise CellsError(404, f'unknown KG {code}')
        return _not_ready({'kgs': [], 'kgs_missing': [code]})
    etag = '"c-' + hashlib.sha256(f'{code}|{h["neo1_digest"]}|{fmt}|{_green_fix.FIX_ID}'.encode()).hexdigest()[:16] + '"'
    if fmt == 'json':
        d = {'kg': code, 'ready': True, 'cells_n': h['cells_n'], 'epoch': h['epoch'], 'product_version': h['product_version'],
             'partial': bool(h.get('partial_cells')), 'neo1_digest': h['neo1_digest'], 'summary': h['summary'],
             'created_at': h.get('created_at'),
             'links': {'h3b': f'/api/v1/kg/{code}/cells?format=h3b', 'dict': '/api/v1/cells/dict'},
             'meta': _meta({'kgs': [code], 'epoch': h['epoch'], 'partial': bool(h.get('partial_cells'))}, h['cells_n'], RES, LAYERS)}
        return _resp(d, etag=etag, partial=bool(h.get('partial_cells')))
    p = pub_file(code)
    if p is None:
        return _not_ready({'kgs': [], 'kgs_missing': [code]})
    inm = request.headers.get('If-None-Match') or ''
    if etag in [x.strip() for x in inm.split(',')]:
        return Response(status=304, headers={'ETag': etag, 'Cache-Control': f'public, max-age={CACHE_MAX_AGE}'})
    r = send_file(p, mimetype='application/x-h3b', conditional=True, download_name=f'{code}_cells.h3b', as_attachment=False)
    r.headers['ETag'] = etag
    r.headers['Cache-Control'] = f'public, max-age={CACHE_MAX_AGE}'
    r.headers['X-Cells-Source'] = 'disk'
    r.headers['X-Cells-Epoch'] = str(h['epoch'])
    if h.get('partial_cells'):
        r.headers['X-Cells-Partial'] = '1'
    return r


# === SECTION: helpers for public_api integrations (§5) ===

def kg_cells_block(code: str) -> dict:
    """``cells{ready, cells_n, epoch, summary}`` for ``/api/v1/kg/<code>``."""
    h = header(code)
    ns = ne_state(code)
    if h is None:
        return {'ready': False, 'status': ns['ne_status'], 'retry_after_s': ns['retry_after_s']}
    return {'ready': True, 'status': ns['ne_status'], 'cells_n': h['cells_n'], 'epoch': h['epoch'],
            'partial': bool(h.get('partial_cells')), 'published_at': ns['ne_cells_published_at'],
            'summary': h['summary'], 'url': f'/api/v1/kg/{str(code).zfill(5)}/cells'}


def landscape_cells_block(w, s, e, n) -> dict:
    """Per-bbox roll-up for ``/api/v1/landscape?include=cells`` (§5)."""
    if (e - w) > MAX_BBOX_SPAN_DEG or (n - s) > MAX_BBOX_SPAN_DEG:
        return {'ready': False, 'error': f'bbox span > {MAX_BBOX_SPAN_DEG}° — ask /api/v1/cells per tile'}
    ids = ids_in_bbox(w, s, e, n)
    if ids.size > 4 * MAX_CELLS:
        return {'ready': False, 'error': 'too many cells in bbox', 'cells': int(ids.size)}
    t, hdrs, meta = select(ids, (w, s, e, n))
    if len(t) == 0:
        return {'ready': False, 'kgs_missing': meta.get('kgs_missing') or [], 'retry_after_s': RETRY_AFTER_S}
    d = aggregate_all(t)
    d['ready'] = True
    d['partial'] = bool(meta.get('partial'))
    if meta.get('kgs_missing'):
        d['kgs_missing'] = meta['kgs_missing']
    d['epoch'] = meta.get('epoch')
    return d


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='cells.sqlite maintenance')
    ap.add_argument('--rebuild', action='store_true', help='rescan data/ne/*.h3b headers into data/ne/cells.sqlite')
    ap.add_argument('--status', action='store_true')
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.rebuild:
        print(json.dumps(rebuild_index()))
    print(json.dumps(index_status(), indent=1))
